//! Replication-compatibility preflight checks for `PostgresUpgrade`.
//!
//! Runs against the source `PostgresCluster` before the FSM transitions
//! `Pending → CreatingTarget`. Each check guards a known logical-replication
//! footgun:
//!
//! - **Replica identity** — tables without a primary key and without an
//!   explicit replica identity drop UPDATE/DELETE silently during logical
//!   replication.
//! - **Large objects** — `pg_largeobject` content is not replicated by
//!   logical replication; if any large objects exist the user must accept
//!   they won't migrate (we refuse rather than silently skip).
//! - **Unlogged tables** — unlogged tables are not replicated; the user
//!   typically intends them to migrate, so we surface this explicitly.
//! - **Blocking extensions** — `pg_cron` and `pg_partman` interfere with
//!   logical replication (Wiz's documented Aurora playbook deactivates them
//!   before upgrade). We refuse rather than auto-disable them on the user's
//!   running cluster.
//! - **Materialized view refresh in progress** — actively refreshing a
//!   materialized view can break the publication, forcing the upgrade to
//!   restart from scratch.
//!
//! Each failure is a permanent condition for *this* upgrade resource.
//! After the user fixes the source, they must create a new
//! `PostgresUpgrade` to retry.

use std::fmt;

use kube::Client;
use tracing::{debug, info, warn};

use crate::resources::postgres_client::{PostgresClientError, PostgresConnection};

/// Errors raised while *running* the preflight checks themselves
/// (connectivity, query failures). These are distinct from preflight
/// *failures* — a check that successfully returns "this user table has no
/// PK" is a [`PreflightFailure`], not a [`PreflightError`].
#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    /// Couldn't connect to the source primary.
    #[error("Failed to connect to source primary for preflight: {0}")]
    Connect(#[source] PostgresClientError),

    /// A preflight query returned an error.
    #[error("Preflight query failed: {0}")]
    Query(#[source] PostgresClientError),
}

impl From<PostgresClientError> for PreflightError {
    fn from(err: PostgresClientError) -> Self {
        // The discriminant between Connect/Query is contextual; the only
        // place we use the `?` operator with this is inside individual
        // check fns whose errors are query errors. Connect is constructed
        // explicitly at the entry point.
        PreflightError::Query(err)
    }
}

/// A single preflight check failure. The variants are deliberately
/// structured so the renderer can emit human-readable messages with
/// concrete details (which tables, which extensions) rather than vague
/// "preflight failed" strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightFailure {
    /// User tables that have neither a primary key nor an explicitly set
    /// non-default replica identity. Limited to a sample of names if the
    /// total is large.
    TablesWithoutReplicaIdentity { tables: Vec<String>, total: usize },

    /// `pg_largeobject` is non-empty. Logical replication does not
    /// replicate large objects.
    LargeObjectsPresent,

    /// User tables with `relpersistence = 'u'` (unlogged) found. Limited
    /// to a sample of names if the total is large.
    UnloggedTables { tables: Vec<String>, total: usize },

    /// Extensions known to interfere with logical replication are active
    /// on the source.
    BlockingExtensions(Vec<String>),

    /// `pg_stat_activity` shows an in-progress `REFRESH MATERIALIZED VIEW`.
    MaterializedViewRefreshInProgress,

    /// The target cluster's storage spec is smaller than
    /// `source_data_bytes * STORAGE_SAFETY_MARGIN`. Without headroom for
    /// WAL accumulation during initial sync, the target PVC fills up
    /// before logical replication catches up.
    InsufficientTargetStorage {
        source_data_bytes: i64,
        target_storage_bytes: i64,
        required_bytes: i64,
        target_storage_size: String,
    },
}

/// Required free headroom on the target PVC, expressed as a multiplier of
/// the source's current `pg_database_size` total. Covers WAL accumulation
/// during initial sync plus normal write growth.
pub const STORAGE_SAFETY_MARGIN: f64 = 1.5;

impl fmt::Display for PreflightFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PreflightFailure::TablesWithoutReplicaIdentity { tables, total } => {
                write!(
                    f,
                    "{total} table(s) lack a primary key and a non-default replica identity; \
                     UPDATE/DELETE will not replicate. Add PRIMARY KEY or \
                     ALTER TABLE ... REPLICA IDENTITY FULL. Examples: {}",
                    render_sample(tables, *total)
                )
            }
            PreflightFailure::LargeObjectsPresent => write!(
                f,
                "pg_largeobject is non-empty; logical replication does not replicate large \
                 objects. Migrate large object data separately or remove it before upgrading."
            ),
            PreflightFailure::UnloggedTables { tables, total } => write!(
                f,
                "{total} unlogged table(s) found; unlogged tables are not replicated. \
                 ALTER TABLE ... SET LOGGED, or accept they will be empty on the new cluster. \
                 Examples: {}",
                render_sample(tables, *total)
            ),
            PreflightFailure::BlockingExtensions(exts) => write!(
                f,
                "Extension(s) known to interfere with logical replication are active: {}. \
                 Disable them on the source before upgrading (DROP EXTENSION) and recreate \
                 on the target after cutover.",
                exts.join(", ")
            ),
            PreflightFailure::MaterializedViewRefreshInProgress => write!(
                f,
                "REFRESH MATERIALIZED VIEW is in progress on the source. Concurrent \
                 materialized view refresh can break logical replication mid-stream. \
                 Wait for the refresh to complete or pause the schedule before retrying."
            ),
            PreflightFailure::InsufficientTargetStorage {
                source_data_bytes,
                target_storage_bytes,
                required_bytes,
                target_storage_size,
            } => write!(
                f,
                "Target storage spec ({target_storage_size} = {} bytes) is smaller than the \
                 required headroom for initial sync (source data = {} bytes, requires at least \
                 {}× = {} bytes). Expand the source cluster's storage spec (the target \
                 inherits it) or set targetClusterOverrides.storage on the PostgresUpgrade.",
                target_storage_bytes, source_data_bytes, STORAGE_SAFETY_MARGIN, required_bytes
            ),
        }
    }
}

fn render_sample(names: &[String], total: usize) -> String {
    const MAX_SHOWN: usize = 5;
    if names.is_empty() {
        return "(none enumerated)".to_string();
    }
    let shown: Vec<_> = names.iter().take(MAX_SHOWN).cloned().collect();
    let suffix = total.saturating_sub(shown.len());
    let mut s = shown.join(", ");
    if suffix > 0 {
        s.push_str(&format!(" (+{suffix} more)"));
    }
    s
}

/// Aggregated outcome of a preflight run.
#[derive(Debug, Clone, Default)]
pub struct PreflightOutcome {
    pub failures: Vec<PreflightFailure>,
}

impl PreflightOutcome {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    /// Render each failure as a self-contained string suitable for the
    /// upgrade status condition `message` field and for Kubernetes Events.
    pub fn failure_messages(&self) -> Vec<String> {
        self.failures.iter().map(|f| f.to_string()).collect()
    }

    /// Short one-line summary, e.g. for the `last_error` status field and
    /// the `UpgradeError::PreflightCheckFailed.summary`.
    pub fn summary(&self) -> String {
        match self.failures.len() {
            0 => "all preflight checks passed".to_string(),
            1 => "1 preflight check failed".to_string(),
            n => format!("{n} preflight checks failed"),
        }
    }
}

/// Run all replication-compatibility preflight checks against the source
/// `PostgresCluster`'s primary. Returns an `Ok(outcome)` whether the
/// checks pass or fail — the `Err` case is reserved for *infrastructure*
/// failures (couldn't connect, query errored).
///
/// `target_storage_size` is the storage spec that the target cluster will
/// be created with (typically inherited from the source spec). It is
/// compared against the source's actual data size for the
/// `InsufficientTargetStorage` check.
pub async fn run_preflight_checks(
    client: &Client,
    source_ns: &str,
    source_cluster: &str,
    target_storage_size: &str,
) -> Result<PreflightOutcome, PreflightError> {
    info!(
        "Running upgrade preflight checks against source {}/{}",
        source_ns, source_cluster
    );

    let conn = PostgresConnection::connect_primary(client, source_ns, source_cluster)
        .await
        .map_err(PreflightError::Connect)?;

    let mut outcome = PreflightOutcome::default();

    if let Some(failure) = check_replica_identity(&conn).await? {
        outcome.failures.push(failure);
    }
    if check_large_objects(&conn).await? {
        outcome.failures.push(PreflightFailure::LargeObjectsPresent);
    }
    if let Some(failure) = check_unlogged_tables(&conn).await? {
        outcome.failures.push(failure);
    }
    if let Some(failure) = check_blocking_extensions(&conn).await? {
        outcome.failures.push(failure);
    }
    if check_active_matview_refresh(&conn).await? {
        outcome
            .failures
            .push(PreflightFailure::MaterializedViewRefreshInProgress);
    }
    if let Some(failure) = check_target_storage_size(&conn, target_storage_size).await? {
        outcome.failures.push(failure);
    }

    if outcome.passed() {
        info!(
            "Preflight checks passed for source {}/{}",
            source_ns, source_cluster
        );
    } else {
        warn!(
            "Preflight checks failed for source {}/{}: {} failure(s)",
            source_ns,
            source_cluster,
            outcome.failures.len()
        );
    }

    Ok(outcome)
}

/// Look for permanent user tables that cannot replicate UPDATE/DELETE under
/// logical replication. Two cases are unsafe:
///
/// - `relreplident = 'n'` ("nothing"): UPDATE/DELETE can never be replicated,
///   **regardless of whether the table has a primary key**. Attempting them on
///   the source produces a publisher-side error
///   (`cannot update table ... because it does not have a replica identity`).
/// - `relreplident = 'd'` ("default"): falls back to the primary key, so it is
///   only unsafe when the table has no primary key.
///
/// Returns `Ok(None)` if all user tables are safe to replicate.
async fn check_replica_identity(
    conn: &PostgresConnection,
) -> Result<Option<PreflightFailure>, PreflightError> {
    let sql = "
        SELECT n.nspname || '.' || c.relname AS table_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND c.relpersistence = 'p'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_%'
          AND (
              c.relreplident = 'n'
              OR (c.relreplident = 'd' AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_index i
                  WHERE i.indrelid = c.oid AND i.indisprimary
              ))
          )
        ORDER BY 1
    ";
    let rows = conn.query(sql, &[]).await?;
    if rows.is_empty() {
        debug!("Replica identity preflight: all user tables have PK or explicit identity");
        return Ok(None);
    }
    let tables: Vec<String> = rows
        .iter()
        .map(|r| r.get::<_, String>("table_name"))
        .collect();
    let total = tables.len();
    Ok(Some(PreflightFailure::TablesWithoutReplicaIdentity {
        tables,
        total,
    }))
}

/// Return true if any rows exist in `pg_largeobject_metadata`.
async fn check_large_objects(conn: &PostgresConnection) -> Result<bool, PreflightError> {
    let sql = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_largeobject_metadata LIMIT 1) AS present";
    let row = conn.query_one(sql, &[]).await?;
    let present: bool = row.get("present");
    if present {
        debug!("Replica identity preflight: pg_largeobject_metadata is non-empty");
    }
    Ok(present)
}

/// Find user-schema unlogged tables.
async fn check_unlogged_tables(
    conn: &PostgresConnection,
) -> Result<Option<PreflightFailure>, PreflightError> {
    let sql = "
        SELECT n.nspname || '.' || c.relname AS table_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND c.relpersistence = 'u'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_%'
        ORDER BY 1
    ";
    let rows = conn.query(sql, &[]).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let tables: Vec<String> = rows
        .iter()
        .map(|r| r.get::<_, String>("table_name"))
        .collect();
    let total = tables.len();
    Ok(Some(PreflightFailure::UnloggedTables { tables, total }))
}

/// Find extensions known to interfere with logical replication.
async fn check_blocking_extensions(
    conn: &PostgresConnection,
) -> Result<Option<PreflightFailure>, PreflightError> {
    let sql = "
        SELECT extname
        FROM pg_catalog.pg_extension
        WHERE extname = ANY($1::text[])
        ORDER BY extname
    ";
    let blocklist = vec!["pg_cron".to_string(), "pg_partman".to_string()];
    let rows = conn.query(sql, &[&blocklist]).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let found: Vec<String> = rows.iter().map(|r| r.get::<_, String>("extname")).collect();
    Ok(Some(PreflightFailure::BlockingExtensions(found)))
}

/// Detect any in-flight `REFRESH MATERIALIZED VIEW` on the source.
async fn check_active_matview_refresh(conn: &PostgresConnection) -> Result<bool, PreflightError> {
    let sql = "
        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_stat_activity
            WHERE state = 'active'
              AND query ILIKE 'REFRESH MATERIALIZED VIEW%'
        ) AS active
    ";
    let row = conn.query_one(sql, &[]).await?;
    let active: bool = row.get("active");
    Ok(active)
}

/// Compare the source's actual on-disk data size against the target's
/// declared storage capacity, requiring [`STORAGE_SAFETY_MARGIN`] of
/// headroom for WAL accumulation during initial sync. Returns `Ok(None)`
/// if storage is adequate.
///
/// Source size is `SUM(pg_database_size)` across all non-template
/// databases — that's what actually lives on the PVC. Comparing against
/// just `current_database()` would miss multi-database clusters.
async fn check_target_storage_size(
    conn: &PostgresConnection,
    target_storage_size: &str,
) -> Result<Option<PreflightFailure>, PreflightError> {
    let Some(target_storage_bytes) = parse_kube_quantity_bytes(target_storage_size) else {
        // Unparseable spec — surface as a failure rather than skip the
        // check silently. The user can read the message and fix the spec.
        warn!(
            "target storage spec {:?} could not be parsed as a Kubernetes quantity",
            target_storage_size
        );
        return Ok(Some(PreflightFailure::InsufficientTargetStorage {
            source_data_bytes: 0,
            target_storage_bytes: 0,
            required_bytes: 0,
            target_storage_size: target_storage_size.to_string(),
        }));
    };

    let row = conn
        .query_one(
            "SELECT COALESCE(SUM(pg_database_size(datname)), 0)::bigint AS total_bytes
             FROM pg_catalog.pg_database
             WHERE datistemplate = false",
            &[],
        )
        .await?;
    let source_data_bytes: i64 = row.get("total_bytes");
    let required_bytes = required_target_bytes(source_data_bytes);

    if target_storage_bytes < required_bytes {
        debug!(
            "Storage preflight: target {} bytes < required {} bytes (source {} bytes × {})",
            target_storage_bytes, required_bytes, source_data_bytes, STORAGE_SAFETY_MARGIN
        );
        return Ok(Some(PreflightFailure::InsufficientTargetStorage {
            source_data_bytes,
            target_storage_bytes,
            required_bytes,
            target_storage_size: target_storage_size.to_string(),
        }));
    }

    Ok(None)
}

/// `source_data_bytes * STORAGE_SAFETY_MARGIN`, clamped to non-negative
/// `i64`. Extracted so the saturation logic is testable without a live
/// Postgres connection.
fn required_target_bytes(source_data_bytes: i64) -> i64 {
    if source_data_bytes < 0 {
        return 0;
    }
    let scaled = (source_data_bytes as f64) * STORAGE_SAFETY_MARGIN;
    if !scaled.is_finite() || scaled < 0.0 {
        return 0;
    }
    if scaled >= i64::MAX as f64 {
        return i64::MAX;
    }
    scaled as i64
}

/// Parse a Kubernetes [resource.Quantity] string to bytes.
///
/// Accepts binary suffixes (`Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei`), decimal
/// suffixes (`k`, `M`, `G`, `T`, `P`, `E`, plus capital `K` as a tolerated
/// non-standard), and a raw integer (interpreted as bytes). Returns
/// `None` on malformed input or overflow.
///
/// [resource.Quantity]: https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity/
fn parse_kube_quantity_bytes(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Longest suffixes first so "Ki" matches before "K".
    const BINARY: &[(&str, i64)] = &[
        ("Ei", 1i64 << 60),
        ("Pi", 1i64 << 50),
        ("Ti", 1i64 << 40),
        ("Gi", 1i64 << 30),
        ("Mi", 1i64 << 20),
        ("Ki", 1i64 << 10),
    ];
    const DECIMAL: &[(&str, i64)] = &[
        ("E", 1_000_000_000_000_000_000),
        ("P", 1_000_000_000_000_000),
        ("T", 1_000_000_000_000),
        ("G", 1_000_000_000),
        ("M", 1_000_000),
        ("k", 1_000),
        // Capital K is non-standard but tolerated by some tools; accept it.
        ("K", 1_000),
    ];

    for (suffix, multiplier) in BINARY.iter().chain(DECIMAL.iter()) {
        if let Some(prefix) = s.strip_suffix(suffix) {
            let value: i64 = prefix.trim().parse().ok()?;
            return value.checked_mul(*multiplier);
        }
    }

    // Raw bytes (no suffix), e.g. "500000000".
    s.parse().ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn outcome_passed_when_no_failures() {
        let outcome = PreflightOutcome::default();
        assert!(outcome.passed());
        assert!(outcome.failure_messages().is_empty());
        assert_eq!(outcome.summary(), "all preflight checks passed");
    }

    #[test]
    fn outcome_summary_counts_failures() {
        let outcome = PreflightOutcome {
            failures: vec![PreflightFailure::LargeObjectsPresent],
        };
        assert!(!outcome.passed());
        assert_eq!(outcome.summary(), "1 preflight check failed");

        let outcome = PreflightOutcome {
            failures: vec![
                PreflightFailure::LargeObjectsPresent,
                PreflightFailure::MaterializedViewRefreshInProgress,
            ],
        };
        assert_eq!(outcome.summary(), "2 preflight checks failed");
    }

    #[test]
    fn replica_identity_message_lists_sample_tables() {
        let failure = PreflightFailure::TablesWithoutReplicaIdentity {
            tables: vec!["public.orders".into(), "shop.items".into()],
            total: 2,
        };
        let msg = failure.to_string();
        assert!(msg.contains("2 table(s) lack a primary key"));
        assert!(msg.contains("public.orders"));
        assert!(msg.contains("shop.items"));
        assert!(msg.contains("ALTER TABLE ... REPLICA IDENTITY FULL"));
    }

    #[test]
    fn replica_identity_message_truncates_long_sample() {
        let tables: Vec<String> = (0..20).map(|i| format!("public.t{i}")).collect();
        let total = tables.len();
        let failure = PreflightFailure::TablesWithoutReplicaIdentity { tables, total };
        let msg = failure.to_string();
        assert!(msg.contains("public.t0"));
        assert!(msg.contains("public.t4"));
        // 20 total, 5 shown → +15 more.
        assert!(msg.contains("(+15 more)"), "got: {msg}");
        // Truncated examples shouldn't include t10 or t19.
        assert!(!msg.contains("public.t10"));
    }

    #[test]
    fn large_objects_message_is_actionable() {
        let msg = PreflightFailure::LargeObjectsPresent.to_string();
        assert!(msg.contains("pg_largeobject"));
        assert!(msg.contains("Migrate"));
    }

    #[test]
    fn unlogged_tables_message_lists_names() {
        let failure = PreflightFailure::UnloggedTables {
            tables: vec!["public.cache".into()],
            total: 1,
        };
        let msg = failure.to_string();
        assert!(msg.contains("1 unlogged table"));
        assert!(msg.contains("public.cache"));
        assert!(msg.contains("SET LOGGED"));
    }

    #[test]
    fn blocking_extensions_message_lists_extensions() {
        let failure =
            PreflightFailure::BlockingExtensions(vec!["pg_cron".into(), "pg_partman".into()]);
        let msg = failure.to_string();
        assert!(msg.contains("pg_cron"));
        assert!(msg.contains("pg_partman"));
        assert!(msg.contains("DROP EXTENSION"));
    }

    #[test]
    fn matview_message_explains_risk() {
        let msg = PreflightFailure::MaterializedViewRefreshInProgress.to_string();
        assert!(msg.contains("REFRESH MATERIALIZED VIEW"));
        assert!(msg.contains("logical replication"));
    }

    #[test]
    fn render_sample_handles_empty() {
        assert_eq!(render_sample(&[], 0), "(none enumerated)");
    }

    #[test]
    fn render_sample_handles_within_max() {
        let names = vec!["a".into(), "b".into(), "c".into()];
        let s = render_sample(&names, 3);
        assert_eq!(s, "a, b, c");
        assert!(!s.contains("more"));
    }

    #[test]
    fn render_sample_truncates_over_max() {
        let names: Vec<String> = (0..10).map(|i| format!("t{i}")).collect();
        let s = render_sample(&names, 10);
        assert!(s.contains("t0"));
        assert!(s.contains("t4"));
        assert!(s.contains("(+5 more)"));
    }

    #[test]
    fn parse_quantity_binary_suffixes() {
        assert_eq!(parse_kube_quantity_bytes("1Ki"), Some(1024));
        assert_eq!(parse_kube_quantity_bytes("1Mi"), Some(1 << 20));
        assert_eq!(parse_kube_quantity_bytes("1Gi"), Some(1 << 30));
        assert_eq!(parse_kube_quantity_bytes("10Gi"), Some(10i64 << 30));
        assert_eq!(parse_kube_quantity_bytes("100Gi"), Some(100i64 << 30));
        assert_eq!(parse_kube_quantity_bytes("1Ti"), Some(1i64 << 40));
    }

    #[test]
    fn parse_quantity_decimal_suffixes() {
        assert_eq!(parse_kube_quantity_bytes("1k"), Some(1_000));
        assert_eq!(parse_kube_quantity_bytes("1K"), Some(1_000));
        assert_eq!(parse_kube_quantity_bytes("1M"), Some(1_000_000));
        assert_eq!(parse_kube_quantity_bytes("1G"), Some(1_000_000_000));
        assert_eq!(parse_kube_quantity_bytes("1T"), Some(1_000_000_000_000));
    }

    #[test]
    fn parse_quantity_raw_bytes_and_zero() {
        assert_eq!(parse_kube_quantity_bytes("500000000"), Some(500_000_000));
        assert_eq!(parse_kube_quantity_bytes("0"), Some(0));
        assert_eq!(parse_kube_quantity_bytes("  10Gi  "), Some(10i64 << 30));
    }

    #[test]
    fn parse_quantity_rejects_garbage() {
        assert_eq!(parse_kube_quantity_bytes(""), None);
        assert_eq!(parse_kube_quantity_bytes("   "), None);
        assert_eq!(parse_kube_quantity_bytes("Gi"), None);
        assert_eq!(parse_kube_quantity_bytes("10XB"), None);
        assert_eq!(parse_kube_quantity_bytes("ten gigabytes"), None);
        // Floating-point sizes are not in the K8s quantity spec we accept;
        // reject explicitly so users get a clear failure.
        assert_eq!(parse_kube_quantity_bytes("1.5Gi"), None);
    }

    #[test]
    fn parse_quantity_overflow_returns_none() {
        // i64::MAX is ~9.2 EiB; 100 Ei overflows.
        assert_eq!(parse_kube_quantity_bytes("100Ei"), None);
        assert_eq!(parse_kube_quantity_bytes("99999999999999999999Gi"), None);
    }

    #[test]
    fn required_target_bytes_scales_by_safety_margin() {
        // 10 Gi source needs at least 15 Gi target.
        let ten_gib: i64 = 10 << 30;
        let fifteen_gib: i64 = 15 << 30;
        assert_eq!(required_target_bytes(ten_gib), fifteen_gib);
    }

    #[test]
    fn required_target_bytes_handles_zero_and_negative() {
        // Zero-byte source: zero requirement.
        assert_eq!(required_target_bytes(0), 0);
        // Defensive: negative input clamped to zero (pg_database_size
        // should never be negative, but be paranoid).
        assert_eq!(required_target_bytes(-1), 0);
    }

    #[test]
    fn required_target_bytes_saturates_at_i64_max() {
        // Multiplying near i64::MAX by 1.5 overflows; the helper clamps.
        let result = required_target_bytes(i64::MAX);
        assert_eq!(result, i64::MAX);
    }

    #[test]
    fn insufficient_storage_message_is_actionable() {
        let failure = PreflightFailure::InsufficientTargetStorage {
            source_data_bytes: 10 * 1_000_000_000, // 10 GB
            target_storage_bytes: 10 * (1 << 30),  // 10 Gi
            required_bytes: 15 * 1_000_000_000,    // 15 GB
            target_storage_size: "10Gi".to_string(),
        };
        let msg = failure.to_string();
        assert!(msg.contains("10Gi"), "got: {msg}");
        assert!(msg.contains("1.5"), "got: {msg}");
        assert!(
            msg.contains("targetClusterOverrides") || msg.contains("source cluster"),
            "should suggest a remediation; got: {msg}"
        );
    }
}
