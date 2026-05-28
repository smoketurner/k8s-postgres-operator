//! DDL audit for the source cluster during a logical-replication upgrade.
//!
//! Logical replication does not replicate DDL — `CREATE TABLE`,
//! `ALTER TABLE`, `CREATE INDEX`, etc. that happen on the source during the
//! replication window will silently fail to land on the target. Wiz's
//! published Aurora playbook calls this out as a primary cause of broken
//! cutovers and ships an audit mechanism around it.
//!
//! This module installs a small server-side audit on the source cluster
//! that logs every successful DDL command into an operator-namespaced
//! table. The reconciler polls the row count between phases and refuses
//! to cut over if any DDL has been observed, unless the user explicitly
//! sets `spec.strategy.acknowledgeDDL: true` to override.
//!
//! ## Lifecycle
//!
//! - **Install** — on entering `ConfiguringReplication`, before the
//!   publication is created. Idempotent — re-running it is safe (the
//!   `IF NOT EXISTS` / `OR REPLACE` clauses cover that).
//! - **Poll** — the reconciler reads `count_ddl_events` periodically while
//!   the upgrade is in `Replicating` / `Verifying` and patches the count
//!   onto `status.replication.ddlCount`.
//! - **Uninstall** — on any terminal phase (`Completed`, `Failed`,
//!   `RolledBack`) and on deletion of the `PostgresUpgrade` resource.
//!   Drops the trigger, function, and audit table. Idempotent.
//!
//! ## Schema
//!
//! Three objects, all owned by the role the operator connects as
//! (typically Spilo's `postgres` superuser):
//!
//! - Table `public.postgres_operator_ddl_audit` — one row per observed
//!   DDL command, with `command_tag`, `object_type`, `schema_name`,
//!   `object_identity`, `occurred_at`.
//! - Function `public.postgres_operator_log_ddl()` — plpgsql, body
//!   iterates `pg_event_trigger_ddl_commands()` and inserts each row.
//! - Event trigger `postgres_operator_ddl_audit` — `ON ddl_command_end`,
//!   executes the function above.
//!
//! The audit table is namespaced with `postgres_operator_` so it doesn't
//! collide with user schema. Operators that run multiple concurrent
//! upgrades against the same source cluster will share this table; today
//! the upgrade-in-progress annotation (introduced in Phase 0) prevents
//! concurrent upgrades from being orchestrated against the same source.

use tracing::{debug, info, instrument};

use crate::resources::postgres_client::PostgresConnection;
use crate::resources::replication::ReplicationResult;

/// SQL identifier of the audit table.
pub const AUDIT_TABLE: &str = "public.postgres_operator_ddl_audit";
/// SQL identifier of the audit function.
pub const AUDIT_FUNCTION: &str = "public.postgres_operator_log_ddl";
/// Name of the event trigger.
pub const AUDIT_TRIGGER: &str = "postgres_operator_ddl_audit";

/// One observed DDL command, as recorded by the audit trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlEvent {
    pub command_tag: String,
    pub object_type: Option<String>,
    pub schema_name: Option<String>,
    pub object_identity: Option<String>,
    /// RFC 3339 timestamp string.
    pub occurred_at: String,
}

/// Install the audit table, function, and event trigger on the source.
/// Safe to call multiple times — every statement is idempotent.
///
/// Requires the connecting role to have `CREATE` on the `public` schema
/// and the ability to create event triggers (superuser, or
/// `pg_event_trigger` predefined role in PG 18+). Spilo's `postgres` role
/// satisfies both.
#[instrument(skip(conn))]
pub async fn install_ddl_audit(conn: &PostgresConnection) -> ReplicationResult<()> {
    // Table first so the trigger function has somewhere to INSERT into
    // even on the very first DDL command.
    conn.batch_execute(&format!(
        "CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
            id              bigserial PRIMARY KEY,
            occurred_at     timestamptz NOT NULL DEFAULT now(),
            command_tag     text        NOT NULL,
            object_type     text,
            schema_name     text,
            object_identity text
        )"
    ))
    .await?;

    // Function. `CREATE OR REPLACE` covers re-installs.
    conn.batch_execute(&format!(
        "CREATE OR REPLACE FUNCTION {AUDIT_FUNCTION}() RETURNS event_trigger
         LANGUAGE plpgsql
         AS $body$
         BEGIN
             INSERT INTO {AUDIT_TABLE} (command_tag, object_type, schema_name, object_identity)
             SELECT command_tag, object_type, schema_name, object_identity
             FROM pg_event_trigger_ddl_commands();
         END
         $body$"
    ))
    .await?;

    // Event trigger. There is no CREATE OR REPLACE for event triggers, so
    // we drop first if present.
    conn.batch_execute(&format!("DROP EVENT TRIGGER IF EXISTS {AUDIT_TRIGGER}"))
        .await?;
    conn.batch_execute(&format!(
        "CREATE EVENT TRIGGER {AUDIT_TRIGGER} ON ddl_command_end EXECUTE FUNCTION {AUDIT_FUNCTION}()"
    ))
    .await?;

    info!(
        "DDL audit installed on source (table {}, trigger {})",
        AUDIT_TABLE, AUDIT_TRIGGER
    );
    Ok(())
}

/// Total number of DDL commands logged since `install_ddl_audit` was
/// called (or `truncate_ddl_audit` was last run).
#[instrument(skip(conn))]
pub async fn count_ddl_events(conn: &PostgresConnection) -> ReplicationResult<i64> {
    // If the table doesn't exist (e.g. someone uninstalled the audit
    // out-of-band), return zero rather than erroring — the reconciler
    // can't recover from a missing table during a poll, and the
    // condition will reflect zero observed.
    let row = conn
        .query_one(
            &format!(
                "SELECT
                    CASE WHEN to_regclass('{AUDIT_TABLE}') IS NULL
                         THEN 0::bigint
                         ELSE (SELECT COUNT(*)::bigint FROM {AUDIT_TABLE})
                    END AS n"
            ),
            &[],
        )
        .await?;
    Ok(row.get::<_, i64>("n"))
}

/// Sample of the most recent DDL events for surfacing in condition
/// messages. Bounded by `limit` so the message fits.
#[instrument(skip(conn))]
pub async fn recent_ddl_samples(
    conn: &PostgresConnection,
    limit: i64,
) -> ReplicationResult<Vec<DdlEvent>> {
    let rows = conn
        .query(
            &format!(
                "SELECT command_tag, object_type, schema_name, object_identity,
                        occurred_at::text AS occurred_at
                 FROM {AUDIT_TABLE}
                 ORDER BY occurred_at DESC
                 LIMIT $1::bigint"
            ),
            &[&limit],
        )
        .await?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        events.push(DdlEvent {
            command_tag: row.get::<_, String>("command_tag"),
            object_type: row
                .try_get::<_, Option<String>>("object_type")
                .unwrap_or(None),
            schema_name: row
                .try_get::<_, Option<String>>("schema_name")
                .unwrap_or(None),
            object_identity: row
                .try_get::<_, Option<String>>("object_identity")
                .unwrap_or(None),
            occurred_at: row.get::<_, String>("occurred_at"),
        });
    }
    Ok(events)
}

/// Drop the audit trigger, function, and table. Idempotent.
/// Called on terminal upgrade phases and on resource deletion.
#[instrument(skip(conn))]
pub async fn uninstall_ddl_audit(conn: &PostgresConnection) -> ReplicationResult<()> {
    // Trigger and function first, since a trigger may reference the
    // function. Then the table.
    conn.batch_execute(&format!("DROP EVENT TRIGGER IF EXISTS {AUDIT_TRIGGER}"))
        .await?;
    conn.batch_execute(&format!("DROP FUNCTION IF EXISTS {AUDIT_FUNCTION}()"))
        .await?;
    conn.batch_execute(&format!("DROP TABLE IF EXISTS {AUDIT_TABLE}"))
        .await?;
    debug!("DDL audit uninstalled from source");
    Ok(())
}

/// Render a short, bounded message describing a DDL audit sample,
/// suitable for the `DDLObserved` condition's `message` field.
pub fn render_ddl_sample_message(count: i64, samples: &[DdlEvent]) -> String {
    if count <= 0 {
        return "No DDL observed on source during the replication window".to_string();
    }
    let head: Vec<String> = samples
        .iter()
        .take(5)
        .map(|e| {
            let obj = e
                .object_identity
                .as_deref()
                .or(e.schema_name.as_deref())
                .unwrap_or("?");
            format!("{} {obj}", e.command_tag)
        })
        .collect();

    if head.is_empty() {
        format!(
            "{count} DDL command(s) observed on source during the replication window. \
             Logical replication does not replicate DDL, so the target schema is now \
             out of sync. Either abort and restart the upgrade, or set \
             spec.strategy.acknowledgeDDL: true if you have manually applied the \
             matching DDL to the target."
        )
    } else {
        format!(
            "{count} DDL command(s) observed on source during the replication window. \
             Most recent: {}. Logical replication does not replicate DDL, so the target \
             schema is now out of sync. Either abort and restart the upgrade, or set \
             spec.strategy.acknowledgeDDL: true if you have manually applied the \
             matching DDL to the target.",
            head.join("; ")
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn sample(tag: &str, identity: &str) -> DdlEvent {
        DdlEvent {
            command_tag: tag.into(),
            object_type: Some("table".into()),
            schema_name: Some("public".into()),
            object_identity: Some(identity.into()),
            occurred_at: "2026-05-28T12:00:00Z".into(),
        }
    }

    #[test]
    fn render_zero_count() {
        let msg = render_ddl_sample_message(0, &[]);
        assert!(msg.contains("No DDL observed"));
    }

    #[test]
    fn render_count_no_samples() {
        let msg = render_ddl_sample_message(3, &[]);
        assert!(msg.contains("3 DDL command(s)"));
        assert!(msg.contains("acknowledgeDDL"));
        // No "Most recent" when we have no samples to show.
        assert!(!msg.contains("Most recent"));
    }

    #[test]
    fn render_with_samples_lists_head() {
        let samples = vec![
            sample("CREATE TABLE", "public.orders"),
            sample("ALTER TABLE", "public.items"),
        ];
        let msg = render_ddl_sample_message(2, &samples);
        assert!(msg.contains("2 DDL command(s)"));
        assert!(msg.contains("Most recent"));
        assert!(msg.contains("CREATE TABLE public.orders"));
        assert!(msg.contains("ALTER TABLE public.items"));
        assert!(msg.contains("acknowledgeDDL"));
    }

    #[test]
    fn render_truncates_samples_to_five() {
        let samples: Vec<DdlEvent> = (0..10)
            .map(|i| sample("CREATE INDEX", &format!("public.idx_{i}")))
            .collect();
        let msg = render_ddl_sample_message(10, &samples);
        assert!(msg.contains("public.idx_0"));
        assert!(msg.contains("public.idx_4"));
        // 6th onward should not appear in the message.
        assert!(!msg.contains("public.idx_5"));
        assert!(!msg.contains("public.idx_9"));
    }

    #[test]
    fn render_handles_missing_identity_fields() {
        let event = DdlEvent {
            command_tag: "REFRESH MATERIALIZED VIEW".into(),
            object_type: None,
            schema_name: None,
            object_identity: None,
            occurred_at: "2026-05-28T12:00:00Z".into(),
        };
        let msg = render_ddl_sample_message(1, &[event]);
        // Falls back to "?" when identity and schema are both missing.
        assert!(msg.contains("REFRESH MATERIALIZED VIEW ?"));
    }
}
