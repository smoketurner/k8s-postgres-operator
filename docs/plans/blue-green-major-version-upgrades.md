# PostgreSQL Operator Review & Blue-Green Major Version Upgrade Plan

## Executive Summary

The platform-engineer review found the operator **~70% production-ready** with a solid Rust/Kubernetes foundation. The most impactful gap for Day-2 operations is **major version upgrades**, which currently rely on Patroni's disruptive approach (cluster downtime during pg_upgrade).

This plan proposes adding a **blue-green upgrade mechanism using logical replication** for near-zero downtime major version upgrades.

**Review Status:**
| Reviewer | Verdict | Key Requirements |
|----------|---------|------------------|
| Rust Engineer | Approved | Error classification, LSN comparison, SyncingSequences phase |
| Kubernetes Specialist | Approved | Atomic service switching, owner reference strategy, webhooks |
| Platform Engineer | 8/10 Approved | Backup required for auto-cutover, automated rollback, connection draining |

---

## Part 1: Platform Engineer Review Findings

### Strengths
| Area | Finding |
|------|---------|
| TLS by default | cert-manager integration |
| Backup encryption | Required when backups configured |
| HA | Patroni automatic failover |
| Modern K8s | In-place resize (K8s 1.35+) |
| Code quality | Panic-free, server-side apply |

### Critical Gaps (Priority Order)

| Priority | Gap | Impact |
|----------|-----|--------|
| HIGH | Database/user provisioning | Most common support ticket |
| HIGH | pg_hba too permissive (10.0.0.0/8 allowed) | Security risk |
| HIGH | No admission webhook | Can't enforce policies |
| HIGH | **Major version upgrades** | Requires cluster downtime |
| MEDIUM | No fleet health metrics | Platform visibility |
| MEDIUM | No tier presets | Too many config knobs |
| MEDIUM | Connection string not in secret | KEDA broken |

---

## Part 2: Blue-Green Major Version Upgrade Design

### Current State (Patroni pg_upgrade)

The Patroni documentation describes a **disruptive** approach:
1. Stop Patroni entirely on all nodes
2. Run `pg_upgrade` on primary
3. Wipe DCS state (new system identifier)
4. Wipe standby data directories
5. Restart and let replication rebuild

**Downtime**: Minutes to hours depending on data size

### Proposed State (Blue-Green Logical Replication)

Near-zero downtime using logical replication:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Blue-Green Upgrade Flow                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   [Blue Cluster v16]  ──logical replication──▶  [Green Cluster v17]
│         │                                              │         │
│    Primary + Replicas                           Primary + Replicas
│         │                                              │         │
│   ◀── traffic ──┐                                      │         │
│                 │                                      │         │
│            [Application]                               │         │
│                 │                                      │         │
│   ──── after cutover ─────────────────────▶  traffic ──┘        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Design Philosophy: Safety First

The upgrade mechanism follows the operator's human-operator-friendly design principles:

| Principle | Implementation |
|-----------|----------------|
| **Err on side of safety** | Zero lag required, row counts verified, backups REQUIRED for automatic mode |
| **No data loss** | Cutover blocked until source and target are identical (LSN match verified) |
| **Reversible operations** | Source cluster never auto-deleted, automated rollback via annotation |
| **Clear status** | Every phase reports detailed conditions, metrics, and audit timestamps |
| **Manual override** | Automatic mode can be switched to Manual at any time |
| **Fail-safe defaults** | All safety checks enabled by default, explicit timeouts prevent silent hangs |

### New CRD: PostgresUpgrade

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresUpgrade
metadata:
  name: orders-db-upgrade-v17
  namespace: orders
spec:
  # Source cluster to upgrade from
  sourceCluster:
    name: orders-db
    namespace: orders

  # Target PostgreSQL version (must be higher than source, Spilo-supported: 15, 16, 17)
  targetVersion: "17"  # enum: "15" | "16" | "17" - must be > source version

  # Target cluster configuration overrides (optional)
  targetClusterOverrides:
    replicas: 3
    resources:
      requests:
        cpu: "4"
        memory: "16Gi"

  # Upgrade behavior
  strategy:
    type: BlueGreen  # Only supported type initially

    # When to cutover (supports fleet automation)
    cutover:
      mode: Automatic   # Manual | Automatic
      # For Manual: annotate with "cutover: now" to trigger
      # For rollback: annotate with "rollback: now" to trigger automated rollback

      # Maintenance window for automatic cutover (optional)
      allowedWindow:
        startTime: "02:00"
        endTime: "04:00"
        timezone: "UTC"

    # Safety checks before automatic cutover (safe defaults - err on side of caution)
    preChecks:
      # Replication lag threshold - must be zero for safe cutover
      maxReplicationLagSeconds: 0  # Default: 0 (strictest - no cutover until fully synced)

      # Row count verification - enabled by default for data integrity
      verifyRowCounts: true        # Default: true (always verify)
      rowCountTolerance: 0         # Default: 0 (exact match required)
      minVerificationPasses: 3     # Default: 3 (row counts must match N times consecutively)
      verificationInterval: "1m"   # Default: 1m (time between verification passes)

      # REQUIRED for mode: Automatic (cannot be disabled for automatic cutover)
      requireBackupWithin: "1h"    # Default: 1h - MANDATORY for automatic mode

      # Connection draining before cutover
      drainConnectionsTimeout: "5m"  # Default: 5m (wait for active connections to complete)

    # Timeouts to prevent silent hangs
    timeouts:
      targetClusterReady: "30m"    # Fail if target not ready
      initialSync: "24h"           # Fail if initial sync takes too long
      replicationCatchup: "1h"     # Fail if lag doesn't reach 0 after initial sync
      verification: "30m"          # Fail if verification doesn't pass

    # Post-cutover behavior
    postCutover:
      # Never auto-delete - user must manually clean up
      # This allows indefinite rollback window
      keepSourceCluster: true
      minRetentionPeriod: "24h"    # Minimum time before source cleanup allowed
      healthCheckInterval: "1m"    # Post-cutover health check frequency
      healthCheckDuration: "10m"   # How long to verify target is healthy

status:
  # Standard Kubernetes fields
  observedGeneration: 1

  # Audit trail
  startedAt: "2025-01-11T09:00:00Z"
  completedAt: null

  phase: Pending | CreatingTarget | ConfiguringReplication | Replicating | Verifying | SyncingSequences | ReadyForCutover | WaitingForManualCutover | CuttingOver | HealthChecking | Completed | Failed | RolledBack

  sourceCluster:
    name: orders-db
    version: "16"
    connectionInfo:
      host: orders-db-primary.orders.svc
      port: 5432

  targetCluster:
    name: orders-db-v17-green
    version: "17"
    ready: true
    connectionInfo:
      host: orders-db-v17-green-primary.orders.svc
      port: 5432

  replication:
    status: Active | Synced | Stopped
    lagBytes: 0
    lagSeconds: 0
    lastSyncTime: "2025-01-11T10:00:00Z"
    sourceLsn: "0/1234ABCD"
    targetLsn: "0/1234ABCD"
    lsnInSync: true

  # Row count verification for data integrity
  verification:
    lastCheckTime: "2025-01-11T10:00:00Z"
    tablesVerified: 42
    tablesMatched: 42
    tablesMismatched: 0
    consecutivePasses: 3
    mismatchedTables: []  # List of tables with row count differences

  # Sequence synchronization status
  sequences:
    synced: true
    syncedCount: 15
    failedCount: 0
    failedSequences: []

  # Rollback feasibility tracking
  rollback:
    feasible: true
    reason: "Source cluster running, no writes to target since cutover"
    dataLossRisk: false
    lastSourceWrite: "2025-01-11T10:00:00Z"

  conditions:
    - type: SourceReady
    - type: TargetReady
    - type: ReplicationHealthy
    - type: LsnInSync            # LSN comparison passed
    - type: RowCountsVerified
    - type: SequencesSynced
    - type: BackupVerified       # Recent backup exists
    - type: ConnectionsDrained   # Active connections completed
    - type: ReadyForCutover
    - type: HealthCheckPassed    # Post-cutover health verified
    - type: CutoverComplete
```

**Print Columns for Fleet Visibility:**
```rust
#[kube(printcolumn = r#"{"name":"Source","type":"string","jsonPath":".spec.sourceCluster.name"}"#)]
#[kube(printcolumn = r#"{"name":"TargetVer","type":"string","jsonPath":".spec.targetVersion"}"#)]
#[kube(printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#)]
#[kube(printcolumn = r#"{"name":"Lag","type":"string","jsonPath":".status.replication.lagSeconds"}"#)]
#[kube(printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#)]
```

### Upgrade Phases

#### Phase 1: Pending
- Validate source cluster exists and is Running
- Validate target version > source version (downgrades rejected at CRD validation)
- Validate both versions are Spilo-supported (15, 16, 17)
- Check source has required `wal_level=logical` (enabled by default)
- Verify no concurrent upgrades on same source cluster

#### Phase 2: CreatingTarget
- Create new PostgresCluster with target version
- Name: `{source-name}-v{version}-green`
- **No owner reference** - green cluster survives upgrade deletion
- Labels link to upgrade: `postgres-operator.smoketurner.com/upgrade: {upgrade-name}`
- Copy spec from source with version override
- Wait for target cluster to reach Running state
- Timeout: `timeouts.targetClusterReady` (default 30m)

#### Phase 3: ConfiguringReplication
- Check publication doesn't already exist (idempotent)
- Create publication on source: `CREATE PUBLICATION upgrade_pub FOR ALL TABLES`
- Create subscription on target: `CREATE SUBSCRIPTION upgrade_sub ...`
- For PostgreSQL 17+: Use `pg_createsubscriber` for simpler setup
- Verify subscription is active via `pg_stat_subscription`

#### Phase 4: Replicating
- Monitor replication lag via `pg_stat_subscription`
- Monitor LSN positions for precise sync verification
- Update status with lag metrics
- Emit events for significant lag changes
- Periodically verify row counts across all tables
- Wait until lag = 0 for cutover readiness
- Timeout: `timeouts.initialSync` (default 24h) + `timeouts.replicationCatchup` (default 1h)

#### Phase 5: Verifying
- Compare row counts: source vs target for all tables
- Query: `SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables`
- Execute queries in parallel using `futures::try_join`
- Must match within tolerance (default: exact match)
- Must pass `minVerificationPasses` consecutive times (default: 3)
- Update `status.verification` with results
- Block cutover if mismatches detected
- Timeout: `timeouts.verification` (default 30m)

#### Phase 6: SyncingSequences (NEW)
- Set source to read-only: `ALTER SYSTEM SET default_transaction_read_only = on; SELECT pg_reload_conf();`
- Query all sequences from source: `pg_sequences`
- Sync each sequence to target: `SELECT setval('schema.seq_name', value, true)`
- Update `status.sequences` with sync results
- Block cutover if any sequences fail to sync

#### Phase 7: ReadyForCutover
- All pre-checks passed:
  - Replication lag = 0
  - LSN positions match (verified via `pg_current_wal_lsn()` comparison)
  - Row counts verified (minVerificationPasses consecutive passes)
  - Sequences synced
  - Recent backup exists (REQUIRED for Automatic mode)
- For Manual mode: Transition to WaitingForManualCutover
- For Automatic mode: Check maintenance window, proceed if allowed

#### Phase 7b: WaitingForManualCutover (Manual mode only)
- Wait for `cutover: now` annotation
- Continue monitoring replication health
- If replication becomes unhealthy, transition back to Replicating

#### Phase 8: CuttingOver
- Verify source is read-only (double-check)
- Wait for connection draining: `drainConnectionsTimeout` (default 5m)
- Wait for final replication sync (LSN match verified)
- **Atomic service switching** (single reconciliation):
  1. Patch `{cluster}-primary` service selector → green cluster labels
  2. Patch `{cluster}-repl` service selector → green cluster labels
  3. Update owner references → green cluster
- Drop subscription on target
- Mark source with annotation: `postgres-operator.smoketurner.com/superseded-by: {green-name}`

#### Phase 9: HealthChecking (NEW)
- Verify applications can connect to target cluster
- Monitor for `healthCheckDuration` (default 10m)
- Check target cluster remains healthy
- If health check fails, consider automatic rollback

#### Phase 10: Completed
- Update source PostgresCluster annotation to mark as superseded
- Source cluster remains for manual cleanup (rollback safety)
- Emit `UpgradeCompleted` event with summary
- Record `status.completedAt` timestamp

### Automated Rollback

**Trigger via annotation:**
```bash
kubectl annotate postgresupgrade orders-db-upgrade rollback=now
```

**Rollback sequence:**
1. Verify rollback is feasible (`status.rollback.feasible`)
2. Warn if `status.rollback.dataLossRisk` is true (writes occurred to target)
3. Stop/drop subscription on target
4. Set source back to read-write: `ALTER SYSTEM RESET default_transaction_read_only; SELECT pg_reload_conf();`
5. Atomic service switching back to source cluster
6. Verify source is receiving traffic
7. Mark upgrade as `RolledBack`
8. Emit `RollbackCompleted` event

### Implementation Components

#### New Files
```
src/crd/postgres_upgrade.rs             # PostgresUpgrade CRD
src/controller/upgrade_reconciler.rs    # Upgrade controller
src/controller/upgrade_state_machine.rs # Upgrade phase transitions
src/controller/upgrade_error.rs         # Classified error types
src/resources/replication.rs            # Logical replication setup
src/webhooks/policies/upgrade.rs        # Upgrade validation webhooks
```

#### Modified Files
```
src/main.rs                             # Register upgrade controller
src/lib.rs                              # Export run_upgrade_controller
src/crd/mod.rs                          # Export new CRD
src/resources/patroni.rs                # Default wal_level=logical
src/resources/service.rs                # Support service target switching
src/webhooks/server.rs                  # Register upgrade webhooks
config/rbac/role.yaml                   # Add PostgresUpgrade permissions
```

#### Key Implementation Details

**1. Error Classification** (`src/controller/upgrade_error.rs`)
```rust
#[derive(Debug, thiserror::Error)]
pub enum UpgradeError {
    // --- Permanent Errors (do not retry) ---
    #[error("Validation failed: {0}")]
    ValidationError(String),

    #[error("Version downgrade not allowed: {source} -> {target}")]
    DowngradeNotAllowed { source: String, target: String },

    // --- Transient Errors (retry with backoff) ---
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Cluster not ready: {0}")]
    ClusterNotReady(String),

    #[error("Target cluster creation in progress")]
    TargetCreationInProgress,

    // --- Verification Errors (block cutover, continue monitoring) ---
    #[error("Row count mismatch: {mismatched_tables} tables differ")]
    RowCountMismatch { mismatched_tables: i32 },

    #[error("Replication lag too high: {lag_bytes} bytes")]
    ReplicationLagTooHigh { lag_bytes: i64 },

    #[error("LSN positions not in sync")]
    LsnNotInSync { source_lsn: String, target_lsn: String },

    #[error("No recent backup found within {required}")]
    BackupRequirementNotMet { required: String },
}

impl UpgradeError {
    /// Returns true if this error should trigger a retry
    pub fn is_retryable(&self) -> bool {
        matches!(self,
            UpgradeError::KubeError(_) |
            UpgradeError::ClusterNotReady(_) |
            UpgradeError::TargetCreationInProgress
        )
    }

    /// Returns true if this error blocks automatic cutover but doesn't prevent progress
    pub fn blocks_cutover(&self) -> bool {
        matches!(self,
            UpgradeError::RowCountMismatch { .. } |
            UpgradeError::ReplicationLagTooHigh { .. } |
            UpgradeError::LsnNotInSync { .. } |
            UpgradeError::BackupRequirementNotMet { .. }
        )
    }
}
```

**2. Logical Replication Setup** (`src/resources/replication.rs`)
```rust
/// Create a publication for logical replication on the source cluster.
/// Returns Ok(true) if created, Ok(false) if already exists.
pub async fn setup_publication(
    client: &Client,
    cluster: &PostgresCluster,
    publication_name: &str,
) -> Result<bool, ReplicationError> {
    // Check if publication already exists (idempotent)
    let check_sql = format!(
        "SELECT 1 FROM pg_publication WHERE pubname = '{}'",
        escape_sql_string(publication_name)
    );
    let result = exec_sql_on_cluster(client, cluster, "postgres", &check_sql).await?;
    if !result.trim().is_empty() {
        return Ok(false); // Already exists
    }

    // Create publication for all tables
    let create_sql = format!(
        "CREATE PUBLICATION {} FOR ALL TABLES",
        quote_identifier(publication_name)
    );
    exec_sql_on_cluster(client, cluster, "postgres", &create_sql).await?;
    Ok(true)
}

pub async fn setup_subscription(
    client: &Client,
    source_cluster: &PostgresCluster,
    target_cluster: &PostgresCluster,
    subscription_name: &str,
) -> Result<(), ReplicationError> {
    // For PG 17+: use pg_createsubscriber
    // For older: manual CREATE SUBSCRIPTION
}
```

**3. LSN Comparison** (Critical for data integrity)
```rust
/// Check if source and target LSNs match (replication fully caught up).
/// This is the definitive check before cutover - ensures no data loss.
pub async fn check_lsn_sync(
    client: &Client,
    source_cluster: &PostgresCluster,
    target_cluster: &PostgresCluster,
    subscription_name: &str,
) -> Result<LsnSyncStatus, ReplicationError> {
    // Get current LSN from source
    let source_lsn = exec_sql_on_cluster(
        client, source_cluster, "postgres",
        "SELECT pg_current_wal_lsn()::text"
    ).await?.trim().to_string();

    // Get received LSN from target subscription
    let target_lsn_sql = format!(
        "SELECT received_lsn::text FROM pg_stat_subscription WHERE subname = '{}'",
        escape_sql_string(subscription_name)
    );
    let target_lsn = exec_sql_on_cluster(
        client, target_cluster, "postgres", &target_lsn_sql
    ).await?.trim().to_string();

    let in_sync = source_lsn == target_lsn;

    Ok(LsnSyncStatus {
        source_lsn,
        target_lsn,
        in_sync,
        lag_bytes: if in_sync { Some(0) } else { None },
    })
}
```

**4. Row Count Verification with Parallel Execution**
```rust
use futures::future::try_join;

pub async fn verify_row_counts(
    client: &Client,
    source_cluster: &PostgresCluster,
    target_cluster: &PostgresCluster,
    tolerance: i64,
) -> Result<RowCountVerification, ReplicationError> {
    const ROW_COUNT_SQL: &str = r#"
        SELECT schemaname, relname, n_live_tup::bigint
        FROM pg_stat_user_tables
        ORDER BY schemaname, relname
    "#;

    // Execute queries in parallel for both clusters
    let (source_counts, target_counts) = try_join(
        get_table_counts(client, source_cluster, ROW_COUNT_SQL),
        get_table_counts(client, target_cluster, ROW_COUNT_SQL),
    ).await?;

    // Compare counts...
}
```

**5. Sequence Synchronization**
```rust
/// Synchronize sequences from source to target cluster.
/// MUST be called AFTER setting source to read-only.
pub async fn sync_sequences(
    client: &Client,
    source_cluster: &PostgresCluster,
    target_cluster: &PostgresCluster,
) -> Result<SequenceSyncResult, ReplicationError> {
    const GET_SEQUENCES_SQL: &str = r#"
        SELECT schemaname, sequencename, last_value
        FROM pg_sequences
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    "#;

    let output = exec_sql_on_cluster(client, source_cluster, "postgres", GET_SEQUENCES_SQL).await?;

    let mut synced_count = 0;
    let mut failed_sequences: Vec<String> = Vec::new();

    for line in output.lines() {
        // Parse schema, sequence name, last_value
        // Execute: SELECT setval('schema.seq_name', last_value, true) on target
    }

    Ok(SequenceSyncResult {
        synced_count,
        failed_count: failed_sequences.len() as i32,
        failed_sequences,
    })
}
```

**6. Atomic Service Switching** (`src/resources/service.rs`)
```rust
/// Switch services from source to target cluster atomically.
/// All service patches happen in a single reconciliation.
pub async fn switch_services_atomically(
    client: &Client,
    namespace: &str,
    source_name: &str,
    target_name: &str,
) -> Result<ServiceSwitchResult, UpgradeError> {
    let services: Api<Service> = Api::namespaced(client.clone(), namespace);

    let service_names = [
        format!("{}-primary", source_name),
        format!("{}-repl", source_name),
    ];

    for service_name in &service_names {
        let patch = serde_json::json!({
            "spec": {
                "selector": {
                    "postgres-operator.smoketurner.com/cluster": target_name
                }
            }
        });

        services.patch(
            service_name,
            &PatchParams::apply("postgres-operator").force(),
            &Patch::Merge(&patch),
        ).await?;
    }

    Ok(ServiceSwitchResult { services_switched: service_names.to_vec() })
}
```

### Webhooks for Upgrade Validation

**File: `src/webhooks/policies/upgrade.rs`**

```rust
/// Validate PostgresUpgrade creation/updates
pub fn validate_upgrade(ctx: &UpgradeValidationContext) -> ValidationResult {
    // 1. Validate version is upgrade (not downgrade)
    let source_version = ctx.source_cluster.spec.version.as_major_version();
    let target_version = ctx.upgrade.spec.target_version.as_major_version();

    if target_version <= source_version {
        return ValidationResult::denied(
            "InvalidVersion",
            format!("Target version {} must be higher than source version {}",
                    target_version, source_version)
        );
    }

    // 2. Validate source cluster exists and is Running
    if ctx.source_cluster.status.phase != ClusterPhase::Running {
        return ValidationResult::denied(
            "SourceNotReady",
            format!("Source cluster must be in Running phase")
        );
    }

    // 3. Block concurrent upgrades on same source
    if has_active_upgrade(&ctx.source_cluster) {
        return ValidationResult::denied(
            "ConcurrentUpgrade",
            "Another upgrade is already in progress for this source cluster"
        );
    }

    ValidationResult::allowed()
}

/// Block modifications to source clusters during active upgrades
pub fn validate_cluster_modification(ctx: &ClusterValidationContext) -> ValidationResult {
    if let Some(upgrade) = ctx.cluster.annotations().get("postgres-operator.smoketurner.com/upgrade-in-progress") {
        return ValidationResult::denied(
            "UpgradeInProgress",
            format!("Cluster is being upgraded by {}. Modifications are blocked.", upgrade)
        );
    }
    ValidationResult::allowed()
}

/// Enforce immutability of key fields after creation
pub fn validate_upgrade_immutability(new: &PostgresUpgrade, old: &PostgresUpgrade) -> ValidationResult {
    if new.spec.source_cluster != old.spec.source_cluster {
        return ValidationResult::denied("ImmutableField", "spec.sourceCluster cannot be changed");
    }
    if new.spec.target_version != old.spec.target_version {
        return ValidationResult::denied("ImmutableField", "spec.targetVersion cannot be changed");
    }
    ValidationResult::allowed()
}
```

### Version Constraints (enforced at CRD validation)

```rust
// In postgres_upgrade.rs - reuse existing PostgresVersion enum
use crate::crd::PostgresVersion;

pub struct PostgresUpgradeSpec {
    // Uses the same enum as PostgresCluster - guarantees Spilo-supported versions
    pub target_version: PostgresVersion,  // V15, V16, V17
    // ...
}

// Validation during reconciliation - use as_major_version() directly
fn validate_upgrade_direction(
    source_version: &PostgresVersion,
    target_version: &PostgresVersion,
) -> Result<(), UpgradeError> {
    let source_major = source_version.as_major_version();
    let target_major = target_version.as_major_version();

    if target_major <= source_major {
        return Err(UpgradeError::DowngradeNotAllowed {
            source: source_version.to_string(),
            target: target_version.to_string(),
        });
    }
    Ok(())
}
```

### Owner Reference Strategy

**Green cluster has NO owner reference:**
- Survives PostgresUpgrade deletion (critical for production safety)
- Labels link to upgrade: `postgres-operator.smoketurner.com/upgrade: {upgrade-name}`
- After cutover, source marked with: `postgres-operator.smoketurner.com/superseded-by: {green-name}`
- Cleanup is always manual (never automatic)

### RBAC Updates

**Add to `config/rbac/role.yaml`:**
```yaml
- apiGroups:
    - postgres-operator.smoketurner.com
  resources:
    - postgresupgrades
  verbs:
    - get
    - list
    - watch
    - create
    - update
    - patch
    - delete
- apiGroups:
    - postgres-operator.smoketurner.com
  resources:
    - postgresupgrades/status
  verbs:
    - get
    - update
    - patch
- apiGroups:
    - postgres-operator.smoketurner.com
  resources:
    - postgresupgrades/finalizers
  verbs:
    - update
```

### Limitations & Considerations

| Limitation | Mitigation |
|------------|------------|
| Sequences not replicated | Sync sequences during SyncingSequences phase (after source read-only) |
| DDL changes blocked | Document freeze period, webhook blocks source modifications |
| Large objects limited | Document limitation |
| Only Spilo versions (15, 16, 17) | Enforced at CRD level |
| Downgrades not allowed | Rejected during validation |
| Extra resources during migration | Document capacity needs |
| Rollback complexity | Keep source cluster indefinitely, automated rollback via annotation |
| Concurrent upgrades | Webhook blocks multiple upgrades on same source |

### Fleet-Wide Upgrade Pattern

For upgrading many databases (e.g., 50+ clusters), the automatic cutover mode enables:

```yaml
# Create PostgresUpgrade resources for all clusters
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresUpgrade
metadata:
  name: orders-db-upgrade
spec:
  sourceCluster:
    name: orders-db
  targetVersion: "17"
  strategy:
    cutover:
      mode: Automatic  # Cuts over when safe
      allowedWindow:   # Optional: restrict cutover times
        startTime: "02:00"
        endTime: "04:00"
        timezone: "UTC"
    preChecks:
      maxReplicationLagSeconds: 0
      verifyRowCounts: true
      rowCountTolerance: 0
      minVerificationPasses: 3
      requireBackupWithin: "1h"  # Required for Automatic
---
# Repeat for all clusters...
```

**Monitoring fleet upgrade progress:**
```bash
# Watch all upgrades
kubectl get postgresupgrade -A -w

# Find upgrades not yet completed
kubectl get postgresupgrade -A -o json | jq '.items[] | select(.status.phase != "Completed") | {name: .metadata.name, phase: .status.phase}'

# Find upgrades with row count mismatches (blocked)
kubectl get postgresupgrade -A -o json | jq '.items[] | select(.status.verification.tablesMismatched > 0)'

# Find upgrades with rollback risk
kubectl get postgresupgrade -A -o json | jq '.items[] | select(.status.rollback.dataLossRisk == true)'
```

**Fleet metrics (exposed by operator):**
- `postgres_upgrades_total{phase}` - count by phase
- `postgres_upgrades_replication_lag_seconds{upgrade}` - lag per upgrade
- `postgres_upgrades_row_count_mismatches{upgrade}` - verification failures
- `postgres_upgrade_phase_duration_seconds{phase}` - time in each phase

---

## Part 3: Implementation Plan

### Phase 1: Foundation
1. Create `PostgresUpgrade` CRD with full spec (including print columns)
2. Create `UpgradeError` enum with classification methods
3. Create upgrade state machine with all phases
4. Implement source/target cluster validation
5. Add `wal_level=logical` to Patroni config by default

### Phase 2: Target Cluster Creation
1. Implement target cluster creation from source spec
2. **No owner reference** - labels only for linking
3. Wait for target Running state with timeout
4. Add upgrade status tracking

### Phase 3: Replication Setup
1. Implement idempotent publication creation on source
2. Implement subscription creation on target
3. Handle PG 17+ vs older versions
4. Add subscription state monitoring
5. Add replication lag monitoring

### Phase 4: Verification
1. Implement row count verification with parallel queries
2. Implement LSN comparison
3. Implement multiple verification passes
4. Add verification timeout

### Phase 5: Sequence Sync and Cutover
1. Implement source read-only toggle
2. Implement sequence synchronization
3. Implement connection draining
4. Implement atomic service switching
5. Implement subscription cleanup

### Phase 6: Health Check and Completion
1. Implement post-cutover health check
2. Add health check duration/interval
3. Mark upgrade as completed
4. Record audit timestamps

### Phase 7: Rollback
1. Implement rollback annotation detection
2. Implement automated rollback sequence
3. Track rollback feasibility
4. Track data loss risk

### Phase 8: Webhooks
1. Implement upgrade validation webhook
2. Implement immutability enforcement
3. Block concurrent upgrades
4. Block source modifications during upgrade

### Phase 9: Documentation
1. Upgrade documentation
2. Runbooks for each failure mode
3. Fleet upgrade guide
4. Sample YAMLs

---

## Part 4: Verification Plan

### Unit Tests
```rust
#[test] fn test_validate_upgrade_direction()     // V16→V17 ok, V17→V16 rejected
#[test] fn test_upgrade_phase_transitions()      // FSM valid/invalid transitions
#[test] fn test_row_count_comparison()           // Tolerance handling
#[test] fn test_error_classification()           // is_retryable(), blocks_cutover()
#[test] fn test_lsn_sync_detection()             // LSN comparison logic
```

### Integration Tests
```rust
#[tokio::test]
async fn test_blue_green_upgrade_v16_to_v17() {
    // 1. Create source cluster v16
    // 2. Insert test data
    // 3. Create PostgresUpgrade
    // 4. Wait for ReadyForCutover
    // 5. Verify row counts and LSN sync
    // 6. Trigger cutover
    // 7. Verify data in target
    // 8. Verify services point to target
}

#[tokio::test]
async fn test_upgrade_rollback() {
    // 1. Create upgrade to Completed state
    // 2. Annotate with rollback=now
    // 3. Verify services switch back to source
    // 4. Verify phase is RolledBack
}

#[tokio::test]
async fn test_concurrent_upgrade_blocked() {
    // 1. Create first upgrade
    // 2. Attempt to create second upgrade on same source
    // 3. Verify webhook rejects second upgrade
}
```

### Manual Verification
1. Deploy source PostgresCluster v16 with sample data
2. Create PostgresUpgrade targeting v17
3. Monitor replication via `kubectl get postgresupgrade -w`
4. Verify replication lag decreases to 0
5. Verify LSN sync
6. Trigger cutover (Manual) or wait (Automatic)
7. Verify application connectivity to target
8. Test rollback procedure

---

## Part 5: Files to Modify/Create

### New Files
| File | Purpose |
|------|---------|
| `src/crd/postgres_upgrade.rs` | PostgresUpgrade CRD definition |
| `src/controller/upgrade_reconciler.rs` | Upgrade reconciliation logic |
| `src/controller/upgrade_state_machine.rs` | Upgrade phase transitions |
| `src/controller/upgrade_error.rs` | Classified error types |
| `src/resources/replication.rs` | Logical replication management |
| `src/webhooks/policies/upgrade.rs` | Upgrade validation webhooks |
| `tests/integration/upgrade_tests.rs` | Integration tests |
| `config/crd/postgres-upgrade.yaml` | CRD manifest |
| `config/samples/upgrade-v16-to-v17.yaml` | Sample upgrade |
| `docs/upgrades.md` | Upgrade documentation |
| `docs/runbooks/upgrade-stuck.md` | Runbook for stuck upgrades |
| `docs/runbooks/upgrade-verification-failed.md` | Runbook for verification failures |
| `docs/runbooks/upgrade-rollback.md` | Runbook for rollback procedure |

### Modified Files
| File | Changes |
|------|---------|
| `src/main.rs` | Register upgrade controller |
| `src/lib.rs` | Export run_upgrade_controller |
| `src/crd/mod.rs` | Export PostgresUpgrade |
| `src/resources/patroni.rs` | Default `wal_level=logical` |
| `src/resources/service.rs` | Support target override and atomic switching |
| `src/webhooks/server.rs` | Register upgrade webhooks |
| `Cargo.toml` | No new dependencies expected |
| `config/rbac/role.yaml` | Add PostgresUpgrade permissions |

---

## Part 6: Future Enhancements (Post-GA)

These are out of scope for initial implementation:

1. **PostgresUpgradePolicy CRD** - Fleet-wide coordinated upgrades with waves
2. **kubectl plugin** - `kubectl postgres upgrade-preflight --version 17`
3. **PrometheusRule templates** - Ready-to-use alerting configurations
4. **Audit trail** - Detailed status.audit with actor/action/timestamp
5. **Cost attribution** - Track resource usage during upgrade

---

## Sources

- [PostgreSQL 17-18 Blue-Green Migration](https://www.dbi-services.com/blog/postgresql-17-18-major-upgrade-blue-green-migration-with-minimal-downtime/)
- [Xata: Postgres Major Version Upgrades](https://xata.io/blog/postgres-major-version-upgrades)
- [Patroni: Major Upgrade Documentation](https://patroni.readthedocs.io/en/latest/existing_data.html)
- [AWS Wiz Blue-Green Deployments](https://aws.amazon.com/blogs/database/how-wiz-achieved-near-zero-downtime-for-amazon-aurora-postgresql-major-version-upgrades-at-scale-using-aurora-blue-green-deployments/)
