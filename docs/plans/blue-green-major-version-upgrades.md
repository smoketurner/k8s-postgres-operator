# PostgreSQL Operator Review & Blue-Green Major Version Upgrade Plan

## Executive Summary

The platform-engineer review found the operator **~70% production-ready** with a solid Rust/Kubernetes foundation. The most impactful gap for Day-2 operations is **major version upgrades**, which currently rely on Patroni's disruptive approach (cluster downtime during pg_upgrade).

This plan proposes adding a **blue-green upgrade mechanism using logical replication** for near-zero downtime major version upgrades.

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
| **Err on side of safety** | Zero lag required, row counts verified, backups encouraged |
| **No data loss** | Cutover blocked until source and target are identical |
| **Reversible operations** | Source cluster never auto-deleted, rollback always possible |
| **Clear status** | Every phase reports detailed conditions and metrics |
| **Manual override** | Automatic mode can be switched to Manual at any time |
| **Fail-safe defaults** | All safety checks enabled by default |

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

    # Safety checks before automatic cutover (safe defaults - err on side of caution)
    preChecks:
      # Replication lag threshold - must be zero for safe cutover
      maxReplicationLagSeconds: 0  # Default: 0 (strictest - no cutover until fully synced)

      # Row count verification - enabled by default for data integrity
      verifyRowCounts: true       # Default: true (always verify)
      rowCountTolerance: 0        # Default: 0 (exact match required)

      # Optional: require recent backup before cutover
      requireBackupWithin: "1h"   # Default: 1h (optional but recommended)

    # Post-cutover behavior
    postCutover:
      # Never auto-delete - user must manually clean up
      # This allows indefinite rollback window
      keepSourceCluster: true

status:
  phase: Pending | CreatingTarget | ConfiguringReplication | Replicating | Verifying | ReadyForCutover | CuttingOver | Completed | Failed | RolledBack

  sourceCluster:
    name: orders-db
    version: "16"

  targetCluster:
    name: orders-db-v17-green
    version: "17"
    ready: true

  replication:
    status: Active | Synced | Stopped
    lagBytes: 0
    lagSeconds: 0
    lastSyncTime: "2025-01-11T10:00:00Z"

  # Row count verification for data integrity
  verification:
    lastCheckTime: "2025-01-11T10:00:00Z"
    tablesVerified: 42
    tablesMatched: 42
    tablesMismatched: 0
    mismatchedTables: []  # List of tables with row count differences

  conditions:
    - type: SourceReady
    - type: TargetReady
    - type: ReplicationHealthy
    - type: RowCountsVerified      # New: data integrity check
    - type: ReadyForCutover
    - type: CutoverComplete
```

### Upgrade Phases

#### Phase 1: Pending
- Validate source cluster exists and is Running
- Validate target version > source version (downgrades rejected at CRD validation)
- Validate both versions are Spilo-supported (15, 16, 17)
- Check source has required `wal_level=logical` (enabled by default)

#### Phase 2: CreatingTarget
- Create new PostgresCluster with target version
- Name: `{source-name}-v{version}-green`
- Copy spec from source with version override
- Wait for target cluster to reach Running state

#### Phase 3: ConfiguringReplication
- Create publication on source: `CREATE PUBLICATION upgrade_pub FOR ALL TABLES`
- Create subscription on target: `CREATE SUBSCRIPTION upgrade_sub ...`
- For PostgreSQL 17+: Use `pg_createsubscriber` for simpler setup

#### Phase 4: Replicating
- Monitor replication lag via `pg_stat_subscription`
- Update status with lag metrics
- Emit events for significant lag changes
- Periodically verify row counts across all tables
- Wait until lag < threshold for cutover readiness

#### Phase 5: Verifying (New - for automatic cutover)
- Compare row counts: source vs target for all tables
- Query: `SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables`
- Must match within tolerance (default: exact match)
- Update `status.verification` with results
- Block cutover if mismatches detected

#### Phase 6: ReadyForCutover
- All pre-checks passed:
  - Replication lag < threshold
  - Row counts verified (if enabled)
  - Recent backup exists (if required)
- For Manual mode: Wait for `cutover: now` annotation
- For Automatic mode: Proceed immediately

#### Phase 7: CuttingOver
- Set source to read-only: `ALTER SYSTEM SET default_transaction_read_only = on`
- Wait for final replication sync (LSN match)
- Update Services to point to target cluster pods
- Drop subscription on target
- Promote target if needed

#### Phase 8: Completed
- Update source PostgresCluster annotation to mark as superseded
- Optionally rename target cluster to original name
- Source cluster remains for manual cleanup (rollback safety)
- Emit `UpgradeCompleted` event with summary

### Implementation Components

#### New Files
```
src/crd/postgres_upgrade.rs           # PostgresUpgrade CRD
src/controller/upgrade_reconciler.rs  # Upgrade controller
src/resources/replication.rs          # Logical replication setup
```

#### Modified Files
```
src/main.rs                           # Register upgrade controller
src/crd/mod.rs                        # Export new CRD
src/resources/service.rs              # Support service target switching
src/controller/reconciler.rs          # Respect "superseded" annotation
```

#### Key Implementation Details

**1. Logical Replication Setup** (`src/resources/replication.rs`)
```rust
pub async fn setup_publication(
    client: &Client,
    cluster: &PostgresCluster,
    publication_name: &str,
) -> Result<()> {
    // Exec into primary pod
    // CREATE PUBLICATION {name} FOR ALL TABLES
}

pub async fn setup_subscription(
    client: &Client,
    source_cluster: &PostgresCluster,
    target_cluster: &PostgresCluster,
    subscription_name: &str,
) -> Result<()> {
    // For PG 17+: use pg_createsubscriber
    // For older: manual CREATE SUBSCRIPTION
}
```

**2. Replication Monitoring**
```rust
pub async fn get_replication_lag(
    client: &Client,
    cluster: &PostgresCluster,
) -> Result<ReplicationLag> {
    // Query pg_stat_subscription
    // Return lag in bytes and estimated seconds
}

pub async fn verify_row_counts(
    client: &Client,
    source_cluster: &PostgresCluster,
    target_cluster: &PostgresCluster,
    tolerance: i64,
) -> Result<RowCountVerification> {
    // 1. Query source: SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables
    // 2. Query target: same query
    // 3. Compare counts per table
    // 4. Return verification result with any mismatches
}

pub struct RowCountVerification {
    pub tables_verified: i32,
    pub tables_matched: i32,
    pub tables_mismatched: i32,
    pub mismatched_tables: Vec<TableMismatch>,
}

pub struct TableMismatch {
    pub schema: String,
    pub table: String,
    pub source_count: i64,
    pub target_count: i64,
    pub difference: i64,
}
```

**3. Service Switching** (`src/resources/service.rs`)
```rust
pub fn generate_service_for_cluster(
    cluster: &PostgresCluster,
    target_override: Option<&str>, // Override pod selector
) -> Service {
    // Allow services to point to different cluster's pods
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

// Validation during reconciliation
fn validate_upgrade_direction(
    source_version: &PostgresVersion,
    target_version: &PostgresVersion,
) -> Result<()> {
    let source_major: u8 = source_version.as_str().parse()?;
    let target_major: u8 = target_version.as_str().parse()?;

    if target_major <= source_major {
        return Err(Error::ValidationError(
            "targetVersion must be higher than source cluster version (no downgrades)"
        ));
    }
    Ok(())
}
```

### Limitations & Considerations

| Limitation | Mitigation |
|------------|------------|
| Sequences not replicated | Sync sequences during cutover phase |
| DDL changes blocked | Document freeze period, add validation |
| Large objects limited | Document limitation |
| Only Spilo versions (15, 16, 17) | Enforced at CRD level |
| Downgrades not allowed | Rejected during validation |
| Extra resources during migration | Document capacity needs |
| Rollback complexity | Keep source cluster indefinitely |

### Rollback Procedure

If issues discovered after cutover:
1. Stop writes to target cluster
2. Re-enable writes on source cluster
3. Update Services back to source pods
4. Mark upgrade as RolledBack
5. Investigate and retry

### Sequence Sync Implementation

```rust
pub async fn sync_sequences(
    source_cluster: &PostgresCluster,
    target_cluster: &PostgresCluster,
) -> Result<()> {
    // 1. Query all sequences from source: pg_sequences
    // 2. For each sequence, get last_value
    // 3. On target: SELECT setval('seq_name', value)
}
```

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
    preChecks:
      maxReplicationLagSeconds: 0
      verifyRowCounts: true
      rowCountTolerance: 0
---
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresUpgrade
metadata:
  name: users-db-upgrade
spec:
  sourceCluster:
    name: users-db
  targetVersion: "17"
  strategy:
    cutover:
      mode: Automatic
    preChecks:
      maxReplicationLagSeconds: 0
      verifyRowCounts: true
      rowCountTolerance: 0
# ... repeat for all clusters
```

**Monitoring fleet upgrade progress:**
```bash
# Watch all upgrades
kubectl get postgresupgrade -A -w

# Find upgrades not yet completed
kubectl get postgresupgrade -A -o json | jq '.items[] | select(.status.phase != "Completed") | {name: .metadata.name, phase: .status.phase}'

# Find upgrades with row count mismatches (blocked)
kubectl get postgresupgrade -A -o json | jq '.items[] | select(.status.verification.tablesMismatched > 0)'
```

**Fleet metrics (exposed by operator):**
- `postgres_upgrades_total{phase}` - count by phase
- `postgres_upgrades_replication_lag_seconds{upgrade}` - lag per upgrade
- `postgres_upgrades_row_count_mismatches{upgrade}` - verification failures

---

## Part 3: Implementation Plan

### Phase 1: Foundation
1. Create `PostgresUpgrade` CRD with full spec
2. Create upgrade controller skeleton
3. Implement source/target cluster validation
4. Add `wal_level=logical` to Patroni config by default

### Phase 2: Target Cluster Creation
1. Implement target cluster creation from source spec
2. Add "managed-by-upgrade" label
3. Wait for target Running state
4. Add upgrade status tracking

### Phase 3: Replication Setup
1. Implement publication creation on source
2. Implement subscription creation on target
3. Handle PG 17+ vs older versions
4. Add replication monitoring

### Phase 4: Cutover
1. Implement pre-cutover checks
2. Implement read-only toggle on source
3. Implement service switching
4. Implement sequence sync
5. Implement subscription cleanup

### Phase 5: Polish
1. Add rollback capability
2. Add scheduled cutover
3. Add automatic cutover mode
4. Documentation and examples

---

## Part 4: Verification Plan

### Unit Tests
- CRD validation for PostgresUpgrade
- State machine transitions
- Replication lag calculations

### Integration Tests
```rust
#[tokio::test]
async fn test_blue_green_upgrade_v16_to_v17() {
    // 1. Create source cluster v16
    // 2. Insert test data
    // 3. Create PostgresUpgrade
    // 4. Wait for ReadyForCutover
    // 5. Trigger cutover
    // 6. Verify data in target
    // 7. Verify services point to target
}
```

### Manual Verification
1. Deploy source PostgresCluster v16 with sample data
2. Create PostgresUpgrade targeting v17
3. Monitor replication via `kubectl get postgresupgrade -w`
4. Trigger cutover
5. Verify application connectivity
6. Test rollback procedure

---

## Part 5: Files to Modify/Create

### New Files
| File | Purpose |
|------|---------|
| `src/crd/postgres_upgrade.rs` | PostgresUpgrade CRD definition |
| `src/controller/upgrade_reconciler.rs` | Upgrade reconciliation logic |
| `src/controller/upgrade_state_machine.rs` | Upgrade phase transitions |
| `src/resources/replication.rs` | Logical replication management |
| `tests/integration/upgrade_tests.rs` | Integration tests |
| `config/crd/postgres-upgrade.yaml` | CRD manifest |
| `config/samples/upgrade-v16-to-v17.yaml` | Sample upgrade |
| `docs/upgrades.md` | Upgrade documentation |

### Modified Files
| File | Changes |
|------|---------|
| `src/main.rs` | Register upgrade controller |
| `src/crd/mod.rs` | Export PostgresUpgrade |
| `src/resources/patroni.rs` | Default `wal_level=logical` |
| `src/resources/service.rs` | Support target override |
| `Cargo.toml` | No new dependencies expected |
| `config/rbac/role.yaml` | Add PostgresUpgrade permissions |

---

## Sources

- [PostgreSQL 17-18 Blue-Green Migration](https://www.dbi-services.com/blog/postgresql-17-18-major-upgrade-blue-green-migration-with-minimal-downtime/)
- [Xata: Postgres Major Version Upgrades](https://xata.io/blog/postgres-major-version-upgrades)
- [Patroni: Major Upgrade Documentation](https://patroni.readthedocs.io/en/latest/existing_data.html)
- [AWS Wiz Blue-Green Deployments](https://aws.amazon.com/blogs/database/how-wiz-achieved-near-zero-downtime-for-amazon-aurora-postgresql-major-version-upgrades-at-scale-using-aurora-blue-green-deployments/)
