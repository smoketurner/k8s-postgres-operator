# Major Version Upgrade Guide

This guide covers how to perform near-zero downtime major version upgrades of PostgreSQL clusters using the operator's blue-green upgrade mechanism.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Creating an Upgrade](#creating-an-upgrade)
4. [Monitoring Progress](#monitoring-progress)
5. [Manual Cutover](#manual-cutover)
6. [Automatic Cutover](#automatic-cutover)
7. [Rollback](#rollback)
8. [Fleet Upgrades](#fleet-upgrades)
9. [Troubleshooting](#troubleshooting)

## Overview

The operator provides near-zero downtime major version upgrades using **logical replication** between a source (blue) cluster and a target (green) cluster running the new PostgreSQL version.

### How It Works

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

### Upgrade Phases

| Phase | Description |
|-------|-------------|
| `Pending` | Validating source cluster and upgrade configuration |
| `CreatingTarget` | Creating the new PostgreSQL cluster with target version |
| `ConfiguringReplication` | Setting up logical replication from source to target |
| `Replicating` | Data is being replicated; monitoring replication lag |
| `Verifying` | Verifying row counts match between source and target |
| `SyncingSequences` | Synchronizing sequences; source is already read-only (set during `Verifying`) |
| `ReadyForCutover` | All checks passed; ready for traffic cutover |
| `WaitingForManualCutover` | Manual mode only: waiting for cutover annotation |
| `CuttingOver` | Switching services to point to the new cluster |
| `HealthChecking` | Verifying target cluster is healthy post-cutover |
| `Completed` | Upgrade completed successfully |
| `Failed` | Upgrade failed (see conditions for details) |
| `RolledBack` | Upgrade was rolled back to source cluster |

## Prerequisites

Before starting an upgrade:

1. **Source cluster must be running**: The source PostgresCluster must be in `Running` phase
2. **Sufficient resources**: The upgrade creates a complete replica of your cluster
3. **WAL level**: `wal_level=logical` is required (enabled by default in this operator)
4. **Backup recommended**: For automatic cutover mode, a recent backup is required
5. **Replication-compatibility preflight** (automatic): The operator runs a set of
   preflight checks against the source before it touches any resources. See
   [Replication-compatibility preflight](#replication-compatibility-preflight) below
   for the full list. If any check fails, the upgrade transitions to `Failed`
   with the failing condition surfaced on the resource — fix the source and
   create a new `PostgresUpgrade` to retry.

### Replication-compatibility preflight

Logical replication has several well-known sharp edges. The operator refuses to
start an upgrade if any of the following are true on the source cluster:

| Check | What it looks for | How to fix |
|-------|-------------------|------------|
| **Replica identity** | User tables without a primary key and without an explicit non-default replica identity (`relreplident IN ('n', 'd')` plus no PK). Such tables have UPDATE/DELETE silently dropped during logical replication. | Add a `PRIMARY KEY`, or `ALTER TABLE ... REPLICA IDENTITY FULL` (slower, but works). |
| **Large objects** | Any rows in `pg_largeobject_metadata`. Logical replication does not replicate large objects. | Migrate large object data separately, or remove it before upgrading. |
| **Unlogged tables** | User-schema tables with `relpersistence = 'u'`. These won't be replicated, and the user usually expects them to migrate. | `ALTER TABLE ... SET LOGGED`, or accept the table will be empty on the new cluster. |
| **Blocking extensions** | `pg_cron` or `pg_partman` active. These interfere with logical replication (per Wiz's documented Aurora playbook). | `DROP EXTENSION pg_cron CASCADE;` (and similarly for `pg_partman`) on the source before the upgrade, then recreate on the target after cutover. |
| **Materialized view refresh in progress** | `pg_stat_activity` shows an active `REFRESH MATERIALIZED VIEW`. Concurrent refresh can break the publication mid-stream. | Wait for the refresh to finish, or pause the refresh schedule before retrying. |
| **Target storage capacity** | Source's total `pg_database_size` × 1.5 must fit in the target storage spec. Without that headroom, WAL accumulation during initial sync fills the target PVC. | Expand the source cluster's `spec.storage.size` (the target inherits it), or wait for a future release that supports `targetClusterOverrides.storage`. |

When preflight fails, the upgrade resource will have a `PreflightPassed=False`
condition with the specific failures inline and a `PreflightFailed` event:

```bash
kubectl describe postgresupgrade my-cluster-upgrade
```

```
Conditions:
  Type:                 PreflightPassed
  Status:               False
  Reason:               PreflightFailed
  Message:              2 preflight checks failed: 3 table(s) lack a primary key
                        and a non-default replica identity; UPDATE/DELETE will not
                        replicate. Add PRIMARY KEY or ALTER TABLE ... REPLICA
                        IDENTITY FULL. Examples: public.orders, public.items,
                        shop.products; Extension(s) known to interfere with
                        logical replication are active: pg_cron. Disable them on
                        the source before upgrading (DROP EXTENSION) and recreate
                        on the target after cutover.

Events:
  Type     Reason             Age   From               Message
  ----     ------             ----  ----               -------
  Warning  PreflightFailed    1m    upgrade-controller 2 preflight checks failed: ...
```

After the user resolves the failures, they must delete the failed upgrade
and create a fresh `PostgresUpgrade` to retry.

### DDL audit

Logical replication does not replicate DDL. `CREATE TABLE`, `ALTER TABLE`,
`CREATE INDEX`, `DROP COLUMN`, etc. that happen on the source during the
replication window will silently fail to land on the target — the row count
verification still passes, but the schemas have diverged. Wiz's published
playbook flags this as one of the most common causes of broken cutovers.

When the operator enters `ConfiguringReplication`, it installs a small
server-side audit on the source cluster (an event trigger on `ddl_command_end`)
that logs every DDL command into an audit table. The reconciler polls the
row count between phases and patches it onto
`status.replication.ddlCount`. While this is non-zero, the cutover guards in
the FSM refuse to transition to `CuttingOver`.

**Objects installed on the source** (all under the operator's connecting role,
typically Spilo's `postgres` superuser):

| Object | SQL identifier |
|--------|----------------|
| Audit table | `public.postgres_operator_ddl_audit` |
| Audit function | `public.postgres_operator_log_ddl` |
| Event trigger | `postgres_operator_ddl_audit` |

The operator uninstalls all three on terminal phases (`Completed`, `Failed`,
`RolledBack`) and on deletion of the `PostgresUpgrade`. If the resource is
force-deleted (`kubectl delete --force --grace-period=0`) and the upgrade
finalizer is bypassed, you can clean up manually:

```sql
DROP EVENT TRIGGER IF EXISTS postgres_operator_ddl_audit;
DROP FUNCTION IF EXISTS public.postgres_operator_log_ddl();
DROP TABLE IF EXISTS public.postgres_operator_ddl_audit;
```

#### `spec.strategy.acknowledgeDDL` (escape hatch)

There are legitimate reasons to make schema changes on both source and target
mid-upgrade — e.g. an emergency index addition, an `ALTER TABLE` that's been
manually applied to both sides. To proceed with cutover after that:

1. Apply the matching DDL to the target cluster yourself (the operator does
   not infer this for you — getting it wrong silently is worse than
   stopping).
2. Set `spec.strategy.acknowledgeDDL: true` on the `PostgresUpgrade`.

The cutover guard then allows the transition. The Warning Event and
`DDLObserved=True` condition remain on the resource as an audit record.

The operator never auto-acknowledges. The default is always `false`.

### Cutover-readiness gate

Even when row counts match, logical-replication lag can still be non-zero —
new writes may be arriving on the source faster than the target catches up.
And even if lag momentarily hits zero, the next write on the source advances
it again. Cutting over at a moment-in-time zero is a race against ongoing
writes.

To make the gate type-safe and durable, the operator:

1. Refreshes replication lag at the start of every `Verifying` tick using
   the slot's `pg_current_wal_lsn() - confirmed_flush_lsn` (Wiz's exact
   query, expressed via the new `Lsn` newtype on
   `status.replication.{sourceLsn,targetLsn}`).
2. Once row counts have converged for the configured number of consecutive
   passes **and** the LSN distance is zero, takes the source primary
   read-only via `ALTER SYSTEM SET default_transaction_read_only = on` and
   records `status.sourceReadOnlyAt`.
3. On the **next** tick, refreshes lag again. With the source no longer
   accepting writes, the target's `confirmed_flush_lsn` catches up to the
   final `pg_current_wal_lsn()` and the distance is durably zero.
4. The FSM transition from `Verifying → SyncingSequences` is guarded on
   all four: passes ≥ required, no mismatches, lag = 0, source read-only.
   Sequence sync runs only after all four hold.

```bash
kubectl get postgresupgrade my-cluster-upgrade -o jsonpath='{.status}' | jq '
  {phase, sourceReadOnlyAt,
   verification: {consecutivePasses: .verification.consecutivePasses,
                  tablesMismatched: .verification.tablesMismatched},
   replication: {lagBytes: .replication.lagBytes,
                 sourceLsn: .replication.sourceLsn,
                 targetLsn: .replication.targetLsn}}'
```

On rollback, the source returns to read-write and `sourceReadOnlyAt` is
cleared. On successful completion, the timestamp persists on the status as
an audit record.

### Idle-in-transaction purger

The single biggest cause of opaque `ConfiguringReplication` stalls on busy
clusters is `CREATE_REPLICATION_SLOT` hanging while it waits for a consistent
snapshot — any long-running `idle in transaction` session blocks the snapshot
until it commits or rolls back. Wiz's published Aurora playbook calls this out
explicitly and ships an idle-transaction purger as their workaround.

Before the operator triggers slot creation (via `CREATE SUBSCRIPTION` on the
target), it queries `pg_stat_activity` on the source for sessions in
`idle in transaction` state for at least `spec.strategy.preChecks.idleTransactionThreshold`
(default 5 minutes), excluding its own backend.

| Setting | Default | Behavior |
|---------|---------|----------|
| `idleTransactionThreshold` | `"5m"` | Only sessions older than this are considered. Shorter is more aggressive. |
| `terminateIdleTransactions` | `true` | If `true`, the operator calls `pg_terminate_backend()` on each session and emits a `IdleTransactionsPurged` Normal Event. If `false`, the operator emits a `IdleTransactionsNotPurged` Warning Event listing the offenders and leaves them in place — the user is responsible for cleanup. |

If a *new* idle session appears in the window between the purge and slot
creation and trips the publisher on a `consistent snapshot` error, the operator
re-runs the purge and retries `CREATE SUBSCRIPTION` up to 3 times with bounded
exponential backoff (2s, 4s, 8s) before failing the upgrade.

The operator's database role on the source must have either the
`pg_signal_backend` predefined role or superuser status for
`pg_terminate_backend()` to work. The default Spilo `postgres` role provided
by this operator already has this.

### Supported Versions

The operator supports upgrades between PostgreSQL versions 15, 16, and 17 (Spilo-supported versions).

| Source Version | Target Version | Supported |
|---------------|----------------|-----------|
| 15 | 16 | Yes |
| 15 | 17 | Yes |
| 16 | 17 | Yes |
| 17 | 16 | No (downgrades not allowed) |

## Creating an Upgrade

Create a `PostgresUpgrade` resource to initiate an upgrade:

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresUpgrade
metadata:
  name: my-cluster-upgrade
  namespace: my-namespace
spec:
  # Reference to the source cluster
  sourceCluster:
    name: my-cluster
    # namespace: my-namespace  # Optional, defaults to same namespace

  # Target PostgreSQL version
  targetVersion: "17"

  # Optional: Override target cluster settings
  # targetClusterOverrides:
  #   replicas: 3
  #   resources:
  #     requests:
  #       cpu: "2"
  #       memory: "8Gi"

  strategy:
    strategyType: BlueGreen

    cutover:
      mode: Manual  # or Automatic

    preChecks:
      maxReplicationLagSeconds: 0          # Must be fully synced
      minVerificationPasses: 3             # Row counts must match 3 times
      verificationInterval: "1m"           # Time between checks
      requireBackupWithin: "1h"            # Required for Automatic mode
      drainConnectionsTimeout: "5m"        # Wait for connections to close
      idleTransactionThreshold: "5m"       # Idle-in-tx sessions older than this
                                           # are terminated before slot creation
      terminateIdleTransactions: true      # Opt out only if you can't tolerate it

    timeouts:
      targetClusterReady: "30m"
      initialSync: "24h"
      replicationCatchup: "1h"
      verification: "30m"
      cutover: "15m"

    postCutover:
      healthCheckDuration: "5m"
      cleanupSourceCluster: false  # Never auto-delete source
```

Apply the upgrade:

```bash
kubectl apply -f my-cluster-upgrade.yaml
```

## Monitoring Progress

### Watch Upgrade Status

```bash
# Watch upgrade progress
kubectl get postgresupgrade my-cluster-upgrade -w

# Get detailed status
kubectl describe postgresupgrade my-cluster-upgrade

# Check conditions
kubectl get postgresupgrade my-cluster-upgrade -o jsonpath='{.status.conditions}'
```

### Key Status Fields

```yaml
status:
  phase: Replicating
  replication:
    status: Active
    lagBytes: 1024
    lagSeconds: 0
    lsnInSync: false
  verification:
    tablesVerified: 42
    tablesMatched: 42
    consecutivePasses: 2
  sequences:
    synced: false
    syncedCount: 0
```

### Print Columns

The CRD includes helpful print columns for quick status checks:

```bash
kubectl get postgresupgrade -A
NAME                SOURCE       TARGET  PHASE         LAG   AGE
my-cluster-upgrade  my-cluster   17      Replicating   0     15m
```

## Manual Cutover

For `mode: Manual`, you must explicitly trigger the cutover when ready:

### 1. Verify Readiness

```bash
# Check that upgrade is ready
kubectl get postgresupgrade my-cluster-upgrade -o jsonpath='{.status.phase}'
# Should show: WaitingForManualCutover

# Verify all conditions are met
kubectl get postgresupgrade my-cluster-upgrade \
  -o jsonpath='{.status.conditions[?(@.type=="ReadyForCutover")].status}'
# Should show: True
```

### 2. Trigger Cutover

```bash
kubectl annotate postgresupgrade my-cluster-upgrade \
  postgres-operator.smoketurner.com/cutover=now
```

### 3. Monitor Cutover

```bash
kubectl get postgresupgrade my-cluster-upgrade -w
```

The upgrade will transition through `CuttingOver` -> `HealthChecking` -> `Completed`.

## Automatic Cutover

For `mode: Automatic`, the operator will cut over automatically when:

1. Replication lag is zero
2. LSN positions match (fully synced)
3. Row counts have been verified (consecutive passes)
4. Sequences are synced
5. A recent backup exists (within `requireBackupWithin`)
6. Maintenance window allows (if configured)

### Maintenance Window

Optionally restrict when automatic cutover can occur:

```yaml
spec:
  strategy:
    cutover:
      mode: Automatic
      allowedWindow:
        startTime: "02:00"
        endTime: "04:00"
        timezone: "UTC"
```

## Rollback

**Rollback is supported only before the `CuttingOver` phase begins.** Once
service selectors flip to the target, the new primary may have accepted
writes that the source does not have. Automatic rollback at that point would
silently drop data. This operator deliberately refuses post-cutover rollback
to make that guarantee enforceable.

This matches the published stance of other production blue/green tooling
(AWS RDS Blue/Green Deployments, Wiz's Aurora playbook, `pg_easy_replicate`).

### Trigger Rollback (pre-cutover only)

```bash
kubectl annotate postgresupgrade my-cluster-upgrade \
  postgres-operator.smoketurner.com/rollback=now
```

Valid source phases: `CreatingTarget`, `ConfiguringReplication`,
`Replicating`, `Verifying`, `SyncingSequences`, `ReadyForCutover`, and
`Failed` (when the failure happened pre-cutover).

The annotation is rejected with `RollbackNotAllowedInPhase` if the upgrade
is in `CuttingOver`, `HealthChecking`, or `Completed`. The error message
points users to the post-cutover recovery procedure below.

### Rollback Behavior (pre-cutover)

1. The operator stops replication
2. Sets source cluster back to read-write (if it was made read-only)
3. Drops the publication on source and subscription on target
4. Marks the upgrade as `RolledBack`
5. The target cluster is left in place for manual inspection; delete it
   with `kubectl delete postgrescluster <target-name>` once you're done

### Post-cutover recovery (manual)

If a problem with the target is discovered after cutover, recovery is a
manual operation, not an automated one:

1. Stop application traffic to the cluster (scale your apps to zero, or
   point them at a read-only/maintenance page).
2. Identify a WAL-G backup taken **before** the cutover started. The
   `status.startedAt` timestamp on the `PostgresUpgrade` resource is the
   correct boundary.
3. Restore the source cluster from that backup using PITR. See
   `docs/backup-restore.md` for the restore procedure.
4. Cut traffic back to the restored cluster.
5. Decide whether to retry the upgrade with the issue addressed, or
   defer.

This is intentionally a deliberate operation. The blue/green design gives
you a long replication window to find problems before cutover; use it.

## Fleet Upgrades

For upgrading many clusters, use automatic mode with monitoring:

### 1. Create Upgrades for All Clusters

```bash
for cluster in cluster-a cluster-b cluster-c; do
  cat <<EOF | kubectl apply -f -
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresUpgrade
metadata:
  name: ${cluster}-upgrade
spec:
  sourceCluster:
    name: ${cluster}
  targetVersion: "17"
  strategy:
    cutover:
      mode: Automatic
      allowedWindow:
        startTime: "02:00"
        endTime: "04:00"
        timezone: "UTC"
    preChecks:
      requireBackupWithin: "1h"
EOF
done
```

### 2. Monitor Fleet Progress

```bash
# Watch all upgrades
kubectl get postgresupgrade -A -w

# Find incomplete upgrades
kubectl get postgresupgrade -A -o json | \
  jq '.items[] | select(.status.phase != "Completed") |
      {name: .metadata.name, phase: .status.phase}'

# Find upgrades with verification issues
kubectl get postgresupgrade -A -o json | \
  jq '.items[] | select(.status.verification.tablesMismatched > 0)'
```

### 3. Operator Metrics

The operator exposes Prometheus metrics for fleet monitoring:

- `postgres_upgrades_total{phase}` - Count by phase
- `postgres_upgrades_replication_lag_seconds{upgrade}` - Lag per upgrade
- `postgres_upgrades_row_count_mismatches{upgrade}` - Verification failures
- `postgres_upgrade_phase_duration_seconds{phase}` - Time in each phase

## Troubleshooting

### Common Issues

| Issue | Cause | Resolution |
|-------|-------|------------|
| Stuck in `Pending` | Source cluster not found or not running | Verify source cluster exists and is in `Running` phase |
| Stuck in `CreatingTarget` | Target cluster won't start | Check target cluster events and pod logs |
| Replication lag not decreasing | Network issues or high write load | Check network policies and consider reducing write load |
| Verification failing | Schema differences or ongoing writes | Check for DDL changes and verify no writes during verification |

### Debug Commands

```bash
# Check upgrade events
kubectl describe postgresupgrade my-cluster-upgrade

# Check target cluster status
kubectl get postgrescluster my-cluster-v17-green

# Check replication status on target
kubectl exec -it my-cluster-v17-green-0 -- \
  psql -c "SELECT * FROM pg_stat_subscription;"

# Check publication on source
kubectl exec -it my-cluster-0 -- \
  psql -c "SELECT * FROM pg_publication;"
```

### Runbooks

For detailed troubleshooting procedures, see:

- [Upgrade Stuck Runbook](runbooks/upgrade-stuck.md)
- [Verification Failed Runbook](runbooks/upgrade-verification-failed.md)
- [Rollback Procedure Runbook](runbooks/upgrade-rollback.md)

## Cleanup

After a successful upgrade, the source cluster is NOT automatically deleted. This allows for manual recovery via PITR if issues are discovered later (see [Rollback](#rollback) for the post-cutover recovery procedure).

To clean up the source cluster manually:

```bash
# Verify upgrade is complete
kubectl get postgresupgrade my-cluster-upgrade -o jsonpath='{.status.phase}'
# Should show: Completed

# Delete the old source cluster when confident
kubectl delete postgrescluster my-cluster

# Optionally delete the upgrade resource
kubectl delete postgresupgrade my-cluster-upgrade
```

## Post-Upgrade Cluster Management

After a successful upgrade completes, you have two PostgresCluster resources:

| Cluster | Phase | Purpose |
|---------|-------|---------|
| Original (e.g., `my-cluster`) | `Superseded` | Kept for PITR recovery, no longer receives traffic |
| Target (e.g., `my-cluster-upgrade-target`) | `Running` | Active cluster, receives all traffic |

### Which Cluster to Manage

**Always manage the target cluster** for day-to-day operations:

```bash
# View both clusters - note the Successor column shows the relationship
kubectl get postgresclusters
NAME                          VERSION   PHASE        SUCCESSOR                      READY   AGE
my-cluster                    16        Superseded   my-cluster-upgrade-target      -       30d
my-cluster-upgrade-target     17        Running      -                              3       1d

# Scale the ACTIVE cluster (target)
kubectl patch postgrescluster my-cluster-upgrade-target \
  -p '{"spec":{"replicas":5}}'

# Modifying the superseded cluster will be BLOCKED:
kubectl patch postgrescluster my-cluster -p '{"spec":{"replicas":5}}'
# Error: Cluster 'my-cluster' has been superseded by 'my-cluster-upgrade-target'.
#        Modifications are blocked. To manage the upgraded cluster,
#        modify 'my-cluster-upgrade-target' instead.
```

### Service Continuity

Your applications continue to work without changes because the **services keep the original names**:

- `my-cluster-primary.my-namespace.svc` → Routes to target cluster primary
- `my-cluster-repl.my-namespace.svc` → Routes to target cluster replicas

The service name IS the stable identity. Applications connecting via services are unaffected by the cluster name change.

### Traceability

Each cluster tracks its lineage via status fields:

```bash
# Check where the target came from (origin)
kubectl get postgrescluster my-cluster-upgrade-target \
  -o jsonpath='{.status.origin}' | jq
{
  "name": "my-cluster",
  "namespace": "my-namespace",
  "upgradeName": "my-cluster-upgrade",
  "createdAt": "2025-01-13T10:00:00Z"
}

# Check where the source was upgraded to (successor)
kubectl get postgrescluster my-cluster \
  -o jsonpath='{.status.successor}' | jq
{
  "name": "my-cluster-upgrade-target",
  "namespace": "my-namespace",
  "upgradeName": "my-cluster-upgrade",
  "createdAt": "2025-01-13T10:00:00Z"
}
```

### GitOps Considerations

If using GitOps (ArgoCD, Flux, etc.), update your manifests to reference the new cluster name after an upgrade:

1. **Before upgrade**: Your manifest defines `my-cluster` with version `16`
2. **After upgrade**: Update manifest to define `my-cluster-upgrade-target` with version `17`
3. **Cleanup**: Remove the old `my-cluster` manifest after confirming stability

Alternatively, consider using a naming convention that includes the version:
- `my-cluster-v16` → `my-cluster-v17`

This makes it clearer in GitOps which version each manifest manages.

## Limitations

| Limitation | Details |
|------------|---------|
| DDL changes | Schema changes are not replicated; apply DDL to both clusters during upgrade |
| Large objects | LOBs have limited logical replication support |
| Sequences | Synced during `SyncingSequences` phase; source goes read-only during `Verifying` before this phase |
| Concurrent upgrades | Only one upgrade per source cluster allowed |
| Resource usage | Requires double resources during upgrade period |
