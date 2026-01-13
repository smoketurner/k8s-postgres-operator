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
| `SyncingSequences` | Synchronizing sequences after setting source to read-only |
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
      maxReplicationLagSeconds: 0    # Must be fully synced
      minVerificationPasses: 3       # Row counts must match 3 times
      verificationInterval: "1m"     # Time between checks
      requireBackupWithin: "1h"      # Required for Automatic mode
      drainConnectionsTimeout: "5m"  # Wait for connections to close

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

If issues are detected, you can rollback to the source cluster.

### Trigger Rollback

```bash
kubectl annotate postgresupgrade my-cluster-upgrade \
  postgres-operator.smoketurner.com/rollback=now
```

### Rollback Behavior

1. The operator stops replication
2. Sets source cluster back to read-write
3. Switches services back to source cluster
4. Marks upgrade as `RolledBack`

### Check Rollback Feasibility

```bash
kubectl get postgresupgrade my-cluster-upgrade \
  -o jsonpath='{.status.rollback}'
```

```yaml
rollback:
  feasible: true
  dataLossRisk: false
  reason: "Source cluster running, no writes to target since cutover"
```

> **Warning**: If `dataLossRisk: true`, writes have occurred to the target cluster after cutover. Rollback will lose those writes.

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

After a successful upgrade, the source cluster is NOT automatically deleted. This allows for rollback if issues are discovered later.

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
| Original (e.g., `my-cluster`) | `Superseded` | Kept for rollback safety, no longer receives traffic |
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
| Sequences | Synced during `SyncingSequences` phase after source goes read-only |
| Concurrent upgrades | Only one upgrade per source cluster allowed |
| Resource usage | Requires double resources during upgrade period |
