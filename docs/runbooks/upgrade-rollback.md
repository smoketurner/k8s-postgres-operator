# Runbook: Upgrade Rollback Procedure

This runbook covers how to safely rollback a PostgresUpgrade to the original source cluster.

## When to Rollback

Consider rolling back when:
- Performance degradation on new version discovered during replication
- Compatibility issues with new PostgreSQL version found during testing
- Verification failures that cannot be resolved
- Unexpected behavior in the target cluster during replication

**Note**: Rollback must be triggered before the `CuttingOver` phase begins.

## Prerequisites

Before rolling back, understand:

1. **Current Phase**: Rollback is only supported before `CuttingOver` begins
2. **Post-cutover**: Recovery requires manual PITR from a pre-upgrade backup

## Check Current Phase

```bash
kubectl get postgresupgrade <upgrade-name> -o jsonpath='{.status.phase}'
```

## Rollback by Phase

### Before Cutover (Phases: Pending through WaitingForManualCutover)

Rollback is straightforward - no data has been written to target as primary.

```bash
kubectl annotate postgresupgrade <upgrade-name> \
  postgres-operator.smoketurner.com/rollback=now
```

**What happens**:
1. Subscription is dropped on target
2. Publication is dropped on source
3. Source remains in its current state (read-write)
4. Target cluster is left for manual cleanup
5. Upgrade status changes to `RolledBack`

---

### After Cutover (Phases: CuttingOver, HealthChecking, Completed)

**Automated rollback is not supported after `CuttingOver` begins.** If you
add the rollback annotation during these phases:

1. A `RollbackNotAllowed` warning event is emitted
2. The rollback annotation is cleared
3. The upgrade remains in its current phase

This is intentional: once service selectors flip to the target, the new
primary may have accepted writes that the source does not have. Rolling back
would silently drop data.

**Recovery requires manual PITR** from a pre-upgrade backup. See
`docs/upgrades.md` for the post-cutover recovery procedure.

## Manual Rollback Procedure

If automated rollback fails or you need more control:

### Step 1: Stop Application Traffic

```bash
# Scale down applications or update their database connection
# This prevents new writes during the rollback
```

### Step 2: Export Critical Data (if needed)

```bash
# If data was written to target, export it first
kubectl exec -it <target-cluster-primary> -- \
  pg_dump --data-only --table=<critical-table> > critical_data.sql
```

### Step 3: Set Source to Read-Write

```bash
kubectl exec -it <source-cluster-primary> -- psql -c "
ALTER SYSTEM RESET default_transaction_read_only;
SELECT pg_reload_conf();
"

# Verify
kubectl exec -it <source-cluster-primary> -- psql -c "
SHOW default_transaction_read_only;
"
```

### Step 4: Switch Services Back to Source

```bash
# Patch the primary service
kubectl patch service <cluster-name>-primary -p '
{
  "spec": {
    "selector": {
      "postgres-operator.smoketurner.com/cluster": "<source-cluster-name>",
      "spilo-role": "master"
    }
  }
}'

# Patch the replica service
kubectl patch service <cluster-name>-repl -p '
{
  "spec": {
    "selector": {
      "postgres-operator.smoketurner.com/cluster": "<source-cluster-name>",
      "spilo-role": "replica"
    }
  }
}'
```

### Step 5: Verify Traffic is Going to Source

```bash
# Check that source is receiving connections
kubectl exec -it <source-cluster-primary> -- psql -c "
SELECT count(*) FROM pg_stat_activity WHERE datname = 'postgres';
"

# Test a write operation
kubectl exec -it <source-cluster-primary> -- psql -c "
SELECT 1;
"
```

### Step 6: Clean Up Replication

```bash
# Drop subscription on target
kubectl exec -it <target-cluster-primary> -- psql -c "
DROP SUBSCRIPTION IF EXISTS upgrade_sub;
"

# Drop publication on source
kubectl exec -it <source-cluster-primary> -- psql -c "
DROP PUBLICATION IF EXISTS upgrade_pub;
"
```

### Step 7: Mark Upgrade as Rolled Back

```bash
kubectl patch postgresupgrade <upgrade-name> --type=merge -p '
{
  "status": {
    "phase": "RolledBack"
  }
}'
```

### Step 8: Resume Application Traffic

```bash
# Scale applications back up or restore database connections
```

## Post-Rollback Actions

### Verify Source Cluster Health

```bash
# Check cluster status
kubectl get postgrescluster <source-cluster-name>

# Check replication status (if HA)
kubectl exec -it <source-cluster-primary> -- psql -c "
SELECT client_addr, state, sent_lsn, replay_lsn
FROM pg_stat_replication;
"

# Run health checks
kubectl exec -it <source-cluster-primary> -- psql -c "
SELECT pg_is_in_recovery();
"
```

### Decide on Target Cluster

The target cluster remains after rollback. Options:

1. **Keep for retry**: Leave it for another upgrade attempt
   ```bash
   # Delete and recreate the upgrade resource
   kubectl delete postgresupgrade <upgrade-name>
   kubectl apply -f upgrade.yaml
   ```

2. **Delete target**: Remove if not needed
   ```bash
   kubectl delete postgrescluster <target-cluster-name>
   ```

### Investigate Root Cause

Before retrying the upgrade:

1. Review what went wrong
2. Check application compatibility with new PostgreSQL version
3. Test in a non-production environment first
4. Address any issues discovered

## Rollback Failures

### Rollback Not Allowed

If rollback is requested during `CuttingOver`, `HealthChecking`, or `Completed`:

- A `RollbackNotAllowed` event is emitted
- The annotation is cleared automatically
- No state change occurs

**Resolution**: Follow the manual PITR recovery procedure in `docs/upgrades.md`

### Services Won't Switch

If service patching fails:

```bash
# Check service ownership
kubectl get service <cluster-name>-primary -o yaml | grep ownerReferences -A5

# Force patch with strategic merge
kubectl patch service <cluster-name>-primary --type=strategic -p '
{
  "spec": {
    "selector": {
      "postgres-operator.smoketurner.com/cluster": "<source-cluster-name>"
    }
  }
}'
```

### Source Stuck in Read-Only

If source won't accept writes:

```bash
# Check current setting
kubectl exec -it <source-cluster-primary> -- psql -c "
SHOW default_transaction_read_only;
"

# Force reset
kubectl exec -it <source-cluster-primary> -- psql -c "
ALTER SYSTEM RESET default_transaction_read_only;
ALTER SYSTEM RESET transaction_read_only;
SELECT pg_reload_conf();
"

# If still stuck, restart Patroni
kubectl delete pod <source-cluster-primary>
```

## Emergency Rollback Checklist

For urgent situations:

- [ ] Check current phase: `kubectl get postgresupgrade <name> -o jsonpath='{.status.phase}'`
- [ ] Verify phase is before `CuttingOver` (rollback not supported after cutover begins)
- [ ] Stop application traffic if possible
- [ ] Trigger rollback: `kubectl annotate postgresupgrade <name> postgres-operator.smoketurner.com/rollback=now`
- [ ] Monitor rollback progress: `kubectl get postgresupgrade <name> -w`
- [ ] Verify source is accepting writes
- [ ] Verify services point to source
- [ ] Resume application traffic
- [ ] Document the incident

## Escalation

If rollback fails:

1. Collect diagnostics:
   ```bash
   kubectl get postgresupgrade <upgrade-name> -o yaml > upgrade.yaml
   kubectl get postgrescluster <source-cluster> -o yaml > source.yaml
   kubectl get postgrescluster <target-cluster> -o yaml > target.yaml
   kubectl describe postgresupgrade <upgrade-name> > events.txt
   kubectl logs -l app.kubernetes.io/name=postgres-operator -n postgres-operator > operator.log
   ```

2. Check if this is a data integrity emergency

3. Engage database team for manual intervention if needed
