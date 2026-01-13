# Runbook: Upgrade Stuck

This runbook covers how to diagnose and resolve PostgresUpgrade resources that are stuck in a non-terminal phase.

## Symptoms

- PostgresUpgrade resource not progressing
- Phase has not changed for an extended period
- No events being generated

## Diagnosis

### 1. Check Current Phase

```bash
kubectl get postgresupgrade <upgrade-name> -o yaml
```

Look at:
- `.status.phase` - Current phase
- `.status.conditions` - Detailed condition status
- `.status.startedAt` - When the upgrade started

### 2. Check Operator Logs

```bash
kubectl logs -l app.kubernetes.io/name=postgres-operator -n postgres-operator --tail=100 | grep <upgrade-name>
```

### 3. Check Events

```bash
kubectl describe postgresupgrade <upgrade-name>
```

## Resolution by Phase

### Stuck in `Pending`

**Cause**: Source cluster validation failed

**Steps**:
1. Verify source cluster exists:
   ```bash
   kubectl get postgrescluster <source-cluster-name>
   ```

2. Verify source cluster is Running:
   ```bash
   kubectl get postgrescluster <source-cluster-name> -o jsonpath='{.status.phase}'
   ```

3. If source is not Running, fix the source cluster first

4. Check for concurrent upgrades:
   ```bash
   kubectl get postgresupgrade -l postgres-operator.smoketurner.com/source-cluster=<source-cluster-name>
   ```

**Resolution**: Fix the source cluster or delete conflicting upgrades

---

### Stuck in `CreatingTarget`

**Cause**: Target cluster failed to start

**Steps**:
1. Find the target cluster:
   ```bash
   kubectl get postgrescluster -l postgres-operator.smoketurner.com/upgrade=<upgrade-name>
   ```

2. Check target cluster status:
   ```bash
   kubectl describe postgrescluster <target-cluster-name>
   ```

3. Check target cluster pods:
   ```bash
   kubectl get pods -l postgres-operator.smoketurner.com/cluster=<target-cluster-name>
   kubectl describe pod <target-cluster-pod>
   ```

4. Check for common issues:
   - Insufficient resources (CPU/memory)
   - PVC not provisioning (storage class issues)
   - Image pull errors
   - Network policy blocking

**Resolution**: Fix the target cluster issue. Common fixes:
- Increase cluster quota
- Fix storage class configuration
- Update image pull secrets

---

### Stuck in `ConfiguringReplication`

**Cause**: Unable to set up logical replication

**Steps**:
1. Check if publication exists on source:
   ```bash
   kubectl exec -it <source-cluster-primary> -- \
     psql -c "SELECT * FROM pg_publication WHERE pubname = 'upgrade_pub';"
   ```

2. Check if subscription exists on target:
   ```bash
   kubectl exec -it <target-cluster-primary> -- \
     psql -c "SELECT * FROM pg_subscription WHERE subname = 'upgrade_sub';"
   ```

3. Check subscription status:
   ```bash
   kubectl exec -it <target-cluster-primary> -- \
     psql -c "SELECT * FROM pg_stat_subscription;"
   ```

4. Check for connection issues:
   ```bash
   kubectl exec -it <target-cluster-primary> -- \
     psql -c "SELECT * FROM pg_stat_subscription_stats;"
   ```

**Resolution**:
- Verify network connectivity between clusters
- Check that `wal_level=logical` on source
- Verify superuser credentials are correct

---

### Stuck in `Replicating`

**Cause**: Replication lag not reaching zero

**Steps**:
1. Check current lag:
   ```bash
   kubectl get postgresupgrade <upgrade-name> -o jsonpath='{.status.replication}'
   ```

2. Monitor lag over time:
   ```bash
   watch -n 5 "kubectl get postgresupgrade <upgrade-name> -o jsonpath='{.status.replication.lagBytes}'"
   ```

3. Check if lag is decreasing or stable

4. On the target, check subscription health:
   ```bash
   kubectl exec -it <target-cluster-primary> -- \
     psql -c "SELECT subname, received_lsn, latest_end_lsn FROM pg_stat_subscription;"
   ```

**Resolution**:
- If lag is stable but non-zero, check for long-running transactions on source
- If lag is increasing, reduce write load on source
- Check network bandwidth between clusters
- Consider timeout extension if initial sync is large

---

### Stuck in `Verifying`

**Cause**: Row count verification failing

**Steps**:
1. Check verification status:
   ```bash
   kubectl get postgresupgrade <upgrade-name> -o jsonpath='{.status.verification}'
   ```

2. Check for mismatched tables:
   ```bash
   kubectl get postgresupgrade <upgrade-name> -o jsonpath='{.status.verification.mismatchedTables}'
   ```

3. Compare row counts manually:
   ```bash
   # On source
   kubectl exec -it <source-cluster-primary> -- \
     psql -c "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables ORDER BY schemaname, relname;"

   # On target
   kubectl exec -it <target-cluster-primary> -- \
     psql -c "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables ORDER BY schemaname, relname;"
   ```

**Resolution**: See [upgrade-verification-failed.md](upgrade-verification-failed.md)

---

### Stuck in `SyncingSequences`

**Cause**: Sequence synchronization failed

**Steps**:
1. Check sequence status:
   ```bash
   kubectl get postgresupgrade <upgrade-name> -o jsonpath='{.status.sequences}'
   ```

2. Check failed sequences:
   ```bash
   kubectl get postgresupgrade <upgrade-name> -o jsonpath='{.status.sequences.failedSequences}'
   ```

3. Manually compare sequences:
   ```bash
   # On source
   kubectl exec -it <source-cluster-primary> -- \
     psql -c "SELECT schemaname, sequencename, last_value FROM pg_sequences WHERE schemaname NOT IN ('pg_catalog', 'information_schema');"
   ```

**Resolution**:
- Check for sequences with special permissions
- Manually sync failed sequences if necessary

---

### Stuck in `WaitingForManualCutover`

**Cause**: This is expected for `mode: Manual`

**Steps**:
1. Verify all pre-checks passed:
   ```bash
   kubectl get postgresupgrade <upgrade-name> -o jsonpath='{.status.conditions}'
   ```

2. If ready, trigger cutover:
   ```bash
   kubectl annotate postgresupgrade <upgrade-name> \
     postgres-operator.smoketurner.com/cutover=now
   ```

---

## General Recovery Steps

### Force Refresh

Delete and recreate the upgrade (data is preserved):

```bash
kubectl delete postgresupgrade <upgrade-name>
# Wait for cleanup
kubectl apply -f upgrade.yaml
```

### Manual Cleanup

If the upgrade is in a bad state:

1. Delete the upgrade resource:
   ```bash
   kubectl delete postgresupgrade <upgrade-name>
   ```

2. Clean up the target cluster if needed:
   ```bash
   kubectl delete postgrescluster <target-cluster-name>
   ```

3. Clean up replication on source:
   ```bash
   kubectl exec -it <source-cluster-primary> -- \
     psql -c "DROP PUBLICATION IF EXISTS upgrade_pub;"
   ```

4. Retry the upgrade:
   ```bash
   kubectl apply -f upgrade.yaml
   ```

## Escalation

If the issue cannot be resolved:

1. Collect diagnostic information:
   ```bash
   kubectl get postgresupgrade <upgrade-name> -o yaml > upgrade-status.yaml
   kubectl describe postgresupgrade <upgrade-name> > upgrade-events.txt
   kubectl logs -l app.kubernetes.io/name=postgres-operator -n postgres-operator > operator-logs.txt
   ```

2. Consider rolling back if urgent:
   ```bash
   kubectl annotate postgresupgrade <upgrade-name> \
     postgres-operator.smoketurner.com/rollback=now
   ```

3. Open an issue with the collected diagnostics
