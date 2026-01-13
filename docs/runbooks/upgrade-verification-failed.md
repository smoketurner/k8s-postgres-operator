# Runbook: Upgrade Verification Failed

This runbook covers how to diagnose and resolve row count verification failures during PostgresUpgrade operations.

## Symptoms

- Upgrade stuck in `Verifying` phase
- `RowCountsVerified` condition is `False`
- `status.verification.tablesMismatched > 0`

## Understanding Verification

The operator verifies data integrity by comparing row counts between source and target clusters. Verification must pass multiple consecutive times (default: 3) to ensure stability.

```yaml
status:
  verification:
    lastCheckTime: "2025-01-11T10:00:00Z"
    tablesVerified: 42
    tablesMatched: 40
    tablesMismatched: 2
    consecutivePasses: 0
    mismatchedTables:
      - schema: public
        table: orders
        sourceCount: 1000
        targetCount: 998
      - schema: public
        table: order_items
        sourceCount: 5000
        targetCount: 4995
```

## Diagnosis

### 1. Check Verification Status

```bash
kubectl get postgresupgrade <upgrade-name> -o jsonpath='{.status.verification}' | jq
```

### 2. Identify Mismatched Tables

```bash
kubectl get postgresupgrade <upgrade-name> \
  -o jsonpath='{.status.verification.mismatchedTables}' | jq
```

### 3. Compare Row Counts Manually

```bash
# On source cluster
kubectl exec -it <source-cluster-primary> -- psql -c "
SELECT schemaname, relname, n_live_tup
FROM pg_stat_user_tables
ORDER BY schemaname, relname;
"

# On target cluster
kubectl exec -it <target-cluster-primary> -- psql -c "
SELECT schemaname, relname, n_live_tup
FROM pg_stat_user_tables
ORDER BY schemaname, relname;
"
```

### 4. Check Replication Status

```bash
kubectl exec -it <target-cluster-primary> -- psql -c "
SELECT subname, received_lsn, latest_end_lsn,
       last_msg_send_time, last_msg_receipt_time
FROM pg_stat_subscription;
"
```

## Common Causes and Solutions

### Cause 1: Ongoing Writes to Source

**Symptom**: Row counts keep changing; differences are small

**Diagnosis**:
```bash
# Check for active connections on source
kubectl exec -it <source-cluster-primary> -- psql -c "
SELECT count(*) as active_connections
FROM pg_stat_activity
WHERE state = 'active' AND query NOT LIKE '%pg_stat_activity%';
"
```

**Resolution**:
1. The verification will eventually pass once replication catches up
2. If urgent, consider reducing write load during verification:
   ```bash
   # Optionally pause non-critical writes to the source
   ```
3. Increase verification tolerance if small differences are acceptable:
   ```yaml
   spec:
     strategy:
       preChecks:
         rowCountTolerance: 10  # Allow up to 10 row difference
   ```

---

### Cause 2: Tables Not Being Replicated

**Symptom**: Large differences in row counts; target table is empty or much smaller

**Diagnosis**:
```bash
# Check publication on source
kubectl exec -it <source-cluster-primary> -- psql -c "
SELECT * FROM pg_publication_tables WHERE pubname = 'upgrade_pub';
"

# Check subscription status on target
kubectl exec -it <target-cluster-primary> -- psql -c "
SELECT * FROM pg_subscription_rel;
"
```

**Resolution**:
1. Verify all tables are included in the publication:
   ```bash
   kubectl exec -it <source-cluster-primary> -- psql -c "
   SELECT schemaname, tablename FROM pg_tables
   WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'metric_helpers', 'admin')
   EXCEPT
   SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'upgrade_pub';
   "
   ```

2. If tables are missing, the schema may not have been copied correctly. Consider:
   - Deleting the upgrade and retrying
   - Manually adding missing tables to the publication

---

### Cause 3: Tables Without Primary Keys

**Symptom**: Logical replication requires primary keys or REPLICA IDENTITY

**Diagnosis**:
```bash
kubectl exec -it <source-cluster-primary> -- psql -c "
SELECT n.nspname, c.relname
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND NOT EXISTS (
    SELECT 1 FROM pg_index i
    WHERE i.indrelid = c.oid AND i.indisprimary
  )
  AND c.relreplident = 'd';
"
```

**Resolution**:
1. Add primary keys to tables that need them:
   ```sql
   ALTER TABLE <table> ADD PRIMARY KEY (<columns>);
   ```

2. Or set REPLICA IDENTITY FULL for tables without natural keys:
   ```sql
   ALTER TABLE <table> REPLICA IDENTITY FULL;
   ```

3. After fixing, delete and recreate the upgrade

---

### Cause 4: Schema Differences

**Symptom**: Tables exist on source but not target, or vice versa

**Diagnosis**:
```bash
# Find tables on source not on target
kubectl exec -it <source-cluster-primary> -- psql -c "
SELECT schemaname, tablename FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'metric_helpers', 'admin');
" > source_tables.txt

kubectl exec -it <target-cluster-primary> -- psql -c "
SELECT schemaname, tablename FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'metric_helpers', 'admin');
" > target_tables.txt

diff source_tables.txt target_tables.txt
```

**Resolution**:
1. If tables are missing on target, schema copy may have failed:
   ```bash
   # Dump schema from source and apply to target manually
   kubectl exec -it <source-cluster-primary> -- \
     pg_dump --schema-only -N pg_catalog -N information_schema \
       -N metric_helpers -N admin > schema.sql

   kubectl cp schema.sql <target-cluster-primary>:/tmp/schema.sql
   kubectl exec -it <target-cluster-primary> -- \
     psql -f /tmp/schema.sql
   ```

2. Delete and recreate the upgrade

---

### Cause 5: Triggers Modifying Data

**Symptom**: Row counts differ even after replication catches up

**Diagnosis**:
```bash
kubectl exec -it <target-cluster-primary> -- psql -c "
SELECT trigger_schema, trigger_name, event_object_table, action_statement
FROM information_schema.triggers
WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema');
"
```

**Resolution**:
1. Disable triggers on target during replication:
   ```bash
   kubectl exec -it <target-cluster-primary> -- psql -c "
   ALTER TABLE <table> DISABLE TRIGGER ALL;
   "
   ```

2. After cutover, re-enable triggers:
   ```bash
   kubectl exec -it <target-cluster-primary> -- psql -c "
   ALTER TABLE <table> ENABLE TRIGGER ALL;
   "
   ```

---

### Cause 6: Statistics Not Updated

**Symptom**: `n_live_tup` in `pg_stat_user_tables` is inaccurate

**Diagnosis**:
```bash
# Compare pg_stat_user_tables with actual count
kubectl exec -it <target-cluster-primary> -- psql -c "
SELECT relname, n_live_tup,
       (SELECT count(*) FROM <schema>.<table>) as actual_count
FROM pg_stat_user_tables
WHERE relname = '<table>';
"
```

**Resolution**:
```bash
# Run ANALYZE on target to update statistics
kubectl exec -it <target-cluster-primary> -- psql -c "ANALYZE;"
```

## Adjusting Verification Settings

If small differences are acceptable, adjust the upgrade spec:

```yaml
spec:
  strategy:
    preChecks:
      # Allow small row count differences
      rowCountTolerance: 100

      # Reduce required consecutive passes
      minVerificationPasses: 1

      # Increase time between checks
      verificationInterval: "5m"
```

> **Warning**: Loosening verification increases the risk of undetected data loss

## Forcing Cutover (Use with Caution)

If you've verified data integrity manually and want to proceed despite verification failures:

1. Switch to manual mode if not already:
   ```bash
   kubectl patch postgresupgrade <upgrade-name> --type=merge -p '
   {
     "spec": {
       "strategy": {
         "cutover": {
           "mode": "Manual"
         }
       }
     }
   }'
   ```

2. Manually verify data integrity

3. Trigger cutover despite verification status:
   ```bash
   kubectl annotate postgresupgrade <upgrade-name> \
     postgres-operator.smoketurner.com/cutover=now \
     postgres-operator.smoketurner.com/skip-verification=true
   ```

> **Danger**: Only do this if you've manually verified data integrity

## Recovery

If verification keeps failing and you need to restart:

1. Delete the upgrade:
   ```bash
   kubectl delete postgresupgrade <upgrade-name>
   ```

2. Clean up the target cluster:
   ```bash
   kubectl delete postgrescluster <target-cluster-name>
   ```

3. Fix any schema or data issues on source

4. Retry the upgrade:
   ```bash
   kubectl apply -f upgrade.yaml
   ```

## Escalation

If unable to resolve:

1. Collect diagnostics:
   ```bash
   kubectl get postgresupgrade <upgrade-name> -o yaml > upgrade.yaml
   kubectl describe postgresupgrade <upgrade-name> > events.txt

   # Row counts from both clusters
   kubectl exec -it <source-primary> -- psql -c \
     "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables;" > source_counts.txt
   kubectl exec -it <target-primary> -- psql -c \
     "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables;" > target_counts.txt
   ```

2. Open an issue with collected data
