# Operations Guide

This guide covers day-2 operations for managing PostgreSQL clusters with the operator.

## Cluster Lifecycle

### Creating a Cluster

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresCluster
metadata:
  name: my-cluster
  namespace: default
spec:
  version: "16"
  replicas: 3
  storage:
    size: 100Gi
    storageClass: fast-ssd
```

```bash
kubectl apply -f my-cluster.yaml
```

Monitor creation progress:

```bash
# Watch status
kubectl get pgc my-cluster -w

# Check events
kubectl describe pgc my-cluster

# Check pods
kubectl get pods -l postgres-operator.smoketurner.com/cluster=my-cluster
```

### Deleting a Cluster

```bash
kubectl delete pgc my-cluster
```

The operator will:
1. Set the cluster to `Deleting` phase
2. Remove the finalizer after cleanup
3. Let Kubernetes garbage collect child resources

**Note**: PersistentVolumeClaims may be retained based on your StorageClass `reclaimPolicy`.

## Scaling

### Scale Replicas

```bash
kubectl patch pgc my-cluster --type merge -p '{"spec":{"replicas":5}}'
```

Or edit the manifest:

```yaml
spec:
  replicas: 5  # Changed from 3
```

The operator will:
1. Transition to `Scaling` phase
2. Update the StatefulSet
3. Wait for new replicas to sync
4. Return to `Running` phase

### Scaling Considerations

| From | To | Consideration |
|------|-----|---------------|
| 1 | 2+ | Enables streaming replication |
| 2 | 3+ | Enables automatic failover quorum |
| N | N-1 | Safe if current primary not removed |
| N | 1 | Loses HA, only do for development |

## Configuration Changes

### PostgreSQL Parameters

Update `postgresqlParams`:

```yaml
spec:
  postgresqlParams:
    max_connections: "500"
    shared_buffers: "2GB"
    work_mem: "64MB"
```

**Restart-required parameters** (e.g., `shared_buffers`, `max_connections`):
- Patroni will perform a rolling restart
- One pod restarts at a time
- Primary failover may occur

**Runtime parameters** (e.g., `work_mem`):
- Applied without restart
- Takes effect on new connections

### Resource Limits

```yaml
spec:
  resources:
    requests:
      cpu: "2"
      memory: "4Gi"
    limits:
      cpu: "4"
      memory: "8Gi"
```

**Kubernetes 1.35+ (In-Place Resizing):**

On Kubernetes 1.35+, the operator supports in-place resource resizing:

```bash
# Update resources without pod restarts
kubectl patch pgc my-cluster --type merge -p '{"spec":{"resources":{"requests":{"cpu":"4"}}}}'

# Monitor resize progress
kubectl get pgc my-cluster -o jsonpath='{.status.resize_status}' | jq .
```

| Resource | Default Behavior | Notes |
|----------|------------------|-------|
| CPU | Resize in-place | No restart required |
| Memory | Container restart | Requires cgroup update |

Check resize status:

```bash
# View per-pod resize status
kubectl get pgc my-cluster -o yaml | grep -A 20 resize_status

# Check if all pods are synced
kubectl get pgc my-cluster -o jsonpath='{.status.all_pods_synced}'
```

**Kubernetes < 1.35:**

Changes trigger a rolling restart of pods.

## Version Upgrades

### Minor Version Upgrades

Minor upgrades (e.g., 16.1 → 16.2) are handled automatically:

```yaml
spec:
  version: "16"  # Uses latest 16.x Spilo image
```

### Major Version Upgrades

Major upgrades (e.g., 15 → 16) require careful planning:

```yaml
spec:
  version: "16"  # Changed from "15"
```

**Current behavior**: The operator validates that the new version is not lower than the current version. Major upgrades use pg_upgrade internally via Spilo.

**Recommendations**:
1. Backup before upgrading
2. Test in non-production first
3. Monitor the upgrade process
4. Have a rollback plan

## Backup and Recovery

### Manual Backup

If backup is configured with a destination:

```yaml
spec:
  backup:
    schedule: "0 2 * * *"  # 2 AM daily
    retention:
      count: 7
    destination:
      type: S3
      bucket: my-backups
      region: us-east-1
      credentialsSecret: aws-creds
```

### Point-in-Time Recovery

PITR uses WAL archiving (when backup is configured):

```bash
# Check last backup
kubectl get pgc my-cluster -o jsonpath='{.status.lastBackup}'
```

### Disaster Recovery

For complete cluster loss:

1. Ensure backup destination is accessible
2. Create new cluster with same name
3. Operator restores from latest backup (when backup restore is implemented)

## Monitoring

### Cluster Status

```bash
# Quick status
kubectl get pgc

# Detailed status
kubectl get pgc my-cluster -o yaml

# Conditions
kubectl get pgc my-cluster -o jsonpath='{.status.conditions}' | jq .
```

### Cluster Phases

| Phase | Meaning | Action |
|-------|---------|--------|
| Pending | Waiting to start | Wait |
| Creating | Building resources | Wait |
| Running | Healthy | None needed |
| Updating | Config change | Wait |
| Scaling | Replica change | Wait |
| Degraded | Some replicas down | Investigate |
| Recovering | Auto-recovery | Wait |
| Failed | Needs intervention | Investigate |
| Deleting | Being deleted | Wait |

### Patroni Status

```bash
# List cluster members
kubectl exec my-cluster-0 -- patronictl list

# Show cluster configuration
kubectl exec my-cluster-0 -- patronictl show-config

# Check replication lag
kubectl exec my-cluster-0 -- patronictl list | grep -E "Lag|Member"
```

### Prometheus Metrics

If metrics are enabled in the operator:

```promql
# Reconciliation rate
rate(postgres_operator_reconciliations[5m])

# Error rate
rate(postgres_operator_reconciliation_errors[5m])

# Reconciliation latency
histogram_quantile(0.99, rate(postgres_operator_reconcile_duration_seconds_bucket[5m]))
```

### Alerting Examples

```yaml
groups:
  - name: postgres-operator
    rules:
      - alert: PostgresClusterDegraded
        expr: |
          kube_customresource_postgrescluster_status_phase{phase="Degraded"} == 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "PostgreSQL cluster {{ $labels.name }} is degraded"

      - alert: PostgresClusterFailed
        expr: |
          kube_customresource_postgrescluster_status_phase{phase="Failed"} == 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "PostgreSQL cluster {{ $labels.name }} has failed"
```

## Troubleshooting

### Cluster Stuck in Creating

**Symptoms**: Cluster stays in `Creating` for more than 10 minutes.

**Check**:
```bash
# Pod status
kubectl get pods -l postgres-operator.smoketurner.com/cluster=my-cluster

# PVC status (storage issues)
kubectl get pvc -l postgres-operator.smoketurner.com/cluster=my-cluster

# Pod events
kubectl describe pod my-cluster-0
```

**Common causes**:
- Storage class doesn't exist
- Insufficient cluster resources
- Image pull failures
- Init container failures

### Failover Not Working

**Symptoms**: Primary fails but no automatic failover.

**Check**:
```bash
# Patroni status
kubectl exec my-cluster-0 -- patronictl list

# Endpoints (DCS)
kubectl get endpoints my-cluster

# Patroni logs
kubectl logs my-cluster-0 -c postgres | grep -i election
```

**Common causes**:
- Only 1-2 replicas (no quorum)
- Network partition
- RBAC issues (can't update endpoints)

### Replication Lag

**Symptoms**: Replicas fall behind primary.

**Check**:
```bash
# Check lag in Patroni
kubectl exec my-cluster-0 -- patronictl list

# Check PostgreSQL replication
kubectl exec my-cluster-0 -- psql -U postgres -c "SELECT * FROM pg_stat_replication;"
```

**Common causes**:
- Heavy write load
- Slow storage on replicas
- Network issues
- Resource constraints

### Connection Issues

**Symptoms**: Applications can't connect.

**Check**:
```bash
# Service exists
kubectl get svc my-cluster-primary

# Endpoints have targets
kubectl get endpoints my-cluster-primary

# Test connectivity
kubectl run test --rm -it --image=postgres:16 -- \
  psql -h my-cluster-primary -U postgres -c "SELECT 1"
```

**Common causes**:
- Service selector mismatch
- NetworkPolicy blocking
- Credentials incorrect
- Primary not elected yet

### Operator Not Reconciling

**Symptoms**: Changes not being applied.

**Check**:
```bash
# Operator running
kubectl get pods -n postgres-operator-system

# Leader election
kubectl get lease -n postgres-operator-system postgres-operator-leader

# Operator logs
kubectl logs -n postgres-operator-system deploy/postgres-operator
```

**Common causes**:
- Operator crashed
- Leader election stuck
- RBAC permissions missing

## Maintenance

### Rolling Restart

Trigger a rolling restart without config changes:

```bash
kubectl rollout restart sts my-cluster
```

Or via Patroni:

```bash
kubectl exec my-cluster-0 -- patronictl restart my-cluster
```

### Switchover (Planned Failover)

Perform a controlled primary switchover:

```bash
# Check current primary
kubectl exec my-cluster-0 -- patronictl list

# Switchover to specific member
kubectl exec my-cluster-0 -- patronictl switchover my-cluster --master my-cluster-0 --candidate my-cluster-1

# Or let Patroni choose
kubectl exec my-cluster-0 -- patronictl switchover my-cluster --master my-cluster-0
```

### Node Maintenance

Before draining a node:

1. Check PodDisruptionBudget:
   ```bash
   kubectl get pdb -l postgres-operator.smoketurner.com/cluster=my-cluster
   ```

2. If primary is on the node, switchover first:
   ```bash
   kubectl exec my-cluster-0 -- patronictl switchover
   ```

3. Drain the node:
   ```bash
   kubectl drain <node> --ignore-daemonsets --delete-emptydir-data
   ```

### Storage Expansion

If your StorageClass supports expansion:

```yaml
spec:
  storage:
    size: 200Gi  # Increased from 100Gi
```

The operator will update PVC requests. Expansion may require pod restart depending on storage driver.

## Security

### Rotate Credentials

Generate new credentials secret:

```bash
# Delete existing secret (operator will regenerate)
kubectl delete secret my-cluster-credentials

# Or manually update
kubectl create secret generic my-cluster-credentials \
  --from-literal=password=$(openssl rand -base64 32) \
  --from-literal=replication-password=$(openssl rand -base64 32) \
  --dry-run=client -o yaml | kubectl apply -f -
```

**Note**: Rotating credentials requires application reconnection.

### Enable TLS

```yaml
spec:
  tls:
    enabled: true
    certSecret: my-cluster-tls  # Pre-created TLS secret
```

Create the TLS secret:

```bash
kubectl create secret tls my-cluster-tls \
  --cert=server.crt \
  --key=server.key
```

### Network Policies

Apply the sample network policy:

```bash
kubectl apply -f config/samples/networkpolicy-postgresql.yaml
```

This restricts PostgreSQL access to pods with specific labels.
