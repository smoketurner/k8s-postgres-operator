# Backup and Restore Guide

This guide covers how to configure, monitor, and restore PostgreSQL backups managed by the k8s-postgres-operator.

## Table of Contents

1. [Overview](#overview)
2. [Configuring Backups](#configuring-backups)
3. [Cloud Provider Setup](#cloud-provider-setup)
4. [Monitoring Backups](#monitoring-backups)
5. [Restore Operations](#restore-operations)
6. [Point-in-Time Recovery](#point-in-time-recovery)
7. [Troubleshooting](#troubleshooting)

## Overview

The operator integrates with [WAL-G](https://github.com/wal-g/wal-g) through the Spilo container image to provide:

- **Continuous WAL archiving**: Every transaction is streamed to cloud storage
- **Scheduled base backups**: Full physical backups on a cron schedule
- **Point-in-time recovery (PITR)**: Restore to any moment between backups
- **Delta backups**: Store only changed pages to reduce backup size
- **Encryption**: AES-256 or PGP encryption for backups at rest

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PostgresCluster Pod                         │
│  ┌──────────────────────┐    ┌─────────────────────────────┐   │
│  │  PostgreSQL Server   │    │       WAL-G                 │   │
│  │                      │    │                             │   │
│  │  ┌───────────────┐   │    │  • wal-push (continuous)    │   │
│  │  │ WAL Files     │───┼────│  • backup-push (scheduled)  │   │
│  │  └───────────────┘   │    │  • backup-list              │   │
│  │                      │    │  • backup-fetch (restore)   │   │
│  └──────────────────────┘    └──────────────┬──────────────┘   │
└─────────────────────────────────────────────┼───────────────────┘
                                              │
                                              ▼
                    ┌───────────────────────────────────────────┐
                    │          Cloud Storage                    │
                    │  (S3 / GCS / Azure Blob)                  │
                    │                                           │
                    │  /{namespace}/{cluster-name}/             │
                    │  ├── basebackups/                         │
                    │  │   ├── base_000000010000000000000001/   │
                    │  │   └── base_000000010000000000000002/   │
                    │  └── wal/                                 │
                    │      ├── 000000010000000000000001         │
                    │      ├── 000000010000000000000002         │
                    │      └── ...                              │
                    └───────────────────────────────────────────┘
```

## Configuring Backups

Add a `backup` section to your PostgresCluster spec:

```yaml
apiVersion: postgres.example.com/v1alpha1
kind: PostgresCluster
metadata:
  name: my-cluster
spec:
  version: "16"
  replicas: 3
  storage:
    size: 100Gi

  backup:
    # Required: Cron schedule for base backups
    schedule: "0 2 * * *"  # Daily at 2 AM

    # Required: How many backups to keep
    retention:
      count: 7           # Keep 7 most recent base backups
      maxAge: "30d"      # Also delete backups older than 30 days

    # Required: Where to store backups
    destination:
      type: S3           # S3, GCS, or Azure
      bucket: my-backups
      region: us-east-1
      credentialsSecret: aws-credentials

    # Optional settings
    compression: zstd              # lz4 (fast), zstd (balanced), lzma (small)
    backupFromReplica: true        # Reduce primary load
    uploadConcurrency: 16          # Parallel uploads
    enableDeltaBackups: false      # Enable delta/incremental backups
```

### Backup Schedule Examples

```yaml
# Every day at 2 AM
schedule: "0 2 * * *"

# Every 6 hours
schedule: "0 */6 * * *"

# Every Sunday at midnight
schedule: "0 0 * * 0"

# Every hour (for testing)
schedule: "0 * * * *"
```

### Retention Policies

```yaml
retention:
  # Keep last N backups (recommended)
  count: 7

  # Delete backups older than duration
  maxAge: "30d"  # Supports: d (days), w (weeks), m (months)

  # Both can be used together - backup deleted when EITHER condition is met
```

## Cloud Provider Setup

### AWS S3

1. **Create an S3 bucket:**
   ```bash
   aws s3 mb s3://my-postgres-backups --region us-east-1
   ```

2. **Create IAM user/role with permissions:**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:PutObject",
           "s3:GetObject",
           "s3:DeleteObject",
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::my-postgres-backups",
           "arn:aws:s3:::my-postgres-backups/*"
         ]
       }
     ]
   }
   ```

3. **Create Kubernetes secret:**
   ```bash
   kubectl create secret generic aws-backup-credentials \
     --from-literal=AWS_ACCESS_KEY_ID=<access-key> \
     --from-literal=AWS_SECRET_ACCESS_KEY=<secret-key>
   ```

4. **Configure backup:**
   ```yaml
   backup:
     destination:
       type: S3
       bucket: my-postgres-backups
       region: us-east-1
       credentialsSecret: aws-backup-credentials
   ```

### Google Cloud Storage

1. **Create a GCS bucket:**
   ```bash
   gsutil mb gs://my-postgres-backups
   ```

2. **Create service account:**
   ```bash
   gcloud iam service-accounts create postgres-backup

   gcloud projects add-iam-policy-binding PROJECT_ID \
     --member="serviceAccount:postgres-backup@PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/storage.objectAdmin"

   gcloud iam service-accounts keys create key.json \
     --iam-account=postgres-backup@PROJECT_ID.iam.gserviceaccount.com
   ```

3. **Create Kubernetes secret:**
   ```bash
   kubectl create secret generic gcs-backup-credentials \
     --from-file=GOOGLE_APPLICATION_CREDENTIALS=./key.json
   ```

4. **Configure backup:**
   ```yaml
   backup:
     destination:
       type: GCS
       bucket: my-postgres-backups
       credentialsSecret: gcs-backup-credentials
   ```

### Azure Blob Storage

1. **Create storage account and container:**
   ```bash
   az storage account create --name mypostgresbackups --resource-group mygroup
   az storage container create --name backups --account-name mypostgresbackups
   ```

2. **Get storage key:**
   ```bash
   az storage account keys list --account-name mypostgresbackups
   ```

3. **Create Kubernetes secret:**
   ```bash
   kubectl create secret generic azure-backup-credentials \
     --from-literal=AZURE_STORAGE_ACCESS_KEY=<storage-key>
   ```

4. **Configure backup:**
   ```yaml
   backup:
     destination:
       type: Azure
       container: backups
       storageAccount: mypostgresbackups
       credentialsSecret: azure-backup-credentials
   ```

### S3-Compatible Storage (MinIO, Ceph, etc.)

```yaml
backup:
  destination:
    type: S3
    bucket: postgres-backups
    region: us-east-1  # Required but can be any value
    endpoint: "http://minio.minio-system.svc:9000"
    credentialsSecret: minio-credentials
    forcePathStyle: true   # Required for most S3-compatible storage
    disableSse: true       # Disable AWS SSE
```

## Monitoring Backups

### Check Backup Status

The cluster status shows backup information:

```bash
kubectl get postgrescluster my-cluster -o jsonpath='{.status.backup}'
```

Example output:
```json
{
  "enabled": true,
  "destinationType": "S3",
  "lastBackupTime": "2025-01-10T02:00:00Z",
  "lastBackupName": "base_000000010000000000000010",
  "backupCount": 7,
  "walArchivingHealthy": true,
  "recoveryWindowStart": "2025-01-03T02:00:00Z",
  "recoveryWindowEnd": "2025-01-10T12:34:56Z"
}
```

### List Available Backups

Connect to a pod and use WAL-G:

```bash
# Get pod name
kubectl get pods -l postgres.example.com/cluster=my-cluster

# List backups
kubectl exec -it my-cluster-0 -- su postgres -c "wal-g backup-list"
```

Example output:
```
name                            modified              wal_segment_backup_start
base_000000010000000000000010   2025-01-10T02:00:00Z  000000010000000000000010
base_000000010000000000000008   2025-01-09T02:00:00Z  000000010000000000000008
base_000000010000000000000006   2025-01-08T02:00:00Z  000000010000000000000006
...
```

### Check WAL Archiving

```bash
# Check archive status in PostgreSQL
kubectl exec -it my-cluster-0 -- psql -U postgres -c \
  "SELECT * FROM pg_stat_archiver;"

# Check recent WAL segments
kubectl exec -it my-cluster-0 -- su postgres -c "wal-g wal-show"
```

## Restore Operations

### Restore to New Cluster (Clone)

The recommended approach is to create a new cluster and restore into it. This is safer and allows testing before switching traffic.

1. **Create a restore cluster configuration:**

```yaml
# restore-cluster.yaml
apiVersion: postgres.example.com/v1alpha1
kind: PostgresCluster
metadata:
  name: my-cluster-restored
spec:
  version: "16"
  replicas: 1  # Start with 1, scale up after restore
  storage:
    size: 100Gi

  # Same backup configuration as source cluster
  backup:
    schedule: "0 2 * * *"
    retention:
      count: 7
    destination:
      type: S3
      bucket: my-postgres-backups  # Same bucket as source
      region: us-east-1
      credentialsSecret: aws-backup-credentials
      path: original-namespace/my-cluster  # Point to source cluster's backups
```

2. **Create the cluster (it will start empty):**
```bash
kubectl apply -f restore-cluster.yaml
```

3. **Wait for the cluster to be ready, then stop Patroni:**
```bash
kubectl exec -it my-cluster-restored-0 -- patronictl pause
```

4. **Perform the restore:**
```bash
# Restore the most recent backup
kubectl exec -it my-cluster-restored-0 -- su postgres -c \
  "wal-g backup-fetch /var/lib/postgresql/data LATEST"

# Or restore a specific backup
kubectl exec -it my-cluster-restored-0 -- su postgres -c \
  "wal-g backup-fetch /var/lib/postgresql/data base_000000010000000000000010"
```

5. **Create recovery configuration for PITR (optional):**
```bash
kubectl exec -it my-cluster-restored-0 -- bash -c 'cat > /var/lib/postgresql/data/recovery.signal'

# If you want PITR, also set target time:
kubectl exec -it my-cluster-restored-0 -- bash -c \
  "echo \"recovery_target_time = '2025-01-10 10:30:00 UTC'\" >> /var/lib/postgresql/data/postgresql.conf"
```

6. **Resume Patroni:**
```bash
kubectl exec -it my-cluster-restored-0 -- patronictl resume
```

7. **Scale up replicas once restore is complete:**
```bash
kubectl patch postgrescluster my-cluster-restored -p '{"spec":{"replicas":3}}' --type=merge
```

### In-Place Restore (Dangerous!)

> ⚠️ **Warning**: This will destroy all data in the current cluster. Only use if you understand the risks.

1. **Scale down to prevent data corruption:**
```bash
kubectl scale postgrescluster my-cluster --replicas=0
```

2. **Delete PVCs (data will be lost!):**
```bash
kubectl delete pvc -l postgres.example.com/cluster=my-cluster
```

3. **Scale back up and perform restore:**
```bash
kubectl scale postgrescluster my-cluster --replicas=1
# Wait for pod to start
kubectl exec -it my-cluster-0 -- patronictl pause
kubectl exec -it my-cluster-0 -- su postgres -c "wal-g backup-fetch /var/lib/postgresql/data LATEST"
kubectl exec -it my-cluster-0 -- patronictl resume
```

## Point-in-Time Recovery

PITR allows restoring to any specific moment, not just backup times.

### Recovery Window

The recovery window is the time range you can restore to:
- **Start**: Oldest available base backup time
- **End**: Most recent archived WAL file time

Check your recovery window:
```bash
kubectl get postgrescluster my-cluster -o jsonpath='{.status.backup.recoveryWindowStart}'
kubectl get postgrescluster my-cluster -o jsonpath='{.status.backup.recoveryWindowEnd}'
```

### PITR to Specific Time

1. Follow the "Restore to New Cluster" steps above
2. Before resuming Patroni, configure the target time:

```bash
# Create recovery signal file
kubectl exec -it my-cluster-restored-0 -- touch /var/lib/postgresql/data/recovery.signal

# Set the target time
kubectl exec -it my-cluster-restored-0 -- bash -c \
  "echo \"recovery_target_time = '2025-01-10 10:30:00+00'\" >> /var/lib/postgresql/data/postgresql.auto.conf"
kubectl exec -it my-cluster-restored-0 -- bash -c \
  "echo \"recovery_target_action = 'promote'\" >> /var/lib/postgresql/data/postgresql.auto.conf"
```

3. Resume and PostgreSQL will replay WAL until the target time

### PITR to Transaction ID

```bash
kubectl exec -it my-cluster-restored-0 -- bash -c \
  "echo \"recovery_target_xid = '12345678'\" >> /var/lib/postgresql/data/postgresql.auto.conf"
```

### PITR to Named Restore Point

If you create named restore points in your application:
```sql
SELECT pg_create_restore_point('before_migration_v2');
```

You can restore to them:
```bash
kubectl exec -it my-cluster-restored-0 -- bash -c \
  "echo \"recovery_target_name = 'before_migration_v2'\" >> /var/lib/postgresql/data/postgresql.auto.conf"
```

## Encryption

### Enable Encryption

1. **Generate encryption key:**
```bash
# For AES-256 (recommended)
openssl rand 32 > encryption.key

# Create secret
kubectl create secret generic backup-encryption-key \
  --from-file=encryption-key=./encryption.key
```

2. **Configure encrypted backups:**
```yaml
backup:
  encryption:
    enabled: true
    method: aes256
    keySecret: backup-encryption-key
  destination:
    # ... your cloud storage config
```

### Key Management

- **Store keys securely**: Use a secrets manager (Vault, AWS Secrets Manager, etc.)
- **Backup your keys**: Without the key, backups cannot be restored
- **Key rotation**: Not supported for existing backups; create new cluster with new key

## Troubleshooting

### Backup Failures

1. **Check pod logs:**
```bash
kubectl logs my-cluster-0 -c postgres | grep -i backup
kubectl logs my-cluster-0 -c postgres | grep -i wal
```

2. **Check backup status:**
```bash
kubectl get postgrescluster my-cluster -o yaml | grep -A20 'status:'
```

3. **Common issues:**
   - **Credentials invalid**: Verify secret exists and has correct keys
   - **Bucket doesn't exist**: Create the bucket/container
   - **Permission denied**: Check IAM permissions
   - **Network issues**: Check if pod can reach cloud storage

### WAL Archiving Issues

1. **Check archive status:**
```bash
kubectl exec -it my-cluster-0 -- psql -U postgres -c \
  "SELECT * FROM pg_stat_archiver;"
```

2. **Check for failed archive commands:**
```bash
kubectl exec -it my-cluster-0 -- psql -U postgres -c \
  "SELECT last_failed_wal, last_failed_time FROM pg_stat_archiver;"
```

3. **Test WAL upload manually:**
```bash
kubectl exec -it my-cluster-0 -- su postgres -c \
  "wal-g wal-push /var/lib/postgresql/data/pg_wal/000000010000000000000001"
```

### Restore Failures

1. **Verify backup exists:**
```bash
kubectl exec -it my-cluster-0 -- su postgres -c "wal-g backup-list"
```

2. **Check download permissions:**
```bash
# Test fetching a backup
kubectl exec -it my-cluster-0 -- su postgres -c "wal-g backup-fetch /tmp/test LATEST --dry-run"
```

3. **Check disk space:**
```bash
kubectl exec -it my-cluster-0 -- df -h /var/lib/postgresql
```

### Performance Issues

1. **Increase concurrency:**
```yaml
backup:
  uploadConcurrency: 32   # More parallel uploads
  downloadConcurrency: 20 # More parallel downloads during restore
```

2. **Use faster compression:**
```yaml
backup:
  compression: lz4  # Fastest, good compression
  # compression: none  # No compression, fastest but largest
```

3. **Backup from replica:**
```yaml
backup:
  backupFromReplica: true  # Reduces primary load
```

## Best Practices

1. **Test restores regularly**: Schedule periodic restore tests to verify backups work
2. **Monitor backup status**: Set up alerts for backup failures or WAL archiving lag
3. **Use encryption**: Always enable encryption for sensitive data
4. **Geographic redundancy**: Consider cross-region bucket replication
5. **Retention policy**: Balance storage costs with recovery requirements
6. **Document recovery procedures**: Have runbooks for common restore scenarios
