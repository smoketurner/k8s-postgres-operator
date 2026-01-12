# API Reference

This document provides a complete reference for the operator's custom resources:
- [PostgresCluster](#postgrescluster) - Manages PostgreSQL clusters with high availability
- [PostgresDatabase](#postgresdatabase) - Declarative database and role provisioning

## PostgresCluster

The `PostgresCluster` resource defines a PostgreSQL cluster managed by the operator.

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresCluster
metadata:
  name: my-cluster
spec:
  # ... spec fields
status:
  # ... status fields (read-only)
```

## Spec

### Core Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `version` | string | Yes | - | PostgreSQL major version ("15", "16", or "17") |
| `replicas` | integer | No | 1 | Number of cluster members (1-100) |
| `storage` | [StorageSpec](#storagespec) | Yes | - | Storage configuration |
| `resources` | [ResourceRequirements](#resourcerequirements) | No | - | CPU/memory resources |
| `postgresqlParams` | map[string]string | No | - | PostgreSQL configuration parameters |
| `labels` | map[string]string | No | - | Custom labels for cost allocation (team, cost-center, etc.) |
| `service` | [ServiceSpec](#servicespec) | No | - | Service configuration |
| `pgbouncer` | [PgBouncerSpec](#pgbouncerspec) | No | - | Connection pooling configuration |
| `tls` | [TLSSpec](#tlsspec) | No | enabled=true | TLS configuration (enabled by default) |
| `metrics` | [MetricsSpec](#metricsspec) | No | - | Metrics exporter configuration |
| `backup` | [BackupSpec](#backupspec) | No | - | Backup configuration (encryption required) |
| `scaling` | [ScalingSpec](#scalingspec) | No | - | KEDA auto-scaling configuration |
| `networkPolicy` | [NetworkPolicySpec](#networkpolicyspec) | No | - | Network policy configuration |
| `restore` | [RestoreSpec](#restorespec) | No | - | Restore from backup configuration |

### StorageSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `size` | string | Yes | - | PVC size (e.g., "10Gi", "100Gi") |
| `storageClass` | string | No | cluster default | Kubernetes StorageClass name |

**Example:**

```yaml
storage:
  size: 100Gi
  storageClass: fast-ssd
```

### ResourceRequirements

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `limits` | [ResourceList](#resourcelist) | No | Maximum resources |
| `requests` | [ResourceList](#resourcelist) | No | Guaranteed resources |

### ResourceList

| Field | Type | Description |
|-------|------|-------------|
| `cpu` | string | CPU quantity (e.g., "500m", "2") |
| `memory` | string | Memory quantity (e.g., "1Gi", "4Gi") |

**Example:**

```yaml
resources:
  requests:
    cpu: "1"
    memory: 2Gi
  limits:
    cpu: "2"
    memory: 4Gi
```

### ServiceSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | No | ClusterIP | Service type: ClusterIP, NodePort, LoadBalancer |
| `annotations` | map[string]string | No | - | Service annotations |
| `loadBalancerSourceRanges` | []string | No | - | CIDR ranges for LoadBalancer access |
| `externalTrafficPolicy` | string | No | - | Cluster or Local (NodePort/LoadBalancer only) |
| `nodePort` | integer | No | - | NodePort number (30000-32767, NodePort only) |

**Example:**

```yaml
service:
  type: LoadBalancer
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-internal: "true"
  loadBalancerSourceRanges:
    - 10.0.0.0/8
    - 192.168.0.0/16
```

### PgBouncerSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | No | false | Enable PgBouncer connection pooler |
| `replicas` | integer | No | 2 | Number of PgBouncer pods (1-10) |
| `poolMode` | string | No | transaction | Pool mode: session, transaction, statement |
| `maxDbConnections` | integer | No | 60 | Max connections to PostgreSQL (1-10000) |
| `defaultPoolSize` | integer | No | 20 | Default pool size per user/db (1-1000) |
| `maxClientConn` | integer | No | 10000 | Max client connections per instance (10-100000) |
| `image` | string | No | bitnami/pgbouncer | PgBouncer container image |
| `resources` | [ResourceRequirements](#resourcerequirements) | No | - | PgBouncer resources |
| `enableReplicaPooler` | boolean | No | false | Separate pooler for read replicas |

**Example:**

```yaml
pgbouncer:
  enabled: true
  replicas: 2
  poolMode: transaction
  maxDbConnections: 100
  defaultPoolSize: 25
  maxClientConn: 5000
  enableReplicaPooler: true
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 256Mi
```

### TLSSpec

TLS is **enabled by default** for security. The operator integrates with [cert-manager](https://cert-manager.io/) to automatically provision and renew certificates.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | No | true | Enable TLS encryption |
| `issuerRef` | [IssuerRef](#issuerref) | When TLS enabled | - | cert-manager Issuer reference |
| `additionalDnsNames` | []string | No | - | Additional DNS names for certificate |
| `duration` | string | No | - | Certificate validity (e.g., "2160h" for 90 days) |
| `renewBefore` | string | No | - | Renew before expiry (e.g., "360h" for 15 days) |

### IssuerRef

Reference to a cert-manager Issuer or ClusterIssuer.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | - | Issuer name |
| `kind` | string | No | ClusterIssuer | Issuer or ClusterIssuer |
| `group` | string | No | cert-manager.io | Issuer API group |

**Example (using ClusterIssuer):**

```yaml
tls:
  issuerRef:
    name: letsencrypt-prod
    kind: ClusterIssuer
```

**Example (using namespace-scoped Issuer):**

```yaml
tls:
  issuerRef:
    name: my-issuer
    kind: Issuer
```

**Example (with additional DNS names and custom duration):**

```yaml
tls:
  issuerRef:
    name: letsencrypt-prod
    kind: ClusterIssuer
  additionalDnsNames:
    - postgres.example.com
    - db.example.com
  duration: "2160h"    # 90 days
  renewBefore: "360h"  # 15 days
```

**Disabling TLS (not recommended):**

```yaml
tls:
  enabled: false
```

### MetricsSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | No | false | Enable Prometheus metrics exporter |
| `port` | integer | No | 9187 | Metrics exporter port |

**Example:**

```yaml
metrics:
  enabled: true
  port: 9187
```

### BackupSpec

**Important:** Encryption is **required** for all backups to protect sensitive data at rest.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `schedule` | string | Conditional | - | Cron schedule (required if destination set) |
| `retention` | [RetentionSpec](#retentionspec) | No | - | Backup retention policy |
| `destination` | [BackupDestination](#backupdestination) | No | - | Backup storage destination |
| `encryption` | [EncryptionSpec](#encryptionspec) | Yes | - | Backup encryption (required) |
| `compression` | string | No | - | Compression: lz4, lzma, brotli, zstd, none |
| `walArchiving` | [WALArchivingSpec](#walarchivingspec) | No | enabled=true | WAL archiving settings |
| `backupFromReplica` | boolean | No | false | Take backups from replica |
| `uploadConcurrency` | integer | No | 16 | WAL-G upload concurrency |
| `downloadConcurrency` | integer | No | 10 | WAL-G download concurrency |
| `enableDeltaBackups` | boolean | No | false | Enable delta backups |
| `deltaMaxSteps` | integer | No | - | Max delta steps before full backup |

### EncryptionSpec

Encryption configuration for backups. This is **required** when backups are configured.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `method` | string | No | aes256 | Encryption method: aes256 or pgp |
| `keySecret` | string | Yes | - | Secret containing encryption key |

**Secret format for AES-256:**

```bash
kubectl create secret generic backup-encryption-key \
  --from-literal=WALG_LIBSODIUM_KEY=$(openssl rand -hex 32)
```

**Secret format for PGP:**

```bash
kubectl create secret generic backup-encryption-key \
  --from-file=WALG_PGP_KEY=/path/to/private.key
```

### WALArchivingSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | No | true | Enable WAL archiving |
| `restoreTimeout` | integer | No | - | WAL restore timeout in seconds |

### RetentionSpec

| Field | Type | Description |
|-------|------|-------------|
| `count` | integer | Number of backups to retain (min: 1) |
| `maxAge` | string | Maximum age of backups (e.g., "7d", "30d") |

### BackupDestination

Supports S3 and S3-compatible storage (e.g., MinIO, DigitalOcean Spaces).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bucket` | string | Yes | S3 bucket name |
| `region` | string | Yes | AWS region (e.g., "us-east-1") |
| `endpoint` | string | No | Custom S3-compatible endpoint URL |
| `credentialsSecret` | string | Yes | Secret with AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) |
| `path` | string | No | Custom path prefix within bucket (default: namespace/cluster-name) |
| `forcePathStyle` | boolean | No | Force path-style URLs (required for some S3-compatible storage) |

**Example (AWS S3):**

```yaml
backup:
  schedule: "0 2 * * *"
  retention:
    count: 7
  destination:
    S3:
      bucket: my-postgres-backups
      region: us-east-1
      credentialsSecret: aws-backup-credentials
  encryption:
    method: aes256
    keySecret: backup-encryption-key
```

**Example (MinIO / S3-compatible):**

```yaml
backup:
  schedule: "0 2 * * *"
  retention:
    count: 7
  destination:
    S3:
      bucket: postgres-backups
      region: us-east-1
      endpoint: "https://minio.example.com:9000"
      credentialsSecret: minio-credentials
      forcePathStyle: true
  encryption:
    method: aes256
    keySecret: backup-encryption-key
```

### ScalingSpec

KEDA-based auto-scaling configuration. Requires [KEDA](https://keda.sh/) to be installed in the cluster.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | No | false | Enable auto-scaling |
| `minReplicas` | integer | No | 1 | Minimum replica count |
| `maxReplicas` | integer | No | 10 | Maximum replica count |
| `cpuTargetUtilization` | integer | No | - | CPU utilization target (percentage) |
| `connectionThreshold` | integer | No | - | Connection count threshold for scaling |
| `scaleDownStabilization` | integer | No | 300 | Seconds to wait before scaling down |
| `pollingInterval` | integer | No | 30 | Seconds between metric checks |
| `cooldownPeriod` | integer | No | 300 | Cooldown after scaling event |

**Example (CPU-based scaling):**

```yaml
scaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  cpuTargetUtilization: 70
  scaleDownStabilization: 600
```

**Example (Connection-based scaling):**

```yaml
scaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 15
  connectionThreshold: 100
  pollingInterval: 15
```

### NetworkPolicySpec

Network policy configuration for cluster access control.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | No | false | Enable NetworkPolicy generation |
| `allowFromNamespaces` | []string | No | - | Namespaces allowed to access the cluster |
| `allowFromPodSelector` | map[string]string | No | - | Pod labels allowed to access |
| `allowExternalCidrs` | []string | No | - | External CIDR ranges allowed |
| `denyEgress` | boolean | No | false | Deny all egress except DNS |

**Example:**

```yaml
networkPolicy:
  enabled: true
  allowFromNamespaces:
    - app-namespace
    - monitoring
  allowExternalCidrs:
    - 10.0.0.0/8
```

### RestoreSpec

Configuration for restoring from a backup. Used for point-in-time recovery (PITR) or creating a new cluster from an existing backup.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `source` | [RestoreSource](#restoresource) | Yes | - | Backup source configuration |
| `target` | [RestoreTarget](#restoretarget) | No | - | Recovery target (time, backup name, etc.) |

### RestoreSource

Supports restoring from S3 and S3-compatible storage.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `prefix` | string | Yes | Full S3 prefix path (e.g., "s3://bucket/path/to/backup") |
| `region` | string | Yes | AWS region |
| `endpoint` | string | No | Custom S3-compatible endpoint |
| `credentialsSecret` | string | Yes | Secret with cloud credentials |

### RestoreTarget

| Field | Type | Description |
|-------|------|-------------|
| `time` | string | Point-in-time target (RFC3339 timestamp) |
| `backupName` | string | Specific backup name to restore |
| `timeline` | integer | PostgreSQL timeline to restore to |

**Example (point-in-time recovery):**

```yaml
restore:
  source:
    s3:
      prefix: s3://my-postgres-backups/prod/source-cluster
      region: us-east-1
      credentialsSecret: aws-backup-credentials
  recoveryTarget:
    time: "2024-01-15T10:30:00Z"
```

**Example (restore from specific backup):**

```yaml
restore:
  source:
    s3:
      prefix: s3://my-postgres-backups/prod/source-cluster
      region: us-east-1
      credentialsSecret: aws-backup-credentials
  recoveryTarget:
    backup: base_000000010000000000000005
```

## Status

The status subresource is read-only and managed by the operator.

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | Current cluster phase |
| `readyReplicas` | integer | Number of ready pods |
| `replicas` | integer | Desired replica count |
| `primaryPod` | string | Name of the current primary pod |
| `replicaPods` | []string | Names of replica pods |
| `conditions` | [][Condition](#condition) | Kubernetes-style conditions |
| `observedGeneration` | integer | Last observed spec generation |
| `backup` | [BackupStatus](#backupstatus) | Backup status information |
| `currentVersion` | string | Running PostgreSQL version |
| `tlsEnabled` | boolean | Whether TLS is currently enabled |
| `pgbouncerEnabled` | boolean | Whether PgBouncer is currently enabled |
| `pgbouncerReadyReplicas` | integer | Number of ready PgBouncer pods |
| `retryCount` | integer | Current retry count (for backoff) |
| `lastError` | string | Last error message |
| `lastErrorTime` | string | Timestamp of last error |
| `phaseStartedAt` | string | When current phase started |
| `previousReplicas` | integer | Previous replica count |
| `pods` | [][PodInfo](#podinfo) | Per-pod tracking info (K8s 1.35+) |
| `resizeStatus` | [][PodResourceResizeStatus](#podresourceresizestatus) | Resource resize status (K8s 1.35+) |
| `allPodsSynced` | boolean | True when all pods have applied current spec (K8s 1.35+) |
| `restoredFrom` | [RestoredFromInfo](#restoredfrominfo) | Source backup info if restored |
| `replicationLag` | [][ReplicaLagInfo](#replicalaginfo) | Per-replica replication lag |
| `maxReplicationLagBytes` | integer | Maximum lag across all replicas (bytes) |
| `replicasLagging` | boolean | Whether any replica exceeds lag threshold |
| `connectionInfo` | [ConnectionInfo](#connectioninfo) | Connection endpoints for applications |

### Phase Values

| Phase | Description |
|-------|-------------|
| Pending | Initial state, no resources created |
| Creating | Resources being created |
| Running | Cluster healthy, all replicas ready |
| Updating | Configuration change in progress |
| Scaling | Replica count changing |
| Degraded | Some replicas unavailable |
| Recovering | Automatic recovery in progress |
| Failed | Requires manual intervention |
| Deleting | Deletion in progress |

### Condition

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Condition type |
| `status` | string | True, False, or Unknown |
| `reason` | string | Machine-readable reason |
| `message` | string | Human-readable message |
| `lastTransitionTime` | string | Last status change timestamp |
| `observedGeneration` | integer | Generation when condition set |

### Condition Types

| Type | Description |
|------|-------------|
| Ready | Cluster is ready for connections |
| Progressing | Moving toward desired state |
| Degraded | Running but with issues |
| ConfigurationValid | Spec passes validation |
| ReplicasReady | All replicas synchronized |
| PodGenerationSynced | All pods have applied their spec (K8s 1.35+) |
| ResourceResizeInProgress | In-place resource resize in progress (K8s 1.35+) |

### PodInfo

Per-pod tracking information (Kubernetes 1.35+).

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Pod name |
| `generation` | integer | Pod metadata.generation |
| `observed_generation` | integer | Pod status.observedGeneration (when kubelet processed spec) |
| `spec_applied` | boolean | True when observedGeneration >= generation |

### PodResourceResizeStatus

Per-pod resource resize status (Kubernetes 1.35+).

| Field | Type | Description |
|-------|------|-------------|
| `pod_name` | string | Pod name |
| `status` | string | Resize status: NotInProgress, Proposed, InProgress, Infeasible |
| `allocated_cpu` | string | Currently allocated CPU |
| `allocated_memory` | string | Currently allocated memory |

**Resize Status Values:**

| Status | Description |
|--------|-------------|
| NotInProgress | No resize pending |
| Proposed | Resize requested, waiting for kubelet |
| InProgress | Kubelet is applying the resize |
| Infeasible | Cannot resize (insufficient node resources) |

### BackupStatus

Backup status information.

| Field | Type | Description |
|-------|------|-------------|
| `lastBackupTime` | string | Timestamp of last successful backup (RFC3339) |
| `lastBackupSizeBytes` | integer | Size of last backup in bytes |
| `lastBackupName` | string | Name/identifier of last backup |

### RestoredFromInfo

Information about the backup a cluster was restored from.

| Field | Type | Description |
|-------|------|-------------|
| `backupName` | string | Name of the backup restored from |
| `sourceCluster` | string | Name of the source cluster |
| `restoreTime` | string | Time of restore completion (RFC3339) |
| `pitrTarget` | string | Point-in-time recovery target if used |

### ReplicaLagInfo

Replication lag information for a single replica.

| Field | Type | Description |
|-------|------|-------------|
| `podName` | string | Name of the replica pod |
| `lagBytes` | integer | Replication lag in bytes |
| `lagTime` | string | Replication lag as duration (e.g., "1.5s") |
| `exceedsThreshold` | boolean | Whether replica exceeds configured lag threshold |
| `lastMeasured` | string | Last measurement timestamp (RFC3339) |
| `state` | string | Replication state (streaming, catchup, etc.) |

### ConnectionInfo

Connection information for applications.

| Field | Type | Description |
|-------|------|-------------|
| `primary` | string | Primary (read-write) service endpoint |
| `replicas` | string | Replica (read-only) service endpoint |
| `pooler` | string | PgBouncer primary pooler endpoint (if enabled) |
| `poolerReplicas` | string | PgBouncer replica pooler endpoint (if enabled) |
| `credentialsSecret` | string | Name of Secret containing credentials |
| `database` | string | Default database name |

**Example connection info in status:**

```yaml
status:
  connectionInfo:
    primary: my-cluster-primary.default.svc:5432
    replicas: my-cluster-repl.default.svc:5432
    pooler: my-cluster-pooler.default.svc:6432
    credentialsSecret: my-cluster-credentials
    database: postgres
```

## Full Example

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresCluster
metadata:
  name: production-db
  namespace: databases
spec:
  version: "16"
  replicas: 3

  storage:
    size: 500Gi
    storageClass: fast-nvme

  resources:
    requests:
      cpu: "4"
      memory: 16Gi
    limits:
      cpu: "8"
      memory: 32Gi

  labels:
    team: platform
    cost-center: eng-001
    environment: production
    project: core-database

  postgresqlParams:
    max_connections: "1000"
    shared_buffers: "8GB"
    effective_cache_size: "24GB"
    maintenance_work_mem: "2GB"
    work_mem: "64MB"
    wal_buffers: "64MB"
    checkpoint_completion_target: "0.9"
    random_page_cost: "1.1"
    effective_io_concurrency: "200"
    min_wal_size: "2GB"
    max_wal_size: "8GB"

  service:
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-internal: "true"
      service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled: "true"
    loadBalancerSourceRanges:
      - 10.0.0.0/8

  pgbouncer:
    enabled: true
    replicas: 3
    poolMode: transaction
    maxDbConnections: 200
    defaultPoolSize: 50
    maxClientConn: 10000
    enableReplicaPooler: true
    resources:
      requests:
        cpu: 500m
        memory: 512Mi
      limits:
        cpu: "2"
        memory: 1Gi

  tls:
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer
    duration: "2160h"    # 90 days
    renewBefore: "360h"  # 15 days

  metrics:
    enabled: true
    port: 9187

  backup:
    schedule: "0 */6 * * *"
    retention:
      count: 28
    destination:
      type: S3
      bucket: prod-postgres-backups
      region: us-east-1
      credentialsSecret: aws-backup-credentials
    encryption:
      method: aes256
      keySecret: backup-encryption-key
    compression: zstd
    backupFromReplica: true
```

## Validation Rules

The CRD includes CEL validation rules:

1. **Version**: Must be one of "15", "16", or "17"
2. **Storage size**: Must be valid Kubernetes quantity
3. **Replicas**: Must be between 1 and 100
4. **Backup schedule**: Required if destination is configured
5. **Backup encryption**: Required if backup is configured
6. **TLS issuerRef**: Required if TLS is enabled (default)
7. **S3 destination**: Requires bucket, region, credentialsSecret
8. **NodePort**: Only valid when service type is NodePort
9. **LoadBalancer ranges**: Only valid when service type is LoadBalancer

## Labels and Annotations

### Standard Labels

The operator applies these labels to all managed resources:

| Label | Description |
|-------|-------------|
| `app.kubernetes.io/name` | Cluster name |
| `app.kubernetes.io/component` | Component name (postgresql, pgbouncer) |
| `app.kubernetes.io/managed-by` | "postgres-operator" |
| `postgres-operator.smoketurner.com/cluster` | Cluster name (protected, cannot be overridden) |

### User-Defined Labels

Use `spec.labels` to add custom labels for cost allocation and organization:

```yaml
spec:
  labels:
    team: platform
    cost-center: eng-001
    project: core-database
    environment: production
```

User-defined labels are merged with standard labels on all managed resources. The `postgres-operator.smoketurner.com/cluster` label cannot be overridden.

### Patroni Labels

Patroni manages these labels on pods:

| Label | Description |
|-------|-------------|
| `application` | "spilo" |
| `spilo-role` | "master" or "replica" |

---

## PostgresDatabase

The `PostgresDatabase` resource enables declarative database and role provisioning within a PostgresCluster.

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresDatabase
metadata:
  name: my-app-db
spec:
  clusterRef:
    name: my-cluster
  database:
    name: myapp
    owner: myapp_owner
  roles:
    - name: myapp_owner
      privileges: [LOGIN, CREATEDB]
      secretName: myapp-owner-credentials
    - name: myapp_readonly
      privileges: [LOGIN]
      secretName: myapp-readonly-credentials
  grants:
    - role: myapp_readonly
      database: myapp
      privileges: [SELECT]
      onTables: ALL
  extensions:
    - pg_stat_statements
    - uuid-ossp
```

### PostgresDatabaseSpec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `clusterRef` | [ClusterRef](#clusterref) | Yes | Reference to the parent PostgresCluster |
| `database` | [DatabaseSpec](#databasespec) | Yes | Database to create |
| `roles` | [][RoleSpec](#rolespec) | No | Roles to create with credentials |
| `grants` | [][GrantSpec](#grantspec) | No | Permission grants to apply |
| `extensions` | []string | No | PostgreSQL extensions to enable |

### ClusterRef

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Name of the PostgresCluster |
| `namespace` | string | No | Namespace (defaults to same namespace) |

### DatabaseSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | - | Database name |
| `owner` | string | Yes | - | Owner role for the database |
| `encoding` | string | No | UTF8 | Database encoding |
| `locale` | string | No | - | Database locale |
| `connectionLimit` | integer | No | -1 | Connection limit (-1 = unlimited) |

### RoleSpec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Role name |
| `privileges` | []string | No | Role privileges (LOGIN, CREATEDB, CREATEROLE, SUPERUSER) |
| `secretName` | string | No | Secret name for generated credentials |
| `connectionLimit` | integer | No | Connection limit for this role |
| `validUntil` | string | No | Password expiration (RFC3339) |

### GrantSpec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `role` | string | Yes | Role to grant privileges to |
| `database` | string | Yes | Target database |
| `privileges` | []string | Yes | Privileges to grant (SELECT, INSERT, UPDATE, DELETE, ALL) |
| `onTables` | string | No | Tables to grant on (ALL or specific table) |
| `onSchema` | string | No | Schema for grants (default: public) |

### PostgresDatabaseStatus

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | Current phase (Pending, Creating, Ready, Failed) |
| `conditions` | []Condition | Kubernetes-style conditions |
| `provisioned` | boolean | Whether database is provisioned |
| `secretName` | string | Name of generated credentials secret |

### Generated Secrets

For each role with `secretName` specified, the operator creates a Secret containing:

| Key | Description |
|-----|-------------|
| `username` | Role name |
| `password` | Generated password |
| `host` | Primary service hostname |
| `port` | PostgreSQL port (5432) |
| `database` | Database name |
| `uri` | Full connection URI |

**Example generated secret:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: myapp-owner-credentials
type: Opaque
stringData:
  username: myapp_owner
  password: <generated>
  host: my-cluster-primary.default.svc
  port: "5432"
  database: myapp
  uri: postgresql://myapp_owner:<password>@my-cluster-primary.default.svc:5432/myapp
```
