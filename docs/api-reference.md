# API Reference

This document provides a complete reference for the PostgresCluster custom resource.

## PostgresCluster

The `PostgresCluster` resource defines a PostgreSQL cluster managed by the operator.

```yaml
apiVersion: postgres.example.com/v1alpha1
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
| `version` | string | Yes | - | PostgreSQL major version (e.g., "15", "16") |
| `replicas` | integer | No | 1 | Number of cluster members (1-100) |
| `storage` | [StorageSpec](#storagespec) | Yes | - | Storage configuration |
| `resources` | [ResourceRequirements](#resourcerequirements) | No | - | CPU/memory resources |
| `postgresqlParams` | map[string]string | No | - | PostgreSQL configuration parameters |
| `service` | [ServiceSpec](#servicespec) | No | - | Service configuration |
| `pgbouncer` | [PgBouncerSpec](#pgbouncerspec) | No | - | Connection pooling configuration |
| `tls` | [TLSSpec](#tlsspec) | No | - | TLS configuration |
| `metrics` | [MetricsSpec](#metricsspec) | No | - | Metrics exporter configuration |
| `backup` | [BackupSpec](#backupspec) | No | - | Backup configuration |

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

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | No | false | Enable TLS encryption |
| `certSecret` | string | No | - | Secret containing tls.crt and tls.key |
| `caSecret` | string | No | - | Secret containing CA certificate |
| `certificateFile` | string | No | tls.crt | Certificate filename in secret |
| `privateKeyFile` | string | No | tls.key | Private key filename in secret |
| `caFile` | string | No | ca.crt | CA certificate filename in secret |

**Example:**

```yaml
tls:
  enabled: true
  certSecret: my-cluster-tls
  caSecret: my-cluster-ca
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

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `schedule` | string | Conditional | - | Cron schedule (required if destination set) |
| `retention` | [RetentionSpec](#retentionspec) | No | - | Backup retention policy |
| `destination` | [BackupDestination](#backupdestination) | No | - | Backup storage destination |

### RetentionSpec

| Field | Type | Description |
|-------|------|-------------|
| `count` | integer | Number of backups to retain (min: 1) |
| `maxAge` | string | Maximum age of backups (e.g., "7d", "30d") |

### BackupDestination

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Destination type: S3, GCS, Azure |
| `bucket` | string | S3/GCS | Bucket name |
| `region` | string | S3 | AWS region |
| `endpoint` | string | No | Custom S3-compatible endpoint |
| `container` | string | Azure | Azure blob container name |
| `storageAccount` | string | Azure | Azure storage account name |
| `credentialsSecret` | string | Yes | Secret with cloud credentials |

**Example (S3):**

```yaml
backup:
  schedule: "0 2 * * *"
  retention:
    count: 7
  destination:
    type: S3
    bucket: my-postgres-backups
    region: us-east-1
    credentialsSecret: aws-backup-credentials
```

**Example (GCS):**

```yaml
backup:
  schedule: "0 2 * * *"
  retention:
    maxAge: "30d"
  destination:
    type: GCS
    bucket: my-postgres-backups
    credentialsSecret: gcs-backup-credentials
```

**Example (Azure):**

```yaml
backup:
  schedule: "0 2 * * *"
  retention:
    count: 14
  destination:
    type: Azure
    container: postgres-backups
    storageAccount: mystorageaccount
    credentialsSecret: azure-backup-credentials
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
| `lastBackup` | string | Timestamp of last successful backup |
| `currentVersion` | string | Running PostgreSQL version |
| `tlsEnabled` | boolean | Whether TLS is currently enabled |
| `pgbouncerEnabled` | boolean | Whether PgBouncer is currently enabled |
| `pgbouncerReadyReplicas` | integer | Number of ready PgBouncer pods |
| `retryCount` | integer | Current retry count (for backoff) |
| `lastError` | string | Last error message |
| `lastErrorTime` | string | Timestamp of last error |
| `phaseStartedAt` | string | When current phase started |
| `previousReplicas` | integer | Previous replica count |

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

## Full Example

```yaml
apiVersion: postgres.example.com/v1alpha1
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
    enabled: true
    certSecret: production-db-tls

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
```

## Validation Rules

The CRD includes CEL validation rules:

1. **Version format**: Must match `^[0-9]{1,2}(\.[0-9]+)?$`
2. **Storage size**: Must be valid Kubernetes quantity
3. **Replicas**: Must be between 1 and 100
4. **Backup schedule**: Required if destination is configured
5. **S3 destination**: Requires bucket, region, credentialsSecret
6. **GCS destination**: Requires bucket, credentialsSecret
7. **Azure destination**: Requires container, storageAccount, credentialsSecret
8. **NodePort**: Only valid when service type is NodePort
9. **LoadBalancer ranges**: Only valid when service type is LoadBalancer

## Labels and Annotations

### Standard Labels

The operator applies these labels to all managed resources:

| Label | Description |
|-------|-------------|
| `app.kubernetes.io/name` | Always "postgresql" |
| `app.kubernetes.io/instance` | Cluster name |
| `app.kubernetes.io/component` | Component name (postgresql, pgbouncer) |
| `app.kubernetes.io/managed-by` | "postgres-operator" |
| `postgres.example.com/cluster` | Cluster name |

### Patroni Labels

Patroni manages these labels on pods:

| Label | Description |
|-------|-------------|
| `application` | "spilo" |
| `spilo-role` | "master" or "replica" |
