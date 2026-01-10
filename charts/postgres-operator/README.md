# PostgreSQL Operator Helm Chart

A Helm chart for deploying the PostgreSQL Operator, which manages PostgreSQL clusters with high availability using Patroni.

## Prerequisites

- Kubernetes 1.35+
- Helm 3.8+
- [cert-manager](https://cert-manager.io/) v1.0+ (required for TLS)

### Installing cert-manager

TLS is enabled by default for all PostgreSQL clusters for security. This requires cert-manager to be installed:

```bash
# Add the Jetstack Helm repository
helm repo add jetstack https://charts.jetstack.io
helm repo update

# Install cert-manager with CRDs
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true

# Verify cert-manager is running
kubectl get pods -n cert-manager
```

After installing cert-manager, create a ClusterIssuer or Issuer for your environment. See [TLS Configuration](#tls-configuration-spectls) below.

## Installation

### Add the Helm repository (if published)

```bash
helm repo add postgres-operator https://smoketurner.github.io/k8s-postgres-operator
helm repo update
```

### Install from local chart

```bash
helm install postgres-operator ./charts/postgres-operator \
  --namespace postgres-operator-system \
  --create-namespace
```

### Install with custom values

```bash
helm install postgres-operator ./charts/postgres-operator \
  --namespace postgres-operator-system \
  --create-namespace \
  --set image.tag=v0.1.0 \
  --set resources.limits.memory=512Mi
```

## Uninstallation

```bash
helm uninstall postgres-operator -n postgres-operator-system
```

Note: By default, the CRD is retained to prevent accidental data loss. To remove it:

```bash
kubectl delete crd postgresclusters.postgres-operator.smoketurner.com
```

## Configuration

### Operator Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.repository` | Operator image repository | `ghcr.io/example/postgres-operator` |
| `image.tag` | Operator image tag | `""` (uses appVersion) |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `imagePullSecrets` | Image pull secrets | `[]` |
| `replicaCount` | Number of operator replicas | `1` |
| `operator.logLevel` | Log level (trace, debug, info, warn, error) | `info` |
| `operator.healthPort` | Health server port | `8080` |

### RBAC Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create service account | `true` |
| `serviceAccount.annotations` | Service account annotations | `{}` |
| `serviceAccount.name` | Service account name | `""` (generated) |
| `rbac.create` | Create RBAC resources | `true` |

### CRD Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `crd.install` | Install the PostgresCluster CRD | `true` |
| `crd.keep` | Keep CRD on uninstall | `true` |

### Resource Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.limits.cpu` | CPU limit | `500m` |
| `resources.limits.memory` | Memory limit | `256Mi` |
| `resources.requests.cpu` | CPU request | `100m` |
| `resources.requests.memory` | Memory request | `128Mi` |

### Pod Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `affinity` | Affinity rules | `{}` |
| `podAnnotations` | Pod annotations | `{}` |
| `podLabels` | Additional pod labels | `{}` |
| `priorityClassName` | Priority class name | `""` |

### Security Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `podSecurityContext.runAsNonRoot` | Run as non-root | `true` |
| `podSecurityContext.runAsUser` | Run as user | `1000` |
| `podSecurityContext.fsGroup` | FS group | `1000` |
| `securityContext.allowPrivilegeEscalation` | Allow privilege escalation | `false` |
| `securityContext.readOnlyRootFilesystem` | Read-only root filesystem | `true` |

### Monitoring Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceMonitor.enabled` | Enable Prometheus ServiceMonitor | `false` |
| `serviceMonitor.labels` | Additional ServiceMonitor labels | `{}` |
| `serviceMonitor.interval` | Scrape interval | `30s` |
| `serviceMonitor.scrapeTimeout` | Scrape timeout | `10s` |

## PostgresCluster CRD Reference

Once the operator is installed, you can create PostgresCluster resources. Below is a summary of the available configuration options.

### Basic Configuration

| Field | Description | Required |
|-------|-------------|----------|
| `spec.version` | PostgreSQL version (e.g., "15", "16") | Yes |
| `spec.replicas` | Number of Patroni members (1 = standalone, 2+ = HA) | No (default: 1) |
| `spec.storage.size` | PVC size (e.g., "10Gi") | Yes |
| `spec.storage.storageClass` | Storage class name | No |
| `spec.resources` | CPU/memory requests and limits | No |
| `spec.postgresqlParams` | Custom postgresql.conf parameters | No |
| `spec.labels` | Custom labels for cost allocation (team, cost-center, etc.) | No |

### Backup Configuration (`spec.backup`)

**Important:** Encryption is **required** for all backups to protect sensitive data at rest.

| Field | Description |
|-------|-------------|
| `schedule` | Cron schedule for base backups (e.g., "0 2 * * *") |
| `retention.count` | Number of backups to retain |
| `retention.maxAge` | Maximum backup age (e.g., "30d") |
| `destination.type` | Storage type: S3, GCS, or Azure |
| `destination.bucket` | Bucket name (S3/GCS) |
| `destination.region` | AWS region (S3) |
| `destination.container` | Container name (Azure) |
| `destination.storageAccount` | Storage account (Azure) |
| `destination.credentialsSecret` | Secret with cloud credentials |
| `destination.path` | Path prefix within bucket |
| `destination.endpoint` | Custom S3-compatible endpoint |
| `destination.forcePathStyle` | Force path-style URLs for S3 |
| `destination.disableSse` | Disable S3 server-side encryption |
| `walArchiving.enabled` | Enable WAL archiving (default: true) |
| `walArchiving.restoreTimeout` | WAL restore timeout in seconds |
| `encryption.method` | Encryption method: aes256 or pgp (default: aes256) |
| `encryption.keySecret` | **Required**: Secret containing encryption key |
| `compression` | Compression: lz4, lzma, brotli, zstd, none |
| `backupFromReplica` | Take backups from replica |
| `uploadConcurrency` | WAL-G upload concurrency (default: 16) |
| `downloadConcurrency` | WAL-G download concurrency (default: 10) |
| `enableDeltaBackups` | Enable delta backups |
| `deltaMaxSteps` | Max delta steps before full backup |

### Restore Configuration (`spec.restore`)

Bootstrap a new cluster from an existing backup:

| Field | Description |
|-------|-------------|
| `source.s3.prefix` | S3 backup path (e.g., "s3://bucket/path") |
| `source.s3.region` | AWS region |
| `source.s3.credentialsSecret` | Secret with AWS credentials |
| `source.s3.endpoint` | Custom S3 endpoint |
| `source.gcs.prefix` | GCS backup path |
| `source.gcs.credentialsSecret` | Secret with GCS credentials |
| `source.azure.prefix` | Azure backup path |
| `source.azure.storageAccount` | Azure storage account |
| `source.azure.credentialsSecret` | Secret with Azure credentials |
| `recoveryTarget.time` | Point-in-time recovery timestamp |
| `recoveryTarget.backup` | Restore to specific backup name |
| `recoveryTarget.timeline` | Restore to specific timeline |

### PgBouncer Configuration (`spec.pgbouncer`)

| Field | Description |
|-------|-------------|
| `enabled` | Enable PgBouncer connection pooler |
| `replicas` | Number of PgBouncer pods (default: 2) |
| `poolMode` | Pool mode: session, transaction, statement |
| `maxDbConnections` | Max database connections (default: 60) |
| `defaultPoolSize` | Default pool size (default: 20) |
| `maxClientConn` | Max client connections (default: 10000) |
| `image` | Custom PgBouncer image |
| `resources` | CPU/memory for PgBouncer pods |
| `enableReplicaPooler` | Enable pooler for read replicas |

### TLS Configuration (`spec.tls`)

TLS is **enabled by default** for security. The operator integrates with cert-manager to automatically provision and renew certificates.

| Field | Description | Default |
|-------|-------------|---------|
| `enabled` | Enable TLS for PostgreSQL connections | `true` |
| `issuerRef.name` | Name of the cert-manager Issuer or ClusterIssuer | (required when TLS enabled) |
| `issuerRef.kind` | `Issuer` or `ClusterIssuer` | `ClusterIssuer` |
| `issuerRef.group` | Issuer API group | `cert-manager.io` |
| `additionalDnsNames` | Additional DNS names for the certificate | `[]` |
| `duration` | Certificate validity duration (e.g., "2160h" for 90 days) | cert-manager default |
| `renewBefore` | Renew certificate before expiry (e.g., "360h" for 15 days) | cert-manager default |

**Setting up TLS:**

1. Create a ClusterIssuer (self-signed for development):

```yaml
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-issuer
spec:
  selfSigned: {}
```

2. For production, use Let's Encrypt or your organization's PKI:

```yaml
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: admin@example.com
    privateKeySecretRef:
      name: letsencrypt-prod-key
    solvers:
    - http01:
        ingress:
          class: nginx
```

3. Reference the issuer in your PostgresCluster:

```yaml
spec:
  tls:
    issuerRef:
      name: selfsigned-issuer
      kind: ClusterIssuer
```

**Disabling TLS (not recommended):**

```yaml
spec:
  tls:
    enabled: false
```

### Service Configuration (`spec.service`)

| Field | Description |
|-------|-------------|
| `type` | ClusterIP, NodePort, or LoadBalancer |
| `annotations` | Service annotations |
| `loadBalancerSourceRanges` | CIDR ranges for LoadBalancer |
| `externalTrafficPolicy` | Cluster or Local |
| `nodePort` | NodePort number (30000-32767) |

### Metrics Configuration (`spec.metrics`)

| Field | Description |
|-------|-------------|
| `enabled` | Enable Prometheus metrics exporter |
| `port` | Metrics port (default: 9187) |

## Examples

### Production deployment with monitoring

```yaml
# values-production.yaml
replicaCount: 1

resources:
  limits:
    cpu: 1
    memory: 512Mi
  requests:
    cpu: 200m
    memory: 256Mi

serviceMonitor:
  enabled: true
  labels:
    release: prometheus

affinity:
  nodeAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        preference:
          matchExpressions:
            - key: node-role.kubernetes.io/control-plane
              operator: DoesNotExist
```

```bash
helm install postgres-operator ./charts/postgres-operator \
  -f values-production.yaml \
  --namespace postgres-operator-system \
  --create-namespace
```

### Using a private registry

```yaml
# values-private-registry.yaml
image:
  repository: my-registry.example.com/postgres-operator
  pullPolicy: Always

imagePullSecrets:
  - name: my-registry-secret
```

### PostgresCluster with S3 backups

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresCluster
metadata:
  name: production-db
spec:
  version: "16"
  replicas: 3
  storage:
    size: 100Gi
    storageClass: fast-ssd
  resources:
    requests:
      cpu: "2"
      memory: 8Gi
    limits:
      cpu: "4"
      memory: 16Gi
  backup:
    schedule: "0 2 * * *"  # Daily at 2 AM
    retention:
      count: 7
      maxAge: "30d"
    destination:
      type: S3
      bucket: my-postgres-backups
      region: us-east-1
      path: production/main-db
      credentialsSecret: aws-backup-credentials
    compression: zstd
    encryption:
      method: aes256
      keySecret: backup-encryption-key
    backupFromReplica: true
```

### Restore from backup (point-in-time recovery)

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresCluster
metadata:
  name: restored-db
spec:
  version: "16"
  replicas: 3
  storage:
    size: 100Gi
  restore:
    source:
      s3:
        prefix: s3://my-postgres-backups/production/main-db
        region: us-east-1
        credentialsSecret: aws-backup-credentials
    recoveryTarget:
      time: "2024-01-15T14:30:00Z"  # Restore to this point in time
```

### PostgresCluster with PgBouncer and TLS

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresCluster
metadata:
  name: secure-db
spec:
  version: "16"
  replicas: 3
  storage:
    size: 50Gi
  pgbouncer:
    enabled: true
    replicas: 2
    poolMode: transaction
    maxDbConnections: 100
  tls:
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer
  service:
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-internal: "true"
```

## Upgrading

### From 0.x to 0.y

```bash
helm upgrade postgres-operator ./charts/postgres-operator \
  --namespace postgres-operator-system \
  --reuse-values
```

## Troubleshooting

### Operator not starting

Check the operator logs:

```bash
kubectl logs -n postgres-operator-system deploy/postgres-operator
```

### CRD conflicts

If upgrading and the CRD already exists:

```bash
kubectl apply -f charts/postgres-operator/templates/crd.yaml
```

### Leader election issues

With multiple replicas, only one operator is active. Check leader election:

```bash
kubectl get lease -n postgres-operator-system postgres-operator-leader
```
