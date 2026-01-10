# PostgreSQL Operator Helm Chart

A Helm chart for deploying the PostgreSQL Operator, which manages PostgreSQL clusters with high availability using Patroni.

## Prerequisites

- Kubernetes 1.26+
- Helm 3.8+

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
kubectl delete crd postgresclusters.postgres.example.com
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
