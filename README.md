# PostgreSQL Kubernetes Operator

A Kubernetes operator for managing PostgreSQL clusters with high availability using [Patroni](https://github.com/patroni/patroni).

## Features

- **High Availability**: Automatic failover using Patroni with Kubernetes-native leader election
- **Declarative Configuration**: Manage PostgreSQL clusters using Kubernetes custom resources
- **Automatic Scaling**: Scale replicas up or down with automatic replication setup
- **Connection Pooling**: Optional PgBouncer sidecar for connection pooling
- **TLS Support**: Encrypted connections with certificate management
- **Backup Configuration**: S3, GCS, and Azure blob storage backup destinations
- **Metrics**: Prometheus-compatible metrics endpoint
- **Zero-Downtime Updates**: Rolling updates with PodDisruptionBudgets

## Prerequisites

- Kubernetes 1.26+ (tested up to 1.35)
- kubectl configured to access your cluster
- Rust 1.92+ (for building from source)

## Quick Start

### Install the Operator

```bash
# Install CRD and RBAC
make install

# Deploy the operator
make deploy
```

### Create a PostgreSQL Cluster

```yaml
apiVersion: postgres.example.com/v1alpha1
kind: PostgresCluster
metadata:
  name: my-postgres
spec:
  version: "16"
  replicas: 3
  storage:
    size: 10Gi
    storageClass: standard
```

```bash
kubectl apply -f my-postgres.yaml
```

### Check Status

```bash
# List clusters
kubectl get postgresclusters

# Watch cluster status
kubectl get pgc my-postgres -w

# View detailed status
kubectl describe pgc my-postgres
```

## Configuration

### PostgresCluster Spec

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `version` | string | PostgreSQL version (e.g., "15", "16") | Required |
| `replicas` | integer | Number of cluster members (1-100) | 1 |
| `storage.size` | string | PVC size (e.g., "10Gi") | Required |
| `storage.storageClass` | string | Kubernetes StorageClass | cluster default |
| `resources` | object | CPU/memory requests and limits | none |
| `postgresqlParams` | map | Custom PostgreSQL parameters | none |
| `service.type` | string | Service type (ClusterIP, NodePort, LoadBalancer) | ClusterIP |
| `tls.enabled` | boolean | Enable TLS connections | false |
| `pgbouncer.enabled` | boolean | Enable PgBouncer sidecar | false |
| `metrics.enabled` | boolean | Enable metrics exporter | false |
| `backup` | object | Backup configuration | none |

### Replica Configurations

| Replicas | Description |
|----------|-------------|
| 1 | Single server (development) |
| 2 | Primary + 1 replica (no HA benefit) |
| 3+ | Highly available cluster with automatic failover |

### Example: Production HA Cluster

```yaml
apiVersion: postgres.example.com/v1alpha1
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
      memory: 4Gi
    limits:
      cpu: "4"
      memory: 8Gi
  postgresqlParams:
    max_connections: "200"
    shared_buffers: "1GB"
    effective_cache_size: "3GB"
  service:
    type: LoadBalancer
    loadBalancerSourceRanges:
      - 10.0.0.0/8
  tls:
    enabled: true
    certSecret: postgres-tls
  pgbouncer:
    enabled: true
    poolMode: transaction
    maxClientConn: 1000
  backup:
    schedule: "0 2 * * *"
    retention:
      count: 7
    destination:
      type: S3
      bucket: my-backups
      region: us-east-1
      credentialsSecret: aws-credentials
```

## Connecting to PostgreSQL

### Primary (Read-Write)

```bash
# Get the primary service
kubectl get svc my-postgres-primary

# Connect via psql
kubectl run psql --rm -it --image=postgres:16 -- \
  psql -h my-postgres-primary -U postgres
```

### Replicas (Read-Only)

```bash
# Connect to read replicas
kubectl run psql --rm -it --image=postgres:16 -- \
  psql -h my-postgres-repl -U postgres
```

### Credentials

Credentials are stored in a Kubernetes Secret:

```bash
# Get credentials
kubectl get secret my-postgres-credentials -o jsonpath='{.data.password}' | base64 -d
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                                                              │
│  ┌──────────────────┐    ┌─────────────────────────────┐   │
│  │ postgres-operator│    │     PostgresCluster CR       │   │
│  │                  │◄───│                              │   │
│  │  - Reconciler    │    │  spec:                       │   │
│  │  - State Machine │    │    version: "16"             │   │
│  │  - Leader Election│   │    replicas: 3               │   │
│  └────────┬─────────┘    └─────────────────────────────┘   │
│           │                                                  │
│           │ creates/manages                                  │
│           ▼                                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                   StatefulSet                        │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐             │   │
│  │  │ Pod-0   │  │ Pod-1   │  │ Pod-2   │             │   │
│  │  │(Primary)│  │(Replica)│  │(Replica)│             │   │
│  │  │         │  │         │  │         │             │   │
│  │  │ Patroni │  │ Patroni │  │ Patroni │             │   │
│  │  │ Postgres│  │ Postgres│  │ Postgres│             │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘             │   │
│  │       │            │            │                   │   │
│  │  ┌────▼────┐  ┌────▼────┐  ┌────▼────┐             │   │
│  │  │  PVC-0  │  │  PVC-1  │  │  PVC-2  │             │   │
│  │  └─────────┘  └─────────┘  └─────────┘             │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ svc/primary  │  │ svc/replicas │  │ svc/headless │      │
│  │ (read-write) │  │ (read-only)  │  │ (internal)   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## Cluster Lifecycle States

| Phase | Description |
|-------|-------------|
| Pending | Initial state before resources are created |
| Creating | Resources are being created, waiting for pods |
| Running | All replicas ready, cluster is healthy |
| Updating | Configuration change in progress |
| Scaling | Replica count change in progress |
| Degraded | Some replicas unavailable |
| Recovering | Automatic recovery in progress |
| Failed | Cluster needs manual intervention |
| Deleting | Cluster is being deleted |

## Development

### Build from Source

```bash
# Build the operator
make build

# Run locally (uses current kubeconfig)
make run

# Run tests
make test

# Run lints
make lint
```

### Project Structure

```
├── src/
│   ├── main.rs              # Entry point, leader election
│   ├── lib.rs               # Controller setup
│   ├── controller/
│   │   ├── reconciler.rs    # Main reconciliation loop
│   │   ├── state_machine.rs # Cluster lifecycle FSM
│   │   ├── status.rs        # Status management
│   │   └── error.rs         # Error types
│   ├── crd/
│   │   └── postgres_cluster.rs  # CRD types
│   ├── resources/
│   │   ├── patroni.rs       # StatefulSet, ConfigMap, RBAC
│   │   ├── service.rs       # Services
│   │   ├── secret.rs        # Credentials
│   │   └── pdb.rs           # PodDisruptionBudget
│   └── health.rs            # Health/metrics server
├── config/
│   ├── crd/                 # CustomResourceDefinition
│   ├── rbac/                # RBAC resources
│   ├── deploy/              # Operator deployment
│   └── samples/             # Example PostgresClusters
└── tests/
    ├── unit/                # Unit tests
    └── integration/         # Integration tests
```

### Makefile Targets

```bash
make build              # Build the operator
make docker-build       # Build Docker image
make docker-push        # Push Docker image
make run                # Run locally
make test               # Run unit tests
make test-integration   # Run integration tests
make install            # Install CRD and RBAC
make deploy             # Deploy operator
make undeploy           # Remove operator
make deploy-sample      # Deploy sample cluster
make clean              # Clean build artifacts
```

## Monitoring

### Prometheus Metrics

The operator exposes metrics at `:8080/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `postgres_operator_reconciliations` | Counter | Total reconciliations |
| `postgres_operator_reconciliation_errors` | Counter | Failed reconciliations |
| `postgres_operator_reconcile_duration_seconds` | Histogram | Reconciliation duration |

### Health Endpoints

| Endpoint | Description |
|----------|-------------|
| `/healthz` | Liveness probe |
| `/readyz` | Readiness probe |
| `/metrics` | Prometheus metrics |

## Security

### Pod Security Standards

The operator runs with the `restricted` Pod Security Standard:
- Non-root user (UID 1000)
- Read-only root filesystem
- No privilege escalation
- All capabilities dropped

### Network Policies

Sample NetworkPolicies are provided in `config/samples/networkpolicy-postgresql.yaml` to:
- Restrict PostgreSQL access to labeled client pods
- Allow Patroni cluster communication
- Allow Prometheus metrics scraping

### RBAC

The operator uses least-privilege RBAC:
- Cluster-scoped: CRD management, node reading
- Namespace-scoped: Pod, Service, ConfigMap, Secret management

## Troubleshooting

### Cluster Stuck in Creating

```bash
# Check pod status
kubectl get pods -l postgres.example.com/cluster=my-postgres

# Check PVC status (storage class issues)
kubectl get pvc -l postgres.example.com/cluster=my-postgres

# Check operator logs
kubectl logs -n postgres-operator-system deploy/postgres-operator
```

### Failover Not Working

```bash
# Check Patroni status
kubectl exec my-postgres-0 -- patronictl list

# Check endpoints (leader election)
kubectl get endpoints my-postgres
```

### Connection Refused

```bash
# Verify service exists
kubectl get svc my-postgres-primary

# Check if pods are ready
kubectl get pods -l postgres.example.com/cluster=my-postgres

# Test connectivity
kubectl run test --rm -it --image=busybox -- nc -zv my-postgres-primary 5432
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run `make lint` and `make test`
5. Submit a pull request

## License

MIT License - see [LICENSE.md](LICENSE.md)
