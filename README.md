# PostgreSQL Kubernetes Operator

A Kubernetes operator for managing PostgreSQL clusters with high availability using [Patroni](https://github.com/patroni/patroni).

## Features

- **High Availability**: Automatic failover using Patroni with Kubernetes-native leader election
- **Declarative Configuration**: Manage PostgreSQL clusters using Kubernetes custom resources
- **Automatic Scaling**: Scale replicas up or down with automatic replication setup
- **In-Place Resource Resizing**: CPU/memory changes without pod restarts (Kubernetes 1.35+)
- **Connection Pooling**: Optional PgBouncer sidecar for connection pooling
- **TLS by Default**: Encrypted connections with automatic cert-manager integration
- **Cloud Backups**: Continuous WAL archiving and scheduled base backups to S3 or S3-compatible storage with point-in-time recovery (PITR)
- **Major Version Upgrades**: Near-zero downtime upgrades using blue-green deployment with logical replication
- **Metrics**: Prometheus-compatible metrics endpoint
- **Zero-Downtime Updates**: Rolling updates with PodDisruptionBudgets

## Prerequisites

- Kubernetes 1.35+
- kubectl configured to access your cluster
- [cert-manager](https://cert-manager.io/) v1.0+ (required for TLS certificate management)

## Installation

### Install the Operator

```bash
# Install CRD and RBAC
make install

# Deploy the operator
make deploy
```

### Create a PostgreSQL Cluster

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresCluster
metadata:
  name: my-postgres
spec:
  version: "16"
  replicas: 3
  storage:
    size: 10Gi
    storageClass: standard
  tls:
    issuerRef:
      name: selfsigned-issuer
      kind: ClusterIssuer
```

```bash
kubectl apply -f my-postgres.yaml
```

### Check Status

```bash
kubectl get postgresclusters
kubectl describe pgc my-postgres
```

### Connect to PostgreSQL

```bash
# Primary (read-write)
kubectl run psql --rm -it --image=postgres:16 -- \
  psql -h my-postgres-primary -U postgres

# Get credentials
kubectl get secret my-postgres-credentials -o jsonpath='{.data.password}' | base64 -d
```

## Uninstallation

```bash
# Remove the operator
make undeploy

# Remove CRD and RBAC
make uninstall
```

## Documentation

For detailed documentation, see the [docs/](docs/) directory:

- **[API Reference](docs/api-reference.md)** - Complete CRD field reference
- **[Architecture](docs/architecture.md)** - Design decisions, state machine, HA patterns
- **[Backup & Restore](docs/backup-restore.md)** - Cloud backup configuration, PITR, restore procedures
- **[Development](docs/development.md)** - Building, testing, contributing
- **[Operations](docs/operations.md)** - Day-2 operations, scaling, troubleshooting, maintenance
- **[Upgrades](docs/upgrades.md)** - Major version upgrades using blue-green deployment

Sample configurations are available in [`config/samples/`](config/samples/).

## Support

This project is maintained on a **best-effort basis**. For questions, bug reports, or feature requests, please [open an issue on GitHub](https://github.com/smoketurner/k8s-postgres-operator/issues).

## License

MIT License - see [LICENSE.md](LICENSE.md)
