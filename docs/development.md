# Development Guide

This guide covers setting up a development environment and contributing to the PostgreSQL Operator.

## Prerequisites

### Required Tools

| Tool | Version | Purpose |
|------|---------|---------|
| Rust | 1.88+ | Build the operator |
| Docker | 20.10+ | Build container images |
| kubectl | 1.26+ | Interact with Kubernetes |
| kind/k3d/minikube | Latest | Local Kubernetes cluster |
| Helm | 3.8+ | Chart development |

### Install Rust

```bash
# Install rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install the required toolchain
rustup install stable
rustup default stable

# Install components
rustup component add rustfmt clippy
```

### Install a Local Kubernetes Cluster

```bash
# Option 1: kind (recommended)
brew install kind
kind create cluster --name postgres-operator

# Option 2: k3d
brew install k3d
k3d cluster create postgres-operator

# Option 3: minikube
brew install minikube
minikube start
```

## Building

### Build the Operator Binary

```bash
# Debug build
cargo build

# Release build
make build

# Check without building
make check
```

### Build the Container Image

```bash
# Build image
make docker-build

# Build and push (requires registry access)
make docker-push

# For kind clusters
kind load docker-image postgres-operator:latest --name postgres-operator
```

## Running Locally

### Option 1: Run Outside Cluster

Run the operator on your machine, connecting to a Kubernetes cluster:

```bash
# Install CRD and RBAC
make install

# Run the operator (uses ~/.kube/config)
make run

# Or with custom log level
RUST_LOG=debug cargo run
```

### Option 2: Run Inside Cluster

Deploy the operator to your cluster:

```bash
# Install CRD and RBAC
make install

# Deploy operator
make deploy

# Check status
kubectl get pods -n postgres-operator-system
kubectl logs -n postgres-operator-system deploy/postgres-operator
```

## Testing

### Unit Tests

```bash
# Run all unit tests
make test

# Run specific test module
cargo test --test unit state_machine

# Run with output
cargo test --test unit -- --nocapture
```

### Integration Tests

Integration tests require a running Kubernetes cluster:

```bash
# Install CRD and RBAC first
make install

# Run integration tests
make test-integration

# Run specific integration test
cargo test --test integration cluster_lifecycle -- --nocapture
```

### Property-Based Tests

```bash
cargo test --test proptest
```

### Test Coverage

```bash
# Install coverage tool
cargo install cargo-tarpaulin

# Generate coverage report
cargo tarpaulin --out Html --output-dir coverage
```

## Code Quality

### Formatting

```bash
# Check formatting
cargo fmt --check

# Fix formatting
make fmt
```

### Linting

```bash
# Run clippy
make lint

# With all warnings as errors
cargo clippy --all-targets --all-features -- -D warnings
```

### Pre-commit Checks

Run all checks before committing:

```bash
make fmt && make lint && make test
```

## Project Structure

```
├── src/
│   ├── main.rs              # Entry point, leader election
│   ├── lib.rs               # Controller setup, exports
│   ├── controller/
│   │   ├── mod.rs           # Module exports
│   │   ├── reconciler.rs    # Main reconciliation loop
│   │   ├── state_machine.rs # Cluster lifecycle FSM
│   │   ├── status.rs        # Status/condition management
│   │   ├── context.rs       # Shared context
│   │   └── error.rs         # Error types, backoff
│   ├── crd/
│   │   ├── mod.rs           # Module exports
│   │   └── postgres_cluster.rs  # CRD types
│   ├── resources/
│   │   ├── mod.rs           # Module exports
│   │   ├── common.rs        # Labels, owner refs
│   │   ├── patroni.rs       # StatefulSet, ConfigMap, RBAC
│   │   ├── service.rs       # Services
│   │   ├── secret.rs        # Credentials
│   │   ├── pdb.rs           # PodDisruptionBudget
│   │   └── pgbouncer.rs     # PgBouncer Deployment
│   └── health.rs            # Health/metrics server
├── tests/
│   ├── unit/                # Unit tests
│   ├── integration/         # Integration tests
│   └── proptest/            # Property-based tests
├── config/
│   ├── crd/                 # CRD YAML
│   ├── rbac/                # RBAC manifests
│   ├── deploy/              # Deployment manifests
│   └── samples/             # Example PostgresClusters
└── charts/
    └── postgres-operator/   # Helm chart
```

## Debugging

### Operator Logs

```bash
# Local run
RUST_LOG=debug make run

# In-cluster
kubectl logs -n postgres-operator-system deploy/postgres-operator -f

# With structured output
kubectl logs -n postgres-operator-system deploy/postgres-operator | jq .
```

### Kubernetes Events

```bash
# Cluster events
kubectl get events --field-selector involvedObject.kind=PostgresCluster

# Detailed events
kubectl describe pgc <cluster-name>
```

### Patroni Status

```bash
# Check Patroni cluster state
kubectl exec <cluster>-0 -- patronictl list

# Check Patroni configuration
kubectl exec <cluster>-0 -- patronictl show-config

# Patroni logs
kubectl logs <cluster>-0 -c postgres
```

### Common Issues

#### CRD Not Found

```bash
# Reinstall CRD
make install-crd

# Verify
kubectl get crd postgresclusters.postgres.example.com
```

#### Operator Not Reconciling

```bash
# Check leader election
kubectl get lease -n postgres-operator-system postgres-operator-leader

# Check operator health
kubectl get pods -n postgres-operator-system
kubectl logs -n postgres-operator-system deploy/postgres-operator
```

#### StatefulSet Not Creating Pods

```bash
# Check PVC status
kubectl get pvc -l postgres.example.com/cluster=<name>

# Check storage class
kubectl get storageclass

# Check events
kubectl describe sts <cluster-name>
```

## Adding a New Feature

### 1. Update the CRD

Add new fields to `src/crd/postgres_cluster.rs`:

```rust
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct NewFeatureSpec {
    /// Enable the new feature
    pub enabled: bool,
    // Add other fields...
}
```

### 2. Update Resource Generators

Add resource generation in `src/resources/`:

```rust
pub fn build_new_feature(cluster: &PostgresCluster) -> Option<SomeResource> {
    let spec = cluster.spec.new_feature.as_ref()?;
    if !spec.enabled {
        return None;
    }
    // Build and return the resource
}
```

### 3. Update the Reconciler

Add to the reconciliation loop in `src/controller/reconciler.rs`:

```rust
// In reconcile function
if let Some(resource) = build_new_feature(&cluster) {
    apply_resource(&ctx, &resource).await?;
}
```

### 4. Update the CRD YAML

Regenerate or manually update `config/crd/postgres-cluster.yaml`.

### 5. Add Tests

Add unit tests and integration tests for the new feature.

### 6. Update Documentation

Update README.md, API reference, and add samples.

## Releasing

### Version Bump

1. Update `Cargo.toml` version
2. Update `charts/postgres-operator/Chart.yaml` version
3. Update CHANGELOG.md

### Create Release

```bash
git tag v0.2.0
git push origin v0.2.0
```

The GitHub Actions workflow will:
1. Build the container image
2. Push to ghcr.io
3. Package and push the Helm chart
4. Create a GitHub release

## IDE Setup

### VS Code

Install extensions:
- rust-analyzer
- Even Better TOML
- YAML

Recommended settings (`.vscode/settings.json`):

```json
{
  "rust-analyzer.cargo.features": "all",
  "rust-analyzer.checkOnSave.command": "clippy"
}
```

### IntelliJ IDEA / RustRover

Install the Rust plugin. Enable:
- Use rustfmt on save
- Use clippy for linting
