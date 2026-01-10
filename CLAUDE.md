# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Kubernetes operator written in Rust that manages PostgreSQL clusters using Patroni for high availability. The operator is designed to be human-operator friendly, integrating seamlessly into the Kubernetes ecosystem with clear status reporting, meaningful events, and graceful error recovery.

## Version Requirements

| Component | Minimum Version | Notes |
|-----------|-----------------|-------|
| Rust | 1.92+ | Edition 2024, MSRV enforced in `Cargo.toml` |
| Kubernetes | 1.35+ | Required for in-place resize, pod generation tracking |
| kube-rs | 2.x | With k8s-openapi (currently v1_34, upgrade to v1_35 when available) |
| Patroni | 3.0+ | Used via Spilo image |
| PostgreSQL | 14, 15, 16, 17 | Supported versions in Spilo |

## Coding Standards

### Panic-Free Code Policy

The operator must **never panic** in production code paths. This ensures continuous operation and recovery even when encountering unexpected states.

- **Never use** `unwrap()`, `expect()`, or `panic!()` in production code
- **Always use** `Result<T, Error>` with the `?` operator for error propagation
- **For Option types**, use `unwrap_or_default()`, `map()`, `and_then()`, or pattern matching
- **Test-only code** may use `unwrap()` where panicking on failure is acceptable

### Error Handling

- Use `thiserror` for custom error types with descriptive messages
- Classify errors as:
  - **Transient**: Retry with exponential backoff (network timeouts, temporary API failures)
  - **Validation**: Fail fast, update status condition, do not retry
  - **Permanent**: Require manual intervention, emit event, set Failed state
- Preserve error context through the call stack for debugging

### Reconciliation Safety

- All reconcile operations must be **idempotent** (safe to run multiple times)
- Handle partial failures gracefully - don't leave resources in inconsistent states
- Use server-side apply for atomic resource updates
- Track generation to avoid redundant reconciliations

## Build & Test Commands

```bash
# Build
make build              # Build the operator binary (release)
make docker-build       # Build the Docker image
make docker-push        # Push the Docker image

# Development
make run                # Run the operator locally (uses current kubeconfig)
make fmt                # Format code
make lint               # Run clippy lints
make check              # Run cargo check

# Testing
make test               # Run unit tests
make test-integration   # Run integration tests (installs CRD/RBAC first)

# Installation (onto cluster via kubectl)
make install            # Install CRD and RBAC onto the cluster
make install-crd        # Install just the CRD
make install-rbac       # Install just RBAC (creates namespace if needed)
make uninstall          # Uninstall CRD and RBAC from the cluster

# Deployment
make deploy             # Deploy the operator to the cluster
make undeploy           # Undeploy the operator from the cluster
make deploy-sample      # Deploy a sample PostgresCluster (standalone)
make delete-sample      # Delete the sample PostgresCluster

# Cleanup
make clean              # Clean build artifacts
make clean-all          # Uninstall from cluster and clean build artifacts
```

## Architecture

### Entry Point (`src/main.rs`)
Initializes logging, creates Kubernetes client, builds shared Context, and runs the Controller watching PostgresCluster CRD and owned resources (StatefulSet, Services, ConfigMaps, Secrets, PodDisruptionBudgets).

### CRD (`src/crd/postgres_cluster.rs`)
`PostgresCluster` custom resource with:
- **Spec**: `version`, `replicas`, `storage`, `resources`, `postgresql_params`, `backup`, `pgbouncer`, `tls`, `metrics`
- **Status**: `phase`, `readyReplicas`, `primaryPod`, `replicaPods`, `conditions`, retry tracking, `observed_generation`

API version: `postgres.example.com/v1alpha1`

### Controller (`src/controller/`)
- `reconciler.rs`: Main reconciliation loop - handles finalizers, spec change detection, resource application, state transitions
- `state_machine.rs`: Formal FSM with states (Pending, Creating, Running, Updating, Scaling, Degraded, Recovering, Failed, Deleting) and guarded transitions
- `context.rs`: Shared context with Kubernetes client and event recorder
- `status.rs`: Condition management (Ready, Progressing, Degraded, ConfigurationValid, ReplicasReady)
- `error.rs`: Custom errors with exponential backoff configuration

### Resources (`src/resources/`)
Each module generates Kubernetes resources:
- `patroni.rs`: StatefulSet (Spilo image), ConfigMap with Patroni config, RBAC (ServiceAccount, Role, RoleBinding)
- `service.rs`: Primary (spilo-role=master), Replicas (spilo-role=replica), Headless services
- `secret.rs`: Credentials with generated passwords
- `pdb.rs`: PodDisruptionBudget
- `pgbouncer.rs`: PgBouncer Deployment for connection pooling
- `backup.rs`: WAL-G backup configuration
- `common.rs`: Standard labels, owner references

### Key Patterns
- **Finalizer pattern** for graceful deletion
- **Server-side apply** via `PatchParams::apply()`
- **Generation tracking** to detect spec changes (`metadata.generation` vs `observed_generation`)
- **Owner references** for automatic garbage collection
- **Patroni DCS** uses Kubernetes Endpoints for leader election

## Kubernetes 1.35 Features

The operator leverages Kubernetes 1.35+ features for enhanced functionality:

### In-Place Resource Resizing
- Uses `resizePolicy` on containers to control restart behavior during resource changes
- CPU changes: `NotRequired` (resize without restart)
- Memory changes: `RestartContainer` (restart required)
- Monitor `pod.status.resize` for resize progress (Proposed, InProgress, Infeasible)

### Pod Generation Tracking
- `pod.status.observedGeneration` indicates when kubelet has processed pod spec
- Compare with `metadata.generation` to detect sync status
- Enables precise detection of when pod changes are fully applied

**Note**: Currently using k8s-openapi v1_34 with JSON patching for 1.35 features. See `Cargo.toml` TODO for upgrade path when v1_35 support is released.

## Patroni/Spilo Integration

### Overview
- **Spilo**: Zalando's Docker image combining PostgreSQL + Patroni + WAL-G
- **Patroni**: HA solution providing automatic failover and distributed consensus
- Project links: [Patroni](https://github.com/patroni/patroni), [Spilo](https://github.com/zalando/spilo)

### DCS (Distributed Configuration Store)
- Uses Kubernetes Endpoints as the DCS backend
- Leader election via Kubernetes API (no external etcd/consul needed)
- Each PostgresCluster creates a dedicated Endpoints resource for Patroni DCS

### Key Integration Points
- `src/resources/patroni.rs`: Generates StatefulSet with Spilo container, ConfigMap with Patroni bootstrap config
- Patroni REST API on port 8008 for health checks and cluster state
- Service selectors use `spilo-role=master` and `spilo-role=replica` labels set by Patroni

### Spilo Environment Variables
| Variable | Purpose |
|----------|---------|
| `SCOPE` | Patroni cluster name (matches PostgresCluster name) |
| `PGROOT` | PostgreSQL data directory |
| `POD_IP` | Pod's IP for Patroni communication (from downward API) |
| `PATRONI_KUBERNETES_NAMESPACE` | Namespace for DCS endpoints |
| `PATRONI_KUBERNETES_LABELS` | Labels for DCS endpoint filtering |
| `PATRONI_POSTGRESQL_*` | PostgreSQL configuration parameters |

### Failover Behavior
1. Patroni detects primary failure via DCS TTL (default 30s)
2. Automatic leader election among replicas based on replication lag
3. New primary updates DCS and acquires `spilo-role=master` label
4. Services automatically route traffic via label selectors
5. Operator monitors cluster state via Patroni API and pod labels

### WAL-G Backup Integration
- Configured via `spec.backup` in PostgresCluster CRD
- Supports S3, GCS, Azure Blob storage backends
- Environment variables injected into Spilo container for WAL-G configuration
- See `docs/BACKUP_AND_RESTORE.md` for detailed configuration

## Operator Design Principles

### Human-Operator Friendly
- Clear status conditions with meaningful `reason` and `message` fields
- Kubernetes events emitted for significant state changes
- Actionable error messages that guide remediation
- Status reflects actual cluster health, not just desired state

### Kubernetes-Native
- Follow controller-runtime patterns and conventions
- Use standard conditions: Ready, Progressing, Degraded
- Implement proper leader election for HA deployments
- Support standard kubectl workflows (apply, delete, describe)

### Observable
- Prometheus metrics at `/metrics` endpoint
- Structured JSON logging with tracing
- Kubernetes events for audit trail
- Health endpoints (`/healthz`, `/readyz`) for probes

### Graceful Degradation
- Continue operating in degraded mode when possible
- Don't block reconciliation on non-critical failures
- Attempt recovery from transient errors automatically
- Only transition to Failed state when manual intervention is required

### Safe Defaults
- Secure by default (TLS, RBAC, network policies in samples)
- Require explicit opt-in for less secure options
- Validate configurations before applying

## Documentation

For detailed documentation, see:
- `docs/architecture.md` - State machine, design decisions, HA patterns
- `docs/development.md` - Build, test, debug instructions
- `docs/operations.md` - Day-2 operations, monitoring, troubleshooting
- `docs/api-reference.md` - CRD field reference
- `docs/BACKUP_AND_RESTORE.md` - WAL-G backup configuration
