# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Kubernetes operator written in Rust that manages PostgreSQL clusters using Patroni for high availability. The operator is designed to be human-operator friendly, integrating seamlessly into the Kubernetes ecosystem with clear status reporting, meaningful events, and graceful error recovery.

## Version Requirements

| Component | Minimum Version | Notes |
|-----------|-----------------|-------|
| Rust | 1.95+ | Edition 2024, MSRV enforced in `Cargo.toml` |
| Kubernetes | 1.35+ | Required for in-place resize, pod generation tracking |
| kube-rs | 3.x | With k8s-openapi v1_35 (Kubernetes 1.35 native types) |
| cert-manager | 1.0+ | Required for TLS certificate management |
| Patroni | 3.0+ | Used via Spilo image |
| PostgreSQL | 15, 16, 17 | Supported versions in Spilo |

## Coding Standards

### Panic-Free Code Policy

The operator must **never panic** in production code paths. This ensures continuous operation and recovery even when encountering unexpected states.

- **Never use** `unwrap()`, `expect()`, or `panic!()` in production code
- **Always use** `Result<T, Error>` with the `?` operator for error propagation
- **For Option types**, use `unwrap_or_default()`, `map()`, `and_then()`, or pattern matching
- **Test-only code** may use `unwrap()` where panicking on failure is acceptable
- **Test modules** (`#[cfg(test)] mod tests`) and files under `tests/` opt out explicitly with `#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]` at the module level, so the exemption is visible at the boundary

Additional lints denied in `Cargo.toml` / `.clippy.toml`: `indexing_slicing` (use `.get()`), `unreachable`, `unsafe_code`, `unwrap_in_result`, `panic_in_result_fn`, `get_unwrap`, `exit`. Function size capped at 150 lines (`too-many-lines-threshold`); cognitive complexity capped at 30.

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
make test               # Run unit, proptest, and integration test binaries (non-ignored)
make test-integration   # Run integration tests with --ignored (installs CRD/RBAC first)
make audit              # Run cargo audit for security advisories

# Run a specific test
cargo test --test unit state_machine           # Single unit test module
cargo test --test integration -- --ignored scaling   # Single integration test (needs cluster)
cargo test --test proptest                     # Property-based tests

# Installation (onto cluster via kubectl)
make install            # Install CRD and RBAC onto the cluster
make install-crd        # Install just the CRD
make install-rbac       # Install just RBAC (creates namespace if needed)
make uninstall          # Uninstall CRD and RBAC from the cluster

# Deployment
make deploy             # Deploy the operator to the cluster
make undeploy           # Undeploy the operator from the cluster
make deploy-sample      # Deploy a sample PostgresCluster (automatic failover)
make delete-sample      # Delete the sample PostgresCluster

# Cleanup
make clean              # Clean build artifacts
make clean-all          # Uninstall from cluster and clean build artifacts
```

## Architecture

### Entry Point (`src/main.rs` + `src/lib.rs`)
`src/main.rs` is thin: it initializes logging/TLS and delegates to `src/lib.rs`. The wiring lives in `lib.rs`, which exposes:

- `run_controller` / `run_controller_scoped` — PostgresCluster controller (watches StatefulSet, Services, ConfigMaps, Secrets, PodDisruptionBudgets)
- `run_database_controller` / `run_database_controller_scoped` — PostgresDatabase controller
- `run_upgrade_controller` / `run_upgrade_controller_scoped` — PostgresUpgrade controller

The `_scoped` variants take an optional namespace and are used by integration tests for parallel execution. Integration tests call into `lib.rs` directly, not `main.rs`.

### Health & Metrics (`src/health.rs`)
`HealthState` and Prometheus `Metrics` types backing the `/healthz`, `/readyz`, and `/metrics` HTTP endpoints. Reconcilers record metrics through `Context.health_state`.

### CRDs (`src/crd/`)

#### PostgresCluster (`postgres_cluster.rs`)
`PostgresCluster` custom resource with:
- **Spec**: `version`, `replicas`, `storage`, `resources`, `postgresql_params`, `labels`, `backup`, `pgbouncer`, `tls`, `metrics`, `service`, `restore`, `scaling`, `networkPolicy`, `sidecars`, `nodeSelector`, `tolerations`, `topologySpreadConstraints`, `priorityClassName`
- **Status**: `phase`, `readyReplicas`, `primaryPod`, `replicaPods`, `conditions`, `observedGeneration`, `backup`, `restoredFrom`, `replicationLag`, `connectionInfo`

API version: `postgres-operator.smoketurner.com/v1alpha1`

Key design decisions:
- **TLS enabled by default**: Requires cert-manager issuer reference
- **Backup encryption required**: When backups are configured, encryption must be specified
- **User labels support**: `spec.labels` allows cost allocation labels that are merged with standard labels

#### PostgresDatabase (`postgres_database.rs`)
`PostgresDatabase` custom resource for declarative database and role provisioning:
- **Spec**: `clusterRef`, `database`, `roles`, `grants`, `extensions`
- **Status**: `phase`, `conditions`, `provisioned`, `secretName`

API version: `postgres-operator.smoketurner.com/v1alpha1`

Features:
- Create databases within a PostgresCluster
- Provision roles with specified privileges (LOGIN, CREATEDB, etc.)
- Generate Kubernetes secrets with credentials and connection strings
- Apply grants to control table/schema access
- Enable PostgreSQL extensions

#### PostgresUpgrade (`postgres_upgrade.rs`)
`PostgresUpgrade` custom resource for blue-green major version upgrades using logical replication:
- **Spec**: `sourceCluster`, `targetVersion`, `targetClusterOverrides`, `strategy`
- **Status**: `phase`, `observedGeneration`, `startedAt`, `completedAt`, `replication`, `verification`, `sequences`, `conditions`

API version: `postgres-operator.smoketurner.com/v1alpha1`

Features:
- Near-zero downtime major version upgrades (15→16, 16→17, etc.)
- Blue-green deployment using PostgreSQL logical replication
- Row count verification before cutover
- Manual or automatic cutover modes with maintenance windows
- Sequence synchronization after source goes read-only
- Rollback support via annotation

See `docs/upgrades.md` for detailed upgrade procedures.

### Controller (`src/controller/`)
PostgresCluster-specific modules use a `cluster_` prefix; PostgresUpgrade-specific modules use an `upgrade_` prefix.

- `cluster_reconciler.rs`: Main PostgresCluster reconciliation loop - handles finalizers, spec change detection, resource application, state transitions
- `cluster_state_machine.rs`: Formal FSM with states (Pending, Creating, Running, Updating, Scaling, Degraded, Recovering, Failed, Deleting) and guarded transitions
- `cluster_error.rs`: Custom errors with exponential backoff configuration
- `cluster_status.rs`: Condition management (Ready, Progressing, Degraded, ConfigurationValid, ReplicasReady, ResourceResizeInProgress)
- `cluster_validation.rs`: Spec validation logic
- `cluster_replication_lag.rs`: Replication lag monitoring via Patroni REST API
- `cluster_backup_status.rs`: Backup status collection from WAL-G
- `database_reconciler.rs`: PostgresDatabase reconciliation - database/role provisioning via SQL execution, secret generation
- `upgrade_reconciler.rs`: PostgresUpgrade reconciliation - manages blue-green upgrade lifecycle
- `upgrade_state_machine.rs`: Upgrade FSM with phases (Pending, CreatingTarget, ConfiguringReplication, Replicating, Verifying, SyncingSequences, ReadyForCutover, CuttingOver, HealthChecking, Completed, Failed, RolledBack)
- `upgrade_preflight.rs`: Replication-compatibility preflight checks (replica identity, large objects, unlogged tables, blocking extensions, in-flight matview refresh) — runs on `Pending → CreatingTarget`
- `upgrade_error.rs`: Upgrade-specific errors with backoff configuration
- `context.rs`: Shared context with Kubernetes client and event recorder
- `cleanup.rs`: Resource cleanup utilities for graceful deletion

### Resources (`src/resources/`)
Each module generates Kubernetes resources:
- `patroni.rs`: StatefulSet (Spilo image), ConfigMap with Patroni config, RBAC (ServiceAccount, Role, RoleBinding)
- `service.rs`: Primary (spilo-role=master), Replicas (spilo-role=replica), Headless services
- `secret.rs`: Credentials with generated passwords
- `pdb.rs`: PodDisruptionBudget
- `pgbouncer.rs`: PgBouncer Deployment for connection pooling
- `backup.rs`: WAL-G backup configuration (encryption required)
- `logical_backup.rs`: CronJob-based `pg_dumpall` logical backup (scheduled full SQL dumps to S3)
- `ddl_audit.rs`: Server-side DDL audit (event trigger + audit table) installed on source during the upgrade replication window; cutover refused if non-zero count unless `spec.strategy.acknowledgeDDL=true`
- `certificate.rs`: cert-manager Certificate CR for TLS
- `common.rs`: Standard labels, owner references, user label merging
- `scaled_object.rs`: KEDA ScaledObject for auto-scaling (CPU/connection metrics)
- `network_policy.rs`: NetworkPolicy generation for cluster access control
- `sql.rs`: SQL execution via pod exec for database provisioning
- `postgres_client.rs`: PostgreSQL client connection handling for direct SQL execution
- `replication.rs`: Logical replication configuration for upgrades (publication/subscription setup)
- `port_forward.rs`: Kubernetes port-forward API for establishing connections to pods

### Webhooks (`src/webhooks/`)
ValidatingAdmissionWebhook for policy enforcement:
- `server.rs`: HTTP server handling admission review requests
- `policies/backup.rs`: Enforces encryption requirement when backups are configured
- `policies/tls.rs`: Requires cert-manager issuer reference when TLS enabled
- `policies/immutability.rs`: Prevents changing immutable fields (storage size, version downgrades)
- `policies/production.rs`: Production-specific requirements for namespaces containing "prod"
- `policies/upgrade.rs`: Validates PostgresUpgrade resources (version compatibility, source cluster state)

### Tests (`tests/`)
Four sibling test crates, all registered in `Cargo.toml`:

- `tests/unit/` — pure unit tests (state machine, validation, network policy, resource builders, webhooks)
- `tests/integration/` — full controller-against-cluster tests, gated behind `--ignored`
- `tests/proptest/` — property-based tests (parsers, serialization, FSM invariants)
- `tests/common/` — shared fixtures used by the other crates

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

These features are available natively via k8s-openapi v1_35 (`ContainerResizePolicy`, `pod.status.resize`, `container_status.allocated_resources`).

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
- Supports S3 and S3-compatible storage backends (AWS S3, MinIO, DigitalOcean Spaces, etc.)
- **Encryption required**: Must specify `encryption.keySecret` when backups are configured
- Environment variables injected into Spilo container for WAL-G configuration
- Optional `spec.backup.logical` enables a scheduled `pg_dumpall` CronJob for logical backups (orthogonal to WAL-G)
- See `docs/backup-restore.md` for detailed configuration

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
- **TLS enabled by default**: Requires cert-manager issuer reference for certificate management
- **Backup encryption required**: When backups are configured, an encryption key secret must be specified
- RBAC and network policies provided in samples
- Require explicit opt-out for less secure options (`tls.enabled: false`)
- Validate configurations before applying (e.g., encryption key secret exists)

## Documentation

For detailed documentation, see:
- `docs/architecture.md` - State machine, design decisions, HA patterns
- `docs/development.md` - Build, test, debug instructions
- `docs/operations.md` - Day-2 operations, monitoring, troubleshooting
- `docs/api-reference.md` - CRD field reference
- `docs/backup-restore.md` - WAL-G backup configuration and logical (`pg_dumpall`) backups (encryption required)
- `docs/upgrades.md` - Blue-green major version upgrades using logical replication
