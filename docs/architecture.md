# Architecture

This document describes the architecture and design decisions of the PostgreSQL Kubernetes Operator.

## Overview

The PostgreSQL Operator is a Kubernetes controller that manages PostgreSQL clusters using Patroni for high availability. It follows the Kubernetes operator pattern, watching custom resources and reconciling the actual state to match the desired state.

```
┌─────────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                          │
│                                                                   │
│  ┌───────────────────┐                                           │
│  │ PostgresCluster   │◄──── User creates/modifies                │
│  │ Custom Resource   │                                           │
│  └────────┬──────────┘                                           │
│           │                                                       │
│           │ watches                                               │
│           ▼                                                       │
│  ┌───────────────────┐     ┌─────────────────────────────────┐  │
│  │ postgres-operator │────►│ Reconciliation Loop              │  │
│  │                   │     │                                   │  │
│  │ - Leader Election │     │ 1. Read PostgresCluster spec     │  │
│  │ - Health Server   │     │ 2. Validate configuration        │  │
│  │ - Metrics         │     │ 3. Generate child resources      │  │
│  └───────────────────┘     │ 4. Apply via server-side apply   │  │
│                            │ 5. Update status                  │  │
│                            └─────────────────────────────────┘  │
│                                         │                        │
│                                         │ creates/manages        │
│                                         ▼                        │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                     Child Resources                      │    │
│  │                                                          │    │
│  │  StatefulSet    Services    ConfigMap    Secret    PDB   │    │
│  │  (Patroni)      (3 types)   (Patroni)    (creds)         │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### Controller (`src/controller/`)

The controller is the heart of the operator, implementing the reconciliation loop.

#### Reconciler (`reconciler.rs`)

The reconciler handles the main reconciliation logic:

1. **Finalizer handling**: Adds/removes finalizers for graceful deletion
2. **Validation**: Validates spec changes (version, replicas, etc.)
3. **Change detection**: Compares `metadata.generation` with `status.observedGeneration`
4. **Resource generation**: Creates/updates all child resources
5. **Status management**: Updates cluster status and conditions

```rust
async fn reconcile(cluster: Arc<PostgresCluster>, ctx: Arc<Context>) -> Result<Action> {
    // 1. Handle deletion with finalizer
    if cluster.metadata.deletion_timestamp.is_some() {
        return handle_deletion(cluster, ctx).await;
    }

    // 2. Ensure finalizer exists
    ensure_finalizer(&cluster, &ctx).await?;

    // 3. Validate spec
    validate_spec(&cluster.spec)?;

    // 4. Check for spec changes
    let spec_changed = has_spec_changed(&cluster);

    // 5. Apply child resources
    apply_resources(&cluster, &ctx).await?;

    // 6. Update status
    update_status(&cluster, &ctx).await?;

    Ok(Action::requeue(Duration::from_secs(60)))
}
```

#### State Machine (`state_machine.rs`)

The operator implements a formal finite state machine (FSM) for cluster lifecycle management:

```
                              ┌─────────┐
                              │ Pending │
                              └────┬────┘
                                   │ ResourcesApplied
                                   ▼
                              ┌─────────┐
                      ┌───────│Creating │───────┐
                      │       └────┬────┘       │
                      │            │            │ ReconcileError
           ReplicasDegraded   AllReplicasReady  │
                      │            │            ▼
                      ▼            ▼       ┌────────┐
                 ┌─────────┐  ┌─────────┐  │ Failed │
                 │Degraded │  │ Running │  └────┬───┘
                 └────┬────┘  └────┬────┘       │
                      │            │            │ RecoveryInitiated
          FullyRecovered     SpecChanged/       ▼
                      │      ReplicaCountChanged  ┌───────────┐
                      │            │              │Recovering │
                      │            ▼              └───────────┘
                      │       ┌──────────┐
                      └──────►│ Updating │
                              │ Scaling  │
                              └──────────┘
```

**State Definitions:**

| State | Description | Exit Conditions |
|-------|-------------|-----------------|
| Pending | Initial state, no resources created | Resources applied |
| Creating | Resources created, waiting for pods | All ready, degraded, or error |
| Running | Cluster healthy, all replicas ready | Spec change, scaling, degraded |
| Updating | Configuration change in progress | All ready or error |
| Scaling | Replica count change in progress | All ready or error |
| Degraded | Some replicas unavailable | Fully recovered or error |
| Recovering | Auto-recovery in progress | Complete or error |
| Failed | Needs manual intervention | Recovery initiated |
| Deleting | Cleanup in progress | Resources removed |

#### Status Management (`status.rs`)

The operator maintains Kubernetes-standard conditions:

| Condition | Meaning |
|-----------|---------|
| Ready | Cluster is ready for connections |
| Progressing | Moving toward desired state |
| Degraded | Running but with issues |
| ConfigurationValid | Spec passes validation |
| ReplicasReady | All replicas synchronized |
| PodGenerationSynced | All pods have applied their spec (K8s 1.35+) |

#### Kubernetes 1.35+ Enhanced Status Tracking

On Kubernetes 1.35+, the operator tracks additional pod-level status:

**Pod Generation Tracking:**
- `pod.status.observedGeneration` indicates when kubelet has processed a pod spec
- The operator compares `metadata.generation` with `observedGeneration` to detect sync status
- Enables precise detection of when pod changes are fully applied

**In-Place Resource Resize Status:**
- `pod.status.resize` field tracks resize progress per pod
- States: `Proposed` (pending), `InProgress` (resizing), `Infeasible` (cannot resize)
- Container-level `allocatedResources` shows current resource allocation

### Custom Resource Definition (`src/crd/`)

The `PostgresCluster` CRD defines the API for managing clusters:

```yaml
apiVersion: postgres-operator.smoketurner.com/v1alpha1
kind: PostgresCluster
metadata:
  name: my-cluster
spec:
  version: "16"           # PostgreSQL version
  replicas: 3             # Patroni members
  storage:
    size: 100Gi
    storageClass: fast-ssd
  resources: {}           # CPU/memory limits
  postgresqlParams: {}    # postgresql.conf
  pgbouncer: {}          # Connection pooling
  tls: {}                # TLS configuration
  backup: {}             # Backup settings
  service: {}            # Service configuration
status:
  phase: Running
  readyReplicas: 3
  conditions: []
```

### Resource Generators (`src/resources/`)

Each module generates specific Kubernetes resources:

#### Patroni (`patroni.rs`)

Generates the StatefulSet with Spilo containers:

- **Container image**: Spilo (Zalando's Patroni + PostgreSQL)
- **Leader election**: Uses Kubernetes Endpoints as DCS
- **Configuration**: Injected via ConfigMap
- **RBAC**: ServiceAccount, Role, RoleBinding per cluster

#### Services (`service.rs`)

Generates three service types:

1. **Primary service** (`<name>-primary`): Routes to `spilo-role=master`
2. **Replica service** (`<name>-repl`): Routes to `spilo-role=replica`
3. **Headless service** (`<name>`): For StatefulSet DNS discovery

#### Secrets (`secret.rs`)

Generates credential secrets:

- `postgres` user password
- `replication` user password
- Passwords auto-generated if not specified

## Design Decisions

### Why Patroni?

Patroni provides battle-tested PostgreSQL HA with:

- Automatic failover with configurable timeouts
- Split-brain prevention via distributed consensus
- Native Kubernetes integration (Endpoints-based DCS)
- Synchronous/asynchronous replication options
- Built-in REST API for health checks

### Why Server-Side Apply?

All resources use Kubernetes server-side apply (`PatchParams::apply()`):

- **Idempotent**: Safe to apply multiple times
- **Conflict detection**: Detects field ownership conflicts
- **Partial updates**: Only sends changed fields
- **Field management**: Clear ownership via field manager

### Why Generation Tracking?

The operator tracks `metadata.generation` vs `status.observedGeneration`:

- **Optimization**: Skip reconciliation if spec unchanged
- **Change detection**: Know exactly when spec changes
- **Status-only updates**: Don't trigger full reconcile

### Why Finalizers?

Finalizers ensure graceful deletion:

```rust
const FINALIZER: &str = "postgres-operator.smoketurner.com/finalizer";
```

- Prevents premature resource deletion
- Allows cleanup of external resources
- Guarantees deletion order

### In-Place Resource Resizing (Kubernetes 1.35+)

The operator supports in-place pod resource resizing introduced in Kubernetes 1.35:

**How it works:**
1. When `spec.resources` changes, the operator updates the StatefulSet
2. Kubernetes applies `resizePolicy` to determine resize behavior per resource
3. Pods resize in-place without restart (for `NotRequired` policy)
4. The operator monitors `pod.status.resize` for progress

**Resize Policies:**
```yaml
resizePolicy:
  - resourceName: cpu
    restartPolicy: NotRequired   # Resize without restart
  - resourceName: memory
    restartPolicy: RestartContainer  # Restart required for memory
```

**Status Tracking:**
- `status.resize_status[]` shows per-pod resize state
- `status.pods[]` tracks generation sync status
- `status.all_pods_synced` indicates when all pods reflect current spec

**Fallback Behavior:**
On Kubernetes < 1.35, resource changes trigger standard rolling restarts via StatefulSet update strategy.

## High Availability

### Quorum and Failover

| Replicas | Quorum | Automatic Failover |
|----------|--------|-------------------|
| 1 | N/A | No (single point of failure) |
| 2 | N/A | No (no majority possible) |
| 3+ | ⌊n/2⌋+1 | Yes |

### Pod Anti-Affinity

The operator configures pod anti-affinity to spread replicas:

```yaml
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          topologyKey: kubernetes.io/hostname
```

### PodDisruptionBudget

A PDB protects cluster availability during node maintenance:

```yaml
spec:
  minAvailable: 1  # For 2+ replicas
  selector:
    matchLabels:
      postgres-operator.smoketurner.com/cluster: <name>
```

## Leader Election

The operator uses Kubernetes Leases for leader election:

```rust
LeaderElection::new(client, "postgres-operator-leader", namespace)
    .with_lease_ttl(Duration::from_secs(15))
    .with_renew_interval(Duration::from_secs(5))
```

Only the leader processes reconciliation events. On leadership loss, the operator exits, allowing Kubernetes to restart it.

## Error Handling

### Exponential Backoff

Transient errors trigger exponential backoff:

```rust
BackoffConfig {
    initial_delay: Duration::from_secs(5),
    max_delay: Duration::from_secs(300),
    multiplier: 2.0,
    jitter: 0.1,  // ±10%
}
```

### Error Classification

| Error Type | Behavior |
|------------|----------|
| Transient | Retry with backoff |
| Validation | Fail fast, update status |
| NotFound | Ignore (likely deleted) |
| Permanent | Fail, require intervention |

## Metrics and Observability

### Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `postgres_operator_reconciliations` | Counter | Total reconciliations |
| `postgres_operator_reconciliation_errors` | Counter | Failed reconciliations |
| `postgres_operator_reconcile_duration_seconds` | Histogram | Reconciliation latency |

### Health Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/healthz` | Liveness probe |
| `/readyz` | Readiness probe |
| `/metrics` | Prometheus metrics |

## Security

### Pod Security

The operator runs with restricted security context:

- Non-root user (UID 1000)
- Read-only root filesystem
- All capabilities dropped
- No privilege escalation

### RBAC

Minimal permissions following least-privilege:

- **Cluster-scoped**: CRD, Leases
- **Namespace-scoped**: Pods, Services, Secrets, ConfigMaps, StatefulSets

### Network Isolation

Sample NetworkPolicies restrict PostgreSQL access to labeled clients only.
