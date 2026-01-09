# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Kubernetes operator written in Rust that manages PostgreSQL clusters using Patroni for high availability. Uses kube-rs 2.x with Rust edition 2024 (requires Rust 1.88+).

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
- `common.rs`: Standard labels, owner references

### Key Patterns
- **Finalizer pattern** for graceful deletion
- **Server-side apply** via `PatchParams::apply()`
- **Generation tracking** to detect spec changes (`metadata.generation` vs `observed_generation`)
- **Owner references** for automatic garbage collection
- **Patroni DCS** uses Kubernetes Endpoints for leader election
