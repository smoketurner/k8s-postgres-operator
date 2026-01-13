# Plan: Adopt tokio-postgres for Direct SQL Connections

> **Status**: Ready for implementation - upgrade operator testing structure is complete

## Summary

Replace pod exec (`psql` CLI) with direct PostgreSQL connections using `tokio-postgres`. Move `tokio-postgres` and `tokio-postgres-rustls-improved` to production dependencies. The operator will use non-TLS connections via kube-rs port forwarding, with `pg_hba.conf` configured to allow this.

**Key decisions:**
- Full migration: Remove `exec_sql()` pod exec approach entirely (no backward compatibility needed)
- No TLS for operator: Configure pg_hba.conf to allow non-TLS from localhost/internal
- Keep rustls-improved: Available for future flexibility and TLS integration tests

## Current State

| Aspect | Test Code | Production Code |
|--------|-----------|-----------------|
| Dependencies | `tokio-postgres` + `tokio-postgres-rustls` (dev) | None |
| SQL Execution | Direct TCP via kube-rs `portforward()` | Pod exec with `psql` CLI |
| Query Building | Prepared statements (`$1`, `$2`) | String interpolation (`format!()`) |
| Result Handling | Typed via `row.get()` | Manual string parsing (split on `\|`) |

### Test Infrastructure Maturity

The existing test code provides a solid foundation for production use:

| Module | Lines | Features |
|--------|-------|----------|
| `tests/integration/port_forward.rs` | 384 | RAII wrapper, label-based pod resolution (`spilo-role=master/replica`), automatic port selection |
| `tests/integration/postgres.rs` | 831 | TLS modes (Disabled, RequireUnverified, RequireVerified), connection retry with exponential backoff, role-specific credential fetching, JDBC/connection string support |

Key capabilities already implemented in test code:
- `TlsMode` enum for flexible TLS configuration
- `PostgresCredentials` struct with connection string generation
- `verify_connection_with_retry()` for robust connectivity
- `fetch_ca_certificate()` for TLS verification tests

## Benefits of Migration

1. **Code simplification**: Single SQL execution path for tests and production
2. **Prepared statements**: Type-safe parameter binding, no SQL injection risk from escaping bugs
3. **Typed results**: No manual string parsing - `row.get::<_, i64>("lag_bytes")` instead of `line.split('|').get(2).parse()`
4. **Better error handling**: PostgreSQL error codes instead of string-matching "ERROR" in stderr
5. **Remove dead code**: Delete `exec_sql()`, `escape_sql_string_pub()`, `quote_identifier_pub()`, and manual parsing logic

## Upgrade Operator Integration

The upgrade reconciler is a **major consumer** of SQL operations. This migration affects critical upgrade workflows:

### SQL Operations in Upgrade Workflow

**Setup Phase:**
- `copy_schema()` - Dump DDL from source, apply to target
- `setup_publication()` - Create logical replication publication on source
- `setup_subscription()` - Create subscription on target with connection string

**Monitoring Phase (Loop):**
- `get_replication_lag()` - Monitor WAL sync progress via `pg_replication_slots`
- `get_subscription_state()` - Check subscription status
- `verify_row_counts()` - Cross-cluster data integrity validation

**Pre-Cutover Phase:**
- `set_source_readonly()` - `ALTER SYSTEM` to prevent writes
- `sync_sequences()` - Copy sequence values from source to target
- `wait_for_connections_drain()` - Check active connections

**Cleanup Phase:**
- `drop_subscription()`, `drop_publication()`, `drop_replication_slot()`
- `set_source_readwrite()` - Restore write access on rollback

### Connection Lifecycle

The upgrade reconciler performs long-running operations with multiple SQL calls. The migration should reuse `PostgresConnection` within reconcile cycles rather than creating new connections per operation:

```rust
// Create connections once per reconcile, reuse across operations
let source_conn = PostgresConnection::connect_primary(&client, &ns, &source_name).await?;
let target_conn = PostgresConnection::connect_primary(&client, &ns, &target_name).await?;

// Reuse connections
let result = setup_publication(&source_conn, &pub_name).await?;
let lag = get_replication_lag(&source_conn, &sub_name).await?;
```

## Why tokio-postgres-rustls-improved

Needed for TLS integration tests (verifying client TLS works). The original `tokio-postgres-rustls` is unmaintained:
- Broken SASL/SCRAM channel binding
- No test coverage
- Contains unsafe code

`tokio-postgres-rustls-improved` provides:
- Fixed channel binding (critical for `scram-sha-256` auth)
- Zero unsafe code
- aws-lc-rs crypto backend (matches existing `rustls` dependency)
- Active maintenance with CI/CD pipeline

## Implementation Plan

### Phase 1: Dependency Changes

**File: `Cargo.toml`**

```toml
[dependencies]
# Add to production dependencies
tokio-postgres = { version = "=0.7.15", default-features = false, features = ["runtime"] }
tokio-postgres-rustls-improved = { version = "=0.1.x", default-features = false }  # For TLS tests

[dev-dependencies]
# Remove these lines entirely (moved to main deps):
# tokio-postgres = ...
# tokio-postgres-rustls = ...
```

### Phase 2: Move Port Forwarding to Production

**Move**: `tests/integration/port_forward.rs` → `src/resources/port_forward.rs`

This module already exists and is well-tested. Changes needed:
- Add `#[cfg(test)]` gates for test-only helpers
- Export from `src/resources/mod.rs`

### Phase 3: Create PostgreSQL Client Module

**Create**: `src/resources/postgres_client.rs`

Merge and refactor from `tests/integration/postgres.rs`:

```rust
use tokio_postgres::{Client, NoTls};
use crate::resources::port_forward::{PortForward, PortForwardTarget};

/// PostgreSQL connection via kube-rs port-forward
pub struct PostgresConnection {
    client: Client,
    _port_forward: PortForward,  // RAII cleanup on drop
}

impl PostgresConnection {
    /// Connect to a PostgresCluster's primary (non-TLS)
    pub async fn connect_primary(
        kube_client: &kube::Client,
        namespace: &str,
        cluster_name: &str,
    ) -> Result<Self, Error> {
        let credentials = fetch_credentials(kube_client, namespace, cluster_name).await?;
        let pf = PortForward::start(
            kube_client.clone(),
            namespace,
            PortForwardTarget::service(format!("{}-primary", cluster_name), 5432),
            None,
        ).await?;

        let config = format!(
            "host=127.0.0.1 port={} user={} password={} dbname=postgres",
            pf.local_port(), credentials.username, credentials.password
        );

        let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;
        tokio::spawn(async move { let _ = connection.await; });

        Ok(Self { client, _port_forward: pf })
    }

    /// Query returning rows
    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>;

    /// Query returning optional single row
    pub async fn query_opt(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>, Error>;

    /// Execute statement returning affected rows
    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>;
}
```

### Phase 4: Rewrite replication.rs

**Note**: No pg_hba.conf changes needed. The existing configuration already uses `host all all samenet scram-sha-256` which allows non-TLS connections with password auth. Port-forward connections will match this rule.

**File**: `src/resources/replication.rs` (complete rewrite)

All functions change signature from:
```rust
pub async fn setup_publication(
    client: &Client,           // kube::Client
    namespace: &str,
    cluster_name: &str,
    publication_name: &str,
) -> ReplicationResult<bool>
```

To:
```rust
pub async fn setup_publication(
    conn: &PostgresConnection,  // Direct PG connection
    publication_name: &str,
) -> ReplicationResult<bool>
```

**Example migration - `get_replication_lag()`:**

Before (58 lines):
```rust
let sql = format!(r#"SELECT ... FROM pg_replication_slots WHERE slot_name = '{}'"#,
    escape_sql_string_pub(subscription_name));
let result = exec_sql(...).await?;
let parts: Vec<&str> = result.trim().split('|').collect();
let lag_bytes: i64 = parts.get(2).map(|s| s.trim()).unwrap_or("0").parse()?;
```

After (~15 lines):
```rust
let row = conn.query_opt(
    "SELECT pg_current_wal_lsn()::text, confirmed_flush_lsn::text,
            pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
     FROM pg_replication_slots WHERE slot_name = $1",
    &[&subscription_name]
).await?;

let row = row.ok_or(ReplicationError::ReplicationSlotNotFound(subscription_name.into()))?;
Ok(LagStatus {
    source_lsn: row.get(0),
    target_lsn: row.get(1),
    lag_bytes: row.get(2),
    in_sync: row.get::<_, i64>(2) == 0,
})
```

### Phase 5: Rewrite sql.rs

**File**: `src/resources/sql.rs` (complete rewrite)

Remove:
- `exec_sql()` function
- `find_primary_pod()` function
- `escape_sql_string_pub()` / `quote_identifier_pub()` functions
- All manual result parsing

Keep:
- Error types (refactor for tokio-postgres errors)
- `is_valid_identifier()` (still useful for CRD validation)

### Phase 6: Update Reconcilers

**Files**:
- `src/controller/upgrade_reconciler.rs`
- `src/controller/database_reconciler.rs`

Change from passing `kube::Client` + namespace + cluster_name to SQL functions, to:
1. Create `PostgresConnection` at start of reconcile
2. Pass connection to SQL functions
3. Connection auto-closes on drop

### Phase 7: Refactor ALL Tests to Use Production Modules

**Goal**: Single code path for tests and production. No test-specific SQL helpers. Maximum code reuse.

**Files to delete** (moved to `src/resources/`):
- `tests/integration/postgres.rs`
- `tests/integration/port_forward.rs`

**All test files must use production modules:**

| Test File | Current Pattern | New Pattern |
|-----------|-----------------|-------------|
| `database_tests.rs` | `fetch_credentials()`, `verify_database_exists()` | `PostgresConnection::connect_primary()`, `.query_opt()` |
| `upgrade_tests.rs` | Direct `tokio_postgres::Config::new()` | `PostgresConnection::connect_primary()` |
| `connectivity_tests.rs` | Custom connection helpers | `PostgresConnection` with TLS options |
| `tls_tests.rs` | TLS-specific helpers | `PostgresConnection::connect_with_tls()` |
| `scaling_tests.rs` | Connection verification | `PostgresConnection` |
| `functional_tests.rs` | Ad-hoc connections | `PostgresConnection` |

**TLS Support in Production Module:**

The `PostgresConnection` must support TLS modes to enable TLS tests to use production code:

```rust
impl PostgresConnection {
    /// Connect with TLS (for tls_tests.rs and production TLS support)
    pub async fn connect_with_tls(
        kube_client: &kube::Client,
        namespace: &str,
        service_name: &str,
        port: u16,
        credentials: &PostgresCredentials,
        tls_mode: TlsMode,
    ) -> Result<Self, Error>;
}

pub enum TlsMode {
    Disabled,
    RequireUnverified,  // Self-signed certs
    RequireVerified { ca_cert_pem: String },
}
```

**No direct tokio_postgres usage in tests** - all connections go through `PostgresConnection`.

**Example test refactoring**:

Before (in `database_tests.rs`):
```rust
use crate::postgres::{fetch_credentials, verify_database_exists, PortForward, PortForwardTarget};

async fn test_database_creation() {
    let pf = PortForward::start(...).await?;
    let creds = fetch_credentials(&client, &ns, &secret_name).await?;
    let exists = verify_database_exists(&creds, "127.0.0.1", pf.local_port(), "mydb").await?;
}
```

After:
```rust
use postgres_operator::resources::postgres_client::PostgresConnection;

async fn test_database_creation() {
    let conn = PostgresConnection::connect_primary(&client, &ns, &cluster_name).await?;
    let exists: bool = conn.query_opt(
        "SELECT 1 FROM pg_database WHERE datname = $1",
        &[&"mydb"]
    ).await?.is_some();
}
```

## Critical Files to Modify

### Production Code
| File | Action |
|------|--------|
| `Cargo.toml` | Move deps, replace `tokio-postgres-rustls` with `tokio-postgres-rustls-improved` |
| `src/resources/mod.rs` | Export new modules |
| `src/resources/port_forward.rs` | **NEW** - move from `tests/integration/port_forward.rs` |
| `src/resources/postgres_client.rs` | **NEW** - merge from `tests/integration/postgres.rs`, with TlsMode |
| `src/resources/replication.rs` | **REWRITE** - use prepared statements |
| `src/resources/sql.rs` | **REWRITE** - remove exec_sql, use PostgresConnection |
| `src/controller/upgrade_reconciler.rs` | Update to use PostgresConnection |
| `src/controller/database_reconciler.rs` | Update to use PostgresConnection |

### Test Code
| File | Action |
|------|--------|
| `tests/integration/postgres.rs` | **DELETE** - moved to production |
| `tests/integration/port_forward.rs` | **DELETE** - moved to production |
| `tests/integration/main.rs` | Update imports |
| `tests/integration/database_tests.rs` | Migrate to `PostgresConnection` |
| `tests/integration/upgrade_tests.rs` | Migrate from direct `tokio_postgres::Config` |
| `tests/integration/connectivity_tests.rs` | Migrate to `PostgresConnection` |
| `tests/integration/tls_tests.rs` | Migrate to `PostgresConnection::connect_with_tls()` |
| `tests/integration/scaling_tests.rs` | Migrate to `PostgresConnection` |
| `tests/integration/functional_tests.rs` | Migrate to `PostgresConnection` |

**Note**: No changes to `src/resources/patroni.rs` needed - existing pg_hba.conf already allows non-TLS connections.

## Verification Plan

1. **Build verification**: `make build` succeeds
2. **Lint verification**: `make lint` passes (no unwrap/expect in new code)
3. **Unit tests**: `make test` passes
4. **Integration tests**: `make test-integration` passes
   - Database provisioning creates databases/roles
   - Upgrade operator performs version upgrade
   - TLS connections work (via rustls-improved in test code)
5. **Manual cluster testing**:
   - Deploy operator to kind/k3s cluster
   - Create PostgresCluster (TLS disabled for simplicity)
   - Create PostgresDatabase, verify role/database creation
   - Trigger upgrade, verify replication monitoring works
   - Verify sequence sync completes

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Port forward overhead per reconcile | Performance | Accept for now; can add connection caching later if measured |
| tokio-postgres-rustls-improved version stability | Build breaks | Pin exact version, test before merging |

**Confirmed safe**: No pg_hba.conf changes needed - existing config already uses `host all all samenet scram-sha-256` which allows non-TLS connections.

## Implementation Order

Recommended order to minimize breakage:

1. **Cargo.toml changes** - Add new deps, keep old ones temporarily
2. **Add new modules** - `port_forward.rs`, `postgres_client.rs`
3. **Rewrite sql.rs** - Core SQL functions using PostgresConnection
4. **Rewrite replication.rs** - Upgrade operator SQL
5. **Update reconcilers** - Use new connection pattern
6. **Update tests** - Switch to production modules
7. **Remove old code** - Delete exec_sql, test duplicates, old deps

## Not In Scope

- Connection pooling (can be added later)
- Async streaming for large result sets
- Multi-statement transactions (each operation is self-contained)
