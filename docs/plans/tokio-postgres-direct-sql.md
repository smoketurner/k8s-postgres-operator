# Plan: Adopt tokio-postgres for Direct SQL Connections

> **Status**: Parked - implement after upgrade operator testing is complete

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

## Benefits of Migration

1. **Code simplification**: Single SQL execution path for tests and production
2. **Prepared statements**: Type-safe parameter binding, no SQL injection risk from escaping bugs
3. **Typed results**: No manual string parsing - `row.get::<_, i64>("lag_bytes")` instead of `line.split('|').get(2).parse()`
4. **Better error handling**: PostgreSQL error codes instead of string-matching "ERROR" in stderr
5. **Remove dead code**: Delete `exec_sql()`, `escape_sql_string_pub()`, `quote_identifier_pub()`, and manual parsing logic

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

### Phase 7: Refactor Tests to Use Production Modules

**Goal**: Tests use the exact same code paths as production, eliminating duplication.

**Files to delete** (moved to `src/resources/`):
- `tests/integration/postgres.rs`
- `tests/integration/port_forward.rs`

**Files to update**:
- `tests/integration/main.rs` - Update imports
- `tests/integration/database_tests.rs` - Use `PostgresConnection`
- `tests/integration/upgrade_tests.rs` - Use `PostgresConnection`
- `tests/integration/connectivity_tests.rs` - Use `PostgresConnection`
- `tests/integration/tls_tests.rs` - Keep TLS-specific helpers, use production for base

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

**TLS tests** (`tls_tests.rs`):

These tests verify that TLS works for external clients. They will continue to use `tokio-postgres-rustls-improved` to make TLS connections and verify SSL is active:

```rust
use tokio_postgres_rustls_improved::MakeRustlsConnect;

async fn test_tls_connection() {
    let pf = PortForward::start(...).await?;  // From production module
    let tls = build_tls_connector(&ca_cert)?; // Use rustls-improved for TLS tests
    let (client, conn) = tokio_postgres::connect(&config, tls).await?;
    // Verify ssl_enabled = true
}
```

## Critical Files to Modify

| File | Action |
|------|--------|
| `Cargo.toml` | Move deps, replace `tokio-postgres-rustls` with `tokio-postgres-rustls-improved` |
| `src/resources/mod.rs` | Export new modules |
| `src/resources/port_forward.rs` | **NEW** - move from `tests/integration/port_forward.rs` |
| `src/resources/postgres_client.rs` | **NEW** - merge from `tests/integration/postgres.rs` |
| `src/resources/replication.rs` | **REWRITE** - use prepared statements |
| `src/resources/sql.rs` | **REWRITE** - remove exec_sql, use PostgresConnection |
| `src/controller/upgrade_reconciler.rs` | Update to use PostgresConnection |
| `src/controller/database_reconciler.rs` | Update to use PostgresConnection |
| `tests/integration/postgres.rs` | **DELETE** - use production module |
| `tests/integration/port_forward.rs` | **DELETE** - use production module |
| `tests/integration/*.rs` | Update imports to use `crate::resources::*` |

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
