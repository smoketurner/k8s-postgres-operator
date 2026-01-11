//! PostgreSQL connectivity integration tests
//!
//! These tests verify actual PostgreSQL connectivity through the operator-managed
//! services. They use port-forwarding to connect from the test runner to the
//! PostgreSQL pods.
//!
//! Each test verifies:
//! 1. Connectivity: SELECT 1 succeeds
//! 2. Version: PostgreSQL major version matches spec
//! 3. TLS status: ssl_is_used() returns expected value
//! 4. Role: Primary vs replica status
//!
//! Run with: cargo test --test integration connectivity -- --ignored --test-threads=1

use kube::api::PostParams;
use kube::Api;
use postgres_operator::crd::{PostgresCluster, PostgresVersion};

use crate::port_forward::{PortForward, PortForwardTarget};
use crate::postgres::{
    execute_sql, fetch_credentials, verify_connection_with_retry, CONNECT_RETRY_INTERVAL,
    MAX_CONNECT_RETRIES, POSTGRES_READY_TIMEOUT,
};
use crate::{
    cluster_operational, ensure_crd_installed, ensure_operator_running, wait_for_cluster,
    PostgresClusterBuilder, ScopedOperator, SharedTestCluster, TestNamespace,
};

/// Test context with operator and cluster client
struct TestContext {
    client: kube::Client,
    _operator: ScopedOperator,
    _cluster: std::sync::Arc<SharedTestCluster>,
}

/// Setup test context - starts operator and returns client
async fn setup() -> TestContext {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,kube=warn,postgres_operator=debug")
        .with_test_writer()
        .try_init();

    let cluster = SharedTestCluster::get()
        .await
        .expect("Failed to get cluster");

    ensure_crd_installed(&cluster)
        .await
        .expect("Failed to install CRD");

    let client = cluster.new_client().await.expect("Failed to create client");

    // Start the operator for this test
    let operator = ensure_operator_running(client.clone()).await;

    TestContext {
        client,
        _operator: operator,
        _cluster: cluster,
    }
}

// =============================================================================
// Connectivity Tests
// =============================================================================

/// Test: Single replica cluster (standalone.yaml equivalent)
///
/// Verifies:
/// - PostgreSQL version 16
/// - TLS disabled
/// - Is primary (not in recovery)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_standalone_connectivity() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "conn-standalone")
        .await
        .expect("create ns");

    // Create cluster: 1 replica, version 16, TLS disabled
    let pg = PostgresClusterBuilder::single("standalone", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for cluster to be operational
    tracing::info!("Waiting for cluster to become operational...");
    wait_for_cluster(&api, "standalone", cluster_operational(1), POSTGRES_READY_TIMEOUT)
        .await
        .expect("cluster should become operational");

    // Fetch credentials
    let credentials = fetch_credentials(&client, ns.name(), "standalone-credentials")
        .await
        .expect("fetch credentials");

    // Start port-forward
    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("standalone-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    // Verify connection (one-shot: connect, query, close)
    let info = verify_connection_with_retry(
        &credentials,
        "127.0.0.1",
        pf.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify connection");

    info.assert_version(16);
    info.assert_tls(false);
    info.assert_is_primary();

    tracing::info!(
        version = %info.pg_version,
        ssl = info.ssl_enabled,
        is_replica = info.is_replica,
        "Connectivity test passed"
    );
}

/// Test: Minimal dev cluster (dev-ephemeral.yaml equivalent)
///
/// Verifies:
/// - PostgreSQL version 17
/// - TLS disabled
/// - Is primary
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_dev_ephemeral_connectivity() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "conn-dev")
        .await
        .expect("create ns");

    // Create cluster: 1 replica, version 17, minimal resources
    let pg = PostgresClusterBuilder::single("dev-ephemeral", ns.name())
        .with_version(PostgresVersion::V17)
        .with_storage("1Gi", None)
        .with_resources_full("50m", "128Mi", "500m", "512Mi")
        .with_param("shared_buffers", "32MB")
        .with_param("max_connections", "20")
        .without_tls()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for cluster to be operational
    tracing::info!("Waiting for cluster to become operational...");
    wait_for_cluster(
        &api,
        "dev-ephemeral",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster should become operational");

    // Fetch credentials
    let credentials = fetch_credentials(&client, ns.name(), "dev-ephemeral-credentials")
        .await
        .expect("fetch credentials");

    // Start port-forward
    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("dev-ephemeral-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    // Verify connection (one-shot: connect, query, close)
    let info = verify_connection_with_retry(
        &credentials,
        "127.0.0.1",
        pf.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify connection");

    info.assert_version(17);
    info.assert_tls(false);
    info.assert_is_primary();

    tracing::info!(
        version = %info.pg_version,
        ssl = info.ssl_enabled,
        is_replica = info.is_replica,
        "Connectivity test passed"
    );
}

/// Test: HA cluster with 3 replicas (automatic-failover.yaml equivalent)
///
/// Verifies:
/// - Primary and replica services both work
/// - Primary is not in recovery
/// - Replica is in recovery
/// - TLS disabled
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_ha_cluster_connectivity() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "conn-ha")
        .await
        .expect("create ns");

    // Create HA cluster: 3 replicas, TLS disabled
    let pg = PostgresClusterBuilder::ha("ha-cluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_param("max_connections", "100")
        .without_tls()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for all 3 replicas to be ready
    tracing::info!("Waiting for HA cluster to become operational...");
    wait_for_cluster(&api, "ha-cluster", cluster_operational(3), POSTGRES_READY_TIMEOUT)
        .await
        .expect("cluster should become operational");

    // Fetch credentials
    let credentials = fetch_credentials(&client, ns.name(), "ha-cluster-credentials")
        .await
        .expect("fetch credentials");

    // Test primary service
    tracing::info!("Testing primary service...");
    let pf_primary = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("ha-cluster-primary", 5432),
        None,
    )
    .await
    .expect("start primary port-forward");

    let primary_info = verify_connection_with_retry(
        &credentials,
        "127.0.0.1",
        pf_primary.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify primary");

    primary_info.assert_version(16);
    primary_info.assert_tls(false);
    primary_info.assert_is_primary();

    tracing::info!(
        version = %primary_info.pg_version,
        ssl = primary_info.ssl_enabled,
        "Primary connectivity verified"
    );

    // Test replica service
    tracing::info!("Testing replica service...");
    let pf_replica = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("ha-cluster-repl", 5432),
        None,
    )
    .await
    .expect("start replica port-forward");

    let replica_info = verify_connection_with_retry(
        &credentials,
        "127.0.0.1",
        pf_replica.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify replica");

    replica_info.assert_version(16);
    replica_info.assert_tls(false);
    replica_info.assert_is_replica();
    replica_info.assert_lag_under(60.0); // Allow up to 60s lag for test

    tracing::info!(
        version = %replica_info.pg_version,
        ssl = replica_info.ssl_enabled,
        lag_secs = ?replica_info.replication_lag_secs,
        "Replica connectivity verified"
    );
}

/// Test: Primary + 1 replica (streaming-replicas.yaml equivalent)
///
/// Verifies:
/// - Primary accepts writes
/// - Replica is in recovery mode
/// - Replication lag is reasonable
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_streaming_replica_connectivity() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "conn-repl")
        .await
        .expect("create ns");

    // Create cluster: 2 replicas (primary + 1 streaming replica)
    let pg = PostgresClusterBuilder::new("streaming", ns.name())
        .with_version(PostgresVersion::V16)
        .with_replicas(2)
        .with_storage("1Gi", None)
        .with_param("max_connections", "100")
        .without_tls()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for both replicas to be ready
    tracing::info!("Waiting for streaming replica cluster to become operational...");
    wait_for_cluster(&api, "streaming", cluster_operational(2), POSTGRES_READY_TIMEOUT)
        .await
        .expect("cluster should become operational");

    // Fetch credentials
    let credentials = fetch_credentials(&client, ns.name(), "streaming-credentials")
        .await
        .expect("fetch credentials");

    // Test primary - verify it can accept writes
    tracing::info!("Testing primary service...");
    let pf_primary = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("streaming-primary", 5432),
        None,
    )
    .await
    .expect("start primary port-forward");

    let primary_info = verify_connection_with_retry(
        &credentials,
        "127.0.0.1",
        pf_primary.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify primary");

    primary_info.assert_version(16);
    primary_info.assert_tls(false);
    primary_info.assert_is_primary();

    // Test write capability on primary
    execute_sql(
        &credentials,
        "127.0.0.1",
        pf_primary.local_port(),
        "CREATE TABLE IF NOT EXISTS connectivity_test (id serial PRIMARY KEY)",
    )
    .await
    .expect("create table on primary");

    tracing::info!(
        version = %primary_info.pg_version,
        "Primary read-write capability verified"
    );

    // Test replica - verify it's read-only
    tracing::info!("Testing replica service...");
    let pf_replica = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("streaming-repl", 5432),
        None,
    )
    .await
    .expect("start replica port-forward");

    let replica_info = verify_connection_with_retry(
        &credentials,
        "127.0.0.1",
        pf_replica.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify replica");

    replica_info.assert_version(16);
    replica_info.assert_tls(false);
    replica_info.assert_is_replica();
    replica_info.assert_lag_under(60.0);

    tracing::info!(
        version = %replica_info.pg_version,
        lag_secs = ?replica_info.replication_lag_secs,
        "Replica read-only capability verified"
    );
}

/// Test: PgBouncer connection pooling (pgbouncer-pooler.yaml equivalent)
///
/// Verifies:
/// - Connection through PgBouncer pooler works
/// - Version matches expected
/// - TLS status is correct
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_pgbouncer_connectivity() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "conn-pgb")
        .await
        .expect("create ns");

    // Create cluster with PgBouncer: 3 replicas, 2 PgBouncer replicas
    let pg = PostgresClusterBuilder::ha("pgbouncer", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_pgbouncer()
        .without_tls()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for cluster to be operational
    tracing::info!("Waiting for PgBouncer cluster to become operational...");
    wait_for_cluster(&api, "pgbouncer", cluster_operational(3), POSTGRES_READY_TIMEOUT)
        .await
        .expect("cluster should become operational");

    // Also wait for PgBouncer to be ready
    tracing::info!("Waiting for PgBouncer to be ready...");
    wait_for_cluster(
        &api,
        "pgbouncer",
        crate::pgbouncer_ready(2),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("pgbouncer should become ready");

    // Fetch credentials
    let credentials = fetch_credentials(&client, ns.name(), "pgbouncer-credentials")
        .await
        .expect("fetch credentials");

    // Test direct connection to primary (port 5432)
    tracing::info!("Testing direct primary connection...");
    let pf_direct = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("pgbouncer-primary", 5432),
        None,
    )
    .await
    .expect("start direct port-forward");

    let direct_info = verify_connection_with_retry(
        &credentials,
        "127.0.0.1",
        pf_direct.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify direct connection");

    direct_info.assert_version(16);
    direct_info.assert_tls(false);
    direct_info.assert_is_primary();

    tracing::info!(
        version = %direct_info.pg_version,
        "Direct connection verified"
    );

    // Test connection through PgBouncer pooler (port 6432)
    tracing::info!("Testing PgBouncer pooler connection...");
    let pf_pooler = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("pgbouncer-pooler", 6432),
        None,
    )
    .await
    .expect("start pooler port-forward");

    let pooler_info = verify_connection_with_retry(
        &credentials,
        "127.0.0.1",
        pf_pooler.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify pooler connection");

    pooler_info.assert_version(16);
    pooler_info.assert_tls(false);
    // Connection through PgBouncer should still route to primary
    pooler_info.assert_is_primary();

    tracing::info!(
        version = %pooler_info.pg_version,
        "PgBouncer pooler connection verified"
    );
}
