//! TLS-enabled PostgreSQL connectivity integration tests
//!
//! These tests verify PostgreSQL connectivity with TLS enabled, testing
//! both direct connections and connections through PgBouncer.
//!
//! Prerequisites:
//! - Kubernetes cluster with cert-manager installed
//! - A ClusterIssuer named "selfsigned-issuer" (or set TEST_CLUSTER_ISSUER env var)
//!
//! Run with: cargo test --test integration tls -- --ignored

use kube::Api;
use kube::api::PostParams;
use postgres_operator::crd::{PostgresCluster, PostgresVersion};
use postgres_operator::resources::port_forward::{PortForward, PortForwardTarget};

use crate::postgres::{
    CONNECT_RETRY_INTERVAL, MAX_CONNECT_RETRIES, POSTGRES_READY_TIMEOUT, TlsMode,
    fetch_ca_certificate, fetch_credentials, get_test_cluster_issuer,
    verify_connection_with_retry_tls,
};
use crate::{
    PostgresClusterBuilder, ScopedOperator, SharedTestCluster, TestNamespace, cluster_operational,
    ensure_crd_installed, pgbouncer_ready, wait_for_cluster,
};

/// Initialize tracing and ensure CRD is installed
async fn init_test() -> std::sync::Arc<SharedTestCluster> {
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

    cluster
}

// =============================================================================
// TLS Connectivity Tests
// =============================================================================

/// Test: TLS-enabled standalone cluster (single replica)
///
/// Verifies:
/// - PostgreSQL version 16
/// - TLS enabled and working
/// - Is primary (not in recovery)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with cert-manager"]
async fn test_tls_standalone_connectivity() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "tls-standalone")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let issuer = get_test_cluster_issuer();
    tracing::info!(issuer = %issuer, "Using ClusterIssuer for TLS");

    // Create cluster: 1 replica, version 16, TLS enabled
    let pg = PostgresClusterBuilder::single("tls-single", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_tls(&issuer)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for cluster to be operational
    tracing::info!("Waiting for TLS cluster to become operational...");
    wait_for_cluster(
        &api,
        "tls-single",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster should become operational");

    // Fetch credentials
    let credentials = fetch_credentials(&client, ns.name(), "tls-single-credentials")
        .await
        .expect("fetch credentials");

    // Fetch CA certificate for TLS verification
    let ca_cert = fetch_ca_certificate(&client, ns.name(), "tls-single-tls")
        .await
        .expect("fetch CA certificate");

    tracing::info!("Fetched CA certificate, length: {} bytes", ca_cert.len());

    // Start port-forward
    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("tls-single-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    // Verify TLS connection
    // Note: Using RequireUnverified because the certificate's CN/SAN won't match
    // 127.0.0.1 when connecting via port-forward
    let info = verify_connection_with_retry_tls(
        &credentials,
        "127.0.0.1",
        pf.local_port(),
        &TlsMode::RequireUnverified,
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify TLS connection");

    info.assert_version(16);
    info.assert_tls(true);
    info.assert_is_primary();

    tracing::info!(
        version = %info.pg_version,
        ssl = info.ssl_enabled,
        ssl_version = ?info.ssl_version,
        is_replica = info.is_replica,
        "TLS standalone connectivity test passed"
    );
}

/// Test: TLS-enabled HA cluster (3 replicas)
///
/// Verifies:
/// - Primary and replica services both work with TLS
/// - Primary is not in recovery
/// - Replica is in recovery
/// - TLS enabled on both connections
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with cert-manager"]
async fn test_tls_ha_cluster_connectivity() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "tls-ha")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let issuer = get_test_cluster_issuer();
    tracing::info!(issuer = %issuer, "Using ClusterIssuer for TLS");

    // Create HA cluster: 3 replicas, TLS enabled
    let pg = PostgresClusterBuilder::ha("tls-ha", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_param("max_connections", "100")
        .with_tls(&issuer)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for all 3 replicas to be ready
    tracing::info!("Waiting for TLS HA cluster to become operational...");
    wait_for_cluster(
        &api,
        "tls-ha",
        cluster_operational(3),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster should become operational");

    // Fetch credentials
    let credentials = fetch_credentials(&client, ns.name(), "tls-ha-credentials")
        .await
        .expect("fetch credentials");

    // Test primary service with TLS
    tracing::info!("Testing primary service with TLS...");
    let pf_primary = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("tls-ha-primary", 5432),
        None,
    )
    .await
    .expect("start primary port-forward");

    let primary_info = verify_connection_with_retry_tls(
        &credentials,
        "127.0.0.1",
        pf_primary.local_port(),
        &TlsMode::RequireUnverified,
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify primary TLS connection");

    primary_info.assert_version(16);
    primary_info.assert_tls(true);
    primary_info.assert_is_primary();

    tracing::info!(
        version = %primary_info.pg_version,
        ssl = primary_info.ssl_enabled,
        ssl_version = ?primary_info.ssl_version,
        "Primary TLS connectivity verified"
    );

    // Test replica service with TLS
    tracing::info!("Testing replica service with TLS...");
    let pf_replica = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("tls-ha-repl", 5432),
        None,
    )
    .await
    .expect("start replica port-forward");

    let replica_info = verify_connection_with_retry_tls(
        &credentials,
        "127.0.0.1",
        pf_replica.local_port(),
        &TlsMode::RequireUnverified,
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify replica TLS connection");

    replica_info.assert_version(16);
    replica_info.assert_tls(true);
    replica_info.assert_is_replica();
    replica_info.assert_lag_under(60.0);

    tracing::info!(
        version = %replica_info.pg_version,
        ssl = replica_info.ssl_enabled,
        ssl_version = ?replica_info.ssl_version,
        lag_secs = ?replica_info.replication_lag_secs,
        "Replica TLS connectivity verified"
    );
}

/// Test: TLS-enabled cluster with PgBouncer
///
/// Verifies:
/// - Direct TLS connection to primary works
/// - TLS connection through PgBouncer pooler works
/// - Both connections report TLS enabled
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with cert-manager"]
async fn test_tls_pgbouncer_connectivity() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "tls-pgb")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let issuer = get_test_cluster_issuer();
    tracing::info!(issuer = %issuer, "Using ClusterIssuer for TLS");

    // Create cluster with PgBouncer: 3 replicas, 2 PgBouncer replicas, TLS enabled
    let pg = PostgresClusterBuilder::ha("tls-pgb", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_pgbouncer()
        .with_tls(&issuer)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for cluster to be operational
    tracing::info!("Waiting for TLS PgBouncer cluster to become operational...");
    wait_for_cluster(
        &api,
        "tls-pgb",
        cluster_operational(3),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster should become operational");

    // Also wait for PgBouncer to be ready
    tracing::info!("Waiting for PgBouncer to be ready...");
    wait_for_cluster(&api, "tls-pgb", pgbouncer_ready(2), POSTGRES_READY_TIMEOUT)
        .await
        .expect("pgbouncer should become ready");

    // Fetch credentials
    let credentials = fetch_credentials(&client, ns.name(), "tls-pgb-credentials")
        .await
        .expect("fetch credentials");

    // Test direct TLS connection to primary (port 5432)
    tracing::info!("Testing direct TLS connection to primary...");
    let pf_direct = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("tls-pgb-primary", 5432),
        None,
    )
    .await
    .expect("start direct port-forward");

    let direct_info = verify_connection_with_retry_tls(
        &credentials,
        "127.0.0.1",
        pf_direct.local_port(),
        &TlsMode::RequireUnverified,
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify direct TLS connection");

    direct_info.assert_version(16);
    direct_info.assert_tls(true);
    direct_info.assert_is_primary();

    tracing::info!(
        version = %direct_info.pg_version,
        ssl = direct_info.ssl_enabled,
        ssl_version = ?direct_info.ssl_version,
        "Direct TLS connection verified"
    );

    // Test TLS connection through PgBouncer pooler (port 6432)
    tracing::info!("Testing TLS connection through PgBouncer pooler...");
    let pf_pooler = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("tls-pgb-pooler", 6432),
        None,
    )
    .await
    .expect("start pooler port-forward");

    let pooler_info = verify_connection_with_retry_tls(
        &credentials,
        "127.0.0.1",
        pf_pooler.local_port(),
        &TlsMode::RequireUnverified,
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify pooler TLS connection");

    pooler_info.assert_version(16);
    // Note: pg_stat_ssl shows the PgBouncer->PostgreSQL connection, not client->PgBouncer.
    // When TLS is enabled on the cluster, PgBouncer uses TLS for its backend connection,
    // so ssl_enabled will be true.
    pooler_info.assert_tls(true);
    pooler_info.assert_is_primary();

    tracing::info!(
        version = %pooler_info.pg_version,
        ssl = pooler_info.ssl_enabled,
        ssl_version = ?pooler_info.ssl_version,
        "PgBouncer pooler TLS connection verified"
    );
}

/// Test: TLS-enabled cluster with PgBouncer replica pooler
///
/// Verifies:
/// - TLS connection through primary pooler routes to primary
/// - TLS connection through replica pooler routes to replicas
/// - Both pooler connections report TLS enabled
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with cert-manager"]
async fn test_tls_pgbouncer_replica_pooler() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "tls-pgb-repl")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let issuer = get_test_cluster_issuer();
    tracing::info!(issuer = %issuer, "Using ClusterIssuer for TLS");

    // Create cluster with PgBouncer replica pooler: 3 replicas, TLS enabled
    let pg = PostgresClusterBuilder::ha("tls-repl-pgb", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_pgbouncer_replica()
        .with_tls(&issuer)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for cluster to be operational
    tracing::info!("Waiting for TLS PgBouncer replica cluster to become operational...");
    wait_for_cluster(
        &api,
        "tls-repl-pgb",
        cluster_operational(3),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster should become operational");

    // Wait for PgBouncer to be ready (2 primary + 2 replica = 4 total)
    tracing::info!("Waiting for PgBouncer pools to be ready...");
    wait_for_cluster(
        &api,
        "tls-repl-pgb",
        pgbouncer_ready(4),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("pgbouncer should become ready");

    // Fetch credentials
    let credentials = fetch_credentials(&client, ns.name(), "tls-repl-pgb-credentials")
        .await
        .expect("fetch credentials");

    // Test TLS connection through primary pooler (port 6432)
    tracing::info!("Testing TLS connection through primary pooler...");
    let pf_primary_pooler = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("tls-repl-pgb-pooler", 6432),
        None,
    )
    .await
    .expect("start primary pooler port-forward");

    let primary_pooler_info = verify_connection_with_retry_tls(
        &credentials,
        "127.0.0.1",
        pf_primary_pooler.local_port(),
        &TlsMode::RequireUnverified,
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify primary pooler TLS connection");

    primary_pooler_info.assert_version(16);
    primary_pooler_info.assert_tls(true);
    primary_pooler_info.assert_is_primary();

    tracing::info!(
        version = %primary_pooler_info.pg_version,
        ssl = primary_pooler_info.ssl_enabled,
        ssl_version = ?primary_pooler_info.ssl_version,
        "Primary pooler TLS connection verified"
    );

    // Test TLS connection through replica pooler (port 6432)
    tracing::info!("Testing TLS connection through replica pooler...");
    let pf_replica_pooler = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("tls-repl-pgb-pooler-repl", 6432),
        None,
    )
    .await
    .expect("start replica pooler port-forward");

    let replica_pooler_info = verify_connection_with_retry_tls(
        &credentials,
        "127.0.0.1",
        pf_replica_pooler.local_port(),
        &TlsMode::RequireUnverified,
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify replica pooler TLS connection");

    replica_pooler_info.assert_version(16);
    replica_pooler_info.assert_tls(true);
    replica_pooler_info.assert_is_replica();
    replica_pooler_info.assert_lag_under(60.0);

    tracing::info!(
        version = %replica_pooler_info.pg_version,
        ssl = replica_pooler_info.ssl_enabled,
        ssl_version = ?replica_pooler_info.ssl_version,
        lag_secs = ?replica_pooler_info.replication_lag_secs,
        "Replica pooler TLS connection verified"
    );
}
