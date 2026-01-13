//! PostgresDatabase integration tests
//!
//! These tests verify the PostgresDatabase CRD functionality against a live
//! Kubernetes cluster. They test database provisioning, role creation, secret
//! generation, grants, extensions, and cleanup.
//!
//! Run with: cargo test --test integration database -- --ignored

use k8s_openapi::api::core::v1::Secret;
use kube::Api;
use kube::api::{DeleteParams, PostParams};
use postgres_operator::crd::{
    DatabaseConditionType, DatabasePhase, PostgresCluster, PostgresDatabase, PostgresVersion,
    TablePrivilege,
};

use crate::port_forward::{PortForward, PortForwardTarget};
use crate::postgres::{
    CONNECT_RETRY_INTERVAL, MAX_CONNECT_RETRIES, POSTGRES_READY_TIMEOUT, decode_secret_value,
    fetch_credentials, fetch_role_credentials, query_extensions, verify_connection_with_retry,
    verify_database_exists, verify_role_exists, verify_schema_usage,
};
use crate::{
    DATABASE_TIMEOUT, PostgresClusterBuilder, PostgresDatabaseBuilder, ScopedOperator,
    SharedTestCluster, TestNamespace, cluster_operational, database_ready, ensure_crd_installed,
    ensure_database_crd_installed, wait_for_cluster, wait_for_database, wait_for_database_deletion,
};

/// Initialize tracing and ensure both CRDs are installed
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
        .expect("Failed to install PostgresCluster CRD");

    ensure_database_crd_installed(&cluster)
        .await
        .expect("Failed to install PostgresDatabase CRD");

    cluster
}

// =============================================================================
// Test 1: Basic CRD Creation
// =============================================================================

/// Test: Verify basic PostgresDatabase resource creation
///
/// This test creates a PostgresDatabase resource and verifies it's accepted
/// by the Kubernetes API without needing a running cluster.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_resource_created() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-create")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create just the PostgresDatabase (no cluster yet)
    let db = PostgresDatabaseBuilder::new("testdb", ns.name(), "nonexistent-cluster")
        .with_database_name("mydb")
        .with_owner("mydb_owner")
        .build();

    let api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    let created = api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    // Verify the resource was created with correct spec
    assert_eq!(
        created.spec.database.name, "mydb",
        "database name should match"
    );
    assert_eq!(
        created.spec.database.owner, "mydb_owner",
        "owner should match"
    );
    assert_eq!(
        created.spec.cluster_ref.name, "nonexistent-cluster",
        "cluster ref should match"
    );

    tracing::info!("PostgresDatabase resource created successfully");
}

// =============================================================================
// Test 2: Waits for Cluster Ready
// =============================================================================

/// Test: Verify PostgresDatabase stays Pending when cluster is not Running
///
/// Creates a PostgresDatabase without a cluster and verifies it stays in
/// Pending phase with appropriate condition.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_waits_for_cluster_ready() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-pending")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create PostgresDatabase without a cluster
    let db = PostgresDatabaseBuilder::new("waitdb", ns.name(), "missing-cluster")
        .with_database_name("waitdb")
        .with_owner("waitdb_owner")
        .build();

    let api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &db)
        .await
        .expect("create database");

    // Wait briefly and check it's still pending
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let db = api.get("waitdb").await.expect("get database");
    let status = db.status.as_ref().expect("status should exist");

    assert_eq!(
        status.phase,
        DatabasePhase::Pending,
        "phase should be Pending when cluster doesn't exist"
    );

    tracing::info!("PostgresDatabase correctly stays in Pending state");
}

// =============================================================================
// Test 3: Creates Database
// =============================================================================

/// Test: Verify database is actually created in PostgreSQL
///
/// Creates a PostgresCluster and PostgresDatabase, then verifies the database
/// exists in PostgreSQL via SQL query.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_creates_database() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-creates")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create PostgresCluster first
    let pg = PostgresClusterBuilder::single("dbcluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for cluster to be operational
    tracing::info!("Waiting for cluster to become operational...");
    wait_for_cluster(
        &cluster_api,
        "dbcluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster should become operational");

    // Create PostgresDatabase
    let db = PostgresDatabaseBuilder::new("appdb", ns.name(), "dbcluster")
        .with_database_name("appdb")
        .with_owner("appdb_owner")
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    // Wait for database to be Ready
    tracing::info!("Waiting for database to become ready...");
    wait_for_database(&db_api, "appdb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database should become ready");

    // Verify database exists via SQL
    let credentials = fetch_credentials(&client, ns.name(), "dbcluster-credentials")
        .await
        .expect("fetch credentials");

    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("dbcluster-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    let exists = verify_database_exists(&credentials, "127.0.0.1", pf.local_port(), "appdb")
        .await
        .expect("check database exists");

    assert!(exists, "database 'appdb' should exist in PostgreSQL");

    tracing::info!("Database created and verified in PostgreSQL");
}

// =============================================================================
// Test 4: Creates Role with Secret
// =============================================================================

/// Test: Verify role creation and corresponding Kubernetes Secret
///
/// Creates a database with a role and verifies:
/// 1. The role exists in PostgreSQL
/// 2. A Kubernetes Secret is created with correct keys
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_creates_role_with_secret() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-role")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create cluster
    let pg = PostgresClusterBuilder::single("rolecluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    wait_for_cluster(
        &cluster_api,
        "rolecluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster operational");

    // Create database with role
    let db = PostgresDatabaseBuilder::new("roledb", ns.name(), "rolecluster")
        .with_database_name("roledb")
        .with_owner("roledb_owner")
        .with_role("appuser", "appuser-creds")
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    wait_for_database(&db_api, "roledb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database ready");

    // Verify Secret exists with all required keys
    let secrets: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    let secret = secrets.get("appuser-creds").await.expect("get secret");

    let data = secret.data.as_ref().expect("secret should have data");

    // PostgresDatabase secrets have 7 keys
    let required_keys = [
        "username",
        "password",
        "host",
        "port",
        "database",
        "connection-string",
        "jdbc-url",
    ];

    for key in &required_keys {
        assert!(
            data.contains_key(*key),
            "secret should contain key '{}'",
            key
        );
    }

    // Verify role exists in PostgreSQL
    let credentials = fetch_credentials(&client, ns.name(), "rolecluster-credentials")
        .await
        .expect("fetch credentials");

    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("rolecluster-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    let role_exists = verify_role_exists(&credentials, "127.0.0.1", pf.local_port(), "appuser")
        .await
        .expect("check role exists");

    assert!(role_exists, "role 'appuser' should exist in PostgreSQL");

    tracing::info!("Role created with Secret successfully");
}

// =============================================================================
// Test 5: Role Connectivity
// =============================================================================

/// Test: Verify generated credentials work for actual connections
///
/// Creates a database with a role and connects using the credentials
/// from the generated Secret.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_role_connectivity() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-conn")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create cluster
    let pg = PostgresClusterBuilder::single("conncluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    wait_for_cluster(
        &cluster_api,
        "conncluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster operational");

    // Create database with role
    let db = PostgresDatabaseBuilder::new("conndb", ns.name(), "conncluster")
        .with_database_name("conndb")
        .with_owner("conndb_owner")
        .with_role("connuser", "connuser-creds")
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    wait_for_database(&db_api, "conndb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database ready");

    // Fetch role credentials from the generated secret
    let role_creds = fetch_role_credentials(&client, ns.name(), "connuser-creds")
        .await
        .expect("fetch role credentials");

    // Port forward and connect using role credentials
    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("conncluster-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    // Verify we can connect with the role credentials
    let info = verify_connection_with_retry(
        &role_creds,
        "127.0.0.1",
        pf.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("connect with role credentials");

    info.assert_version(16);
    info.assert_is_primary();

    tracing::info!("Role connectivity verified successfully");
}

// =============================================================================
// Test 6: Creates Extensions
// =============================================================================

/// Test: Verify extension creation in the database
///
/// Creates a database with extensions and verifies they are installed.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_creates_extension() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-ext")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create cluster
    let pg = PostgresClusterBuilder::single("extcluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    wait_for_cluster(
        &cluster_api,
        "extcluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster operational");

    // Create database with extensions
    let db = PostgresDatabaseBuilder::new("extdb", ns.name(), "extcluster")
        .with_database_name("extdb")
        .with_owner("extdb_owner")
        .with_extension("uuid-ossp")
        .with_extension("pg_trgm")
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    wait_for_database(&db_api, "extdb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database ready");

    // Connect to the specific database to check extensions
    let credentials = fetch_credentials(&client, ns.name(), "extcluster-credentials")
        .await
        .expect("fetch credentials");

    // Modify credentials to connect to the new database
    let db_creds = crate::postgres::PostgresCredentials {
        username: credentials.username,
        password: credentials.password,
        database: "extdb".to_string(),
    };

    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("extcluster-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    let extensions = query_extensions(&db_creds, "127.0.0.1", pf.local_port())
        .await
        .expect("query extensions");

    assert!(
        extensions.contains(&"uuid-ossp".to_string()),
        "uuid-ossp extension should be installed"
    );
    assert!(
        extensions.contains(&"pg_trgm".to_string()),
        "pg_trgm extension should be installed"
    );

    tracing::info!("Extensions created successfully: {:?}", extensions);
}

// =============================================================================
// Test 7: Applies Grants
// =============================================================================

/// Test: Verify grants are correctly applied
///
/// Creates a database with a role and grant, then verifies the grant is applied.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_applies_grants() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-grant")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create cluster
    let pg = PostgresClusterBuilder::single("grantcluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    wait_for_cluster(
        &cluster_api,
        "grantcluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster operational");

    // Create database with role and grant
    let db = PostgresDatabaseBuilder::new("grantdb", ns.name(), "grantcluster")
        .with_database_name("grantdb")
        .with_owner("grantdb_owner")
        .with_role("reader", "reader-creds")
        .with_grant("reader", "public", vec![TablePrivilege::Select], true)
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    wait_for_database(&db_api, "grantdb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database ready");

    // Verify grant was applied (USAGE on schema is granted as part of grant application)
    let credentials = fetch_credentials(&client, ns.name(), "grantcluster-credentials")
        .await
        .expect("fetch credentials");

    let db_creds = crate::postgres::PostgresCredentials {
        username: credentials.username,
        password: credentials.password,
        database: "grantdb".to_string(),
    };

    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("grantcluster-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    let has_usage =
        verify_schema_usage(&db_creds, "127.0.0.1", pf.local_port(), "reader", "public")
            .await
            .expect("check schema usage");

    assert!(
        has_usage,
        "role 'reader' should have USAGE on 'public' schema"
    );

    tracing::info!("Grants applied successfully");
}

// =============================================================================
// Test 8: Credentials Secret Contents
// =============================================================================

/// Test: Detailed verification of secret content format
///
/// Creates a database with a role and verifies all secret fields have
/// correct format.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_credentials_secret_contents() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-secret")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create cluster
    let pg = PostgresClusterBuilder::single("secretcluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    wait_for_cluster(
        &cluster_api,
        "secretcluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster operational");

    // Create database with role
    let db = PostgresDatabaseBuilder::new("secretdb", ns.name(), "secretcluster")
        .with_database_name("secretdb")
        .with_owner("secretdb_owner")
        .with_role("secretuser", "secretuser-creds")
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    wait_for_database(&db_api, "secretdb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database ready");

    // Fetch and verify secret contents
    let secrets: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    let secret = secrets.get("secretuser-creds").await.expect("get secret");

    let data = secret.data.as_ref().expect("secret should have data");

    // Verify username
    let username = decode_secret_value(data, "username").expect("username");
    assert_eq!(username, "secretuser", "username should be role name");

    // Verify password is non-empty
    let password = decode_secret_value(data, "password").expect("password");
    assert!(!password.is_empty(), "password should not be empty");

    // Verify host ends with .svc (K8s service DNS)
    let host = decode_secret_value(data, "host").expect("host");
    assert!(host.ends_with(".svc"), "host should end with .svc");
    assert!(
        host.contains("secretcluster-primary"),
        "host should contain cluster name"
    );

    // Verify port
    let port = decode_secret_value(data, "port").expect("port");
    assert_eq!(port, "5432", "port should be 5432");

    // Verify database
    let database = decode_secret_value(data, "database").expect("database");
    assert_eq!(database, "secretdb", "database should match");

    // Verify connection-string format
    let conn_string = decode_secret_value(data, "connection-string").expect("connection-string");
    assert!(
        conn_string.starts_with("postgresql://"),
        "connection-string should start with postgresql://"
    );
    assert!(
        conn_string.contains("sslmode="),
        "connection-string should contain sslmode"
    );

    // Verify jdbc-url format
    let jdbc_url = decode_secret_value(data, "jdbc-url").expect("jdbc-url");
    assert!(
        jdbc_url.starts_with("jdbc:postgresql://"),
        "jdbc-url should start with jdbc:postgresql://"
    );

    tracing::info!("Secret contents verified successfully");
}

// =============================================================================
// Test 9: Deletion Cleanup
// =============================================================================

/// Test: Verify database and roles are dropped on PostgresDatabase deletion
///
/// Creates a database with a role, deletes the PostgresDatabase resource,
/// and verifies the database and role are dropped from PostgreSQL.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_deletion_cleanup() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-delete")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create cluster
    let pg = PostgresClusterBuilder::single("deletecluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    wait_for_cluster(
        &cluster_api,
        "deletecluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster operational");

    // Create database with role
    let db = PostgresDatabaseBuilder::new("deletedb", ns.name(), "deletecluster")
        .with_database_name("deletedb")
        .with_owner("deletedb_owner")
        .with_role("deleteuser", "deleteuser-creds")
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    wait_for_database(&db_api, "deletedb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database ready");

    // Verify database and role exist before deletion
    let credentials = fetch_credentials(&client, ns.name(), "deletecluster-credentials")
        .await
        .expect("fetch credentials");

    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("deletecluster-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    let db_exists = verify_database_exists(&credentials, "127.0.0.1", pf.local_port(), "deletedb")
        .await
        .expect("check database");
    assert!(db_exists, "database should exist before deletion");

    let role_exists = verify_role_exists(&credentials, "127.0.0.1", pf.local_port(), "deleteuser")
        .await
        .expect("check role");
    assert!(role_exists, "role should exist before deletion");

    // Delete PostgresDatabase
    tracing::info!("Deleting PostgresDatabase...");
    db_api
        .delete("deletedb", &DeleteParams::default())
        .await
        .expect("delete database");

    // Wait for deletion (finalizer should run)
    wait_for_database_deletion(&db_api, "deletedb", DATABASE_TIMEOUT)
        .await
        .expect("database should be deleted");

    // Verify database and role are dropped
    let db_exists_after =
        verify_database_exists(&credentials, "127.0.0.1", pf.local_port(), "deletedb")
            .await
            .expect("check database after deletion");
    assert!(
        !db_exists_after,
        "database should be dropped after deletion"
    );

    let role_exists_after =
        verify_role_exists(&credentials, "127.0.0.1", pf.local_port(), "deleteuser")
            .await
            .expect("check role after deletion");
    assert!(!role_exists_after, "role should be dropped after deletion");

    // Verify secret is garbage collected
    let secrets: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let secret_result = secrets.get("deleteuser-creds").await;
    assert!(
        secret_result.is_err(),
        "secret should be garbage collected after deletion"
    );

    tracing::info!("Deletion cleanup verified successfully");
}

// =============================================================================
// Test 10: Owner Reference
// =============================================================================

/// Test: Verify secrets have correct owner reference for garbage collection
///
/// Creates a database with a role and verifies the secret has proper
/// owner references pointing to the PostgresDatabase resource.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_owner_reference() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-owner")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create cluster
    let pg = PostgresClusterBuilder::single("ownercluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    wait_for_cluster(
        &cluster_api,
        "ownercluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster operational");

    // Create database with role
    let db = PostgresDatabaseBuilder::new("ownerdb", ns.name(), "ownercluster")
        .with_database_name("ownerdb")
        .with_owner("ownerdb_owner")
        .with_role("owneruser", "owneruser-creds")
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    wait_for_database(&db_api, "ownerdb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database ready");

    // Get the PostgresDatabase UID
    let db_resource = db_api.get("ownerdb").await.expect("get database");
    let db_uid = db_resource
        .metadata
        .uid
        .as_ref()
        .expect("database should have UID");

    // Verify secret has owner reference
    let secrets: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    let secret = secrets.get("owneruser-creds").await.expect("get secret");

    let owner_refs = secret
        .metadata
        .owner_references
        .as_ref()
        .expect("secret should have owner references");

    assert!(
        !owner_refs.is_empty(),
        "secret should have owner references"
    );

    let owner_ref = &owner_refs[0];
    assert_eq!(owner_ref.kind, "PostgresDatabase", "kind should match");
    assert_eq!(owner_ref.name, "ownerdb", "name should match");
    assert_eq!(&owner_ref.uid, db_uid, "UID should match");
    assert!(
        owner_ref.controller.unwrap_or(false),
        "controller should be true"
    );
    assert!(
        owner_ref.block_owner_deletion.unwrap_or(false),
        "blockOwnerDeletion should be true"
    );

    tracing::info!("Owner reference verified successfully");
}

// =============================================================================
// Test 11: Status Connection Info
// =============================================================================

/// Test: Verify status.connectionInfo is properly populated
///
/// Creates a database and verifies the status contains connection info.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_status_connection_info() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-status")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create cluster
    let pg = PostgresClusterBuilder::single("statuscluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    wait_for_cluster(
        &cluster_api,
        "statuscluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster operational");

    // Create database with role
    let db = PostgresDatabaseBuilder::new("statusdb", ns.name(), "statuscluster")
        .with_database_name("statusdb")
        .with_owner("statusdb_owner")
        .with_role("statususer", "statususer-creds")
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    wait_for_database(&db_api, "statusdb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database ready");

    // Verify status fields
    let db_resource = db_api.get("statusdb").await.expect("get database");
    let status = db_resource.status.as_ref().expect("status should exist");

    // Verify connection info
    let conn_info = status
        .connection_info
        .as_ref()
        .expect("connectionInfo should exist");
    assert!(
        conn_info
            .host
            .contains(&format!("statuscluster-primary.{}", ns.name())),
        "host should contain cluster-primary.namespace"
    );
    assert_eq!(conn_info.port, 5432, "port should be 5432");
    assert_eq!(conn_info.database, "statusdb", "database should match spec");

    // Verify credential secrets list
    assert!(
        status
            .credential_secrets
            .contains(&"statususer-creds".to_string()),
        "credentialSecrets should contain role secret"
    );

    // Verify Ready condition
    let ready_condition = status
        .conditions
        .iter()
        .find(|c| c.condition_type == DatabaseConditionType::Ready);
    assert!(ready_condition.is_some(), "Ready condition should exist");
    assert_eq!(
        ready_condition.unwrap().status,
        "True",
        "Ready condition should be True"
    );

    tracing::info!("Status connection info verified successfully");
}

// =============================================================================
// Test 12: Multiple Roles
// =============================================================================

/// Test: Verify multiple roles can be created with distinct secrets
///
/// Creates a database with 3 roles and verifies:
/// 1. All secrets are created
/// 2. All roles exist in PostgreSQL
/// 3. Status lists all secrets
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_database_multiple_roles() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "db-multi")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_database(client.clone(), ns.name()).await;

    // Create cluster
    let pg = PostgresClusterBuilder::single("multicluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    wait_for_cluster(
        &cluster_api,
        "multicluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("cluster operational");

    // Create database with 3 roles
    let db = PostgresDatabaseBuilder::new("multidb", ns.name(), "multicluster")
        .with_database_name("multidb")
        .with_owner("multidb_owner")
        .with_role("admin", "admin-creds")
        .with_role("reader", "reader-creds")
        .with_role("writer", "writer-creds")
        .build();

    let db_api: Api<PostgresDatabase> = Api::namespaced(client.clone(), ns.name());
    db_api
        .create(&PostParams::default(), &db)
        .await
        .expect("create database");

    wait_for_database(&db_api, "multidb", database_ready(), DATABASE_TIMEOUT)
        .await
        .expect("database ready");

    // Verify all secrets exist
    let secrets: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    for secret_name in &["admin-creds", "reader-creds", "writer-creds"] {
        let secret_result = secrets.get(secret_name).await;
        assert!(
            secret_result.is_ok(),
            "secret '{}' should exist",
            secret_name
        );
    }

    // Verify all roles exist in PostgreSQL
    let credentials = fetch_credentials(&client, ns.name(), "multicluster-credentials")
        .await
        .expect("fetch credentials");

    let pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("multicluster-primary", 5432),
        None,
    )
    .await
    .expect("start port-forward");

    for role_name in &["admin", "reader", "writer"] {
        let exists = verify_role_exists(&credentials, "127.0.0.1", pf.local_port(), role_name)
            .await
            .expect("check role");
        assert!(exists, "role '{}' should exist in PostgreSQL", role_name);
    }

    // Verify status lists all secrets
    let db_resource = db_api.get("multidb").await.expect("get database");
    let status = db_resource.status.as_ref().expect("status should exist");

    assert_eq!(
        status.credential_secrets.len(),
        3,
        "should have 3 credential secrets"
    );

    tracing::info!("Multiple roles verified successfully");
}
