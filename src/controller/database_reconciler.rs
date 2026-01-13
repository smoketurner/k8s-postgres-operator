//! Database reconciler for PostgresDatabase resources
//!
//! This reconciler provisions databases, roles, and grants within PostgresCluster instances.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Client, Resource, ResourceExt};
use tracing::{debug, error, info, warn};

use crate::crd::{
    ClusterPhase, DatabaseCondition, DatabaseConditionType, DatabaseConnectionInfo, DatabasePhase,
    GrantSpec, PostgresCluster, PostgresDatabase, PostgresDatabaseStatus, RoleSpec,
};
use crate::resources::sql::{
    self, SqlError, create_extension, drop_database, drop_role, ensure_database, ensure_role,
    generate_password, grant_privileges,
};

/// Context for the database reconciler
pub struct DatabaseContext {
    pub client: Client,
}

impl DatabaseContext {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// Error type for database reconciliation
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("SQL execution error: {0}")]
    SqlError(#[from] SqlError),

    #[error("Referenced cluster not found: {0}/{1}")]
    ClusterNotFound(String, String),

    #[error("Cluster not ready: {0}/{1} is in phase {2}")]
    ClusterNotReady(String, String, String),

    #[error("Missing cluster name in metadata")]
    MissingClusterName,

    #[error("Missing namespace in metadata")]
    MissingNamespace,

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

/// Result type for database reconciliation
pub type Result<T, E = DatabaseError> = std::result::Result<T, E>;

/// Finalizer for PostgresDatabase resources
pub(crate) const DATABASE_FINALIZER: &str =
    "postgresdatabase.postgres-operator.smoketurner.com/finalizer";

/// Reconcile a PostgresDatabase resource
pub async fn reconcile_database(
    db: Arc<PostgresDatabase>,
    ctx: Arc<DatabaseContext>,
) -> Result<Action> {
    let name = db.name_any();
    let namespace = db.namespace().ok_or(DatabaseError::MissingNamespace)?;

    debug!(name = %name, namespace = %namespace, "Reconciling PostgresDatabase");

    // Check if being deleted
    if db.metadata.deletion_timestamp.is_some() {
        return handle_deletion(&db, &ctx, &namespace).await;
    }

    // Ensure finalizer is set
    if !has_finalizer(&db) {
        return add_finalizer(&db, &ctx, &namespace).await;
    }

    // Get the referenced cluster
    let cluster = match get_referenced_cluster(&db, &ctx, &namespace).await {
        Ok(cluster) => cluster,
        Err(DatabaseError::ClusterNotFound(ns, cluster_name)) => {
            info!(
                name = %name,
                cluster = %cluster_name,
                "Waiting for cluster to exist"
            );

            // Update status to show we're waiting for cluster
            update_status(
                &db,
                &ctx,
                &namespace,
                DatabasePhase::Pending,
                vec![DatabaseCondition {
                    condition_type: DatabaseConditionType::ClusterReady,
                    status: "False".to_string(),
                    last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                    reason: Some("ClusterNotFound".to_string()),
                    message: Some(format!("Cluster {}/{} not found", ns, cluster_name)),
                }],
                None,
                vec![],
            )
            .await?;

            // Requeue to check again
            return Ok(Action::requeue(Duration::from_secs(10)));
        }
        Err(e) => return Err(e),
    };

    // Check if cluster is ready
    let cluster_phase = cluster
        .status
        .as_ref()
        .map(|s| &s.phase)
        .unwrap_or(&ClusterPhase::Pending);

    if *cluster_phase != ClusterPhase::Running {
        info!(
            name = %name,
            cluster = %db.spec.cluster_ref.name,
            phase = %cluster_phase,
            "Waiting for cluster to be ready"
        );

        // Update status to show we're waiting
        update_status(
            &db,
            &ctx,
            &namespace,
            DatabasePhase::Pending,
            vec![DatabaseCondition {
                condition_type: DatabaseConditionType::ClusterReady,
                status: "False".to_string(),
                last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                reason: Some("ClusterNotReady".to_string()),
                message: Some(format!(
                    "Cluster {} is in phase {}",
                    db.spec.cluster_ref.name, cluster_phase
                )),
            }],
            None,
            vec![],
        )
        .await?;

        // Requeue to check again
        return Ok(Action::requeue(Duration::from_secs(10)));
    }

    // Provision the database
    let result = provision_database(&db, &ctx, &cluster, &namespace).await;

    match result {
        Ok(secrets) => {
            // Generate connection info
            let connection_info = DatabaseConnectionInfo {
                host: format!("{}-primary.{}.svc", cluster.name_any(), namespace),
                port: 5432,
                database: db.spec.database.name.clone(),
            };

            // Update status to Ready
            update_status(
                &db,
                &ctx,
                &namespace,
                DatabasePhase::Ready,
                vec![
                    DatabaseCondition {
                        condition_type: DatabaseConditionType::ClusterReady,
                        status: "True".to_string(),
                        last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                        reason: Some("ClusterRunning".to_string()),
                        message: None,
                    },
                    DatabaseCondition {
                        condition_type: DatabaseConditionType::DatabaseCreated,
                        status: "True".to_string(),
                        last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                        reason: Some("DatabaseProvisioned".to_string()),
                        message: None,
                    },
                    DatabaseCondition {
                        condition_type: DatabaseConditionType::RolesCreated,
                        status: "True".to_string(),
                        last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                        reason: Some("RolesProvisioned".to_string()),
                        message: None,
                    },
                    DatabaseCondition {
                        condition_type: DatabaseConditionType::SecretsCreated,
                        status: "True".to_string(),
                        last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                        reason: Some("SecretsCreated".to_string()),
                        message: None,
                    },
                    DatabaseCondition {
                        condition_type: DatabaseConditionType::Ready,
                        status: "True".to_string(),
                        last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                        reason: Some("Ready".to_string()),
                        message: None,
                    },
                ],
                Some(connection_info),
                secrets,
            )
            .await?;

            info!(name = %name, namespace = %namespace, "PostgresDatabase is ready");
            Ok(Action::requeue(Duration::from_secs(300))) // Recheck every 5 minutes
        }
        Err(e) => {
            error!(name = %name, namespace = %namespace, error = %e, "Failed to provision database");

            // Update status to Failed
            update_status(
                &db,
                &ctx,
                &namespace,
                DatabasePhase::Failed,
                vec![DatabaseCondition {
                    condition_type: DatabaseConditionType::Ready,
                    status: "False".to_string(),
                    last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                    reason: Some("ProvisioningFailed".to_string()),
                    message: Some(e.to_string()),
                }],
                None,
                vec![],
            )
            .await?;

            // Requeue with backoff
            Ok(Action::requeue(Duration::from_secs(30)))
        }
    }
}

/// Provision the database, roles, grants, and extensions
async fn provision_database(
    db: &PostgresDatabase,
    ctx: &DatabaseContext,
    cluster: &PostgresCluster,
    namespace: &str,
) -> Result<Vec<String>> {
    let cluster_name = cluster.name_any();
    let db_name = &db.spec.database.name;
    let owner = &db.spec.database.owner;

    info!(
        database = %db_name,
        cluster = %cluster_name,
        namespace = %namespace,
        "Provisioning database"
    );

    // First, ensure the owner role exists (create with a temporary password if needed)
    // The owner needs to exist before we can create the database
    let owner_exists = sql::role_exists(&ctx.client, namespace, &cluster_name, owner).await?;
    if !owner_exists {
        debug!(role = %owner, "Creating owner role");
        let temp_password = generate_password();
        sql::create_role(
            &ctx.client,
            namespace,
            &cluster_name,
            owner,
            &temp_password,
            &[],
            None,
            true,
        )
        .await?;
    }

    // Create the database
    ensure_database(
        &ctx.client,
        namespace,
        &cluster_name,
        db_name,
        owner,
        db.spec.database.encoding.as_deref(),
        db.spec.database.locale.as_deref(),
        db.spec.database.connection_limit,
    )
    .await?;

    // Create extensions
    for extension in &db.spec.extensions {
        debug!(extension = %extension, database = %db_name, "Creating extension");
        create_extension(&ctx.client, namespace, &cluster_name, db_name, extension).await?;
    }

    // Create roles and secrets
    let mut created_secrets = Vec::new();
    for role_spec in &db.spec.roles {
        let secret_name =
            create_role_with_secret(db, ctx, cluster, namespace, db_name, role_spec).await?;
        created_secrets.push(secret_name);
    }

    // Apply grants
    for grant in &db.spec.grants {
        apply_grant(&ctx.client, namespace, &cluster_name, db_name, grant).await?;
    }

    Ok(created_secrets)
}

/// Create a role and its credential secret
async fn create_role_with_secret(
    db: &PostgresDatabase,
    ctx: &DatabaseContext,
    cluster: &PostgresCluster,
    namespace: &str,
    db_name: &str,
    role_spec: &RoleSpec,
) -> Result<String> {
    let cluster_name = cluster.name_any();
    let role_name = &role_spec.name;
    let secret_name = &role_spec.secret_name;

    // Check if secret already exists to get existing password
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let existing_password = match secrets.get(secret_name).await {
        Ok(secret) => secret
            .data
            .as_ref()
            .and_then(|d| d.get("password"))
            .map(|p| String::from_utf8_lossy(&p.0).to_string()),
        Err(_) => None,
    };

    // Use existing password or generate new one
    let password = existing_password.unwrap_or_else(generate_password);

    // Build privileges list
    let privileges: Vec<String> = role_spec
        .privileges
        .iter()
        .map(|p| p.as_sql().to_string())
        .collect();

    // Create or update the role
    ensure_role(
        &ctx.client,
        namespace,
        &cluster_name,
        role_name,
        &password,
        &privileges,
        role_spec.connection_limit,
        role_spec.login,
    )
    .await?;

    // Create or update the credential secret
    let host = format!("{}-primary.{}.svc", cluster_name, namespace);
    let port = 5432;

    let connection_string = format!(
        "postgresql://{}:{}@{}:{}/{}?sslmode=require",
        role_name, password, host, port, db_name
    );

    let jdbc_url = format!(
        "jdbc:postgresql://{}:{}/{}?user={}&password={}&ssl=true",
        host, port, db_name, role_name, password
    );

    let secret = Secret {
        metadata: kube::api::ObjectMeta {
            name: Some(secret_name.clone()),
            namespace: Some(namespace.to_string()),
            owner_references: Some(vec![owner_reference_for_database(db)]),
            labels: Some(
                [
                    (
                        "postgres-operator.smoketurner.com/database".to_string(),
                        db.name_any(),
                    ),
                    (
                        "postgres-operator.smoketurner.com/cluster".to_string(),
                        cluster_name.clone(),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        },
        type_: Some("Opaque".to_string()),
        data: Some(
            [
                (
                    "username".to_string(),
                    ByteString(role_name.as_bytes().to_vec()),
                ),
                (
                    "password".to_string(),
                    ByteString(password.as_bytes().to_vec()),
                ),
                ("host".to_string(), ByteString(host.as_bytes().to_vec())),
                (
                    "port".to_string(),
                    ByteString(port.to_string().as_bytes().to_vec()),
                ),
                (
                    "database".to_string(),
                    ByteString(db_name.as_bytes().to_vec()),
                ),
                (
                    "connection-string".to_string(),
                    ByteString(connection_string.as_bytes().to_vec()),
                ),
                (
                    "jdbc-url".to_string(),
                    ByteString(jdbc_url.as_bytes().to_vec()),
                ),
            ]
            .into_iter()
            .collect(),
        ),
        ..Default::default()
    };

    secrets
        .patch(
            secret_name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Apply(&secret),
        )
        .await?;

    info!(secret = %secret_name, role = %role_name, "Created credential secret");
    Ok(secret_name.clone())
}

/// Apply a grant specification
async fn apply_grant(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    db_name: &str,
    grant: &GrantSpec,
) -> Result<()> {
    let privileges: Vec<String> = grant
        .privileges
        .iter()
        .map(|p| p.as_sql().to_string())
        .collect();

    if !privileges.is_empty() && grant.all_tables {
        grant_privileges(
            client,
            namespace,
            cluster_name,
            db_name,
            &grant.role,
            &grant.schema,
            &privileges,
            true,
        )
        .await?;
    }

    // Grant USAGE on schema
    let usage_sql = format!(
        "GRANT USAGE ON SCHEMA {} TO {}",
        sql::quote_identifier_pub(&grant.schema),
        sql::quote_identifier_pub(&grant.role)
    );
    sql::exec_sql(client, namespace, cluster_name, db_name, &usage_sql).await?;

    Ok(())
}

/// Handle deletion of a PostgresDatabase resource
async fn handle_deletion(
    db: &PostgresDatabase,
    ctx: &DatabaseContext,
    namespace: &str,
) -> Result<Action> {
    let name = db.name_any();
    info!(name = %name, namespace = %namespace, "Handling PostgresDatabase deletion");

    // Get the cluster reference
    let cluster_result = get_referenced_cluster(db, ctx, namespace).await;

    if let Ok(cluster) = cluster_result {
        let cluster_name = cluster.name_any();

        // Check if cluster is still running
        let cluster_phase = cluster
            .status
            .as_ref()
            .map(|s| &s.phase)
            .unwrap_or(&ClusterPhase::Pending);

        if *cluster_phase == ClusterPhase::Running {
            // Drop roles first (they depend on the database)
            for role_spec in &db.spec.roles {
                if let Err(e) =
                    drop_role(&ctx.client, namespace, &cluster_name, &role_spec.name).await
                {
                    warn!(role = %role_spec.name, error = %e, "Failed to drop role during cleanup");
                }
            }

            // Drop the owner role if it was created by us
            if let Err(e) = drop_role(
                &ctx.client,
                namespace,
                &cluster_name,
                &db.spec.database.owner,
            )
            .await
            {
                warn!(role = %db.spec.database.owner, error = %e, "Failed to drop owner role during cleanup");
            }

            // Drop the database
            if let Err(e) = drop_database(
                &ctx.client,
                namespace,
                &cluster_name,
                &db.spec.database.name,
            )
            .await
            {
                warn!(database = %db.spec.database.name, error = %e, "Failed to drop database during cleanup");
            }
        } else {
            warn!(
                cluster = %cluster_name,
                phase = %cluster_phase,
                "Cluster not running, skipping database cleanup"
            );
        }
    } else {
        warn!("Referenced cluster not found, skipping database cleanup");
    }

    // Remove finalizer
    remove_finalizer(db, ctx, namespace).await?;

    Ok(Action::await_change())
}

/// Get the referenced PostgresCluster
async fn get_referenced_cluster(
    db: &PostgresDatabase,
    ctx: &DatabaseContext,
    namespace: &str,
) -> Result<PostgresCluster> {
    let cluster_namespace = db
        .spec
        .cluster_ref
        .namespace
        .as_deref()
        .unwrap_or(namespace);
    let cluster_name = &db.spec.cluster_ref.name;

    let clusters: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), cluster_namespace);

    clusters.get(cluster_name).await.map_err(|e| {
        if matches!(e, kube::Error::Api(ref ae) if ae.code == 404) {
            DatabaseError::ClusterNotFound(cluster_namespace.to_string(), cluster_name.clone())
        } else {
            DatabaseError::KubeError(e)
        }
    })
}

/// Check if the resource has the finalizer
fn has_finalizer(db: &PostgresDatabase) -> bool {
    db.metadata
        .finalizers
        .as_ref()
        .map(|f| f.contains(&DATABASE_FINALIZER.to_string()))
        .unwrap_or(false)
}

/// Add finalizer to the resource
async fn add_finalizer(
    db: &PostgresDatabase,
    ctx: &DatabaseContext,
    namespace: &str,
) -> Result<Action> {
    let name = db.name_any();
    let databases: Api<PostgresDatabase> = Api::namespaced(ctx.client.clone(), namespace);

    let patch = serde_json::json!({
        "metadata": {
            "finalizers": [DATABASE_FINALIZER]
        }
    });

    databases
        .patch(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

    Ok(Action::requeue(Duration::from_secs(1)))
}

/// Remove finalizer from the resource
async fn remove_finalizer(
    db: &PostgresDatabase,
    ctx: &DatabaseContext,
    namespace: &str,
) -> Result<()> {
    let name = db.name_any();
    let databases: Api<PostgresDatabase> = Api::namespaced(ctx.client.clone(), namespace);

    let patch = serde_json::json!({
        "metadata": {
            "finalizers": null
        }
    });

    databases
        .patch(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

    Ok(())
}

/// Update the status of the PostgresDatabase resource
async fn update_status(
    db: &PostgresDatabase,
    ctx: &DatabaseContext,
    namespace: &str,
    phase: DatabasePhase,
    conditions: Vec<DatabaseCondition>,
    connection_info: Option<DatabaseConnectionInfo>,
    credential_secrets: Vec<String>,
) -> Result<()> {
    let name = db.name_any();
    let databases: Api<PostgresDatabase> = Api::namespaced(ctx.client.clone(), namespace);

    let status = PostgresDatabaseStatus {
        phase,
        conditions,
        connection_info,
        credential_secrets,
        observed_generation: db.metadata.generation,
    };

    let patch = serde_json::json!({
        "status": status
    });

    databases
        .patch_status(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

    Ok(())
}

/// Create an owner reference for a PostgresDatabase
fn owner_reference_for_database(db: &PostgresDatabase) -> OwnerReference {
    OwnerReference {
        api_version: PostgresDatabase::api_version(&()).to_string(),
        kind: PostgresDatabase::kind(&()).to_string(),
        name: db.name_any(),
        uid: db.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Error policy for database reconciliation
pub fn database_error_policy(
    db: Arc<PostgresDatabase>,
    error: &DatabaseError,
    _ctx: Arc<DatabaseContext>,
) -> Action {
    let name = db.name_any();
    error!(name = %name, error = %error, "Database reconciliation error");

    // Exponential backoff for errors
    Action::requeue(Duration::from_secs(30))
}
