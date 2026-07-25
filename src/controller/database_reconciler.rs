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
use kube::runtime::events::{EventType, Reporter};
use kube::{Client, Resource, ResourceExt};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use tracing::{debug, error, info, instrument, warn};

use crate::controller::cleanup::{cleanup_stuck_resource, is_namespace_not_found_error};
use crate::controller::conditions::{new_condition, set_status_condition, status as cond_status};
use crate::controller::events;
use crate::controller::finalizer::{add_operator_finalizer, remove_operator_finalizer};
use crate::crd::{
    ClusterPhase, Condition, DatabaseConditionType, DatabaseConnectionInfo, DatabasePhase,
    GrantSpec, PostgresCluster, PostgresDatabase, PostgresDatabaseStatus, RoleSpec,
};
use crate::resources::postgres_client::PostgresConnection;
use crate::resources::secret::generate_password;
use crate::resources::sql::{
    self, SqlError, create_extension, drop_database, drop_role, ensure_database, ensure_role,
    grant_privileges,
};

/// Length of generated role passwords.
const ROLE_PASSWORD_LEN: usize = 24;

/// Context for the database reconciler
pub struct DatabaseContext {
    pub client: Client,
    reporter: Reporter,
}

impl DatabaseContext {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            reporter: events::reporter(),
        }
    }

    /// Publish a Normal event attached to the PostgresDatabase.
    pub async fn publish_normal_event(
        &self,
        db: &PostgresDatabase,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        events::publish_event(
            &self.client,
            &self.reporter,
            db,
            EventType::Normal,
            reason,
            action,
            note,
        )
        .await;
    }

    /// Publish a Warning event attached to the PostgresDatabase.
    pub async fn publish_warning_event(
        &self,
        db: &PostgresDatabase,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        events::publish_event(
            &self.client,
            &self.reporter,
            db,
            EventType::Warning,
            reason,
            action,
            note,
        )
        .await;
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

    #[error("Missing namespace in metadata")]
    MissingNamespace,

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("PostgreSQL client error: {0}")]
    PostgresClientError(#[from] crate::resources::postgres_client::PostgresClientError),

    #[error("Validation failed: {0}")]
    ValidationError(String),
}

/// Result type for database reconciliation
pub type Result<T, E = DatabaseError> = std::result::Result<T, E>;

/// Finalizer for PostgresDatabase resources
pub(crate) const DATABASE_FINALIZER: &str =
    "postgresdatabase.postgres-operator.smoketurner.com/finalizer";

/// Return the prior condition list for a PostgresDatabase, or an empty Vec
/// if none has been recorded yet.
fn existing_conditions(db: &PostgresDatabase) -> Vec<Condition> {
    db.status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default()
}

/// Merge a single condition update into the existing list using
/// [`set_status_condition`] so dedup, transition time, and observedGeneration
/// follow the standard `metav1.Condition` semantics.
fn merge_condition(
    mut conditions: Vec<Condition>,
    type_: DatabaseConditionType,
    status: &str,
    reason: &str,
    message: &str,
    generation: Option<i64>,
) -> Vec<Condition> {
    set_status_condition(
        &mut conditions,
        new_condition(type_.as_str(), status, reason, message, generation),
    );
    conditions
}

/// Reconcile a PostgresDatabase resource
#[instrument(skip(db, ctx), fields(name = %db.name_any(), namespace = db.namespace().unwrap_or_default()))]
pub async fn reconcile_database(
    db: Arc<PostgresDatabase>,
    ctx: Arc<DatabaseContext>,
) -> Result<Action> {
    let start_time = std::time::Instant::now();
    let name = db.name_any();
    let namespace = db.namespace().ok_or(DatabaseError::MissingNamespace)?;

    info!("Reconciling PostgresDatabase");

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
            let cluster_not_found_msg = format!("Cluster {}/{} not found", ns, cluster_name);
            let conditions = merge_condition(
                existing_conditions(&db),
                DatabaseConditionType::ClusterReady,
                cond_status::FALSE,
                "ClusterNotFound",
                &cluster_not_found_msg,
                db.metadata.generation,
            );
            let conditions = merge_condition(
                conditions,
                DatabaseConditionType::Ready,
                cond_status::FALSE,
                "ClusterNotFound",
                &cluster_not_found_msg,
                db.metadata.generation,
            );
            update_status(
                &db,
                &ctx,
                &namespace,
                DatabasePhase::Pending,
                conditions,
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
        let cluster_not_ready_msg = format!(
            "Cluster {} is in phase {}",
            db.spec.cluster_ref.name, cluster_phase
        );
        let conditions = merge_condition(
            existing_conditions(&db),
            DatabaseConditionType::ClusterReady,
            cond_status::FALSE,
            "ClusterNotReady",
            &cluster_not_ready_msg,
            db.metadata.generation,
        );
        let conditions = merge_condition(
            conditions,
            DatabaseConditionType::Ready,
            cond_status::FALSE,
            "ClusterNotReady",
            &cluster_not_ready_msg,
            db.metadata.generation,
        );
        update_status(
            &db,
            &ctx,
            &namespace,
            DatabasePhase::Pending,
            conditions,
            None,
            vec![],
        )
        .await?;

        // Requeue to check again
        return Ok(Action::requeue(Duration::from_secs(10)));
    }

    // Provision the database
    let was_ready = db
        .status
        .as_ref()
        .is_some_and(|s| s.phase == DatabasePhase::Ready);
    let was_failed = db
        .status
        .as_ref()
        .is_some_and(|s| s.phase == DatabasePhase::Failed);
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
            let generation = db.metadata.generation;
            let mut conditions = existing_conditions(&db);
            for (type_, reason, message) in [
                (
                    DatabaseConditionType::ClusterReady,
                    "ClusterRunning",
                    "Parent cluster is running",
                ),
                (
                    DatabaseConditionType::DatabaseCreated,
                    "DatabaseProvisioned",
                    "Database has been provisioned",
                ),
                (
                    DatabaseConditionType::RolesCreated,
                    "RolesProvisioned",
                    "Roles have been provisioned",
                ),
                (
                    DatabaseConditionType::GrantsApplied,
                    "GrantsApplied",
                    "Grants have been applied",
                ),
                (
                    DatabaseConditionType::SecretsCreated,
                    "SecretsCreated",
                    "Credential secrets have been created",
                ),
                (
                    DatabaseConditionType::Ready,
                    "Ready",
                    "Database is ready for use",
                ),
            ] {
                set_status_condition(
                    &mut conditions,
                    new_condition(
                        type_.as_str(),
                        cond_status::TRUE,
                        reason,
                        message,
                        generation,
                    ),
                );
            }
            update_status(
                &db,
                &ctx,
                &namespace,
                DatabasePhase::Ready,
                conditions,
                Some(connection_info),
                secrets,
            )
            .await?;

            if !was_ready {
                ctx.publish_normal_event(
                    &db,
                    "Provisioned",
                    "ProvisionDatabase",
                    Some(format!(
                        "Database {} is ready on cluster {}",
                        db.spec.database.name, db.spec.cluster_ref.name
                    )),
                )
                .await;
            }

            let duration_secs = start_time.elapsed().as_secs_f64();
            info!(
                name = %name,
                namespace = %namespace,
                "Reconciliation completed successfully in {:.3}s (phase: Ready)",
                duration_secs
            );
            Ok(Action::requeue(Duration::from_secs(300))) // Recheck every 5 minutes
        }
        Err(e) => {
            let duration_secs = start_time.elapsed().as_secs_f64();
            error!(
                name = %name,
                namespace = %namespace,
                error = %e,
                "Reconciliation failed after {:.3}s",
                duration_secs
            );

            // Update status to Failed
            let conditions = merge_condition(
                existing_conditions(&db),
                DatabaseConditionType::Ready,
                cond_status::FALSE,
                "ProvisioningFailed",
                &e.to_string(),
                db.metadata.generation,
            );
            update_status(
                &db,
                &ctx,
                &namespace,
                DatabasePhase::Failed,
                conditions,
                None,
                vec![],
            )
            .await?;

            if !was_failed {
                ctx.publish_warning_event(
                    &db,
                    "ProvisioningFailed",
                    "ProvisionDatabase",
                    Some(e.to_string()),
                )
                .await;
            }

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

    // Connect to the cluster's primary
    let conn = PostgresConnection::connect_primary(&ctx.client, namespace, &cluster_name).await?;

    // First, ensure the owner role exists (create with a temporary password if needed)
    // The owner needs to exist before we can create the database
    let owner_exists = sql::role_exists(&conn, owner).await?;
    if !owner_exists {
        debug!(role = %owner, "Creating owner role");
        let temp_password = generate_password(ROLE_PASSWORD_LEN);
        sql::create_role(&conn, owner, &temp_password, &[], None, true).await?;
    }

    // Create the database
    ensure_database(
        &conn,
        db_name,
        owner,
        db.spec.database.encoding.as_deref(),
        db.spec.database.locale.as_deref(),
        db.spec.database.connection_limit,
    )
    .await?;

    // For extensions and grants, we need to connect to the target database
    // Create a new connection to the specific database
    let db_conn =
        PostgresConnection::connect_database(&ctx.client, namespace, &cluster_name, db_name)
            .await?;

    // Create extensions
    for extension in &db.spec.extensions {
        debug!(extension = %extension, database = %db_name, "Creating extension");
        create_extension(&db_conn, extension).await?;
    }

    // Create roles and secrets (still use the postgres database connection)
    let mut created_secrets = Vec::new();
    for role_spec in &db.spec.roles {
        let secret_name =
            create_role_with_secret(db, ctx, cluster, namespace, db_name, role_spec, &conn).await?;
        created_secrets.push(secret_name);
    }

    // Apply grants (use the target database connection)
    for grant in &db.spec.grants {
        apply_grant(&db_conn, grant).await?;
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
    conn: &PostgresConnection,
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
    let password = existing_password.unwrap_or_else(|| generate_password(ROLE_PASSWORD_LEN));

    // Build privileges list
    let privileges: Vec<String> = role_spec
        .privileges
        .iter()
        .map(|p| p.as_sql().to_string())
        .collect();

    // Create or update the role
    ensure_role(
        conn,
        role_name,
        &password,
        &privileges,
        role_spec.connection_limit,
        role_spec.login,
    )
    .await?;

    // Create or update the credential secret
    let host = format!("{}-primary.{}.svc", cluster_name, namespace);
    let port: u16 = 5432;

    let connection_string = build_connection_string(role_name, &password, &host, port, db_name);
    let jdbc_url = build_jdbc_url(role_name, &password, &host, port, db_name);

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

/// Percent-encode a password for embedding in a URI.
///
/// Passwords generated today are alphanumeric, but secrets provisioned by older
/// operator versions can contain `@`, `&`, `%` and friends: `@` makes the libpq
/// URI's userinfo/host split ambiguous and `&` truncates the JDBC password at a
/// query-parameter boundary. `NON_ALPHANUMERIC` also encodes `+`, which JDBC
/// would otherwise decode as a space.
fn encode_password(password: &str) -> String {
    utf8_percent_encode(password, NON_ALPHANUMERIC).to_string()
}

/// Build the libpq URI stored in the credentials Secret's `connection-string` key.
fn build_connection_string(
    role_name: &str,
    password: &str,
    host: &str,
    port: u16,
    db_name: &str,
) -> String {
    format!(
        "postgresql://{}:{}@{}:{}/{}?sslmode=require",
        role_name,
        encode_password(password),
        host,
        port,
        db_name
    )
}

/// Build the JDBC URL stored in the credentials Secret's `jdbc-url` key.
fn build_jdbc_url(role_name: &str, password: &str, host: &str, port: u16, db_name: &str) -> String {
    format!(
        "jdbc:postgresql://{}:{}/{}?user={}&password={}&ssl=true",
        host,
        port,
        db_name,
        role_name,
        encode_password(password)
    )
}

/// Apply a grant specification
async fn apply_grant(conn: &PostgresConnection, grant: &GrantSpec) -> Result<()> {
    let privileges: Vec<String> = grant
        .privileges
        .iter()
        .map(|p| p.as_sql().to_string())
        .collect();

    if !privileges.is_empty() {
        // Table privileges can only be applied via the ALL TABLES IN SCHEMA
        // form; silently ignoring them when allTables is false would leave
        // the role without the requested access. Fail fast so the
        // misconfiguration is visible in status instead.
        if !grant.all_tables {
            return Err(DatabaseError::ValidationError(format!(
                "grant for role \"{}\" on schema \"{}\" specifies table privileges but allTables is false; set allTables: true to apply them",
                grant.role, grant.schema
            )));
        }
        grant_privileges(conn, &grant.role, &grant.schema, &privileges).await?;
    }

    // Grant USAGE on schema
    let usage_sql = format!(
        "GRANT USAGE ON SCHEMA {} TO {}",
        sql::quote_identifier_pub(&grant.schema),
        sql::quote_identifier_pub(&grant.role)
    );
    conn.batch_execute(&usage_sql).await?;

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
            // Connect to the cluster
            if let Ok(conn) =
                PostgresConnection::connect_primary(&ctx.client, namespace, &cluster_name).await
            {
                // Drop the database first (owner role owns it, so database must go first)
                if let Err(e) = drop_database(&conn, &db.spec.database.name).await {
                    warn!(database = %db.spec.database.name, error = %e, "Failed to drop database during cleanup");
                }

                // Drop roles (they were granted access to the database)
                for role_spec in &db.spec.roles {
                    if let Err(e) = drop_role(&conn, &role_spec.name).await {
                        warn!(role = %role_spec.name, error = %e, "Failed to drop role during cleanup");
                    }
                }

                // Drop the owner role last (after database is dropped)
                if let Err(e) = drop_role(&conn, &db.spec.database.owner).await {
                    warn!(role = %db.spec.database.owner, error = %e, "Failed to drop owner role during cleanup");
                }
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

/// Add finalizer to the resource, preserving any existing finalizers
async fn add_finalizer(
    db: &PostgresDatabase,
    ctx: &DatabaseContext,
    namespace: &str,
) -> Result<Action> {
    let name = db.name_any();
    let databases: Api<PostgresDatabase> = Api::namespaced(ctx.client.clone(), namespace);

    add_operator_finalizer(
        &databases,
        &name,
        db.metadata.finalizers.as_ref(),
        DATABASE_FINALIZER,
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

    match remove_operator_finalizer(
        &databases,
        &name,
        db.metadata.finalizers.as_ref(),
        DATABASE_FINALIZER,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(e) if is_namespace_not_found_error(&e) => {
            // Namespace is gone - use special cleanup procedure
            cleanup_stuck_resource::<PostgresDatabase>(
                ctx.client.clone(),
                &name,
                namespace,
                DATABASE_FINALIZER,
            )
            .await?;
            Ok(())
        }
        Err(e) => Err(DatabaseError::KubeError(e)),
    }
}

/// Update the status of the PostgresDatabase resource
async fn update_status(
    db: &PostgresDatabase,
    ctx: &DatabaseContext,
    namespace: &str,
    phase: DatabasePhase,
    conditions: Vec<Condition>,
    connection_info: Option<DatabaseConnectionInfo>,
    credential_secrets: Vec<String>,
) -> Result<()> {
    let name = db.name_any();
    let databases: Api<PostgresDatabase> = Api::namespaced(ctx.client.clone(), namespace);

    let (reason, message) = conditions
        .iter()
        .find(|c| c.type_ == DatabaseConditionType::Ready.as_str())
        .map(|c| (Some(c.reason.clone()), Some(c.message.clone())))
        .unwrap_or_default();

    let status = PostgresDatabaseStatus {
        phase,
        conditions,
        reason,
        message,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::{build_connection_string, build_jdbc_url};

    /// A password from a secret provisioned before the charset was narrowed.
    const LEGACY_PASSWORD: &str = "k3X@qR7m&p%1#z";

    #[test]
    fn connection_string_encodes_reserved_characters() {
        let uri =
            build_connection_string("myrole", LEGACY_PASSWORD, "db-primary.ns.svc", 5432, "mydb");

        assert_eq!(
            uri,
            "postgresql://myrole:k3X%40qR7m%26p%251%23z@db-primary.ns.svc:5432/mydb?sslmode=require"
        );
        // Exactly one `@` may remain: the userinfo/host delimiter. A raw `@` in
        // the password would make the authority ambiguous.
        assert_eq!(uri.matches('@').count(), 1);
    }

    #[test]
    fn jdbc_url_encodes_reserved_characters() {
        let url = build_jdbc_url("myrole", LEGACY_PASSWORD, "db-primary.ns.svc", 5432, "mydb");

        assert_eq!(
            url,
            "jdbc:postgresql://db-primary.ns.svc:5432/mydb\
             ?user=myrole&password=k3X%40qR7m%26p%251%23z&ssl=true"
        );
        // Only the two separators the operator emits; a raw `&` in the password
        // would truncate it at a query-parameter boundary.
        assert_eq!(url.matches('&').count(), 2);
    }

    #[test]
    fn alphanumeric_password_is_unchanged() {
        // Passwords generated today need no escaping, so the stored values stay
        // readable and byte-identical to the `password` key.
        let password = "aB3xY9zQ7mN2pK5vR8tL4wJ6";

        assert_eq!(
            build_connection_string("myrole", password, "db-primary.ns.svc", 5432, "mydb"),
            format!("postgresql://myrole:{password}@db-primary.ns.svc:5432/mydb?sslmode=require")
        );
        assert!(
            build_jdbc_url("myrole", password, "db-primary.ns.svc", 5432, "mydb")
                .contains(&format!("password={password}&"))
        );
    }

    #[test]
    fn plus_is_encoded_so_jdbc_does_not_read_it_as_space() {
        let uri = build_connection_string("myrole", "a+b", "h", 5432, "d");
        assert!(uri.contains("myrole:a%2Bb@"), "got {uri}");
    }
}
