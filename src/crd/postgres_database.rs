//! PostgresDatabase CRD definition
//!
//! This CRD enables declarative database and role provisioning for PostgresCluster resources.
//! Creating a PostgresDatabase resource will:
//! - Create the specified database in the referenced PostgresCluster
//! - Create roles with specified privileges
//! - Generate Kubernetes secrets with credentials and connection strings
//! - Apply grants to control access

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// PostgresDatabase is the Schema for the postgresdatabases API
///
/// A PostgresDatabase resource declares a database and its roles within
/// a parent PostgresCluster. The operator provisions these resources
/// and creates Kubernetes secrets with credentials.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "postgres-operator.smoketurner.com",
    version = "v1alpha1",
    kind = "PostgresDatabase",
    plural = "postgresdatabases",
    shortname = "pgdb",
    namespaced,
    status = "PostgresDatabaseStatus",
    printcolumn = r#"{"name":"Cluster", "type":"string", "jsonPath":".spec.clusterRef.name"}"#,
    printcolumn = r#"{"name":"Database", "type":"string", "jsonPath":".spec.database.name"}"#,
    printcolumn = r#"{"name":"Phase", "type":"string", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresDatabaseSpec {
    /// Reference to the parent PostgresCluster
    pub cluster_ref: ClusterRef,

    /// Database to create
    pub database: DatabaseSpec,

    /// Roles to create with credentials
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<RoleSpec>,

    /// Permission grants to apply
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<GrantSpec>,

    /// Extensions to enable in the database
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
}

/// Reference to a PostgresCluster
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRef {
    /// Name of the PostgresCluster
    pub name: String,

    /// Namespace of the PostgresCluster (defaults to same namespace)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Database specification
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseSpec {
    /// Name of the database to create
    pub name: String,

    /// Owner role for the database
    pub owner: String,

    /// Encoding for the database (default: UTF8)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,

    /// Locale for the database
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,

    /// Connection limit for the database (-1 for unlimited)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_limit: Option<i32>,
}

/// Role specification
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RoleSpec {
    /// Name of the role to create
    pub name: String,

    /// Privileges for this role
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub privileges: Vec<RolePrivilege>,

    /// Name of the Secret to create with credentials
    /// The secret will contain:
    /// - username: role name
    /// - password: generated password
    /// - host: primary service hostname
    /// - port: PostgreSQL port
    /// - database: database name
    /// - connection-string: ready-to-use connection URL
    /// - jdbc-url: JDBC connection string
    pub secret_name: String,

    /// Connection limit for this role (-1 for unlimited)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_limit: Option<i32>,

    /// Whether this role can log in (default: true for roles with secrets)
    #[serde(default = "default_login")]
    pub login: bool,
}

fn default_login() -> bool {
    true
}

/// Role privileges
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RolePrivilege {
    /// Can create databases
    Createdb,
    /// Can create roles
    Createrole,
    /// Superuser (not recommended)
    Superuser,
    /// Can log in
    Login,
    /// Can replicate
    Replication,
    /// Bypass row-level security
    BypassRls,
}

impl RolePrivilege {
    /// Returns the SQL keyword for this privilege
    pub fn as_sql(&self) -> &'static str {
        match self {
            RolePrivilege::Createdb => "CREATEDB",
            RolePrivilege::Createrole => "CREATEROLE",
            RolePrivilege::Superuser => "SUPERUSER",
            RolePrivilege::Login => "LOGIN",
            RolePrivilege::Replication => "REPLICATION",
            RolePrivilege::BypassRls => "BYPASSRLS",
        }
    }
}

/// Grant specification for object privileges
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GrantSpec {
    /// Role to grant privileges to
    pub role: String,

    /// Schema to grant on (default: public)
    #[serde(default = "default_schema")]
    pub schema: String,

    /// Table privileges to grant
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub privileges: Vec<TablePrivilege>,

    /// Grant on all tables in schema
    #[serde(default)]
    pub all_tables: bool,

    /// Grant on all sequences in schema
    #[serde(default)]
    pub all_sequences: bool,

    /// Grant on all functions in schema
    #[serde(default)]
    pub all_functions: bool,
}

fn default_schema() -> String {
    "public".to_string()
}

/// Table-level privileges
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TablePrivilege {
    /// SELECT privilege
    Select,
    /// INSERT privilege
    Insert,
    /// UPDATE privilege
    Update,
    /// DELETE privilege
    Delete,
    /// TRUNCATE privilege
    Truncate,
    /// REFERENCES privilege
    References,
    /// TRIGGER privilege
    Trigger,
    /// All privileges
    All,
}

impl TablePrivilege {
    /// Returns the SQL keyword for this privilege
    pub fn as_sql(&self) -> &'static str {
        match self {
            TablePrivilege::Select => "SELECT",
            TablePrivilege::Insert => "INSERT",
            TablePrivilege::Update => "UPDATE",
            TablePrivilege::Delete => "DELETE",
            TablePrivilege::Truncate => "TRUNCATE",
            TablePrivilege::References => "REFERENCES",
            TablePrivilege::Trigger => "TRIGGER",
            TablePrivilege::All => "ALL",
        }
    }
}

/// Status of the PostgresDatabase resource
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PostgresDatabaseStatus {
    /// Current phase of the database provisioning
    #[serde(default)]
    pub phase: DatabasePhase,

    /// Conditions representing the current state
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<DatabaseCondition>,

    /// Connection information for the database
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_info: Option<DatabaseConnectionInfo>,

    /// List of secrets created for role credentials
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credential_secrets: Vec<String>,

    /// Last observed generation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

/// Phase of database provisioning
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum DatabasePhase {
    /// Initial state, not yet processed
    #[default]
    Pending,
    /// Database and roles are being created
    Creating,
    /// Database is ready and all roles are provisioned
    Ready,
    /// Updating roles or grants
    Updating,
    /// Failed to provision - see conditions for details
    Failed,
    /// Being deleted
    Deleting,
}

impl std::fmt::Display for DatabasePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabasePhase::Pending => write!(f, "Pending"),
            DatabasePhase::Creating => write!(f, "Creating"),
            DatabasePhase::Ready => write!(f, "Ready"),
            DatabasePhase::Updating => write!(f, "Updating"),
            DatabasePhase::Failed => write!(f, "Failed"),
            DatabasePhase::Deleting => write!(f, "Deleting"),
        }
    }
}

/// Condition of the database resource
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseCondition {
    /// Type of condition
    #[serde(rename = "type")]
    pub condition_type: DatabaseConditionType,

    /// Status of the condition (True, False, Unknown)
    pub status: String,

    /// Last time the condition transitioned
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,

    /// Human-readable reason for the condition
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Human-readable message
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Types of conditions for PostgresDatabase
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
pub enum DatabaseConditionType {
    /// Parent cluster is available and running
    ClusterReady,
    /// Database has been created
    DatabaseCreated,
    /// All roles have been created
    RolesCreated,
    /// All grants have been applied
    GrantsApplied,
    /// All credential secrets have been created
    SecretsCreated,
    /// Resource is ready for use
    Ready,
}

/// Connection information for the database
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseConnectionInfo {
    /// Primary service hostname
    pub host: String,

    /// PostgreSQL port
    pub port: i32,

    /// Database name
    pub database: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn test_role_privilege_sql() {
        assert_eq!(RolePrivilege::Createdb.as_sql(), "CREATEDB");
        assert_eq!(RolePrivilege::Login.as_sql(), "LOGIN");
        assert_eq!(RolePrivilege::Superuser.as_sql(), "SUPERUSER");
    }

    #[test]
    fn test_table_privilege_sql() {
        assert_eq!(TablePrivilege::Select.as_sql(), "SELECT");
        assert_eq!(TablePrivilege::Insert.as_sql(), "INSERT");
        assert_eq!(TablePrivilege::All.as_sql(), "ALL");
    }

    #[test]
    fn test_database_phase_display() {
        assert_eq!(DatabasePhase::Pending.to_string(), "Pending");
        assert_eq!(DatabasePhase::Ready.to_string(), "Ready");
        assert_eq!(DatabasePhase::Failed.to_string(), "Failed");
    }

    #[test]
    fn test_default_schema() {
        assert_eq!(default_schema(), "public");
    }

    #[test]
    fn test_default_login() {
        assert!(default_login());
    }
}
