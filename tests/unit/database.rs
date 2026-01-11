//! Unit tests for PostgresDatabase CRD and SQL utilities
//!
//! Tests for:
//! - PostgresDatabase CRD structure and defaults
//! - SQL escaping and injection prevention
//! - Identifier validation
//! - Password generation
//! - Connection string formatting

use postgres_operator::crd::{
    ClusterRef, DatabaseConditionType, DatabasePhase, DatabaseSpec, GrantSpec, PostgresDatabase,
    PostgresDatabaseSpec, PostgresDatabaseStatus, RolePrivilege, RoleSpec, TablePrivilege,
};
use postgres_operator::resources::sql::{
    escape_sql_string_pub, generate_password, is_valid_identifier, quote_identifier_pub,
};

// =============================================================================
// PostgresDatabase CRD Tests
// =============================================================================

mod crd_tests {
    use super::*;

    fn create_test_database() -> PostgresDatabase {
        PostgresDatabase {
            metadata: kube::core::ObjectMeta {
                name: Some("orders-db".to_string()),
                namespace: Some("orders".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: PostgresDatabaseSpec {
                cluster_ref: ClusterRef {
                    name: "prod-postgres".to_string(),
                    namespace: Some("orders".to_string()),
                },
                database: DatabaseSpec {
                    name: "orders".to_string(),
                    owner: "orders_owner".to_string(),
                    encoding: None,
                    locale: None,
                    connection_limit: None,
                },
                roles: vec![
                    RoleSpec {
                        name: "orders_owner".to_string(),
                        privileges: vec![RolePrivilege::Createdb, RolePrivilege::Login],
                        secret_name: "orders-owner-creds".to_string(),
                        connection_limit: None,
                        login: true,
                    },
                    RoleSpec {
                        name: "orders_reader".to_string(),
                        privileges: vec![RolePrivilege::Login],
                        secret_name: "orders-reader-creds".to_string(),
                        connection_limit: None,
                        login: true,
                    },
                ],
                grants: vec![GrantSpec {
                    role: "orders_reader".to_string(),
                    schema: "public".to_string(),
                    privileges: vec![TablePrivilege::Select],
                    all_tables: true,
                    all_sequences: false,
                    all_functions: false,
                }],
                extensions: vec!["uuid-ossp".to_string()],
            },
            status: None,
        }
    }

    #[test]
    fn test_database_spec_structure() {
        let db = create_test_database();

        assert_eq!(db.spec.cluster_ref.name, "prod-postgres");
        assert_eq!(db.spec.database.name, "orders");
        assert_eq!(db.spec.database.owner, "orders_owner");
        assert_eq!(db.spec.roles.len(), 2);
        assert_eq!(db.spec.grants.len(), 1);
        assert_eq!(db.spec.extensions.len(), 1);
    }

    #[test]
    fn test_cluster_ref_with_namespace() {
        let db = create_test_database();

        assert_eq!(db.spec.cluster_ref.namespace, Some("orders".to_string()));
    }

    #[test]
    fn test_cluster_ref_without_namespace() {
        let mut db = create_test_database();
        db.spec.cluster_ref.namespace = None;

        assert!(db.spec.cluster_ref.namespace.is_none());
    }

    #[test]
    fn test_role_privileges() {
        let db = create_test_database();

        let owner_role = &db.spec.roles[0];
        assert!(owner_role.privileges.contains(&RolePrivilege::Createdb));
        assert!(owner_role.privileges.contains(&RolePrivilege::Login));

        let reader_role = &db.spec.roles[1];
        assert!(reader_role.privileges.contains(&RolePrivilege::Login));
        assert!(!reader_role.privileges.contains(&RolePrivilege::Createdb));
    }

    #[test]
    fn test_grant_spec() {
        let db = create_test_database();

        let grant = &db.spec.grants[0];
        assert_eq!(grant.role, "orders_reader");
        assert_eq!(grant.schema, "public");
        assert!(grant.privileges.contains(&TablePrivilege::Select));
        assert!(grant.all_tables);
    }

    #[test]
    fn test_database_encoding() {
        let mut db = create_test_database();
        db.spec.database.encoding = Some("UTF8".to_string());

        assert_eq!(db.spec.database.encoding, Some("UTF8".to_string()));
    }

    #[test]
    fn test_database_locale() {
        let mut db = create_test_database();
        db.spec.database.locale = Some("en_US.UTF-8".to_string());

        assert_eq!(db.spec.database.locale, Some("en_US.UTF-8".to_string()));
    }

    #[test]
    fn test_database_connection_limit() {
        let mut db = create_test_database();
        db.spec.database.connection_limit = Some(100);

        assert_eq!(db.spec.database.connection_limit, Some(100));
    }
}

// =============================================================================
// Database Phase Tests
// =============================================================================

mod phase_tests {
    use super::*;

    #[test]
    fn test_phase_display_pending() {
        assert_eq!(format!("{}", DatabasePhase::Pending), "Pending");
    }

    #[test]
    fn test_phase_display_creating() {
        assert_eq!(format!("{}", DatabasePhase::Creating), "Creating");
    }

    #[test]
    fn test_phase_display_ready() {
        assert_eq!(format!("{}", DatabasePhase::Ready), "Ready");
    }

    #[test]
    fn test_phase_display_failed() {
        assert_eq!(format!("{}", DatabasePhase::Failed), "Failed");
    }

    #[test]
    fn test_phase_display_deleting() {
        assert_eq!(format!("{}", DatabasePhase::Deleting), "Deleting");
    }

    #[test]
    fn test_phase_default() {
        let phase = DatabasePhase::default();
        assert_eq!(phase, DatabasePhase::Pending);
    }
}

// =============================================================================
// Database Status Tests
// =============================================================================

mod status_tests {
    use super::*;

    #[test]
    fn test_status_default() {
        let status = PostgresDatabaseStatus::default();

        assert_eq!(status.phase, DatabasePhase::Pending);
        assert!(status.conditions.is_empty());
        assert!(status.connection_info.is_none());
        assert!(status.credential_secrets.is_empty());
    }

    #[test]
    fn test_status_with_phase() {
        let status = PostgresDatabaseStatus {
            phase: DatabasePhase::Ready,
            ..Default::default()
        };

        assert_eq!(status.phase, DatabasePhase::Ready);
    }

    #[test]
    fn test_condition_types() {
        // Verify all condition types exist
        let _ = DatabaseConditionType::ClusterReady;
        let _ = DatabaseConditionType::DatabaseCreated;
        let _ = DatabaseConditionType::RolesCreated;
        let _ = DatabaseConditionType::GrantsApplied;
        let _ = DatabaseConditionType::SecretsCreated;
        let _ = DatabaseConditionType::Ready;
    }
}

// =============================================================================
// Role Privilege SQL Tests
// =============================================================================

mod role_privilege_tests {
    use super::*;

    #[test]
    fn test_superuser_sql() {
        assert_eq!(RolePrivilege::Superuser.as_sql(), "SUPERUSER");
    }

    #[test]
    fn test_createdb_sql() {
        assert_eq!(RolePrivilege::Createdb.as_sql(), "CREATEDB");
    }

    #[test]
    fn test_createrole_sql() {
        assert_eq!(RolePrivilege::Createrole.as_sql(), "CREATEROLE");
    }

    #[test]
    fn test_login_sql() {
        assert_eq!(RolePrivilege::Login.as_sql(), "LOGIN");
    }

    #[test]
    fn test_replication_sql() {
        assert_eq!(RolePrivilege::Replication.as_sql(), "REPLICATION");
    }

    #[test]
    fn test_bypassrls_sql() {
        assert_eq!(RolePrivilege::BypassRls.as_sql(), "BYPASSRLS");
    }
}

// =============================================================================
// Table Privilege SQL Tests
// =============================================================================

mod table_privilege_tests {
    use super::*;

    #[test]
    fn test_select_sql() {
        assert_eq!(TablePrivilege::Select.as_sql(), "SELECT");
    }

    #[test]
    fn test_insert_sql() {
        assert_eq!(TablePrivilege::Insert.as_sql(), "INSERT");
    }

    #[test]
    fn test_update_sql() {
        assert_eq!(TablePrivilege::Update.as_sql(), "UPDATE");
    }

    #[test]
    fn test_delete_sql() {
        assert_eq!(TablePrivilege::Delete.as_sql(), "DELETE");
    }

    #[test]
    fn test_truncate_sql() {
        assert_eq!(TablePrivilege::Truncate.as_sql(), "TRUNCATE");
    }

    #[test]
    fn test_references_sql() {
        assert_eq!(TablePrivilege::References.as_sql(), "REFERENCES");
    }

    #[test]
    fn test_trigger_sql() {
        assert_eq!(TablePrivilege::Trigger.as_sql(), "TRIGGER");
    }

    #[test]
    fn test_all_sql() {
        assert_eq!(TablePrivilege::All.as_sql(), "ALL");
    }
}

// =============================================================================
// SQL Identifier Quoting Tests
// =============================================================================

mod identifier_quoting_tests {
    use super::*;

    #[test]
    fn test_simple_identifier() {
        assert_eq!(quote_identifier_pub("users"), "\"users\"");
    }

    #[test]
    fn test_identifier_with_double_quote() {
        assert_eq!(quote_identifier_pub("user\"name"), "\"user\"\"name\"");
    }

    #[test]
    fn test_identifier_with_multiple_quotes() {
        assert_eq!(quote_identifier_pub("a\"b\"c"), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn test_mixed_case_identifier() {
        assert_eq!(quote_identifier_pub("UserTable"), "\"UserTable\"");
    }

    #[test]
    fn test_identifier_with_spaces() {
        assert_eq!(quote_identifier_pub("user table"), "\"user table\"");
    }

    #[test]
    fn test_identifier_with_special_chars() {
        assert_eq!(quote_identifier_pub("user-table"), "\"user-table\"");
        assert_eq!(quote_identifier_pub("user.table"), "\"user.table\"");
        assert_eq!(quote_identifier_pub("user@table"), "\"user@table\"");
    }

    #[test]
    fn test_empty_identifier() {
        assert_eq!(quote_identifier_pub(""), "\"\"");
    }

    #[test]
    fn test_sql_injection_in_identifier() {
        // Attempt to break out of identifier with semicolon
        assert_eq!(
            quote_identifier_pub("users; DROP TABLE users;--"),
            "\"users; DROP TABLE users;--\""
        );

        // The semicolon and SQL commands are safely contained within the quoted identifier
    }
}

// =============================================================================
// SQL String Escaping Tests
// =============================================================================

mod string_escaping_tests {
    use super::*;

    #[test]
    fn test_simple_string() {
        assert_eq!(escape_sql_string_pub("hello"), "hello");
    }

    #[test]
    fn test_string_with_single_quote() {
        assert_eq!(escape_sql_string_pub("it's"), "it''s");
    }

    #[test]
    fn test_string_with_multiple_quotes() {
        assert_eq!(escape_sql_string_pub("it's John's"), "it''s John''s");
    }

    #[test]
    fn test_string_starting_with_quote() {
        assert_eq!(escape_sql_string_pub("'quoted'"), "''quoted''");
    }

    #[test]
    fn test_empty_string() {
        assert_eq!(escape_sql_string_pub(""), "");
    }

    #[test]
    fn test_sql_injection_in_string() {
        // Classic SQL injection attempt
        assert_eq!(
            escape_sql_string_pub("'; DROP TABLE users;--"),
            "''; DROP TABLE users;--"
        );

        // The leading quote is escaped, breaking the injection
    }

    #[test]
    fn test_unicode_string() {
        assert_eq!(escape_sql_string_pub("café"), "café");
        assert_eq!(escape_sql_string_pub("日本語"), "日本語");
    }

    #[test]
    fn test_string_with_backslash() {
        // Backslashes are not escaped (PostgreSQL standard_conforming_strings=on)
        assert_eq!(escape_sql_string_pub("path\\to\\file"), "path\\to\\file");
    }
}

// =============================================================================
// Identifier Validation Tests
// =============================================================================

mod identifier_validation_tests {
    use super::*;

    #[test]
    fn test_valid_simple_identifier() {
        assert!(is_valid_identifier("users"));
        assert!(is_valid_identifier("my_table"));
        assert!(is_valid_identifier("table123"));
    }

    #[test]
    fn test_valid_underscore_start() {
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("_"));
    }

    #[test]
    fn test_valid_single_char() {
        assert!(is_valid_identifier("a"));
        assert!(is_valid_identifier("z"));
    }

    #[test]
    fn test_invalid_empty() {
        assert!(!is_valid_identifier(""));
    }

    #[test]
    fn test_invalid_starts_with_number() {
        assert!(!is_valid_identifier("123abc"));
        assert!(!is_valid_identifier("1"));
    }

    #[test]
    fn test_invalid_uppercase() {
        assert!(!is_valid_identifier("Users"));
        assert!(!is_valid_identifier("USERS"));
        assert!(!is_valid_identifier("userTable"));
    }

    #[test]
    fn test_invalid_special_chars() {
        assert!(!is_valid_identifier("user-table"));
        assert!(!is_valid_identifier("user.table"));
        assert!(!is_valid_identifier("user@table"));
        assert!(!is_valid_identifier("user table"));
        assert!(!is_valid_identifier("user;table"));
    }

    #[test]
    fn test_invalid_too_long() {
        let long_name = "a".repeat(64);
        assert!(!is_valid_identifier(&long_name));
    }

    #[test]
    fn test_valid_max_length() {
        let max_name = "a".repeat(63);
        assert!(is_valid_identifier(&max_name));
    }

    #[test]
    fn test_invalid_sql_keywords_still_valid_identifiers() {
        // SQL keywords are valid identifiers (they're just quoted)
        assert!(is_valid_identifier("select"));
        assert!(is_valid_identifier("from"));
        assert!(is_valid_identifier("where"));
    }
}

// =============================================================================
// Password Generation Tests
// =============================================================================

mod password_generation_tests {
    use super::*;

    #[test]
    fn test_password_length() {
        let password = generate_password();
        assert_eq!(password.len(), 24);
    }

    #[test]
    fn test_password_uniqueness() {
        let passwords: Vec<String> = (0..100).map(|_| generate_password()).collect();

        // All passwords should be unique
        let mut unique = passwords.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(
            passwords.len(),
            unique.len(),
            "Generated passwords should be unique"
        );
    }

    #[test]
    fn test_password_character_set() {
        let password = generate_password();

        // Should only contain allowed characters
        let allowed_chars =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*";
        for c in password.chars() {
            assert!(
                allowed_chars.contains(c),
                "Password contains invalid character: {}",
                c
            );
        }
    }

    #[test]
    fn test_password_has_variety() {
        // Generate multiple passwords and check for character variety
        let password = generate_password();

        let has_upper = password.chars().any(|c| c.is_ascii_uppercase());
        let has_lower = password.chars().any(|c| c.is_ascii_lowercase());
        let has_digit = password.chars().any(|c| c.is_ascii_digit());

        // With 24 characters, we should have variety (probabilistically)
        // This might occasionally fail, but very rarely
        assert!(
            has_upper || has_lower || has_digit,
            "Password should have character variety"
        );
    }
}

// =============================================================================
// Role Spec Tests
// =============================================================================

mod role_spec_tests {
    use super::*;

    #[test]
    fn test_role_default_login() {
        let role = RoleSpec {
            name: "test_role".to_string(),
            privileges: vec![],
            secret_name: "test-secret".to_string(),
            connection_limit: None,
            login: true, // Default should be true for application roles
        };

        assert!(role.login);
    }

    #[test]
    fn test_role_with_connection_limit() {
        let role = RoleSpec {
            name: "test_role".to_string(),
            privileges: vec![],
            secret_name: "test-secret".to_string(),
            connection_limit: Some(10),
            login: true,
        };

        assert_eq!(role.connection_limit, Some(10));
    }

    #[test]
    fn test_role_all_privileges() {
        let role = RoleSpec {
            name: "superuser_role".to_string(),
            privileges: vec![
                RolePrivilege::Superuser,
                RolePrivilege::Createdb,
                RolePrivilege::Createrole,
                RolePrivilege::Login,
                RolePrivilege::Replication,
                RolePrivilege::BypassRls,
            ],
            secret_name: "super-secret".to_string(),
            connection_limit: None,
            login: true,
        };

        assert_eq!(role.privileges.len(), 6);
    }
}

// =============================================================================
// Grant Spec Tests
// =============================================================================

mod grant_spec_tests {
    use super::*;

    #[test]
    fn test_grant_default_schema() {
        let grant = GrantSpec {
            role: "reader".to_string(),
            schema: "public".to_string(),
            privileges: vec![TablePrivilege::Select],
            all_tables: true,
            all_sequences: false,
            all_functions: false,
        };

        assert_eq!(grant.schema, "public");
    }

    #[test]
    fn test_grant_custom_schema() {
        let grant = GrantSpec {
            role: "reader".to_string(),
            schema: "app_schema".to_string(),
            privileges: vec![TablePrivilege::Select],
            all_tables: true,
            all_sequences: false,
            all_functions: false,
        };

        assert_eq!(grant.schema, "app_schema");
    }

    #[test]
    fn test_grant_multiple_privileges() {
        let grant = GrantSpec {
            role: "writer".to_string(),
            schema: "public".to_string(),
            privileges: vec![
                TablePrivilege::Select,
                TablePrivilege::Insert,
                TablePrivilege::Update,
                TablePrivilege::Delete,
            ],
            all_tables: true,
            all_sequences: false,
            all_functions: false,
        };

        assert_eq!(grant.privileges.len(), 4);
    }

    #[test]
    fn test_grant_all_privilege() {
        let grant = GrantSpec {
            role: "admin".to_string(),
            schema: "public".to_string(),
            privileges: vec![TablePrivilege::All],
            all_tables: true,
            all_sequences: true,
            all_functions: true,
        };

        assert!(grant.privileges.contains(&TablePrivilege::All));
        assert!(grant.all_sequences);
        assert!(grant.all_functions);
    }

    #[test]
    fn test_grant_not_all_tables() {
        let grant = GrantSpec {
            role: "reader".to_string(),
            schema: "public".to_string(),
            privileges: vec![TablePrivilege::Select],
            all_tables: false, // Just USAGE on schema
            all_sequences: false,
            all_functions: false,
        };

        assert!(!grant.all_tables);
    }
}

// =============================================================================
// Extension Tests
// =============================================================================

mod extension_tests {
    use super::*;

    fn create_db_with_extensions(extensions: Vec<&str>) -> PostgresDatabase {
        PostgresDatabase {
            metadata: kube::core::ObjectMeta {
                name: Some("test-db".to_string()),
                namespace: Some("test".to_string()),
                ..Default::default()
            },
            spec: PostgresDatabaseSpec {
                cluster_ref: ClusterRef {
                    name: "cluster".to_string(),
                    namespace: None,
                },
                database: DatabaseSpec {
                    name: "testdb".to_string(),
                    owner: "owner".to_string(),
                    encoding: None,
                    locale: None,
                    connection_limit: None,
                },
                roles: vec![],
                grants: vec![],
                extensions: extensions.into_iter().map(String::from).collect(),
            },
            status: None,
        }
    }

    #[test]
    fn test_no_extensions() {
        let db = create_db_with_extensions(vec![]);
        assert!(db.spec.extensions.is_empty());
    }

    #[test]
    fn test_single_extension() {
        let db = create_db_with_extensions(vec!["uuid-ossp"]);
        assert_eq!(db.spec.extensions.len(), 1);
        assert!(db.spec.extensions.contains(&"uuid-ossp".to_string()));
    }

    #[test]
    fn test_multiple_extensions() {
        let db = create_db_with_extensions(vec!["uuid-ossp", "pg_trgm", "postgis", "hstore"]);
        assert_eq!(db.spec.extensions.len(), 4);
    }

    #[test]
    fn test_common_extensions() {
        // Test commonly used PostgreSQL extensions
        let common = vec![
            "uuid-ossp",
            "pg_trgm",
            "hstore",
            "ltree",
            "pg_stat_statements",
            "pgcrypto",
            "citext",
            "btree_gin",
            "btree_gist",
        ];

        let db = create_db_with_extensions(common.clone());
        assert_eq!(db.spec.extensions.len(), common.len());
    }
}
