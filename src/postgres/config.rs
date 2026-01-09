//! PostgreSQL configuration utilities
//!
//! This module contains utilities for generating PostgreSQL configuration
//! files and managing PostgreSQL-specific settings.

/// Default PostgreSQL configuration parameters
pub const DEFAULT_MAX_CONNECTIONS: i32 = 100;
pub const DEFAULT_SHARED_BUFFERS: &str = "128MB";
pub const DEFAULT_WAL_LEVEL: &str = "replica";
pub const DEFAULT_MAX_WAL_SENDERS: i32 = 10;
pub const DEFAULT_WAL_KEEP_SIZE: &str = "1GB";

/// Get the PostgreSQL image for a given version
pub fn get_postgres_image(version: &str) -> String {
    format!("postgres:{}", version)
}

/// Validate a PostgreSQL version string
pub fn validate_version(version: &str) -> bool {
    // Accept major versions like "15", "16" or full versions like "15.4", "16.1"
    let parts: Vec<&str> = version.split('.').collect();
    if parts.is_empty() || parts.len() > 2 {
        return false;
    }

    // Check that the first part is a valid major version (12-20)
    if let Ok(major) = parts[0].parse::<i32>() {
        if !(12..=20).contains(&major) {
            return false;
        }
    } else {
        return false;
    }

    // If there's a minor version, validate it
    if parts.len() == 2 && parts[1].parse::<i32>().is_err() {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_version() {
        assert!(validate_version("15"));
        assert!(validate_version("16"));
        assert!(validate_version("15.4"));
        assert!(validate_version("16.1"));
        assert!(!validate_version("11")); // Too old
        assert!(!validate_version("21")); // Too new
        assert!(!validate_version("invalid"));
        assert!(!validate_version("15.4.1")); // Too many parts
    }

    #[test]
    fn test_get_postgres_image() {
        assert_eq!(get_postgres_image("16"), "postgres:16");
        assert_eq!(get_postgres_image("15.4"), "postgres:15.4");
    }
}
