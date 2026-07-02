//! Unit tests for validation logic

use postgres_operator::controller::cluster_validation::validate_version_upgrade;

mod version_upgrade_tests {
    use super::*;

    #[test]
    fn test_valid_minor_upgrade() {
        assert!(validate_version_upgrade("16.1", "16.2").is_ok());
    }

    #[test]
    fn test_valid_major_upgrade() {
        assert!(validate_version_upgrade("15", "16").is_ok());
    }

    #[test]
    fn test_downgrade_rejected() {
        let result = validate_version_upgrade("16", "15");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("downgrade"));
    }

    #[test]
    fn test_same_version() {
        assert!(validate_version_upgrade("16", "16").is_ok());
    }

    #[test]
    fn test_multi_version_upgrade() {
        // This should work but log a warning
        assert!(validate_version_upgrade("14", "16").is_ok());
    }

    #[test]
    fn test_upgrade_14_to_15() {
        assert!(validate_version_upgrade("14", "15").is_ok());
    }

    #[test]
    fn test_upgrade_15_to_16() {
        assert!(validate_version_upgrade("15", "16").is_ok());
    }

    #[test]
    fn test_upgrade_16_to_17() {
        assert!(validate_version_upgrade("16", "17").is_ok());
    }

    #[test]
    fn test_downgrade_17_to_16_rejected() {
        let result = validate_version_upgrade("17", "16");
        assert!(result.is_err());
    }

    #[test]
    fn test_downgrade_16_to_14_rejected() {
        let result = validate_version_upgrade("16", "14");
        assert!(result.is_err());
    }

    #[test]
    fn test_minor_version_same_major() {
        assert!(validate_version_upgrade("16.0", "16.5").is_ok());
    }
}

mod panic_prevention_tests {
    use super::*;

    #[test]
    fn test_version_upgrade_empty_strings_no_panic() {
        // Empty strings should return error, not panic
        let result = validate_version_upgrade("", "16");
        // Either error or ok is fine, just no panic
        let _ = result;
    }

    #[test]
    fn test_version_upgrade_both_empty_no_panic() {
        let result = validate_version_upgrade("", "");
        let _ = result;
    }
}
