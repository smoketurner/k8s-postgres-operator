//! Admission webhooks for PostgresCluster validation
//!
//! This module implements ValidatingAdmissionWebhook for enforcing policies
//! on PostgresCluster resources before they are persisted to etcd.
//!
//! Policies are organized into tiers:
//! - Tier 1 (Critical): Block invalid configurations
//! - Tier 2 (Production): Enforce production requirements based on namespace labels

pub mod policies;
mod server;

pub use policies::{ValidationContext, ValidationResult};
pub use server::{
    WEBHOOK_CERT_PATH, WEBHOOK_KEY_PATH, WEBHOOK_PORT, WebhookError, create_webhook_router,
    run_webhook_server, validate_postgres_cluster,
};
