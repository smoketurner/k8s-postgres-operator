use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use kube::Client;
use kube_leader_election::{LeaseLock, LeaseLockParams};
use tokio::signal;
use tracing::{error, info, warn};

use postgres_operator::health::{HealthState, run_health_server};
use postgres_operator::run_controller;

/// Lease configuration
const LEASE_NAME: &str = "postgres-operator-leader";
const LEASE_TTL_SECS: u64 = 15;
const LEASE_RENEW_INTERVAL_SECS: u64 = 5;

/// Grace period for in-flight reconciliations to complete during shutdown
const SHUTDOWN_GRACE_PERIOD_SECS: u64 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install the TLS crypto provider before any TLS operations
    // Note: install_default() may fail if called multiple times (e.g., in tests),
    // but a single failure during startup is fatal since TLS won't work
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        // Check if a provider is already installed (common in test scenarios)
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            return Err(
                "Failed to install rustls crypto provider and no provider is available".into(),
            );
        }
        // A provider is already installed, which is fine
    }

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("postgres_operator=info".parse()?)
                .add_directive("kube=info".parse()?)
                .add_directive("kube_leader_election=info".parse()?),
        )
        .init();

    info!("Starting postgres-operator");

    // Create Kubernetes client
    let client = Client::try_default().await?;
    info!("Connected to Kubernetes cluster");

    // Get pod identity for leader election
    let pod_name = std::env::var("POD_NAME").unwrap_or_else(|_| {
        warn!("POD_NAME not set, using hostname");
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    });
    let namespace = std::env::var("POD_NAMESPACE").unwrap_or_else(|_| {
        warn!("POD_NAMESPACE not set, using 'default'");
        "default".to_string()
    });

    info!(
        holder_id = %pod_name,
        namespace = %namespace,
        lease_name = LEASE_NAME,
        "Initializing leader election"
    );

    // Create shared health state
    let health_state = Arc::new(HealthState::new());

    // Track leadership status
    let is_leader = Arc::new(AtomicBool::new(false));

    // Start health server immediately (probes should work even as non-leader)
    let health_handle = {
        let health_state = health_state.clone();
        tokio::spawn(async move {
            if let Err(e) = run_health_server(health_state).await {
                error!("Health server error: {}", e);
            }
        })
    };

    // Create leader election lease lock
    let lease_lock = LeaseLock::new(
        client.clone(),
        &namespace,
        LeaseLockParams {
            holder_id: pod_name.clone(),
            lease_name: LEASE_NAME.to_string(),
            lease_ttl: Duration::from_secs(LEASE_TTL_SECS),
        },
    );

    // Acquire leadership before starting controller
    info!("Waiting to acquire leadership...");
    loop {
        match lease_lock.try_acquire_or_renew().await {
            Ok(result) => {
                if result.acquired_lease {
                    info!("Acquired leadership");
                    is_leader.store(true, Ordering::SeqCst);
                    break;
                } else {
                    info!("Another instance is leader, waiting...");
                }
            }
            Err(e) => {
                warn!("Failed to acquire lease: {}, retrying...", e);
            }
        }
        tokio::time::sleep(Duration::from_secs(LEASE_RENEW_INTERVAL_SECS)).await;
    }

    // Start lease renewal background task
    let lease_renewal_handle = {
        let is_leader = is_leader.clone();
        let lease_lock = LeaseLock::new(
            client.clone(),
            &namespace,
            LeaseLockParams {
                holder_id: pod_name,
                lease_name: LEASE_NAME.to_string(),
                lease_ttl: Duration::from_secs(LEASE_TTL_SECS),
            },
        );

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(LEASE_RENEW_INTERVAL_SECS)).await;

                match lease_lock.try_acquire_or_renew().await {
                    Ok(result) => {
                        if !result.acquired_lease {
                            error!("Lost leadership! Shutting down...");
                            is_leader.store(false, Ordering::SeqCst);
                            // Exit so Kubernetes restarts us and we re-enter election
                            std::process::exit(1);
                        }
                    }
                    Err(e) => {
                        error!("Failed to renew lease: {}. Shutting down...", e);
                        is_leader.store(false, Ordering::SeqCst);
                        std::process::exit(1);
                    }
                }
            }
        })
    };

    info!(
        "Watching PostgresCluster resources (apiVersion: postgres-operator.smoketurner.com/v1alpha1)"
    );

    // Start controller (only runs as leader)
    let controller_handle = {
        let health_state = health_state.clone();
        tokio::spawn(async move {
            run_controller(client, Some(health_state)).await;
        })
    };

    // Wait for any task to complete (or fail), or shutdown signal
    tokio::select! {
        result = controller_handle => {
            if let Err(e) = result {
                error!("Controller task panicked: {}", e);
            }
        }
        result = health_handle => {
            if let Err(e) = result {
                error!("Health server task panicked: {}", e);
            }
        }
        // Lease renewal task only exits via process::exit() or panic
        // so this branch is only reached on panic
        Err(e) = lease_renewal_handle => {
            error!("Lease renewal task panicked: {}", e);
        }
        // Handle graceful shutdown on SIGTERM or SIGINT
        _ = shutdown_signal() => {
            info!("Received shutdown signal, initiating graceful shutdown...");

            // Mark as not ready to stop receiving new work
            health_state.set_ready(false).await;
            info!("Marked operator as not ready");

            // Give in-flight reconciliations time to complete
            info!(
                "Waiting {}s for in-flight reconciliations to complete...",
                SHUTDOWN_GRACE_PERIOD_SECS
            );
            tokio::time::sleep(Duration::from_secs(SHUTDOWN_GRACE_PERIOD_SECS)).await;

            info!("Grace period complete, shutting down");
        }
    }

    info!("Operator stopped");
    Ok(())
}

/// Wait for shutdown signal (SIGTERM or SIGINT)
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
