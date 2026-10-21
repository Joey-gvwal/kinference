//! The `kinference` server: the gRPC picker, the admin listener, discovery and
//! the policy machinery, wired together and run until shutdown.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::config::Config;
use crate::{admin, metrics, version};
use kinference_controller::{self as controller, ControllerHandles};
use kinference_fleet::Fleet;
use kinference_router::grpc;
use kinference_router::service::{KinferenceService, PolicyHandles};

#[derive(Parser, Debug)]
#[command(
    name = "kinference",
    version = version::VERSION,
    about = "KV-cache aware routing scorer"
)]
struct Args {
    /// Path to the JSON config file.
    #[arg(long, short = 'c')]
    config: PathBuf,
}

/// Parse the command line and run the server until it is told to stop.
pub async fn run() -> Result<()> {
    // ANSI colors only on a TTY, and not with NO_COLOR (https://no-color.org).
    // Read here, before the config file, because it configures the log
    // subscriber; every other env var is read by `Config::load`.
    let use_ansi = std::io::IsTerminal::is_terminal(&std::io::stdout())
        && std::env::var_os("NO_COLOR").is_none();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_ansi(use_ansi)
        .init();

    let args = Args::parse();
    let cfg = Config::load(&args.config)?;

    let listen_addr = cfg.listen;
    tracing::info!(
        version = version::VERSION,
        listen = %listen_addr,
        worker_sets = cfg.worker_sets.len(),
        profiles = cfg.profiles.len(),
        "Starting kinference"
    );

    let cancel = CancellationToken::new();
    // Pre-bind so READY is logged only once the socket is listening.
    // `TcpIncoming::new` binds synchronously and fails with an io::Error.
    let incoming = tonic::transport::server::TcpIncoming::new(listen_addr, true, None)
        .map_err(|e| anyhow::anyhow!("failed to bind gRPC listener on {listen_addr}: {e}"))?;

    let shutdown = async {
        wait_for_shutdown_signal().await;
    };

    // One Prometheus registry; each group registers its families, and
    // the admin listener renders it on `/metrics`.
    let registry = metrics::registry();

    // One `PolicyStore` for the process, made before anything reads it.
    // `None` when policy is off, which `/readyz` treats as nothing to wait
    // for. Provider health exists even with policy off, so the feedback RPCs
    // needn't check; the status writer publishes what the service records.
    let policy_store = cfg
        .policy
        .enabled
        .then(kinference_policy::store::PolicyStore::new);
    let provider_health = Arc::new(kinference_policy::provider_health::ProviderHealthStore::new());

    let kubernetes =
        controller::discovery::factory(cfg.kubernetes.enabled, !cfg.worker_sets.is_empty())
            .context("building Kubernetes discovery")?;
    let fleet = Fleet::new(cfg.fleet()?, kubernetes, &registry, cancel.clone());
    let handles = fleet.handles.clone();

    let service = KinferenceService::start(
        cfg.router(),
        handles.clone(),
        PolicyHandles {
            store: policy_store.clone(),
            provider_health: provider_health.clone(),
        },
        &registry,
        cancel.clone(),
    )
    .context("starting the router")?;

    // Config-declared sets are materialized now, CR-declared ones with the
    // watch. Their keys can't collide: CR keys are namespaced, config keys
    // never are.
    let manager = fleet.start().context("starting the fleet")?;

    let controller = controller::start(
        cfg.controller(),
        ControllerHandles {
            manager,
            pools: handles.pools.clone(),
            scrape: handles.scrape.clone(),
            store: policy_store.clone(),
            provider_health,
        },
        &registry,
        cancel.clone(),
    )
    .await
    .context("starting the control plane")?;

    // Admin listener, when configured.
    if let Some(dash_addr) = cfg.admin_listen {
        // `/readyz` gates on the policy store being listed, so picks don't
        // bypass policy at startup.
        let dash_pools = handles.pools.clone();
        let dash_policy = policy_store.clone();
        let dash_metrics = handles.scrape.clone();
        let dash_registry = registry.clone();
        let dash_cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(err) = admin::serve(
                dash_addr,
                dash_pools,
                dash_policy,
                dash_registry,
                dash_metrics,
                dash_cancel,
            )
            .await
            {
                tracing::error!(error = %err, "admin server exited with error");
            }
        });
        tracing::info!(listen = %dash_addr, "Admin listener enabled");
    }

    tracing::info!(
        listen = %listen_addr,
        worker_sets = handles.pools.snapshot().len(),
        "READY: gRPC accepting connections"
    );

    let server_result = Server::builder()
        .add_service(grpc::server(service))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await;

    cancel.cancel();

    controller.shutdown().await;

    server_result?;
    tracing::info!("Shut down complete");
    Ok(())
}

/// Resolve on SIGINT **or SIGTERM**.
///
/// SIGTERM is what Kubernetes and `docker stop` send; without handling it
/// there is no graceful shutdown (no draining, no Lease vacate).
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                // Degrade to SIGINT-only rather than refusing to start.
                tracing::warn!(error = %e, "failed to install SIGTERM handler; SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!(signal = "SIGINT", "shutting down");
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!(signal = "SIGINT", "shutting down"),
            _ = term.recv() => tracing::info!(signal = "SIGTERM", "shutting down"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(signal = "SIGINT", "shutting down");
    }
}
