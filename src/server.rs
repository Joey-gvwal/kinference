//! The `kinference` server: the gRPC picker, the admin listener, discovery and
//! the policy machinery, wired together and run until shutdown.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::config::Config;
use crate::controller::{self, ControllerConfig, ControllerHandles};
use crate::{admin, metrics, version};
use kinference_fleet::coordinator::{Coordinator, Handles, WorkerTaskSettings};
use kinference_fleet::metrics::FleetMetrics;
use kinference_fleet::models::ModelIndex;
use kinference_fleet::pool;
use kinference_fleet::scrape::MetricsRegistry;
use kinference_router::body_store::BodyLimits;
use kinference_router::grpc;
use kinference_router::service::{FleetHandles, KinferenceService, PolicyHandles, RouterConfig};

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

    let metrics = Arc::new(MetricsRegistry::new());
    // One Prometheus registry; each group registers its families, and
    // the admin listener renders it on `/metrics`.
    let registry = metrics::registry();
    let fleet_prom = Arc::new(FleetMetrics::new(&registry));
    let poll_ms = cfg.metrics_poll_ms.unwrap_or(1000);
    let models_refresh_secs = cfg.models_refresh_secs.unwrap_or(30);
    let ingest_cfg = cfg.ingest;

    // One `PoolSpec` per `workerset` entry, resolved against its profile.
    // Each pool owns its own KvIndexer, WorkerRegistry, InflightPrefixes,
    // InflightRegistry and pick lock.
    //
    // The pools themselves are built by `PoolManager` further down, once
    // the coordinator and eviction channels exist; publishing one earlier
    // would make it visible to picks while nothing fed it.
    let specs = cfg.pool_specs()?;
    // The published pool set every consumer reads.
    let pool_registry = pool::PoolRegistry::new();
    for spec in &specs {
        tracing::info!(
            workerset = spec.name,
            profile = spec.profile,
            block_size = spec.kv_block_size,
            service = ?spec.discovery.service_name,
            "Worker set configured"
        );
    }

    let models = Arc::new(ModelIndex::new());

    // TTL for a pick's in-flight bookkeeping (pending observation +
    // inflight counter + inflight-prefix entry). Must exceed worst-case
    // *full* response time; see `Config::observation_ttl_secs`.
    let observation_ttl = std::time::Duration::from_secs(cfg.observation_ttl_secs.unwrap_or(600));

    // Buffered bodies for PushBody, Pick, PullBody. A much shorter TTL than
    // `observation_ttl`: bodies are large and consumed immediately.
    let body_ttl = std::time::Duration::from_secs(cfg.body_ttl_secs.unwrap_or(60));
    let body_idle_timeout =
        std::time::Duration::from_secs(cfg.body_idle_timeout_secs.unwrap_or(30));
    // Config is in KB; everything downstream is in bytes. `saturating_mul`
    // so an absurd value clamps instead of wrapping to a tiny cap.
    const KB: u64 = 1024;
    let body_max_request_bytes = cfg
        .body_max_request_kb
        .unwrap_or(8 * 1024)
        .saturating_mul(KB);
    let body_max_total_bytes = cfg
        .body_max_total_kb
        .unwrap_or(256 * 1024)
        .saturating_mul(KB);
    let bodies = BodyLimits {
        max_total_bytes: body_max_total_bytes,
        max_request_bytes: body_max_request_bytes,
        ttl: body_ttl,
        idle_timeout: body_idle_timeout,
    };
    tracing::info!(
        max_total_bytes = body_max_total_bytes,
        max_request_bytes = body_max_request_bytes,
        ttl_secs = body_ttl.as_secs(),
        idle_timeout_secs = body_idle_timeout.as_secs(),
        "Body buffer configured"
    );

    // One `PolicyStore` for the process, made before anything reads it.
    // `None` when policy is off, which `/readyz` treats as nothing to wait
    // for. Provider health exists even with policy off, so the feedback RPCs
    // needn't check; the status writer publishes what the service records.
    let policy_store = cfg
        .policy
        .enabled
        .then(kinference_policy::store::PolicyStore::new);
    let provider_health = Arc::new(kinference_policy::provider_health::ProviderHealthStore::new());

    let service = KinferenceService::start(
        RouterConfig {
            tokenize: cfg.tokenize.clone(),
            metrics: cfg.metrics.clone(),
            bodies,
            observation_ttl,
            observations_csv: cfg.observations_csv.clone(),
        },
        FleetHandles {
            pools: pool_registry.clone(),
            models: models.clone(),
            scrape: metrics.clone(),
        },
        PolicyHandles {
            store: policy_store.clone(),
            provider_health: provider_health.clone(),
        },
        &registry,
        cancel.clone(),
    )
    .context("starting the router")?;

    let kubernetes = controller::discovery::factory(cfg.kubernetes.enabled, !specs.is_empty())
        .context("building Kubernetes discovery")?;

    // Pools can be built now. `PoolManager` owns the live set and
    // publishes it to the registry; the coordinator starts each pool's
    // discovery and owns its workers.
    let (coordinator, inbox) = Coordinator::new(
        Handles {
            cancel: cancel.clone(),
            kubernetes,
            metrics: metrics.clone(),
            prom: fleet_prom,
            models: models.clone(),
        },
        WorkerTaskSettings {
            poll_ms,
            models_refresh_secs,
            ingest: ingest_cfg,
            observation_ttl,
        },
    );
    let mut pool_manager = pool::PoolManager::new(
        pool_registry.clone(),
        coordinator.clone(),
        cancel.clone(),
        models.clone(),
        pool::ReapPolicy {
            linger: std::time::Duration::from_secs(30),
            // A retired pool must outlive observations that may still
            // reference it.
            backstop: observation_ttl.max(std::time::Duration::from_secs(600)),
        },
    );
    // Config-declared sets are materialized now, CR-declared ones with the
    // watch. Their keys can't collide: CR keys are namespaced, config keys
    // never are.
    let report = pool_manager.reconcile(specs.clone());
    if !report.failed.is_empty() {
        // Fatal at startup, unlike at runtime, so a misconfigured set
        // doesn't silently shrink the fleet.
        anyhow::bail!(
            "failed to start worker set(s): {}",
            report
                .failed
                .iter()
                .map(|k| k.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let live_pools = pool_registry.snapshot();
    tokio::spawn(coordinator.run(inbox));

    let controller = controller::start(
        ControllerConfig {
            kubernetes: cfg.kubernetes.clone(),
            policy: cfg.policy.clone(),
            leader_election: cfg.leader_election.clone(),
            profiles: cfg.profiles.clone(),
        },
        ControllerHandles {
            manager: pool_manager,
            pools: pool_registry.clone(),
            scrape: metrics.clone(),
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
        let dash_pools = pool_registry.clone();
        let dash_policy = policy_store.clone();
        let dash_metrics = metrics.clone();
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
        worker_sets = live_pools.len(),
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
