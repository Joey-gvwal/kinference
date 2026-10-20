//! The `kinference` server: the gRPC picker, the admin listener, discovery and
//! the policy machinery, wired together and run until shutdown.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::config::Config;
use crate::controller::discovery::KubeDiscovery;
use crate::controller::leader;
use crate::controller::metrics::ControllerMetrics;
use crate::controller::policy;
use crate::router::body_store;
use crate::router::metrics::RouterMetrics;
use crate::router::observations;
use crate::router::proto::picker_server::PickerServer;
use crate::router::service::KinferenceService;
use crate::router::tokenizer::{http, local};
use crate::{admin, metrics, version};
use kinference_fleet::coordinator::{Coordinator, Handles, WorkerTaskSettings};
use kinference_fleet::discovery::{DiscoveryFactory, DiscoveryTask};
use kinference_fleet::metrics::FleetMetrics;
use kinference_fleet::models::ModelIndex;
use kinference_fleet::pool;
use kinference_fleet::scrape::MetricsRegistry;

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

    // The elector is the one task whose work must complete after
    // cancellation: vacating the Lease lets a peer take over immediately.
    // Held here so shutdown can await it before the runtime drops.
    let mut leader_task: Option<tokio::task::JoinHandle<()>> = None;

    let metrics = Arc::new(MetricsRegistry::new());
    // One Prometheus registry; each group registers its families, and
    // the admin listener renders it on `/metrics`.
    let registry = metrics::registry();
    let router_prom = Arc::new(RouterMetrics::new(&registry, &cfg.metrics));
    let fleet_prom = Arc::new(FleetMetrics::new(&registry));
    let controller_prom = Arc::new(ControllerMetrics::new(&registry));
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

    // Pending-observation stash, inserted at pick and consumed at report.
    // Its TTL sweep, which also releases each evicted pick's inflight
    // state, is spawned below once the service exists.
    let pending_observations = observations::shared(observation_ttl);
    // Per-request metric labels (model + qos), same TTL/lifecycle as the
    // pending-observation stash; taken at ReportFinalToken, TTL-swept.
    let request_labels = observations::shared_request_labels(observation_ttl);

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
    let body_store = body_store::shared(body_max_total_bytes, body_max_request_bytes, body_ttl);
    tracing::info!(
        max_total_bytes = body_max_total_bytes,
        max_request_bytes = body_max_request_bytes,
        ttl_secs = body_ttl.as_secs(),
        idle_timeout_secs = body_idle_timeout.as_secs(),
        "Body buffer configured"
    );

    // One `/render` client for every pool, so connections pool.
    let pool_max_idle_per_host = cfg.tokenize.pool_max_idle_per_host.unwrap_or(16);
    let http = Arc::new(
        http::HttpRender::new(pool_max_idle_per_host, metrics.clone())
            .map_err(|e| anyhow::anyhow!("failed to build tokenize HTTP client: {e}"))?,
    );
    tracing::info!(pool_max_idle_per_host, "tokenize HTTP client built");

    // The local backend is fleet-wide and cheap to build, so it is always
    // built; each pool's tokenize mode decides whether to use it.
    let warmup_enabled = cfg.tokenize.local.warmup;
    let local_tokenizer = Arc::new(
        local::LocalTokenizer::new(
            cfg.tokenize.local.clone(),
            http.clone(),
            router_prom.clone(),
        )
        .map_err(|e| anyhow::anyhow!("failed to build the local tokenizer: {e}"))?,
    );
    tracing::info!(
        mount_dir = ?cfg.tokenize.local.mount_dir,
        warmup = warmup_enabled,
        "Local tokenize registry built"
    );
    if warmup_enabled {
        local_tokenizer.clone().spawn_warmup(
            pool_registry.clone(),
            models.subscribe(),
            cancel.clone(),
        );
    }

    // In-cluster credentials, acquired once here so pool creation stays
    // synchronous and infallible. Required (fatal) when the config declares
    // a worker set; with only `kubernetes.enabled`, a failure surfaces per
    // pool instead, so CR resolution can run outside a cluster.
    let kube_client = if !specs.is_empty() {
        Some(KubeDiscovery::client().await?)
    } else if cfg.kubernetes.enabled {
        match KubeDiscovery::client().await {
            Ok(c) => Some(c),
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "kubernetes.enabled is set but in-cluster credentials are \
                     unavailable; InferenceWorkerSets will resolve but their pools \
                     cannot start discovery"
                );
                None
            }
        }
    } else {
        None
    };
    let kubernetes: Option<DiscoveryFactory> = kube_client.map(|client| {
        Arc::new(move |cfg, tx, cancel| {
            Box::pin(KubeDiscovery::from_client(cfg, client.clone()).run(tx, cancel))
                as DiscoveryTask
        }) as DiscoveryFactory
    });

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
    // Written by the worker-set watch, read by the status writer. Empty
    // with `kubernetes.enabled` off.
    let rejection_store = policy::worker_sets::RejectionStore::new();
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

    // The manager moves into one task, so it needs no lock. With
    // `kubernetes.enabled`, that task also watches `InferenceWorkerSet`s;
    // otherwise it only reaps.
    if cfg.kubernetes.enabled {
        // Inferred, so it also works outside a cluster.
        let client = kube::Client::try_from(kube::Config::infer().await?)?;
        let watcher = policy::worker_sets::WorkerSetWatcher::new(
            &cfg.kubernetes,
            cfg.profiles.clone(),
            pool_manager,
            rejection_store.clone(),
            controller_prom.clone(),
        );
        let cancel = cancel.clone();
        tokio::spawn(async move { watcher.run(client, cancel).await });
    } else {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tick.tick() => { pool_manager.sweep(); }
                }
            }
        });
    }
    tokio::spawn(coordinator.run(inbox));

    // One `PolicyStore` for the process, made before anything reads it.
    // `None` when policy is off, which `/readyz` treats as nothing to wait
    // for.
    let policy_store_cell = cfg.policy.enabled.then(policy::store::PolicyStore::new);

    // Admin listener, when configured.
    if let Some(dash_addr) = cfg.admin_listen {
        // `/readyz` gates on the policy store being listed, so picks don't
        // bypass policy at startup.
        let dash_pools = pool_registry.clone();
        let dash_policy = policy_store_cell.clone();
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

    // The observation CSV sink is shared across pools; rows carry their
    // pool and config snapshot.
    let observation_sink = Arc::new(observations::ObservationSink::open(
        cfg.observations_csv.clone(),
    ));

    // Optional CRD-driven routing policy, sharing the snapshot cell the
    // pick path reads. When disabled, a pool-naming Pick falls back to
    // model-based routing.
    let policy_bits = if cfg.policy.enabled {
        if !cfg.kubernetes.enabled {
            tracing::warn!(
                "policy.enabled is set but kubernetes.enabled is not, so no pool has a \
                 namespaced identity for an InferenceWorkerSet to resolve to — every \
                 destination will report NoMatchingPool. Policy routes between worker \
                 sets that the CRs themselves define; set kubernetes.enabled = true."
            );
        }
        let store = policy_store_cell
            .clone()
            .expect("created whenever policy.enabled is set");
        // Shared: the service writes it from the feedback handlers and the
        // status writer publishes it.
        let provider_health =
            Arc::new(kinference_policy::provider_health::ProviderHealthStore::new());
        let watcher = policy::store::PolicyWatcher::new(
            cfg.policy.namespace.clone(),
            pool_registry.clone(),
            rejection_store.clone(),
            store.clone(),
        )
        .await?;
        {
            let cancel = cancel.clone();
            tokio::spawn(async move { watcher.run(cancel).await });
        }
        // Only one replica writes status; everything else runs on every
        // replica. With election disabled the channel is set to `true` and
        // no elector is spawned.
        let leader_rx = if cfg.leader_election.enabled {
            let (tx, rx) = tokio::sync::watch::channel(false);
            let ns = cfg
                .leader_election
                .namespace
                .clone()
                .or_else(kube_namespace_hint)
                .unwrap_or_else(|| "default".to_string());
            let identity = cfg.leader_election.identity.clone();
            let params = leader::LeaseParams::from(&cfg.leader_election);
            // `Config::infer`, like the status writer.
            let kube_cfg = kube::Config::infer().await?;
            let client = kube::Client::try_from(kube_cfg)?;
            tracing::info!(
                lease = %params.name,
                namespace = %ns,
                identity = %identity,
                lease_duration_secs = params.lease_duration.as_secs(),
                "leader election enabled; only the holder writes CRD status"
            );
            let shutdown = {
                let cancel = cancel.clone();
                async move { cancel.cancelled().await }
            };
            leader_task = Some(tokio::spawn(async move {
                leader::run(client, &ns, identity, params, tx, shutdown).await;
            }));
            rx
        } else {
            // Always leader. The sender is leaked so the channel never
            // closes, which the writers would log as an error.
            let (tx, rx) = tokio::sync::watch::channel(true);
            std::mem::forget(tx);
            rx
        };

        // A conforming gateway won't program a route until the pool reports
        // Accepted=True, so nothing routes until this runs.
        policy::status::spawn(
            policy::status::StatusHandles {
                store: store.clone(),
                pools: pool_registry.clone(),
                rejections: rejection_store.clone(),
                metrics: metrics.clone(),
                provider_health: provider_health.clone(),
                leader: leader_rx.clone(),
                prom: controller_prom.clone(),
            },
            cancel.clone(),
        )
        .await?;

        // `BackendBinding`s let a gateway resolve a route naming an
        // `InferencePool`. Leader-gated like status.
        if cfg.policy.backend_bindings {
            policy::binding::spawn(
                cfg.policy.namespace.clone(),
                store.clone(),
                cfg.policy.binding_extension().map(str::to_string),
                leader_rx,
                cancel.clone(),
            )
            .await?;
        }
        Some((store, provider_health))
    } else {
        None
    };

    // Provider health exists even with policy off, so the feedback RPCs
    // needn't check.
    let (policy_store, provider_health) = match policy_bits {
        Some((store, health)) => (Some(store), health),
        None => (
            None,
            Arc::new(kinference_policy::provider_health::ProviderHealthStore::new()),
        ),
    };
    let service = Arc::new(KinferenceService::new(
        pool_registry.clone(),
        models.clone(),
        metrics.clone(),
        http.clone(),
        local_tokenizer,
        pending_observations.clone(),
        request_labels.clone(),
        body_store.clone(),
        policy_store,
        provider_health,
        body_idle_timeout,
        observation_sink,
        router_prom,
    ));

    // Body-buffer reaper, separate from the observation sweep since the TTLs
    // differ greatly. It guarantees release (`Abort` only shortens the
    // window) and refreshes the occupancy gauges.
    {
        let service = service.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        let (reaped, bytes) = service.sweep_expired_bodies();
                        if reaped > 0 {
                            service.prom.record_bodies_reaped(reaped as u64);
                            tracing::debug!(
                                reaped,
                                bytes,
                                "body_store sweep: reclaimed buffered bodies past TTL"
                            );
                        }
                        let (entries, in_use) = service.body_store.stats();
                        service.prom.set_body_buffer(entries as u64, in_use);
                    }
                }
            }
        });
    }

    // Pending-observation TTL sweep. Releases each expired pick's inflight
    // state, which has no sweep of its own, and bounds the stash.
    {
        let service = service.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        let evicted = service.sweep_expired_observations();
                        if evicted > 0 {
                            tracing::debug!(
                                evicted,
                                "pending_observations sweep: evicted entries past TTL (inflight state released)"
                            );
                        }
                        // So a deleted provider's entry doesn't linger.
                        let dropped = service.provider_health.sweep();
                        if dropped > 0 {
                            tracing::debug!(
                                dropped,
                                "provider health sweep: dropped providers with no live samples or failures"
                            );
                        }
                    }
                }
            }
        });
    }

    let server_result = Server::builder()
        .add_service(
            PickerServer::from_arc(service)
                // Above tonic's 4 MiB default, so a body sent as one chunk
                // hits our own budget (RESOURCE_EXHAUSTED) rather than an
                // opaque transport error. Callers should still chunk.
                .max_decoding_message_size((body_max_request_bytes + 1024 * 1024) as usize),
        )
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await;

    cancel.cancel();

    // Give the elector a bounded moment to vacate the Lease, so a slow
    // apiserver can't hold the pod past its grace period. Otherwise the handover
    // waits out the lease, as on SIGKILL.
    if let Some(task) = leader_task {
        const VACATE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        match tokio::time::timeout(VACATE_BUDGET, task).await {
            Ok(Ok(())) => tracing::debug!("leader elector stopped; lease released if held"),
            Ok(Err(e)) => tracing::warn!(error = %e, "leader elector task failed"),
            Err(_) => tracing::warn!(
                "leader elector did not finish within {VACATE_BUDGET:?}; a peer will \
                 take over after the lease expires instead of immediately"
            ),
        }
    }

    server_result?;
    tracing::info!("Shut down complete");
    Ok(())
}

/// The pod's own namespace, from the service-account mount. Used when
/// `leader_election.namespace` is unset so a normal in-cluster deployment
/// needs no configuration at all.
fn kube_namespace_hint() -> Option<String> {
    std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
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
