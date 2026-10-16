//! The `kinference` server: the gRPC picker, the admin listener, discovery and
//! the policy machinery, wired together and run until shutdown.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use dashmap::DashMap;
use dynamo_kv_router::indexer::KvIndexerInterface;
use kinference_policy::PoolId;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::config::Config;
use crate::controller::leader;
use crate::controller::policy;
use crate::fleet::discovery::{
    Discovery, DiscoveryMode, WorkerChange, WorkerConfig, kubernetes::KubeDiscovery,
    static_config::StaticDiscovery,
};
use crate::fleet::eviction;
use crate::fleet::ingest;
use crate::fleet::models::ModelIndex;
use crate::fleet::pool;
use crate::fleet::pool::Pool;
use crate::fleet::rollup;
use crate::fleet::scrape::{MetricsRegistry, spawn_metrics_poller, spawn_one_shot_warmup};
use crate::fleet::workers::{WorkerEntry, WorkerRegistry};
use crate::router::body_store;
use crate::router::local_tokenize;
use crate::router::mock::MockKinferenceService;
use crate::router::observations;
use crate::router::proto::picker_server::PickerServer;
use crate::router::service::KinferenceService;
use crate::{admin, metrics, version};

#[derive(Parser, Debug)]
#[command(
    name = "kinference",
    version = version::VERSION,
    about = "KV-cache aware routing scorer"
)]
struct Args {
    /// Path to the TOML config file.
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
        mock_mode = cfg.mock_mode,
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

    // The two service implementations have different types, so serve inside
    // each arm.
    let server_result = if cfg.mock_mode {
        // Mock mode flattens every pool's static workers into one registry
        // and answers deterministically, with no per-pool tuning.
        let registry = Arc::new(WorkerRegistry::new());
        let mut next_num: u64 = 1;
        for spec in &cfg.pool_specs()? {
            let workers = spec.load_static_workers()?;
            for w in &workers {
                tracing::info!(
                    workerset = spec.name,
                    worker_id = w.worker_id_str,
                    model_name = w.model_name,
                    block_size = spec.kv_block_size,
                    "Mock worker"
                );
                let entry = WorkerEntry::from_config(w, spec.kv_block_size);
                registry.register(w.worker_id_num, entry);
                next_num = next_num.max(w.worker_id_num + 1);
            }
        }
        let _ = next_num;
        tracing::info!("MOCK mode: ingest, metrics polling, and /tokenize are all bypassed");
        if cfg.admin_listen.is_some() {
            // Mock data on the admin listener would be misleading.
            tracing::warn!(
                "admin_listen is set but ignored in mock mode (its telemetry is for diagnosing real-world traffic)"
            );
        }
        tracing::info!(
            listen = %listen_addr,
            worker_sets = cfg.worker_sets.len(),
            mock_mode = true,
            "READY: gRPC accepting connections"
        );
        let service = MockKinferenceService::new(registry.clone());
        Server::builder()
            .add_service(
                PickerServer::new(service)
                    // See the note on the real server below.
                    .max_decoding_message_size(9 * 1024 * 1024),
            )
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
    } else {
        let metrics = Arc::new(MetricsRegistry::new());
        // Prometheus registry, shared by the gRPC handlers and the admin
        // listener's `/metrics`.
        let prom = Arc::new(metrics::PromMetrics::with_config(&cfg.metrics));
        let poll_ms = cfg.metrics_poll_ms.unwrap_or(1000);
        let models_refresh_secs = cfg.models_refresh_secs.unwrap_or(30);
        let ingest_cfg = cfg.ingest;

        // One `PoolSpec` per `[[workerset]]`, resolved against its profile.
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
                discovery = ?spec.discovery.mode,
                "Worker set configured"
            );
        }

        // Model to pools, and the worker to pool reverse index.
        let models = Arc::new(ModelIndex::new());
        let worker_pool: Arc<DashMap<u64, Arc<Pool>>> = Arc::new(DashMap::new());

        // TTL for a pick's in-flight bookkeeping (pending observation +
        // inflight counter + inflight-prefix entry). Must exceed worst-case
        // *full* response time; see `Config::observation_ttl_secs`.
        let observation_ttl =
            std::time::Duration::from_secs(cfg.observation_ttl_secs.unwrap_or(600));

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

        // Shared HTTP client for tokenization, so connections pool.
        //
        // Settings mirror the Gateway API Inference Extension EPP's. vLLM speaks
        // HTTP/1.1 only, so force it; without multiplexing that means several
        // connections per host (`tokenize.pool_max_idle_per_host`, default 16;
        // raise to peak concurrency with a single `render_url`).
        // `pool_idle_timeout = 90s` matches the EPP and AWS NLB idle tolerance;
        // `tcp_keepalive` detects half-closed connections.
        let pool_max_idle_per_host = cfg.tokenize.pool_max_idle_per_host.unwrap_or(16);
        let tokenize_client = Arc::new(
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .pool_idle_timeout(std::time::Duration::from_secs(90))
                .pool_max_idle_per_host(pool_max_idle_per_host)
                .tcp_keepalive(std::time::Duration::from_secs(30))
                .tcp_nodelay(true)
                .http1_only()
                .build()
                .map_err(|e| anyhow::anyhow!("failed to build tokenize HTTP client: {e}"))?,
        );
        tracing::info!(pool_max_idle_per_host, "tokenize HTTP client built");

        // The local tokenizer registry is fleet-wide and cheap to build, so it
        // is always built; each pool's tokenize mode decides whether to use it.
        // It must exist before the coordinator spawns, for `handle_added`.
        let warmup_enabled = cfg.tokenize.local.warmup;
        let local_tokenizer =
            match local_tokenize::LocalTokenizerRegistry::new(cfg.tokenize.local.clone()) {
                Ok(reg) => {
                    tracing::info!(
                        mount_dir = ?cfg.tokenize.local.mount_dir,
                        warmup = warmup_enabled,
                        "Local tokenize registry built"
                    );
                    Some(Arc::new(reg))
                }
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        "Failed to build LocalTokenizerRegistry; local/compare \
                         pools will fall back to HTTP /render"
                    );
                    None
                }
            };

        // Coordinator channel of (pool, change). Each pool runs its own
        // discovery, and a per-pool forwarder tags its events with the pool.
        let (coord_tx, mut rx) = tokio::sync::mpsc::channel::<(Arc<Pool>, WorkerChange)>(64);
        // Per-worker cancellation tokens so Removed can stop a single
        // worker's listeners without disturbing the rest of the fleet.
        //
        // Keyed by (pool, worker), so when a worker moves between worker sets
        // the old pool's Removed doesn't cancel the new pool's listeners.
        let task_tokens: Arc<DashMap<(PoolId, u64), CancellationToken>> = Arc::new(DashMap::new());

        // Ingest and metrics request a worker's eviction here on a homogeneity
        // violation; the coordinator runs the same teardown as for Removed.
        let (eviction_tx, mut eviction_rx) = eviction::channel();

        // In-cluster credentials, acquired once here so pool creation stays
        // synchronous and infallible. Required (fatal) when a TOML worker set
        // uses kubernetes discovery; with only `[kubernetes]` on, a failure
        // surfaces per pool instead, so CR resolution can run outside a
        // cluster.
        let needs_kube_now = specs
            .iter()
            .any(|s| s.discovery.mode == DiscoveryMode::Kubernetes);
        let kube_client = if needs_kube_now {
            Some(KubeDiscovery::client().await?)
        } else if cfg.kubernetes.enabled {
            match KubeDiscovery::client().await {
                Ok(c) => Some(c),
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        "[kubernetes] is enabled but in-cluster credentials are \
                         unavailable; InferenceWorkerSets will resolve but their pools \
                         cannot start discovery"
                    );
                    None
                }
            }
        } else {
            None
        };

        // Pools can be built now. `PoolManager` owns the live set and
        // publishes it to the registry.
        let hooks = Arc::new(MainHooks {
            cancel: cancel.clone(),
            coord_tx: coord_tx.clone(),
            kube_client: kube_client.clone(),
            metrics: metrics.clone(),
            task_tokens: task_tokens.clone(),
            worker_pool: worker_pool.clone(),
            observation_ttl,
            discovery_tokens: DashMap::new(),
        });
        // Written by the worker-set watch, read by the status writer. Empty
        // with `[kubernetes]` off.
        let rejection_store = policy::worker_sets::RejectionStore::new();
        let mut pool_manager = pool::PoolManager::new(
            pool_registry.clone(),
            hooks,
            cancel.clone(),
            models.clone(),
            pool::ReapPolicy {
                linger: std::time::Duration::from_secs(30),
                // A retired pool must outlive observations that may still
                // reference it.
                backstop: observation_ttl.max(std::time::Duration::from_secs(600)),
            },
        );
        // TOML-declared sets are materialized now, CR-declared ones with the
        // watch. Their keys can't collide: CR keys are namespaced, TOML keys
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
        // `[kubernetes]` on, that task also watches `InferenceWorkerSet`s;
        // otherwise it only reaps.
        if cfg.kubernetes.enabled {
            // Inferred, so it also works outside a cluster.
            let client = kube::Client::try_from(kube::Config::infer().await?)?;
            let watcher = policy::worker_sets::WorkerSetWatcher::new(
                &cfg.kubernetes,
                cfg.profiles.clone(),
                pool_manager,
                rejection_store.clone(),
                prom.clone(),
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
        // Keep a sender alive in the coordinator, so the channel doesn't close
        // when the pool count reaches zero; cancellation is the only shutdown
        // signal.
        let coord_keepalive = coord_tx.clone();
        drop(coord_tx);

        // Coordinator. Dispatches each (pool, change) to that pool's state
        // stores.
        let coord_metrics = metrics.clone();
        let coord_cancel = cancel.clone();
        let coord_task_tokens = task_tokens.clone();
        let coord_local_tokenizer = local_tokenizer.clone();
        let coord_eviction_tx = eviction_tx.clone();
        let coord_models = models.clone();
        let coord_worker_pool = worker_pool.clone();
        let coord_prom = prom.clone();
        tokio::spawn(async move {
            // Lives as long as the coordinator.
            let _keepalive = coord_keepalive;
            loop {
                tokio::select! {
                    biased;
                    Some(req) = eviction_rx.recv() => {
                        // Resolve the worker's pool; a worker we no longer
                        // track (already evicted) is skipped.
                        let owner = coord_worker_pool
                            .get(&req.worker_id_num)
                            .map(|e| e.value().clone());
                        if let Some(pool) = owner {
                            // Counted here, the only path with a reason, and
                            // after the lookup so a gone worker isn't counted.
                            coord_prom.record_worker_eviction(req.reason.as_label());
                            evict_worker(
                                req.worker_id_num,
                                Some(req.reason),
                                &pool,
                                &coord_metrics,
                                &coord_task_tokens,
                                &coord_worker_pool,
                            )
                            .await;
                        }
                    }
                    change = rx.recv() => {
                        let Some((pool, change)) = change else {
                            // Unreachable while `_keepalive` is held.
                            tracing::error!(
                                "coordinator: discovery channel closed while a sender is held; \
                                 no further worker registration will happen"
                            );
                            break;
                        };
                        let pool = &pool;
                        match change {
                            WorkerChange::Added(w) => {
                                handle_added(
                                    pool,
                                    w,
                                    poll_ms,
                                    models_refresh_secs,
                                    ingest_cfg,
                                    warmup_enabled,
                                    &coord_metrics,
                                    &coord_prom,
                                    &coord_task_tokens,
                                    &coord_cancel,
                                    coord_local_tokenizer.clone(),
                                    &coord_eviction_tx,
                                    &coord_models,
                                    &coord_worker_pool,
                                );
                            }
                            WorkerChange::Removed(num) => {
                                evict_worker(
                                    num,
                                    None,
                                    pool,
                                    &coord_metrics,
                                    &coord_task_tokens,
                                    &coord_worker_pool,
                                )
                                .await;
                            }
                        }
                    }
                }
            }
        });

        // One `PolicyStore` for the process, made before anything reads it.
        // `None` when policy is off, which `/readyz` treats as nothing to wait
        // for.
        let policy_store_cell = cfg.policy.enabled.then(policy::store::PolicyStore::new);

        // Admin listener and the metrics rollup, when configured.
        if let Some(dash_addr) = cfg.admin_listen {
            rollup::spawn(
                pool_registry.clone(),
                metrics.clone(),
                prom.clone(),
                cancel.clone(),
            );
            // `/readyz` gates on the policy store being listed, so picks don't
            // bypass policy at startup.
            let dash_pools = pool_registry.clone();
            let dash_policy = policy_store_cell.clone();
            let dash_metrics = metrics.clone();
            let dash_prom = prom.clone();
            let dash_cancel = cancel.clone();
            tokio::spawn(async move {
                if let Err(err) = admin::serve(
                    dash_addr,
                    dash_pools,
                    dash_policy,
                    dash_prom,
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
            mock_mode = false,
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
                    "[policy] is enabled but [kubernetes] is not, so no pool has a \
                     namespaced identity for an InferenceWorkerSet to resolve to — every \
                     destination will report NoMatchingPool. Policy routes between worker \
                     sets that the CRs themselves define; set [kubernetes] enabled = true."
                );
            }
            let store = policy_store_cell
                .clone()
                .expect("created whenever [policy] is enabled");
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
                cfg.policy.namespace.as_deref(),
                store.clone(),
                pool_registry.clone(),
                rejection_store.clone(),
                metrics.clone(),
                provider_health.clone(),
                leader_rx.clone(),
                prom.clone(),
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
            tokenize_client.clone(),
            local_tokenizer,
            pending_observations.clone(),
            request_labels.clone(),
            body_store.clone(),
            policy_store,
            provider_health,
            body_idle_timeout,
            observation_sink,
            prom.clone(),
        ));

        // Body-buffer reaper, separate from the observation sweep since the TTLs
        // differ greatly. It guarantees release (`Abort` only shortens the
        // window) and refreshes the occupancy gauges.
        {
            let service = service.clone();
            let prom = prom.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = interval.tick() => {
                            let (reaped, bytes) = service.sweep_expired_bodies();
                            if reaped > 0 {
                                prom.record_bodies_reaped(reaped as u64);
                                tracing::debug!(
                                    reaped,
                                    bytes,
                                    "body_store sweep: reclaimed buffered bodies past TTL"
                                );
                            }
                            let (entries, in_use) = service.body_store.stats();
                            prom.set_body_buffer(entries as u64, in_use);
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

        Server::builder()
            .add_service(
                PickerServer::from_arc(service)
                    // Above tonic's 4 MiB default, so a body sent as one chunk
                    // hits our own budget (RESOURCE_EXHAUSTED) rather than an
                    // opaque transport error. Callers should still chunk.
                    .max_decoding_message_size((body_max_request_bytes + 1024 * 1024) as usize),
            )
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
    };

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

/// Handle a `WorkerChange::Added`. Registers the worker across all state
/// stores, spawns its ZMQ listener + metrics poller (under a per-worker
/// cancel token), and kicks a one-shot warmup so the worker is routable
/// before the next periodic `/v1/models` tick.
///
/// On `SlotExhausted`, logs an error and skips the worker.
#[allow(clippy::too_many_arguments)]
fn handle_added(
    pool: &Arc<pool::Pool>,
    w: WorkerConfig,
    poll_ms: u64,
    models_refresh_secs: u64,
    ingest_cfg: ingest::IngestConfig,
    warmup_enabled: bool,
    metrics: &Arc<MetricsRegistry>,
    prom: &Arc<metrics::PromMetrics>,
    task_tokens: &Arc<DashMap<(PoolId, u64), CancellationToken>>,
    parent_cancel: &CancellationToken,
    // Passed to the per-worker tasks only when this pool's tokenize mode
    // uses it.
    local_tokenizer: Option<Arc<local_tokenize::LocalTokenizerRegistry>>,
    eviction_tx: &eviction::EvictionTx,
    models: &Arc<ModelIndex>,
    worker_pool: &Arc<DashMap<u64, Arc<Pool>>>,
) {
    let block_size = pool.block_size;
    let registry = &pool.registry;
    let indexer = &pool.indexer;
    let inflight_prefixes = &pool.inflight_prefixes;
    // Warm up only for pools using the in-process tokenizer (Local/Compare).
    let warmup_tokenizer = if warmup_enabled
        && !matches!(
            pool.tokenize_mode,
            crate::router::tokenize::TokenizeMode::Render
        ) {
        local_tokenizer
    } else {
        None
    };
    tracing::info!(
        pool = pool.name,
        worker_id = w.worker_id_str,
        block_size,
        zmq_endpoint = w.zmq_endpoint.as_deref().unwrap_or("-"),
        http_url = w.http_url.as_deref().unwrap_or("-"),
        "Added worker"
    );

    // 1. Two worker sets selecting one pod. Must bail: `worker_id_num` keys
    //    fleet-wide state the first registration owns. The fix is a Service
    //    selector.
    if let Some(owner) = worker_pool.get(&w.worker_id_num)
        && owner.value().id != pool.id
    {
        tracing::warn!(
            pool = pool.name,
            worker_id = w.worker_id_str,
            owner = owner.value().name,
            "worker already belongs to another worker set; skipping this Add"
        );
        return;
    }

    // Reverse index for eviction and the observation lifecycle. Provisionally
    // route the model_name hint, before the first /v1/models scrape.
    worker_pool.insert(w.worker_id_num, pool.clone());
    if !w.model_name.is_empty() {
        models.associate(pool.id, &pool.name, std::slice::from_ref(&w.model_name));
    }

    // 2. WorkerRegistry, seeded from config.model_name; `by_model` is updated
    //    by the metrics poller and warmup.
    let entry = WorkerEntry::from_config(&w, block_size);
    // The key the pick path uses for inflight state, which the ZMQ listener
    // also keys `on_stored` by.
    let endpoint_key = entry.score_addr.clone();
    if !registry.register(w.worker_id_num, entry) {
        tracing::warn!(
            worker_id = w.worker_id_str,
            "worker id collides with one already in the registry; skipping this Add"
        );
        worker_pool.remove(&w.worker_id_num);
        return;
    }

    // 3. Per-worker cancellation token. Stored so Removed can target this
    //    worker without taking down siblings.
    let worker_token = parent_cancel.child_token();
    task_tokens.insert((pool.id, w.worker_id_num), worker_token.clone());

    // 3b. A declared model list is authoritative: seed it and skip
    //     `/v1/models`, for engines that don't serve it.
    if let Some(declared) = pool.static_models.as_deref() {
        if declared.is_empty() {
            tracing::warn!(
                workerset = pool.name,
                worker_id = w.worker_id_str,
                "modelDiscovery.mode is Static but spec.models is empty; this worker \
                 serves nothing and no scrape will discover otherwise"
            );
        } else {
            // `set_served_models` refuses on a mismatch, so only claim the
            // models when it accepted them.
            match registry.set_served_models(w.worker_id_num, declared) {
                // Every worker gets the same declared list, so these mean the
                // pin outlived a `spec.models` edit (`reset_declared_models`
                // didn't run). Still routable.
                crate::fleet::workers::ServedModelsVerdict::Widened { .. }
                | crate::fleet::workers::ServedModelsVerdict::Narrowed { .. } => {
                    tracing::warn!(
                        workerset = pool.name,
                        worker_id = w.worker_id_str,
                        "declared model list differs from this set's pinned models; \
                         routing anyway on the declared list"
                    );
                    models.associate(pool.id, &pool.name, declared);
                }
                crate::fleet::workers::ServedModelsVerdict::Accepted
                | crate::fleet::workers::ServedModelsVerdict::Empty => {
                    models.associate(pool.id, &pool.name, declared);
                }
                crate::fleet::workers::ServedModelsVerdict::Mismatch { expected, observed } => {
                    // As above, but not routable: skip the worker rather than
                    // half-register it.
                    tracing::error!(
                        workerset = pool.name,
                        worker_id = w.worker_id_str,
                        ?expected,
                        ?observed,
                        "declared model list disagrees with this set's pinned models; \
                         not routing to this worker"
                    );
                }
            }
        }
        // Readiness normally waits on a `/v1/models` scrape. With a declared
        // list there is nothing to wait for.
        registry.set_initial_sync_complete(w.worker_id_num);
    }

    // 4. Spawn the per-worker tasks. The metrics poller and warmup update
    //    model routing after a /v1/models scrape.
    ingest::spawn_worker_listener(
        w.clone(),
        block_size,
        ingest_cfg,
        endpoint_key,
        indexer.clone(),
        prom.clone(),
        inflight_prefixes.clone(),
        registry.clone(),
        eviction_tx.clone(),
        worker_token.clone(),
    );
    // With a declared model list, push the models refresh out of the way; the
    // poller still scrapes load metrics.
    let refresh_secs = if pool.static_models.is_some() {
        u64::MAX / 2000
    } else {
        pool.models_refresh_secs.unwrap_or(models_refresh_secs)
    };
    spawn_metrics_poller(
        w.clone(),
        poll_ms,
        refresh_secs,
        warmup_tokenizer.clone(),
        metrics.clone(),
        registry.clone(),
        eviction_tx.clone(),
        worker_token,
        models.clone(),
        pool.id,
        pool.name.clone(),
    );

    // 5. Warmup: an immediate /v1/models scrape, so the worker is routable
    //    without waiting for the periodic tick, plus tokenizer warmup in
    //    local mode.
    spawn_one_shot_warmup(
        w,
        metrics.clone(),
        registry.clone(),
        warmup_tokenizer,
        eviction_tx.clone(),
        models.clone(),
        pool.id,
        pool.name.clone(),
    );
}

/// The side effects `PoolManager` delegates: starting a pool's discovery, and
/// dropping its workers' fleet-wide state when it retires.
///
/// Here rather than in `pool`, which can then be tested against a
/// stub.
struct MainHooks {
    /// Process token. Pool and discovery tokens are its children.
    cancel: CancellationToken,
    coord_tx: tokio::sync::mpsc::Sender<(Arc<Pool>, WorkerChange)>,
    kube_client: Option<kube::Client>,
    metrics: Arc<MetricsRegistry>,
    task_tokens: Arc<DashMap<(PoolId, u64), CancellationToken>>,
    worker_pool: Arc<DashMap<u64, Arc<Pool>>>,
    observation_ttl: std::time::Duration,
    /// Per-pool token scoping that pool's discovery, forwarder, and prefix
    /// sweep.
    ///
    /// Not `pool.cancel`, which survives retirement until the reaper fires
    /// (the `KvIndexer` panics if used after cancellation). Discovery must
    /// stop the moment the pool retires.
    discovery_tokens: DashMap<PoolId, CancellationToken>,
}

impl pool::PoolHooks for MainHooks {
    fn on_create(&self, pool: &Arc<Pool>, spec: &pool::PoolSpec) -> anyhow::Result<()> {
        let discovery: Arc<dyn Discovery> = match spec.discovery.mode {
            DiscoveryMode::Static => {
                let workers = spec.load_static_workers()?;
                Arc::new(StaticDiscovery::new(workers))
            }
            DiscoveryMode::Kubernetes => {
                let client = self.kube_client.clone().ok_or_else(|| {
                    anyhow::anyhow!(
                        "worker set needs Kubernetes discovery but no in-cluster \
                         client was built at startup"
                    )
                })?;
                Arc::new(KubeDiscovery::from_client(spec.discovery.clone(), client))
            }
        };

        let token = self.cancel.child_token();
        self.discovery_tokens.insert(pool.id, token.clone());

        let (pool_tx, mut pool_rx) = tokio::sync::mpsc::channel::<WorkerChange>(64);
        {
            let token = token.clone();
            tokio::spawn(async move { discovery.run(pool_tx, token).await });
        }
        // Forwarder: tag this pool's WorkerChange events with the pool.
        {
            let coord_tx = self.coord_tx.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                while let Some(wc) = pool_rx.recv().await {
                    if coord_tx.send((pool.clone(), wc)).await.is_err() {
                        break;
                    }
                }
            });
        }
        // Per-pool inflight-prefix TTL sweep, a backstop against lifecycle
        // leaks.
        {
            let inflight = pool.inflight_prefixes.clone();
            let ttl = self.observation_ttl;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
                loop {
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = interval.tick() => inflight.sweep(ttl),
                    }
                }
            });
        }
        Ok(())
    }

    fn on_retire(&self, pool: &Arc<Pool>) {
        // Stop discovery FIRST, or it re-announces the workers we are about to
        // drop and the coordinator resurrects them into a retired pool.
        if let Some((_, token)) = self.discovery_tokens.remove(&pool.id) {
            token.cancel();
        }
        // Drop every worker's fleet-wide state. Synchronous and indexer-free:
        // the whole radix tree goes at reap, and touching it here would race
        // the cancellation.
        let workers: Vec<u64> = pool
            .registry
            .iter_snapshot()
            .into_iter()
            .map(|(num, _)| num)
            .collect();
        let count = workers.len();
        for num in workers {
            drop_worker_state(
                num,
                None,
                pool,
                &self.metrics,
                &self.task_tokens,
                &self.worker_pool,
            );
        }
        tracing::info!(
            workerset = %pool.key,
            workers = count,
            "released this worker set's workers"
        );
    }
}

/// Teardown for both discovery removals and evictions: cancel the worker's
/// tasks, drop its state, and drop its radix-tree subtree.
///
/// `reason` is `None` for discovery removals and `Some` for evictions, which
/// are logged with the reason.
async fn evict_worker(
    worker_id_num: u64,
    reason: Option<eviction::EvictionReason>,
    pool: &Arc<pool::Pool>,
    metrics: &Arc<MetricsRegistry>,
    task_tokens: &Arc<DashMap<(PoolId, u64), CancellationToken>>,
    worker_pool: &Arc<DashMap<u64, Arc<Pool>>>,
) {
    drop_worker_state(
        worker_id_num,
        reason,
        pool,
        metrics,
        task_tokens,
        worker_pool,
    );

    pool.indexer.remove_worker(worker_id_num).await;
}

/// Everything `evict_worker` does except the indexer call, for retiring pools:
/// `KvIndexer::remove_worker` panics if it races cancellation, and the whole
/// tree is dropped anyway.
#[allow(clippy::too_many_arguments)]
fn drop_worker_state(
    worker_id_num: u64,
    reason: Option<eviction::EvictionReason>,
    pool: &Arc<pool::Pool>,
    metrics: &Arc<MetricsRegistry>,
    task_tokens: &Arc<DashMap<(PoolId, u64), CancellationToken>>,
    worker_pool: &Arc<DashMap<u64, Arc<Pool>>>,
) {
    // For the log line, before the registry entry is dropped.
    let worker_id = pool
        .registry
        .lookup(worker_id_num)
        .map(|e| e.worker_id_str)
        .unwrap_or_else(|| format!("<unregistered:{worker_id_num}>"));
    match &reason {
        Some(r) => {
            tracing::warn!(pool = pool.name, worker_id, reason = %r, "Evicting worker")
        }
        None => tracing::info!(pool = pool.name, worker_id, "Removed worker"),
    }

    // Stop the per-worker tasks, for this pool only.
    if let Some((_, token)) = task_tokens.remove(&(pool.id, worker_id_num)) {
        token.cancel();
    }

    // Per-pool state: unconditionally ours to drop.
    pool.registry.unregister(worker_id_num);

    // Fleet-wide state is keyed by `worker_id_num` alone, so only drop it while
    // this pool is the owner; blanked metrics would make the worker look idle.
    //
    // The model index is left alone: other workers may still serve the model.
    let still_ours = worker_pool
        .get(&worker_id_num)
        .is_none_or(|e| e.value().id == pool.id);
    if still_ours {
        metrics.forget(worker_id_num);
        worker_pool.remove(&worker_id_num);
    } else {
        tracing::info!(
            pool = pool.name,
            worker_id,
            "worker now belongs to another worker set; leaving fleet-wide state alone"
        );
    }
}

/// The pod's own namespace, from the service-account mount. Used when
/// `[leader_election].namespace` is unset so a normal in-cluster deployment
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
