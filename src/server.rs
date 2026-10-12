//! The `kinference` server: the gRPC picker, the admin listener, discovery and
//! the policy machinery, wired together and run until shutdown.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use clap::Parser;
use dashmap::DashMap;
use dynamo_kv_router::indexer::KvIndexerInterface;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::config::{Config, DEFAULT_DASHBOARD_MAX_WORKERS, DiscoveryMode};
use crate::discovery::{
    Discovery, WorkerChange, kubernetes::KubeDiscovery, static_config::StaticDiscovery,
};
use crate::metrics::{MetricsRegistry, spawn_metrics_poller, spawn_one_shot_warmup};
use crate::mock::MockKinferenceService;
use crate::monitor::KinferenceService;
use crate::pool::{Pool, PoolId};
use crate::proto::picker_server::PickerServer;
use crate::telemetry::{Telemetry, WorkerMeta};
use crate::workers::{WorkerEntry, WorkerRegistry};
use crate::{
    body_store, config, dashboard, eviction, ingest, leader, local_tokenize, observation_sink,
    observations, policy, pool, pool_manager, prom, telemetry, token_review, version,
};

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
///
/// The whole of what `main` used to be. It lives in the library so that a
/// second binary, `kinfctl`, can share every module with the server without
/// widening a single item's visibility: both binaries are thin shims over
/// this crate.
pub async fn run() -> Result<()> {
    // ANSI color codes only when stdout is a TTY and the user hasn't opted out
    // via NO_COLOR (https://no-color.org). Piping or redirecting therefore
    // produces clean ASCII without escape sequences.
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

    let listen_addr: std::net::SocketAddr = cfg.listen.parse()?;
    tracing::info!(
        version = version::VERSION,
        listen = %listen_addr,
        worker_sets = cfg.worker_sets.len(),
        profiles = cfg.profiles.len(),
        mock_mode = cfg.mock_mode,
        "Starting kinference"
    );

    let cancel = CancellationToken::new();
    let dashboard_max_workers = cfg
        .dashboard_max_workers
        .unwrap_or(DEFAULT_DASHBOARD_MAX_WORKERS);

    // Pre-bind so we can log READY only after the socket is genuinely
    // listening — i.e., a successful nc/grpcurl/score connection would now
    // be accepted. `TcpIncoming::new` binds synchronously and returns an
    // io::Error on failure, which we surface before logging anything else.
    let incoming = tonic::transport::server::TcpIncoming::new(listen_addr, true, None)
        .map_err(|e| anyhow::anyhow!("failed to bind gRPC listener on {listen_addr}: {e}"))?;

    let shutdown = async {
        wait_for_shutdown_signal().await;
    };

    // The elector is the one spawned task with work that MUST complete after
    // cancellation: vacating the Lease is what lets a peer take over
    // immediately instead of waiting out the full lease duration. Everything
    // else can be dropped mid-flight. Held here so shutdown can await it —
    // otherwise `main` returns, the runtime drops, and the vacate is aborted
    // in flight, which makes the whole feature silently inert.
    let mut leader_task: Option<tokio::task::JoinHandle<()>> = None;

    // The two service implementations have different concrete types, so the
    // `Router` returned by `add_service` doesn't unify. Serve inside each
    // arm and let `?` short-circuit out at the end.
    let server_result = if cfg.mock_mode {
        // Mock mode flattens every pool's static workers into one
        // registry and answers deterministically. Per-pool tuning is
        // not modeled — mock only needs model→endpoint resolution.
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
            // The dashboard exists to diagnose real-world traffic; mock data
            // there is misleading. Surface the no-op clearly.
            tracing::warn!(
                "admin_listen is set but ignored in mock mode (the dashboard is for diagnosing real-world traffic)"
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
        // Prometheus exposition registry — shared by the gRPC
        // `report_final_token` recorder and the dashboard's `/metrics` route.
        let prom = Arc::new(prom::PromMetrics::with_config(&cfg.metrics));
        let poll_ms = cfg.metrics_poll_ms.unwrap_or(1000);
        let models_refresh_secs = cfg.models_refresh_secs.unwrap_or(30);
        // Wall-clock budget for a single replay_gap call against the
        // DEALER socket. Initial-drain replays may need to apply
        // 10k+ events from history; at ~150-250 events/sec apply
        // rate that's 40-60s of wall time, so we default high.
        // Steady-state gap recovery completes far faster.
        let replay_timeout_ms = cfg.replay_timeout_ms.unwrap_or(60_000);

        // Build one Pool per `[[workerset]]`. Each owns its own KvIndexer
        // (sized to the pool's block_size), WorkerRegistry,
        // InflightPrefixes, InflightRegistry, and pick lock.
        // Paired with its config rather than looked up by id: a PoolId is no
        // longer an index, and indexing `cfg.pools` by one was only ever
        // correct because the two vectors happened to be built together.
        // Declarations resolved against their profiles once, here. Everything
        // downstream sees `PoolSpec`, never a profile reference.
        //
        // The pools themselves are built by `PoolManager` further down, once
        // the things a pool needs to be wired to exist (the coordinator
        // channel, telemetry, the eviction channel). Publishing a pool before
        // those exist would make it visible to picks while nothing fed it.
        let specs = cfg.pool_specs()?;
        // The published set every consumer reads. Swapping it is a pointer
        // write rather than a structural change to each consumer.
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

        // Telemetry aggregates its index gauge across every pool's tree; it
        // reads the registry each rollup tick rather than holding the indexers.
        let telemetry = Telemetry::new(dashboard_max_workers, metrics.clone());

        // Reactive model→pool routing + worker→pool reverse index.
        let model_router: Arc<RwLock<HashMap<String, BTreeSet<PoolId>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let worker_pool: Arc<DashMap<u64, Arc<Pool>>> = Arc::new(DashMap::new());

        // TTL for a pick's in-flight bookkeeping (pending observation +
        // inflight counter + inflight-prefix entry). Must exceed worst-case
        // *full* response time — see `Config::observation_ttl_secs`.
        let observation_ttl =
            std::time::Duration::from_secs(cfg.observation_ttl_secs.unwrap_or(600));

        // Shared pending-observation stash. Inserted at pick time by
        // `pick_and_record` (gRPC `Pick()`),
        // consumed at observe time. Its TTL sweep — which also releases the
        // inflight/prefix state each evicted pick reserved — is spawned once
        // the `KinferenceService` exists (the cleanup needs the service); see
        // below.
        let pending_observations = observations::shared(observation_ttl);
        // Per-request metric labels (model + qos), same TTL/lifecycle as the
        // pending-observation stash; taken at ReportFinalToken, TTL-swept.
        let request_labels = observations::shared_request_labels(observation_ttl);

        // Buffered request bodies for PushBody → Pick → PullBody. Its TTL is
        // deliberately much shorter than `observation_ttl`: bodies are large and
        // every step that consumes one follows immediately, so a body still here
        // after a minute belongs to a caller that vanished.
        let body_ttl = std::time::Duration::from_secs(cfg.body_ttl_secs.unwrap_or(60));
        let body_idle_timeout =
            std::time::Duration::from_secs(cfg.body_idle_timeout_secs.unwrap_or(30));
        // Config is in KB for legibility; everything downstream — the store, the
        // gRPC frame limit, `kinference_body_buffer_bytes` — is in bytes. Convert
        // once, here, so only this line knows about the unit.
        //
        // `saturating_mul` so an absurd config value clamps instead of wrapping
        // to a small cap, which would silently make the buffer tiny.
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

        // Shared HTTP client for tokenize fanout. Built once here so
        // connections pool across score requests; per-call
        // `Client::new()` would forfeit pooling, timeout, and every
        // tuning knob below.
        //
        // The pool/keepalive settings mirror what the upstream
        // Gateway-API-Inference-Extension EPP uses for its vLLM
        // tokenize calls. vLLM speaks HTTP/1.1 only — no h2c — so
        // we force H/1.1 here and skip any ALPN negotiation. Under
        // bench concurrency, HTTP/1.1 needs multiple concurrent TCP
        // connections per host since H/1.1 can't multiplex. The
        // pool idle cap is operator-tunable via
        // `tokenize.pool_max_idle_per_host` — default 16 fits the
        // per-worker rotation path; bump to ≥ peak concurrency when
        // pointing `tokenize.url` at a single dedicated render
        // endpoint, so warmed connections survive between bursts.
        // `pool_idle_timeout = 90s` matches the EPP default and
        // AWS NLB's idle-tolerance. `tcp_keepalive` lets the OS
        // detect half-closed connections without us discovering them
        // via a failed request.
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

        // Build the LocalTokenizerRegistry unconditionally. It's
        // ~free to construct (empty DashMaps + a reqwest client),
        // and keeping it around in every mode means an operator can
        // flip Render → Compare → Local at runtime without a
        // process restart — the cost of the first request after a
        // flip is one Hub fetch per model, then cache-warm for the
        // session. The registry must exist BEFORE the coordinator
        // spawns so handle_added can thread it into the per-worker
        // metrics poller / warmup tasks.
        // The local tokenizer registry is fleet-wide (keyed by model,
        // one on-disk cache). The tokenization MODE is per-pool now, so
        // the registry is always built; each pool decides whether it
        // consults it. Warmup is gated per-pool inside `handle_added`
        // (only non-Render pools pre-load tokenizers).
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

        // Coordinator channel carries (PoolId, WorkerChange). Each pool
        // runs its own Discovery; a per-pool forwarder tags that pool's
        // events with its id onto this shared channel.
        // Carries the pool itself rather than its id: the forwarder already
        // holds an `Arc<Pool>`, so passing it removes a lookup that could
        // otherwise resolve to nothing once the pool set is mutable.
        let (coord_tx, mut rx) = tokio::sync::mpsc::channel::<(Arc<Pool>, WorkerChange)>(64);
        // Per-worker cancellation tokens so Removed can stop a single
        // worker's listeners without disturbing the rest of the fleet.
        //
        // Keyed by (pool, worker) rather than worker alone. A worker moving
        // between worker sets otherwise has its new pool's token overwrite the
        // old one, and the old pool's Removed then cancels the NEW pool's
        // listeners — a healthy worker in the correct pool goes dark, and the
        // logs blame the pool that left.
        let task_tokens: Arc<DashMap<(pool::PoolId, u64), CancellationToken>> =
            Arc::new(DashMap::new());

        // Eviction signal — subsystems (ingest, metrics) request a
        // worker's removal here when they detect a homogeneity
        // violation. The coordinator below drains the receiver and
        // runs the same teardown as a discovery-initiated Removed.
        let (eviction_tx, mut eviction_rx) = eviction::channel();

        // In-cluster credentials, acquired ONCE if any worker set needs them.
        //
        // `KubeDiscovery::new` used to do this per pool, which made pool
        // creation async and fallible. That is tolerable at startup — failing
        // hard on unreachable credentials is right — and intolerable in a
        // reconcile loop, where a transient apiserver hiccup would turn "a
        // worker set appeared" into a silent failure with nothing to retry it.
        // Fail here, where fatal is the correct response.
        // Needed the moment any pool watches EndpointSlices. A TOML worker set
        // in kubernetes mode says so at startup; a CR-derived one cannot be
        // known until the watch lists, so try whenever `[kubernetes]` is on and
        // let a failure surface per-pool rather than killing the process. That
        // ordering is what lets the CR resolution path be exercised from
        // outside a cluster, where pod IPs are unreachable but the CRs are not.
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

        // Everything a pool has to be wired to now exists, so pools can be
        // built. `PoolManager` owns the live set; the registry above is what
        // it publishes for everyone else to read.
        let hooks = Arc::new(MainHooks {
            cancel: cancel.clone(),
            coord_tx: coord_tx.clone(),
            kube_client: kube_client.clone(),
            telemetry: telemetry.clone(),
            metrics: metrics.clone(),
            task_tokens: task_tokens.clone(),
            worker_pool: worker_pool.clone(),
            observation_ttl,
            discovery_tokens: DashMap::new(),
        });
        // Published by the worker-set watch, read by the status writer. Built
        // unconditionally so the status writer never has to care whether the
        // watch is running; with `[kubernetes]` off it simply stays empty.
        let rejection_store = policy::worker_sets::RejectionStore::new();
        let mut pool_manager = pool_manager::PoolManager::new(
            pool_registry.clone(),
            hooks,
            cancel.clone(),
            model_router.clone(),
            pool_manager::ReapPolicy {
                linger: std::time::Duration::from_secs(30),
                // A retired pool must outlive the observations that might still
                // resolve against it, or the backstop fires while a request is
                // genuinely mid-flight.
                backstop: observation_ttl.max(std::time::Duration::from_secs(600)),
            },
        );
        // TOML-declared sets are materialized immediately; CR-declared ones
        // arrive with the watch. The two coexist: a static set and an
        // `InferenceWorkerSet` are both just a `PoolSpec`, and their keys cannot
        // collide because a CR key is always namespaced and a TOML key never is.
        let report = pool_manager.reconcile(specs.clone());
        if !report.failed.is_empty() {
            // At STARTUP a worker set that cannot be wired is fatal: the
            // operator asked for it explicitly and would otherwise get a
            // silently smaller fleet. A reconcile at runtime logs and carries
            // on, because the rest of the fleet is already serving.
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

        // The manager moves into exactly one task — its single owner, so the
        // live map needs no lock. With `[kubernetes]` on that task also watches
        // `InferenceWorkerSet`s and reconciles; without it, it only reaps.
        if cfg.kubernetes.enabled {
            // Its own client, inferred rather than in-cluster-only: reading CRs
            // from outside the cluster is harmless, and it is only the pod IPs
            // that EndpointSlice discovery derives that would be unreachable.
            // Same reasoning as `policy::status::spawn`.
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
        // KEEP a sender alive alongside the coordinator, rather than dropping
        // ours here.
        //
        // Dropping it was right while the pool set was fixed: the only way for
        // every forwarder to exit was process cancellation, so a closed channel
        // meant shutdown. Once pools come and go it means something else
        // entirely — the pool count reached zero — and the coordinator would
        // exit permanently on `rx.recv() == None`. Scale to zero worker sets
        // and back up, and no worker would ever be registered again for the
        // life of the process, with one debug line as the only trace.
        //
        // Holding a sender the coordinator itself owns makes the channel
        // outlive any number of forwarders, so cancellation stays the single
        // shutdown signal.
        let coord_keepalive = coord_tx.clone();
        drop(coord_tx);

        // Coordinator. Dispatches each (pool, change) to that pool's
        // state stores.
        //
        // It no longer holds a snapshot of the pool set. Both the discovery
        // channel and `worker_pool` carry `Arc<Pool>` directly, so there is
        // nothing left to look up — which also means there is no frozen vec
        // here to go stale, and no id that could index past its end.
        let coord_telemetry = telemetry.clone();
        let coord_metrics = metrics.clone();
        let coord_cancel = cancel.clone();
        let coord_task_tokens = task_tokens.clone();
        let coord_local_tokenizer = local_tokenizer.clone();
        let coord_eviction_tx = eviction_tx.clone();
        let coord_model_router = model_router.clone();
        let coord_worker_pool = worker_pool.clone();
        let coord_prom = prom.clone();
        tokio::spawn(async move {
            // Moved in so it lives exactly as long as the coordinator: the
            // channel cannot close underneath it, whatever happens to the
            // forwarders. Cancellation is the only way out of the loop below.
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
                            // Counted here rather than inside the teardown: this
                            // is the only path that carries a reason, and doing
                            // it after the pool lookup means a request for a
                            // worker already gone is not counted twice.
                            coord_prom.record_worker_eviction(req.reason.as_label());
                            evict_worker(
                                req.worker_id_num,
                                Some(req.reason),
                                &pool,
                                &coord_telemetry,
                                &coord_metrics,
                                &coord_task_tokens,
                                &coord_worker_pool,
                            )
                            .await;
                        }
                    }
                    change = rx.recv() => {
                        let Some((pool, change)) = change else {
                            // Unreachable while `_keepalive` is held: the
                            // coordinator owns a sender, so the channel cannot
                            // close just because every forwarder exited. Left
                            // as a loud error rather than deleted, because
                            // reaching it would mean that invariant broke.
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
                                    replay_timeout_ms,
                                    dashboard_max_workers,
                                    warmup_enabled,
                                    &coord_telemetry,
                                    &coord_metrics,
                                    &coord_task_tokens,
                                    &coord_cancel,
                                    coord_local_tokenizer.clone(),
                                    &coord_eviction_tx,
                                    &coord_model_router,
                                    &coord_worker_pool,
                                );
                            }
                            WorkerChange::Removed(num) => {
                                evict_worker(
                                    num,
                                    None,
                                    pool,
                                    &coord_telemetry,
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

        // Live dashboard + telemetry rollup — only when configured.
        // One `PolicyStore` for the whole process, made before anything that
        // reads it. `None` when policy is off, which `/readyz` treats as "nothing
        // to wait for".
        let policy_store_cell = cfg.policy.enabled.then(policy::store::PolicyStore::new);

        if let Some(addr_str) = &cfg.admin_listen {
            let dash_addr: std::net::SocketAddr = addr_str
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid admin_listen `{addr_str}`: {e}"))?;
            telemetry
                .clone()
                .spawn_rollup(pool_registry.clone(), prom.clone(), cancel.clone());
            // Created here rather than with the watcher below, because `/readyz`
            // gates on it: the store starts un-listed, and that is precisely the
            // window where serving a Pick would silently bypass policy.
            let tel = telemetry.clone();
            let dash_pools = pool_registry.clone();
            let dash_policy = policy_store_cell.clone();
            let dash_metrics = metrics.clone();
            let dash_prom = prom.clone();
            let dash_cancel = cancel.clone();
            tokio::spawn(async move {
                if let Err(err) = dashboard::serve(
                    dash_addr,
                    tel,
                    dash_pools,
                    dash_policy,
                    dash_prom,
                    dash_metrics,
                    dash_cancel,
                )
                .await
                {
                    tracing::error!(error = %err, "dashboard server exited with error");
                }
            });
            tracing::info!(listen = %dash_addr, "Live dashboard enabled");
        }

        tracing::info!(
            listen = %listen_addr,
            worker_sets = live_pools.len(),
            mock_mode = false,
            "READY: gRPC accepting connections"
        );

        // The observation CSV sink is shared across pools + both
        // front-ends; rows carry their pool + config snapshot so offline
        // analysis can split by pool. Per-pool inflight counters and
        // pick locks live inside each `Pool`.
        let observation_sink = Arc::new(observation_sink::ObservationSink::from_env());

        // Optional CRD-driven routing policy. Built before the service so the
        // watcher can be handed the same snapshot cell the pick path reads.
        // `None` when disabled, which makes a pool-naming Pick fall back to
        // model→pool routing rather than fail.
        let policy_bits = if cfg.policy.enabled {
            // A destination resolves by IDENTITY now: an `InferenceWorkerSet`
            // IS a pool, so the lookup is (namespace, name) against the live
            // pool set. There is nothing to bind at startup.
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
            // ONE store, shared deliberately: the service writes into it from the
            // Abort / ReportFirstToken handlers, and the status writer publishes
            // what it holds. Two instances would leave provider status permanently
            // reporting a provider nobody had reported anything about.
            let provider_health = Arc::new(policy::provider_health::ProviderHealthStore::new());
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
            // Only ONE replica may write status. Everything else — discovery,
            // these watches, KV ingest, picking — runs on every replica, so a
            // follower is a fully functional router that just doesn't publish.
            //
            // With election disabled the channel is pre-set to `true` and no
            // elector is spawned, so a single-replica deployment behaves exactly
            // as it did before this existed.
            let leader_rx = if cfg.leader_election.enabled {
                let (tx, rx) = tokio::sync::watch::channel(false);
                let ns = cfg
                    .leader_election
                    .namespace
                    .clone()
                    .or_else(kube_namespace_hint)
                    .unwrap_or_else(|| "default".to_string());
                let identity = leader_identity();
                let params = leader::LeaseParams::from(&cfg.leader_election);
                // Reuse the client construction policy the controller side
                // already uses (`Config::infer`), so the Lease is reachable in
                // exactly the deployments where status writing is.
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
                // Always-leader. `watch::channel` keeps the sender alive by
                // leaking it deliberately: dropping it would close the channel,
                // and the writer treats a closed channel as "hold current
                // state" plus an error — noise for a case that is not an error.
                let (tx, rx) = tokio::sync::watch::channel(true);
                std::mem::forget(tx);
                rx
            };

            // kinference is the router-controller for these kinds, so it publishes
            // their status. This is not just observability: a conforming
            // gateway will not program a route until the pool reports
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

            // The other half of being the router-controller: a gateway has no
            // schema for `InferencePool` and cannot resolve a route that names
            // one. kinference publishes what each pool resolves to as a
            // `BackendBinding`, in kinds the gateway does understand.
            //
            // Leader-gated with the same receiver as status, for the same
            // reason: these are objects, and two writers that disagree on one
            // field rewrite each other forever.
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

        // Provider health is NOT behind the policy toggle: the feedback RPCs that
        // write it do not know whether policy is enabled, and an always-present
        // empty store is cheaper than threading that question into them.
        let (policy_store, provider_health) = match policy_bits {
            Some((store, health)) => (Some(store), health),
            None => (
                None,
                Arc::new(policy::provider_health::ProviderHealthStore::new()),
            ),
        };
        let service = Arc::new(KinferenceService::new(
            pool_registry.clone(),
            model_router.clone(),
            metrics.clone(),
            telemetry.clone(),
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

        // Body-buffer reaper. Separate task from the observation sweep because
        // the TTLs differ by an order of magnitude. This is the mechanism that
        // guarantees release — `Abort` only shortens the window — so it also
        // refreshes the occupancy gauges while it's here.
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

        // Pending-observation TTL sweep. Runs on the service so eviction can
        // release each expired pick's inflight + inflight-prefix state — a
        // caller that never reports, or a long non-streaming request that
        // outran `observation_ttl`, would otherwise leak the worker's
        // inflight count (the counter has no sweep of its own). Also caps the
        // stash so it can't grow unbounded.
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
                            // Same tick, same reasoning: a deleted
                            // InferenceProvider would otherwise pin its health
                            // entry for the life of the process.
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

        // One more service on the SAME listener. It owns its own toggle and
        // its own metrics, so there is nothing to decide here. Token review
        // acquires a kube client and a failure is fatal — see its `start`.
        let token_review =
            token_review::start(&cfg.token_review, prom.registry(), listen_addr).await?;

        Server::builder()
            .add_optional_service(token_review)
            .add_service(
                PickerServer::from_arc(service)
                    // Raise tonic's 4 MiB default so a caller that sends its
                    // whole body as ONE `BodyChunk` isn't rejected by the
                    // transport before our own per-request budget gets a say —
                    // that would surface as an opaque error instead of a clean
                    // RESOURCE_EXHAUSTED. Headroom over the body cap covers
                    // framing. (Callers SHOULD still chunk at ≤1 MiB; see the
                    // proto's Conformance section.)
                    .max_decoding_message_size((body_max_request_bytes + 1024 * 1024) as usize),
            )
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
    };

    cancel.cancel();

    // Give the elector a bounded moment to vacate the Lease. Bounded because a
    // slow or unreachable apiserver must not hold the pod past its termination
    // grace period — a handover that waits out the lease expiry is the correct
    // fallback, and it is what happens on SIGKILL anyway.
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
/// On `SlotExhausted`, logs ERROR and skips — no panic, the fleet keeps
/// serving its existing workers.
#[allow(clippy::too_many_arguments)]
fn handle_added(
    pool: &Arc<pool::Pool>,
    w: config::WorkerConfig,
    poll_ms: u64,
    models_refresh_secs: u64,
    replay_timeout_ms: u64,
    dashboard_max_workers: usize,
    warmup_enabled: bool,
    telemetry: &Arc<Telemetry>,
    metrics: &Arc<MetricsRegistry>,
    task_tokens: &Arc<DashMap<(pool::PoolId, u64), CancellationToken>>,
    parent_cancel: &CancellationToken,
    // The fleet-wide local tokenizer registry (keyed by model). Passed
    // through to the per-worker warmup/poller only when this pool's
    // tokenize mode actually consults it — see `warmup_for_pool` below.
    local_tokenizer: Option<Arc<local_tokenize::LocalTokenizerRegistry>>,
    eviction_tx: &eviction::EvictionTx,
    model_router: &Arc<
        std::sync::RwLock<
            std::collections::HashMap<String, std::collections::BTreeSet<pool::PoolId>>,
        >,
    >,
    worker_pool: &Arc<DashMap<u64, Arc<Pool>>>,
) {
    let block_size = pool.block_size;
    let registry = &pool.registry;
    let indexer = &pool.indexer;
    let inflight_prefixes = &pool.inflight_prefixes;
    // Warmup fires only when enabled AND this pool actually uses the
    // in-process tokenizer (Local/Compare). Render-mode pools skip it —
    // they don't consult the local registry, so there's nothing to warm.
    let warmup_tokenizer =
        if warmup_enabled && !matches!(pool.tokenize_mode, config::TokenizeMode::Render) {
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

    // 1. Telemetry slot. The two refusals are NOT equivalent and are handled
    //    differently on purpose.
    if let Err(err) = telemetry.register_worker(WorkerMeta::from_config(&w, block_size, &pool.name))
    {
        match err {
            // Capacity only. Telemetry is pure observability — nothing on the
            // routing path reads it — so a worker without a slot routes
            // perfectly well, it is just invisible on the dashboard and absent
            // from `kinference_workers`. Refusing to add it would trade an
            // unobservable worker for an idle one, and idle is more expensive:
            // the GPU is paid for either way. Every telemetry write looks its
            // worker up first, so the missing slot is inert rather than unsafe.
            telemetry::RegisterError::SlotExhausted { .. } => {
                // DEBUG, not ERROR: with a small `dashboard_max_workers` this
                // is the normal state of any fleet larger than the dashboard,
                // and one line per worker at ERROR would be alarming and wrong.
                // `kinference_dashboard_slots_denied_total` carries the signal.
                tracing::debug!(
                    pool = pool.name,
                    worker_id = w.worker_id_str,
                    capacity = dashboard_max_workers,
                    "dashboard slot pool full; this worker routes normally and is \
                     counted by kinference_workers, but the live dashboard will not \
                     show it. Raise dashboard_max_workers to display more"
                );
                // Fall through: register everywhere else.
            }
            // Two worker sets selecting one pod. This one MUST still bail:
            // `worker_id_num` keys the fleet-wide telemetry slots, the
            // `MetricsRegistry` and the eviction channel alike, so continuing
            // would corrupt state the first registration owns. The fix is a
            // Service selector, not a kinference setting.
            telemetry::RegisterError::AlreadyRegistered { .. } => {
                tracing::warn!(
                    pool = pool.name,
                    worker_id = w.worker_id_str,
                    error = %err,
                    "worker already belongs to another worker set; skipping this Add"
                );
                return;
            }
        }
    }

    // Reverse index for eviction + observation lifecycle. Provisionally
    // route this worker's model_name hint to this pool so requests can
    // land before the first /v1/models scrape confirms it.
    worker_pool.insert(w.worker_id_num, pool.clone());
    if !w.model_name.is_empty() {
        crate::monitor::associate_models(
            model_router,
            pool.id,
            &pool.name,
            std::slice::from_ref(&w.model_name),
        );
    }

    // 2. WorkerRegistry — bootstrap-hint from config.model_name; reactive
    //    by_model is updated by the metrics poller / warmup.
    let entry = WorkerEntry::from_config(&w, block_size);
    // The score_addr we just resolved here is the same key the
    // pick path uses for InflightRegistry / InflightPrefixes — grab
    // it before `entry` is moved into the registry so we can pass
    // it to the ZMQ listener (which will key `on_stored` by it).
    let endpoint_key = entry.score_addr.clone();
    if !registry.register(w.worker_id_num, entry) {
        tracing::warn!(
            worker_id = w.worker_id_str,
            "worker id collides with one already in the registry; \
             rolling back the telemetry slot"
        );
        telemetry.unregister_worker(w.worker_id_num);
        worker_pool.remove(&w.worker_id_num);
        return;
    }

    // 3. Per-worker cancellation token. Stored so Removed can target this
    //    worker without taking down siblings.
    let worker_token = parent_cancel.child_token();
    task_tokens.insert((pool.id, w.worker_id_num), worker_token.clone());

    // 3b. A declared model list is authoritative: seed it and skip the
    //     `/v1/models` leg entirely. An engine behind an authenticating proxy,
    //     or one that simply doesn't serve that endpoint, would otherwise never
    //     become routable despite being perfectly healthy.
    if let Some(models) = pool.static_models.as_deref() {
        if models.is_empty() {
            tracing::warn!(
                workerset = pool.name,
                worker_id = w.worker_id_str,
                "modelDiscovery.mode is Static but spec.models is empty; this worker \
                 serves nothing and no scrape will discover otherwise"
            );
        } else {
            // The verdict is NOT optional here. `set_served_models` refuses on a
            // mismatch and returns without touching `by_model`, so claiming the
            // models anyway would advertise a pool that cannot serve them: the
            // router resolves the request here and `score_workers` then finds no
            // worker, forever.
            match registry.set_served_models(w.worker_id_num, models) {
                // Static seeding hands every worker the SAME declared list, so
                // these two cannot arise from a real fleet difference — only from
                // the pin outliving a `spec.models` edit. Routable, but worth a
                // line, because it means `reset_declared_models` did not run.
                crate::workers::ServedModelsVerdict::Widened { .. }
                | crate::workers::ServedModelsVerdict::Narrowed { .. } => {
                    tracing::warn!(
                        workerset = pool.name,
                        worker_id = w.worker_id_str,
                        "declared model list differs from this set's pinned models; \
                         routing anyway on the declared list"
                    );
                    crate::monitor::associate_models(model_router, pool.id, &pool.name, models);
                }
                crate::workers::ServedModelsVerdict::Accepted
                | crate::workers::ServedModelsVerdict::Empty => {
                    crate::monitor::associate_models(model_router, pool.id, &pool.name, models);
                }
                crate::workers::ServedModelsVerdict::Mismatch { expected, observed } => {
                    // Unreachable while the whole set is seeded from one
                    // declared list, so reaching it means the pin outlived a
                    // change to that list — which `reset_declared_models` is
                    // supposed to prevent. Loud, and the worker is skipped
                    // rather than half-registered.
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

    // 4. Spawn the per-worker tasks. The metrics poller + warmup carry
    //    the pool's `model_router` handle so a confirmed /v1/models
    //    scrape updates model→pool routing.
    ingest::spawn_worker_listener(
        w.clone(),
        block_size,
        replay_timeout_ms,
        endpoint_key,
        indexer.clone(),
        telemetry.clone(),
        inflight_prefixes.clone(),
        registry.clone(),
        eviction_tx.clone(),
        worker_token.clone(),
    );
    // A declared model list means nothing to scrape for, so the refresh is
    // pushed out of the way rather than the poller being skipped — it still
    // scrapes queue depth and KV usage, which is most of what the picker uses.
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
        model_router.clone(),
        pool.id,
        pool.name.clone(),
    );

    // 5. Warmup: kick an immediate /v1/models scrape so the worker is
    //    routable on its live model set within sub-second instead of
    //    waiting for the periodic tick (~30s). Also kicks the
    //    LocalTokenizerRegistry warmup if local-mode is on, so the
    //    first user request finds the tokenizer cache warm.
    spawn_one_shot_warmup(
        w,
        metrics.clone(),
        registry.clone(),
        warmup_tokenizer,
        eviction_tx.clone(),
        model_router.clone(),
        pool.id,
        pool.name.clone(),
    );
}

/// The side effects `PoolManager` delegates: starting a pool's discovery, and
/// dropping its workers' fleet-wide state when it retires.
///
/// It lives here rather than in `pool_manager` because it needs handles the
/// manager has no business knowing about — the coordinator channel, telemetry,
/// the eviction channel. That split is what keeps the lifecycle logic testable
/// against a recording stub.
struct MainHooks {
    /// Process token. Pool tokens are children of it (via `Pool::from_spec`);
    /// the discovery tokens below are children of it too, so process shutdown
    /// still reaches everything.
    cancel: CancellationToken,
    coord_tx: tokio::sync::mpsc::Sender<(Arc<Pool>, WorkerChange)>,
    kube_client: Option<kube::Client>,
    telemetry: Arc<Telemetry>,
    metrics: Arc<MetricsRegistry>,
    task_tokens: Arc<DashMap<(pool::PoolId, u64), CancellationToken>>,
    worker_pool: Arc<DashMap<u64, Arc<Pool>>>,
    observation_ttl: std::time::Duration,
    /// Per-pool token scoping that pool's discovery, forwarder, and prefix
    /// sweep.
    ///
    /// Deliberately NOT `pool.cancel`: that one survives retirement until the
    /// reaper fires, because the `KvIndexer` panics if anything reaches it
    /// after cancellation. Discovery has the opposite requirement — it must
    /// stop the instant the pool retires, or it keeps announcing workers into
    /// a pool nothing routes to.
    discovery_tokens: DashMap<pool::PoolId, CancellationToken>,
}

impl pool_manager::PoolHooks for MainHooks {
    fn on_create(&self, pool: &Arc<Pool>, spec: &config::PoolSpec) -> anyhow::Result<()> {
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
        // Per-pool inflight-prefix TTL sweep. Drops dispatches older than
        // `observation_ttl` as a backstop against lifecycle leaks (a
        // BlockStored event lost upstream, a stream that dropped without TTFT).
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
                &self.telemetry,
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

/// Single teardown sequence for both discovery-initiated removals
/// (`WorkerChange::Removed`) and active evictions (homogeneity
/// violations reported on the eviction channel). Cancels the
/// worker's per-task token, drops every state store entry, and asks
/// the KvIndexer to drop the worker's radix-tree subtree.
///
/// `reason` is `None` for discovery removals (the pod just left;
/// nothing anomalous to log) and `Some(_)` for evictions triggered
/// by a homogeneity violation. The latter produces a WARN with the
/// reason so the operator's incident trail explains why a worker
/// disappeared mid-flight.
async fn evict_worker(
    worker_id_num: u64,
    reason: Option<eviction::EvictionReason>,
    pool: &Arc<pool::Pool>,
    telemetry: &Arc<Telemetry>,
    metrics: &Arc<MetricsRegistry>,
    task_tokens: &Arc<DashMap<(pool::PoolId, u64), CancellationToken>>,
    worker_pool: &Arc<DashMap<u64, Arc<Pool>>>,
) {
    drop_worker_state(
        worker_id_num,
        reason,
        pool,
        telemetry,
        metrics,
        task_tokens,
        worker_pool,
    );

    // Ask the KvIndexer to drop this worker's subtree. `remove_worker` is
    // the dynamo-kv-router public API for this; on the next find_matches
    // call the removed worker's blocks won't be attributed.
    pool.indexer.remove_worker(worker_id_num).await;
}

/// Everything `evict_worker` does EXCEPT the indexer call — synchronous, and
/// safe to run against a pool that is being torn down.
///
/// Retirement needs exactly this half. `KvIndexer::remove_worker` unwraps a
/// channel send, so calling it on a pool whose token is about to be cancelled
/// races the cancellation and panics the loser; and the subtree is about to be
/// dropped wholesale anyway, so removing workers from it one at a time buys
/// nothing.
#[allow(clippy::too_many_arguments)]
fn drop_worker_state(
    worker_id_num: u64,
    reason: Option<eviction::EvictionReason>,
    pool: &Arc<pool::Pool>,
    telemetry: &Arc<Telemetry>,
    metrics: &Arc<MetricsRegistry>,
    task_tokens: &Arc<DashMap<(pool::PoolId, u64), CancellationToken>>,
    worker_pool: &Arc<DashMap<u64, Arc<Pool>>>,
) {
    // Resolved BEFORE the registry entry is dropped below, and only for the
    // log line: the numeric id is an internal handle that means nothing to
    // whoever is reading this, while the string is the pod's k8s coordinate.
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

    // Stop the per-worker tasks (ZMQ listener + metrics poller). They
    // select on the cancel token and exit cleanly. Scoped to THIS pool, so a
    // worker that has since moved keeps the new pool's listeners running.
    if let Some((_, token)) = task_tokens.remove(&(pool.id, worker_id_num)) {
        token.cancel();
    }

    // Per-pool state: unconditionally ours to drop.
    pool.registry.unregister(worker_id_num);

    // Fleet-wide state is keyed by `worker_id_num` alone, so tear it down only
    // while this pool is still the registered owner. Without the guard a
    // straggling Removed from a departed pool blanks the current owner's
    // metrics — and a worker with no samples reads as the CHEAPEST on every
    // load term, so it attracts every pick until the next scrape.
    //
    // `model_router` is deliberately not touched either way: the pool may still
    // serve the model via other workers, and a stale claim only yields a
    // transient NoWorkers that self-heals.
    let still_ours = worker_pool
        .get(&worker_id_num)
        .is_none_or(|e| e.value().id == pool.id);
    if still_ours {
        telemetry.unregister_worker(worker_id_num);
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

/// This replica's `holderIdentity` on the Lease.
///
/// `POD_NAME` (downward API) is the useful answer, because it is what an
/// operator sees in `kubectl get pods` and can act on. Hostname is the same
/// string in practice for a Deployment pod, and the random fallback only exists
/// so two processes on one host cannot both claim the identity and fight
/// silently — a random loser is better than a stalemate.
fn leader_identity() -> String {
    if let Ok(name) = std::env::var("POD_NAME")
        && !name.trim().is_empty()
    {
        return name;
    }
    if let Ok(host) = hostname()
        && !host.trim().is_empty()
    {
        return host;
    }
    let mut b = [0u8; 8];
    let _ = getrandom::getrandom(&mut b);
    format!("kinference-{}", u64::from_le_bytes(b))
}

fn hostname() -> std::io::Result<String> {
    std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string())
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
/// SIGTERM is the one that matters in production: it is what Kubernetes sends
/// to begin pod termination, and what `docker stop` sends. Handling only
/// `ctrl_c()` (SIGINT) meant that in a cluster kinference got no graceful shutdown at
/// all — SIGTERM's default disposition killed the process outright, so the
/// cancellation token never fired. Nothing downstream of it ran: the gRPC server
/// never drained in-flight requests, buffered bodies were never released, and
/// the leader Lease was never vacated, which silently cost every failover the
/// full lease duration instead of an immediate handover.
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
