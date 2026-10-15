//! `KinferenceService`: the scoring and pick state behind the gRPC handlers in
//! `crate::grpc`.
//!
//! This module owns the service struct, `score_workers` (tokenize, query the
//! indexer, assemble `WorkerScore` rows), and the value types passed through
//! it. The pick lifecycle is in `lifecycle`; tokenize/render HTTP plumbing is
//! in [`crate::router::tokenize_client`].

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use dynamo_kv_router::indexer::KvIndexerInterface;
use dynamo_kv_router::protocols::{BlockHashOptions, compute_block_hash_for_seq};
use kinference_policy::{AUTO_MODEL, PoolId};

use crate::fleet::pool::Pool;
use crate::fleet::scrape::MetricsRegistry;
use crate::fleet::workers::WorkerEntry;
use crate::router::observations::{
    ObservationSink, PendingObservations, RequestLabels, RequestMeta, format_csv_row,
};
use crate::router::picker::WorkerScore;
use crate::router::tokenize::TokenizeInput;

/// Quality-of-service class for a request, mapped from the proto
/// `PickRequest.QosClass`. Used as the `qos` label on `/metrics` series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Qos {
    /// Latency-sensitive / interactive (the proto zero value / default).
    Interactive,
    /// Throughput-oriented; tolerant of higher TTFT.
    Batch,
    /// Lowest priority; may be deferred under load.
    Deferrable,
    /// The class could not be determined, so none is asserted.
    ///
    /// Only on the terminal-report path, when `report_final_token` gets a
    /// `response_id` it never issued or whose labels expired. Never from
    /// [`Qos::from_proto`], since policy matches on QoS at pick time.
    Unknown,
}

impl Qos {
    /// Map the proto `QosClass` i32 (INTERACTIVE=0, BATCH=1, DEFERRABLE=2).
    /// Unknown values fall back to `Interactive` (the proto zero value).
    pub(crate) fn from_proto(v: i32) -> Self {
        match v {
            1 => Qos::Batch,
            2 => Qos::Deferrable,
            _ => Qos::Interactive,
        }
    }

    /// Stable lowercase value for the Prometheus `qos=…` label.
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Qos::Interactive => "interactive",
            Qos::Batch => "batch",
            Qos::Deferrable => "deferrable",
            Qos::Unknown => "unknown",
        }
    }
}

/// The scores from `score_workers`, held in `ScoreOutcome`.
#[derive(Debug, Clone)]
pub struct ScoreResponse {
    pub scores: Vec<WorkerScore>,
}

/// Outcome of [`KinferenceService::score_workers`].
pub(crate) enum ScoreOutcome {
    Ok(ScoreResponse),
    /// No workers in the registry serve the requested model name.
    NoWorkers,
    /// The prefix-cache signal was lost: `matched_blocks` and `total_blocks`
    /// are 0 on every entry, but the load fields are populated, so the picker
    /// still runs, without the overlap signal.
    Unavailable(ScoreResponse, Degraded),
}

/// Which subsystem cost this pick its prefix-cache signal.
///
/// Both yield a load-only pick, but have different causes: the engine or
/// network, or kinference's own index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Degraded {
    /// Every `/render` attempt across the pool failed, so there are no token
    /// ids to hash and no prefix signal is possible.
    Tokenize,
    /// Tokenization succeeded, but this pool's radix-tree query failed. The
    /// tokens are good; the pool's view of what is cached where is not.
    PrefixIndex,
}

impl Degraded {
    /// Metric label and the operator-facing wording in `PickResponse.reason`.
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::Tokenize => "tokenize",
            Self::PrefixIndex => "prefix_index",
        }
    }

    fn reason(self) -> &'static str {
        match self {
            Self::Tokenize => "tokenize unavailable across the fleet",
            Self::PrefixIndex => "prefix index query failed",
        }
    }
}

/// One `(pool, model)` pair, scored and ready to build candidates from.
///
/// Prefix hashes and block size are per-pool, so the critical section must
/// commit with the scores and side data of the group that actually won.
pub(crate) struct ScoredGroup {
    pub pool: Arc<Pool>,
    /// The upstream model name, which is what the engine serves and what gets
    /// written into the outbound body. Differs from the client-facing name when
    /// an alias resolved.
    pub model: String,
    pub scores: Vec<WorkerScore>,
    pub side: ScoreSideData,
}

/// What the policy chose.
///
/// A provider win has no group (no pool, scores or prefix hashes), hence an
/// enum.
pub(crate) enum PolicySelection {
    /// The policy had no opinion; the ordinary picker decides within `group`.
    /// `None` when there is no local group, e.g. a model only a provider
    /// serves on a pool with no policy.
    Picker { group: Option<ScoredGroup> },
    /// The policy chose this endpoint, inside `group`.
    Local {
        group: ScoredGroup,
        endpoint: String,
    },
    /// The policy chose an off-cluster provider. Nothing pool-scoped applies.
    Provider(ProviderPick),
}

/// A chosen off-cluster destination, flattened for the pick response.
///
/// A backend named by reference, the path to dispatch on, and the vendor's
/// spelling of the model. No URL or credential.
#[derive(Debug, Clone)]
pub(crate) struct ProviderPick {
    pub provider: kinference_policy::engine::ObjectKey,
    pub backend_ref: kinference_policy::engine::ObjectKey,
    pub path: Option<String>,
    /// The vendor's name for the model, written into the body.
    pub upstream_model: String,
    /// The canonical name the request asked for, for logs and status.
    pub canonical_model: String,
}

/// Turn a winning candidate into a [`PolicySelection`].
///
/// The local arm takes the scored group the winner indexes, so the critical
/// section commits against the pool that produced the scores.
fn selection(
    scored: &mut Vec<ScoredGroup>,
    candidate: kinference_policy::engine::Candidate,
) -> PolicySelection {
    match candidate.target {
        kinference_policy::engine::CandidateTarget::Local {
            score_idx,
            endpoint,
            ..
        } => PolicySelection::Local {
            group: scored.swap_remove(score_idx),
            endpoint,
        },
        kinference_policy::engine::CandidateTarget::Provider {
            provider,
            backend_ref,
            path,
            ..
        } => PolicySelection::Provider(ProviderPick {
            provider,
            backend_ref,
            path,
            upstream_model: candidate.upstream_model,
            canonical_model: candidate.canonical_model,
        }),
    }
}

/// Log the winner precisely enough to find it in the ranked candidate list.
fn log_winner(
    req_label: &str,
    pool: &str,
    candidate: &kinference_policy::engine::Candidate,
    trace: &kinference_policy::engine::Trace,
) {
    let (endpoint, objective, path) = match &candidate.target {
        kinference_policy::engine::CandidateTarget::Local {
            endpoint,
            predicted_ttft_ms,
            ..
        } => (
            endpoint.clone(),
            format!("{predicted_ttft_ms:.1}"),
            // A local engine is OpenAI-native, so the path is never rewritten.
            "",
        ),
        kinference_policy::engine::CandidateTarget::Provider {
            backend_ref,
            observed_ttft_ms,
            path,
            ..
        } => (
            backend_ref.label(),
            observed_ttft_ms
                .map(|ms| format!("~{ms:.1}"))
                .unwrap_or_else(|| "unobserved".to_string()),
            path.as_deref().unwrap_or(""),
        ),
    };
    // `path` is logged empty rather than omitted when nothing is rewritten.
    tracing::debug!(
        req = %req_label,
        pool = %pool,
        backend = %candidate.backend,
        class = %candidate.backend_type().as_str(),
        endpoint = %endpoint,
        path = %path,
        predicted_ms = %objective,
        model = %candidate.upstream_model,
        "policy: {}",
        trace.summary()
    );
}

/// Candidates for one off-cluster provider and one canonical model name.
///
/// Nothing is scored, queried or tokenized. Every per-endpoint signal is
/// `None` (not a default, since a latency of 0 would win every ranking), and
/// the only latency is the caller-reported p95.
fn provider_candidates_for(
    health: &kinference_policy::provider_health::ProviderHealthStore,
    prov: &kinference_policy::resolve::ResolvedProvider,
    canonical_model: &str,
) -> Vec<kinference_policy::engine::Candidate> {
    let provider = kinference_policy::engine::ObjectKey::new(&prov.namespace, &prov.name);
    // A suppressed provider (repeated dispatch failures) produces no
    // candidates.
    if health.is_suppressed(&provider) {
        return Vec::new();
    }
    let observed_ttft_ms = health.observed_ttft_ms(&provider);
    let labels = Arc::new(prov.labels.clone());
    prov.upstreams_for(canonical_model)
        .into_iter()
        .map(
            |(upstream, context_window)| kinference_policy::engine::Candidate {
                canonical_model: canonical_model.to_string(),
                upstream_model: upstream,
                backend: prov.name.clone(),
                labels: labels.clone(),
                target: kinference_policy::engine::CandidateTarget::Provider {
                    provider: provider.clone(),
                    backend_ref: prov.backend_ref.clone(),
                    path: prov.path.clone(),
                    // `None` until the first report; still eligible under
                    // EstimatedTTFT.
                    observed_ttft_ms,
                },
                attrs: kinference_policy::predicate::CandidateAttrs {
                    // Provider: no local engine, so neither saturation gauge exists.
                    kv_cache_usage: None,
                    // Declared, not discovered.
                    context_window: context_window.map(|w| w as i64),
                    predicted_ttft_ms: None,
                    prefix_cache_hit_ratio: None,
                    queued_prompts: None,
                    model_name: Some(canonical_model.to_string()),
                    backend_name: Some(prov.name.clone()),
                    backend_type: Some(
                        kinference_policy::crd::BackendType::Provider
                            .as_str()
                            .to_string(),
                    ),
                    backend_ready_endpoints: None,
                    backend_observed_ttft_ms: observed_ttft_ms.map(|ms| ms as f64),
                    // No source yet; compile warns when a policy names it.
                    backend_budget_exhausted: None,
                },
            },
        )
        .collect()
}

/// Byproducts of `score_workers` the pick path needs after the pick.
///
/// `WorkerScore.matched_blocks` carries the raw radix-tree match; α-decay
/// layering happens later, inside the `pick_lock` critical section.
pub(crate) struct ScoreSideData {
    /// Block hashes, one list per block size. The pick path looks up the
    /// chosen worker's to record the pending prefix.
    pub hashes_by_block_size: HashMap<u32, Vec<u64>>,
    /// The resolved token IDs.
    ///
    /// Tokenization depends on the model, not the pool, so a policy scoring
    /// one model across several pools tokenizes once and re-scores via
    /// [`Input::Tokens`].
    pub tokens: Vec<u32>,
}

pub struct KinferenceService {
    /// The pools this fleet manages. Each owns its own predictor, picker,
    /// block size, indexer and registry. Pools come and go, and the service is
    /// shared behind an `Arc`, so the set is swapped rather than edited.
    pub(crate) pools: Arc<crate::fleet::pool::PoolRegistry>,
    /// Model to serving pools, populated as workers report `/v1/models` (and
    /// seeded from static `model_name` hints). Read on the pick hot path.
    /// Several pools may serve one model; `pick_and_record` ranks across them.
    pub(crate) model_router: Arc<RwLock<HashMap<String, BTreeSet<PoolId>>>>,
    pub(crate) metrics: Arc<MetricsRegistry>,
    /// Shared HTTP client for tokenization calls, with a per-request timeout.
    /// The tokenization mode and render URL are per pool.
    pub(crate) tokenize_client: Arc<reqwest::Client>,
    /// In-process tokenizer registry, keyed by model and shared across pools.
    /// `None` when it couldn't be built at startup; `local`/`compare` pools
    /// then fall back to `/render`.
    pub(crate) local_tokenizer: Option<Arc<crate::router::local_tokenize::LocalTokenizerRegistry>>,
    /// Warn-once latch for local tokenization falling back to `/render`, per
    /// `(model_name, error_shape)`, since some models fail locally on every
    /// request.
    pub(crate) local_tokenize_warn_latch: dashmap::DashMap<String, ()>,
    /// Per-pick observation stash. Inserted by `pick_and_record`, consumed by
    /// `finalize_observation` (CSV row) or `abort_observation`, and TTL-swept.
    pub(crate) pending_observations: Arc<PendingObservations>,
    /// Per-request metric labels (model + qos), keyed by `response_id`.
    /// Inserted at pick; taken at `ReportFinalToken` to label the token +
    /// ITL metrics (the `PendingObservation` is gone by then). TTL-swept.
    pub(crate) request_labels: Arc<RequestLabels>,
    /// Predicted-vs-observed CSV sink.
    pub(crate) observation_sink: Arc<ObservationSink>,
    /// Buffered request bodies for the `PushBody` → `Pick` → `PullBody` flow.
    /// See [`crate::router::body_store`].
    pub(crate) body_store: Arc<crate::router::body_store::BodyStore>,
    /// CRD-sourced routing policy, when `[policy] enabled = true`. When `None`,
    /// a request naming an `InferencePool` falls back to model-based routing.
    pub(crate) policy: Option<Arc<crate::policy::store::PolicyStore>>,
    /// What the caller has reported back about off-cluster destinations.
    ///
    /// Always present, even with policy disabled, so the feedback handlers
    /// needn't check.
    pub(crate) provider_health: Arc<kinference_policy::provider_health::ProviderHealthStore>,
    /// Maximum silence between two `PushBody` chunks before the push is
    /// abandoned. The push itself has no deadline, since it spans the client's
    /// upload.
    pub(crate) body_idle_timeout: std::time::Duration,
    /// The metrics kinference publishes on its own `/metrics`.
    pub(crate) prom: Arc<crate::metrics::PromMetrics>,
}

impl KinferenceService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pools: Arc<crate::fleet::pool::PoolRegistry>,
        model_router: Arc<RwLock<HashMap<String, BTreeSet<PoolId>>>>,
        metrics: Arc<MetricsRegistry>,
        tokenize_client: Arc<reqwest::Client>,
        local_tokenizer: Option<Arc<crate::router::local_tokenize::LocalTokenizerRegistry>>,
        pending_observations: Arc<PendingObservations>,
        request_labels: Arc<RequestLabels>,
        body_store: Arc<crate::router::body_store::BodyStore>,
        policy: Option<Arc<crate::policy::store::PolicyStore>>,
        provider_health: Arc<kinference_policy::provider_health::ProviderHealthStore>,
        body_idle_timeout: std::time::Duration,
        observation_sink: Arc<ObservationSink>,
        prom: Arc<crate::metrics::PromMetrics>,
    ) -> Self {
        Self {
            pools,
            model_router,
            metrics,
            tokenize_client,
            local_tokenizer,
            local_tokenize_warn_latch: dashmap::DashMap::new(),
            pending_observations,
            request_labels,
            body_store,
            policy,
            provider_health,
            body_idle_timeout,
            observation_sink,
            prom,
        }
    }

    /// Mint a fresh `response_id` for a new pick.
    ///
    /// Random, so a peer can't guess another caller's id and `PullBody` its
    /// body, and never 0, which means "no id" on the wire. Collisions are
    /// negligible at 64 bits.
    pub(crate) fn mint_response_id(&self) -> u64 {
        loop {
            let mut buf = [0u8; 8];
            let id = match getrandom::getrandom(&mut buf) {
                Ok(()) => u64::from_le_bytes(buf),
                Err(e) => {
                    tracing::error!(error = %e, "getrandom failed while minting a response_id");
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(1)
                }
            };
            if id != 0 {
                return id;
            }
        }
    }

    /// Every client-facing name declared for `upstream` across the named pool's
    /// worker sets, sorted, deduped, and excluding `upstream` itself.
    ///
    /// Feeds `RequestAttrs.model` so a predicate can match the client's
    /// spelling. Empty when no pool is named or policy is off.
    fn declared_aliases(
        &self,
        pool_ref: Option<&kinference_policy::PoolRef>,
        upstream: &str,
    ) -> Vec<String> {
        let Some(store) = self.policy.as_ref() else {
            return Vec::new();
        };
        let Some(pref) = pool_ref else {
            return Vec::new();
        };
        let snapshot = store.snapshot();
        let Some(resolved) = snapshot.lookup(&pref.namespace, &pref.name) else {
            return Vec::new();
        };
        // Local alias tables only; a provider's rows map the other way.
        let mut out: Vec<String> = resolved
            .local_destinations()
            .flat_map(|d| d.aliases_for(upstream))
            .filter(|a| a != upstream)
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Translate a client-facing alias into an upstream model an engine
    /// actually serves, using the alias tables on the named pool's worker sets.
    ///
    /// `None` when there is nothing to translate: no pool named, policy off,
    /// the pool unresolved, or no declared alias whose upstream is served.
    ///
    /// Only reached after `resolve_pools` came back empty, so this never
    /// shadows a model an engine reports under its own name. Local
    /// destinations only; providers map names when candidates are built.
    fn resolve_alias(
        &self,
        pool_ref: Option<&kinference_policy::PoolRef>,
        canonical: &str,
    ) -> Option<String> {
        let store = self.policy.as_ref()?;
        let pref = pool_ref?;
        let snapshot = store.snapshot();
        let resolved = snapshot.lookup(&pref.namespace, &pref.name)?;
        let local: Vec<&kinference_policy::resolve::ResolvedWorkerSet> =
            resolved.local_destinations().collect();
        first_served_upstream(&local, canonical, |m| !self.resolve_pools(m).is_empty())
    }

    /// Whether any provider in the named pool declares this canonical model.
    ///
    /// Consulted after `resolve_pools` came back empty, so a model only a
    /// provider serves isn't refused before `policy_select` runs.
    fn provider_serves(&self, pool_ref: Option<&kinference_policy::PoolRef>, model: &str) -> bool {
        let Some(store) = self.policy.as_ref() else {
            return false;
        };
        let Some(pref) = pool_ref else {
            return false;
        };
        let snapshot = store.snapshot();
        let Some(resolved) = snapshot.lookup(&pref.namespace, &pref.name) else {
            return false;
        };
        resolved
            .destinations
            .iter()
            .filter_map(|d| d.as_provider())
            .any(|p| !p.upstreams_for(model).is_empty())
    }

    /// Every pool currently serving `model`, in stable id order.
    ///
    /// Empty when no pool has claimed the model yet, or every claiming pool has
    /// since been removed (ids that resolve to nothing are dropped). The
    /// returned handles keep their pools alive.
    pub(crate) fn resolve_pools(&self, model: &str) -> Vec<Arc<Pool>> {
        let ids: Vec<PoolId> = match self.model_router.read().unwrap().get(model) {
            Some(set) => set.iter().copied().collect(),
            None => return Vec::new(),
        };
        let snapshot = self.pools.snapshot();
        ids.into_iter()
            .filter_map(|id| snapshot.get(id).cloned())
            .collect()
    }

    /// Resolve the `"auto"` pseudo-model to a concrete model for this prompt.
    ///
    /// For now, uniform random among the models any pool serves; `messages` is
    /// passed for future prompt-aware selection. `None` when no model is known
    /// yet, which the caller treats like an unknown model.
    pub(crate) fn choose_model_for_auto(&self, _messages: &[serde_json::Value]) -> Option<String> {
        let router = self.model_router.read().unwrap();
        // Never route "auto" to itself.
        let candidates: Vec<&String> = router.keys().filter(|m| m.as_str() != AUTO_MODEL).collect();
        match candidates.len() {
            0 => None,
            1 => Some(candidates[0].clone()),
            n => {
                let mut buf = [0u8; 8];
                let idx = match getrandom::getrandom(&mut buf) {
                    Ok(()) => (u64::from_le_bytes(buf) as usize) % n,
                    Err(_) => 0,
                };
                Some(candidates[idx].clone())
            }
        }
    }
}

/// Project a scraped sample onto a [`WorkerScore`], for both `score_workers`
/// and [`KinferenceService::degraded_response`].
///
/// The prefix-cache values are parameters because the degraded path zeroes
/// them while still reporting real load.
fn worker_score(
    entry: &WorkerEntry,
    sample: &crate::fleet::scrape::MetricsSample,
    block_size: u32,
    matched_blocks: u32,
    total_blocks: u32,
) -> WorkerScore {
    WorkerScore {
        name: entry.worker_id_str.clone(),
        endpoint: entry.score_addr.clone(),
        matched_blocks,
        completion_rate_per_sec: sample.completion_rate_per_sec,
        queue_waiting: sample.waiting,
        queue_running: sample.running,
        queue_swapped: sample.swapped,
        kv_cache_usage: sample.kv_cache_usage,
        block_size,
        total_blocks,
    }
}

/// Associate `models` with `pool_id` in the model router. Called after a
/// `/v1/models` scrape, and provisionally from `handle_added` with the static
/// `model_name` hint.
///
/// Several pools may serve one model, and all are recorded; ranking happens at
/// pick time. Claims are removed by [`release_models`].
pub(crate) fn associate_models(
    router: &RwLock<HashMap<String, BTreeSet<PoolId>>>,
    pool_id: PoolId,
    pool_name: &str,
    models: &[String],
) {
    if models.is_empty() {
        return;
    }
    // Fast path for the steady state, so concurrent picks aren't blocked by
    // the write lock.
    {
        let map = router.read().unwrap();
        if models
            .iter()
            .all(|m| map.get(m).is_some_and(|set| set.contains(&pool_id)))
        {
            return;
        }
    }
    let mut map = router.write().unwrap();
    for m in models {
        if map.entry(m.clone()).or_default().insert(pool_id) {
            let serving = map[m].len();
            if serving > 1 {
                tracing::info!(
                    model = %m,
                    pool = %pool_name,
                    pools_serving = serving,
                    "model also served by this pool; both are pick candidates"
                );
            } else {
                tracing::info!(model = %m, pool = %pool_name, "model routed to pool");
            }
        }
    }
}

/// Input variants `score_workers` accepts. The Pick handler uses `Messages`;
/// policy re-scoring uses `Tokens`.
pub(crate) enum Input {
    #[allow(dead_code)]
    Raw(String),
    #[allow(dead_code)]
    Chat(String),
    /// OpenAI-style messages array, already parsed.
    Messages(Vec<serde_json::Value>),
    #[allow(dead_code)]
    Tokens(Vec<u32>),
}

/// First upstream among `destinations`' alias tables that `is_served` accepts.
///
/// Split out of `KinferenceService::resolve_alias` for testing. Skips the
/// canonical name itself, which no pool serves if we got here. Takes the first
/// match, so interchangeable upstreams resolve to one fixed choice.
pub(crate) fn first_served_upstream(
    destinations: &[&kinference_policy::resolve::ResolvedWorkerSet],
    canonical: &str,
    is_served: impl Fn(&str) -> bool,
) -> Option<String> {
    destinations
        .iter()
        .flat_map(|d| d.upstreams_for(canonical))
        .find(|u| u != canonical && is_served(u))
}

/// Pick the pool a no-policy request should be served from.
///
/// Each group is scored by its best endpoint under
/// [`crate::router::picker::predicted_wait_ms`], the same term the policy path's
/// `candidates_for` uses.
///
/// Only the pool is decided here; the endpoint is chosen afterwards under
/// that pool's `pick_lock`. Ranking without locks avoids deadlocks between
/// requests taking two pools' locks in opposite orders.
///
/// Groups whose scores are empty cannot win; `None` means no pool serving
/// the model had a usable worker.
fn best_group(
    groups: Vec<ScoredGroup>,
    req_label: &str,
    // `Option` so tests can omit it. Predictions are recorded here, where
    // they are computed per endpoint.
    prom: Option<&crate::metrics::PromMetrics>,
) -> Option<ScoredGroup> {
    let now = Instant::now();
    let multi = groups.len() > 1;
    let mut best: Option<(f32, ScoredGroup)> = None;
    for group in groups {
        let Some(wait) = group
            .scores
            .iter()
            .map(|s| {
                let summaries: Vec<crate::fleet::inflight::InflightSummary> = group
                    .pool
                    .inflight_prefixes
                    .dispatches_for(&s.endpoint)
                    .iter()
                    .map(|d| d.summary())
                    .collect();
                let ms = crate::router::picker::predicted_wait_ms(
                    s,
                    &group.pool.predictor,
                    &summaries,
                    now,
                )
                .total_ms();
                // Every endpoint's prediction is recorded, as on the policy
                // path.
                if let Some(p) = prom {
                    p.record_predicted_ttft(&s.endpoint, &group.model, ms as f64);
                }
                ms
            })
            .fold(None::<f32>, |acc, ms| Some(acc.map_or(ms, |a| a.min(ms))))
        else {
            continue;
        };
        if multi {
            tracing::debug!(
                req = %req_label,
                pool = %group.pool.name,
                endpoints = group.scores.len(),
                best_predicted_wait_ms = wait,
                "pick: pool candidate"
            );
        }
        // Strictly-better keeps the first pool on a tie, and `serving` is in
        // pool-id order, so ties resolve the same way on every replica.
        if best.as_ref().is_none_or(|(b, _)| wait < *b) {
            best = Some((wait, group));
        }
    }
    let (wait, group) = best?;
    if multi {
        tracing::info!(
            req = %req_label,
            pool = %group.pool.name,
            model = %group.model,
            predicted_wait_ms = wait,
            "pick: chose worker set among several serving the model"
        );
    }
    Some(group)
}

/// Drop every claim held by `pool_id`, and forget models nobody serves.
///
/// Called when a pool leaves the fleet.
pub(crate) fn release_models(
    router: &RwLock<HashMap<String, BTreeSet<PoolId>>>,
    pool_id: PoolId,
    pool_name: &str,
) {
    let mut map = router.write().unwrap();
    map.retain(|model, set| {
        if !set.remove(&pool_id) {
            return true;
        }
        if set.is_empty() {
            tracing::info!(
                model = %model,
                workerset = %pool_name,
                "last worker set serving this model went away"
            );
            false
        } else {
            true
        }
    });
}

/// OpenAI ChatCompletionRequest fields beyond `messages` that local
/// chat-template rendering needs to match the engine's tokens. Ends up in
/// [`crate::router::tokenize::TokenizeInput::Chat`].
///
/// The default has no tools and `add_generation_prompt = true`, the OpenAI
/// default.
#[derive(Debug, Clone)]
pub struct ChatExtras {
    pub tools: Vec<serde_json::Value>,
    pub tool_choice: Option<serde_json::Value>,
    pub add_generation_prompt: bool,
}

impl Default for ChatExtras {
    fn default() -> Self {
        Self {
            tools: Vec::new(),
            tool_choice: None,
            add_generation_prompt: true,
        }
    }
}

/// Resolved input: pre-supplied tokens, or input for
/// `tokenize_with_fallback`.
pub(crate) enum TokenSource {
    Direct(Vec<u32>),
    NeedsTokenize(TokenizeInput),
}

/// Outcome of [`KinferenceService::pick_and_record`]. For `Ok` and `Degraded`
/// the side effects (inflight increment, inflight-prefix insert,
/// pending-observation stash) have been applied; the error outcomes write no
/// state.
pub(crate) enum PickAndRecordOutcome {
    /// Successful pick. The caller dispatches to `endpoint` and later reports
    /// the observed TTFT.
    Ok {
        endpoint: String,
        response_id: u64,
        /// Rewritten request body, or empty when unchanged, in which case
        /// `grpc::pick` reuses the pushed body. See [`crate::router::pick_body`].
        body: Vec<u8>,
        /// The model this request resolved to (post-`auto`). Surfaced as
        /// `PickResponse.requested_model`.
        model: String,
    },
    /// The prefix-cache signal was unavailable, so the pick was made on load
    /// alone. Otherwise like `Ok`.
    Degraded {
        endpoint: String,
        response_id: u64,
        reason: String,
        /// Rewritten request body; empty when unchanged (see `Ok::body`).
        body: Vec<u8>,
        /// The model this request resolved to (post-`auto`).
        model: String,
    },
    /// A policy chose an off-cluster provider. The body names the vendor's
    /// model and a pending observation is held, but nothing pool-scoped was
    /// touched. `outcome=OK` on the wire, with a `ProviderTarget` instead of
    /// an endpoint.
    Provider {
        pick: ProviderPick,
        response_id: u64,
        /// Rewritten request body. Never empty, since the vendor's model name
        /// always differs.
        body: Vec<u8>,
    },
    /// No ready worker serves the requested model. `model`, the name the
    /// picker tried, appears in the response message. `outcome=NO_WORKERS`.
    NoWorkers { model: String },
    /// A policy refused the request, though workers may be available.
    /// `outcome=POLICY_REJECTED`.
    PolicyRejected { reason: String, model: String },
    /// The body isn't a valid OpenAI ChatCompletionRequest, or has no model.
    /// `model` is the best-known name, often empty.
    BadRequest { reason: String, model: String },
}

mod lifecycle;
mod policy_driver;
mod provider_feedback;
mod scoring;

#[cfg(test)]
mod tests;
