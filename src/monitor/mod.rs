//! Core `KinferenceService` — the shared scoring + pick state used by
//! both front-ends (gRPC `Pick`/`Observe` in `crate::grpc`, and the
//! gRPC `Pick()` handler in `crate::grpc`).
//!
//! This module owns:
//! - The service struct + accessors (`KinferenceService::new` and friends).
//! - The shared `score_workers` engine: tokenize → indexer → assemble
//!   `WorkerScore` rows.
//! - The shared `pick_and_record` / `finalize_observation` /
//!   `abort_observation` lifecycle.
//! - Internal value types passed through the scoring API: `WorkerScore`,
//!   `ScoreResponse`, `ScoreOutcome`, `ScoreSideData`,
//!   `Input`, `ChatExtras`, `PickAndRecordOutcome`.
//!
//! Tokenize/render HTTP plumbing lives next door in
//! [`crate::tokenize_client`] as additional `impl KinferenceService`
//! methods.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use dynamo_kv_router::indexer::KvIndexerInterface;
use dynamo_kv_router::protocols::{BlockHashOptions, compute_block_hash_for_seq};

use crate::metrics::MetricsRegistry;
use crate::observation_sink::ObservationSink;
use crate::observations::{PendingObservations, RequestLabels, RequestMeta, format_csv_row};
use crate::picker::WorkerScore;
use crate::policy::{AUTO_MODEL, PoolId};
use crate::pool::Pool;
use crate::telemetry::{CallSample, Telemetry, TopWorker};
use crate::tokenize::TokenizeInput;
use crate::workers::WorkerEntry;

/// Quality-of-service class for a request. Mapped from the proto
/// `PickRequest.QosClass` at the gRPC boundary so this module stays
/// proto-agnostic. Threaded through `pick_and_record` and stamped as the
/// `qos` label on every `/metrics` series.
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
    /// Reached only on the TERMINAL-REPORT path, when `report_final_token`
    /// gets a `response_id` it never issued or whose labels aged out of the
    /// 600s TTL. There is no `RequestMeta` left to read the real class from.
    ///
    /// NOT reachable from [`Qos::from_proto`], which still maps an
    /// unrecognized wire value to `Interactive`: that is the PICK path, where
    /// policy matches on QoS, and silently declassifying a request would
    /// change routing rather than just reporting.
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

/// Internal scoring response — the result of `score_workers`. Held
/// inside `ScoreOutcome::Ok` / `Unavailable`. Not wire-visible after
/// the Pick/Observe migration.
#[derive(Debug, Clone)]
pub struct ScoreResponse {
    pub scores: Vec<WorkerScore>,
}

/// Outcome of a "compute score components" pass. Returned by
/// [`KinferenceService::score_workers`], which is the shared core used
/// by the gRPC `Pick` handler
/// picker.
///
/// Mapping to caller responses:
/// - gRPC `score()`:
///   - `Ok(resp)` → `Response::new(resp)`
///   - `NoWorkers` → `Status::not_found`
///   - `Unavailable(degraded)` → `Response::new(degraded)`
///     (token_count==0 signals "degraded" to clients).
/// - Pick handler:
///   - `Ok(resp)` → run the picker on `resp.scores`
///   - `NoWorkers` → `ImmediateResponse` 404
///   - `Unavailable(degraded, _)` → run the picker on
///     `degraded.scores` (queue/cache metrics are intact; only the
///     prefix-cache trio is zeroed, so the picker just doesn't get
///     the overlap signal).
pub(crate) enum ScoreOutcome {
    Ok(ScoreResponse),
    /// No workers in the registry serve the requested model name.
    NoWorkers,
    /// The prefix-cache signal was lost. The contained response has
    /// `token_count=0` and `matched_blocks=0`/`total_blocks=0` on every
    /// entry — live load fields (queue, kv_cache_usage, completion rate,
    /// etc.) are populated as usual.
    Unavailable(ScoreResponse, Degraded),
}

/// Which subsystem cost this pick its prefix-cache signal.
///
/// The two are indistinguishable in the result — a load-only pick either way —
/// and were reported identically, so a failing radix tree was logged and
/// recorded as "tokenize unavailable". They have nothing in common
/// operationally: one is the engine or the network, the other is kinference's own
/// index, and only one of them is kinference's bug.
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

/// Per-pick byproducts of `score_workers` that the pick path needs
/// after pick (currently only the prompt's block hashes, used to
/// seed `PendingPrefixes::insert` once the worker is chosen). The
/// gRPC `Score` handler ignores this — only the outcome is
/// wire-visible.
///
/// `WorkerScore.matched_blocks` carries the **raw radix-tree match**
/// (i.e. the kv_match value). Any α-decay layering happens later,
/// inside the `pick_lock` critical section, so all pending
/// reads see a consistent snapshot relative to concurrent dispatches.
/// One `(pool, model)` pair, scored and ready to build candidates from.
///
/// The pool matters as much as the model: prefix hashes and block size are
/// per-pool, so the critical section must commit with the scores and side data
/// of the group that actually won — not the group we happened to score first.
pub(crate) struct ScoredGroup {
    pub pool: Arc<Pool>,
    /// The UPSTREAM model name, which is what the engine serves and what gets
    /// written into the outbound body. Differs from the client-facing name when
    /// an alias resolved.
    pub model: String,
    pub scores: Vec<WorkerScore>,
    pub side: ScoreSideData,
}

/// What the policy chose.
///
/// An enum rather than a group plus an optional endpoint, because a provider win
/// has no group at all — no pool, no scores, no prefix hashes — and carrying a
/// meaningless one would invite the critical section to commit against it.
pub(crate) enum PolicySelection {
    /// The policy had no opinion; the ordinary picker decides within `group`.
    /// `None` when there was no local group to have an opinion about — a model
    /// only a provider serves, on a pool with no policy attached.
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
/// This is everything the caller needs and nothing kinference holds that it should
/// not: a backend named by reference, the path to dispatch on, and the vendor's
/// own spelling of the model. No URL, no credential.
#[derive(Debug, Clone)]
pub(crate) struct ProviderPick {
    pub provider: crate::policy::engine::ObjectKey,
    pub backend_ref: crate::policy::engine::ObjectKey,
    pub path: Option<String>,
    /// The vendor's name for the model — what the body must end up saying.
    pub upstream_model: String,
    /// The canonical name the request asked for, for logs and status.
    pub canonical_model: String,
}

/// Turn a winning candidate into a [`PolicySelection`].
///
/// The local arm consumes the scored group the winner indexes, so the critical
/// section commits against the pool whose radix tree and inflight state actually
/// produced the scores. The provider arm consumes nothing — there is no group.
fn selection(
    scored: &mut Vec<ScoredGroup>,
    candidate: crate::policy::engine::Candidate,
) -> PolicySelection {
    match candidate.target {
        crate::policy::engine::CandidateTarget::Local {
            score_idx,
            endpoint,
            ..
        } => PolicySelection::Local {
            group: scored.swap_remove(score_idx),
            endpoint,
        },
        crate::policy::engine::CandidateTarget::Provider {
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
    candidate: &crate::policy::engine::Candidate,
    trace: &crate::policy::engine::Trace,
) {
    let (endpoint, objective, path) = match &candidate.target {
        crate::policy::engine::CandidateTarget::Local {
            endpoint,
            predicted_ttft_ms,
            ..
        } => (
            endpoint.clone(),
            format!("{predicted_ttft_ms:.1}"),
            // A local engine is OpenAI-native, so the request line is never
            // rewritten — the same thing an empty `path` means for a provider.
            "",
        ),
        crate::policy::engine::CandidateTarget::Provider {
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
    // `path` is logged EMPTY, not omitted, when nothing is rewritten. It is the
    // only place an operator can see the request line kinference handed the gateway
    // — kinference never dispatches, so a wrong path comes back as a 404 the caller
    // reports and nothing else — and a field that disappears when unset cannot
    // distinguish "dispatching on the client's path" from a log line written
    // before the field existed.
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
/// Nothing is scored, queried or tokenized: a provider has no endpoints to
/// scrape, so every per-endpoint signal is `None` and the only latency figure is
/// the aggregate the status writer maintains. `None` here is what makes guide
/// §7.1 hold — a predicate over a signal the candidate does not carry does not
/// match — and it must stay `None` rather than becoming a default, since a
/// defaulted latency of zero would win every ranking.
fn provider_candidates_for(
    health: &crate::policy::provider_health::ProviderHealthStore,
    prov: &crate::policy::store::ResolvedProvider,
    canonical_model: &str,
) -> Vec<crate::policy::engine::Candidate> {
    let provider = crate::policy::engine::ObjectKey::new(&prov.namespace, &prov.name);
    // A destination the caller has repeatedly failed to dispatch to produces no
    // candidates at all, rather than losing on score. There is no dispatch retry
    // anywhere in this design, so every pick it wins is a failed request — and
    // kinference cannot see the cause, only the reports.
    if health.is_suppressed(&provider) {
        return Vec::new();
    }
    let observed_ttft_ms = health.observed_ttft_ms(&provider);
    let labels = Arc::new(prov.labels.clone());
    prov.upstreams_for(canonical_model)
        .into_iter()
        .map(
            |(upstream, context_window)| crate::policy::engine::Candidate {
                canonical_model: canonical_model.to_string(),
                upstream_model: upstream,
                backend: prov.name.clone(),
                labels: labels.clone(),
                target: crate::policy::engine::CandidateTarget::Provider {
                    provider: provider.clone(),
                    backend_ref: prov.backend_ref.clone(),
                    path: prov.path.clone(),
                    // `None` until the first caller report. That keeps the provider
                    // ELIGIBLE under EstimatedTTFT rather than unreachable — it
                    // cannot earn an observation without first being picked, so
                    // dropping it here would be a deadlock.
                    observed_ttft_ms,
                },
                attrs: crate::policy::predicate::CandidateAttrs {
                    // Provider: no local engine, so neither saturation gauge exists.
                    kv_cache_usage: None,
                    // Declared, not discovered. Feeds the same built-in
                    // context-window admission check as a local candidate's, and is
                    // the one signal a provider does carry.
                    context_window: context_window.map(|w| w as i64),
                    predicted_ttft_ms: None,
                    prefix_cache_hit_ratio: None,
                    queued_prompts: None,
                    model_name: Some(canonical_model.to_string()),
                    backend_name: Some(prov.name.clone()),
                    backend_type: Some(
                        crate::policy::crd::BackendType::Provider
                            .as_str()
                            .to_string(),
                    ),
                    backend_ready_endpoints: None,
                    backend_observed_ttft_ms: observed_ttft_ms.map(|ms| ms as f64),
                    // Sourced from a UsageBudget, which does not exist yet. Compile
                    // WARNS when a policy names it, so this being permanently
                    // `None` is visible rather than silent.
                    backend_budget_exhausted: None,
                },
            },
        )
        .collect()
}

pub(crate) struct ScoreSideData {
    /// Computed block-hash list, one Vec per worker block_size. The
    /// The pick path looks up by `chosen.block_size` to feed
    /// `pending.insert()`.
    pub hashes_by_block_size: HashMap<u32, Vec<u64>>,
    /// The resolved token IDs.
    ///
    /// Tokenization is a property of the MODEL (its tokenizer + chat
    /// template), not of the pool — only the block hashing and the index
    /// query are per-pool. Surfacing the tokens lets a policy that reaches
    /// the same model across several worker sets tokenize once and re-score
    /// via [`Input::Tokens`], instead of paying a render round-trip per pool.
    pub tokens: Vec<u32>,
}

pub struct KinferenceService {
    /// The pools this fleet manages, read as an immutable snapshot. Each
    /// pool owns its own predictor/picker/block-size/indexer/registry.
    /// A request resolves to the pools serving its model via `model_router`.
    ///
    /// A registry rather than a `Vec` because the pool set is becoming
    /// mutable: `KinferenceService` lives behind an `Arc` once tonic owns it, so
    /// there is no `&mut self` anywhere and the set has to be swapped rather
    /// than edited.
    pub(crate) pools: Arc<crate::pool::PoolRegistry>,
    /// Reactive model→pools index. Populated as workers report `/v1/models`
    /// (and seeded provisionally from static `model_name` hints at
    /// registration). Read on the pick hot path.
    ///
    /// An INDEX, not a partition: worker sets are independent, so two of them
    /// serving one model is an ordinary redundancy arrangement, not a conflict.
    /// This map used to hold one owner per model and log later claimants as
    /// ignored, which left the losing set dead for that model on the no-policy
    /// path. Now every serving pool is a pick candidate and `pick_and_record`
    /// ranks across them.
    pub(crate) model_router: Arc<RwLock<HashMap<String, BTreeSet<PoolId>>>>,
    pub(crate) metrics: Arc<MetricsRegistry>,
    pub(crate) telemetry: Arc<Telemetry>,
    /// Shared HTTP client for all `/tokenize` calls. Built in `main.rs`
    /// with a 3s per-request timeout so connections pool and hangs are
    /// bounded. Fleet-wide — one client serves every pool. (The
    /// tokenization strategy — mode + render URL — is per-pool; see
    /// `Pool::tokenize_mode` / `Pool::tokenize_render_url`.)
    pub(crate) tokenize_client: Arc<reqwest::Client>,
    /// In-process tokenizer registry, keyed by model. Shared across
    /// pools (one on-disk cache). `None` only when local tokenization
    /// couldn't be built at startup; pools in `local`/`compare` mode
    /// then fall back to HTTP `/render`.
    pub(crate) local_tokenizer: Option<Arc<crate::local_tokenize::LocalTokenizerRegistry>>,
    /// One-shot warn latch for "local tokenize failed; falling back
    /// to HTTP /render". Models whose `tokenizer_config.json` has no
    /// `chat_template` field (e.g. some Gemma builds) make local fail
    /// on every chat request — without a latch the operator gets one
    /// WARN per request. We log the first failure for each
    /// `(model_name, error_shape)` pair and stay quiet thereafter;
    /// behavior is unchanged (always falls back to HTTP /render).
    pub(crate) local_tokenize_warn_latch: dashmap::DashMap<String, ()>,
    /// Per-pick observation stash. Inserted at pick time by
    /// `pick_and_record`; consumed by `finalize_observation` (CSV
    /// emit) or `abort_observation` (drop without CSV). TTL sweep
    /// runs in main.rs as a backstop against leaked entries.
    pub(crate) pending_observations: Arc<PendingObservations>,
    /// Per-request metric labels (model + qos), keyed by `response_id`.
    /// Inserted at pick; taken at `ReportFinalToken` to label the token +
    /// ITL metrics (the `PendingObservation` is gone by then). TTL-swept.
    pub(crate) request_labels: Arc<RequestLabels>,
    /// Predicted-vs-observed CSV sink. Single shared instance so
    /// rows from both front-ends interleave into one file (offline
    /// analysis can split by `replication_fired` / `x_request_id`
    /// pattern if a per-source breakdown is needed).
    pub(crate) observation_sink: Arc<ObservationSink>,
    /// Buffered request bodies for the `PushBody` → `Pick` → `PullBody` flow.
    /// kinference owns the bytes between those calls; the store bounds that memory
    /// and reclaims by TTL. See [`crate::body_store`].
    pub(crate) body_store: Arc<crate::body_store::BodyStore>,
    /// CRD-sourced routing policy, when `[policy] enabled = true`. `None`
    /// disables the whole feature: a request naming an `InferencePool` then
    /// falls back to today's model→pool routing rather than failing.
    pub(crate) policy: Option<Arc<crate::policy::store::PolicyStore>>,
    /// What the caller has reported back about off-cluster destinations.
    ///
    /// Not behind the `policy` `Option` even though a provider is only reachable
    /// through a pool: the feedback path is on the `Abort` and
    /// `ReportFirstToken` handlers, which do not know whether policy is enabled,
    /// and an always-present empty store is cheaper than threading that question
    /// through them.
    pub(crate) provider_health: Arc<crate::policy::provider_health::ProviderHealthStore>,
    /// Maximum silence between two `PushBody` chunks before the push is
    /// abandoned. The call itself gets no deadline — a push spans the end
    /// client's upload, which can legitimately take minutes — so this gap
    /// bound plus the byte budgets are what keep it finite.
    pub(crate) body_idle_timeout: std::time::Duration,
    /// Prometheus exposition registry — the counters surfaced on our
    /// own `/metrics`. `report_final_token` records per-model token totals
    /// here. Shared `Arc` with the dashboard's `/metrics` handler.
    pub(crate) prom: Arc<crate::prom::PromMetrics>,
}

impl KinferenceService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pools: Arc<crate::pool::PoolRegistry>,
        model_router: Arc<RwLock<HashMap<String, BTreeSet<PoolId>>>>,
        metrics: Arc<MetricsRegistry>,
        telemetry: Arc<Telemetry>,
        tokenize_client: Arc<reqwest::Client>,
        local_tokenizer: Option<Arc<crate::local_tokenize::LocalTokenizerRegistry>>,
        pending_observations: Arc<PendingObservations>,
        request_labels: Arc<RequestLabels>,
        body_store: Arc<crate::body_store::BodyStore>,
        policy: Option<Arc<crate::policy::store::PolicyStore>>,
        provider_health: Arc<crate::policy::provider_health::ProviderHealthStore>,
        body_idle_timeout: std::time::Duration,
        observation_sink: Arc<ObservationSink>,
        prom: Arc<crate::prom::PromMetrics>,
    ) -> Self {
        Self {
            pools,
            model_router,
            metrics,
            telemetry,
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
    /// Random (not a counter) and never 0, per the proto: after `Pick` the
    /// `response_id` *is* the body's identity for `PullBody`, so a guessable id
    /// would let a peer pull another caller's body. 0 is reserved on the wire to
    /// mean "no id" — every non-OK `Pick` returns it.
    ///
    /// Collisions across the `PendingObservations` / `RequestLabels` /
    /// `InflightPrefixes` keyspaces are a non-issue at 64 bits (a birthday
    /// collision needs ~2^32 concurrent live ids; the TTL caps us many orders of
    /// magnitude below that).
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

    /// Every pool currently serving `model`, in stable id order.
    ///
    /// Empty when no pool has claimed the model yet (unknown model, or
    /// cold-start before any worker has reported `/v1/models`), and empty
    /// again if every claiming pool has since been removed — a claim can
    /// outlive its pool by one reconcile, so ids that resolve to nothing are
    /// dropped rather than reported.
    ///
    /// Returns OWNED handles: the caller may hold them across awaits, and
    /// holding one keeps that pool alive even if the fleet drops it meanwhile.
    /// Every client-facing name declared for `upstream` across the named pool's
    /// worker sets — sorted, deduped, and excluding `upstream` itself.
    ///
    /// Feeds `RequestAttrs.model` so a predicate can match the spelling the
    /// CLIENT used, not just the upstream the request was rewritten to. Empty
    /// when no pool is named or policy is off, in which case `model` is just the
    /// upstream and behaves as it always did.
    fn declared_aliases(
        &self,
        pool_ref: Option<&crate::policy::PoolRef>,
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
        // Local alias tables only. A provider's model rows are the mirror image
        // — a canonical name mapping FORWARD to a vendor's spelling — so
        // inverting one here would answer a different question: it would report
        // the vendor's name as an alias of itself.
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
    /// `None` when there is nothing to translate — no pool named, policy off,
    /// the pool unresolved, or no declared alias whose upstream is currently
    /// served. Callers treat that as "leave the model alone", so an ordinary
    /// request for a real model name costs one failed `model_router` lookup and
    /// nothing else.
    ///
    /// Only reached AFTER `resolve_pools` came back empty, so this never
    /// shadows a model an engine reports under its own name.
    ///
    /// LOCAL destinations only. A provider needs no translation step: its
    /// `models[]` already maps a canonical name to the vendor's spelling, and
    /// that mapping is applied when the candidate is built, not here.
    fn resolve_alias(
        &self,
        pool_ref: Option<&crate::policy::PoolRef>,
        canonical: &str,
    ) -> Option<String> {
        let store = self.policy.as_ref()?;
        let pref = pool_ref?;
        let snapshot = store.snapshot();
        let resolved = snapshot.lookup(&pref.namespace, &pref.name)?;
        let local: Vec<&crate::policy::store::ResolvedWorkerSet> =
            resolved.local_destinations().collect();
        first_served_upstream(&local, canonical, |m| !self.resolve_pools(m).is_empty())
    }

    /// Whether any provider in the named pool declares this canonical model.
    ///
    /// Consulted only after `resolve_pools` came back empty, and it exists to keep
    /// the "nothing serves this model" gate honest. A provider is reachable ONLY
    /// through a pool's `destinations`, so without asking here a model that only a
    /// vendor serves would be refused before `policy_select` — the one place that
    /// can reach a provider — was ever called.
    ///
    /// Says nothing about whether the provider works. That is unknowable from
    /// here; see [`crate::policy::provider_health`].
    fn provider_serves(&self, pool_ref: Option<&crate::policy::PoolRef>, model: &str) -> bool {
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

    /// Resolve the `"auto"` pseudo-model to a concrete model for this
    /// prompt. kinference picks which real model should serve the request,
    /// and normal pool/endpoint selection proceeds against that choice.
    ///
    /// **Placeholder matching logic: uniform-random** among the models
    /// any pool currently serves (the `model_router` keys). This is the
    /// deliberate first step — prompt-aware selection will replace the
    /// body later. `messages` (the whole prompt) is already threaded in
    /// so that change won't touch the call site.
    ///
    /// Returns `None` when no concrete model is known yet (cold start
    /// before any worker has reported), which the caller maps to the
    /// same `NoWorkers` outcome as an unknown model.
    pub(crate) fn choose_model_for_auto(&self, _messages: &[serde_json::Value]) -> Option<String> {
        let router = self.model_router.read().unwrap();
        // Defensive filter: `AUTO_MODEL` is a pseudo-model and should
        // never be a key (no worker serves it), but never route "auto"
        // back to itself even if a config somehow introduced it.
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

    /// Accessor for the shared `Telemetry`. Exposed for the in-process
    /// pick path (it records picks under `picks.*` counters
    /// that share the same Arc as the Score path's `score.*` counters).
    pub(crate) fn telemetry(&self) -> &Arc<Telemetry> {
        &self.telemetry
    }
}

/// Associate `models` with `pool_id` in the reactive router. Called
/// from the metrics poller / warmup after a `/v1/models` scrape, and
/// provisionally from `handle_added` with the static `model_name` hint.
///
/// The router is an INDEX, not a partition: several pools may serve one model
/// and all of them are recorded. It used to keep a single owner per model name,
/// resolved by lowest-pool-id-wins, which silently made every other pool serving
/// that model unroutable — worker sets are independent, so "two of them serve
/// gemma" has to mean both are candidates. Ranking between them happens at pick
/// time, where the load information lives.
///
/// Claims are additive here and removed by [`release_models`] when a pool goes
/// away. Without that removal a departed pool's claim would be permanent.
/// Project a scraped sample onto a [`WorkerScore`].
///
/// The single place this projection happens. It used to be written out twice —
/// once on the success path in `score_workers` and once in
/// [`KinferenceService::degraded_response`] — so adding a scraped field and
/// updating only one site produced a score that was silently zero on the
/// tokenize-outage path alone: a bug that surfaces during an incident, which
/// is the worst time to find it.
///
/// `block_size` and the prefix-cache pair are parameters rather than read off
/// `entry`, because the degraded path deliberately zeroes the cache trio while
/// still reporting real load. Passing `block_size` explicitly also means a
/// reader does not have to prove it equals `entry.block_size` on the success
/// path (it does — `by_block` is keyed on it).
fn worker_score(
    entry: &WorkerEntry,
    sample: &crate::metrics::MetricsSample,
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

pub(crate) fn associate_models(
    router: &RwLock<HashMap<String, BTreeSet<PoolId>>>,
    pool_id: PoolId,
    pool_name: &str,
    models: &[String],
) {
    if models.is_empty() {
        return;
    }
    // Fast path: the steady state is "every model already claimed by this
    // pool", once per worker per `models_refresh_secs`. Taking the write lock
    // unconditionally there would momentarily block every concurrent pick.
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

/// Input variants `score_workers` accepts. After the Pick/Observe
/// migration only `Messages` is reached from in-tree callers (the
/// new Pick handler parses an OpenAI ChatCompletionRequest body). The
/// other variants stay defined so the function remains usable from
/// mock/test code and future internal callers without re-plumbing.
pub(crate) enum Input {
    #[allow(dead_code)]
    Raw(String),
    #[allow(dead_code)]
    Chat(String),
    /// OpenAI-style messages array, already parsed. Carries the
    /// `Vec<Value>` directly so the chat-template path doesn't pay
    /// a redundant serialize → reparse on every request.
    Messages(Vec<serde_json::Value>),
    #[allow(dead_code)]
    Tokens(Vec<u32>),
}

/// First upstream among `destinations`' alias tables that `is_served` accepts.
///
/// Split out of `Monitor::resolve_alias` so the decision is testable without a
/// live policy store and worker fleet: this is the part with the ordering rule
/// in it, and the wrapper is just where the tables come from.
///
/// Skips the canonical name itself, which `upstreams_for` appends as an identity
/// fallback — the caller only reaches here because no pool serves it.
///
/// Takes the FIRST match rather than collecting all of them, which is what makes
/// interchangeable upstream models resolve to one fixed choice instead of
/// becoming candidates. Pinned by
/// `several_upstreams_take_the_first_that_is_served`.
pub(crate) fn first_served_upstream(
    destinations: &[&crate::policy::store::ResolvedWorkerSet],
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
/// Since `model_router` became an index, a `Pick` naming no `InferencePool`
/// can resolve to several pools, and something has to choose. Each group is
/// scored by the best endpoint it offers under
/// [`crate::picker::predicted_wait_ms`] — the same term
/// [`Self::candidates_for`] hands the policy engine, so both paths agree on
/// what "better" means and a fleet does not reorder itself the day someone
/// attaches a policy to it.
///
/// Only the POOL is decided here. The winner's endpoint is chosen again
/// below, under that pool's `pick_lock`, with α-decay layering applied —
/// this ranking is deliberately the cheap lock-free approximation, for the
/// same reason the policy path ranks lock-free: taking two pools' locks to
/// compare them invites a deadlock between requests that take them in
/// opposite orders.
///
/// Groups whose scores are empty cannot win; `None` means no pool serving
/// the model had a usable worker.
fn best_group(
    groups: Vec<ScoredGroup>,
    req_label: &str,
    // `Option` so the tests can keep calling this without a registry. The
    // predictions are recorded HERE rather than at the call site because this
    // is where they are computed per endpoint; the caller only ever sees the
    // winning group.
    prom: Option<&crate::prom::PromMetrics>,
) -> Option<ScoredGroup> {
    let now = Instant::now();
    let multi = groups.len() > 1;
    let mut best: Option<(f32, ScoredGroup)> = None;
    for group in groups {
        let Some(wait) = group
            .scores
            .iter()
            .map(|s| {
                let summaries: Vec<crate::inflight_prefixes::InflightSummary> = group
                    .pool
                    .inflight_prefixes
                    .dispatches_for(&s.endpoint)
                    .iter()
                    .map(|d| d.summary())
                    .collect();
                let ms =
                    crate::picker::predicted_wait_ms(s, &group.pool.predictor, &summaries, now)
                        .total_ms();
                // Every endpoint considered, same as the policy path. A
                // prediction is a prediction whether or not a policy is
                // attached — the fold below keeps only the minimum for the
                // POOL choice, but all of them were computed and all of them
                // are values a `predictedTTFT` threshold would have seen.
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
/// Called when a pool leaves the fleet. Without it a departed pool's claims
/// would be permanent: `resolve_pools` would keep returning an id that resolves
/// to nothing, so a model whose only pool was replaced would 404 forever while
/// its replacement sat idle.
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

/// OpenAI ChatCompletionRequest fields beyond `messages` that the LOCAL
/// `minijinja` chat-template render path needs in order to produce
/// the same token sequence the engine would. Travels alongside
/// [`Input`] through [`KinferenceService::score_workers`] and lands
/// in [`crate::tokenize::TokenizeInput::Chat`].
///
/// Callers that don't have these (the gRPC `Score` path) pass
/// [`ChatExtras::default()`]: empty tools, `tool_choice = None`,
/// `add_generation_prompt = true` (the OpenAI default for
/// `/v1/chat/completions`).
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

/// Resolved input — either pre-supplied tokens (no /tokenize call
/// needed) or an input shape we hand to `tokenize_with_fallback`.
pub(crate) enum TokenSource {
    Direct(Vec<u32>),
    NeedsTokenize(TokenizeInput),
}

/// Outcome of [`KinferenceService::pick_and_record`]. Distinguishes
/// the four pick outcomes the gRPC `Pick()` handler
/// need to react to. Side effects (inflight increment, inflight-prefix
/// insert, pending-observation stash) have already been applied for
/// `Ok` / `Degraded`; `NoWorkers` / `BadRequest` are pure no-ops.
pub(crate) enum PickAndRecordOutcome {
    /// Successful pick. inflight + inflight-prefix + pending-observation
    /// state all written; caller should dispatch to `endpoint` and
    /// (eventually) report observed TTFT via `finalize_observation`.
    Ok {
        endpoint: String,
        response_id: u64,
        /// Rewritten request body, or EMPTY when Kinference left the body
        /// unchanged. Empty is an internal fast path, not a wire contract:
        /// `PullBody` always serves a complete body, so `grpc::pick` reuses
        /// the pushed `Arc` when this is empty instead of copying it. See
        /// [`crate::pick_body`].
        body: Vec<u8>,
        /// The model this request resolved to (post-`auto`). Surfaced as
        /// `PickResponse.requested_model`.
        model: String,
    },
    /// Tokenize was unavailable across the fleet; the pick was made on
    /// load metrics alone. `endpoint` is set and inflight state has
    /// still been written, so caller dispatches normally — only the
    /// prefix-cache part of the routing decision is missing.
    Degraded {
        endpoint: String,
        response_id: u64,
        reason: String,
        /// Rewritten request body; empty when unchanged (see `Ok::body`).
        body: Vec<u8>,
        /// The model this request resolved to (post-`auto`).
        model: String,
    },
    /// A policy chose an off-cluster provider. The body has been rewritten to
    /// name the vendor's model and stashed for `PullBody`, and a pending
    /// observation is held so a reported TTFT can be attributed — but NOTHING
    /// pool-scoped was touched, because there is no pool: no inflight count, no
    /// prefix insert, no critical section. `outcome=OK` on the wire, with a
    /// `ProviderTarget` in place of an endpoint.
    Provider {
        pick: ProviderPick,
        response_id: u64,
        /// Rewritten request body. Never empty here: a provider's model name
        /// differs from the canonical one by construction, so the body always
        /// changed (see `Ok::body` for why empty means "unchanged").
        body: Vec<u8>,
    },
    /// No ready worker serves the requested model. No state written.
    /// `model` carries the name the picker tried to route, read from the
    /// request body — surfaced verbatim in the response message so
    /// the operator can tell whether the request's model name
    /// matches what the fleet actually serves.
    /// `outcome=NO_WORKERS` on the wire.
    NoWorkers { model: String },
    /// A policy refused the request. No state written.
    ///
    /// Separate from `NoWorkers` because it says something different about the
    /// cluster: workers may be sitting idle and perfectly able to serve, and a
    /// rule declined to send this request to them. Reporting it as an absence
    /// sends the operator to look at Deployments over an edit to a policy.
    /// `outcome=POLICY_REJECTED` on the wire.
    PolicyRejected { reason: String, model: String },
    /// Body couldn't be parsed as an OpenAI ChatCompletionRequest, or
    /// `model` was empty in both the request and the body. No state
    /// written. `model` is the best-known name (often empty — the parse may
    /// have failed before one was found).
    BadRequest { reason: String, model: String },
}

mod lifecycle;
mod policy_driver;
mod provider_feedback;
mod scoring;

#[cfg(test)]
mod tests;
