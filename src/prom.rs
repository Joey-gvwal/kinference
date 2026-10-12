//! Prometheus exposition for kinference's own metrics — the counters we
//! surface on `GET /metrics` for an external Prometheus to scrape.
//!
//! This is the EXPOSITION side (kinference-as-source). It's unrelated to
//! `src/metrics.rs`, which is the SCRAPE side (kinference parsing each
//! vLLM/SGLang worker's `/metrics`).
//!
//! `PromMetrics` owns one `Registry` plus the typed metric families for the
//! request path. It's held in an `Arc` shared between the recording path (the
//! gRPC `report_first_token` / `report_final_token` handlers on
//! `KinferenceService`) and the exposition path (the dashboard's `/metrics` route).
//!
//! Adding a metric to a family THIS struct owns is three edits here: the
//! field, a `register` call in `new()`, and a recorder method. Prefer not to:
//! a subsystem with its own directory should own its own families and register
//! them via [`PromMetrics::registry`], so the metric lives in the file whose
//! owner maintains it. `/metrics` output is unaffected either way — see that
//! method's docs. The `/metrics` handler (`render`) never changes.

use std::collections::HashSet;
use std::sync::Mutex;

use prometheus::{
    Encoder, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder, exponential_buckets,
};

use crate::config::{DEFAULT_TENANT_LABEL, MetricsConfig, TenantLabels};

/// Inter-token-latency bucket layout version. MUST match
/// `ReportFinalTokenRequest.itl_bucket_layout` in `proto/kinference.proto`; bump
/// both together if the edges below change. Reports carrying a different
/// layout are skipped rather than mis-bucketed.
const ITL_BUCKET_LAYOUT: u32 = 1;

/// Prometheus `le` labels for ITL Layout 1, index-aligned to the client's
/// `itl_bucket_counts`. Upper edges (ms) `[5,10,15,20,30,40,50,75,100,150,
/// 200,300,500,1000]` plus the `+Inf` overflow bucket — see the proto's
/// Layout 1 doc, which this MUST stay in sync with.
const ITL_LE_LABELS: [&str; 15] = [
    "5", "10", "15", "20", "30", "40", "50", "75", "100", "150", "200", "300", "500", "1000",
    "+Inf",
];

// ── Closed label sets, pre-initialized at construction ────────────────────
//
// A `*Vec` child series does not exist until its label combination is first
// touched, and an ABSENT series is not the same as a zero one: `rate()` of a
// series that never existed returns nothing, so a panel draws no line and a
// threshold alert silently never fires. "Has never happened" and "is not
// happening" then look identical to anyone reading a dashboard, and only one
// of them is reassuring.
//
// Every set below is closed and small, so seeding all of it costs a bounded
// handful of series. The `{model,qos,tenant}` families are deliberately NOT
// seeded — their label values are open, and inventing series for them is how
// you invent cardinality.
//
// `kinference_workers{state}` already had this property by accident: `set_worker_states`
// writes both states on every call.

/// `kinference_picks_total{outcome}` and `kinference_pick_duration_ms{outcome}`.
/// Authoritative source: [`crate::telemetry::PickOutcome::as_label`].
const PICK_OUTCOMES: [&str; 3] = ["ok", "bad_request", "no_workers"];

/// `kinference_score_degraded_total{reason}`. Source: [`crate::monitor::Degraded::as_label`].
const DEGRADE_REASONS: [&str; 2] = ["tokenize", "prefix_index"];

/// `kinference_tokenize_total{path}`. Source: `crate::tokenize_client`; exactly one
/// per tokenize attempt.
const TOKENIZE_PATHS: [&str; 3] = ["local", "http_fallback", "unavailable"];

/// `kinference_tokenize_compare_total{result}`. Source: `crate::tokenize_client`.
/// Seeding these makes the zero honest: it still only moves in compare mode,
/// but now the panel says "no divergences" rather than showing nothing at all.
const TOKENIZE_COMPARE_RESULTS: [&str; 3] = ["match", "divergence", "error"];

/// `kinference_index_events_total{kind}`. Source: [`PromMetrics::record_index_events`].
const INDEX_EVENT_KINDS: [&str; 3] = ["stored", "removed", "cleared"];

/// `kinference_body_rejected_total{reason}`.
/// Source: [`crate::body_store::RejectReason::as_label`].
const BODY_REJECT_REASONS: [&str; 3] = ["declared_too_large", "request_too_large", "server_full"];

/// `kinference_body_aborted_total{reason}`. Source: `crate::grpc::abort_reason_label`.
const BODY_ABORT_REASONS: [&str; 5] = [
    "unspecified",
    "not_dispatched",
    "pull_failed",
    "backend_unresolved",
    "backend_unauthorized",
];

/// `kinference_policy_decisions_total{outcome}`.
///
/// The one set here with NO enum behind it — the call sites in
/// `monitor::policy_driver` and `monitor::lifecycle` pass string literals, so
/// this list is the only enumeration that exists and `prom::tests` cannot
/// check it against anything. Adding an outcome means adding it here too.
const POLICY_DECISION_OUTCOMES: [&str; 9] = [
    "chosen",
    "unmatched_served",
    "unmatched_empty",
    "rejected_unmatched",
    "rejected_on_empty",
    "unknown_pool",
    "not_listed",
    "no_destinations",
    "forced_endpoint_missing",
];

/// `kinference_worker_evictions_total{reason}`.
/// Source: `crate::eviction::EvictionReason::as_label`.
///
/// Counts only evictions kinference DECIDED on. A pod that simply went away arrives
/// as `WorkerChange::Removed` with no reason and is logged "Removed worker" at
/// INFO — normal churn, deliberately not counted here, or a rolling restart
/// would look like a fault.
const EVICTION_REASONS: [&str; 2] = ["block_size_mismatch", "model_mismatch"];

/// `kinference_workerset_rejected_total{reason}`.
/// Source: `crate::policy::worker_sets::RejectReason::as_condition_reason`.
/// CamelCase, unlike every other reason label — these double as Kubernetes
/// condition reasons, which is the convention that wins.
const WORKERSET_REJECT_REASONS: [&str; 7] = [
    "MissingProfile",
    "UnknownProfile",
    "MissingKvBlockSize",
    "InvalidKvBlockSize",
    "MissingServiceRef",
    "ModelMissingName",
    "KvEventsMissingPort",
];

/// kinference's Prometheus metric set. Clone-free: share via `Arc`.
///
/// The per-request families are labeled by `model` and `qos`, plus a `tenant`
/// resolved through [`Self::tenant_label`] — see [`TenantPolicy`] for why that
/// is not simply the value the caller sent.
pub struct PromMetrics {
    registry: Registry,
    /// How to label the `tenant` dimension. Resolved once at construction from
    /// `[metrics] tenant_labels`.
    tenants: TenantPolicy,
    /// `kinference_prompt_tokens_total{model,qos}` — prompt tokens, from ReportFinalToken.
    prompt_tokens: IntCounterVec,
    /// `kinference_completion_tokens_total{model,qos}` — completion tokens, from ReportFinalToken.
    completion_tokens: IntCounterVec,
    /// `kinference_cached_prompt_tokens_total{model,qos,tenant}` — prompt tokens the
    /// ENGINE served from its prefix cache.
    cached_prompt_tokens: IntCounterVec,
    /// `kinference_cached_tokens_absent_total` — reports that omitted the field.
    cached_tokens_absent: IntCounter,
    /// `kinference_cached_tokens_reported_total` — reports that carried it.
    cached_tokens_reported: IntCounter,
    /// `kinference_orphaned_reports_total` — final-token reports kinference could not
    /// attribute to a pick.
    orphaned_reports: IntCounter,
    /// `kinference_prompt_length_tokens{model,qos}` — distribution of per-request
    /// prompt (input) token counts. The counter above is the running total;
    /// this is the SHAPE (p50/p90/p99 via `histogram_quantile`).
    prompt_length: HistogramVec,
    /// `kinference_completion_length_tokens{model,qos}` — distribution of per-request
    /// completion (output) token counts. Same buckets as `prompt_length`.
    completion_length: HistogramVec,
    /// `kinference_request_duration_ms{model,qos,tenant}` — pick → ReportFinalToken.
    request_duration_ms: HistogramVec,
    /// `kinference_ttft_ms{model,qos}` — time-to-first-token distribution in
    /// milliseconds, from `ReportFirstToken`. STREAMING requests only: for
    /// non-streaming responses the reported time is total latency, not a TTFT.
    ttft_ms: HistogramVec,
    /// `kinference_provider_ttft_ms{provider,model}` — time-to-first-token for
    /// OFF-CLUSTER picks, in milliseconds, from the caller's `ReportFirstToken`.
    ///
    /// Separate from `ttft_ms` rather than a label on it, because a provider pick
    /// holds no `PendingObservation` — there is no pool, so nothing pool-scoped
    /// to release — and `record_ttft` is called from `finalize_observation`,
    /// which a provider never reaches. Without this, provider latency appears in
    /// no histogram at all.
    ///
    /// This is the only operator-visible reading of a provider's latency. The
    /// router's own p95 (the `EstimatedTTFT` ranking input, and what a
    /// `backend.observedTTFTP95` predicate compares against) is computed on the
    /// routing path and never published, so `histogram_quantile(0.95, sum by (le,
    /// provider) (rate(kinference_provider_ttft_ms_bucket[5m])))` is how you see the
    /// figure a policy is being written against.
    ///
    /// SUCCESSFUL reports only: a 401 or a 5xx carries a time-to-error, and
    /// mixing that into a latency distribution would make a failing vendor look
    /// fast. Those land on the provider's `Suppressed`/`Backend*` state instead.
    ///
    /// No `qos` label, unlike `ttft_ms`. Vendor latency is not something a QoS
    /// class changes, and provider × model is already the split that acts.
    provider_ttft_ms: HistogramVec,
    /// `kinference_itl_ms_bucket{model,qos,le}` — CUMULATIVE inter-token-latency
    /// bucket counts (ms), summed fleet-wide from each request's per-bucket
    /// counts (see `record_itl`). A hand-aggregated Prometheus histogram: the
    /// crate's native `Histogram` can't ingest pre-bucketed counts, so this is
    /// an `IntCounterVec` with an `le` label. `histogram_quantile` works on it;
    /// there is no `_sum` (the client sends counts, not raw gap values).
    itl_bucket: IntCounterVec,
    /// `kinference_itl_ms_count{model,qos}` — total ITL gap samples (equals the
    /// `le="+Inf"` bucket). Companion to `itl_bucket`.
    itl_count: IntCounterVec,
    // --- Body buffer (PushBody → Pick → PullBody). ---
    //
    // Deliberately NOT labeled by {model,qos} like everything above: no model
    // is known at push time, and these describe server memory rather than
    // per-model traffic.
    /// `kinference_body_buffer_bytes` — bytes currently held (uploads in progress +
    /// bodies awaiting a pull). Watch against `body_max_total_kb`, which is
    /// configured in KB while this reports bytes.
    body_buffer_bytes: IntGauge,
    /// `kinference_body_buffer_entries` — buffered bodies currently tracked.
    body_buffer_entries: IntGauge,
    /// `kinference_body_pushed_total` / `..._bytes_total` — completed pushes.
    body_pushed: IntCounter,
    body_pushed_bytes: IntCounter,
    /// `kinference_body_rejected_total{reason}` — pushes refused over budget. A
    /// rising count is the signal the buffer is under-provisioned, as opposed
    /// to the process being OOM-killed.
    body_rejected: IntCounterVec,
    /// `kinference_body_reaped_total` — bodies reclaimed by the TTL sweep, i.e.
    /// callers that vanished without aborting.
    body_reaped: IntCounter,
    /// `kinference_body_aborted_total{reason}` — explicit `Abort` calls.
    body_aborted: IntCounterVec,
    // --- CRD routing policy. Not {model,qos}-labeled: these describe the
    // policy machinery, and `rule` is already the useful dimension.
    /// `kinference_picks_total{outcome}` — every Pick() by wire outcome.
    picks: IntCounterVec,
    /// `kinference_pick_duration_ms{outcome}` — the router's own decision latency.
    pick_duration_ms: HistogramVec,
    /// `kinference_tokenize_total{path}` — how each request got tokenized.
    tokenize_path: IntCounterVec,
    /// `kinference_tokenize_duration_ms` — tokenize latency, all paths.
    tokenize_duration_ms: Histogram,
    /// `kinference_tokenize_compare_total{result}` — compare-mode agreement.
    tokenize_compare: IntCounterVec,
    /// `kinference_workers{state}` — worker reachability.
    workers: IntGaugeVec,
    /// `kinference_worker_evictions_total{reason}` — workers kinference removed itself.
    worker_evictions: IntCounterVec,
    /// `kinference_workers_by_model{model,state}` — the same reachability, split by
    /// served model. A SEPARATE family rather than a label on `workers`,
    /// because a worker serving several models (LoRA adapters) is counted once
    /// per model here: summing this across models would exceed the fleet size,
    /// and `kinference_workers` must stay the authoritative total.
    workers_by_model: IntGaugeVec,
    /// Models `workers_by_model` has emitted. A model whose worker set is
    /// deleted stops appearing in the input, and a gauge left alone would hold
    /// its last non-zero value forever; this lets the recorder zero it instead.
    workers_by_model_seen: Mutex<HashSet<String>>,
    /// `kinference_dashboard_slots_used` / `_capacity` — the dashboard's arena.
    dashboard_slots_used: IntGauge,
    dashboard_slots_capacity: IntGauge,
    /// `kinference_dashboard_slots_denied_total` — workers the dashboard omits.
    dashboard_slots_denied: IntCounter,
    /// `kinference_index_blocks` — blocks in the radix tree, fleet-wide.
    index_blocks: IntGauge,
    /// `kinference_index_workers` — workers present in the radix tree, fleet-wide.
    index_workers: IntGauge,
    /// `kinference_index_events_total{kind}` — KV events applied, by kind.
    index_events: IntCounterVec,
    /// `kinference_index_blocks_stored_total` / `_removed_total` — block churn.
    index_blocks_stored: IntCounter,
    index_blocks_removed: IntCounter,
    /// `kinference_prefix_requested_blocks_total{model,qos}` — prompt blocks scored.
    prefix_requested_blocks: IntCounterVec,
    /// `kinference_prefix_matched_blocks_total{model,qos}` — LAYERED overlap.
    prefix_matched_blocks: IntCounterVec,
    /// `kinference_prefix_radix_matched_blocks_total{model,qos}` — RAW radix match.
    prefix_radix_matched_blocks: IntCounterVec,
    /// `kinference_prefix_match_ratio{model,qos}` — layered / requested, per pick.
    prefix_match_ratio: HistogramVec,
    /// `kinference_prefix_overmatch_total{model,qos}` — picks where matched > requested.
    prefix_overmatch: IntCounterVec,
    /// `kinference_score_degraded_total{reason}`.
    score_degraded: IntCounterVec,
    /// `kinference_workerset_rejected_total{reason}`.
    workerset_rejected: IntCounterVec,
    /// `kinference_policy_decisions_total{outcome}`.
    policy_decisions: IntCounterVec,
    /// `kinference_routed_requests_total{tenant,destination,type}` — picks that
    /// chose somewhere to send the request: which worker set or provider each
    /// tenant's traffic went to, and so whether it stayed on the cluster. The
    /// token families cannot say: they are labelled by the model each request
    /// was sent with, and a model name does not say where it ran.
    routed_requests: IntCounterVec,
    // --- Leader election. See `crate::leader`.
    /// `kinference_leader` — 1 when this replica holds the Lease, else 0.
    ///
    /// The primary signal for the whole feature. Exactly one pod in a
    /// deployment should report 1; NO pod reporting 1 means either election is
    /// broken or a foreign deployment holds the lease under the same name
    /// (check the Lease's `holderIdentity`).
    leader: IntGauge,
    /// `kinference_leader_transitions_total` — leadership changes observed by THIS
    /// replica. A steady rate means flapping, which costs a full status
    /// re-assert on every promotion.
    leader_transitions: IntCounter,
    /// `kinference_leader_election_errors_total` — failed Lease reads/writes.
    ///
    /// Matters because election fails CLOSED: a replica that cannot reach the
    /// Lease stays a follower and writes no status, so `Accepted=True` never
    /// appears and a conforming gateway never programs a route. Rising here
    /// with `kinference_leader` flat at 0 everywhere is that failure.
    leader_election_errors: IntCounter,
    /// `kinference_policy_rule_matches_total{rule}` — which rules are actually live.
    policy_rule_matches: IntCounterVec,
    /// `kinference_policy_revision_mismatch_total` — the gateway routed on a pool
    /// revision we don't hold. Rising means our watch lags the gateway.
    policy_revision_mismatch: IntCounter,
    /// `kinference_policy_warnings{policy,code}` — how many findings each
    /// policy carries, by kind. The same list `status.warnings` shows, as a
    /// number a dashboard or alert can watch: a policy that picks up a warning
    /// on its next edit is otherwise visible only to someone who reads its
    /// status. Cardinality is bounded by policies × warning codes.
    policy_warnings: IntGaugeVec,
    /// `kinference_worker_queued_prompts_mean30s{worker,model}` and its KV sibling —
    /// the two signals `InferencePolicy` thresholds on, published PER WORKER so
    /// a threshold can be calibrated against what the fleet actually does
    /// rather than guessed.
    ///
    /// The only per-worker families here, and the cardinality is deliberate:
    /// two series per (worker, served model), bounded by the fleet, against
    /// ~91 per (model, qos, tenant) already accepted for the request families.
    /// A `worker` label is the point — an aggregate cannot tell you that one
    /// endpoint sits at 95% while three idle, which is exactly the case a
    /// `drop` rule exists to catch.
    worker_queued_prompts_mean: GaugeVec,
    worker_kv_cache_usage_mean: GaugeVec,
    /// `kinference_predicted_ttft_ms{worker,model}` — the distribution of values
    /// `predictedTTFT` compares against, so that threshold is calibratable the
    /// same way the two gauges above are.
    ///
    /// Observed for EVERY candidate the policy engine scored, not just the
    /// winner. That distinction is the whole point: a threshold is applied to
    /// every candidate, and the winner's distribution is biased low precisely
    /// because it won. Mixing the two would flatter the p95.
    ///
    /// Recorded by exactly ONE ranker per request — `policy_driver`'s
    /// `candidates_for` when a policy is in play, `monitor::best_group`
    /// otherwise. Both predict per endpoint; recording in both would
    /// double-count the winning group and weight the distribution toward the
    /// fastest one.
    predicted_ttft_ms: HistogramVec,
}

/// Build a per-(model,qos) token-count histogram (a `length` distribution)
/// with the shared bucket layout: exponential factor-2 from 8 to ~262k tokens
/// — wide enough for tiny prompts through long-context / long-generation
/// requests. Used for both the prompt- and completion-length histograms so
/// the two stay directly comparable.
fn length_histogram(name: &str, help: &str) -> HistogramVec {
    HistogramVec::new(
        HistogramOpts::new(name, help).buckets(
            exponential_buckets(8.0, 2.0, 16).expect("length histogram buckets are valid"),
        ),
        &["model", "qos", "tenant"],
    )
    .expect("length histogram opts are valid")
}

/// The resolved form of `[metrics] tenant_labels`.
///
/// Distinct from [`crate::config::TenantLabels`] only in that the allowlist is
/// a `HashSet`: `tenant_label` runs once per request per family, so the lookup
/// is on the hot path and a linear scan over a config `Vec` is the wrong shape.
enum TenantPolicy {
    /// Everything buckets under [`DEFAULT_TENANT_LABEL`].
    None,
    /// Every tenant seen becomes its own label value.
    All,
    Allow(HashSet<String>),
}

impl From<&TenantLabels> for TenantPolicy {
    fn from(l: &TenantLabels) -> Self {
        match l {
            TenantLabels::None => Self::None,
            TenantLabels::All => Self::All,
            TenantLabels::Allow(v) => Self::Allow(v.iter().cloned().collect()),
        }
    }
}

impl PromMetrics {
    /// Tenant labelling OFF — every request buckets under
    /// [`DEFAULT_TENANT_LABEL`]. This is the `[metrics]` default, so it is also
    /// what every test wants; [`Self::with_config`] is the configured path and
    /// is used only by `main`.
    pub fn new() -> Self {
        Self::with_tenants(TenantPolicy::None)
    }

    /// Built from `[metrics]`. Warns once if tenant labelling is unbounded,
    /// for the same reason `NoopRateLimiter::warn_on_startup` does: a config
    /// that quietly does something expensive should say so.
    pub fn with_config(cfg: &MetricsConfig) -> Self {
        if matches!(cfg.tenant_labels, TenantLabels::All) {
            tracing::warn!(
                "[metrics] tenant_labels = \"all\": every tenant id seen becomes its own \
                 label value, which is UNBOUNDED — about 91 Prometheus series per \
                 (model, qos, tenant). A caller that sends a unique tenant per request \
                 will grow the registry without limit. Use an explicit allowlist to bound it."
            );
        }
        Self::with_tenants(TenantPolicy::from(&cfg.tenant_labels))
    }

    fn with_tenants(tenants: TenantPolicy) -> Self {
        let registry = Registry::new();
        // Build identity, in the conventional Prometheus shape: a gauge pinned
        // to 1 whose LABELS carry the information. The value is meaningless on
        // its own — what it buys is `group_left`, so any other series can be
        // joined to the version that produced it, and an alert on more than one
        // version being live during a rollout.
        //
        // Set here rather than through a recorder because `VERSION` is resolved
        // at compile time; there is no later moment at which it could change.
        let build_info = IntGaugeVec::new(
            Opts::new(
                "kinference_build_info",
                "Build identity of this router process. Always 1; read the labels. \
                 Matches the `version` field on /api/snapshot and the `--version` \
                 output, so a scrape and a human agree on which build is running",
            ),
            &["version"],
        )
        .expect("build_info gauge opts are valid");
        build_info
            .with_label_values(&[crate::version::VERSION])
            .set(1);
        let prompt_tokens = IntCounterVec::new(
            Opts::new(
                "kinference_prompt_tokens_total",
                "Prompt (input) tokens by served model and QoS class",
            ),
            &["model", "qos", "tenant"],
        )
        .expect("prompt_tokens counter opts are valid");
        let completion_tokens = IntCounterVec::new(
            Opts::new(
                "kinference_completion_tokens_total",
                "Completion (output) tokens by served model and QoS class",
            ),
            &["model", "qos", "tenant"],
        )
        .expect("completion_tokens counter opts are valid");
        // Prompt + completion length share one bucket layout (see
        // `length_histogram`) so the distributions are directly comparable.
        let cached_prompt_tokens = IntCounterVec::new(
            Opts::new(
                "kinference_cached_prompt_tokens_total",
                "Prompt tokens the ENGINE reports it served from its own prefix \
                 cache, from ReportFinalToken. Measured by the engine, not \
                 predicted by the router: subtract from kinference_prompt_tokens_total \
                 for the prompt tokens that actually cost prefill compute",
            ),
            &["model", "qos", "tenant"],
        )
        .expect("cached_prompt_tokens counter opts are valid");
        let cached_tokens_absent = IntCounter::new(
            "kinference_cached_tokens_absent_total",
            "Final-token reports that carried no cached_tokens field. Those \
             requests contribute nothing to kinference_cached_prompt_tokens_total, so \
             a non-zero value means the cached figure UNDERSTATES the true cache \
             hit and any billing derived from it OVERCHARGES. Unlabelled on \
             purpose: it answers whether the basis is complete, not whose",
        )
        .expect("cached_tokens_absent counter opts are valid");
        let cached_tokens_reported = IntCounter::new(
            "kinference_cached_tokens_reported_total",
            "Final-token reports that DID carry cached_tokens. The companion \
             denominator to kinference_cached_tokens_absent_total: the two partition \
             every report, so absent/(absent+reported) is the exact share of \
             traffic missing from the cached-token basis. Both are unlabelled \
             single series on purpose — a ratio built from a labelled family \
             instead is a sum-of-rates over many sparse children, which does not \
             equal the rate of their sum and drifts above 100%",
        )
        .expect("cached_tokens_reported counter opts are valid");
        let orphaned_reports = IntCounter::new(
            "kinference_orphaned_reports_total",
            "Final-token reports whose response_id kinference never issued, or whose \
             labels aged out of the 600s TTL. Their model comes from the request \
             field rather than the pick, their tenant falls back to the default \
             bucket, and their qos is reported as `unknown` — so a non-zero value \
             means that much of every per-request family is misattributed. \
             Unlabelled: it answers whether attribution is intact, not whose",
        )
        .expect("orphaned_reports counter opts are valid");
        let prompt_length = length_histogram(
            "kinference_prompt_length_tokens",
            "Distribution of per-request prompt (input) token counts, by model and QoS",
        );
        let completion_length = length_histogram(
            "kinference_completion_length_tokens",
            "Distribution of per-request completion (output) token counts, by model and QoS",
        );
        // TTFT is reported in milliseconds (matching `observed_ttft_ms` and
        // the offline latency analysis), with curated latency buckets
        // spanning 10ms to 80s.
        let request_duration_ms = HistogramVec::new(
            HistogramOpts::new(
                "kinference_request_duration_ms",
                "Router-observed end-to-end request duration in milliseconds: the \
                 interval from the pick to the caller's ReportFinalToken. Includes \
                 the caller's own handling and the report's network hop, so it is \
                 NOT client-observed latency. Only requests that report are counted",
            )
            // The `ttft_ms` edges through 80s, so the two are directly
            // comparable where they overlap and `duration - ttft` reads as
            // decode time, plus three longer buckets because a full generation
            // routinely outruns any TTFT.
            .buckets(vec![
                10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0,
                40000.0, 80000.0, 160000.0, 300000.0, 600000.0,
            ]),
            &["model", "qos", "tenant"],
        )
        .expect("request_duration_ms histogram opts are valid");
        let ttft_ms = HistogramVec::new(
            HistogramOpts::new(
                "kinference_ttft_ms",
                "Time-to-first-token in milliseconds, by model and QoS (streaming requests only)",
            )
            .buckets(vec![
                10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0,
                40000.0, 80000.0,
            ]),
            &["model", "qos", "tenant"],
        )
        .expect("ttft_ms histogram opts are valid");
        // Same buckets as `ttft_ms` on purpose: the two are read side by side
        // when deciding whether to spill off-cluster, and different bucket edges
        // would make that comparison quietly wrong.
        let provider_ttft_ms = HistogramVec::new(
            HistogramOpts::new(
                "kinference_provider_ttft_ms",
                "Time-to-first-token for off-cluster provider picks, in milliseconds, \
                 by provider and model (successful reports only)",
            )
            .buckets(vec![
                10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0,
                40000.0, 80000.0,
            ]),
            &["provider", "model"],
        )
        .expect("provider_ttft_ms histogram opts are valid");
        // ITL: a hand-aggregated histogram fed from the client's per-request
        // bucket counts (approach B). Counters, so Prometheus windows them via
        // rate(); `histogram_quantile` reads the `_bucket`/`le` series.
        let itl_bucket = IntCounterVec::new(
            Opts::new(
                "kinference_itl_ms_bucket",
                "Cumulative inter-token-latency bucket counts (ms), by model and QoS",
            ),
            &["model", "qos", "le", "tenant"],
        )
        .expect("itl_bucket counter opts are valid");
        let itl_count = IntCounterVec::new(
            Opts::new(
                "kinference_itl_ms_count",
                "Total inter-token-latency gap samples, by model and QoS",
            ),
            &["model", "qos", "tenant"],
        )
        .expect("itl_count counter opts are valid");
        // Body-buffer stats. First gauges in this registry — everything else
        // here is a monotonic counter or histogram.
        let body_buffer_bytes = IntGauge::new(
            "kinference_body_buffer_bytes",
            "Request-body bytes currently buffered (uploads in progress + bodies awaiting PullBody)",
        )
        .expect("body_buffer_bytes gauge opts are valid");
        let body_buffer_entries = IntGauge::new(
            "kinference_body_buffer_entries",
            "Request bodies currently tracked by the body store",
        )
        .expect("body_buffer_entries gauge opts are valid");
        let leader = IntGauge::new(
            "kinference_leader",
            "1 when this replica holds the leader Lease and writes CRD status, else 0",
        )
        .expect("leader gauge opts are valid");
        let leader_transitions = IntCounter::new(
            "kinference_leader_transitions_total",
            "Leadership transitions observed by this replica",
        )
        .expect("leader_transitions counter opts are valid");
        let leader_election_errors = IntCounter::new(
            "kinference_leader_election_errors_total",
            "Failed leader-election Lease reads or writes",
        )
        .expect("leader_election_errors counter opts are valid");
        let body_pushed = IntCounter::new(
            "kinference_body_pushed_total",
            "Request bodies successfully pushed and buffered",
        )
        .expect("body_pushed counter opts are valid");
        let body_pushed_bytes = IntCounter::new(
            "kinference_body_pushed_bytes_total",
            "Total request-body bytes accepted by PushBody",
        )
        .expect("body_pushed_bytes counter opts are valid");
        let body_rejected = IntCounterVec::new(
            Opts::new(
                "kinference_body_rejected_total",
                "PushBody calls refused because a buffer budget was exceeded",
            ),
            &["reason"],
        )
        .expect("body_rejected counter opts are valid");
        let body_reaped = IntCounter::new(
            "kinference_body_reaped_total",
            "Buffered bodies reclaimed by the TTL sweep (caller vanished without aborting)",
        )
        .expect("body_reaped counter opts are valid");
        let body_aborted = IntCounterVec::new(
            Opts::new("kinference_body_aborted_total", "Abort calls, by reason"),
            &["reason"],
        )
        .expect("body_aborted counter opts are valid");
        let workerset_rejected = IntCounterVec::new(
            Opts::new(
                "kinference_workerset_rejected_total",
                "InferenceWorkerSets that produced no pool, by why",
            ),
            &["reason"],
        )
        .expect("workerset_rejected counter opts are valid");
        let picks = IntCounterVec::new(
            Opts::new(
                "kinference_picks_total",
                "Pick() calls by wire outcome: ok, bad_request (the body did not \
                 parse or carried nothing to route on), no_workers (nothing serves \
                 the requested model). The router's top-line request outcome — \
                 distinct from kinference_policy_decisions_total, which is about CRD \
                 policy resolution rather than the result returned to the caller",
            ),
            &["outcome"],
        )
        .expect("picks counter opts are valid");
        let pick_duration_ms = HistogramVec::new(
            HistogramOpts::new(
                "kinference_pick_duration_ms",
                "How long the router took to answer a Pick(), in milliseconds. This \
                 is kinference's OWN service latency, not the model's — it covers \
                 scoring and any tokenize round-trip, and ends when the response is \
                 built. Labeled by outcome because a rejected pick is fast and \
                 would otherwise flatter the distribution",
            )
            .buckets(vec![
                0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0,
            ]),
            &["outcome"],
        )
        .expect("pick_duration_ms histogram opts are valid");
        let tokenize_path = IntCounterVec::new(
            Opts::new(
                "kinference_tokenize_total",
                "How each request got tokenized: local (the in-process tokenizer \
                 served it), http_fallback (local failed, the engine's /render \
                 saved it), unavailable (both failed — the pick is DEGRADED and \
                 routes on load alone). Exactly one per tokenize attempt, so the \
                 three sum to the total",
            ),
            &["path"],
        )
        .expect("tokenize_path counter opts are valid");
        let tokenize_duration_ms = Histogram::with_opts(
            HistogramOpts::new(
                "kinference_tokenize_duration_ms",
                "Tokenize latency in milliseconds, across every path. On the Pick \
                 critical path, so it is a component of kinference_pick_duration_ms",
            )
            .buckets(vec![
                0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
            ]),
        )
        .expect("tokenize_duration_ms histogram opts are valid");
        let tokenize_compare = IntCounterVec::new(
            Opts::new(
                "kinference_tokenize_compare_total",
                "Compare-mode agreement between the local tokenizer and the engine's \
                 /render: match, divergence, error. A DIVERGENCE means every prefix \
                 block hash is wrong, so cache-affinity routing silently stops \
                 working while the prefix metrics still report plausible numbers. \
                 Only ever non-zero when tokenize.mode = compare",
            ),
            &["result"],
        )
        .expect("tokenize_compare counter opts are valid");
        let dashboard_slots_used = IntGauge::new(
            "kinference_dashboard_slots_used",
            "Dashboard display slots occupied. The pool is preallocated at startup \
             and sized by `dashboard_max_workers`",
        )
        .expect("dashboard_slots_used gauge opts are valid");
        let dashboard_slots_capacity = IntGauge::new(
            "kinference_dashboard_slots_capacity",
            "Dashboard slot pool size, from `dashboard_max_workers`. Compare against \
             kinference_dashboard_slots_used for headroom",
        )
        .expect("dashboard_slots_capacity gauge opts are valid");
        let dashboard_slots_denied = IntCounter::new(
            "kinference_dashboard_slots_denied_total",
            "Workers not shown on the live dashboard for want of a display slot. \
             They route normally and are still counted by kinference_workers — only \
             the demo dashboard omits them. EXPECTED whenever the fleet is \
             larger than dashboard_max_workers; it is not an error condition",
        )
        .expect("dashboard_slots_denied counter opts are valid");
        let workers = IntGaugeVec::new(
            Opts::new(
                "kinference_workers",
                "Workers by reachability: ok, or unresponsive when its /metrics \
                 scrape has been stale for over 5s. An unresponsive worker is still \
                 routable — it is scored on its last known sample",
            ),
            &["state"],
        )
        .expect("workers gauge opts are valid");
        let worker_evictions = IntCounterVec::new(
            Opts::new(
                "kinference_worker_evictions_total",
                "Workers kinference evicted itself, by reason. An eviction means the \
                 worker was discovered and healthy but disagreed with the pool \
                 about something the pool cannot vary per worker — so it is \
                 removed to keep the rest serving on coherent assumptions. \
                 Normal discovery removals are NOT counted: a rolling restart \
                 would otherwise read as a fault. Non-zero means a CONFIG \
                 mismatch; the WARN log carries both values and the fix",
            ),
            &["reason"],
        )
        .expect("worker_evictions counter opts are valid");
        let workers_by_model = IntGaugeVec::new(
            Opts::new(
                "kinference_workers_by_model",
                "Worker reachability by served model and state. A worker serving \
                 several models is counted under EACH, so summing across models \
                 can exceed the fleet — use kinference_workers for the total",
            ),
            &["model", "state"],
        )
        .expect("workers_by_model gauge opts are valid");
        let index_blocks = IntGauge::new(
            "kinference_index_blocks",
            "Blocks currently in the prefix radix tree, summed across pools. \
             Refreshed on a THROTTLED tree dump (roughly every 10s), not per \
             scrape, because the dump is O(tree)",
        )
        .expect("index_blocks gauge opts are valid");
        let index_workers = IntGauge::new(
            "kinference_index_workers",
            "Workers appearing in the prefix radix tree, summed across pools. \
             Compare against the configured worker count: a worker missing here \
             can never win a cache-affinity pick, so it silently gets no prefix \
             credit. Same throttled refresh as kinference_index_blocks",
        )
        .expect("index_workers gauge opts are valid");
        let index_events = IntCounterVec::new(
            Opts::new(
                "kinference_index_events_total",
                "KV-cache events applied to the radix tree, by kind (stored, \
                 removed, cleared)",
            ),
            &["kind"],
        )
        .expect("index_events counter opts are valid");
        let index_blocks_stored = IntCounter::new(
            "kinference_index_blocks_stored_total",
            "Blocks added to the radix tree",
        )
        .expect("index_blocks_stored counter opts are valid");
        let index_blocks_removed = IntCounter::new(
            "kinference_index_blocks_removed_total",
            "Blocks evicted from the radix tree",
        )
        .expect("index_blocks_removed counter opts are valid");
        let prefix_requested_blocks = IntCounterVec::new(
            Opts::new(
                "kinference_prefix_requested_blocks_total",
                "Prompt blocks scored for prefix reuse, by served model and QoS. The \
                 shared denominator for both matched-block counters",
            ),
            &["model", "qos"],
        )
        .expect("prefix_requested_blocks counter opts are valid");
        let prefix_matched_blocks = IntCounterVec::new(
            Opts::new(
                "kinference_prefix_matched_blocks_total",
                "Prompt blocks the router expected the CHOSEN worker to already hold: \
                 the layered overlap it actually scored on, radix match plus credit \
                 for blocks in-flight requests are still prefilling",
            ),
            &["model", "qos"],
        )
        .expect("prefix_matched_blocks counter opts are valid");
        let prefix_radix_matched_blocks = IntCounterVec::new(
            Opts::new(
                "kinference_prefix_radix_matched_blocks_total",
                "Prompt blocks the radix tree alone reported cached on the chosen \
                 worker. Same picks as the layered counter, so the difference between \
                 them is the credit given to in-flight prefill",
            ),
            &["model", "qos"],
        )
        .expect("prefix_radix_matched_blocks counter opts are valid");
        let prefix_match_ratio = HistogramVec::new(
            HistogramOpts::new(
                "kinference_prefix_match_ratio",
                "Per-pick layered prefix overlap as a fraction of the prompt. The \
                 counters give the volume-weighted rate; this gives the SHAPE, which \
                 is the actionable part — a fleet averaging 0.45 may be uniformly \
                 mediocre or bimodal, and those want opposite responses",
            )
            // Linear, not exponential: the interesting structure is at BOTH ends
            // (cold misses and full hits), not concentrated near zero.
            .buckets(vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]),
            &["model", "qos"],
        )
        .expect("prefix_match_ratio histogram opts are valid");
        let prefix_overmatch = IntCounterVec::new(
            Opts::new(
                "kinference_prefix_overmatch_total",
                "Picks where matched blocks exceeded the prompt's block count. Real, \
                 not defensive: overlap scores are summed across data-parallel ranks, \
                 so a DP>1 worker publishing one prefix from two ranks double-counts. \
                 The ratio is clamped to 1.0; this counts how often that happened",
            ),
            &["model", "qos"],
        )
        .expect("prefix_overmatch counter opts are valid");
        let score_degraded = IntCounterVec::new(
            Opts::new(
                "kinference_score_degraded_total",
                "Picks that fell back to load-only scoring, by which subsystem failed",
            ),
            &["reason"],
        )
        .expect("score_degraded counter opts are valid");
        let policy_decisions = IntCounterVec::new(
            Opts::new(
                "kinference_policy_decisions_total",
                "CRD routing-policy decisions, by outcome",
            ),
            &["outcome"],
        )
        .expect("policy_decisions counter opts are valid");
        let routed_requests = IntCounterVec::new(
            Opts::new(
                "kinference_routed_requests_total",
                "Picks that chose a destination, by tenant, destination (worker set or \
                 provider) and its type (Local or Provider)",
            ),
            &["tenant", "destination", "type"],
        )
        .expect("routed_requests counter opts are valid");
        let policy_rule_matches = IntCounterVec::new(
            Opts::new(
                "kinference_policy_rule_matches_total",
                "InferencePolicy rules that produced a routing decision, by rule name",
            ),
            &["rule"],
        )
        .expect("policy_rule_matches counter opts are valid");
        let predicted_ttft_ms = HistogramVec::new(
            HistogramOpts::new(
                "kinference_predicted_ttft_ms",
                "Predicted time-to-first-token in milliseconds, per candidate scored by \
                 an InferencePolicy, by worker and model. The exact values a \
                 `drop: {predictedTTFT: ...}` clause compares against — every candidate, \
                 not just the winner. Recorded whether or not a policy is attached",
            )
            // Deliberately the same buckets as `kinference_ttft_ms`: the predicted
            // and the observed distributions are meant to be read against each
            // other, and different bucket edges would make that a guess.
            .buckets(vec![
                10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0,
                40000.0, 80000.0,
            ]),
            &["worker", "model"],
        )
        .expect("predicted_ttft_ms histogram opts are valid");
        let worker_queued_prompts_mean = GaugeVec::new(
            Opts::new(
                "kinference_worker_queued_prompts_mean30s",
                "Mean queued prompts per worker over a 30s window — the exact \
                 value `InferencePolicy`'s `queuedPrompts` predicate reads. \
                 Fractional: a worker that queued 5 prompts for two seconds \
                 out of thirty reads about 0.3. Plot it to pick a threshold",
            ),
            &["worker", "model"],
        )
        .expect("worker_queued_prompts_mean gauge opts are valid");
        let worker_kv_cache_usage_mean = GaugeVec::new(
            Opts::new(
                "kinference_worker_kv_cache_usage_mean30s",
                "Mean KV-cache utilization per worker (0..1) over the same 30s \
                 window, as read by the `kvCacheUsage` predicate. The headroom \
                 signal: it rises monotonically with pressure, where the queue \
                 can read low at high pressure",
            ),
            &["worker", "model"],
        )
        .expect("worker_kv_cache_usage_mean gauge opts are valid");
        let policy_revision_mismatch = IntCounter::new(
            "kinference_policy_revision_mismatch_total",
            "Picks whose named InferencePool revision differed from the watched one",
        )
        .expect("policy_revision_mismatch counter opts are valid");
        let policy_warnings = IntGaugeVec::new(
            Opts::new(
                "kinference_policy_warnings",
                "Findings an InferencePolicy carries, by warning code — the list in \
                 its status.warnings. Non-zero means some part of the policy does not \
                 do what it says; the code names which kind. Published by the status \
                 writer, so only the leader exports it",
            ),
            &["policy", "code"],
        )
        .expect("policy_warnings gauge opts are valid");
        // Registering a clone is the crate's standard pattern: the clone
        // shares the same underlying atomic storage, so increments on the
        // struct's copy are visible when the registry gathers.
        registry
            .register(Box::new(cached_prompt_tokens.clone()))
            .expect("register cached_prompt_tokens");
        registry
            .register(Box::new(cached_tokens_absent.clone()))
            .expect("register cached_tokens_absent");
        registry
            .register(Box::new(cached_tokens_reported.clone()))
            .expect("register cached_tokens_reported");
        registry
            .register(Box::new(orphaned_reports.clone()))
            .expect("register orphaned_reports");
        registry
            .register(Box::new(prompt_tokens.clone()))
            .expect("register prompt_tokens");
        registry
            .register(Box::new(completion_tokens.clone()))
            .expect("register completion_tokens");
        registry
            .register(Box::new(prompt_length.clone()))
            .expect("register prompt_length");
        registry
            .register(Box::new(completion_length.clone()))
            .expect("register completion_length");
        registry
            .register(Box::new(request_duration_ms.clone()))
            .expect("register request_duration_ms");
        registry
            .register(Box::new(ttft_ms.clone()))
            .expect("register ttft_ms");
        registry
            .register(Box::new(provider_ttft_ms.clone()))
            .expect("register provider_ttft_ms");
        registry
            .register(Box::new(itl_bucket.clone()))
            .expect("register itl_bucket");
        registry
            .register(Box::new(itl_count.clone()))
            .expect("register itl_count");
        registry
            .register(Box::new(body_buffer_bytes.clone()))
            .expect("register body_buffer_bytes");
        registry
            .register(Box::new(body_buffer_entries.clone()))
            .expect("register body_buffer_entries");
        registry
            .register(Box::new(leader.clone()))
            .expect("register leader");
        registry
            .register(Box::new(leader_transitions.clone()))
            .expect("register leader_transitions");
        registry
            .register(Box::new(leader_election_errors.clone()))
            .expect("register leader_election_errors");
        registry
            .register(Box::new(body_pushed.clone()))
            .expect("register body_pushed");
        registry
            .register(Box::new(body_pushed_bytes.clone()))
            .expect("register body_pushed_bytes");
        registry
            .register(Box::new(body_rejected.clone()))
            .expect("register body_rejected");
        registry
            .register(Box::new(body_reaped.clone()))
            .expect("register body_reaped");
        registry
            .register(Box::new(body_aborted.clone()))
            .expect("register body_aborted");
        // Registered by value and not kept on the struct: it is set once, above,
        // and never read again. The registry owns the only handle that matters.
        registry
            .register(Box::new(build_info))
            .expect("register build_info");
        registry
            .register(Box::new(dashboard_slots_used.clone()))
            .expect("register dashboard_slots_used");
        registry
            .register(Box::new(dashboard_slots_capacity.clone()))
            .expect("register dashboard_slots_capacity");
        registry
            .register(Box::new(dashboard_slots_denied.clone()))
            .expect("register dashboard_slots_denied");
        registry
            .register(Box::new(picks.clone()))
            .expect("register picks");
        registry
            .register(Box::new(pick_duration_ms.clone()))
            .expect("register pick_duration_ms");
        registry
            .register(Box::new(tokenize_path.clone()))
            .expect("register tokenize_path");
        registry
            .register(Box::new(tokenize_duration_ms.clone()))
            .expect("register tokenize_duration_ms");
        registry
            .register(Box::new(tokenize_compare.clone()))
            .expect("register tokenize_compare");
        registry
            .register(Box::new(workers.clone()))
            .expect("register workers");
        registry
            .register(Box::new(workers_by_model.clone()))
            .expect("register workers_by_model");
        registry
            .register(Box::new(worker_evictions.clone()))
            .expect("register worker_evictions");
        registry
            .register(Box::new(index_blocks.clone()))
            .expect("register index_blocks");
        registry
            .register(Box::new(index_workers.clone()))
            .expect("register index_workers");
        registry
            .register(Box::new(index_events.clone()))
            .expect("register index_events");
        registry
            .register(Box::new(index_blocks_stored.clone()))
            .expect("register index_blocks_stored");
        registry
            .register(Box::new(index_blocks_removed.clone()))
            .expect("register index_blocks_removed");
        registry
            .register(Box::new(prefix_requested_blocks.clone()))
            .expect("register prefix_requested_blocks");
        registry
            .register(Box::new(prefix_matched_blocks.clone()))
            .expect("register prefix_matched_blocks");
        registry
            .register(Box::new(prefix_radix_matched_blocks.clone()))
            .expect("register prefix_radix_matched_blocks");
        registry
            .register(Box::new(prefix_match_ratio.clone()))
            .expect("register prefix_match_ratio");
        registry
            .register(Box::new(prefix_overmatch.clone()))
            .expect("register prefix_overmatch");
        registry
            .register(Box::new(score_degraded.clone()))
            .expect("register score_degraded");
        registry
            .register(Box::new(workerset_rejected.clone()))
            .expect("register workerset_rejected");
        registry
            .register(Box::new(policy_decisions.clone()))
            .expect("register policy_decisions");
        registry
            .register(Box::new(routed_requests.clone()))
            .expect("register routed_requests");
        registry
            .register(Box::new(policy_rule_matches.clone()))
            .expect("register policy_rule_matches");
        registry
            .register(Box::new(predicted_ttft_ms.clone()))
            .expect("register predicted_ttft_ms");
        registry
            .register(Box::new(worker_queued_prompts_mean.clone()))
            .expect("register worker_queued_prompts_mean");
        registry
            .register(Box::new(worker_kv_cache_usage_mean.clone()))
            .expect("register worker_kv_cache_usage_mean");
        registry
            .register(Box::new(policy_revision_mismatch.clone()))
            .expect("register policy_revision_mismatch");
        registry
            .register(Box::new(policy_warnings.clone()))
            .expect("register policy_warnings");
        let m = Self {
            tenants,
            dashboard_slots_used,
            dashboard_slots_capacity,
            dashboard_slots_denied,
            picks,
            pick_duration_ms,
            tokenize_path,
            tokenize_duration_ms,
            tokenize_compare,
            workers,
            workers_by_model,
            worker_evictions,
            workers_by_model_seen: Mutex::new(HashSet::new()),
            index_blocks,
            index_workers,
            index_events,
            index_blocks_stored,
            index_blocks_removed,
            prefix_requested_blocks,
            prefix_matched_blocks,
            prefix_radix_matched_blocks,
            prefix_match_ratio,
            prefix_overmatch,
            request_duration_ms,
            registry,
            cached_prompt_tokens,
            cached_tokens_absent,
            cached_tokens_reported,
            orphaned_reports,
            prompt_tokens,
            completion_tokens,
            prompt_length,
            completion_length,
            ttft_ms,
            provider_ttft_ms,
            itl_bucket,
            itl_count,
            body_buffer_bytes,
            body_buffer_entries,
            body_pushed,
            body_pushed_bytes,
            body_rejected,
            body_reaped,
            body_aborted,
            score_degraded,
            workerset_rejected,
            policy_decisions,
            routed_requests,
            leader,
            leader_transitions,
            leader_election_errors,
            policy_rule_matches,
            predicted_ttft_ms,
            worker_queued_prompts_mean,
            worker_kv_cache_usage_mean,
            policy_revision_mismatch,
            policy_warnings,
        };
        m.preinit();
        m
    }

    /// Create every closed-set child series at zero, so a dashboard can tell
    /// "has not happened" from "is not happening".
    ///
    /// `with_label_values` materializes the child as a side effect of handing
    /// it back, which is the whole mechanism — there is nothing to increment.
    ///
    /// COUNTERS ONLY, and that is not an oversight. `rate()` over a seeded
    /// counter returns 0, which draws a line — the entire benefit. But
    /// `histogram_quantile` over an all-zero histogram returns NOTHING, so
    /// seeding `pick_duration_ms` would draw no line while costing ~45 series
    /// (3 outcomes × 13 buckets, plus `_sum` and `_count`). Verified against a
    /// live Prometheus rather than assumed.
    ///
    /// `policy_rule_matches{rule}` is skipped for the other reason: `rule` is
    /// an operator-authored string with no bound, so there is no set to seed.
    fn preinit(&self) {
        for outcome in PICK_OUTCOMES {
            self.picks.with_label_values(&[outcome]);
        }
        for reason in DEGRADE_REASONS {
            self.score_degraded.with_label_values(&[reason]);
        }
        for path in TOKENIZE_PATHS {
            self.tokenize_path.with_label_values(&[path]);
        }
        for result in TOKENIZE_COMPARE_RESULTS {
            self.tokenize_compare.with_label_values(&[result]);
        }
        for kind in INDEX_EVENT_KINDS {
            self.index_events.with_label_values(&[kind]);
        }
        for reason in BODY_REJECT_REASONS {
            self.body_rejected.with_label_values(&[reason]);
        }
        for reason in BODY_ABORT_REASONS {
            self.body_aborted.with_label_values(&[reason]);
        }
        for outcome in POLICY_DECISION_OUTCOMES {
            self.policy_decisions.with_label_values(&[outcome]);
        }
        for reason in WORKERSET_REJECT_REASONS {
            self.workerset_rejected.with_label_values(&[reason]);
        }
        for reason in EVICTION_REASONS {
            self.worker_evictions.with_label_values(&[reason]);
        }
    }

    /// One body accepted by `PushBody`.
    pub fn record_body_pushed(&self, bytes: u64) {
        self.body_pushed.inc();
        self.body_pushed_bytes.inc_by(bytes);
    }

    /// One `PushBody` refused over budget. `reason` comes from
    /// [`crate::body_store::RejectReason::as_label`] so the label and the
    /// caller-facing message can't drift.
    pub fn record_body_rejected(&self, reason: &str) {
        self.body_rejected.with_label_values(&[reason]).inc();
    }

    /// One `Abort`, by reason.
    pub fn record_body_aborted(&self, reason: &str) {
        self.body_aborted.with_label_values(&[reason]).inc();
    }

    /// Bodies reclaimed by the TTL sweep.
    pub fn record_bodies_reaped(&self, count: u64) {
        self.body_reaped.inc_by(count);
    }

    /// A worker set the operator declared but the router could not use.
    /// Counted on the TRANSITION into rejection, not per watch tick, so the
    /// counter measures how often configuration breaks rather than how chatty
    /// the apiserver is.
    pub fn record_workerset_rejected(&self, reason: &str) {
        self.workerset_rejected.with_label_values(&[reason]).inc();
    }

    /// One completed `Pick()`: its wire outcome and how long the router took.
    ///
    /// This is the router's own SLO. `outcome` is the result handed back to the
    /// caller — not a policy decision, which `record_policy_decision` covers
    /// separately and which can differ (a pick can resolve no policy and still
    /// return `ok`).
    pub fn record_pick(&self, outcome: &str, latency_ms: f64) {
        self.picks.with_label_values(&[outcome]).inc();
        self.pick_duration_ms
            .with_label_values(&[outcome])
            .observe(latency_ms);
    }

    /// Which path tokenized one request: `local`, `http_fallback`, or
    /// `unavailable`. Exactly one per attempt.
    pub fn record_tokenize_path(&self, path: &str) {
        self.tokenize_path.with_label_values(&[path]).inc();
    }

    /// Tokenize latency, all paths.
    pub fn record_tokenize_duration(&self, latency_ms: f64) {
        self.tokenize_duration_ms.observe(latency_ms);
    }

    /// Compare-mode local-vs-`/render` agreement: `match`, `divergence`, or
    /// `error`.
    ///
    /// A divergence is the one to alert on. It means kinference's block hashes do not
    /// match what the engine would produce, so nothing the router indexes can
    /// ever match a real prompt — and the prefix metrics keep reporting numbers
    /// that look fine.
    pub fn record_tokenize_compare(&self, result: &str) {
        self.tokenize_compare.with_label_values(&[result]).inc();
    }

    /// Dashboard slot occupancy, recomputed each rollup tick.
    pub fn set_dashboard_slots(&self, used: u64, capacity: u64) {
        self.dashboard_slots_used.set(used as i64);
        self.dashboard_slots_capacity.set(capacity as i64);
    }

    /// Registrations refused for want of a slot, this tick.
    ///
    /// Takes a DELTA: the underlying counter is `swap(0)`-ed each tick, as with
    /// the index counters.
    pub fn record_dashboard_slots_denied(&self, n: u64) {
        self.dashboard_slots_denied.inc_by(n);
    }

    /// Worker reachability, recomputed each rollup tick.
    pub fn set_worker_states(&self, ok: u64, unresponsive: u64) {
        self.workers.with_label_values(&["ok"]).set(ok as i64);
        self.workers
            .with_label_values(&["unresponsive"])
            .set(unresponsive as i64);
    }

    /// One final-token report that could not be attributed to a pick.
    ///
    /// A direct counter rather than a derived one. The same fact is visible as
    /// `prompt_length_tokens_count - request_duration_ms_count`, because the
    /// duration is the one family skipped for an orphan — but a difference
    /// between two counters aggregated over different label sets is exactly the
    /// shape that made the cached-tokens ratio read 200%.
    pub fn record_orphaned_report(&self) {
        self.orphaned_reports.inc();
    }

    /// One worker evicted by kinference's own decision.
    ///
    /// `reason` comes from [`crate::eviction::EvictionReason::as_label`] so the
    /// label and the WARN log cannot name different things.
    pub fn record_worker_eviction(&self, reason: &str) {
        self.worker_evictions.with_label_values(&[reason]).inc();
    }

    /// Per-model worker reachability. Companion to [`Self::set_worker_states`],
    /// which stays the fleet total.
    ///
    /// Zeroes any model that has dropped out of the input since the last call.
    /// Without that a deleted worker set leaves its gauge frozen at its final
    /// count, which reads as a fleet that never shrank. Zeroing rather than
    /// `reset()`-ing the whole vec avoids a scrape landing in the window where
    /// the family has been cleared but not yet repopulated.
    pub fn set_worker_states_by_model(&self, per_model: &[(String, u64, u64)]) {
        let mut seen = self.workers_by_model_seen.lock().unwrap();
        for (model, ok, unresponsive) in per_model {
            self.workers_by_model
                .with_label_values(&[model, "ok"])
                .set(*ok as i64);
            self.workers_by_model
                .with_label_values(&[model, "unresponsive"])
                .set(*unresponsive as i64);
            seen.insert(model.clone());
        }
        let present: HashSet<&str> = per_model.iter().map(|(m, _, _)| m.as_str()).collect();
        for model in seen.iter() {
            if !present.contains(model.as_str()) {
                self.workers_by_model
                    .with_label_values(&[model, "ok"])
                    .set(0);
                self.workers_by_model
                    .with_label_values(&[model, "unresponsive"])
                    .set(0);
            }
        }
    }

    /// The radix tree's size, from a throttled dump.
    ///
    /// Called only on a dump tick, so between dumps the gauges hold their last
    /// value — which is what a gauge does anyway, and matches the dashboard.
    pub fn set_index_size(&self, blocks: u64, workers: u64) {
        self.index_blocks.set(blocks as i64);
        self.index_workers.set(workers as i64);
    }

    /// Radix-tree ingest activity since the last rollup tick.
    ///
    /// Takes DELTAS, not totals: the underlying atomics are `swap(0)`-ed each
    /// tick, so the caller hands over what happened in that interval and this
    /// accumulates it into monotonic counters. That is what lets `rate()` work
    /// on them — the per-second gauges the dashboard shows are re-derivable,
    /// but the reverse is not.
    pub fn record_index_events(
        &self,
        stored: u64,
        removed: u64,
        cleared: u64,
        blocks_stored: u64,
        blocks_removed: u64,
    ) {
        self.index_events
            .with_label_values(&["stored"])
            .inc_by(stored);
        self.index_events
            .with_label_values(&["removed"])
            .inc_by(removed);
        self.index_events
            .with_label_values(&["cleared"])
            .inc_by(cleared);
        self.index_blocks_stored.inc_by(blocks_stored);
        self.index_blocks_removed.inc_by(blocks_removed);
    }

    /// One pick's prefix-cache outcome for the CHOSEN worker.
    ///
    /// `layered` is the overlap the picker actually scored on — the radix match
    /// plus credit for blocks in-flight requests are still prefilling, weighted
    /// by `expected_overlap_alpha`. `radix` is the raw tree match for the same
    /// pick. Both are recorded against one denominator so
    /// `layered - radix` reads as the in-flight credit, which is otherwise
    /// tuned blind.
    ///
    /// **Do not call this for a DEGRADED pick.** A tokenize failure yields
    /// `requested == 0` and `layered == 0`, which would record a 0.0 ratio
    /// indistinguishable from a genuine cache miss — so a tokenize outage would
    /// read as a prefix-routing collapse. A prefix-index failure can yield
    /// `layered > 0` with `requested == 0`, which is incoherent. Those picks are
    /// counted by [`Self::record_score_degraded`] instead. The `requested == 0`
    /// guard below is a backstop, not the policy.
    ///
    /// Counts PICKS, not requests: it fires at pick time, so unlike the token
    /// and duration families it includes non-streaming picks and picks whose
    /// caller never reports back. The totals will not line up.
    pub fn record_prefix_match(
        &self,
        model: &str,
        qos: &str,
        requested: u32,
        layered: u32,
        radix: u32,
    ) {
        if model.is_empty() || requested == 0 {
            return;
        }
        let labels = [model, qos];
        self.prefix_requested_blocks
            .with_label_values(&labels)
            .inc_by(requested as u64);
        self.prefix_matched_blocks
            .with_label_values(&labels)
            .inc_by(layered as u64);
        self.prefix_radix_matched_blocks
            .with_label_values(&labels)
            .inc_by(radix as u64);
        // Overlap scores are summed across data-parallel ranks, so a DP>1 worker
        // that publishes one prefix from two ranks can report more matched
        // blocks than the prompt has. Clamping keeps the ratio meaningful;
        // counting keeps the misconfiguration visible rather than merely
        // survived.
        if layered > requested {
            self.prefix_overmatch.with_label_values(&labels).inc();
        }
        let ratio = (layered as f64 / requested as f64).clamp(0.0, 1.0);
        self.prefix_match_ratio
            .with_label_values(&labels)
            .observe(ratio);
    }

    /// A pick that lost its prefix-cache signal. `reason` names the subsystem
    /// that failed, because the two have completely different fixes: a
    /// tokenize failure is an engine/network problem, a prefix-index failure is
    /// kinference's own radix tree. Both look identical in the pick's latency.
    pub fn record_score_degraded(&self, reason: &str) {
        self.score_degraded.with_label_values(&[reason]).inc();
    }

    /// One CRD-policy decision, by outcome (`chosen`, `rejected_on_empty`,
    /// `unmatched_served`, `unknown_pool`, …).
    pub fn record_policy_decision(&self, outcome: &str) {
        self.policy_decisions.with_label_values(&[outcome]).inc();
    }

    /// One pick that chose a destination: the worker set or provider, by the
    /// name a policy uses for it, and its type.
    ///
    /// Counted at the pick, not on completion, so it says where a tenant's
    /// requests were SENT, whether or not they then succeeded. A caller that
    /// retries starts over with a fresh pick, so a retry counts again.
    pub fn record_routed(
        &self,
        tenant: &str,
        destination: &str,
        kind: crate::policy::crd::BackendType,
    ) {
        self.routed_requests
            .with_label_values(&[self.tenant_label(tenant), destination, kind.as_str()])
            .inc();
    }

    /// Publish this replica's leadership state. Counts a transition only on an
    /// actual change, so the counter measures flapping rather than tick rate.
    pub fn set_leader(&self, is_leader: bool) {
        let want = i64::from(is_leader);
        if self.leader.get() != want {
            self.leader.set(want);
            self.leader_transitions.inc();
        }
    }

    /// A Lease read or write failed. See the field docs: election fails closed,
    /// so this climbing while `kinference_leader` is 0 everywhere means no replica is
    /// writing status at all.
    pub fn record_leader_election_error(&self) {
        self.leader_election_errors.inc();
    }

    /// A rule fired. Lets an operator see which rules are actually live —
    /// especially useful next to a rule flagged inert by a placeholder
    /// classifier.
    pub fn record_policy_rule_match(&self, rule: &str) {
        self.policy_rule_matches.with_label_values(&[rule]).inc();
    }

    /// The pool revision the gateway routed on differs from the one we hold.
    /// Advisory, but a rising count means the watch is lagging the gateway.
    /// One candidate's predicted TTFT, as scored by a policy.
    pub fn record_predicted_ttft(&self, worker: &str, model: &str, ms: f64) {
        self.predicted_ttft_ms
            .with_label_values(&[worker, model])
            .observe(ms);
    }

    /// Publish the per-worker saturation means, replacing the previous tick.
    ///
    /// `reset()` first because a worker that goes away must not leave its last
    /// reading behind: a stale 0.95 would read as a permanently saturated GPU,
    /// and a stale 0.0 as permanent headroom. Both are worse than absence,
    /// which Prometheus renders as a gap.
    pub fn set_worker_saturation(&self, rows: &[(String, String, f64, f64)]) {
        self.worker_queued_prompts_mean.reset();
        self.worker_kv_cache_usage_mean.reset();
        for (worker, model, queued, kv) in rows {
            self.worker_queued_prompts_mean
                .with_label_values(&[worker, model])
                .set(*queued);
            self.worker_kv_cache_usage_mean
                .with_label_values(&[worker, model])
                .set(*kv);
        }
    }

    pub fn record_policy_revision_mismatch(&self) {
        self.policy_revision_mismatch.inc();
    }

    /// Publish per-policy warning counts, replacing the previous pass.
    ///
    /// `reset()` first, like the saturation gauges: a policy that was fixed —
    /// or deleted — must drop to absent, not keep advertising the warnings it
    /// used to have.
    pub fn set_policy_warnings(&self, rows: &[(String, &'static str, i64)]) {
        self.policy_warnings.reset();
        for (policy, code, n) in rows {
            self.policy_warnings
                .with_label_values(&[policy.as_str(), code])
                .set(*n);
        }
    }

    /// Publish the body store's current occupancy. Called from the sweep task,
    /// which is the only place with a natural tick — gauges are point-in-time,
    /// so a 10s refresh is plenty for a memory-pressure signal.
    pub fn set_body_buffer(&self, entries: u64, bytes: u64) {
        self.body_buffer_entries.set(entries as i64);
        self.body_buffer_bytes.set(bytes as i64);
    }

    /// Prompt tokens the ENGINE served from its prefix cache, if it said.
    ///
    /// `None` means the caller sent no `cached_tokens` — an older engine, or a
    /// caller not relaying the field — and is NOT the same as zero. Recording a
    /// missing value as zero would understate the cache hit and overstate
    /// billable prefill, so it is counted separately instead.
    ///
    /// Distinct from `kinference_prefix_matched_blocks_total`, which is what the
    /// ROUTER predicted from its own index. This is what the engine actually
    /// did. Comparing them is the only check on whether the index reflects
    /// reality; billing should use this one.
    pub fn record_cached_prompt_tokens(
        &self,
        model: &str,
        qos: &str,
        tenant: &str,
        cached_tokens: Option<u32>,
    ) {
        if model.is_empty() {
            return;
        }
        match cached_tokens {
            Some(n) => {
                self.cached_prompt_tokens
                    .with_label_values(&[model, qos, self.tenant_label(tenant)])
                    .inc_by(n as u64);
                // Counted separately from the TOKEN sum above: `n` can be a
                // legitimate zero, so the token total cannot say how many
                // reports carried the field.
                self.cached_tokens_reported.inc();
            }
            None => self.cached_tokens_absent.inc(),
        }
    }

    /// Record one `ReportFinalToken`: add the token counts to the per-model/qos
    /// counters and observe the lengths into the per-model/qos histograms. A
    /// blank `model` (undetermined by the caller) is skipped rather than
    /// bucketed under an empty label.
    pub fn record_usage(
        &self,
        model: &str,
        qos: &str,
        tenant: &str,
        prompt_tokens: u32,
        completion_tokens: u32,
    ) {
        if model.is_empty() {
            return;
        }
        let tenant = self.tenant_label(tenant);
        self.prompt_tokens
            .with_label_values(&[model, qos, tenant])
            .inc_by(prompt_tokens as u64);
        self.completion_tokens
            .with_label_values(&[model, qos, tenant])
            .inc_by(completion_tokens as u64);
        // Exactly one observation per request, so each histogram's `_count`
        // is the request count and its `_sum` mirrors the matching counter.
        self.prompt_length
            .with_label_values(&[model, qos, tenant])
            .observe(prompt_tokens as f64);
        self.completion_length
            .with_label_values(&[model, qos, tenant])
            .observe(completion_tokens as f64);
    }

    /// Record one streaming request's observed time-to-first-token (from
    /// `ReportFirstToken`, via `finalize_observation`), in milliseconds. A
    /// blank `model` is skipped. Only call this for STREAMING requests — for
    /// non-streaming the reported first-byte time is the whole response, not
    /// a TTFT.
    /// One completed request, pick → `ReportFinalToken`.
    ///
    /// This is the ROUTER's view, not the client's: it starts when kinference chose
    /// an endpoint and ends when the caller's terminal report arrives, so it
    /// includes the caller's own handling and that report's network hop. Read
    /// it as "how long did this request occupy a worker", not as user-observed
    /// latency.
    ///
    /// Only requests that actually report are counted. One that never sends
    /// `ReportFinalToken` ages out of the label TTL silently, so this
    /// distribution is biased toward requests that completed cleanly — the same
    /// caveat the token counters carry.
    pub fn record_request_duration(&self, model: &str, qos: &str, tenant: &str, ms: u64) {
        if model.is_empty() {
            return;
        }
        self.request_duration_ms
            .with_label_values(&[model, qos, self.tenant_label(tenant)])
            .observe(ms as f64);
    }

    pub fn record_ttft(&self, model: &str, qos: &str, tenant: &str, ttft_ms: u32) {
        if model.is_empty() {
            return;
        }
        self.ttft_ms
            .with_label_values(&[model, qos, self.tenant_label(tenant)])
            .observe(ttft_ms as f64);
    }

    /// Observe a provider pick's time-to-first-token, in milliseconds.
    ///
    /// Call only for a SUCCESSFUL first-token report. A blank `provider` or
    /// `model` is skipped rather than recorded under an empty label, which would
    /// silently pool unrelated providers into one series.
    pub fn record_provider_ttft(&self, provider: &str, model: &str, ttft_ms: u32) {
        if provider.is_empty() || model.is_empty() {
            return;
        }
        self.provider_ttft_ms
            .with_label_values(&[provider, model])
            .observe(ttft_ms as f64);
    }

    /// Merge one request's inter-token-latency bucket counts (from
    /// `ReportFinalToken.itl_bucket_counts`) into the fleet-wide per-(model,qos)
    /// histogram. `counts[i]` is the number of gaps in Layout-1 bucket `i`
    /// (non-cumulative on the wire); we accumulate the cumulative count into
    /// each `le` bucket, which is additive across requests. Skips on empty
    /// `model`, a `layout` that doesn't match [`ITL_BUCKET_LAYOUT`], or a
    /// `counts` length that doesn't match the layout (honors the proto's
    /// "server MUST skip on mismatch").
    pub fn record_itl(&self, model: &str, qos: &str, tenant: &str, layout: u32, counts: &[u32]) {
        if model.is_empty() {
            return;
        }
        if layout != ITL_BUCKET_LAYOUT {
            tracing::debug!(
                layout,
                expected = ITL_BUCKET_LAYOUT,
                "skipping ITL: bucket-layout mismatch"
            );
            return;
        }
        // No gaps to report is NOT a length error. The proto prescribes an empty
        // vec for a response with fewer than two chunks — every non-streaming
        // request, and any single-chunk stream — so warning here would log once
        // per such request and bury a genuine length bug in the noise.
        if counts.is_empty() {
            return;
        }
        if counts.len() != ITL_LE_LABELS.len() {
            tracing::warn!(
                got = counts.len(),
                expected = ITL_LE_LABELS.len(),
                "skipping ITL: bucket-count length mismatch"
            );
            return;
        }
        let tenant = self.tenant_label(tenant);
        let mut cum: u64 = 0;
        for (i, &c) in counts.iter().enumerate() {
            cum += c as u64;
            // Positional: this MUST match the declared order
            // `["model", "qos", "le", "tenant"]`. Every element is a `&str`, so
            // swapping `le` and `tenant` would compile and quietly emit series
            // whose bucket edge is a tenant name.
            self.itl_bucket
                .with_label_values(&[model, qos, ITL_LE_LABELS[i], tenant])
                .inc_by(cum);
        }
        // `cum` is now the total gaps this request (== the +Inf bucket).
        self.itl_count
            .with_label_values(&[model, qos, tenant])
            .inc_by(cum);
    }

    /// The label value for `tenant`, per `[metrics] tenant_labels`.
    ///
    /// An EMPTY tenant always resolves to [`DEFAULT_TENANT_LABEL`], before the
    /// allowlist is consulted — on the wire an absent tenant is the empty
    /// string rather than a presence bit (see `proto/kinference.proto` and
    /// `crate::policy::predicate`), and it also covers the case where a
    /// feedback RPC found no stashed labels at all.
    ///
    /// Note this deliberately DIFFERS from the policy semantics, where
    /// `tenant: ""` is a distinct matchable value naming unattributable
    /// traffic. In metrics it collapses into the shared bucket; the two are
    /// not the same string and should not be read as such.
    ///
    /// Resolution happens here rather than at the call sites so that a caller
    /// cannot bypass the allowlist by passing a raw tenant straight through.
    fn tenant_label<'a>(&'a self, tenant: &'a str) -> &'a str {
        if tenant.is_empty() {
            return DEFAULT_TENANT_LABEL;
        }
        match &self.tenants {
            TenantPolicy::None => DEFAULT_TENANT_LABEL,
            TenantPolicy::All => tenant,
            TenantPolicy::Allow(set) => {
                if set.contains(tenant) {
                    tenant
                } else {
                    DEFAULT_TENANT_LABEL
                }
            }
        }
    }

    /// The shared exposition registry, lent to subsystems that own their own
    /// metrics.
    ///
    /// A subsystem registers its families here from its own file, so adding a
    /// metric touches only that subsystem rather than this struct. Where a
    /// family was registered has no effect on the output: `gather` collects
    /// families into a `BTreeMap` keyed by name, so [`Self::render`] is sorted
    /// by metric name and byte-identical regardless of registration order.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Render the current metric set in Prometheus text-exposition
    /// format. Cheap enough to run per scrape.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        // `encode` only errors on a broken io::Write; our Vec never
        // fails, so an error here is unreachable in practice.
        if encoder.encode(&self.registry.gather(), &mut buf).is_err() {
            return String::new();
        }
        String::from_utf8(buf).unwrap_or_default()
    }

    /// The Prometheus text-format content type, for the HTTP handler's
    /// `Content-Type` header. This is exactly what
    /// `TextEncoder::format_type()` returns.
    pub fn content_type() -> &'static str {
        "text/plain; version=0.0.4"
    }
}

impl Default for PromMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
