use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use crate::config::{Engine, WorkerConfig};
use crate::policy::PoolId;

// `Engine` is no longer used to dispatch metric-name lookups (we probe both
// vLLM- and SGLang-prefixed names on every scrape). It IS still used as a
// label for the engine identity detected from `/v1/models` `owned_by`, so
// the import is retained.

// Both vLLM and SGLang prefix their metric names with `<engine>:`, which is
// valid Prometheus but rejected by prometheus-parse 0.2.5's `\w+` metric-name
// regex. We rewrite `vllm:` → `vllm_` and `sglang:` → `sglang_` in the scraped
// body before parsing and look up the rewritten names here. SGLang v0.5.4+
// already emits the underscore form natively, in which case the rewrite is a
// harmless no-op.

/// Per-engine table of which Prometheus metric names to read for each of the
/// fields on `MetricsSample`. Built lazily as `&'static`, dispatched on
/// `Engine` via [`names_for`].
struct MetricNames {
    /// Candidate names for the "requests waiting in queue" gauge. First match
    /// wins, so put preferred names first.
    waiting: &'static [&'static str],
    /// Candidate names for the "currently running" gauge.
    running: &'static [&'static str],
    /// Candidate names for "swapped to CPU" — empty slice when the engine
    /// doesn't have this concept (SGLang).
    swapped: &'static [&'static str],
    /// Candidate names for KV-cache utilization (0.0..1.0).
    kv_cache_usage: &'static [&'static str],
    /// If `Some(name)`, only accept samples whose label `name` equals the
    /// worker's configured `model_name`. If `None`, take the first matching
    /// sample regardless of labels.
    model_label: Option<&'static str>,
    /// Candidate counter names for "successfully completed requests."
    /// Summed across all label combinations (e.g. `finished_reason`) matching
    /// the worker's model_label. First match wins, so put preferred names
    /// first. Used to derive `completion_rate_per_sec` as a delta over the
    /// scrape interval.
    completions: &'static [&'static str],
    /// Candidate counter names for "prefix-cache lookups" (denominator).
    /// vLLM ≥ 0.6 exposes this as a monotonic counter. Pair with
    /// `prefix_cache_hits` to derive a windowed hit rate.
    prefix_cache_queries: &'static [&'static str],
    /// Candidate counter names for "prefix-cache lookups that hit"
    /// (numerator). Paired with `prefix_cache_queries`.
    prefix_cache_hits: &'static [&'static str],
    /// Candidate gauge names that express the hit rate directly as a
    /// 0..1 ratio. SGLang exposes `sglang:cache_hit_rate` this way.
    /// Consulted only when the counter pair above misses.
    prefix_cache_hit_rate_gauge: &'static [&'static str],
}

// Union of vLLM and SGLang candidate names. The two engines use disjoint
// metric-name prefixes (`vllm_*` vs `sglang_*`), so no name collisions; the
// scraper picks whichever the engine actually emits. Listing vLLM first
// matches the historical preference order.
const ALL_NAMES: MetricNames = MetricNames {
    waiting: &["vllm_num_requests_waiting", "sglang_num_queue_reqs"],
    running: &["vllm_num_requests_running", "sglang_num_running_reqs"],
    // SGLang has no CPU-swap concept; lookup just fails silently for it.
    swapped: &["vllm_num_requests_swapped"],
    // vLLM v0 named this `gpu_cache_usage_perc`; v1 renamed it to
    // `kv_cache_usage_perc`. Accept either, prefer v1. SGLang uses
    // `sglang_token_usage`.
    kv_cache_usage: &[
        "vllm_kv_cache_usage_perc",
        "vllm_gpu_cache_usage_perc",
        "sglang_token_usage",
    ],
    // Both engines label samples with `model_name`.
    model_label: Some("model_name"),
    completions: &[
        // Prefer direct success counters; fall back to histogram observation
        // counts (`_count` is a counter of completed requests).
        "vllm_request_success_total",
        "vllm_e2e_request_latency_seconds_count",
        "sglang_num_requests_total",
        "sglang_e2e_request_latency_seconds_count",
    ],
    // vLLM ≥ 0.6. Defensively include the singular form some
    // intermediate versions emitted.
    prefix_cache_queries: &[
        "vllm_prefix_cache_queries_total",
        "vllm_prefix_cache_query_total",
    ],
    prefix_cache_hits: &["vllm_prefix_cache_hits_total"],
    // SGLang publishes the rate directly as a gauge (0..1). Only used
    // when the vLLM counter pair above isn't present.
    prefix_cache_hit_rate_gauge: &["sglang_cache_hit_rate"],
};

/// Best-effort detection of which engine a `/v1/models` response was served
/// by. Sourced from `data[*].owned_by` (vLLM emits `"vllm"`, SGLang emits
/// `"sglang"`). `None` if neither matches — non-fatal; the field is purely
/// informational (display label, mismatch warnings).
fn engine_from_owned_by(owned_by: &str) -> Option<Engine> {
    match owned_by {
        "vllm" => Some(Engine::Vllm),
        "sglang" => Some(Engine::Sglang),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MetricsSample {
    pub waiting: u32,
    pub running: u32,
    pub swapped: u32,
    pub kv_cache_usage: f32,
    /// `waiting`, averaged over `COMPLETION_RATE_WINDOW`.
    ///
    /// What the policy engine's `queuedPrompts` predicate reads. The raw
    /// `waiting` above stays the picker's input: the picker predicts the TTFT
    /// of a request being dispatched RIGHT NOW, for which the instantaneous
    /// depth is the correct term. Only the admission decision — "is this
    /// worker under sustained pressure" — wants the mean.
    pub waiting_mean: f32,
    /// `kv_cache_usage`, averaged over the same window. What `kvCacheUsage`
    /// reads. Still a 0.0..1.0 ratio.
    pub kv_cache_usage_mean: f32,
    /// Cumulative successful-completion counter at the time of this scrape
    /// (sum across all label combinations matching `model_label`). The
    /// per-sec rate is derived from the delta against the previous sample.
    pub completions_total: u64,
    /// Successfully-completed requests per second, averaged over a sliding
    /// window (~30s — see `COMPLETION_RATE_WINDOW`). A single-tick delta
    /// reads `0` for most ticks under low load (the underlying counter only
    /// ticks when a request finishes), so we take the slope across the
    /// window instead. `0.0` until we have at least two samples or when
    /// the completion counter is unavailable on this engine version.
    pub completion_rate_per_sec: f32,
    /// Cumulative prefix-cache hit counter (vLLM
    /// `prefix_cache_hits_total`). `0` when the engine doesn't expose
    /// this metric (older vLLM versions, SGLang — SGLang publishes
    /// only the derived rate, not the raw counts). Retained for
    /// volume context: a 50% rate over 10k queries is materially
    /// different from 50% over 5.
    pub prefix_cache_hits_total: u64,
    /// Cumulative prefix-cache query counter (vLLM
    /// `prefix_cache_queries_total`). Pair with `prefix_cache_hits_total`
    /// to derive the windowed rate. `0` when the engine doesn't expose
    /// this metric.
    pub prefix_cache_queries_total: u64,
    /// Engine-reported prefix-cache hit rate, 0..1. Sourced from:
    ///   - vLLM counter pair: `Δhits / Δqueries` over the same 30s
    ///     sliding window used by `completion_rate_per_sec`;
    ///   - SGLang gauge `sglang:cache_hit_rate` (already a ratio,
    ///     surfaced verbatim — SGLang chooses its own averaging).
    ///
    /// `0.0` when neither source is present (older engine, cold start
    /// before two samples accumulated, or zero queries in the window).
    pub prefix_cache_hit_rate: f32,
    pub scraped_at: Option<Instant>,
}

/// One entry from `/v1/models`'s `data[]`. Only the fields the predictor /
/// router actually use are retained.
#[derive(Clone, Debug, Default)]
pub struct ModelEntry {
    /// Mirrors the proto `ServedModel.name` field; sourced from
    /// `/v1/models` `data[*].id`.
    pub name: String,
    /// `data[*].max_model_len`. `None` when the engine omitted it (some
    /// embedding-only models don't carry it).
    pub max_model_len: Option<u32>,
}

/// Worker-scoped cache of the engine's `/v1/models` response.
#[derive(Clone, Debug, Default)]
pub struct ModelsInfo {
    pub models: Vec<ModelEntry>,
    /// Engine identity from `data[*].owned_by` (vLLM emits `"vllm"`, SGLang
    /// emits `"sglang"`). `None` until the first successful refresh, or
    /// when no `data[]` entry is present. Purely informational — no
    /// routing decisions key off this value. Consumed by the dashboard /
    /// score-handler in subsequent phases.
    #[allow(dead_code)]
    pub detected_engine: Option<Engine>,
    /// Wall-clock timestamp of the last successful refresh. Written by the
    /// poller; not currently surfaced but kept for a future
    /// "models_age_ms"-style staleness signal.
    #[allow(dead_code)]
    pub fetched_at: Option<Instant>,
}

/// Sliding-window samples of `completions_total` per worker. Used to derive
/// `completion_rate_per_sec` as a slope across the window rather than from a
/// single 1-tick delta — the latter reads `0` for most ticks under low load
/// (the counter only ticks up when a request finishes) and produces a
/// flickering "rate / no rate" display in the dashboard.
#[derive(Clone, Debug, Default)]
struct CompletionHistory {
    /// `(scraped_at, completions_total)` from each successful scrape. Bounded
    /// by both count and age — see `COMPLETION_RATE_WINDOW`.
    samples: std::collections::VecDeque<(Instant, u64)>,
}

/// Per-worker sliding-window history of vLLM's prefix-cache counters.
/// Parallel to `CompletionHistory` — same eviction policy, same
/// `COMPLETION_RATE_WINDOW`, but tracks TWO counters (hits + queries)
/// per sample so the derived hit rate is `Δhits / Δqueries` over the
/// window.
#[derive(Clone, Debug, Default)]
struct PrefixCacheHistory {
    /// `(scraped_at, hits_total, queries_total)` from each scrape that
    /// found the vLLM counter pair. Empty for SGLang workers and
    /// pre-0.6 vLLM (those populate the rate via a different code path
    /// or leave it at 0).
    samples: std::collections::VecDeque<(Instant, u64, u64)>,
}

/// Per-worker sliding-window history of the two SATURATION gauges.
///
/// Third sibling of `CompletionHistory` / `PrefixCacheHistory`, and different
/// in kind: those derive a rate from counter deltas, this averages a gauge.
/// A gauge needs the window for a different reason — not to avoid reading zero
/// between sparse events, but because `num_requests_waiting` is BURSTY. It
/// spikes for a few seconds and clears, so a policy thresholding the
/// instantaneous value drops a worker on a transient that has already passed,
/// and (under `onEmpty: Reject`) can refuse a request over a blip. Averaging
/// makes the threshold mean "sustained pressure".
///
/// `kv_cache_usage` is already smooth and barely changes under averaging; it
/// shares the window for consistency, so the two attributes a policy compares
/// are measured the same way.
#[derive(Clone, Debug, Default)]
struct GaugeHistory {
    /// `(scraped_at, waiting, kv_cache_usage)`. Same eviction policy and the
    /// same `COMPLETION_RATE_WINDOW` as its siblings — one source of truth for
    /// the window length.
    samples: std::collections::VecDeque<(Instant, u32, f32)>,
}

/// Sliding window over which `completion_rate_per_sec` is averaged. 30s is a
/// reasonable compromise: long enough that 1 req every 10s registers (~0.1
/// req/s) instead of "0 most ticks, 1 spike, 0 again," and short enough that
/// the displayed rate still tracks real load changes within ~tens of seconds.
const COMPLETION_RATE_WINDOW: Duration = Duration::from_secs(30);

pub struct MetricsRegistry {
    by_worker: DashMap<u64, MetricsSample>,
    /// Sibling map for `/v1/models` data. Refreshed on a slower cadence than
    /// `by_worker` (~30s vs 1s). Cloned out on read so the gRPC handler
    /// doesn't hold a DashMap ref across an await.
    models_by_worker: DashMap<u64, ModelsInfo>,
    /// Sibling map of completion-counter history for sliding-window rate.
    /// Updated by the poller; read internally to compute the rate before
    /// the new sample is inserted into `by_worker`.
    completion_history: DashMap<u64, CompletionHistory>,
    /// Sibling map of vLLM prefix-cache counter history. Used to derive
    /// the windowed `prefix_cache_hit_rate` for engines that publish
    /// the counter pair. SGLang's gauge skips this map entirely.
    prefix_cache_history: DashMap<u64, PrefixCacheHistory>,
    /// Sibling map of the saturation gauges' history, for the windowed means
    /// the policy engine thresholds on.
    gauge_history: DashMap<u64, GaugeHistory>,
    startup: Instant,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            by_worker: DashMap::new(),
            models_by_worker: DashMap::new(),
            completion_history: DashMap::new(),
            prefix_cache_history: DashMap::new(),
            gauge_history: DashMap::new(),
            startup: Instant::now(),
        }
    }

    /// Push the latest scrape sample into the per-worker history and return
    /// the windowed `completion_rate_per_sec`. Slope is taken over the actual
    /// time range present in the window — so a single sample yields 0.0 (we
    /// don't have two points to differentiate), two samples spaced 30s apart
    /// yield `(c2 - c1) / 30`, and so on.
    fn record_and_compute_rate(&self, worker_id_num: u64, now: Instant, counter: u64) -> f32 {
        let mut entry = self.completion_history.entry(worker_id_num).or_default();
        let hist = entry.value_mut();
        hist.samples.push_back((now, counter));
        // Evict anything older than the window (always keep at least one
        // sample so an idle worker doesn't end up with an empty deque).
        while hist.samples.len() > 1
            && now.saturating_duration_since(hist.samples.front().unwrap().0)
                > COMPLETION_RATE_WINDOW
        {
            hist.samples.pop_front();
        }
        if hist.samples.len() < 2 {
            return 0.0;
        }
        let (t0, c0) = *hist.samples.front().unwrap();
        let (t1, c1) = *hist.samples.back().unwrap();
        let dt = t1.saturating_duration_since(t0).as_secs_f64();
        if dt <= 0.0 {
            return 0.0;
        }
        // saturating_sub guards against engine restarts (counter resets);
        // we just see a one-window dip toward zero, which is fine.
        let delta = c1.saturating_sub(c0);
        (delta as f64 / dt) as f32
    }

    /// Push the latest scrape into the per-worker prefix-cache history
    /// and return the windowed hit rate. Slope-over-slope: `Δhits /
    /// Δqueries` across whatever samples are present in the window.
    /// `0.0` until we have at least two samples, or when no queries
    /// occurred in the window (`Δqueries == 0`). Same window /
    /// eviction policy as `record_and_compute_rate` — single source
    /// of truth for the sliding-window length.
    fn record_and_compute_hit_rate(
        &self,
        worker_id_num: u64,
        now: Instant,
        hits: u64,
        queries: u64,
    ) -> f32 {
        let mut entry = self.prefix_cache_history.entry(worker_id_num).or_default();
        let hist = entry.value_mut();
        hist.samples.push_back((now, hits, queries));
        while hist.samples.len() > 1
            && now.saturating_duration_since(hist.samples.front().unwrap().0)
                > COMPLETION_RATE_WINDOW
        {
            hist.samples.pop_front();
        }
        if hist.samples.len() < 2 {
            return 0.0;
        }
        let (_, h0, q0) = *hist.samples.front().unwrap();
        let (_, h1, q1) = *hist.samples.back().unwrap();
        let dh = h1.saturating_sub(h0);
        let dq = q1.saturating_sub(q0);
        if dq == 0 {
            return 0.0;
        }
        // Clamp to [0, 1] — defensive against counter quirks (e.g. a
        // hits-without-corresponding-queries metric bug).
        ((dh as f64 / dq as f64) as f32).clamp(0.0, 1.0)
    }

    /// Push this scrape's saturation gauges into the per-worker history and
    /// return their means over the window.
    ///
    /// A MEAN, deliberately, not a max: a max would pin the value at the peak
    /// for the whole window, so one blip would extend an exclusion for 30s
    /// rather than damp it — the opposite of the point. Not a p50 either,
    /// which would erase a spike present for under half the window even when
    /// it is real.
    ///
    /// Unlike its rate-computing siblings, a single sample is already a valid
    /// answer (the mean of one value), so there is no two-sample floor and no
    /// `0.0` warm-up. That matters: a freshly discovered worker would
    /// otherwise read "no queue, no cache pressure" for its first tick
    /// regardless of the truth.
    fn record_and_compute_means(
        &self,
        worker_id_num: u64,
        now: Instant,
        waiting: u32,
        kv_cache_usage: f32,
    ) -> (f32, f32) {
        let mut entry = self.gauge_history.entry(worker_id_num).or_default();
        let hist = entry.value_mut();
        hist.samples.push_back((now, waiting, kv_cache_usage));
        while hist.samples.len() > 1
            && now.saturating_duration_since(hist.samples.front().unwrap().0)
                > COMPLETION_RATE_WINDOW
        {
            hist.samples.pop_front();
        }
        let n = hist.samples.len() as f64;
        let (qs, ks) = hist.samples.iter().fold((0.0f64, 0.0f64), |(q, k), s| {
            (q + s.1 as f64, k + s.2 as f64)
        });
        ((qs / n) as f32, ((ks / n) as f32).clamp(0.0, 1.0))
    }

    pub fn latest(&self, worker_id_num: u64) -> MetricsSample {
        self.sample(worker_id_num).unwrap_or_default()
    }

    /// The last successful scrape, or `None` if this worker has never been
    /// scraped. A sample is stored only on success, so absence means "nothing
    /// known" — which [`Self::latest`] flattens into an all-zero sample that
    /// reads as an idle worker with an empty cache. Anything that decides on
    /// these values, rather than displaying them, wants this instead.
    pub fn sample(&self, worker_id_num: u64) -> Option<MetricsSample> {
        self.by_worker.get(&worker_id_num).map(|e| *e.value())
    }

    /// Record `sample` as this worker's latest scrape. Test-only: in production
    /// a sample arrives only through the poller, after a successful scrape.
    #[cfg(test)]
    pub(crate) fn record_sample_for_test(&self, worker_id_num: u64, sample: MetricsSample) {
        self.by_worker.insert(worker_id_num, sample);
    }

    /// Snapshot of the worker's served-models list. Empty `Default` when
    /// the worker isn't being scraped for /v1/models (no http_url) or no
    /// refresh has yet succeeded.
    pub fn models(&self, worker_id_num: u64) -> ModelsInfo {
        self.models_by_worker
            .get(&worker_id_num)
            .map(|e| e.value().clone())
            .unwrap_or_default()
    }

    /// Drop all per-worker state for a removed worker. Called by the
    /// coordinator on a `WorkerChange::Removed` event so the sibling maps
    /// don't accumulate stale entries across pod churn.
    // Wired up in Phase C; static-mode startup never removes workers.
    #[allow(dead_code)]
    pub fn forget(&self, worker_id_num: u64) {
        self.by_worker.remove(&worker_id_num);
        self.gauge_history.remove(&worker_id_num);
        self.models_by_worker.remove(&worker_id_num);
        self.completion_history.remove(&worker_id_num);
        self.prefix_cache_history.remove(&worker_id_num);
    }

    /// Milliseconds since the last successful scrape for this worker. If the
    /// worker has never been scraped (no `metrics_url`, or every attempt has
    /// failed), returns the time elapsed since kinference started — i.e. a
    /// steadily growing untrustworthy signal.
    pub fn age_ms_for(&self, worker_id_num: u64) -> u64 {
        let now = Instant::now();
        match self
            .by_worker
            .get(&worker_id_num)
            .and_then(|e| e.value().scraped_at)
        {
            Some(t) => now.saturating_duration_since(t).as_millis() as u64,
            None => now.saturating_duration_since(self.startup).as_millis() as u64,
        }
    }
}

/// Kick a one-shot `/v1/models` fetch as soon as a worker is registered,
/// in parallel with the recurring metrics poller. Without this, a
/// freshly-Added worker isn't routable on its live model set until the
/// next periodic refresh tick (default ~30s) — the cold window is mostly
/// wasted time. The warmup pushes the served-model set into the reactive
/// `WorkerRegistry::by_model` index and (best-effort) records the
/// detected engine on the `WorkerEntry`.
///
/// Fire-and-forget: any failure here just means we wait for the periodic
/// poller's next tick. No retries; we WARN once if the fetch errors.
#[allow(clippy::too_many_arguments)]
pub fn spawn_one_shot_warmup(
    cfg: WorkerConfig,
    registry: Arc<MetricsRegistry>,
    workers: Arc<crate::workers::WorkerRegistry>,
    local_tokenizer: Option<Arc<crate::local_tokenize::LocalTokenizerRegistry>>,
    eviction_tx: crate::eviction::EvictionTx,
    model_router: Arc<RwLock<HashMap<String, BTreeSet<PoolId>>>>,
    pool_id: PoolId,
    pool_name: String,
) {
    let Some(http_url) = cfg.http_url.clone() else {
        return; // can't warm up without /v1/models reachability
    };
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
        {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(
                    worker_id = cfg.worker_id_str,
                    error = %err,
                    "warmup: failed to build HTTP client; skipping (recurring poller will catch up)"
                );
                return;
            }
        };
        match fetch_models(&client, &http_url).await {
            Ok((models, detected_engine)) => {
                let names: Vec<String> = models.iter().map(|m| m.name.clone()).collect();
                let engine_str = detected_engine
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                tracing::info!(
                    worker_id = cfg.worker_id_str,
                    model_count = names.len(),
                    engine = %engine_str,
                    "warmup: /v1/models scrape succeeded"
                );
                match workers.set_served_models(cfg.worker_id_num, &names) {
                    // Widened/Narrowed are ROUTABLE outcomes, not faults: the
                    // fleet gained or is mid-rollout on a model (a LoRA adapter,
                    // typically). Index what this worker actually serves either
                    // way — evicting here walked an adapter rollout pod by pod
                    // and emptied the pool.
                    crate::workers::ServedModelsVerdict::Widened { added } => {
                        tracing::info!(
                            worker_id = cfg.worker_id_str,
                            ?added,
                            "warmup: worker serves models not previously seen in this set; \
                             widening the set's model list"
                        );
                        crate::monitor::associate_models(
                            &model_router,
                            pool_id,
                            &pool_name,
                            &names,
                        );
                    }
                    crate::workers::ServedModelsVerdict::Narrowed { missing } => {
                        tracing::info!(
                            worker_id = cfg.worker_id_str,
                            ?missing,
                            "warmup: worker is missing models others in this set serve; \
                             routing to it only for what it has"
                        );
                        crate::monitor::associate_models(
                            &model_router,
                            pool_id,
                            &pool_name,
                            &names,
                        );
                    }
                    crate::workers::ServedModelsVerdict::Accepted
                    | crate::workers::ServedModelsVerdict::Empty => {
                        crate::monitor::associate_models(
                            &model_router,
                            pool_id,
                            &pool_name,
                            &names,
                        );
                    }
                    crate::workers::ServedModelsVerdict::Mismatch { expected, observed } => {
                        tracing::warn!(
                            worker_id = cfg.worker_id_str,
                            ?expected,
                            ?observed,
                            "warmup: /v1/models reports a set that disagrees with the pool's canonical models; evicting worker"
                        );
                        let _ = eviction_tx.send(crate::eviction::EvictionRequest {
                            worker_id_num: cfg.worker_id_num,
                            reason: crate::eviction::EvictionReason::ModelMismatch {
                                expected,
                                observed,
                            },
                        });
                        return;
                    }
                }
                if let Some(eng) = detected_engine {
                    workers.set_engine(cfg.worker_id_num, eng);
                }
                // Trigger background tokenizer warmup for every served
                // model. `get_or_load` is idempotent — repeated calls
                // for the same model after first load are cache hits.
                if let Some(local_reg) = local_tokenizer.as_ref() {
                    for name in &names {
                        let reg = local_reg.clone();
                        let model = name.clone();
                        tokio::spawn(async move {
                            if let Err(err) = reg.warmup(&model).await {
                                tracing::warn!(
                                    model,
                                    error = %err,
                                    "tokenizer warmup failed; first request will pay load cost or fall back to HTTP"
                                );
                            }
                        });
                    }
                }
                registry.models_by_worker.insert(
                    cfg.worker_id_num,
                    ModelsInfo {
                        models,
                        detected_engine,
                        fetched_at: Some(Instant::now()),
                    },
                );
            }
            Err(err) => {
                tracing::warn!(
                    worker_id = cfg.worker_id_str,
                    http_url,
                    error = %err,
                    "warmup: /v1/models scrape failed; recurring poller will retry"
                );
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_metrics_poller(
    cfg: WorkerConfig,
    poll_ms: u64,
    models_refresh_secs: u64,
    local_tokenizer: Option<Arc<crate::local_tokenize::LocalTokenizerRegistry>>,
    registry: Arc<MetricsRegistry>,
    workers: Arc<crate::workers::WorkerRegistry>,
    eviction_tx: crate::eviction::EvictionTx,
    cancel: CancellationToken,
    model_router: Arc<RwLock<HashMap<String, BTreeSet<PoolId>>>>,
    pool_id: PoolId,
    pool_name: String,
) {
    let Some(metrics_url) = cfg.metrics_url.clone() else {
        tracing::info!(
            worker_id = cfg.worker_id_str,
            "No metrics_url configured; metrics fields will stay at 0"
        );
        return;
    };

    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
        {
            Ok(c) => c,
            Err(err) => {
                tracing::error!(
                    worker_id = cfg.worker_id_str,
                    error = %err,
                    "Failed to build HTTP client for metrics; poller exiting"
                );
                return;
            }
        };

        // Clamp to >= 1ms — `Duration::from_millis(0)` would spin
        // the recv loop with no delay.
        let poll_ms = poll_ms.max(1);
        let mut tick = tokio::time::interval(Duration::from_millis(poll_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut first_scrape = true;
        let mut warned_about_label = false;

        // Local view of which model_name to use as the /metrics label
        // filter. Initialized from config (set in static mode, empty in K8s
        // mode). Updated below from each /v1/models success: single-model
        // workers get filtered by the discovered name; multi-LoRA workers
        // leave it empty so the scraper aggregates across all loaded
        // models. Stays in sync without needing to read WorkerRegistry on
        // the hot path.
        let mut current_model_name: String = cfg.model_name.clone();

        // /v1/models refresh cadence, expressed as "every N metrics
        // ticks." Refresh on n=0 (first tick) and every N thereafter.
        // models_refresh_secs is in seconds; convert to the poll-tick
        // domain (one tick = poll_ms milliseconds).
        let refresh_every: u64 = ((models_refresh_secs * 1000) / poll_ms).max(1);
        let mut n: u64 = 0;
        let mut warned_about_models = false;
        // Models this poller has already attempted a tokenizer warmup for.
        // The recurring `/v1/models` refresh exists to catch models that
        // appear AFTER registration (LoRA hot-load), so we warm each name
        // at most once. Without this, a model whose tokenizer can't load
        // (not on a mount, un-fetchable Hub id) would re-attempt — and
        // re-warn — the failing load on every refresh. Score requests
        // still load on demand + fall back to HTTP, so skipping the
        // recurring re-attempt loses nothing.
        let mut warmup_attempted: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        tracing::info!(
            worker_id = cfg.worker_id_str,
            url = metrics_url,
            poll_ms,
            models_refresh_secs,
            "Metrics poller started"
        );

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {}
            }

            // /v1/models refresh — only if http_url is set, only every N
            // ticks. Sequential within the same task; bounded by reqwest
            // timeout so a slow response can't stall the metrics tick more
            // than 3s.
            if let Some(http_url) = cfg.http_url.as_deref()
                && n.is_multiple_of(refresh_every)
            {
                match fetch_models(&client, http_url).await {
                    Ok((models, detected_engine)) => {
                        // Push the served-model set into the reactive
                        // by_model index so requests for these names
                        // route here. Also forward the detected
                        // engine for display.
                        let names: Vec<String> = models.iter().map(|m| m.name.clone()).collect();
                        match workers.set_served_models(cfg.worker_id_num, &names) {
                            // See the warmup path: a changed model set is
                            // only fatal when it CONFLICTS.
                            crate::workers::ServedModelsVerdict::Widened { added } => {
                                tracing::info!(
                                    worker_id = cfg.worker_id_str,
                                    ?added,
                                    "poller: worker serves models not previously seen \
                                         in this set; widening the set's model list"
                                );
                                crate::monitor::associate_models(
                                    &model_router,
                                    pool_id,
                                    &pool_name,
                                    &names,
                                );
                            }
                            crate::workers::ServedModelsVerdict::Narrowed { missing } => {
                                tracing::info!(
                                    worker_id = cfg.worker_id_str,
                                    ?missing,
                                    "poller: worker is missing models others in this \
                                         set serve; routing to it only for what it has"
                                );
                                crate::monitor::associate_models(
                                    &model_router,
                                    pool_id,
                                    &pool_name,
                                    &names,
                                );
                            }
                            crate::workers::ServedModelsVerdict::Accepted
                            | crate::workers::ServedModelsVerdict::Empty => {
                                crate::monitor::associate_models(
                                    &model_router,
                                    pool_id,
                                    &pool_name,
                                    &names,
                                );
                            }
                            crate::workers::ServedModelsVerdict::Mismatch {
                                expected,
                                observed,
                            } => {
                                tracing::warn!(
                                    worker_id = cfg.worker_id_str,
                                    ?expected,
                                    ?observed,
                                    "poller: /v1/models reports a set that disagrees with the pool's canonical models; evicting worker"
                                );
                                let _ = eviction_tx.send(crate::eviction::EvictionRequest {
                                    worker_id_num: cfg.worker_id_num,
                                    reason: crate::eviction::EvictionReason::ModelMismatch {
                                        expected,
                                        observed,
                                    },
                                });
                                return;
                            }
                        }
                        if let Some(eng) = detected_engine {
                            workers.set_engine(cfg.worker_id_num, eng);
                        }
                        // Background tokenizer warmup for models we
                        // haven't tried yet — catches models that
                        // appear after registration (LoRA hot-load).
                        // Each name is attempted at most once per
                        // poller (see `warmup_attempted`), so a model
                        // whose tokenizer can't load doesn't re-warn
                        // on every refresh.
                        if let Some(local_reg) = local_tokenizer.as_ref() {
                            for name in &names {
                                if !warmup_attempted.insert(name.clone()) {
                                    continue; // already tried this model
                                }
                                let reg = local_reg.clone();
                                let model = name.clone();
                                tokio::spawn(async move {
                                    if let Err(err) = reg.warmup(&model).await {
                                        tracing::warn!(
                                            model,
                                            error = %err,
                                            "tokenizer warmup failed; on-demand \
                                             requests will fall back to HTTP /render"
                                        );
                                    }
                                });
                            }
                        }
                        // Refresh the local /metrics filter. Single
                        // model: filter by that name (precise). Zero
                        // models (engine still loading) or multi-LoRA:
                        // empty filter, take any sample.
                        current_model_name = if names.len() == 1 {
                            names[0].clone()
                        } else {
                            String::new()
                        };
                        registry.models_by_worker.insert(
                            cfg.worker_id_num,
                            ModelsInfo {
                                models,
                                detected_engine,
                                fetched_at: Some(Instant::now()),
                            },
                        );
                    }
                    Err(err) => {
                        if !warned_about_models {
                            tracing::warn!(
                                worker_id = cfg.worker_id_str,
                                http_url,
                                error = %err,
                                "/v1/models fetch failed; max_model_len + model_ids will stay at last good values (or empty)"
                            );
                            warned_about_models = true;
                        }
                    }
                }
            }
            n = n.wrapping_add(1);

            match scrape_once(
                &client,
                &metrics_url,
                &ALL_NAMES,
                &current_model_name,
                &cfg.worker_id_str,
                first_scrape,
                &mut warned_about_label,
            )
            .await
            {
                Ok(mut sample) => {
                    first_scrape = false;
                    // Push this scrape into the per-worker history and read
                    // back the windowed rate (slope across up to the last
                    // 30s of samples). Smoother than a 1-tick delta and
                    // doesn't flicker to 0 between sparse completions.
                    if let Some(now) = sample.scraped_at {
                        let (qm, km) = registry.record_and_compute_means(
                            cfg.worker_id_num,
                            now,
                            sample.waiting,
                            sample.kv_cache_usage,
                        );
                        sample.waiting_mean = qm;
                        sample.kv_cache_usage_mean = km;
                        sample.completion_rate_per_sec = registry.record_and_compute_rate(
                            cfg.worker_id_num,
                            now,
                            sample.completions_total,
                        );
                        // If the engine published the vLLM counter pair
                        // (queries > 0), derive the windowed hit rate
                        // from them — overriding any SGLang gauge value
                        // scrape_once may have stamped onto the sample.
                        // Counters take precedence because they're
                        // authoritative within our chosen 30s window;
                        // the gauge's averaging is engine-internal and
                        // not necessarily aligned.
                        if sample.prefix_cache_queries_total > 0 {
                            sample.prefix_cache_hit_rate = registry.record_and_compute_hit_rate(
                                cfg.worker_id_num,
                                now,
                                sample.prefix_cache_hits_total,
                                sample.prefix_cache_queries_total,
                            );
                        }
                    }
                    registry.by_worker.insert(cfg.worker_id_num, sample);
                }
                Err(err) => {
                    // `{err:#}` (anyhow alternate Display) prints the
                    // full cause chain on one line, e.g.
                    //   GET http://...: error sending request for url:
                    //     connection reset by peer
                    // The cause (timeout / refused / reset / DNS) is
                    // the load-bearing part for diagnosing port-forward
                    // hiccups; the bare context "GET <url>" alone is
                    // useless.
                    tracing::warn!(
                        worker_id = cfg.worker_id_str,
                        error = %format_args!("{err:#}"),
                        "metrics scrape failed; keeping previous sample"
                    );
                }
            }
        }
    });
}

async fn scrape_once(
    client: &reqwest::Client,
    url: &str,
    names: &MetricNames,
    model_name: &str,
    worker_id_str: &str,
    first_scrape: bool,
    warned_about_label: &mut bool,
) -> Result<MetricsSample> {
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        anyhow::bail!("HTTP {}", resp.status());
    }
    let body = resp.text().await.context("read body")?;
    // Workaround for prometheus-parse rejecting colons in metric names. Both
    // engines historically use a colon prefix; v0.5.4+ SGLang already emits
    // underscores so the second replace is a no-op there.
    let body = body.replace("vllm:", "vllm_").replace("sglang:", "sglang_");
    let scrape = prometheus_parse::Scrape::parse(body.lines().map(|l| Ok(l.to_owned())))
        .context("parsing Prometheus body")?;

    if first_scrape {
        let mut names: Vec<&str> = scrape.samples.iter().map(|s| s.metric.as_str()).collect();
        names.sort();
        names.dedup();
        let mut label_keys: std::collections::BTreeSet<String> = Default::default();
        for s in &scrape.samples {
            for k in s.labels.iter() {
                label_keys.insert(k.0.to_string());
            }
        }
        tracing::debug!(
            worker_id = worker_id_str,
            metric_count = names.len(),
            ?label_keys,
            "First metrics scrape parsed"
        );
        tracing::trace!(
            worker_id = worker_id_str,
            "Available metrics on first scrape: {}",
            names.join(", ")
        );
    }

    // First-match-wins lookup. If the engine has a `model_label`, filter by it;
    // otherwise accept the first matching sample regardless of labels.
    let pick_first = |candidates: &[&'static str]| -> Option<f64> {
        for name in candidates {
            let found = scrape
                .samples
                .iter()
                .filter(|s| s.metric == *name)
                .find(|s| match names.model_label {
                    // Skip the label filter when we don't have a model_name
                    // yet (typical in K8s mode before the first /v1/models
                    // succeeds, or for multi-LoRA workers where we
                    // intentionally aggregate across all loaded models).
                    Some(label) if !model_name.is_empty() => {
                        s.labels.get(label) == Some(model_name)
                    }
                    _ => true,
                })
                .and_then(|s| match s.value {
                    prometheus_parse::Value::Gauge(v)
                    | prometheus_parse::Value::Counter(v)
                    | prometheus_parse::Value::Untyped(v) => Some(v),
                    _ => None,
                });
            if found.is_some() {
                return found;
            }
        }
        None
    };

    let waiting = pick_first(names.waiting);
    let running = pick_first(names.running);
    let swapped = pick_first(names.swapped);
    let kv_cache = pick_first(names.kv_cache_usage);

    // Completion counter: sum across all label combinations (e.g.
    // finished_reason=stop|length|...) that match this worker's model_label.
    // Histogram `_count` series are exposed as Counter values by
    // prometheus-parse, so the same iter handles both candidate names.
    let pick_sum = |candidates: &[&'static str]| -> Option<f64> {
        for name in candidates {
            let mut sum = 0.0;
            let mut found = false;
            for s in scrape.samples.iter().filter(|s| s.metric == *name) {
                // Same "skip filter when model_name is unknown" semantics
                // as the gauge picker above. For a multi-LoRA worker
                // running with model_name empty, we sum across all loaded
                // models — the right behavior for an aggregate counter.
                if let Some(label) = names.model_label
                    && !model_name.is_empty()
                    && s.labels.get(label) != Some(model_name)
                {
                    continue;
                }
                match s.value {
                    prometheus_parse::Value::Counter(v)
                    | prometheus_parse::Value::Gauge(v)
                    | prometheus_parse::Value::Untyped(v) => {
                        sum += v;
                        found = true;
                    }
                    _ => {}
                }
            }
            if found {
                return Some(sum);
            }
        }
        None
    };
    let completions_total = pick_sum(names.completions).unwrap_or(0.0).max(0.0) as u64;

    // Prefix-cache hit-rate inputs. vLLM (≥ 0.6) emits a counter pair;
    // we read both and let the poller derive the windowed ratio. SGLang
    // emits a single gauge `sglang:cache_hit_rate` that's already a
    // 0..1 ratio — the poller reads that directly via the sample, no
    // history needed. Either pair-of-counters OR the gauge is present;
    // never both. `pick_sum` reuses the model_label filter and the
    // multi-label aggregation we already need.
    let prefix_cache_hits_total = pick_sum(names.prefix_cache_hits).unwrap_or(0.0).max(0.0) as u64;
    let prefix_cache_queries_total =
        pick_sum(names.prefix_cache_queries).unwrap_or(0.0).max(0.0) as u64;

    // Only warn about a model_name label mismatch when:
    //  - the engine expects a model_name label at all (vLLM/SGLang do; some
    //    label-less engines wouldn't),
    //  - AND we have a filter to compare against (in K8s mode the filter
    //    starts empty and gets populated by /v1/models; the empty-filter
    //    path takes any sample and doesn't warrant a warning).
    if !*warned_about_label
        && names.model_label.is_some()
        && !model_name.is_empty()
        && waiting.is_none()
        && running.is_none()
        && swapped.is_none()
        && kv_cache.is_none()
    {
        tracing::warn!(
            worker_id = worker_id_str,
            model_name,
            "no metrics matched the model_name label; the value we're filtering on \
             (from config or /v1/models) does not match the label value the engine \
             emits in /metrics"
        );
        *warned_about_label = true;
    }

    // SGLang gauge fallback for the hit rate: a Gauge | Counter | Untyped
    // match arm, so any future engine that publishes the rate directly Just
    // Works.
    // Read here so we can stamp it onto the sample; the poller may
    // overwrite this with the windowed-counter rate if the vLLM
    // counter pair was present (it takes precedence — counters are
    // authoritative within the monitor's window choice).
    let sglang_hit_rate_gauge = names
        .prefix_cache_hit_rate_gauge
        .iter()
        .find_map(|name| {
            scrape.samples.iter().find_map(|s| {
                if &s.metric == name {
                    match s.value {
                        prometheus_parse::Value::Gauge(v)
                        | prometheus_parse::Value::Counter(v)
                        | prometheus_parse::Value::Untyped(v) => Some(v.clamp(0.0, 1.0) as f32),
                        _ => None,
                    }
                } else {
                    None
                }
            })
        })
        .unwrap_or(0.0);

    Ok(MetricsSample {
        waiting: waiting.unwrap_or(0.0).max(0.0) as u32,
        running: running.unwrap_or(0.0).max(0.0) as u32,
        swapped: swapped.unwrap_or(0.0).max(0.0) as u32,
        kv_cache_usage: kv_cache.unwrap_or(0.0).max(0.0) as f32,
        completions_total,
        // Rate is computed by the poller from the delta against the previous
        // sample; we leave it zero here.
        completion_rate_per_sec: 0.0,
        // Likewise the windowed means: a single scrape has no history to
        // average over, so the poller fills these in from `gauge_history`
        // once this sample joins it.
        waiting_mean: 0.0,
        kv_cache_usage_mean: 0.0,
        prefix_cache_hits_total,
        prefix_cache_queries_total,
        // SGLang's already-derived rate. If the vLLM counter pair was
        // also present (hits + queries > 0), the poller overwrites
        // this with the windowed value computed from the history.
        prefix_cache_hit_rate: sglang_hit_rate_gauge,
        scraped_at: Some(Instant::now()),
    })
}

/// Fetch `/v1/models` from the engine's OpenAI-compat HTTP server and parse
/// out `id` + `max_model_len` + `owned_by` per entry. Ignores unknown
/// fields (created, permission, etc.) and tolerates missing
/// `max_model_len` (`Option<u32>::None`).
///
/// `owned_by` is used to identify the engine (vLLM vs SGLang). All entries
/// in a single response share the same owner — pick the first non-empty.
async fn fetch_models(
    client: &reqwest::Client,
    http_url: &str,
) -> Result<(Vec<ModelEntry>, Option<Engine>)> {
    let url = format!("{}/v1/models", http_url.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        anyhow::bail!("HTTP {}", resp.status());
    }
    #[derive(serde::Deserialize)]
    struct ModelsResponse {
        data: Vec<ModelData>,
    }
    #[derive(serde::Deserialize)]
    struct ModelData {
        id: String,
        max_model_len: Option<u32>,
        #[serde(default)]
        owned_by: Option<String>,
    }
    let body: ModelsResponse = resp.json().await.context("decoding /v1/models response")?;
    let detected_engine = body
        .data
        .iter()
        .find_map(|m| m.owned_by.as_deref().and_then(engine_from_owned_by));
    let entries: Vec<ModelEntry> = body
        .data
        .into_iter()
        .map(|m| ModelEntry {
            name: m.id,
            max_model_len: m.max_model_len,
        })
        .collect();
    Ok((entries, detected_engine))
}

#[cfg(test)]
mod tests;
