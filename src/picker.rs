//! Wait-time predictor + top-K-with-tie-extension endpoint picker.
//!
//! Originally ported from
//! `/Users/william/devel/gw-pc/examples/plugins/llm_prefix_selector/src/plugin.rs`
//! (the gw-pc `llm-prefix-selector` WASM plugin); the algorithm has
//! since diverged — kinference adds a dedicated `decode_contention_ms`
//! term (driven by `forward_pass_ms`) and dropped the older
//! discount-floor/ceiling lerp in favor of a strict
//! `(1 - progress)` shape. The top-K / tie-extension / cache-
//! replication logic is unchanged from the original port.
//!
//! Why preserve the algorithm exactly: divergent picks across the two
//! paths would be confusing in incident review and would make A/B
//! comparison across deployments
//! impossible. Any future tuning should land here and in the plugin
//! together.

use std::fmt::Write;
use std::time::Instant;

use crate::config::{PickerConfig, PredictorConfig};
use crate::inflight_prefixes::InflightSummary;

/// Worker-score row produced by `monitor::scoring`. Pre-Pick/Observe
/// refactor this was a proto-generated message (the `Score` RPC
/// returned `repeated WorkerScore`); now it's a plain Rust struct
/// since no caller needs it over the wire — the gRPC interface returns
/// `chosen_endpoint` and that's it.
///
/// It lives HERE rather than in `monitor` because every operation on it is a
/// function in this module — `predicted_wait_ms`, `compute_queue_cost`,
/// `pick_endpoint`, `format_score_table`, `PickResult`. Defining it beside
/// them is what lets this module, which does pure arithmetic and no I/O, stop
/// depending on the request-path service that happens to fill it in.
///
/// The shape is a hand copy of the fields `metrics::MetricsSample` exposes;
/// see `monitor::scoring::worker_score`, which is the one place that
/// projection happens.
#[derive(Debug, Clone)]
pub struct WorkerScore {
    pub name: String,
    pub endpoint: String,
    pub matched_blocks: u32,
    pub completion_rate_per_sec: f32,
    pub queue_waiting: u32,
    pub queue_running: u32,
    pub queue_swapped: u32,
    pub kv_cache_usage: f32,
    pub block_size: u32,
    pub total_blocks: u32,
}

/// Per-term breakdown of the predicted wait, in milliseconds. Kept
/// alongside the total so the log can show what drove a decision.
#[derive(Debug, Clone)]
pub struct WaitPrediction {
    pub base_ms: f32,
    pub q_ms: f32,
    /// The effective per-entry queue rate used to derive `q_ms`.
    /// Diagnostic only — lets the log surface whether queue cost came
    /// from the configured baseline or the cold-saturation fallback.
    pub q_rate_ms_per_entry: f32,
    pub prefill_ms: f32,
    pub cache_ms: f32,
    /// U-shaped latency penalty driven by `WorkerScore.queue_running`
    /// (vLLM's `num_requests_running`). Captures the "scheduler-tick
    /// danger zone" where a worker has enough decode load to contend
    /// for GPU but not enough to absorb new prefills into a running
    /// batch — empirically peaks at qr=2. See [`q_running_penalty_ms`].
    pub q_running_ms: f32,
    /// Flat per-active-decode cost: `(q_running + q_inflight) ×
    /// forward_pass_ms`. Models the marginal cost to the new request
    /// of joining a batch with N decodes in progress — separate from
    /// any prefill-quadratic contribution. Inert when
    /// `PredictorConfig.forward_pass_ms == 0`.
    pub decode_contention_ms: f32,
}

impl WaitPrediction {
    pub fn total_ms(&self) -> f32 {
        self.base_ms
            + self.q_ms
            + self.prefill_ms
            + self.cache_ms
            + self.q_running_ms
            + self.decode_contention_ms
    }
}

/// Quadratic prefill-time estimator:
/// ```text
/// prefill_ms = a × n_uncached + c × n_uncached × n_total
/// ```
/// where `a = predictor.attention_uncached_block_ms` (linear per-block
/// setup cost: weight fetch, KV write, etc.) and
/// `c = predictor.attention_uncached_per_every_block_ms` (attention
/// cost: each uncached query attends to every token in the prompt,
/// so scales with prompt length).
///
/// Used in three places — keep these in sync if the formula changes:
/// 1. `predicted_wait_ms`: our new request's own prefill cost.
/// 2. `compute_queue_cost`: estimated_prefill_ms for each inflight,
///    using the inflight's own `total_at_dispatch`. Drives progress
///    calculation and per-position contention contribution.
/// 3. `inflight_discount`: same as above (consumed inside the helper).
pub fn prefill_ms_for(predictor: &PredictorConfig, uncached: u32, total: u32) -> f32 {
    let u = uncached as f32;
    let t = total as f32;
    predictor.attention_uncached_block_ms * u
        + predictor.attention_uncached_per_every_block_ms * u * t
}

/// Expected block overlap between our prompt and a worker's KV cache
/// *at the time our prompt actually runs*, given the worker's queue
/// of pending dispatches we've already routed to it.
///
/// `per_dispatch` is the per-dispatch overlap count `|P_new ∩ Pᵢ|`,
/// in dispatch order (oldest first). `kv_match` is the radix-tree
/// match count `|P_new ∩ KV|` — the current snapshot. `alpha` is
/// the eviction-decay factor: each layer "one request older" gets
/// multiplied by an additional `alpha`.
///
/// The model, with layers indexed newest-first (L₀ = newest pending,
/// L_k = the radix-tree KV layer):
///
/// ```text
/// overlap = α⁰ · max(0, o₀ − M₀)
///         + α¹ · max(0, o₁ − M₁)
///         + α² · max(0, o₂ − M₂)
///         + …
///         + αᵏ · max(0, o_k − M_k)
///
/// where oᵢ = |P_new ∩ Lᵢ|  and  Mᵢ = max(o₀, o₁, …, oᵢ₋₁) ,  M₀ = 0
/// ```
///
/// Each block of `P_new` is counted exactly once, at the weight of
/// its *most-recent source*. `Mᵢ` is the longest prefix already
/// covered by every layer strictly newer than `Lᵢ`; the
/// `max(0, oᵢ − Mᵢ)` clamp ensures a layer can only *extend* the
/// covered prefix, never remove blocks.
///
/// Limit behavior:
/// - `α = 1` (no eviction): each layer contributes its incremental
///   prefix beyond all newer layers, so the sum equals the longest
///   matched prefix across all sources — i.e.
///   `max(o₀, o₁, …, o_k, kv_match)`. Matches the "100% retention →
///   union of all cached prefixes" intuition.
/// - `α = 0`: only the L₀ term survives, so the sum collapses to
///   the most-recent dispatch's overlap (or `kv_match` if there are
///   no pending dispatches).
/// - `α ∈ (0, 1)`: each "new" block (one extending the covered
///   prefix) is weighted by `αⁱ` where `i` is its layer index from
///   newest. Older layers progressively discounted. Sum is
///   monotonic in α (raising α never decreases overlap).
pub fn compute_layered_overlap(per_dispatch: &[f32], kv_match: f32, alpha: f32) -> f32 {
    // Iterate newest-first so the freshest dispatch is L₀ (weight
    // α⁰ = 1). `cum_max` is Mᵢ — the longest covered prefix from
    // strictly newer layers; a layer can only add blocks beyond
    // that, never debit (hence the `.max(0.0)` clamp on each
    // incremental term).
    let mut total = 0.0_f32;
    let mut cum_max = 0.0_f32;
    let mut weight = 1.0_f32;
    for &overlap in per_dispatch.iter().rev() {
        let new_blocks = (overlap - cum_max).max(0.0);
        total += weight * new_blocks;
        if overlap > cum_max {
            cum_max = overlap;
        }
        weight *= alpha;
    }
    // Final layer: radix-tree KV cache.
    let new_blocks = (kv_match - cum_max).max(0.0);
    total += weight * new_blocks;
    total
}

/// Scale each dispatch's raw overlap by its `progress` so the layered
/// expected-overlap reflects only blocks that have actually been
/// committed to the worker's prefix cache by the time our new request
/// arrives. The remaining (uncomputed) portion of each inflight's
/// prefix doesn't give us a cache hit.
///
/// `progress[i] ∈ [0, 1]`:
/// - `0` for inflight items vLLM hasn't started prefilling yet
///   (overflow past `inflight_concurrent_capacity`, or freshly
///   dispatched with elapsed=0)
/// - `1` for items that have finished prefilling (elapsed ≥
///   estimated_prefill); all their blocks should be in cache.
///
/// The caller computes `progress[i]` per inflight using the
/// same `elapsed / estimated_prefill_ms` formula the queue-cost model
/// uses, and zeroes out positions beyond `cfg.inflight_concurrent_capacity`.
/// Length of `per_dispatch_progress` must equal `per_dispatch_overlap`.
pub fn scale_per_dispatch_by_progress(
    per_dispatch_overlap: &[f32],
    per_dispatch_progress: &[f32],
) -> Vec<f32> {
    debug_assert_eq!(
        per_dispatch_overlap.len(),
        per_dispatch_progress.len(),
        "progress must be parallel to overlap"
    );
    per_dispatch_overlap
        .iter()
        .zip(per_dispatch_progress.iter())
        .map(|(o, p)| o * p.clamp(0.0, 1.0))
        .collect()
}

/// Predicted wait (ms) for sending this prompt to worker `w`. Sum of
/// four components:
///
///   base_ms    = constant per-request intercept
///   q_ms       = time-progressed inflight cost + unknown-queue cost
///   prefill_ms = prefill_ms_for(uncached_blocks, total_blocks)
///                = a × u + c × u × t  (see `prefill_ms_for`)
///   cache_ms   = cache_pressure_penalty_ms * kv_cache_usage
///
/// Queue cost — see `compute_queue_cost` for the detail — is the sum of
/// two contributions:
///
/// - **Remaining-prefill inflight cost**: each in-flight dispatch we
///   have on the worker contributes
///   `(1 - progress) × prefill_ms_for(uncached_at_dispatch, total_at_dispatch)`,
///   i.e. the fraction of its prefill work still in the future.
///   Progress is `elapsed / estimated_prefill`, clamped to [0, 1].
///   At progress=1 the dispatch has finished prefilling and contributes
///   0 here — its decode-time cost is modeled by the separate
///   `decode_contention_ms` term in `predicted_wait_ms`. Inflights at
///   positions > N (`cfg.inflight_concurrent_capacity`) haven't been
///   admitted yet and contribute the full prefill cost regardless of
///   elapsed time.
///
/// - **Unknown-queue cost**: queue entries vLLM reports as waiting
///   that we have no inflight-dispatch record for
///   (`max(0, queue_waiting - inflight_count)`) at
///   `q_rate_ms_per_entry`. Cold-saturation can promote that rate to
///   `cfg.saturated_cold_ms_per_entry`.
///
/// `now` is the score-time clock reading, passed in for testability —
/// production callers grab `Instant::now()` once at the top of the
/// pick critical section so every worker's discount is computed
/// against the same time anchor.
///
/// `total_blocks == 0` (metric unavailable, or prompt smaller than one
/// block) treats uncached_blocks as 0 rather than producing NaN.
pub fn predicted_wait_ms(
    w: &WorkerScore,
    predictor: &PredictorConfig,
    inflight_summaries: &[InflightSummary],
    now: Instant,
) -> WaitPrediction {
    let q = compute_queue_cost(w, predictor, inflight_summaries, now);

    let uncached_blocks = w.total_blocks.saturating_sub(w.matched_blocks);
    let prefill_ms = prefill_ms_for(predictor, uncached_blocks, w.total_blocks);

    let cache_ms = predictor.cache_pressure_penalty_ms * w.kv_cache_usage;

    // Shadow inflight count = picks we've made on this worker that
    // haven't yet shown up in the engine's scraped metrics. Fold
    // those into the U-shape lookup so a worker we just dispatched
    // to doesn't look stale-clean at qr=0 — see comment on
    // `q_running_penalty_ms`.
    let q_inflight = inflight_summaries.len() as u32;
    let q_running_ms = q_running_penalty_ms(w.queue_running, q_inflight, predictor);

    // Flat per-active-decode contention cost. Independent of prefill
    // quadratic — captures the marginal forward-pass cost the new
    // request pays once admitted to a batch with N decodes in flight.
    // Uses `effective_qr = q_running + q_inflight` (same as the
    // U-shape penalty) to include just-dispatched requests the
    // engine hasn't yet surfaced in the scraped metric.
    let effective_qr = w.queue_running.saturating_add(q_inflight) as f32;
    let decode_contention_ms = effective_qr * predictor.forward_pass_ms;

    WaitPrediction {
        base_ms: predictor.base_ms,
        q_ms: q.q_ms,
        q_rate_ms_per_entry: q.q_rate_ms_per_entry,
        prefill_ms,
        cache_ms,
        q_running_ms,
        decode_contention_ms,
    }
}

/// U-shaped latency penalty driven by the worker's *effective*
/// in-decode request count — vLLM's scraped `num_requests_running`
/// PLUS our own shadow inflight count (picks we've dispatched but
/// the engine hasn't yet reflected in metrics).
///
/// Why fold both signals: the engine's metric refreshes every ~50ms.
/// In that window the picker can route to a worker that *looks*
/// qr=0 because the engine hasn't yet acknowledged a recent dispatch.
/// Two concurrent picks see qr=0 inside the same refresh interval
/// and both target the same "clean" worker, creating self-induced
/// contention that the next refresh would have prevented. Treating
/// `effective_qr = qr + q_inflight` closes that staleness window —
/// a worker we just shipped a request to is no longer a qr=0 target.
///
/// Empirically, on a cache-poor concurrent workload the probability
/// of a new prefill landing in a slow-mode TTFT (>3s vs ~1.6s
/// median) follows a U-shape in `effective_qr`:
///
/// - **effective_qr = 0**: ~7% slow-mode rate. Worker is idle, prefill
///   runs alone, no GPU contention.
/// - **effective_qr = 1-2**: ~20% slow-mode rate. Worker is decoding
///   1-2 requests — enough to compete for GPU bandwidth but not
///   enough to fold the new prefill into the running batch's forward
///   pass. This is the "contention zone."
/// - **effective_qr = 3-4**: ~18% slow-mode rate. Starting to recover.
/// - **effective_qr ≥ 5**: ~12% slow-mode rate. Worker's GPU is hot
///   enough that the new prefill can ride along in the existing
///   batch's forward pass for a near-free first token.
///
/// We model the *expected* TTFT cost of landing in the contention
/// zone as a piecewise-linear tent: ramps from 0 at `effective_qr=0`
/// to `peak` at `peak_qr`, then declines back to 0 at `falloff_qr`.
/// At or beyond `falloff_qr` the term is 0 (ride-along regime).
///
/// Setting `peak <= 0.0` disables the term entirely.
fn q_running_penalty_ms(qr: u32, q_inflight: u32, predictor: &PredictorConfig) -> f32 {
    let peak = predictor.q_running_penalty_peak_ms;
    let effective_qr = qr.saturating_add(q_inflight);
    if peak <= 0.0 || effective_qr == 0 {
        return 0.0;
    }
    // Guard against degenerate config: peak_qr must be >= 1 (else
    // the rising-edge denominator is 0); falloff must be > peak_qr
    // (else the falling-edge denominator is 0 or negative).
    let peak_qr = predictor.q_running_penalty_peak_qr.max(1) as f32;
    let falloff = predictor
        .q_running_penalty_falloff_qr
        .max(predictor.q_running_penalty_peak_qr.saturating_add(1)) as f32;
    let qr_f = effective_qr as f32;
    if qr_f >= falloff {
        return 0.0;
    }
    if qr_f <= peak_qr {
        // Rising edge: 0 at effective_qr=0, peak at peak_qr.
        peak * qr_f / peak_qr
    } else {
        // Falling edge: peak at peak_qr, 0 at falloff_qr.
        peak * (falloff - qr_f) / (falloff - peak_qr)
    }
}

/// All the intermediate values in the queue-cost calculation. Used
/// both inside `predicted_wait_ms` (to drive the `q_ms` term) and at
/// trace logging sites (to surface the constituent pieces so operators
/// can see exactly how the per-position discount works out per
/// worker).
#[derive(Debug, Clone)]
pub struct QueueCostBreakdown {
    /// Per-entry queue rate after the cold-saturation check. Equals
    /// `cfg.queue_ms_per_entry` in the common case; promoted to
    /// `cfg.saturated_cold_ms_per_entry` when the worker has no
    /// completion history AND looks cache-saturated AND has waiting
    /// queue depth.
    pub q_rate_ms_per_entry: f32,
    /// `q_rate_ms_per_entry * w.queue_waiting`. Diagnostic only — the
    /// legacy "every queue entry costs the same" reading, retained in
    /// the CSV for offline replay.
    pub q_ms_constant: f32,
    /// `Σ position_contribs[i].contrib_ms + unknown_q_count * q_rate`.
    /// This IS the q_ms that goes into the predictor; the field name
    /// is kept for backward-compat with the trace log + CSV.
    pub q_ms_workload: f32,
    /// q_ms used by the predictor. Equal to `q_ms_workload` in the
    /// current model; the separate field is retained so future model
    /// revisions can distinguish "model output" from "model
    /// internals" without changing the field shape.
    pub q_ms: f32,
    /// Total uncached blocks summed across our inflight dispatches.
    /// CSV preserves this for offline analysis.
    pub inflight_workload_blocks: u32,
}

/// Time-progressed discount for one inflight slot.
///
/// `discount` represents the fraction of this inflight's prefill cost
/// of this inflight's prefill that's still in the future.
///
/// At `progress=0` (just admitted, no prefill done) → discount = 1.0
/// (full prefill cost ahead). At `progress=1` (prefill complete,
/// now decoding) → discount = 0.0 (no remaining prefill cost; the
/// per-decode contention is modeled separately by the
/// `decode_contention_ms` term in `predicted_wait_ms`).
///
/// `position` is 1-indexed in the oldest-first ordering. Positions
/// beyond `n` (batch capacity) haven't been admitted yet → their
/// elapsed time reflects queueing wait, not prefill progress, so they
/// get the full prefill cost ahead (discount = 1.0).
///
/// `estimated_prefill_ms = 0` (warm-hit inflight with nothing to
/// prefill) → discount = 0.0 immediately: no prefill work to contend
/// over.
fn inflight_discount(
    position: u32,
    n: u32,
    elapsed_ms: f32,
    estimated_prefill_ms: f32,
) -> (f32, f32) {
    // Position cap. Position > N means this inflight hasn't been
    // admitted to the batch yet; its elapsed time is queueing wait,
    // not progress → full prefill cost ahead.
    if position > n {
        return (0.0, 1.0);
    }
    if estimated_prefill_ms <= 0.0 {
        // No prefill work to wait through → discount = 0 (nothing left).
        return (1.0, 0.0);
    }
    let progress = (elapsed_ms / estimated_prefill_ms).clamp(0.0, 1.0);
    let discount = (1.0 - progress).clamp(0.0, 1.0);
    (progress, discount)
}

pub fn compute_queue_cost(
    w: &WorkerScore,
    predictor: &PredictorConfig,
    inflight_summaries: &[InflightSummary],
    now: Instant,
) -> QueueCostBreakdown {
    let mut q_rate_ms_per_entry = predictor.queue_ms_per_entry;
    if w.completion_rate_per_sec == 0.0
        && w.kv_cache_usage >= predictor.cache_pressure_saturated_threshold
        && w.queue_waiting > 0
        && predictor.saturated_cold_ms_per_entry > q_rate_ms_per_entry
    {
        // No completion history AND cache says we're stuck AND there's
        // actually a queue to drain — cold-saturation fallback applies.
        q_rate_ms_per_entry = predictor.saturated_cold_ms_per_entry;
    }

    let q_ms_constant = q_rate_ms_per_entry * w.queue_waiting as f32;

    let inflight_count = inflight_summaries.len() as u32;
    let inflight_workload_blocks: u32 = inflight_summaries
        .iter()
        .map(|s| s.uncached_at_dispatch as u64)
        .sum::<u64>()
        .min(u32::MAX as u64) as u32;
    let unknown_q_count = w.queue_waiting.saturating_sub(inflight_count);

    // When `queue_waiting > 0` the batch is at capacity (or vLLM is
    // otherwise declining admission), so every inflight is treated
    // as past-capacity (full prefill cost ahead, regardless of
    // elapsed time).
    let capacity = if w.queue_waiting > 0 {
        // Effective N=0: every position is > N → everyone treated
        // as queued behind, full prefill cost ahead.
        0
    } else {
        predictor.inflight_concurrent_capacity
    };

    let mut position_cost_ms = 0.0_f32;
    for (idx, summary) in inflight_summaries.iter().enumerate() {
        let position = (idx as u32).saturating_add(1);
        // Estimate this inflight's prefill cost using ITS own
        // `total_at_dispatch` (not our new request's total_blocks).
        // Quadratic formula: attention cost scales with the inflight's
        // own prompt length, not ours.
        let estimated_prefill_ms = prefill_ms_for(
            predictor,
            summary.uncached_at_dispatch,
            summary.total_at_dispatch,
        );
        let elapsed_ms = now
            .checked_duration_since(summary.dispatched_at)
            .map(|d| d.as_millis() as f32)
            .unwrap_or(0.0);
        let (_progress, discount) =
            inflight_discount(position, capacity, elapsed_ms, estimated_prefill_ms);
        // Contribution = discount × estimated_prefill_ms. (Same units
        // as the queue cost we're summing into.)
        position_cost_ms += discount * estimated_prefill_ms;
    }

    let unknown_cost_ms = unknown_q_count as f32 * q_rate_ms_per_entry;
    let q_ms_workload = position_cost_ms + unknown_cost_ms;
    let q_ms = q_ms_workload;

    QueueCostBreakdown {
        q_rate_ms_per_entry,
        q_ms_constant,
        q_ms_workload,
        q_ms,
        inflight_workload_blocks,
    }
}

/// Outcome of a pick. `Some(idx)` is the chosen index into the input
/// slice; `None` means there were no scoreable workers (empty input).
///
/// `inflight` is a parallel slice (same length, same order as
/// `scores`) of per-worker in-flight pick counts. It is used SOLELY
/// as a tie-breaker within the top-K pool: workers tied on predicted
/// wait but with lower in-flight counts win over those with higher
/// counts. Inflight does not enter the predicted-wait calculation
/// itself. Pass a slice of zeros when in-flight tracking is disabled
/// — the tie-break will be a no-op and behavior matches the legacy
/// picker.
/// Outcome of a successful pick. `worker` is the chosen
/// `WorkerScore`; `replication_fired` is `true` when the cache-
/// replication override swapped the picker's normal pick for a
/// cold-idle alternative (see [`cache_replication_override`]).
/// Captured in the CSV so offline analysis can separate baseline
/// picks from replication-driven ones — and so the analyze script
/// can compute "what fraction of picks fired replication, and were
/// those picks systematically worse?".
pub struct PickResult<'a> {
    pub worker: &'a WorkerScore,
    pub replication_fired: bool,
}

pub fn pick_endpoint<'a>(
    scores: &'a [WorkerScore],
    predictor: &PredictorConfig,
    picker: &PickerConfig,
    inflight: &[u32],
    inflight_workload: &[Vec<InflightSummary>],
    kv_blocks_per_worker: &[u32],
    now: Instant,
) -> Option<PickResult<'a>> {
    if scores.is_empty() {
        return None;
    }
    debug_assert_eq!(
        scores.len(),
        inflight.len(),
        "inflight must be parallel to scores"
    );
    debug_assert_eq!(
        scores.len(),
        inflight_workload.len(),
        "inflight_workload must be parallel to scores"
    );

    // (index, total predicted wait_ms) for each worker. Unlike the WASM
    // plugin we don't have a separate "candidate" set from the gateway
    // to join against — every WorkerScore we receive is a candidate by
    // construction (it's already filtered to workers serving the
    // requested model via `WorkerRegistry::workers_for_model`).
    let mut scored: Vec<(usize, f32)> = scores
        .iter()
        .enumerate()
        .map(|(i, w)| {
            (
                i,
                predicted_wait_ms(w, predictor, &inflight_workload[i], now).total_ms(),
            )
        })
        .collect();

    // Top-K random anti-dogpiling. Sort by predicted wait ascending,
    // form a pool of the best `effective_k` candidates, then uniform
    // random pick within the pool.
    //
    // Critical: the pool extends past `effective_k` to include any
    // candidates **tied at the K-th cutoff**. Without that, a cold
    // fleet (all workers with identical zero-stats and no prefix
    // match) would have bit-identical predicted waits across all N
    // candidates — `sort_by` is stable, so position 0 would always be
    // the first endpoint, and K=1 would pile every request there.
    // Extending the pool through ties recovers the "ties at min get
    // uniform random pick" behavior as a degenerate case of top-K.
    //
    // partial_cmp + unwrap_or(Equal) protects against NaN (which would
    // sort to the end and never be picked unless every candidate is
    // NaN — degenerate but graceful).
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let n = scored.len();
    let effective_k = std::cmp::max(
        picker.top_k_min as usize,
        ((n as f32) * picker.top_k_fraction).ceil() as usize,
    )
    .max(1)
    .min(n);
    // Cutoff: the larger of (a) the K-th best predicted_ms (for the
    // top-K + tie-extension semantics) and (b) the best predicted_ms
    // plus `tie_tolerance_ms` (for soft equality — workers within
    // the tolerance band are eligible for the inflight tie-break
    // even if not strictly tied). With tie_tolerance_ms == 0 this
    // reduces to the pre-tolerance behavior.
    let cutoff_wait = scored[effective_k - 1]
        .1
        .max(scored[0].1 + picker.tie_tolerance_ms);
    let pool_end = scored
        .iter()
        .position(|(_, w)| *w > cutoff_wait)
        .unwrap_or(n);
    let pool = &scored[..pool_end];

    // In-flight tie-break: among workers in the pool (all within the
    // top-K-with-ties of predicted wait), prefer those with the lowest
    // current in-flight count. This breaks burst dogpile streaks
    // without ever changing the picker's predicted-wait preference —
    // a worker with better predicted_ms always beats one with worse
    // predicted_ms, regardless of inflight. When all `inflight` values
    // are zero (in-flight tracking disabled), this filter is a no-op
    // and behavior matches the legacy picker.
    let min_inflight_in_pool = pool.iter().map(|(i, _)| inflight[*i]).min().unwrap_or(0);
    let eligible: Vec<(usize, f32)> = pool
        .iter()
        .filter(|(i, _)| inflight[*i] == min_inflight_in_pool)
        .copied()
        .collect();

    // Uniform random pick within the eligible set. With one eligible
    // candidate it IS the pick (argmin after tie-break); with more,
    // it's a single modulo over a fresh 64-bit random.
    let chosen_idx = if eligible.len() == 1 {
        eligible[0].0
    } else {
        let mut buf = [0u8; 8];
        match getrandom::getrandom(&mut buf) {
            Ok(()) => {
                let r = u64::from_le_bytes(buf) as usize;
                eligible[r % eligible.len()].0
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "getrandom failed; picking first eligible candidate deterministically"
                );
                eligible[0].0
            }
        }
    };

    // Cache-replication override. If the picker just chose a
    // committed-warm worker but the fleet's warm count is below the
    // replication target, see whether there's a cold-idle alternative
    // that costs only a little more — and if so, pay that small cost
    // to seed cache on a second worker. See `cache_replication_override`
    // for the full rationale.
    let final_idx =
        cache_replication_override(scores, picker, kv_blocks_per_worker, chosen_idx, &scored);
    // `inflight_workload` and `now` are reserved for potential future
    // tie-break extensions inside the override; presently the helper
    // re-reads predicted_ms from the already-computed `scored` slice.
    let _ = (inflight_workload, now);

    Some(PickResult {
        worker: &scores[final_idx],
        replication_fired: final_idx != chosen_idx,
    })
}

/// Apply the cache-replication override. Given `chosen_idx` (the
/// picker's normal pick after top-K + inflight tie-break), decide
/// whether to swap it for a cold-idle worker so the fleet's prefix
/// cache spreads to a second worker. Returns the (possibly-replaced)
/// index into `scores`.
///
/// The override fires only when *all* of:
/// 1. `cfg.replication_threshold_pct > 0` and
///    `cfg.replication_warm_target > 1` (feature enabled).
/// 2. The picker's chosen worker is *committed-warm* — its raw
///    radix-tree match (`kv_blocks`) covers at least the fraction
///    of `total_blocks` configured by
///    `cfg.replication_warm_threshold_fraction`.
/// 3. Fewer than `cfg.replication_warm_target` workers are
///    committed-warm in the current snapshot.
/// 4. There's at least one candidate that's neither committed-warm
///    (raw `kv_blocks` below the threshold) nor *about to be warm*
///    (post-cache-credit `matched_blocks` below the threshold) —
///    using both signals avoids replicating to a worker that already
///    has an in-flight prefill of the same prefix.
/// 5. The cheapest such candidate's predicted total is within
///    `best_ms * (1 + cfg.replication_threshold_pct/100)` of the
///    originally-picked worker's predicted total — i.e. the
///    candidate is no more than X% slower than the best pick. The
///    percentage formulation auto-adapts to prompt size: a 10%
///    threshold accepts ~35ms hit on a 350ms full-cache-hit and a
///    ~300ms hit on a 3000ms cold prefill.
///
/// When any condition fails, returns `chosen_idx` unchanged. When
/// the swap fires, emits a structured INFO log so operators can
/// grep for replication redirects.
fn cache_replication_override(
    scores: &[WorkerScore],
    picker: &PickerConfig,
    kv_blocks_per_worker: &[u32],
    chosen_idx: usize,
    scored: &[(usize, f32)],
) -> usize {
    if picker.replication_threshold_pct <= 0.0 || picker.replication_warm_target <= 1 {
        return chosen_idx;
    }
    debug_assert_eq!(
        scores.len(),
        kv_blocks_per_worker.len(),
        "kv_blocks_per_worker must be parallel to scores"
    );
    let best = &scores[chosen_idx];
    let total = best.total_blocks as f32;
    if total <= 0.0 {
        return chosen_idx;
    }
    let fraction = picker.replication_warm_threshold_fraction.clamp(0.0, 1.0);
    let warm_blocks = (total * fraction).round() as u32;
    let raw_kv = |idx: usize| kv_blocks_per_worker[idx];
    if raw_kv(chosen_idx) < warm_blocks {
        // Best is a cold pick; nothing to replicate.
        return chosen_idx;
    }
    let warm_count = (0..scores.len())
        .filter(|i| raw_kv(*i) >= warm_blocks)
        .count();
    if warm_count >= picker.replication_warm_target as usize {
        // Already replicated enough.
        return chosen_idx;
    }
    // `scored` is parallel to `scores` by index but already has the
    // predicted_ms we computed up top; reuse it instead of re-running
    // the predictor. Build a quick index→predicted_ms lookup.
    let predicted_of = |idx: usize| -> f32 {
        scored
            .iter()
            .find(|(i, _)| *i == idx)
            .map(|(_, p)| *p)
            .unwrap_or(f32::INFINITY)
    };
    // Candidates: not committed-warm (raw radix-tree match below
    // threshold) AND not already-warming-up (post-credit
    // `matched_blocks` also below threshold). Using both signals
    // avoids replicating to a worker whose in-flight prefill will
    // soon commit the same prefix.
    let cheapest_candidate = scores
        .iter()
        .enumerate()
        .filter(|(i, w)| raw_kv(*i) < warm_blocks && w.matched_blocks < warm_blocks)
        .map(|(i, _)| (i, predicted_of(i)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let Some((cand_idx, cand_ms)) = cheapest_candidate else {
        return chosen_idx;
    };
    let best_ms = predicted_of(chosen_idx);
    let extra_cost = cand_ms - best_ms;
    // Threshold scales with the best-pick's cost so a single value
    // covers small and large prompts uniformly. `replication_threshold_pct`
    // is in percentage points (`10` → 10%), so divide by 100 here.
    // Guard against degenerate best_ms <= 0 (shouldn't happen, but
    // cheap defense).
    let max_extra = best_ms.max(0.0) * (picker.replication_threshold_pct / 100.0);
    if extra_cost > max_extra {
        return chosen_idx;
    }
    let cand = &scores[cand_idx];
    tracing::info!(
        from_endpoint = %best.endpoint,
        from_predicted_ms = best_ms,
        to_endpoint = %cand.endpoint,
        to_predicted_ms = cand_ms,
        extra_cost_ms = extra_cost,
        max_extra_ms = max_extra,
        replication_threshold_pct = picker.replication_threshold_pct,
        warm_count,
        target = picker.replication_warm_target,
        warm_blocks_threshold = warm_blocks,
        total_blocks = best.total_blocks,
        warm_threshold_fraction = fraction,
        "cache replication redirect"
    );
    cand_idx
}

/// Render a debug-level table of every scored worker with the
/// wait-time predictor's per-component breakdown. One row per worker,
/// sorted by `name` alphabetically so consecutive log lines line up
/// across requests (same worker in the same row position every time).
/// `chosen_endpoint` highlights which worker actually got picked with
/// an asterisk in the leading column — useful when top-K randomness
/// has spread the pick off the argmin.
///
/// Leading "weights:" line shows the configuration knobs that are
/// constant across all rows: `expected_overlap_alpha`,
/// `inflight_concurrent_capacity`, `forward_pass_ms` from
/// `[predictor]`, plus `inflight_tracking` and `tie_tolerance_ms`
/// from `[picker]`. That keeps the per-row columns purely data.
///
/// Column overview:
/// - `expect_ms` = base + q + prefill + cache (the predicted TTFT)
/// - `overlap/total` shows the layered expected-overlap value the
///   picker scored on; `kv_blocks` shows the raw radix-tree match for
///   comparison. At α=1 with no inflight overlap they're identical.
/// - `q_wait` is the worker's scraped `queue_waiting`; `q_inflight`
///   is our shadow count of picks not-yet-observed-as-TTFT;
///   `q_workload` is the sum of uncached blocks across our inflight
///   dispatches (the workload model's input).
///
/// In-flight is **not** in the predicted-wait calculation — it's
/// used only as a tie-breaker in `pick_endpoint`. The column is here
/// for transparency: when two rows tie on `expect_ms`, the operator
/// can see which one wins via lower `q_inflight`.
#[allow(clippy::too_many_arguments)]
pub fn format_score_table(
    scores: &[WorkerScore],
    predictor: &PredictorConfig,
    picker: &PickerConfig,
    expected_overlap_alpha: f32,
    chosen_endpoint: Option<&str>,
    inflight: &[u32],
    kv_blocks: &[u32],
    inflight_workload: &[Vec<InflightSummary>],
    now: Instant,
) -> String {
    if scores.is_empty() {
        return String::from("(no workers scored)");
    }
    debug_assert_eq!(
        scores.len(),
        inflight.len(),
        "inflight must be parallel to scores"
    );
    debug_assert_eq!(
        scores.len(),
        kv_blocks.len(),
        "kv_blocks must be parallel to scores"
    );
    debug_assert_eq!(
        scores.len(),
        inflight_workload.len(),
        "inflight_workload must be parallel to scores"
    );

    // Sort references by `name` alphabetically. Stable order across
    // requests keeps the table readable when comparing consecutive
    // picks. Carry the parallel inflight + kv_blocks + workload
    // values alongside.
    let mut sorted: Vec<(&WorkerScore, WaitPrediction, u32, u32, u32)> = scores
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let workload = &inflight_workload[i];
            let workload_blocks: u32 = workload
                .iter()
                .map(|s| s.uncached_at_dispatch as u64)
                .sum::<u64>()
                .min(u32::MAX as u64) as u32;
            (
                w,
                predicted_wait_ms(w, predictor, workload, now),
                inflight[i],
                kv_blocks[i],
                workload_blocks,
            )
        })
        .collect();
    sorted.sort_by(|a, b| a.0.name.cmp(&b.0.name));

    let mut out = String::with_capacity(256 + scores.len() * 200);
    // Configuration knobs that affect every row — kept on a dedicated
    // line above the table so the rows themselves stay purely data.
    let _ = writeln!(
        out,
        "weights: expected_overlap_alpha={}  inflight_concurrent_capacity={}  forward_pass_ms={}  inflight_tracking={}  tie_tolerance_ms={}",
        expected_overlap_alpha,
        predictor.inflight_concurrent_capacity,
        predictor.forward_pass_ms,
        picker.inflight_tracking,
        picker.tie_tolerance_ms,
    );
    let _ = writeln!(
        out,
        "{:<2} {:<20} {:<22} {:>10} {:>8} {:>8} {:>11} {:>9} {:>10} {:>10} {:>13} {:>10} {:>7} {:>11} {:>6} {:>11} {:>10} {:>8}",
        "",
        "name",
        "endpoint",
        "expect_ms",
        "base_ms",
        "q_ms",
        "prefill_ms",
        "cache_ms",
        "q_run_ms",
        "dec_cont",
        "overlap/total",
        "kv_blocks",
        "q_wait",
        "q_inflight",
        "q_run",
        "q_workload",
        "kv_cache%",
        "compl/s",
    );
    for (w, pred, q_inflight, kv, q_workload) in &sorted {
        let marker = if Some(w.endpoint.as_str()) == chosen_endpoint {
            "*"
        } else {
            " "
        };
        let _ = writeln!(
            out,
            "{:<2} {:<20} {:<22} {:>10.1} {:>8.1} {:>8.1} {:>11.1} {:>9.1} {:>10.1} {:>10.1} {:>13} {:>10} {:>7} {:>11} {:>6} {:>11} {:>9.1}% {:>8.2}",
            marker,
            truncate(&w.name, 20),
            truncate(&w.endpoint, 22),
            pred.total_ms(),
            pred.base_ms,
            pred.q_ms,
            pred.prefill_ms,
            pred.cache_ms,
            pred.q_running_ms,
            pred.decode_contention_ms,
            format!("{}/{}", w.matched_blocks, w.total_blocks),
            kv,
            w.queue_waiting,
            q_inflight,
            w.queue_running,
            q_workload,
            w.kv_cache_usage * 100.0,
            w.completion_rate_per_sec,
        );
    }
    // Trim trailing newline so tracing's own newline doesn't double up.
    if out.ends_with('\n') {
        out.pop();
    }
    out
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max { s } else { &s[..max] }
}

#[cfg(test)]
mod tests;
