//! Server-side stash of per-pick state needed at observation time.
//!
//! `Pick()` stashes a `PendingObservation` keyed by the server-minted
//! `request_id`. When the caller reports the observed TTFT via
//! `ReportFirstToken()`, `take(request_id)` returns it and
//! `finalize_observation` writes the CSV row and releases the inflight state.
//!
//! The sweep task in `server.rs` evicts entries older than `ttl`, bounding
//! leaks from callers that never report.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::monitor::Qos;
use crate::picker::WaitPrediction;
use crate::picker::WorkerScore;
use crate::picker::{PickerConfig, PredictorConfig};

/// The per-pool state a completed pick must release: decrement the endpoint's
/// shadow count, and forget its in-flight prefix.
///
/// Carried on the record rather than looked up by `PoolId` at release time, so
/// the release path needs no lookup and works whether or not the pool still
/// exists.
///
/// The two registries rather than an `Arc<Pool>`, so a pool torn down
/// mid-request (indexer thread, radix tree) is reclaimed promptly; only these
/// two small maps outlive it, for as long as an observation needs them.
#[derive(Clone)]
pub struct InflightHandles {
    pub inflight: std::sync::Arc<crate::inflight_registry::InflightRegistry>,
    pub prefixes: std::sync::Arc<crate::inflight_prefixes::InflightPrefixes>,
}

impl InflightHandles {
    pub fn of(pool: &crate::pool::Pool) -> Self {
        Self {
            inflight: pool.inflight.clone(),
            prefixes: pool.inflight_prefixes.clone(),
        }
    }
}

impl std::fmt::Debug for InflightHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The registries are interior-mutable maps; their contents are not
        // useful here and printing them would take locks on a Debug call.
        f.write_str("InflightHandles(..)")
    }
}

/// Everything needed to build a per-request observation row when the
/// corresponding `ReportFirstToken()` arrives.
///
/// Built by `KinferenceService::pick_and_record` at the end of the pick
/// critical section; consumed by `KinferenceService::finalize_observation`.
#[derive(Debug, Clone)]
pub struct PendingObservation {
    /// Server-minted identifier for this pick (the `response_id` on the
    /// wire), also the `InflightPrefixes` key.
    pub request_id: u64,
    /// The worker set this pick was routed to. A label for the CSV row and
    /// logs only; the release path uses `inflight`.
    pub workerset: String,
    /// The `[[profile]]` whose coefficients produced this row's prediction.
    pub profile: String,
    /// The two things the release path actually touches, carried directly.
    pub inflight: InflightHandles,
    /// When the pick was made. Drives TTL eviction in `take_expired`.
    pub picked_at: Instant,
    /// The worker selected by the picker. Carries the layered
    /// expected-overlap value (`matched_blocks` after layering), block
    /// size, and live load metrics that landed on the CSV row.
    pub chosen_worker: WorkerScore,
    /// Per-term breakdown of the predicted wait at pick time. Pinned
    /// here so the CSV column contract stays stable even if
    /// `predicted_wait_ms` changes later.
    pub prediction: WaitPrediction,
    /// Shadow `inflight_picks` count for the chosen worker at pick
    /// time, BEFORE the post-pick `inc()`. Zero when inflight
    /// tracking is disabled.
    pub q_inflight: u32,
    /// Raw radix-tree match for the chosen worker at pick time.
    /// `chosen_worker.matched_blocks` carries the layered value the
    /// picker scored on; this is the underlying snapshot.
    pub kv_blocks: u32,
    /// Sum of `uncached_at_dispatch` across inflight dispatches on
    /// the chosen worker at pick time.
    pub inflight_workload_blocks: u32,
    /// Constant-model queue cost (`q_rate × q_waiting`).
    pub q_ms_constant: f32,
    /// Workload-model queue cost (`Σ uncached × prefill_rate +
    /// unknown × q_rate`).
    pub q_ms_workload: f32,
    /// Per-dispatch overlap counts (`|P_new ∩ Pᵢ|`) against the
    /// chosen worker's inflight dispatches, oldest first.
    pub inflight_overlap_blocks: Vec<u32>,
    /// Snapshot of `expected_overlap_alpha` at pick time.
    pub expected_overlap_alpha: f32,
    /// Snapshot of the latency-model coefficients at pick time, so each CSV
    /// row carries the predictor state that produced it.
    pub predictor_cfg: PredictorConfig,
    /// Snapshot of the picker knobs at pick time, likewise.
    pub picker_cfg: PickerConfig,
    /// The caller's `client_request_id`, for log correlation.
    pub external_request_id: Option<String>,
    /// True when the cache-replication override swapped the picker's
    /// normal pick for a cold-idle alternative (see
    /// `picker::cache_replication_override`), so offline analysis can split
    /// residuals by it.
    pub replication_fired: bool,
    /// The model this request resolved to (post-`auto`). The label for
    /// per-model observation metrics (e.g. the TTFT histogram); may differ
    /// from the client-supplied model string when `auto` was resolved.
    pub model: String,
    /// Whether the request asked for a streaming response (`stream: true`).
    /// For non-streaming requests the reported "TTFT" is total latency, so
    /// `finalize_observation` skips the CSV row (but still releases state).
    pub streaming: bool,
    /// QoS class from the Pick request. The `qos` label on the TTFT
    /// histogram, recorded from this record at `ReportFirstToken`.
    pub qos: Qos,
    /// Tenant from the Pick request, raw: the `tenant` label on the TTFT
    /// histogram. Carried here rather than read from `RequestLabels`, whose
    /// entry an abort or TTL sweep may already have consumed.
    pub tenant: String,
}

/// In-memory stash of pending observations. Inserted by `pick_and_record`,
/// consumed by `finalize_observation` or `abort_observation`, and TTL-swept
/// as a backstop.
pub struct PendingObservations {
    by_id: DashMap<u64, PendingObservation>,
    ttl: Duration,
}

impl PendingObservations {
    /// Construct with the given TTL, which must exceed the longest response
    /// time a caller may take to report (see `observation_ttl_secs`).
    pub fn new(ttl: Duration) -> Self {
        Self {
            by_id: DashMap::new(),
            ttl,
        }
    }

    /// Stash a pending observation keyed by `request_id`, overwriting any
    /// existing entry (ids are random 64-bit, so collisions don't happen).
    pub fn insert(&self, obs: PendingObservation) {
        self.by_id.insert(obs.request_id, obs);
    }

    /// Remove and return the entry for `request_id`. `None` means it was
    /// unknown (swept, never inserted, or already consumed).
    pub fn take(&self, request_id: u64) -> Option<PendingObservation> {
        self.by_id.remove(&request_id).map(|(_, v)| v)
    }

    /// Remove and return every entry older than `ttl`, so the caller can
    /// release each one's inflight state (see
    /// `KinferenceService::sweep_expired_observations`).
    pub fn take_expired(&self) -> Vec<PendingObservation> {
        let cutoff = Instant::now() - self.ttl;
        let expired: Vec<u64> = self
            .by_id
            .iter()
            .filter(|e| e.value().picked_at < cutoff)
            .map(|e| *e.key())
            .collect();
        expired
            .into_iter()
            .filter_map(|id| self.by_id.remove(&id).map(|(_, v)| v))
            .collect()
    }

    /// Number of currently-pending observations. Test-only diagnostic.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }
}

/// Arc constructor, used by `server.rs`.
pub fn shared(ttl: Duration) -> Arc<PendingObservations> {
    Arc::new(PendingObservations::new(ttl))
}

/// Per-request metric labels needed at `ReportFinalToken`. The
/// `PendingObservation` is consumed earlier, at `ReportFirstToken`, so this
/// side map survives until `ReportFinalToken` or the TTL sweep. Nothing
/// inflight is tied to it, so eviction is a plain drop.
#[derive(Debug, Clone)]
pub struct RequestMeta {
    pub model: String,
    pub qos: Qos,
    /// Tenant from the `Pick` request, raw; `crate::metrics` applies the
    /// allowlist. Empty buckets under the default label.
    pub tenant: String,
    /// Whether the request asked for a streaming response.
    ///
    /// Needed by the provider path, which has no `PendingObservation`, to tell
    /// a real TTFT from a non-streaming response's total latency.
    pub streaming: bool,
    pub picked_at: Instant,
    /// The `InferenceProvider` this pick went to, when it went off-cluster.
    ///
    /// Here rather than in `PendingObservation`, which is built from
    /// per-endpoint scoring state a provider doesn't have, and because this
    /// map lives until `ReportFinalToken`.
    pub provider: Option<kinference_policy::engine::ObjectKey>,
}

/// What `RequestLabels` knows about a pick that went off-cluster.
pub struct ProviderPickLabels {
    pub provider: kinference_policy::engine::ObjectKey,
    pub model: String,
    /// Whether the caller asked for streaming. Decides whether its reported
    /// first-token time is a real TTFT or the whole response.
    pub streaming: bool,
}

pub struct RequestLabels {
    by_id: DashMap<u64, RequestMeta>,
    ttl: Duration,
}

impl RequestLabels {
    pub fn new(ttl: Duration) -> Self {
        Self {
            by_id: DashMap::new(),
            ttl,
        }
    }

    /// Stash labels for a pick, keyed by the server-minted response id.
    pub fn insert(&self, response_id: u64, meta: RequestMeta) {
        self.by_id.insert(response_id, meta);
    }

    /// Remove and return the labels for `response_id`, if present. `None`
    /// means unknown (already taken, TTL-swept, or never picked).
    pub fn take(&self, response_id: u64) -> Option<RequestMeta> {
        self.by_id.remove(&response_id).map(|(_, v)| v)
    }

    /// What a provider pick's feedback needs, without consuming the entry.
    ///
    /// A peek rather than a take, since provider feedback (`ReportFirstToken`,
    /// `Abort`) may arrive before the terminal report that clears this. One
    /// lookup, so all fields come from the same map state.
    pub fn provider_pick_for(&self, response_id: u64) -> Option<ProviderPickLabels> {
        self.by_id.get(&response_id).and_then(|m| {
            m.provider.clone().map(|provider| ProviderPickLabels {
                provider,
                model: m.model.clone(),
                streaming: m.streaming,
            })
        })
    }

    /// Drop every entry older than `ttl`. Backstop for requests that never
    /// reach `ReportFinalToken`; nothing is tied to these so they're dropped
    /// silently. Returns the number dropped.
    pub fn take_expired(&self) -> usize {
        let cutoff = Instant::now() - self.ttl;
        let before = self.by_id.len();
        self.by_id.retain(|_, v| v.picked_at >= cutoff);
        before - self.by_id.len()
    }
}

/// Arc ctor mirroring [`shared`], for the `RequestLabels` side map.
pub fn shared_request_labels(ttl: Duration) -> Arc<RequestLabels> {
    Arc::new(RequestLabels::new(ttl))
}

/// Header row for `format_csv_row`'s columns, with no trailing newline (the
/// sink appends it). `ObservationSink` writes it once, to a new or empty file.
///
/// Must stay in lockstep with `format_csv_row`; the
/// `csv_header_matches_format_csv_row_arity` test pins that.
pub const CSV_HEADER: &str = "unix_ms,name,endpoint,status,upstream_error,\
predicted_ms,base_ms,q_ms,q_rate_ms_per_entry,prefill_ms,cache_ms,\
observed_ms,residual_ms,\
overlap_blocks,total_blocks,\
q_waiting,q_inflight,q_running,q_swapped,\
kv_cache_usage,compl_per_s,\
kv_blocks,q_workload,request_id,\
q_ms_constant,q_ms_workload,inflight_overlap_blocks,expected_overlap_alpha,\
queue_ms_per_entry,cache_pressure_penalty_ms,saturated_cold_ms_per_entry,cache_pressure_saturated_threshold,\
attention_uncached_block_ms,attention_uncached_per_every_block_ms,\
top_k_min,top_k_fraction,inflight_tracking,tie_tolerance_ms,\
x_request_id,q_running_ms,decode_contention_ms,\
q_running_penalty_peak_ms,q_running_penalty_peak_qr,q_running_penalty_falloff_qr,\
inflight_concurrent_capacity,forward_pass_ms,\
replication_threshold_pct,replication_warm_target,replication_warm_threshold_fraction,\
replication_fired,\
workerset,profile";

/// Format one CSV row, 52 columns, without the header (see [`CSV_HEADER`]).
///
/// The format is append-only: every column keeps its position, and new ones
/// go on the end, so a CSV accumulated across kinference versions stays
/// parseable by column index.
///
/// Column order:
///   0:  unix_ms
///   1:  name
///   2:  endpoint
///   3:  status
///   4:  upstream_error              always empty
///   5:  predicted_ms
///   6:  base_ms
///   7:  q_ms                        queue cost
///   8:  q_rate_ms_per_entry         effective rate after cold-saturation check
///   9:  prefill_ms
///   10: cache_ms
///   11: observed_ms
///   12: residual_ms
///   13: overlap_blocks              layered expected-overlap (picker scored on)
///   14: total_blocks
///   15: q_waiting                   engine's scraped queue depth
///   16: q_inflight                  shadow count at pick time
///   17: q_running                   engine's scraped running batch size
///   18: q_swapped                   engine's scraped swapped-out count
///   19: kv_cache_usage
///   20: compl_per_s                 completion_rate_per_sec
///   21: kv_blocks                   raw radix-tree match (diagnostic)
///   22: q_workload                  sum of uncached blocks across inflight dispatches
///   23: request_id                  server-minted id; joins rows to log lines
///   24: q_ms_constant
///   25: q_ms_workload
///   26: inflight_overlap_blocks     semicolon-delimited list, oldest first
///   27: expected_overlap_alpha
/// ── PredictorConfig snapshot (cols 28-33) ──
///   28: queue_ms_per_entry
///   29: cache_pressure_penalty_ms
///   30: saturated_cold_ms_per_entry
///   31: cache_pressure_saturated_threshold
///   32: attention_uncached_block_ms
///   33: attention_uncached_per_every_block_ms
/// ── PickerConfig snapshot (cols 34-37) ──
///   34: top_k_min
///   35: top_k_fraction
///   36: inflight_tracking           (true/false)
///   37: tie_tolerance_ms
/// ── Per-request annotations (cols 38-40) ──
///   38: x_request_id                caller-supplied; empty when unset
///   39: q_running_ms                U-shape penalty applied at this pick
///   40: decode_contention_ms        flat per-active-decode cost applied
///                                   at this pick
/// ── PredictorConfig snapshot, q_running U-shape + capacity + decode (cols 41-45) ──
///   41: q_running_penalty_peak_ms
///   42: q_running_penalty_peak_qr
///   43: q_running_penalty_falloff_qr
///   44: inflight_concurrent_capacity
///   45: forward_pass_ms             coefficient for decode_contention_ms
/// ── PickerConfig snapshot, replication (cols 46-48) ──
///   46: replication_threshold_pct
///   47: replication_warm_target
///   48: replication_warm_threshold_fraction
/// ── Per-request annotation (col 49) ──
///   49: replication_fired           (true/false) whether the picker
///                                   swapped to a cold-idle worker
///                                   for cache replication
/// ── Provenance (cols 50-51) ──
///   50: workerset                   which worker set served this pick
///   51: profile                     which `[[profile]]`'s coefficients
///                                   produced the prediction in cols 5-12
pub fn format_csv_row(
    record: &PendingObservation,
    observed_ms: u32,
    residual_ms: f32,
    status: i32,
) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let w = &record.chosen_worker;
    let p = &record.prediction;
    let name = w.name.replace(',', ";");
    let endpoint = w.endpoint.replace(',', ";");
    let upstream_error = "";
    let inflight_overlap_str = record
        .inflight_overlap_blocks
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(";");

    let pred = &record.predictor_cfg;
    let pkr = &record.picker_cfg;
    let x_request_id = record
        .external_request_id
        .as_deref()
        .map(|s| s.replace(',', ";"))
        .unwrap_or_default();
    format!(
        "{},{},{},{},{},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{},{:.1},{},{},{},{},{},{},{:.3},{:.3},{},{},{},\
         {:.1},{:.1},{},{:.3},\
         {:.1},{:.1},{:.1},{:.3},{:.3},{:.6},\
         {},{:.3},{},{:.1},\
         {},{:.1},{:.1},\
         {:.1},{},{},{},{:.3},\
         {:.3},{},{:.3},\
         {},\
         {},{}",
        unix_ms,
        name,
        endpoint,
        status,
        upstream_error,
        p.total_ms(),
        p.base_ms,
        p.q_ms,
        p.q_rate_ms_per_entry,
        p.prefill_ms,
        p.cache_ms,
        observed_ms,
        residual_ms,
        w.matched_blocks,
        w.total_blocks,
        w.queue_waiting,
        record.q_inflight,
        w.queue_running,
        w.queue_swapped,
        w.kv_cache_usage,
        w.completion_rate_per_sec,
        record.kv_blocks,
        record.inflight_workload_blocks,
        record.request_id,
        record.q_ms_constant,
        record.q_ms_workload,
        inflight_overlap_str,
        record.expected_overlap_alpha,
        // PredictorConfig snapshot (cols 28-33).
        pred.queue_ms_per_entry,
        pred.cache_pressure_penalty_ms,
        pred.saturated_cold_ms_per_entry,
        pred.cache_pressure_saturated_threshold,
        pred.attention_uncached_block_ms,
        pred.attention_uncached_per_every_block_ms,
        // PickerConfig snapshot (cols 34-37).
        pkr.top_k_min,
        pkr.top_k_fraction,
        pkr.inflight_tracking,
        pkr.tie_tolerance_ms,
        // Per-request annotations (cols 38-40).
        x_request_id,
        p.q_running_ms,
        p.decode_contention_ms,
        // PredictorConfig: q_running U-shape + capacity + decode-contention (cols 41-45).
        pred.q_running_penalty_peak_ms,
        pred.q_running_penalty_peak_qr,
        pred.q_running_penalty_falloff_qr,
        pred.inflight_concurrent_capacity,
        pred.forward_pass_ms,
        // PickerConfig: replication (cols 46-48).
        pkr.replication_threshold_pct,
        pkr.replication_warm_target,
        pkr.replication_warm_threshold_fraction,
        // Per-request annotation (col 49).
        record.replication_fired,
        // Provenance (cols 50-51).
        record.workerset.replace(',', ";"),
        record.profile.replace(',', ";"),
    )
}

#[cfg(test)]
mod tests;
