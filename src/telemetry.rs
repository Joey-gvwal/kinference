//! In-memory telemetry for the live dashboard.
//!
//! Two layers:
//! - **Live counters** (atomics + a small mutex), written by the gRPC `score()`
//!   handler on the hot path. Cheap: atomic bumps + a `try_lock` push.
//! - **Ring buffers** (1 Hz, 1-hour deep), written only by the rollup task,
//!   read only by `snapshot_json`. The rollup folds the live counters and the
//!   latest per-worker metrics sample into one sample per second.
//!
//! `snapshot_json` emits exactly the shape the dashboard prototype's
//! `simulation.js::snapshot()` produced, so the ported React panels consume it
//! unchanged.
//!
//! ## Slot pool
//!
//! Per-worker state lives in fixed-capacity, slot-indexed arrays. A worker
//! gets a slot index when `register_worker` is called; the slot is stable
//! for that worker's lifetime and recycled on `unregister_worker`. The
//! capacity is set at `Telemetry::new(dashboard_max_workers, ...)` time and never
//! grows. Allocations stay bounded; `topWorkerIdx` references in recent
//! calls remain valid until that worker is removed.
//!
//! The slot index is purely internal — `snapshot_json` translates it into
//! a stable index in the emitted `workers` JSON array (which lists only
//! active slots in slot-order).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use dynamo_kv_router::indexer::KvIndexerInterface;
use dynamo_kv_router::protocols::KvCacheEventData;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::config::WorkerConfig;
use crate::metrics::MetricsRegistry;
use crate::version::VERSION;

/// Bounded FIFO ring: `push` evicts the oldest element once `cap` is
/// reached. Used for the 1-Hz time-series buffers that back the
/// dashboard's charts and the recent-calls tape. One shared type
/// avoids re-implementing "push + evict" for every metric.
struct Ring<T> {
    buf: VecDeque<T>,
    cap: usize,
}

impl<T> Ring<T> {
    fn new(cap: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(cap),
            cap,
        }
    }

    fn push(&mut self, v: T) {
        if self.buf.len() == self.cap {
            self.buf.pop_front();
        }
        self.buf.push_back(v);
    }

    fn iter(&self) -> std::collections::vec_deque::Iter<'_, T> {
        self.buf.iter()
    }
}

impl<T: Copy + Default> Ring<T> {
    /// Latest sample or `T::default()` if the ring is still empty.
    /// Callers use this for the "current value" fields of the snapshot
    /// (as opposed to the full history).
    fn last_or_default(&self) -> T {
        self.buf.back().copied().unwrap_or_default()
    }
}

impl<T: Copy> Ring<T> {
    /// Copy the full ring into a `Vec` for JSON serialization.
    fn snapshot(&self) -> Vec<T> {
        self.buf.iter().copied().collect()
    }
}

/// Kind of KV-cache event ingested from a worker, for telemetry counting.
#[derive(Clone, Copy)]
/// What one rollup tick observed, handed back for Prometheus.
///
/// The ingest atomics are `swap(0)`-ed by `rollup_once`, so those deltas cannot
/// be read again afterwards — returning them is the only way out. Prometheus
/// accumulates them into monotonic counters; the dashboard turns the same
/// numbers into per-second gauges.
///
/// The worker counts ride along because the tick already walks every active
/// worker and computes its scrape age, so counting them costs nothing extra.
pub struct RollupDeltas {
    pub events_stored: u64,
    pub events_removed: u64,
    pub events_cleared: u64,
    pub blocks_stored: u64,
    pub blocks_removed: u64,
    /// Telemetry slots occupied, and the fixed pool size. `dashboard_max_workers` binds
    /// when these meet.
    pub slots_used: u64,
    pub slots_capacity: u64,
    /// Registrations refused for want of a slot, this tick.
    pub slots_denied: u64,
}

pub enum IngestEvent {
    Stored,
    Removed,
    Cleared,
}

/// Outcome of one pick, for telemetry counting.
///
/// Distinct from the WIRE outcome (`pick_response::Outcome` in the proto,
/// imported as `WireOutcome` in `grpc.rs`): that one is what the caller is
/// told, this one is which counter bucket the pick lands in. They agree today,
/// and keeping them separate means a change to reporting cannot quietly become
/// a change to the protocol.
#[derive(Clone, Copy)]
pub enum PickOutcome {
    /// Picker returned a worker. `outcome=OK` or `DEGRADED` in the response.
    Success,
    /// The body did not parse, or carried nothing to route on.
    /// `outcome=BAD_REQUEST`.
    NoOpinion,
    /// No workers serve the requested model. `outcome=NO_WORKERS`.
    NotFound,
}

impl PickOutcome {
    /// The `outcome` label on `kinference_picks_total` / `kinference_pick_duration_ms`.
    ///
    /// Named for the WIRE outcome rather than the variant, since that is what an
    /// operator sees in a response and in the proto.
    pub fn as_label(self) -> &'static str {
        match self {
            PickOutcome::Success => "ok",
            PickOutcome::NoOpinion => "bad_request",
            PickOutcome::NotFound => "no_workers",
        }
    }
}

const NUM_BUCKETS: usize = 10;
const RING_CAP: usize = 3600; // 1 hour at 1 Hz
const RECENT_CAP: usize = 24;
const LATENCY_CAP: usize = 4096; // per-second latency reservoir cap
/// How often to call `KvIndexer::dump_events()` to recompute the indexed-block
/// and worker counts. `dump_events` clones the whole tree (O(tree)), so it's
/// throttled — the gauge lags by at most this many seconds. Rates stay 1 Hz.
const DUMP_EVERY_TICKS: u64 = 10;

/// Why `register_worker` refused.
#[derive(Debug, Clone)]
pub enum RegisterError {
    /// The slot pool has no free slots left.
    SlotExhausted { capacity: usize },
    /// This `worker_id_num` is already registered — to the named worker set.
    ///
    /// `worker_id_num` is a FLEET-WIDE key (telemetry slots, `MetricsRegistry`,
    /// the eviction channel), and in Kubernetes mode it is derived from the pod
    /// UID. So two worker sets whose Services select overlapping pods produce
    /// the same id twice. Registering it twice used to pop a second slot and
    /// overwrite `idx_of`, stranding the first slot permanently — the fleet
    /// then stopped growing at `dashboard_max_workers` with no explanation. Refusing is
    /// the honest answer while the fleet-wide keyspaces are keyed by id alone.
    AlreadyRegistered { worker_set: String },
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SlotExhausted { capacity } => write!(
                f,
                "telemetry slot pool exhausted (capacity = {capacity}); \
                 raise `dashboard_max_workers` to display more workers"
            ),
            Self::AlreadyRegistered { worker_set } => write!(
                f,
                "worker_id_num is already registered to worker set `{worker_set}`; \
                 two worker sets cannot select the same pod"
            ),
        }
    }
}

impl std::error::Error for RegisterError {}

/// Static per-worker fields surfaced in the snapshot (mutable only via
/// register/unregister — within a slot's lifetime they don't change).
#[derive(Clone)]
pub struct WorkerMeta {
    pub worker_id_num: u64,
    pub id: String,        // worker_id_str
    pub pool: String,      // owning worker-set name (from [[workerset]].name)
    pub namespace: String, // "" if unset
    pub ip: String,        // host part of http_url (or worker_id_str fallback)
    pub port: u32,         // port part of http_url (0 if unknown)
    pub block_size: u32,
}

impl WorkerMeta {
    pub fn from_config(w: &WorkerConfig, block_size: u32, pool: &str) -> Self {
        // Derive ip:port from http_url when present. http_url looks like
        // "http://10.0.0.1:8000/"; split scheme then authority then host/port.
        let (ip, port) = w
            .http_url
            .as_deref()
            .and_then(|url| {
                let after = url.split("://").nth(1).unwrap_or(url);
                let authority = after.split('/').next().unwrap_or("");
                let (h, p) = authority.rsplit_once(':')?;
                Some((h.to_string(), p.parse::<u32>().unwrap_or(0)))
            })
            .unwrap_or_else(|| (w.worker_id_str.clone(), 0));
        Self {
            worker_id_num: w.worker_id_num,
            id: w.worker_id_str.clone(),
            pool: pool.to_string(),
            namespace: w.namespace.clone().unwrap_or_default(),
            ip,
            port,
            block_size,
        }
    }
}

/// One sampled score() call, surfaced in the "recent calls" tape.
#[derive(Clone)]
pub struct RecentCall {
    pub ts: u64,
    pub model: String,
    pub tokens: u32,
    pub top_worker: String,
    pub top_overlap: f64,
    pub total_blocks: u32,
    pub matched_blocks: u32,
    pub latency_ms: f64,
    /// Stable worker identity. `snapshot_json` resolves this to the
    /// worker's position in the emitted JSON array on each request, so
    /// the field is always live (or `null` if the worker has been
    /// removed since the call was recorded).
    pub top_worker_id_num: u64,
}

/// Everything the handler needs to report about one score() call.
pub struct CallSample<'a> {
    pub model: &'a str,
    pub tokens: u32,
    pub latency_ms: f64,
    /// (worker_id_num, overlap fraction 0..1) for every scored worker.
    pub overlaps: &'a [(u64, f64)],
    /// The worker with the highest overlap in this response, if any.
    pub top: Option<TopWorker>,
}

pub struct TopWorker {
    pub worker_id_num: u64,
    pub addr: String,
    pub overlap: f64,
    pub matched: u32,
    pub total: u32,
}

/// LiveCounters' per-worker arrays are sized at the slot capacity (NOT the
/// active worker count), so slot indices are valid for the entire process
/// lifetime. Reads/writes are atomic, no rings lock held.
struct LiveCounters {
    calls: AtomicU64,
    inflight: AtomicI64,
    per_worker_buckets: Vec<[AtomicU64; NUM_BUCKETS]>,
    per_worker_wins: Vec<AtomicU64>,
    latencies: Mutex<Vec<f32>>,
    /// Worker registrations refused for want of a telemetry slot, since the
    /// last tick. The worker still routes — only its observability is lost —
    /// so this counter is the only signal that `dashboard_max_workers` is
    /// binding. Expected to be non-zero on any fleet larger than the dashboard.
    slots_denied: AtomicU64,
    // Radix-tree ingest counters (since last tick), fed by ingest.rs.
    events_stored: AtomicU64,
    events_removed: AtomicU64,
    events_cleared: AtomicU64,
    blocks_stored: AtomicU64,
    blocks_removed: AtomicU64,
    // Tokenize activity (outbound /tokenize fanout). Mirrors the score
    // activity counters above one-for-one; same Relaxed-atomic / try_lock
    // pattern. Fed by tokenize_with_fallback in src/grpc.rs.
    tk_calls: AtomicU64,
    tk_inflight: AtomicI64,
    tk_attempts: AtomicU64,
    tk_attempt_errors: AtomicU64,
    tk_unavailable: AtomicU64,
    tk_latencies: Mutex<Vec<f32>>,
    // Scout-mode local-tokenize comparison counters. Bumped once per
    // score request when `tokenize.local.compare` is on. Match = local
    // produced bit-identical tokens to HTTP; divergence = different
    // tokens (the signal we're watching); error = local tokenize
    // failed (template missing, model unknown, etc.) — orthogonal to
    // the divergence count.
    tk_local_matches: AtomicU64,
    tk_local_divergences: AtomicU64,
    tk_local_errors: AtomicU64,
    // Production-mode breakdown of which path served each tokenize
    // request. `tk_local_served + tk_http_fallback + tk_unavailable`
    // equals `tk_total_calls`. Surfaced in the Tokenize panel header
    // so the operator can see at-a-glance whether local tokenize is
    // actually carrying production traffic.
    tk_local_served: AtomicU64,
    tk_http_fallback: AtomicU64,
    // Pick counters, bumped by the gRPC `Pick()` handler. Sums:
    //   pick_total = pick_no_opinion + pick_not_found + (successful picks)
    // The "successful picks" count is `pick_total - no_opinion - 404`
    // and isn't stored separately. `pick_latencies` is the wall-clock
    // per-pick reservoir (same try_lock-cap-at-LATENCY_CAP pattern as
    // tokenize latencies); drained per-tick into p50/p95/p99.
    pick_total: AtomicU64,
    pick_no_opinion: AtomicU64,
    pick_not_found: AtomicU64,
    pick_latencies: Mutex<Vec<f32>>,
    // Block-hash divergence counter. Bumped at TTFT when the engine
    // emitted zero `BlockStored` events whose `tokens_hash` matched
    // any of our predicted hashes for a substantial-uncached
    // dispatch. Both sides hash with the same XXH3 function, so
    // divergence here points at a token-sequence mismatch between
    // our query side (/render or local minijinja) and the
    // `token_ids` payload vLLM emits in its events. See
    // `InflightPrefixes::forget` + the warn-log gate in service.rs.
    tokenize_divergence_total: AtomicU64,
}

struct WorkerRings {
    kv: Ring<f64>,
    qw: Ring<f64>,
    qr: Ring<f64>,
    qs: Ring<f64>,
    age: Ring<f64>,
    wins: Ring<f64>,
    buckets: Ring<[u64; NUM_BUCKETS]>,
    /// Engine-reported prefix-cache hit rate (0..1), one sample per
    /// rollup tick. Sampled from `MetricsSample.prefix_cache_hit_rate`
    /// which is itself the 30s windowed slope-of-counter rate (vLLM)
    /// or the SGLang gauge passthrough. The dashboard renders this
    /// as a time series in the worker drawer.
    phr: Ring<f64>,
}

impl WorkerRings {
    fn new() -> Self {
        Self {
            kv: Ring::new(RING_CAP),
            qw: Ring::new(RING_CAP),
            qr: Ring::new(RING_CAP),
            qs: Ring::new(RING_CAP),
            age: Ring::new(RING_CAP),
            wins: Ring::new(RING_CAP),
            buckets: Ring::new(RING_CAP),
            phr: Ring::new(RING_CAP),
        }
    }
}

/// Instantaneous (latest-tick) per-worker values for the snapshot's top-level
/// worker fields. Updated each rollup tick.
#[derive(Clone, Copy)]
struct WorkerLast {
    kv: f32,
    qw: u32,
    qr: u32,
    qs: u32,
    age: u64,
    num_gpu_blocks: u32,
    wins: u64,
    score_calls: u64,
    buckets: [u64; NUM_BUCKETS],
    health: &'static str,
}

impl Default for WorkerLast {
    fn default() -> Self {
        Self {
            kv: 0.0,
            qw: 0,
            qr: 0,
            qs: 0,
            age: 0,
            num_gpu_blocks: 0,
            wins: 0,
            score_calls: 0,
            buckets: [0; NUM_BUCKETS],
            health: "ok",
        }
    }
}

/// Latest radix-tree index values for the snapshot's `index` object.
/// `blocks`/`workers` come from a throttled `dump_events()` (every
/// `DUMP_EVERY_TICKS`); the per-sec rates are 1 Hz from ingest counters.
#[derive(Clone, Copy, Default)]
struct IndexLast {
    blocks: u64,
    workers: u64,
    events_per_sec: f64,
    stored_per_sec: f64,
    removed_per_sec: f64,
    cleared_per_sec: f64,
    blocks_stored_per_sec: f64,
    blocks_removed_per_sec: f64,
}

/// All state guarded by the `rings` Mutex. Slot pool lives here so
/// register/unregister coordinate with the rollup + snapshot codepaths
/// under a single lock.
struct Rings {
    ts: u64,
    total_calls: u64,
    rps: Ring<f64>,
    inflight: Ring<f64>,
    p50: Ring<f64>,
    p95: Ring<f64>,
    p99: Ring<f64>,
    ticks: Ring<u64>,
    /// `None` for unused / freed slots. Positional — index = slot.
    meta: Vec<Option<WorkerMeta>>,
    /// Empty rings for unused slots. Reset to `WorkerRings::new()` on
    /// `register_worker` so a recycled slot starts clean.
    workers: Vec<WorkerRings>,
    /// Default-valued for unused slots; reset on register.
    last: Vec<WorkerLast>,
    /// Free slot indices in LIFO order. Push on unregister, pop on register.
    free_slots: Vec<usize>,
    global_overlap: Ring<[f64; NUM_BUCKETS]>,
    // Radix-tree index series + latest scalars.
    idx_blocks: Ring<f64>,
    idx_events: Ring<f64>,
    index_last: IndexLast,
    // Tokenize activity time-series, parallel to rps/inflight/p* above.
    // Each ring tracks 1 sample/sec, capped at RING_CAP (1h).
    tk_rps: Ring<f64>,
    tk_inflight: Ring<f64>,
    tk_p50: Ring<f64>,
    tk_p95: Ring<f64>,
    tk_p99: Ring<f64>,
    /// Per-tick fraction of /tokenize attempts that failed in that
    /// tick's 1-second window. 0..1; defaults to 0 when no attempts
    /// happened (NaN-free).
    tk_error_rate: Ring<f64>,
    /// Running totals mirror `total_calls` above — read directly into
    /// snapshot_json's `tokenize.total` / `tokenize.unavailableTotal`.
    tk_total_calls: u64,
    tk_unavailable_total: u64,
    /// Per-pick latency percentiles, drained from `pick_latencies`
    /// per tick. Same shape as the tokenize p50/p95/p99 rings.
    /// `pick_total_calls` mirrors `tk_total_calls` for the dashboard
    /// header — cumulative counter of picks served.
    pick_p50: Ring<f64>,
    pick_p95: Ring<f64>,
    pick_p99: Ring<f64>,
    pick_total_calls: u64,
}

pub struct Telemetry {
    live: LiveCounters,
    rings: Mutex<Rings>,
    recent: Mutex<Ring<RecentCall>>,
    /// `worker_id_num -> slot_idx`. Read lock-free by `record_call` on the
    /// hot path; written under the `rings` lock to keep positional state
    /// consistent.
    idx_of: DashMap<u64, usize>,
    metrics: Arc<MetricsRegistry>,
    /// Slot capacity, fixed at construction. Used for SlotExhausted error
    /// messages and bounds-checking.
    capacity: usize,
    /// Mirror of `rings.ts`, read lock-free by the handler to stamp recent calls.
    ts: AtomicU64,
}

impl Telemetry {
    /// Build an empty Telemetry with a slot pool of size `dashboard_max_workers`.
    /// All slots start free; the coordinator calls `register_worker` per
    /// discovered worker.
    pub fn new(dashboard_max_workers: usize, metrics: Arc<MetricsRegistry>) -> Arc<Self> {
        let capacity = dashboard_max_workers.max(1);
        let live = LiveCounters {
            calls: AtomicU64::new(0),
            inflight: AtomicI64::new(0),
            per_worker_buckets: (0..capacity)
                .map(|_| std::array::from_fn(|_| AtomicU64::new(0)))
                .collect(),
            per_worker_wins: (0..capacity).map(|_| AtomicU64::new(0)).collect(),
            latencies: Mutex::new(Vec::new()),
            slots_denied: AtomicU64::new(0),
            events_stored: AtomicU64::new(0),
            events_removed: AtomicU64::new(0),
            events_cleared: AtomicU64::new(0),
            blocks_stored: AtomicU64::new(0),
            blocks_removed: AtomicU64::new(0),
            tk_calls: AtomicU64::new(0),
            tk_inflight: AtomicI64::new(0),
            tk_attempts: AtomicU64::new(0),
            tk_attempt_errors: AtomicU64::new(0),
            tk_unavailable: AtomicU64::new(0),
            tk_latencies: Mutex::new(Vec::new()),
            tk_local_matches: AtomicU64::new(0),
            tk_local_divergences: AtomicU64::new(0),
            tk_local_errors: AtomicU64::new(0),
            tk_local_served: AtomicU64::new(0),
            tk_http_fallback: AtomicU64::new(0),
            pick_total: AtomicU64::new(0),
            pick_no_opinion: AtomicU64::new(0),
            pick_not_found: AtomicU64::new(0),
            pick_latencies: Mutex::new(Vec::new()),
            tokenize_divergence_total: AtomicU64::new(0),
        };

        // Free slots in LIFO order — `pop()` yields the smallest index
        // first so early registrations land in slots 0, 1, 2, ... matching
        // dashboard expectations.
        let free_slots: Vec<usize> = (0..capacity).rev().collect();

        let rings = Rings {
            ts: 0,
            total_calls: 0,
            rps: Ring::new(RING_CAP),
            inflight: Ring::new(RING_CAP),
            p50: Ring::new(RING_CAP),
            p95: Ring::new(RING_CAP),
            p99: Ring::new(RING_CAP),
            ticks: Ring::new(RING_CAP),
            meta: vec![None; capacity],
            workers: (0..capacity).map(|_| WorkerRings::new()).collect(),
            last: vec![WorkerLast::default(); capacity],
            free_slots,
            global_overlap: Ring::new(RING_CAP),
            idx_blocks: Ring::new(RING_CAP),
            idx_events: Ring::new(RING_CAP),
            index_last: IndexLast::default(),
            tk_rps: Ring::new(RING_CAP),
            tk_inflight: Ring::new(RING_CAP),
            tk_p50: Ring::new(RING_CAP),
            tk_p95: Ring::new(RING_CAP),
            tk_p99: Ring::new(RING_CAP),
            tk_error_rate: Ring::new(RING_CAP),
            tk_total_calls: 0,
            tk_unavailable_total: 0,
            pick_p50: Ring::new(RING_CAP),
            pick_p95: Ring::new(RING_CAP),
            pick_p99: Ring::new(RING_CAP),
            pick_total_calls: 0,
        };

        Arc::new(Self {
            live,
            rings: Mutex::new(rings),
            recent: Mutex::new(Ring::new(RECENT_CAP)),
            idx_of: DashMap::new(),
            metrics,
            capacity,
            ts: AtomicU64::new(0),
        })
    }

    /// Claim a slot for a new worker. Returns the slot index (mainly for
    /// logging; callers don't usually need it).
    ///
    /// Refuses rather than overwriting when `worker_id_num` is already
    /// registered — see [`RegisterError::AlreadyRegistered`]. The duplicate
    /// check and the slot pop are under one lock so two concurrent
    /// registrations of the same id cannot both pass.
    pub fn register_worker(&self, meta: WorkerMeta) -> Result<usize, RegisterError> {
        let worker_id_num = meta.worker_id_num;
        let mut rings = self.rings.lock().unwrap();
        if let Some(slot) = self.idx_of.get(&worker_id_num).map(|e| *e.value()) {
            return Err(RegisterError::AlreadyRegistered {
                worker_set: rings
                    .meta
                    .get(slot)
                    .and_then(|m| m.as_ref())
                    .map(|m| m.pool.clone())
                    .unwrap_or_else(|| "<unknown>".to_string()),
            });
        }
        let slot = rings.free_slots.pop().ok_or_else(|| {
            self.live.slots_denied.fetch_add(1, Ordering::Relaxed);
            RegisterError::SlotExhausted {
                capacity: self.capacity,
            }
        })?;

        // Reset the slot's positional state — a recycled slot needs to
        // start clean.
        rings.meta[slot] = Some(meta);
        rings.workers[slot] = WorkerRings::new();
        rings.last[slot] = WorkerLast::default();

        // Zero the slot's atomic counters — also for recycled-slot cleanliness.
        for slot_atom in self.live.per_worker_buckets[slot].iter() {
            slot_atom.store(0, Ordering::Relaxed);
        }
        self.live.per_worker_wins[slot].store(0, Ordering::Relaxed);

        // Install the idx_of mapping last so any concurrent reader either
        // sees the worker as fully ready (slot populated, counters zeroed)
        // or doesn't see it at all.
        self.idx_of.insert(worker_id_num, slot);
        drop(rings);
        Ok(slot)
    }

    /// Release this worker's slot back to the pool. Subsequent
    /// `record_call` invocations for this worker_id_num are dropped
    /// silently. Recent-calls already in the tape retain the
    /// `top_worker_id_num` reference but render as "(removed)" in
    /// `snapshot_json`. No-op if the worker isn't registered.
    // Wired up in Phase C; static-mode startup never removes workers.
    #[allow(dead_code)]
    pub fn unregister_worker(&self, worker_id_num: u64) {
        // `rings` before `idx_of`, matching `register_worker`'s order: the two
        // must not interleave, or a re-registration could pass the duplicate
        // check against an id this call has already removed and then be
        // clobbered by the slot recycling below.
        let mut rings = self.rings.lock().unwrap();
        let Some((_, slot)) = self.idx_of.remove(&worker_id_num) else {
            return;
        };
        if slot < rings.meta.len() {
            rings.meta[slot] = None;
            rings.workers[slot] = WorkerRings::new();
            rings.last[slot] = WorkerLast::default();
            rings.free_slots.push(slot);
        }
    }

    /// Record one ingested KV-cache event (called per event from `ingest.rs`).
    /// Plain atomic adds — cheap on the hot path.
    pub fn record_event(&self, kind: IngestEvent, n_blocks: u64) {
        match kind {
            IngestEvent::Stored => {
                self.live.events_stored.fetch_add(1, Ordering::Relaxed);
                self.live
                    .blocks_stored
                    .fetch_add(n_blocks, Ordering::Relaxed);
            }
            IngestEvent::Removed => {
                self.live.events_removed.fetch_add(1, Ordering::Relaxed);
                self.live
                    .blocks_removed
                    .fetch_add(n_blocks, Ordering::Relaxed);
            }
            IngestEvent::Cleared => {
                self.live.events_cleared.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Increment the in-flight count; the returned guard decrements on drop,
    /// so early returns / errors still balance.
    pub fn inflight_guard(self: &Arc<Self>) -> InflightGuard {
        self.live.inflight.fetch_add(1, Ordering::Relaxed);
        InflightGuard { tel: self.clone() }
    }

    /// Tokenize counterpart of `inflight_guard`. Acquire at the top of
    /// `tokenize_with_fallback`; drop on function return.
    pub fn tokenize_inflight_guard(self: &Arc<Self>) -> TokenizeInflightGuard {
        self.live.tk_inflight.fetch_add(1, Ordering::Relaxed);
        TokenizeInflightGuard { tel: self.clone() }
    }

    /// Local-tokenize comparison counters. Bumped by the side-by-side
    /// harness in `tokenize_with_fallback` when scout-mode is on.
    /// Cumulative — surfaced as running totals in the snapshot rather
    /// than per-tick rates (the signal is "has there ever been a
    /// divergence?" not "what's the divergence rate?").
    pub fn record_local_tokenize_match(&self) {
        self.live.tk_local_matches.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_local_tokenize_divergence(&self) {
        self.live
            .tk_local_divergences
            .fetch_add(1, Ordering::Relaxed);
        // Surface the same event as the aggregate `tokenize_divergence_total`
        // counter consumed by the snapshot JSON's `tokenizeDivergence`
        // field. The per-request block-hash sniff that used to drive it
        // was racy (TTFT outran ZMQ); compare-mode local-vs-render is
        // synchronous and deterministic.
        self.live
            .tokenize_divergence_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_local_tokenize_error(&self) {
        self.live.tk_local_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Production-mode path counters. Exactly one of these is bumped
    /// per tokenize_with_fallback invocation:
    ///   - `record_local_served`: local primary path succeeded.
    ///   - `record_http_fallback`: local primary failed, HTTP saved it.
    ///   - (existing `tk_unavailable`: both failed — degraded response.)
    ///
    /// Sum equals `tk_total_calls`.
    pub fn record_local_served(&self) {
        self.live.tk_local_served.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_http_fallback(&self) {
        self.live.tk_http_fallback.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one completed `Pick()`.
    ///
    /// Exactly one of {success, no_opinion, not_found} per invocation;
    /// `pick_total` is bumped unconditionally for the latency
    /// p50/95/99 rollup. Same try_lock-on-reservoir trick as `record_call` /
    /// `record_tokenize_outcome`.
    pub fn record_pick(&self, outcome: PickOutcome, latency_ms: f64) {
        self.live.pick_total.fetch_add(1, Ordering::Relaxed);
        match outcome {
            PickOutcome::Success => {}
            PickOutcome::NoOpinion => {
                self.live.pick_no_opinion.fetch_add(1, Ordering::Relaxed);
            }
            PickOutcome::NotFound => {
                self.live.pick_not_found.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Ok(mut lat) = self.live.pick_latencies.try_lock()
            && lat.len() < LATENCY_CAP
        {
            lat.push(latency_ms as f32);
        }
    }

    /// Record one individual `/tokenize` HTTP attempt. Called per loop
    /// iteration in the retry fallback — both Ok and Err arms. Cheap:
    /// one or two Relaxed atomics.
    pub fn record_tokenize_attempt(&self, success: bool) {
        self.live.tk_attempts.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.live.tk_attempt_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record one completed `tokenize_with_fallback` invocation —
    /// whether the outcome was `Ok(tokens)` or `Unavailable`. Same
    /// try_lock-on-latency-reservoir trick as `record_call`; bump
    /// counters atomically.
    pub fn record_tokenize_outcome(&self, latency_ms: f64, unavailable: bool) {
        self.live.tk_calls.fetch_add(1, Ordering::Relaxed);
        if unavailable {
            self.live.tk_unavailable.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut lat) = self.live.tk_latencies.try_lock()
            && lat.len() < LATENCY_CAP
        {
            lat.push(latency_ms as f32);
        }
    }

    /// Record one completed score() call. Hot path: atomic bumps + one
    /// `try_lock` push for the recent-calls tape (dropped if contended).
    pub fn record_call(&self, sample: CallSample<'_>) {
        self.live.calls.fetch_add(1, Ordering::Relaxed);

        for (num, overlap) in sample.overlaps {
            if let Some(slot_ref) = self.idx_of.get(num) {
                let idx = *slot_ref.value();
                let b = ((overlap * NUM_BUCKETS as f64) as usize).min(NUM_BUCKETS - 1);
                self.live.per_worker_buckets[idx][b].fetch_add(1, Ordering::Relaxed);
            }
        }

        if let Ok(mut lat) = self.live.latencies.try_lock()
            && lat.len() < LATENCY_CAP
        {
            lat.push(sample.latency_ms as f32);
        }

        if let Some(top) = sample.top
            && let Some(slot_ref) = self.idx_of.get(&top.worker_id_num)
        {
            let idx = *slot_ref.value();
            drop(slot_ref);
            self.live.per_worker_wins[idx].fetch_add(1, Ordering::Relaxed);
            if let Ok(mut recent) = self.recent.try_lock() {
                recent.push(RecentCall {
                    ts: self.ts.load(Ordering::Relaxed),
                    model: sample.model.to_string(),
                    tokens: sample.tokens,
                    top_worker: top.addr,
                    top_overlap: top.overlap,
                    total_blocks: top.total,
                    matched_blocks: top.matched,
                    latency_ms: sample.latency_ms,
                    top_worker_id_num: top.worker_id_num,
                });
            }
        }
    }

    /// Spawn the 1 Hz rollup task. Returns immediately; the task runs until
    /// `cancel` fires.
    /// `prom` receives the radix-tree counters this tick produced. The rollup
    /// is the only place that can: the ingest atomics are `swap(0)`-ed here, so
    /// nothing downstream could read them afterwards.
    pub fn spawn_rollup(
        self: Arc<Self>,
        pools: Arc<crate::pool::PoolRegistry>,
        prom: Arc<crate::prom::PromMetrics>,
        cancel: CancellationToken,
    ) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut n: u64 = 0;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tick.tick() => {
                        // `KvIndexer` doesn't implement `shard_sizes` (returns []),
                        // so the only size source is `dump_events()` — O(tree), so
                        // throttle it. Done BEFORE locking rings (never hold the
                        // std::sync::Mutex guard across an await).
                        let dump = if n.is_multiple_of(DUMP_EVERY_TICKS) {
                            // Sum over every pool's radix tree — blocks add,
                            // worker ids union. A pool whose dump errors is
                            // skipped (best-effort gauge).
                            let mut blocks = 0u64;
                            let mut workers = std::collections::HashSet::new();
                            let mut any = false;
                            // Snapshot taken per tick, not held: a pool added
                            // since the last tick is counted, and one removed
                            // stops being counted — and, more to the point,
                            // stops being PINNED. Holding the indexers in a
                            // field kept a removed pool's radix tree and its OS
                            // thread alive for the life of the process.
                            let current = pools.snapshot();
                            for pool in current.iter() {
                                let indexer = &pool.indexer;
                                if let Ok(events) = indexer.dump_events().await {
                                    any = true;
                                    for e in &events {
                                        if let KvCacheEventData::Stored(d) = &e.event.data {
                                            blocks += d.blocks.len() as u64;
                                        }
                                        workers.insert(e.worker_id);
                                    }
                                }
                            }
                            any.then_some((blocks, workers.len() as u64))
                        } else {
                            None
                        };
                        if let Some((blocks, workers)) = dump {
                            prom.set_index_size(blocks, workers);
                        }
                        let d = self.rollup_once(dump);
                        prom.record_index_events(
                            d.events_stored,
                            d.events_removed,
                            d.events_cleared,
                            d.blocks_stored,
                            d.blocks_removed,
                        );
                        // Worker reachability comes from the ROUTING state, not
                        // from this module's slot pool. The two are decoupled on
                        // purpose: the pool is a dashboard budget (small, and
                        // preallocated), while `kinference_workers` must cover every
                        // worker actually serving traffic however many that is.
                        let (ok, unresponsive) = self.worker_health(&pools);
                        prom.set_worker_states(ok, unresponsive);
                        prom.set_worker_states_by_model(&self.worker_health_by_model(&pools));
                        prom.set_worker_saturation(&self.worker_saturation(&pools));
                        prom.set_dashboard_slots(d.slots_used, d.slots_capacity);
                        prom.record_dashboard_slots_denied(d.slots_denied);
                        n = n.wrapping_add(1);
                    }
                }
            }
        });
    }

    /// `dump` is `Some((blocks, workers))` on throttled ticks where we recomputed
    /// the tree size; `None` otherwise (the previous gauge value is held).
    /// Count routable workers by reachability, for `kinference_workers`.
    ///
    /// Walks the POOL registries rather than this module's slots, so the count
    /// is independent of `dashboard_max_workers` — a worker missing from the
    /// dashboard is still counted here, because it is still being routed to.
    ///
    /// `ids()` rather than `iter_snapshot()`: only the id is needed to look up
    /// staleness, and cloning a `WorkerEntry` per worker per second would be
    /// pure waste at fleet scale.
    fn worker_health(&self, pools: &crate::pool::PoolRegistry) -> (u64, u64) {
        let (mut ok, mut unresponsive) = (0u64, 0u64);
        for pool in pools.snapshot().iter() {
            for id in pool.registry.ids() {
                // Same threshold the dashboard renders, from one helper, so the
                // two surfaces cannot drift apart.
                if derive_health(self.metrics.age_ms_for(id)) == "ok" {
                    ok += 1;
                } else {
                    unresponsive += 1;
                }
            }
        }
        (ok, unresponsive)
    }

    /// The same reachability split by served model, for
    /// `kinference_workers_by_model`.
    ///
    /// Uses the registry's `by_model` index rather than re-deriving membership,
    /// so this agrees with what routing actually sees. A worker serving several
    /// models appears under each — which is why this feeds a separate family
    /// and not a label on `kinference_workers`.
    fn worker_health_by_model(&self, pools: &crate::pool::PoolRegistry) -> Vec<(String, u64, u64)> {
        let mut acc: HashMap<String, (u64, u64)> = HashMap::new();
        for pool in pools.snapshot().iter() {
            for model in pool.registry.known_models() {
                let e = acc.entry(model.clone()).or_default();
                for (id, _) in pool.registry.workers_for_model(&model) {
                    // Same helper as `worker_health`, so the per-model split and
                    // the fleet total cannot disagree about what "ok" means.
                    if derive_health(self.metrics.age_ms_for(id)) == "ok" {
                        e.0 += 1;
                    } else {
                        e.1 += 1;
                    }
                }
            }
        }
        acc.into_iter().map(|(m, (a, b))| (m, a, b)).collect()
    }

    /// The two saturation gauges per worker, as the windowed means an
    /// `InferencePolicy` predicate reads.
    ///
    /// Published so a threshold can be CALIBRATED rather than guessed: these
    /// are the same numbers `queuedPrompts` and `kvCacheUsage` compare
    /// against, so a value seen here is a value a policy can act on.
    ///
    /// Keyed by `(worker, model)` off the same `by_model` index
    /// `worker_health_by_model` uses, so the two families agree on which
    /// workers serve what. A worker serving several models appears once per
    /// model with the same reading — the gauges are properties of the ENGINE,
    /// not of the model, and duplicating is what keeps the label set joinable
    /// against the per-model families.
    fn worker_saturation(
        &self,
        pools: &crate::pool::PoolRegistry,
    ) -> Vec<(String, String, f64, f64)> {
        let mut out = Vec::new();
        for pool in pools.snapshot().iter() {
            for model in pool.registry.known_models() {
                for (id, entry) in pool.registry.workers_for_model(&model) {
                    let m = self.metrics.latest(id);
                    out.push((
                        entry.score_addr.clone(),
                        model.clone(),
                        m.waiting_mean as f64,
                        m.kv_cache_usage_mean as f64,
                    ));
                }
            }
        }
        out
    }

    /// Returns the radix-tree ingest deltas consumed this tick, so the caller
    /// can forward them to Prometheus. They are `swap(0)`-ed below, so this is
    /// the only chance to see them.
    fn rollup_once(&self, dump: Option<(u64, u64)>) -> RollupDeltas {
        let calls = self.live.calls.swap(0, Ordering::Relaxed);
        let inflight = self.live.inflight.load(Ordering::Relaxed).max(0) as f64;
        let mut lat = {
            let mut g = self.live.latencies.lock().unwrap();
            std::mem::take(&mut *g)
        };
        let (p50, p95, p99) = percentiles(&mut lat);

        // Tokenize-activity counter swaps — same shape as the score
        // counters above, run side-by-side so the dashboard's two
        // activity panels show consistent same-tick data.
        let tk_calls = self.live.tk_calls.swap(0, Ordering::Relaxed);
        let tk_attempts = self.live.tk_attempts.swap(0, Ordering::Relaxed);
        let tk_errors = self.live.tk_attempt_errors.swap(0, Ordering::Relaxed);
        let tk_unavail = self.live.tk_unavailable.swap(0, Ordering::Relaxed);
        let tk_inflight = self.live.tk_inflight.load(Ordering::Relaxed).max(0) as f64;
        let mut tk_lat = {
            let mut g = self.live.tk_latencies.lock().unwrap();
            std::mem::take(&mut *g)
        };
        let (tk_p50, tk_p95, tk_p99) = percentiles(&mut tk_lat);
        let tk_error_rate = if tk_attempts > 0 {
            tk_errors as f64 / tk_attempts as f64
        } else {
            0.0
        };

        // Pick latency reservoir + per-pick total.
        let picks_this_tick = self.live.pick_total.load(Ordering::Relaxed);
        let mut pick_lat = {
            let mut g = self.live.pick_latencies.lock().unwrap();
            std::mem::take(&mut *g)
        };
        let (pick_p50, pick_p95, pick_p99) = percentiles(&mut pick_lat);

        let mut rings = self.rings.lock().unwrap();
        rings.ts += 1;
        let ts = rings.ts;
        self.ts.store(ts, Ordering::Relaxed);
        rings.total_calls += calls;
        rings.tk_total_calls += tk_calls;
        rings.tk_unavailable_total += tk_unavail;

        rings.rps.push(calls as f64);
        rings.inflight.push(inflight);
        rings.p50.push(p50);
        rings.p95.push(p95);
        rings.p99.push(p99);
        rings.ticks.push(ts);
        rings.tk_rps.push(tk_calls as f64);
        rings.tk_inflight.push(tk_inflight);
        rings.tk_p50.push(tk_p50);
        rings.tk_p95.push(tk_p95);
        rings.tk_p99.push(tk_p99);
        rings.tk_error_rate.push(tk_error_rate);
        rings.pick_p50.push(pick_p50);
        rings.pick_p95.push(pick_p95);
        rings.pick_p99.push(pick_p99);
        rings.pick_total_calls = picks_this_tick;

        // Active slots only — skip None entries.
        let mut global = [0f64; NUM_BUCKETS];
        // Collect (slot_idx, worker_id_num) first so we can release the
        // immutable borrow on `rings.meta` before mutating `rings.workers` /
        // `rings.last` below.
        let active: Vec<(usize, u64)> = rings
            .meta
            .iter()
            .enumerate()
            .filter_map(|(i, m)| m.as_ref().map(|m| (i, m.worker_id_num)))
            .collect();

        let slots_used = (self.capacity - rings.free_slots.len()) as u64;
        let slots_denied = self.live.slots_denied.swap(0, Ordering::Relaxed);
        for (idx, worker_id_num) in active {
            let s = self.metrics.latest(worker_id_num);
            let age = self.metrics.age_ms_for(worker_id_num);

            let mut buckets = [0u64; NUM_BUCKETS];
            for (b, slot_atom) in self.live.per_worker_buckets[idx].iter().enumerate() {
                buckets[b] = slot_atom.swap(0, Ordering::Relaxed);
                global[b] += buckets[b] as f64;
            }
            let wins = self.live.per_worker_wins[idx].swap(0, Ordering::Relaxed);
            let score_calls: u64 = buckets.iter().sum();

            let wr = &mut rings.workers[idx];
            wr.kv.push(s.kv_cache_usage as f64);
            wr.qw.push(s.waiting as f64);
            wr.qr.push(s.running as f64);
            wr.qs.push(s.swapped as f64);
            wr.age.push(age as f64);
            wr.wins.push(wins as f64);
            wr.buckets.push(buckets);
            wr.phr.push(s.prefix_cache_hit_rate as f64);

            rings.last[idx] = WorkerLast {
                kv: s.kv_cache_usage,
                qw: s.waiting,
                qr: s.running,
                qs: s.swapped,
                age,
                num_gpu_blocks: s.num_gpu_blocks,
                wins,
                score_calls,
                buckets,
                health: derive_health(age),
            };
        }
        rings.global_overlap.push(global);

        // Radix-tree index gauges + ingest rates (counters reset each tick).
        let stored = self.live.events_stored.swap(0, Ordering::Relaxed);
        let removed = self.live.events_removed.swap(0, Ordering::Relaxed);
        let cleared = self.live.events_cleared.swap(0, Ordering::Relaxed);
        let blk_stored = self.live.blocks_stored.swap(0, Ordering::Relaxed);
        let blk_removed = self.live.blocks_removed.swap(0, Ordering::Relaxed);
        let events_total = (stored + removed + cleared) as f64;
        // Gauges (blocks/workers) refresh only on throttled dump ticks; hold the
        // previous value otherwise.
        if let Some((blocks, workers)) = dump {
            rings.index_last.blocks = blocks;
            rings.index_last.workers = workers;
        }
        rings.index_last.events_per_sec = events_total;
        rings.index_last.stored_per_sec = stored as f64;
        rings.index_last.removed_per_sec = removed as f64;
        rings.index_last.cleared_per_sec = cleared as f64;
        rings.index_last.blocks_stored_per_sec = blk_stored as f64;
        rings.index_last.blocks_removed_per_sec = blk_removed as f64;
        let blocks_now = rings.index_last.blocks as f64;
        rings.idx_blocks.push(blocks_now);
        rings.idx_events.push(events_total);

        RollupDeltas {
            events_stored: stored,
            events_removed: removed,
            events_cleared: cleared,
            blocks_stored: blk_stored,
            blocks_removed: blk_removed,
            slots_used,
            slots_capacity: self.capacity as u64,
            slots_denied,
        }
    }

    /// Build the snapshot JSON matching the dashboard contract.
    pub fn snapshot_json(&self) -> Value {
        let rings = self.rings.lock().unwrap();
        let recent = self.recent.lock().unwrap();

        // Build the workers array (active slots only) AND a side-map from
        // worker_id_num -> array index, so recent-call entries can resolve
        // their `top_worker_id_num` back to a JSON-array position.
        let mut workers: Vec<Value> = Vec::with_capacity(rings.meta.len());
        let mut id_to_array_idx: HashMap<u64, usize> = HashMap::new();
        for (slot, slot_meta) in rings.meta.iter().enumerate() {
            let Some(m) = slot_meta else { continue };
            let last = rings.last[slot];
            let wr = &rings.workers[slot];
            // Pull fresh from MetricsRegistry — cheap DashMap reads.
            let latest = self.metrics.latest(m.worker_id_num);
            let cr = latest.completion_rate_per_sec;
            let pc_hit_rate = latest.prefix_cache_hit_rate;
            let pc_queries = latest.prefix_cache_queries_total;
            let served_models: Vec<Value> = self
                .metrics
                .models(m.worker_id_num)
                .models
                .into_iter()
                .map(|sm| {
                    json!({
                        "name": sm.name,
                        // 0 = unknown — matches the gRPC convention
                        "max_model_len": sm.max_model_len.unwrap_or(0),
                    })
                })
                .collect();
            id_to_array_idx.insert(m.worker_id_num, workers.len());
            workers.push(json!({
                "id": m.id,
                "pool": m.pool,
                "namespace": m.namespace,
                "ip": m.ip,
                "port": m.port,
                "block_size": m.block_size,
                "num_gpu_blocks": last.num_gpu_blocks,
                "health": last.health,
                "kv_cache_usage": last.kv,
                "queue_waiting": last.qw,
                "queue_running": last.qr,
                "queue_swapped": last.qs,
                "metrics_age_ms": last.age,
                "completion_rate_per_sec": cr,
                // Engine-reported prefix-cache hit rate (sanity check on
                // the monitor's predicted overlap, NOT a routing input).
                // See proto note in plan: deliberately not on WorkerScore.
                "prefix_cache_hit_rate": pc_hit_rate,
                "prefix_cache_queries_total": pc_queries,
                "served_models": served_models,
                "wins": last.wins,
                "scoreCalls": last.score_calls,
                "overlapBuckets": last.buckets.to_vec(),
                "hist": {
                    "kv": wr.kv.snapshot(),
                    "qw": wr.qw.snapshot(),
                    "qr": wr.qr.snapshot(),
                    "qs": wr.qs.snapshot(),
                    "age": wr.age.snapshot(),
                    "wins": wr.wins.snapshot(),
                    "buckets": wr.buckets.iter().map(|b| b.to_vec()).collect::<Vec<_>>(),
                    "phr": wr.phr.snapshot(),
                },
            }));
        }

        // `recent.iter()` yields oldest→newest; rev() emits newest-first
        // to match the previous `push_front` behavior.
        let recent_calls: Vec<Value> = recent
            .iter()
            .rev()
            .map(|c| {
                let top_idx = id_to_array_idx
                    .get(&c.top_worker_id_num)
                    .map(|i| Value::from(*i))
                    .unwrap_or(Value::Null);
                json!({
                    "ts": c.ts,
                    "model": c.model,
                    "tokens": c.tokens,
                    "topWorker": c.top_worker,
                    "topOverlap": c.top_overlap,
                    "total_blocks": c.total_blocks,
                    "matched_blocks": c.matched_blocks,
                    "latencyMs": c.latency_ms,
                    "topWorkerIdx": top_idx,
                })
            })
            .collect();

        // Fleet-aggregate prefix-cache hit rate. Volume-weighted: Σ hits /
        // Σ queries across all workers' cumulative counters. Lifetime
        // measurement (the deltas-over-window version would need per-worker
        // history access we don't currently expose; cumulative is good
        // enough for "is routing leveraging the cache overall?"). SGLang
        // workers contribute 0 weight because they don't expose raw
        // counters — the fleet headline is effectively vLLM-only in a
        // mixed fleet, by design. Per-worker rates remain visible
        // individually in the workers table.
        let mut fleet_hits: u128 = 0;
        let mut fleet_queries: u128 = 0;
        for entry in rings.meta.iter().flatten() {
            let s = self.metrics.latest(entry.worker_id_num);
            fleet_hits = fleet_hits.saturating_add(s.prefix_cache_hits_total as u128);
            fleet_queries = fleet_queries.saturating_add(s.prefix_cache_queries_total as u128);
        }
        let fleet_prefix_cache_hit_rate: f64 = if fleet_queries > 0 {
            fleet_hits as f64 / fleet_queries as f64
        } else {
            0.0
        };

        json!({
            "ts": rings.ts,
            "numBuckets": NUM_BUCKETS,
            "version": VERSION,
            "score": {
                "rps": rings.rps.last_or_default(),
                "inflight": rings.inflight.last_or_default(),
                "p50": rings.p50.last_or_default(),
                "p95": rings.p95.last_or_default(),
                "p99": rings.p99.last_or_default(),
                "total": rings.total_calls,
                "rpsHist": rings.rps.snapshot(),
                "inflightHist": rings.inflight.snapshot(),
                "p50Hist": rings.p50.snapshot(),
                "p95Hist": rings.p95.snapshot(),
                "p99Hist": rings.p99.snapshot(),
            },
            "tokenize": {
                "rps": rings.tk_rps.last_or_default(),
                "inflight": rings.tk_inflight.last_or_default(),
                "p50": rings.tk_p50.last_or_default(),
                "p95": rings.tk_p95.last_or_default(),
                "p99": rings.tk_p99.last_or_default(),
                "errorRate": rings.tk_error_rate.last_or_default(),
                "total": rings.tk_total_calls,
                "unavailableTotal": rings.tk_unavailable_total,
                "rpsHist": rings.tk_rps.snapshot(),
                "inflightHist": rings.tk_inflight.snapshot(),
                "p50Hist": rings.tk_p50.snapshot(),
                "p95Hist": rings.tk_p95.snapshot(),
                "p99Hist": rings.tk_p99.snapshot(),
                "errorRateHist": rings.tk_error_rate.snapshot(),
                // Scout-mode (mode = "compare") side-by-side audit.
                "localMatchesTotal": self.live.tk_local_matches.load(Ordering::Relaxed),
                "localDivergencesTotal": self.live.tk_local_divergences.load(Ordering::Relaxed),
                "localErrorsTotal": self.live.tk_local_errors.load(Ordering::Relaxed),
                // Production-mode (mode = "local") path breakdown.
                "localServedTotal": self.live.tk_local_served.load(Ordering::Relaxed),
                "httpFallbackTotal": self.live.tk_http_fallback.load(Ordering::Relaxed),
            },
            "workers": workers,
            "globalOverlapHist": rings.global_overlap.iter().map(|b| b.to_vec()).collect::<Vec<_>>(),
            "recentCalls": recent_calls,
            "ticksHistory": rings.ticks.snapshot(),
            "index": {
                "blocks": rings.index_last.blocks,
                "workers": rings.index_last.workers,
                "eventsPerSec": rings.index_last.events_per_sec,
                "storedPerSec": rings.index_last.stored_per_sec,
                "removedPerSec": rings.index_last.removed_per_sec,
                "clearedPerSec": rings.index_last.cleared_per_sec,
                "blocksStoredPerSec": rings.index_last.blocks_stored_per_sec,
                "blocksRemovedPerSec": rings.index_last.blocks_removed_per_sec,
                "blocksHist": rings.idx_blocks.snapshot(),
                "eventsHist": rings.idx_events.snapshot(),
            },
            "engine": {
                // Engine-reported (not monitor-predicted) prefix-cache
                // hit rate, volume-weighted across the fleet. Sanity
                // check on whether prefix routing is actually paying
                // off. 0 when no worker exposes the counter pair.
                "prefixCacheHitRate": fleet_prefix_cache_hit_rate,
                "prefixCacheQueriesTotal": fleet_queries as u64,
            },
            // `Pick()` outcomes. `total` = every pick decided on;
            // `noOpinion` = the subset with nothing routable in the body
            // (parse failure, no model, body cap exceeded) — `BAD_REQUEST` on
            // the wire; `notFound` = the subset where no worker served the
            // model — `NO_WORKERS`. The dashboard hides its chrome line while
            // `total` is 0, so a process that has taken no traffic adds no
            // visual noise.
            "picks": {
                "total": self.live.pick_total.load(Ordering::Relaxed),
                "noOpinion": self.live.pick_no_opinion.load(Ordering::Relaxed),
                "notFound": self.live.pick_not_found.load(Ordering::Relaxed),
                "p50": rings.pick_p50.last_or_default(),
                "p95": rings.pick_p95.last_or_default(),
                "p99": rings.pick_p99.last_or_default(),
                // Tokenize-divergence events, sourced from compare-mode's
                // synchronous local-vs-render check. Non-zero means
                // kinference's local tokenizer disagrees with the engine's
                // `/render` for a token sequence — chat-template, special-
                // token, or vocab-version mismatch. Always 0 when
                // `tokenize.mode` is not `compare`.
                "tokenizeDivergence": self.live.tokenize_divergence_total.load(Ordering::Relaxed),
            },
        })
    }
}

pub struct InflightGuard {
    tel: Arc<Telemetry>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.tel.live.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Tokenize counterpart of `InflightGuard`. Same Arc-held-by-guard
/// semantics: the telemetry stays alive at least as long as any
/// outstanding tokenize_with_fallback invocation, and the counter
/// balances on any return path (incl. panics).
pub struct TokenizeInflightGuard {
    tel: Arc<Telemetry>,
}

impl Drop for TokenizeInflightGuard {
    fn drop(&mut self) {
        self.tel.live.tk_inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Health is purely about metrics reachability: a worker is "unresponsive"
/// when its last successful `/metrics` scrape is older than 5s (or it has no
/// metrics endpoint, in which case the age grows unbounded). Queue depth and
/// KV-cache pressure are surfaced as their own columns, not folded into health.
fn derive_health(age_ms: u64) -> &'static str {
    if age_ms > 5000 { "unresponsive" } else { "ok" }
}

/// p50/p95/p99 of a latency sample set (ms). Returns zeros when empty.
fn percentiles(samples: &mut [f32]) -> (f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let at = |q: f64| -> f64 {
        let idx = ((samples.len() as f64 - 1.0) * q).round() as usize;
        samples[idx.min(samples.len() - 1)] as f64
    };
    (at(0.50), at(0.95), at(0.99))
}

#[cfg(test)]
mod tests;
