//! Per-worker ZMQ ingest listener for vLLM KV-cache events.
//!
//! One [`spawn_worker_listener`] task per worker. Each task owns its sockets,
//! parses vLLM's wire-format events through `dynamo_kv_router::zmq_wire`, and
//! feeds them into the shared [`KvIndexer`] (radix tree) and our bookkeeping
//! ([`InflightPrefixes`], [`PromMetrics`], [`WorkerRegistry`]) in
//! [`apply_frame`].
//!
//! Modelled after dynamo's `standalone_indexer` listener
//! (`lib/kv-router/src/standalone_indexer/listener.rs`): the SUB socket and
//! the DEALER replay socket live in one task, and gap recovery is synchronous
//! and inline. When a live frame arrives with `seq > expected`, the loop
//! requests a replay, applies every replayed event in order, then applies the
//! live frame. Since gaps are only detected from a frame in hand, the
//! listener never waits for a seq the publisher has already passed.
//!
//! Sequence-cursor semantics come from
//! [`dynamo_kv_router::recovery::CursorState`]. The ZMQ socket helpers are
//! reimplemented here because dynamo's are `pub(super)`.
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow};
use dynamo_kv_router::indexer::{KvIndexer, KvIndexerInterface};
use dynamo_kv_router::protocols::{KvCacheEventData, WorkerWithDpRank};
use dynamo_kv_router::recovery::{CursorObservation, CursorState};
use dynamo_kv_router::zmq_wire::{
    KvCacheSpecKind, RawKvEvent, ZmqEventNormalizer, decode_event_batch,
};
use tokio::io::unix::AsyncFd;
use tokio_util::sync::CancellationToken;

use crate::discovery::WorkerConfig;
use crate::eviction::{EvictionReason, EvictionRequest, EvictionTx};
use crate::inflight_prefixes::InflightPrefixes;
use crate::prom::{IngestEvent, PromMetrics};
use crate::workers::WorkerRegistry;

/// Per-recv DEALER timeout, keeping the replay loop responsive.
const REPLAY_DEALER_RCVTIMEO_MS: i32 = 500;

/// Spawn the ingest listener for a single worker. `endpoint_key` is the
/// worker's `score_addr`, the key into [`InflightPrefixes`]. `eviction_tx`
/// lets the listener request eviction on a block_size mismatch.
#[allow(clippy::too_many_arguments)]
pub fn spawn_worker_listener(
    cfg: WorkerConfig,
    block_size: u32,
    replay_timeout_ms: u64,
    endpoint_key: String,
    indexer: Arc<KvIndexer>,
    prom: Arc<PromMetrics>,
    inflight_prefixes: Arc<InflightPrefixes>,
    workers: Arc<WorkerRegistry>,
    eviction_tx: EvictionTx,
    cancel: CancellationToken,
) {
    // Diagnostic switch: `KINFERENCE_DISABLE_INGEST=1` skips ZMQ ingest
    // entirely. Workers are still marked ready, but the radix tree stays
    // empty.
    if std::env::var("KINFERENCE_DISABLE_INGEST").is_ok() {
        tracing::warn!(
            worker_id = cfg.worker_id_str,
            "KINFERENCE_DISABLE_INGEST set — skipping ZMQ ingest. \
             Marking worker ready immediately; radix tree will stay empty \
             and cache-affinity routing will be no-op for this run."
        );
        workers.set_initial_sync_complete(cfg.worker_id_num);
        // This branch holds no references.
        let _ = (
            indexer,
            prom,
            inflight_prefixes,
            endpoint_key,
            block_size,
            cancel,
        );
        return;
    }

    // No publish socket declared (e.g. the `InferenceWorkerSet` omits
    // `spec.kvEvents`): these engines don't publish. Mark the worker ready and
    // route on load alone, without a prefix signal.
    if cfg.zmq_endpoint.as_deref().unwrap_or("").is_empty() {
        tracing::info!(
            worker_id = cfg.worker_id_str,
            "no KV-event socket declared for this worker; routing on load metrics \
             alone (no prefix-cache signal from it)"
        );
        workers.set_initial_sync_complete(cfg.worker_id_num);
        let _ = (
            indexer,
            prom,
            inflight_prefixes,
            endpoint_key,
            block_size,
            cancel,
        );
        return;
    }

    let replay_wall_budget = Duration::from_millis(replay_timeout_ms);
    tokio::spawn(async move {
        loop {
            if cancel.is_cancelled() {
                return;
            }
            if let Err(err) = run_listener(
                &cfg,
                block_size,
                replay_wall_budget,
                &endpoint_key,
                indexer.clone(),
                prom.clone(),
                inflight_prefixes.clone(),
                workers.clone(),
                eviction_tx.clone(),
                cancel.clone(),
            )
            .await
            {
                tracing::warn!(
                    worker_id = cfg.worker_id_str,
                    endpoint = ?cfg.zmq_endpoint,
                    error = %err,
                    "Listener errored; reconnecting in 1s"
                );
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    });
}

/// Per-listener mutable state: the cursor (for gap detection) and the event
/// normalizer, built once from the pool's `kv_block_size`.
struct ListenerState {
    cursor: CursorState,
    normalizer: ZmqEventNormalizer,
    /// Debounces block_size-mismatch eviction so a single corrupt frame
    /// can't take a healthy worker out of rotation. See [`BlockSizeGuard`].
    block_size_guard: BlockSizeGuard,
}

/// Why a `replay_gap` call ended without delivering everything
/// requested. Ported from dynamo's `standalone_indexer::listener`
/// (`ReplayRecoveryFailureReason`).
#[derive(Clone, Copy, Debug)]
enum ReplayFailureReason {
    /// Sentinel arrived before any frame did. Publisher had nothing
    /// in retention for the requested range.
    Empty,
    /// First replayed frame's seq was past `start_seq`. Publisher
    /// already evicted the head of the requested range.
    StartedAfterRequested,
    /// Sequence numbers in the replayed stream weren't contiguous.
    /// Either the publisher dropped some, or HWM truncated us.
    NonContiguous,
    /// Last replayed seq was below `end_seq - 1`. Stream ended (via
    /// sentinel or socket close) before reaching the target.
    EndedBeforeTarget,
    /// Local wall budget exhausted before the publisher's sentinel
    /// arrived. Not in dynamo's enum, which has no such timeout.
    WallBudgetExhausted,
}

impl ReplayFailureReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::StartedAfterRequested => "started_after_requested",
            Self::NonContiguous => "non_contiguous",
            Self::EndedBeforeTarget => "ended_before_target",
            Self::WallBudgetExhausted => "wall_budget_exhausted",
        }
    }
}

/// Per-replay-call tracker, used at exit to decide whether to warn that the
/// replay was incomplete, and why. Ported from dynamo's
/// `ReplayRecoveryProgress`.
struct ReplayProgress {
    start_seq: u64,
    end_seq: u64,
    first_replayed: Option<u64>,
    last_replayed: Option<u64>,
    expected_next: u64,
    replayed: u64,
    non_contiguous: bool,
}

impl ReplayProgress {
    fn new(start_seq: u64, end_seq: u64) -> Self {
        Self {
            start_seq,
            end_seq,
            first_replayed: None,
            last_replayed: None,
            expected_next: start_seq,
            replayed: 0,
            non_contiguous: false,
        }
    }

    fn record(&mut self, seq: u64) {
        if self.first_replayed.is_none() {
            self.first_replayed = Some(seq);
        }
        if seq != self.expected_next {
            self.non_contiguous = true;
        }
        self.expected_next = seq.saturating_add(1);
        self.last_replayed = Some(seq);
        self.replayed += 1;
    }

    fn failure_reason(&self, wall_budget_hit: bool) -> Option<ReplayFailureReason> {
        if self.start_seq >= self.end_seq {
            return None;
        }
        if wall_budget_hit {
            return Some(ReplayFailureReason::WallBudgetExhausted);
        }
        if self.replayed == 0 {
            return Some(ReplayFailureReason::Empty);
        }
        if self
            .first_replayed
            .is_some_and(|first| first > self.start_seq)
        {
            return Some(ReplayFailureReason::StartedAfterRequested);
        }
        if self.non_contiguous {
            return Some(ReplayFailureReason::NonContiguous);
        }
        if self
            .last_replayed
            .is_some_and(|last| last < self.end_seq - 1)
        {
            return Some(ReplayFailureReason::EndedBeforeTarget);
        }
        None
    }
}

/// Run a single listener until it errors or `cancel` fires. All gap recovery
/// happens inside the SUB-recv branch, synchronously, before the triggering
/// frame is applied.
#[allow(clippy::too_many_arguments)]
async fn run_listener(
    cfg: &WorkerConfig,
    block_size: u32,
    replay_wall_budget: Duration,
    endpoint_key: &str,
    indexer: Arc<KvIndexer>,
    prom: Arc<PromMetrics>,
    inflight_prefixes: Arc<InflightPrefixes>,
    workers: Arc<WorkerRegistry>,
    eviction_tx: EvictionTx,
    cancel: CancellationToken,
) -> Result<()> {
    let worker_id_num = cfg.worker_id_num;
    let worker_id_str = cfg.worker_id_str.clone();

    let live_endpoint = cfg
        .zmq_endpoint
        .clone()
        .context("worker has no zmq_endpoint configured (ingest should only run in real mode)")?;
    let replay_endpoint = cfg.replay_endpoint.clone();

    let mut sub = ZmqSocket::sub(&live_endpoint, &worker_id_str)?;
    // Replay is off unless `KINFERENCE_ENABLE_REPLAY=1`. Live ingest still
    // flows, but history is never fetched and gaps aren't recovered. Off by
    // default because the replay loop contends with tokenization for tail
    // latency, and because replayed history can be stale when the engine's
    // KV cache was cleared without a restart, producing phantom cache hits.
    let enable_replay = std::env::var("KINFERENCE_ENABLE_REPLAY").is_ok();
    let mut replay = if !enable_replay {
        tracing::info!(
            worker_id = worker_id_str,
            "replay disabled by default; set KINFERENCE_ENABLE_REPLAY=1 to turn on. \
             Live SUB ingest active; historical events and gap recovery are off."
        );
        None
    } else {
        match replay_endpoint.as_deref() {
            Some(endpoint) => Some(ZmqSocket::dealer(endpoint, &worker_id_str)?),
            None => {
                tracing::warn!(
                    worker_id = worker_id_str,
                    "KINFERENCE_ENABLE_REPLAY set but no replay endpoint configured; \
                     gap recovery still off"
                );
                None
            }
        }
    };

    let mut state = ListenerState {
        cursor: CursorState::Initial,
        normalizer: ZmqEventNormalizer::new(block_size),
        block_size_guard: BlockSizeGuard::new(block_size),
    };
    // Whether this listener has reported that events are flowing, so the
    // registry write happens once.
    let mut observed_events = false;
    // Liveness accounting for the heartbeat. `frames_seen` counts every
    // message, including ones dropped for shape.
    let mut frames_seen: u64 = 0;
    let mut events_applied: u64 = 0;
    let listener_started = Instant::now();
    let mut warned_idle = false;
    let mut heartbeat = tokio::time::interval(SUB_HEARTBEAT_IVL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick.
    heartbeat.tick().await;

    // Mark ready as soon as the sockets are connected, as dynamo does. The
    // radix tree fills in lazily: the first live frame triggers a bounded
    // `replay_gap(0, got)` for the history (bounded, since only bounded
    // requests get a sentinel). On an idle cluster the tree stays empty until
    // the first event, which is harmless.
    workers.set_initial_sync_complete(worker_id_num);
    tracing::info!(
        worker_id = worker_id_str,
        endpoint = live_endpoint,
        replay = replay_endpoint.as_deref().unwrap_or("-"),
        replay_wall_budget_ms = replay_wall_budget.as_millis() as u64,
        "listener ready"
    );

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(
                    worker_id = worker_id_str,
                    "listener exiting on cancel"
                );
                return Ok(());
            }
            // ZMQ `connect` succeeds with no peer, so report silence, which
            // can mean a wrong address as well as an engine publishing
            // nothing.
            _ = heartbeat.tick() => {
                let idle = listener_started.elapsed();
                // Warn once only, since a healthy fleet serving cached
                // prefixes can also be silent.
                if frames_seen == 0 && !warned_idle && idle >= SUB_IDLE_WARN_AFTER {
                    warned_idle = true;
                    tracing::warn!(
                        worker_id = worker_id_str,
                        endpoint = live_endpoint,
                        "KV-event SUB socket connected but no frames received"
                    );
                } else {
                    tracing::trace!(
                        worker_id = worker_id_str,
                        endpoint = live_endpoint,
                        frames_seen,
                        events_applied,
                        idle_secs = idle.as_secs(),
                        "KV-event listener heartbeat"
                    );
                }
                continue;
            }
            recv = sub.recv_multipart() => {
                let msg = recv?;
                frames_seen += 1;
                let (seq, payload) = match parse_seq_payload(&msg) {
                    Some(p) => p,
                    None => {
                        tracing::warn!(
                            worker_id = worker_id_str,
                            frames = msg.len(),
                            "live SUB: unexpected frame shape; dropping"
                        );
                        continue;
                    }
                };
                // A well-formed frame is the only proof the event stream is
                // real.
                events_applied += 1;
                if !observed_events {
                    observed_events = true;
                    workers.note_kv_events_observed(worker_id_num);
                }
                let evicted = handle_observation(
                    &mut state,
                    seq,
                    payload,
                    replay.as_mut(),
                    replay_wall_budget,
                    &indexer,
                    &prom,
                    &inflight_prefixes,
                    endpoint_key,
                    worker_id_num,
                    &worker_id_str,
                    &eviction_tx,
                )
                .await;
                if evicted {
                    // Block-size mismatch: eviction sent, so exit.
                    return Ok(());
                }
            }
        }
    }
}

/// Apply a single live SUB frame, recovering any sequence gap first. Mirrors
/// dynamo's `ListenerLoop::handle_message` / `handle_gap`.
#[allow(clippy::too_many_arguments)]
async fn handle_observation(
    state: &mut ListenerState,
    seq: u64,
    payload: &[u8],
    replay: Option<&mut ZmqSocket>,
    replay_wall_budget: Duration,
    indexer: &KvIndexer,
    prom: &PromMetrics,
    inflight_prefixes: &InflightPrefixes,
    endpoint_key: &str,
    worker_id_num: u64,
    worker_id_str: &str,
    eviction_tx: &EvictionTx,
) -> bool {
    match state.cursor.observe(seq) {
        CursorObservation::Initial { got } if got > 0 => {
            // First frame, at a non-zero seq: drain `[0, got)` first.
            tracing::info!(
                worker_id = worker_id_str,
                start_seq = 0,
                end_seq = got,
                "initial drain: catching up to live seq"
            );
            if let Some(dealer) = replay {
                replay_gap(
                    0,
                    got,
                    dealer,
                    replay_wall_budget,
                    state,
                    indexer,
                    prom,
                    inflight_prefixes,
                    endpoint_key,
                    worker_id_num,
                    worker_id_str,
                    eviction_tx,
                )
                .await;
            }
        }
        CursorObservation::Gap { expected, got } => {
            tracing::warn!(
                worker_id = worker_id_str,
                expected,
                got,
                gap = got.saturating_sub(expected),
                "gap in event stream; inline replay"
            );
            if let Some(dealer) = replay {
                replay_gap(
                    expected,
                    got,
                    dealer,
                    replay_wall_budget,
                    state,
                    indexer,
                    prom,
                    inflight_prefixes,
                    endpoint_key,
                    worker_id_num,
                    worker_id_str,
                    eviction_tx,
                )
                .await;
            }
        }
        CursorObservation::Stale { got, last_applied } => {
            // Already applied.
            tracing::trace!(
                worker_id = worker_id_str,
                got,
                ?last_applied,
                "stale frame, dropping"
            );
            return false;
        }
        CursorObservation::Initial { .. }
        | CursorObservation::Contiguous { .. }
        | CursorObservation::FreshAfterBarrier { .. } => {
            // Apply normally, advance.
        }
    }

    let evicted = apply_frame(
        &mut state.normalizer,
        indexer,
        prom,
        inflight_prefixes,
        endpoint_key,
        worker_id_num,
        worker_id_str,
        &mut state.block_size_guard,
        eviction_tx,
        seq,
        payload,
    )
    .await;
    if evicted {
        return true;
    }
    state.cursor = state.cursor.advance_to(seq);
    false
}

/// Drain `[start_seq, end_seq)` from the DEALER replay socket, applying each
/// frame.
///
/// `end_seq` must come from a live frame: for an open-ended request the
/// publisher tails forever and never sends the sentinel.
///
/// Ported from dynamo's `ListenerLoop::replay_gap`.
#[allow(clippy::too_many_arguments)]
async fn replay_gap(
    start_seq: u64,
    end_seq: u64,
    dealer: &mut ZmqSocket,
    replay_wall_budget: Duration,
    state: &mut ListenerState,
    indexer: &KvIndexer,
    prom: &PromMetrics,
    inflight_prefixes: &InflightPrefixes,
    endpoint_key: &str,
    worker_id_num: u64,
    worker_id_str: &str,
    eviction_tx: &EvictionTx,
) {
    // Request: [empty_delim, start_seq_be_u64]. The publisher's
    // ROUTER prepends our identity and replies with batches of
    // [identity_or_delim, seq_be_u64, payload]. An empty payload
    // signals end-of-replay.
    let req = [vec![], start_seq.to_be_bytes().to_vec()];
    if let Err(err) = dealer.send_multipart(&req).await {
        tracing::warn!(
            worker_id = worker_id_str,
            start_seq,
            end_seq,
            error = %err,
            "replay request send failed; abandoning this gap"
        );
        return;
    }

    let started = std::time::Instant::now();
    let mut progress = ReplayProgress::new(start_seq, end_seq);
    let mut wall_budget_hit = false;
    let mut sentinel_seen = false;

    loop {
        if started.elapsed() >= replay_wall_budget {
            wall_budget_hit = true;
            break;
        }
        let msg = match dealer
            .recv_multipart_with_timeout(Duration::from_millis(REPLAY_DEALER_RCVTIMEO_MS as u64))
            .await
        {
            Ok(Some(m)) => m,
            Ok(None) => continue, // recv timeout; keep waiting until wall budget
            Err(err) => {
                tracing::warn!(
                    worker_id = worker_id_str,
                    start_seq,
                    end_seq,
                    replayed = progress.replayed,
                    error = %err,
                    "replay recv error; abandoning this gap"
                );
                break;
            }
        };

        // Replay batches are [identity_or_delim, seq_be_u64, payload].
        if msg.len() < 3 {
            tracing::trace!(
                worker_id = worker_id_str,
                frames = msg.len(),
                "replay: unexpected frame shape"
            );
            continue;
        }
        let payload = &msg[2];
        if payload.is_empty() {
            sentinel_seen = true;
            break;
        }
        let Ok(seq_bytes) = <[u8; 8]>::try_from(msg[1].as_slice()) else {
            tracing::trace!(
                worker_id = worker_id_str,
                len = msg[1].len(),
                "replay: malformed seq frame"
            );
            continue;
        };
        let seq = u64::from_be_bytes(seq_bytes);

        // The cursor's `Stale` check guards against duplicate replayed
        // events.
        match state.cursor.observe(seq) {
            CursorObservation::Stale { .. } => {
                continue;
            }
            CursorObservation::Initial { .. }
            | CursorObservation::Contiguous { .. }
            | CursorObservation::FreshAfterBarrier { .. } => {}
            CursorObservation::Gap { .. } => {
                // Ahead of the cursor: accept it; a later observation
                // retries the gap.
            }
        }

        let evicted = apply_frame(
            &mut state.normalizer,
            indexer,
            prom,
            inflight_prefixes,
            endpoint_key,
            worker_id_num,
            worker_id_str,
            &mut state.block_size_guard,
            eviction_tx,
            seq,
            payload,
        )
        .await;
        if evicted {
            // Block-size mismatch: eviction sent, stop replaying.
            return;
        }
        state.cursor = state.cursor.advance_to(seq);
        progress.record(seq);

        // Yield every 32 events, so the replay loop doesn't starve other
        // tasks such as tokenization. 32 was picked empirically.
        if progress.replayed.is_multiple_of(32) {
            tokio::task::yield_now().await;
        }

        // Caught up to `end_seq`: the rest will arrive live.
        if seq + 1 >= end_seq {
            break;
        }
    }

    let elapsed_ms = started.elapsed().as_millis() as u64;
    match progress.failure_reason(wall_budget_hit) {
        Some(reason) => {
            tracing::warn!(
                worker_id = worker_id_str,
                start_seq,
                end_seq,
                first_replayed = ?progress.first_replayed,
                last_replayed = ?progress.last_replayed,
                replayed = progress.replayed,
                reason = reason.as_str(),
                sentinel_seen,
                elapsed_ms,
                "replay incomplete"
            );
        }
        None => {
            tracing::info!(
                worker_id = worker_id_str,
                start_seq,
                end_seq,
                replayed = progress.replayed,
                sentinel_seen,
                elapsed_ms,
                "replay complete"
            );
        }
    }
}

/// Extract `(seq, payload)` from a 3-frame ZMQ message
/// `[topic_or_identity, seq_be_u64, payload]`. Returns `None` if the
/// shape is malformed; the caller logs and drops.
fn parse_seq_payload(msg: &[Vec<u8>]) -> Option<(u64, &[u8])> {
    if msg.len() != 3 {
        return None;
    }
    let seq_bytes = <[u8; 8]>::try_from(msg[1].as_slice()).ok()?;
    Some((u64::from_be_bytes(seq_bytes), msg[2].as_slice()))
}

// --- Bookkeeping ------------------------------------------------------

/// Decode one ZMQ payload and apply each event to the radix tree,
/// [`InflightPrefixes::on_stored`] and the ingest metrics.
///
/// Returns `true` when this listener should stop because a
/// block_size mismatch was detected and eviction has been signaled.
#[allow(clippy::too_many_arguments)]
async fn apply_frame(
    normalizer: &mut ZmqEventNormalizer,
    indexer: &KvIndexer,
    prom: &PromMetrics,
    inflight_prefixes: &InflightPrefixes,
    endpoint_key: &str,
    worker_id_num: u64,
    worker_id_str: &str,
    block_size_guard: &mut BlockSizeGuard,
    eviction_tx: &EvictionTx,
    seq: u64,
    payload: &[u8],
) -> bool {
    let batch = match decode_event_batch(payload) {
        Ok(b) => b,
        Err(err) => {
            tracing::warn!(
                seq,
                error = %err,
                "Failed to decode KvEventBatch"
            );
            return false;
        }
    };

    let effective_dp_rank = batch
        .data_parallel_rank
        .map_or(0u32, |rank| rank.cast_unsigned());

    for raw_event in batch.events {
        if block_size_guard.observe(&raw_event, worker_id_num, worker_id_str, eviction_tx) {
            // Mismatch: eviction sent; the rest of the batch is suspect.
            return true;
        }
        let Some(placement_event) = normalizer.normalize(
            raw_event,
            seq,
            WorkerWithDpRank::new(worker_id_num, effective_dp_rank),
        ) else {
            continue;
        };
        let Some(router_event) = placement_event.into_router_event() else {
            continue;
        };
        log_event(worker_id_str, seq, &router_event);
        let (ev_kind, n_blocks) = match &router_event.event.data {
            KvCacheEventData::Stored(d) => {
                // Notify the pending-prefix tracker before the indexer.
                // Once a dispatch's blocks are all stored it drops out, and
                // the radix tree is the sole signal.
                //
                // Match on `tokens_hash` (dynamo-kv-router's XXH3 over the
                // event's token ids), not `block_hash` (the engine's hash):
                // `InflightPrefixes` is populated from
                // `compute_block_hash_for_seq` in `score_workers`, which uses
                // the same XXH3.
                let stored_hashes: Vec<u64> = d.blocks.iter().map(|b| b.tokens_hash.0).collect();
                inflight_prefixes.on_stored(endpoint_key, &stored_hashes);
                (IngestEvent::Stored, d.blocks.len() as u64)
            }
            KvCacheEventData::Removed(d) => (IngestEvent::Removed, d.block_hashes.len() as u64),
            KvCacheEventData::Cleared => (IngestEvent::Cleared, 0),
        };
        prom.record_index_event(ev_kind, n_blocks);
        indexer.apply_event(router_event).await;
    }
    false
}

/// KV block sizes are small (SGLang page-size 1, vLLM 16, Gemma 64). A
/// `BlockStored` event above this bound is treated as a decode or transport
/// glitch (e.g. a misframed message after a reconnect), dropped and never
/// evicted on.
const MAX_PLAUSIBLE_BLOCK_SIZE: u32 = 1024;

/// How many consecutive main-attention `BlockStored` events must agree on
/// the same (plausible) mismatching block_size before we evict. A real
/// mismatch repeats; a corrupt frame doesn't.
const BLOCK_SIZE_MISMATCH_EVICT_THRESHOLD: u32 = 3;

/// Verdict for one observed block_size, from [`BlockSizeGuard::classify`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockSizeVerdict {
    /// Matches the configured value; the stream is healthy.
    Healthy,
    /// Implausible value: a glitch, dropped without affecting the debounce
    /// state.
    Glitch,
    /// Plausible value that mismatches, but not yet seen enough
    /// consecutive times to be sure it's a real config error.
    Pending { consecutive: u32 },
    /// A sustained mismatch at one value: a genuine misconfiguration. The
    /// caller should evict.
    Evict,
}

/// Per-listener guard against evicting a worker on a spurious block_size.
///
/// `dynamo_kv_router` silently drops mismatched main-attention events, so a
/// sustained mismatch would leave the radix tree empty; eviction makes it
/// loud. A transient corrupt frame (an implausible value, or a plausible one
/// seen once) doesn't evict.
struct BlockSizeGuard {
    configured: u32,
    /// The last plausible, mismatching block_size observed, and how many
    /// consecutive events reported exactly it. Reset whenever an event
    /// matches `configured`, so only a *sustained* mismatch trips.
    last_observed: Option<u32>,
    consecutive: u32,
    /// Whether we've already warned that this worker's KV events are being
    /// dropped (e.g. a hybrid/Mamba model). Later drops log at debug.
    warned_persistent_drop: bool,
}

impl BlockSizeGuard {
    fn new(configured: u32) -> Self {
        Self {
            configured,
            last_observed: None,
            consecutive: 0,
            warned_persistent_drop: false,
        }
    }

    /// Debounce and sanity check over a block_size, with no I/O.
    fn classify(&mut self, engine_block_size: u32) -> BlockSizeVerdict {
        if engine_block_size == self.configured {
            self.last_observed = None;
            self.consecutive = 0;
            return BlockSizeVerdict::Healthy;
        }
        if engine_block_size == 0 || engine_block_size > MAX_PLAUSIBLE_BLOCK_SIZE {
            // A glitch; varied garbage shouldn't accumulate toward eviction.
            return BlockSizeVerdict::Glitch;
        }
        if self.last_observed == Some(engine_block_size) {
            self.consecutive += 1;
        } else {
            self.last_observed = Some(engine_block_size);
            self.consecutive = 1;
        }
        if self.consecutive >= BLOCK_SIZE_MISMATCH_EVICT_THRESHOLD {
            BlockSizeVerdict::Evict
        } else {
            BlockSizeVerdict::Pending {
                consecutive: self.consecutive,
            }
        }
    }

    /// Inspect one raw event. Returns `true` iff a sustained block_size
    /// mismatch has been confirmed and an eviction request was sent.
    fn observe(
        &mut self,
        raw_event: &RawKvEvent,
        worker_id_num: u64,
        worker_id_str: &str,
        eviction_tx: &EvictionTx,
    ) -> bool {
        let RawKvEvent::BlockStored {
            block_size,
            kv_cache_spec_kind,
            ..
        } = raw_event
        else {
            return false;
        };
        // Models with sliding-window or chunked-local layers (Gemma 3,
        // Mistral, some MoEs) also emit BlockStored events for those layers,
        // with smaller blocks. Mirror the normalizer's `kv_cache_spec_kind`
        // filter so they don't look like a mismatch.
        //
        // `None` (untagged) is kept, as for a single-attention model. On a
        // hybrid/Mamba model (e.g. Qwen3-*-Instruct) with a vLLM build that
        // doesn't tag events, the Mamba state blocks (huge block_size, e.g.
        // 1568) also arrive untagged, and are caught only by
        // `MAX_PLAUSIBLE_BLOCK_SIZE`, disabling prefix routing for the worker.
        // The fix is an engine build that emits `kv_cache_spec_kind`; the
        // dynamo-kv-router version is not the problem.
        let is_main_attention = match kv_cache_spec_kind {
            None => true,
            Some(
                KvCacheSpecKind::FullAttention
                | KvCacheSpecKind::MlaAttention
                | KvCacheSpecKind::SinkFullAttention,
            ) => true,
            Some(_) => false,
        };
        if !is_main_attention {
            return false;
        }
        let engine_block_size = *block_size as u32;
        match self.classify(engine_block_size) {
            BlockSizeVerdict::Healthy => false,
            BlockSizeVerdict::Glitch => {
                let configured = self.configured;
                if !self.warned_persistent_drop {
                    // Warn once, then log at debug.
                    self.warned_persistent_drop = true;
                    tracing::warn!(
                        worker_id = worker_id_str,
                        engine_block_size,
                        configured,
                        "KV-cache events report block_size={engine_block_size}, \
                         but this worker is configured for kv_block_size={configured}. \
                         Dropping ALL of this worker's KV events — prefix-cache-aware \
                         routing is DISABLED for it (load-based routing still works). \
                         Likely causes: (1) a KV-event SCHEMA mismatch between the \
                         engine build and kinference's dynamo-kv-router pin — hybrid / \
                         Mamba-SSM models are a known case; (2) a genuinely wrong \
                         block size. Compare the engine's /metrics \
                         `vllm:cache_config_info` block_size against this value. \
                         Logged once per worker; further drops are at DEBUG."
                    );
                } else {
                    tracing::debug!(
                        worker_id = worker_id_str,
                        engine_block_size,
                        configured,
                        "implausible engine block_size on a BlockStored event; \
                         dropping (already warned once for this worker)"
                    );
                }
                false
            }
            BlockSizeVerdict::Pending { consecutive } => {
                tracing::debug!(
                    worker_id = worker_id_str,
                    engine_block_size,
                    configured = self.configured,
                    consecutive,
                    threshold = BLOCK_SIZE_MISMATCH_EVICT_THRESHOLD,
                    "engine block_size mismatch observed; waiting for it to \
                     persist before evicting (could be a transient bad frame)"
                );
                false
            }
            BlockSizeVerdict::Evict => {
                let threshold = BLOCK_SIZE_MISMATCH_EVICT_THRESHOLD;
                let configured = self.configured;
                tracing::warn!(
                    worker_id = worker_id_str,
                    configured_block_size = configured,
                    engine_block_size,
                    threshold,
                    "engine block_size={engine_block_size} differs from \
                     configured kv_block_size={configured} on {threshold} \
                     consecutive events; evicting worker. Either the engine \
                     is mis-configured or the pool's `kv_block_size` needs \
                     to be set to {engine_block_size}.",
                );
                let _ = eviction_tx.send(EvictionRequest {
                    worker_id_num,
                    reason: EvictionReason::BlockSizeMismatch {
                        expected: self.configured,
                        observed: engine_block_size,
                    },
                });
                true
            }
        }
    }
}

fn log_event(
    worker_id_str: &str,
    seq: u64,
    router_event: &dynamo_kv_router::protocols::RouterEvent,
) {
    match &router_event.event.data {
        KvCacheEventData::Stored(data) => {
            let parent_hash = data.parent_hash.map(|h| h.0);
            for (idx, block) in data.blocks.iter().enumerate() {
                tracing::trace!(
                    worker_id = worker_id_str,
                    seq,
                    event = "stored",
                    block_idx = idx,
                    block_hash = block.block_hash.0,
                    tokens_hash = block.tokens_hash.0,
                    parent_hash = ?parent_hash,
                    mm = block.mm_extra_info.is_some(),
                    "block stored"
                );
            }
        }
        KvCacheEventData::Removed(data) => {
            for h in &data.block_hashes {
                tracing::trace!(
                    worker_id = worker_id_str,
                    seq,
                    event = "removed",
                    block_hash = h.0,
                    "block removed"
                );
            }
        }
        KvCacheEventData::Cleared => {
            tracing::trace!(
                worker_id = worker_id_str,
                seq,
                event = "cleared",
                "all blocks cleared"
            );
        }
    }
}

// --- ZMQ socket helpers ---------------------------------------------
//
// Tokio wrappers over `zmq::Socket` (dynamo's are `pub(super)`): register
// the socket's fd with `AsyncFd`, and check `get_events()` for actual
// readiness, since the fd alone isn't reliable.

/// How often the KV-event listener re-checks its own liveness. Cheap; the
/// reporting below is what decides whether anything is said.
const SUB_HEARTBEAT_IVL: Duration = Duration::from_secs(60);
/// How long a listener must have received nothing before it warns, once.
/// KV events fire only on newly stored blocks, so a healthy fleet can be
/// silent, but so is a wrong endpoint.
const SUB_IDLE_WARN_AFTER: Duration = Duration::from_secs(300);
const ZMQ_SUB_RCVTIMEO_MS: i32 = 100;
const ZMQ_RECONNECT_IVL_MS: i32 = 100;
const ZMQ_RECONNECT_IVL_MAX_MS: i32 = 5_000;
const ZMQ_HEARTBEAT_IVL_MS: i32 = 10_000;
const ZMQ_HEARTBEAT_TIMEOUT_MS: i32 = 20_000;
const ZMQ_SUB_RCVHWM: i32 = 100_000;
const ZMQ_DEALER_SNDTIMEO_MS: i32 = 3_000;
const ZMQ_LINGER_MS: i32 = 0;
const TCP_KEEPALIVE_IDLE_SECS: i32 = 60;
const TCP_KEEPALIVE_INTVL_SECS: i32 = 30;
const TCP_KEEPALIVE_CNT: i32 = 3;

/// Tokio-driven wrapper around `zmq::Socket`.
struct ZmqSocket {
    inner: AsyncFd<SocketWrapper>,
}

/// The socket plus its raw fd (owned by the socket), for `AsyncFd`.
struct SocketWrapper {
    socket: zmq::Socket,
    fd: RawFd,
}

impl AsRawFd for SocketWrapper {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl ZmqSocket {
    fn sub(endpoint: &str, worker_id: &str) -> Result<Self> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::SUB).context("creating SUB socket")?;
        socket
            .set_subscribe(b"")
            .context("SUB set_subscribe(empty)")?;
        socket
            .set_rcvtimeo(ZMQ_SUB_RCVTIMEO_MS)
            .context("SUB set_rcvtimeo")?;
        socket
            .set_rcvhwm(ZMQ_SUB_RCVHWM)
            .context("SUB set_rcvhwm")?;
        apply_socket_robustness(&socket, worker_id, "SUB");
        socket.connect(endpoint).context("SUB connect")?;
        Self::wrap(socket)
    }

    fn dealer(endpoint: &str, worker_id: &str) -> Result<Self> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::DEALER).context("creating DEALER socket")?;
        socket
            .set_sndtimeo(ZMQ_DEALER_SNDTIMEO_MS)
            .context("DEALER set_sndtimeo")?;
        socket
            .set_rcvtimeo(REPLAY_DEALER_RCVTIMEO_MS)
            .context("DEALER set_rcvtimeo")?;
        socket
            .set_linger(ZMQ_LINGER_MS)
            .context("DEALER set_linger")?;
        apply_socket_robustness(&socket, worker_id, "DEALER");
        socket.connect(endpoint).context("DEALER connect")?;
        Self::wrap(socket)
    }

    fn wrap(socket: zmq::Socket) -> Result<Self> {
        let fd = socket.get_fd().context("socket get_fd")?;
        let wrapper = SocketWrapper { socket, fd };
        let inner = AsyncFd::new(wrapper).context("AsyncFd::new")?;
        Ok(Self { inner })
    }

    async fn recv_multipart(&mut self) -> Result<Vec<Vec<u8>>> {
        loop {
            // ZMQ's fd is edge-triggered. The traces distinguish three ways
            // a listener can go quiet:
            //
            //   no `fd readable` at all  the fd never signals; the socket may
            //                            not be peered.
            //   readable, no POLLIN      woken with nothing queued: no
            //                            publisher, or no subscription.
            //   POLLIN then EAGAIN       the edge-trigger race.
            let mut guard = self
                .inner
                .readable_mut()
                .await
                .context("AsyncFd readable")?;
            tracing::trace!("zmq: fd readable");
            // Not actually ready: clear the readiness and wait again.
            let events = guard
                .get_inner_mut()
                .socket
                .get_events()
                .context("zmq get_events")?;
            if !events.contains(zmq::POLLIN) {
                tracing::trace!(events = events.bits(), "zmq: readable but no POLLIN");
                guard.clear_ready();
                continue;
            }
            match guard.get_inner_mut().socket.recv_multipart(zmq::DONTWAIT) {
                Ok(msg) => {
                    tracing::trace!(frames = msg.len(), "zmq: recv ok");
                    return Ok(msg);
                }
                Err(zmq::Error::EAGAIN) => {
                    tracing::trace!("zmq: POLLIN set but recv returned EAGAIN");
                    guard.clear_ready();
                    continue;
                }
                Err(err) => return Err(anyhow!("recv_multipart: {err}")),
            }
        }
    }

    /// Like [`Self::recv_multipart`] but returns `Ok(None)` if the
    /// `timeout` elapses with no frame available. Cancellation-safe.
    async fn recv_multipart_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<Vec<Vec<u8>>>> {
        match tokio::time::timeout(timeout, self.recv_multipart()).await {
            Ok(Ok(msg)) => Ok(Some(msg)),
            Ok(Err(err)) => Err(err),
            Err(_) => Ok(None),
        }
    }

    async fn send_multipart(&mut self, frames: &[Vec<u8>]) -> Result<()> {
        if frames.is_empty() {
            return Ok(());
        }
        let last = frames.len() - 1;
        for (i, frame) in frames.iter().enumerate() {
            let flags = if i == last {
                zmq::DONTWAIT
            } else {
                zmq::SNDMORE | zmq::DONTWAIT
            };
            loop {
                let mut guard = self
                    .inner
                    .writable_mut()
                    .await
                    .context("AsyncFd writable")?;
                let events = guard
                    .get_inner_mut()
                    .socket
                    .get_events()
                    .context("zmq get_events")?;
                if !events.contains(zmq::POLLOUT) {
                    guard.clear_ready();
                    continue;
                }
                match guard.get_inner_mut().socket.send(&frame[..], flags) {
                    Ok(()) => break,
                    Err(zmq::Error::EAGAIN) => {
                        guard.clear_ready();
                        continue;
                    }
                    Err(err) => return Err(anyhow!("send: {err}")),
                }
            }
        }
        Ok(())
    }
}

/// Socket robustness for streams over an L4 load balancer (e.g. AWS NLB):
///
/// 1. OS TCP keepalive, against half-open connections: probes after 60s
///    idle, then every 30s, dropping after 3. Within NLB's 350s idle timeout.
/// 2. ZMTP 3.1 heartbeats (libzmq 4.2.0+ on both peers): an unresponsive
///    peer's connection is rebuilt.
/// 3. Reconnect backoff capped at 5s.
///
/// Option errors are not fatal. Must be called before `connect()`.
fn apply_socket_robustness(socket: &zmq::Socket, worker_id: &str, kind: &str) {
    let _ = socket.set_tcp_keepalive(1);
    let _ = socket.set_tcp_keepalive_idle(TCP_KEEPALIVE_IDLE_SECS);
    let _ = socket.set_tcp_keepalive_intvl(TCP_KEEPALIVE_INTVL_SECS);
    let _ = socket.set_tcp_keepalive_cnt(TCP_KEEPALIVE_CNT);
    let _ = socket.set_heartbeat_ivl(ZMQ_HEARTBEAT_IVL_MS);
    let _ = socket.set_heartbeat_timeout(ZMQ_HEARTBEAT_TIMEOUT_MS);
    let _ = socket.set_reconnect_ivl(ZMQ_RECONNECT_IVL_MS);
    if let Err(err) = socket.set_reconnect_ivl_max(ZMQ_RECONNECT_IVL_MAX_MS) {
        tracing::warn!(worker_id, socket = kind, error = %err, "set_reconnect_ivl_max failed");
    }
}

// Sockets close when the listener task ends. `ZMQ_LINGER_MS=0` on DEALER
// means closing doesn't block on pending sends.

#[cfg(test)]
mod tests;
