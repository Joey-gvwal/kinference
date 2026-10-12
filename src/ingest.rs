//! Per-worker ZMQ ingest listener for vLLM KV-cache events.
//!
//! One [`spawn_worker_listener`] task per configured worker. Each task
//! owns its sockets, parses vLLM's wire-format events through
//! `dynamo_kv_router::zmq_wire`, and feeds them into the shared
//! [`KvIndexer`] (radix tree) plus our local bookkeeping
//! ([`InflightPrefixes`], [`Telemetry`], [`WorkerRegistry`]) inside a
//! single [`apply_frame`] call.
//!
//! ## Design — drain-inline gap handling
//!
//! Modelled after dynamo's [`standalone_indexer`] listener: one tokio
//! task per worker; the SUB socket and DEALER replay socket live in
//! the same task; **gap recovery is synchronous and inline**. When a
//! live SUB frame arrives with `seq > expected`, the loop sends a
//! replay request on the DEALER socket, drains the response, applies
//! every replayed event in order, and then applies the live frame
//! that triggered the detection.
//!
//! This is structurally immune to the "drain-to-live handover gap"
//! we kept hitting with the previous separate-recv-thread design:
//! gap detection is only ever triggered by a live frame already in
//! hand, so there is no state where the listener silently waits
//! forever for a seq the publisher has already passed.
//!
//! Sequence-cursor semantics are owned by
//! [`dynamo_kv_router::recovery::CursorState`] (`Initial`,
//! `Live(u64)`, `InvalidatedByBarrier(Option<u64>)`).
//!
//! [`standalone_indexer`]: dynamo's
//! `lib/kv-router/src/standalone_indexer/listener.rs` —
//! we reuse `CursorState` from there and port the control-flow
//! pattern, but reimplement the ZMQ socket helpers here because
//! dynamo keeps theirs `pub(super)`.
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

use crate::config::WorkerConfig;
use crate::eviction::{EvictionReason, EvictionRequest, EvictionTx};
use crate::inflight_prefixes::InflightPrefixes;
use crate::telemetry::{IngestEvent, Telemetry};
use crate::workers::WorkerRegistry;

/// Per-recv DEALER timeout. Keeps the inner loop responsive to
/// cancellation and lets us measure inter-event delay during a
/// replay.
const REPLAY_DEALER_RCVTIMEO_MS: i32 = 500;

/// Spawn the ingest listener for a single worker. `endpoint_key` is
/// the worker's `score_addr` (resolved by `WorkerEntry::from_config`)
/// — the key into [`InflightPrefixes`] so the on-store hook can find
/// the matching dispatches. `eviction_tx` lets this listener signal
/// the main coordinator to evict the worker if its engine reports a
/// block_size that doesn't match the fleet's configured value.
#[allow(clippy::too_many_arguments)]
pub fn spawn_worker_listener(
    cfg: WorkerConfig,
    block_size: u32,
    replay_timeout_ms: u64,
    endpoint_key: String,
    indexer: Arc<KvIndexer>,
    telemetry: Arc<Telemetry>,
    inflight_prefixes: Arc<InflightPrefixes>,
    workers: Arc<WorkerRegistry>,
    eviction_tx: EvictionTx,
    cancel: CancellationToken,
) {
    // Temporary diagnostic switch: set `KINFERENCE_DISABLE_INGEST=1`
    // to skip ZMQ ingest entirely. Workers are still marked ready
    // (so picks work) but no SUB/DEALER sockets open, no events
    // applied, no radix-tree state. Useful for isolating whether
    // ingest activity contributes to runtime contention with the
    // tokenize HTTP path. Remove the env var to restore normal
    // ingest behavior — no recompile needed.
    if std::env::var("KINFERENCE_DISABLE_INGEST").is_ok() {
        tracing::warn!(
            worker_id = cfg.worker_id_str,
            "KINFERENCE_DISABLE_INGEST set — skipping ZMQ ingest. \
             Marking worker ready immediately; radix tree will stay empty \
             and cache-affinity routing will be no-op for this run."
        );
        workers.set_initial_sync_complete(cfg.worker_id_num);
        // Drop the indexer / telemetry / inflight_prefixes Arc clones
        // explicitly so it's clear this branch holds no references.
        let _ = (
            indexer,
            telemetry,
            inflight_prefixes,
            endpoint_key,
            block_size,
            cancel,
        );
        return;
    }

    // No publish socket declared. In Kubernetes mode that means the
    // `InferenceWorkerSet` omitted `spec.kvEvents`, which is a DECLARATION —
    // these engines don't publish — and honoring it is the only correct
    // response. Connecting to a default port instead would look healthy
    // forever: ZMQ `connect` always succeeds, so the SUB socket would sit
    // silent, `initial_sync_complete` would never fire, and the worker would
    // stay un-ready with nothing to point at.
    //
    // So mark the worker ready and route to it on load alone. Degraded, not
    // dead: queue depth and KV usage are still scraped, only the prefix signal
    // is absent.
    if cfg.zmq_endpoint.as_deref().unwrap_or("").is_empty() {
        tracing::info!(
            worker_id = cfg.worker_id_str,
            "no KV-event socket declared for this worker; routing on load metrics \
             alone (no prefix-cache signal from it)"
        );
        workers.set_initial_sync_complete(cfg.worker_id_num);
        let _ = (
            indexer,
            telemetry,
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
                telemetry.clone(),
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

/// Per-listener mutable state. Holds the cursor (for in-order
/// gap-detection) and the event normalizer built once at startup
/// from the fleet-wide `Config.kv_block_size`. If the engine ever
/// reports a different block_size, the worker is evicted (we never
/// rebuild the normalizer mid-flight).
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
    /// arrived. Not in dynamo's enum — we add it because we keep a
    /// safety timeout that dynamo doesn't.
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

/// Per-replay-call tracker for log diagnostics. Ported from dynamo's
/// `ReplayRecoveryProgress`. Mutated inside `replay_gap` after each
/// frame is applied; consulted at exit to decide whether to emit the
/// "replay incomplete" WARN with a structured reason.
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

/// Run a single listener until it errors or `cancel` fires. The body
/// is a `tokio::select!` over (cancel, SUB recv). All gap recovery
/// happens *inside* the SUB-recv branch, synchronously, before the
/// triggering frame is applied. There is no separate recv thread,
/// no pending-frame buffer, no mid-stream replay channel — the
/// previous design's recurring bug class lived in those.
#[allow(clippy::too_many_arguments)]
async fn run_listener(
    cfg: &WorkerConfig,
    block_size: u32,
    replay_wall_budget: Duration,
    endpoint_key: &str,
    indexer: Arc<KvIndexer>,
    telemetry: Arc<Telemetry>,
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
    // Replay is OFF by default. Set `KINFERENCE_ENABLE_REPLAY=1` to
    // turn it on. When off, live SUB ingest still flows (new block-
    // stored events continue to populate the radix tree), but
    // historical events are never fetched and gaps are not recovered.
    // The default-off stance reflects two observations: (a) the
    // DEALER replay tight-loop contends with the tokenize HTTP path
    // and degrades tail latency, and (b) the radix tree picks up
    // stale state when the engine's KV cache has been cleared without
    // a process restart, leading to hallucinated cache hits in the
    // picker. Live SUB alone gives consistent state for the duration
    // of the process. Unlike `KINFERENCE_DISABLE_INGEST`, this knob
    // never affects live ingest.
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
    // Whether this listener has already reported that events are flowing.
    // Local, so the registry write below happens once.
    let mut observed_events = false;
    // Liveness accounting for the heartbeat below. `frames_seen` counts every
    // message that reached the socket, including ones we dropped for shape —
    // it answers "is anything on the wire", which is a different question from
    // "did we apply an event".
    let mut frames_seen: u64 = 0;
    let mut events_applied: u64 = 0;
    let listener_started = Instant::now();
    let mut warned_idle = false;
    let mut heartbeat = tokio::time::interval(SUB_HEARTBEAT_IVL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // First tick fires immediately; burn it so the first report is one interval
    // in rather than at t=0 when silence means nothing yet.
    heartbeat.tick().await;

    // Mark ready as soon as the sockets are connected — matching
    // dynamo's `standalone_indexer::listener` pattern. The radix
    // tree fills in lazily: when the first live SUB frame arrives,
    // `handle_observation` sees `CursorObservation::Initial { got
    // > 0 }` and fires a bounded `replay_gap(0, got)` to pull the
    // history. Bounded requests get a publisher-sent sentinel and
    // terminate cleanly; open-ended requests (the previous
    // proactive-drain attempt) never get a sentinel because the
    // publisher tails forever.
    //
    // Cold-start window: if the cluster is idle when kinference
    // starts, the radix tree stays empty until the first event
    // arrives. That's acceptable — no traffic means no routing
    // decisions to get wrong.
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
            // A connected-but-idle SUB is indistinguishable from a
            // correctly-wired one: ZMQ `connect` succeeds with no peer, and
            // `listener ready` prints either way. Without this, "the engine
            // publishes nothing" and "we are subscribed to the wrong address"
            // both present as an absence of log lines, forever. Report the
            // silence positively so it can be acted on.
            _ = heartbeat.tick() => {
                let idle = listener_started.elapsed();
                // ONCE, then never again at this level. The signal is "this
                // listener has never received anything", which is worth saying
                // exactly one time — repeating it every minute turns a real
                // diagnostic into background noise an operator learns to
                // filter, and a healthy fleet serving cached prefixes trips it
                // legitimately.
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
                // A decoded frame is the only proof the event stream is real —
                // the SUB socket reaches steady state whether or not anything
                // is publishing. Gated on a local flag so this costs one
                // registry write per listener, not one per event.
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
                    &telemetry,
                    &inflight_prefixes,
                    endpoint_key,
                    worker_id_num,
                    &worker_id_str,
                    &eviction_tx,
                )
                .await;
                if evicted {
                    // Block-size mismatch — eviction sent. Exit
                    // cleanly; the coordinator will tear down our
                    // task via the per-worker cancellation token.
                    return Ok(());
                }
            }
        }
    }
}

/// Apply a single live SUB frame, recovering any sequence gap
/// synchronously beforehand. Mirrors dynamo's
/// `ListenerLoop::handle_message` / `handle_gap` pattern in
/// `standalone_indexer/listener.rs`.
#[allow(clippy::too_many_arguments)]
async fn handle_observation(
    state: &mut ListenerState,
    seq: u64,
    payload: &[u8],
    replay: Option<&mut ZmqSocket>,
    replay_wall_budget: Duration,
    indexer: &KvIndexer,
    telemetry: &Telemetry,
    inflight_prefixes: &InflightPrefixes,
    endpoint_key: &str,
    worker_id_num: u64,
    worker_id_str: &str,
    eviction_tx: &EvictionTx,
) -> bool {
    match state.cursor.observe(seq) {
        CursorObservation::Initial { got } if got > 0 => {
            // First frame we've seen but the publisher's already at a
            // non-zero seq. Drain `[0, got)` inline so the radix tree
            // is contiguous with the live stream before we apply the
            // in-hand frame.
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
                    telemetry,
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
                    telemetry,
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
            // Already applied this or earlier. Don't reapply, don't
            // advance the cursor.
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
        telemetry,
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

/// Drain `[start_seq, end_seq)` from the DEALER replay socket,
/// applying each replayed frame to the radix tree and our
/// bookkeeping.
///
/// Bounded requests only — `end_seq` must come from a known live-
/// SUB-frame seq (see `handle_observation`). Open-ended requests
/// (`end_seq = u64::MAX`) make the publisher tail forever; the
/// sentinel never arrives and we hit the safety budget.
///
/// On exit, `ReplayProgress::failure_reason` decides whether to
/// emit a structured WARN identifying why replay didn't deliver
/// everything requested. Ported from dynamo's standalone_indexer
/// listener (`ListenerLoop::replay_gap` +
/// `ReplayRecoveryProgress::warn_if_incomplete`).
#[allow(clippy::too_many_arguments)]
async fn replay_gap(
    start_seq: u64,
    end_seq: u64,
    dealer: &mut ZmqSocket,
    replay_wall_budget: Duration,
    state: &mut ListenerState,
    indexer: &KvIndexer,
    telemetry: &Telemetry,
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
            Ok(None) => continue, // recv timeout — keep waiting until wall budget
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

        // Replayed events can arrive in interleaved or duplicate
        // order; the cursor's `Stale` check is what protects the
        // radix tree from double-applies. We still apply if the
        // cursor accepts the frame; otherwise skip.
        match state.cursor.observe(seq) {
            CursorObservation::Stale { .. } => {
                continue;
            }
            CursorObservation::Initial { .. }
            | CursorObservation::Contiguous { .. }
            | CursorObservation::FreshAfterBarrier { .. } => {}
            CursorObservation::Gap { .. } => {
                // Replayed frame is itself ahead of cursor — accept
                // it and advance; the gap will be reattempted by the
                // next observation if needed.
            }
        }

        let evicted = apply_frame(
            &mut state.normalizer,
            indexer,
            telemetry,
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
            // Block-size mismatch — eviction sent, stop replaying.
            return;
        }
        state.cursor = state.cursor.advance_to(seq);
        progress.record(seq);

        // Cooperative yield every 32 applied events. Replay processes
        // hundreds of events per second in a tight loop; without
        // yielding, the tokio worker running this task is held the
        // entire time, starving other async work (notably the
        // tokenize HTTP client's response-body parse). Yield costs
        // ~one queue push and lets the runtime service one HTTP
        // task between event batches. 32 was picked empirically:
        // small enough to keep tokenize-tail responsive under burst,
        // large enough that yield overhead is a rounding error.
        if progress.replayed.is_multiple_of(32) {
            tokio::task::yield_now().await;
        }

        // Optimization: if the cursor has caught up to (or passed)
        // the requested end_seq, we can stop early — the publisher
        // will continue streaming to its sentinel but we don't need
        // those frames (they'll arrive live).
        if seq + 1 >= end_seq {
            break;
        }
    }

    // Decide what to emit. Three cases:
    //   1. failure_reason() returns Some(_) → emit WARN with
    //      structured fields (dynamo's pattern).
    //   2. wall_budget_hit && failure_reason returns
    //      WallBudgetExhausted → already in case 1.
    //   3. Clean completion (sentinel + everything contiguous +
    //      reached end) → emit INFO with the replayed count.
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

// --- Bookkeeping (preserved from previous design) -------------------

/// Decode one ZMQ payload into events and apply each to the radix
/// tree + our local bookkeeping. Side effects:
/// [`InflightPrefixes::on_stored`] (drives layered-overlap math +
/// tokenize-divergence sniff), [`Telemetry::record_event`] (ingest
/// counters), and `indexer.apply_event` (radix-tree state).
///
/// Returns `true` when this listener should stop because a
/// block_size mismatch was detected and eviction has been signaled.
#[allow(clippy::too_many_arguments)]
async fn apply_frame(
    normalizer: &mut ZmqEventNormalizer,
    indexer: &KvIndexer,
    telemetry: &Telemetry,
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
            // Mismatch detected; eviction signal sent. Stop
            // processing further events from this batch — they're
            // suspect under a wrong block_size and the coordinator
            // will tear down the listener shortly.
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
                // Notify the pending-prefix tracker BEFORE we hand
                // the event off to the indexer. Each stored hash is
                // removed from any matching outstanding dispatches
                // for this worker — once an `on_stored` drains a
                // dispatch's `remaining` set, the dispatch drops out
                // and the radix tree becomes the sole signal.
                //
                // We MUST match on `tokens_hash` (dynamo-kv-router's
                // own XXH3 recomputation over the event's token_ids
                // field), NOT `block_hash` (the engine's external
                // hash). `InflightPrefixes` is populated from
                // `compute_block_hash_for_seq` in `score_workers`
                // ([src/grpc.rs]), so both sides need to be using
                // XXH3 over the same token bytes for the match to
                // ever land.
                let stored_hashes: Vec<u64> = d.blocks.iter().map(|b| b.tokens_hash.0).collect();
                inflight_prefixes.on_stored(endpoint_key, &stored_hashes);
                (IngestEvent::Stored, d.blocks.len() as u64)
            }
            KvCacheEventData::Removed(d) => (IngestEvent::Removed, d.block_hashes.len() as u64),
            KvCacheEventData::Cleared => (IngestEvent::Cleared, 0),
        };
        telemetry.record_event(ev_kind, n_blocks);
        indexer.apply_event(router_event).await;
    }
    false
}

/// KV block sizes are small (SGLang page-size 1, vLLM 16, Gemma 64,
/// …). A `BlockStored` event reporting a value above this bound is a
/// decode/transport glitch — a misframed ZMQ message, e.g. after PUB/SUB
/// carried over an L4 load balancer dropped and re-synced the connection
/// mid-stream — NOT a real engine block_size. Such events are dropped,
/// never evicted on.
const MAX_PLAUSIBLE_BLOCK_SIZE: u32 = 1024;

/// How many consecutive main-attention `BlockStored` events must agree on
/// the SAME (plausible) mismatching block_size before we evict. A real
/// config mismatch reports the same wrong value on every event; a one-off
/// corrupt frame doesn't repeat. This debounces transient ZMQ corruption
/// so a single bad frame can't take a healthy worker out of rotation.
const BLOCK_SIZE_MISMATCH_EVICT_THRESHOLD: u32 = 3;

/// Verdict for one observed block_size, from [`BlockSizeGuard::classify`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockSizeVerdict {
    /// Matches the configured value; the stream is healthy.
    Healthy,
    /// Implausible value — treated as a decode/transport glitch and
    /// dropped without affecting the debounce state.
    Glitch,
    /// Plausible value that mismatches, but not yet seen enough
    /// consecutive times to be sure it's a real config error.
    Pending { consecutive: u32 },
    /// A sustained mismatch at one value — a genuine misconfiguration.
    /// The caller should evict.
    Evict,
}

/// Per-listener guard against evicting a worker on a *spurious*
/// block_size. Holds the debounce state a stateless check can't.
///
/// Homogeneous-pool assumption: every worker in a pool runs the same
/// engine build with the same block_size. A real operator misconfig
/// makes every event report the same wrong value, so it trips the
/// threshold quickly and the operator sees a fail-loud eviction WARN
/// with the value the engines actually use. But a *transient* corrupt
/// frame (implausible value, or a plausible value seen once) no longer
/// evicts — the reason this guard exists.
///
/// Mismatched main-attention events (the kind the radix tree indexes)
/// are otherwise silently dropped by `dynamo_kv_router`; an undetected
/// *sustained* mismatch would leave the worker's radix tree empty and
/// every query against it return 0 matches, so eviction stays the
/// fail-loud response to a real mismatch.
struct BlockSizeGuard {
    configured: u32,
    /// The last plausible, mismatching block_size observed, and how many
    /// consecutive events reported exactly it. Reset whenever an event
    /// matches `configured`, so only a *sustained* mismatch trips.
    last_observed: Option<u32>,
    consecutive: u32,
    /// One-shot latch: have we already surfaced (at WARN) that we're
    /// persistently dropping this worker's KV events? Dropping is
    /// otherwise DEBUG-only, so without this an operator would never
    /// learn a worker's KV indexing is dead (e.g. a hybrid/Mamba model
    /// whose event schema our dynamo-kv-router pin can't decode). We
    /// warn once, then fall back to debug to avoid per-event spam.
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

    /// Pure debounce/sanity core over an already-extracted block_size.
    /// Mutates the debounce counters; performs no I/O so it's directly
    /// unit-testable.
    fn classify(&mut self, engine_block_size: u32) -> BlockSizeVerdict {
        if engine_block_size == self.configured {
            self.last_observed = None;
            self.consecutive = 0;
            return BlockSizeVerdict::Healthy;
        }
        if engine_block_size == 0 || engine_block_size > MAX_PLAUSIBLE_BLOCK_SIZE {
            // Obviously-not-a-block-size → decode/transport glitch. Do
            // NOT advance the debounce; a stream of varied garbage
            // shouldn't accumulate toward eviction.
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
        // Models with interleaved sliding-window / chunked-local layers
        // (Gemma 3, Mistral, some MoEs) emit BlockStored events from BOTH
        // their main-attention layers (larger blocks) AND their sliding-
        // window layers (smaller blocks). The library's normalizer filter
        // drops the non-main events via `kv_cache_spec_kind`; we mirror
        // that filter here, otherwise a sliding-window event with a
        // smaller block_size would look like a mismatch.
        //
        // `None` means the engine did NOT tag the event with a spec kind.
        // For a legacy single-attention model that's genuinely
        // main-attention, so we keep it. BUT on a hybrid / Mamba-SSM model
        // (e.g. Qwen3-*-Instruct) whose vLLM build predates KV-event
        // spec-kind tagging, the Mamba state-layer blocks — which carry a
        // huge block_size (e.g. 1568, the whole SSM "page", NOT a
        // misdecode) — ALSO arrive untagged as `None` and slip through
        // this filter; we can't tell them apart from real attention
        // blocks. They're then caught only by the MAX_PLAUSIBLE_BLOCK_SIZE
        // sanity bound in `classify` below (which disables prefix routing
        // for that worker). The real fix is engine-side: run a vLLM build
        // that emits `kv_cache_spec_kind`, so the Mamba blocks are labeled
        // here (`Some(Mamba) => false`) and dropped, leaving the
        // block_size-16 attention blocks to populate the radix tree.
        // NOTE: bumping dynamo-kv-router does NOT help — it has had full
        // Mamba spec-kind support (`filter::KvCacheSpecKind::Mamba`) since
        // well before our pin; the missing metadata is on the wire.
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
                    // First time: surface loudly (once) so the operator
                    // learns this worker's KV indexing is effectively
                    // dead, rather than it failing silently at DEBUG.
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
// Tokio-async wrappers over `zmq::Socket`. We need this because
// dynamo's equivalent (`standalone_indexer/zmq.rs`) is `pub(super)`
// and not importable. The wrappers are mechanical: register the
// socket's fd with tokio's `AsyncFd`, poll the socket's edge-trigger
// `get_events()` to know when it's actually ready to recv/send (the
// fd alone is a level-trigger and lies), and bail out cleanly on
// timeouts so the caller can interleave cancel/budget checks.

/// How often the KV-event listener re-checks its own liveness. Cheap; the
/// reporting below is what decides whether anything is said.
const SUB_HEARTBEAT_IVL: Duration = Duration::from_secs(60);
/// How long a listener must have received NOTHING before it says so — once.
///
/// An idle fleet legitimately publishes nothing: KV events fire on newly
/// STORED blocks, so a workload whose prefixes are all already cached is
/// silent and healthy. Five minutes of total silence is still worth one line,
/// because it is also exactly what a wrong endpoint looks like.
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

/// Tokio-driven wrapper around `zmq::Socket`. Owns the underlying
/// socket; `AsyncFd` waits on the file descriptor's edge-triggered
/// readiness and we check `get_events()` to decide whether the
/// socket is actually ready.
struct ZmqSocket {
    inner: AsyncFd<SocketWrapper>,
}

/// Holds the raw fd alongside the socket so `AsyncFd` can register
/// it. The fd is owned by the socket; this struct just borrows it
/// for `AsRawFd`.
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
            // ZMQ's FD is EDGE-triggered and this loop has three distinct
            // failure modes that all look identical from outside — a listener
            // that logged `listener ready` and then went quiet forever. The
            // traces below tell them apart:
            //
            //   no `fd readable` at all  the FD never signals. Registration or
            //                            edge-trigger problem; the socket may
            //                            not be peered at all.
            //   readable, no POLLIN      the FD woke us with nothing queued —
            //                            connected but no publisher, or the
            //                            subscription never took.
            //   POLLIN then EAGAIN       the classic edge-trigger race: libzmq
            //                            said readable, then had nothing.
            //
            // All at trace, so this costs nothing until someone is debugging.
            let mut guard = self
                .inner
                .readable_mut()
                .await
                .context("AsyncFd readable")?;
            tracing::trace!("zmq: fd readable");
            // Poll the socket's edge-triggered readiness; if it isn't
            // actually ready, clear the readiness and wait again.
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
    /// `timeout` elapses with no frame available. Cancellation-safe:
    /// callers can interleave with other selects.
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

/// Three layers of socket robustness for a healthy PUB→SUB / DEALER
/// stream over an L4 load balancer (AWS NLB and similar):
///
/// 1. **OS TCP keepalive** — defends against a half-open TCP
///    connection. Probes after 60s idle, then every 30s, drop
///    after 3. Well inside NLB's 350s idle timeout.
/// 2. **ZMTP 3.1 heartbeats** — libzmq exchanges PING/PONG at the
///    protocol layer. A peer that stops responding has its
///    connection torn down and rebuilt automatically. Requires
///    libzmq 4.2.0+ on both peers.
/// 3. **Bounded reconnect backoff** — `ZMQ_RECONNECT_IVL_MAX` caps
///    libzmq's exponential backoff at 5s.
///
/// Errors on individual options are logged but not fatal — the
/// socket still works with whatever subset of robustness features
/// succeeded. Must be called before `connect()`.
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

// Drop ergonomics: the surrounding tokio task ends → AsyncFd drops →
// SocketWrapper drops → zmq::Socket drops → libzmq closes the
// connection. `ZMQ_LINGER_MS=0` on DEALER means we don't block on
// pending sends at close time. The cancellation-token-driven exit
// in `run_listener` ensures we leave the loop before the wrapper is
// dropped, so nothing reaches into a half-closed socket.

#[cfg(test)]
mod tests;
