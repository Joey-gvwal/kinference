//! Per-endpoint tracker of dispatched-but-not-yet-stored prompt
//! prefixes — the shadow we maintain so the picker can credit
//! workers for blocks that ARE about to be in their KV cache, before
//! `BlockStored` events confirm it.
//!
//! Why this exists, in one breath: the radix tree only reflects what
//! the publisher has emitted as `BlockStored`. In a prefix-sticky
//! workload, follow-up requests arrive while the prior turn is still
//! tokenized → queued → mid-prefill, so the radix tree under-reports
//! the prefix-overlap for the worker we just routed to. By tracking
//! every dispatch we make, and consuming `BlockStored` events as
//! they land, we can compute an "expected overlap at run time" that
//! includes inflight blocks ahead of us — see
//! `picker::compute_layered_overlap` for the model.
//!
//! Sibling to `InflightRegistry`: same DashMap-per-endpoint shape,
//! same lifecycle hooks (insert at pick under `pick_lock`, forget at
//! TTFT/EOF). The two share the same endpoint key (the worker's
//! `score_addr`). `InflightRegistry` is just a count; this struct
//! adds per-dispatch block-hash data + uncached-blocks workload.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// One outstanding dispatch — a prompt we've routed to `endpoint`
/// whose `BlockStored` events haven't fully landed yet. `remaining`
/// shrinks as matching `on_stored` calls arrive; when it empties,
/// the dispatch is dropped entirely.
///
/// `uncached_at_dispatch` captures the prefill workload this prompt
/// imposed on the worker at the moment we picked: it's
/// `total_blocks − expected_overlap_at_pick`, the number of blocks
/// the engine still had to prefill. Stashed here (rather than
/// derived) because by the time a future score-time query needs it,
/// the radix tree has moved on and we can't reconstruct it.
///
/// `total_at_dispatch` is the full prompt's block count. Needed by
/// the quadratic attention-cost model (`prefill_ms = a × n_uncached +
/// c × n_uncached × n_total`) — uncached blocks alone don't
/// describe the prefill cost when prompt lengths vary.
#[derive(Debug, Clone)]
struct InflightDispatch {
    endpoint: String,
    remaining: HashSet<u64>,
    dispatched_at: Instant,
    uncached_at_dispatch: u32,
    total_at_dispatch: u32,
}

/// Read-only point-in-time view of one outstanding dispatch, surfaced
/// to the picker for the position-discount queue-cost model.
/// `dispatched_at` lets the picker estimate work-remaining; the
/// caller doesn't need the per-dispatch hash set for queue math.
#[derive(Debug, Clone, Copy)]
pub struct InflightSummary {
    pub dispatched_at: Instant,
    pub uncached_at_dispatch: u32,
    pub total_at_dispatch: u32,
}

/// Full per-dispatch snapshot — block-hash set plus timing/workload
/// metadata. Returned by `dispatches_for` so the scoring core can
/// compute (a) prefix-overlap against the new query's hashes and
/// (b) per-dispatch progress for cache-credit/queue-cost models
/// from a single consistent read of the DashMap. Doing both from one
/// call avoids a race where a BlockStored drain between two separate
/// reads would shorten the second view's list.
#[derive(Debug, Clone)]
pub struct DispatchSnapshot {
    pub remaining: HashSet<u64>,
    pub dispatched_at: Instant,
    pub uncached_at_dispatch: u32,
    pub total_at_dispatch: u32,
}

impl DispatchSnapshot {
    /// Project to the cheaper `InflightSummary` view for callers that
    /// don't need the hash set (e.g. the queue-cost math).
    pub fn summary(&self) -> InflightSummary {
        InflightSummary {
            dispatched_at: self.dispatched_at,
            uncached_at_dispatch: self.uncached_at_dispatch,
            total_at_dispatch: self.total_at_dispatch,
        }
    }
}

#[derive(Default)]
pub struct InflightPrefixes {
    /// Keyed by `request_id` so `forget(id)` is O(1). Order across
    /// dispatches on the same endpoint is recovered via
    /// `dispatched_at` at query time (pending counts are small, so
    /// the sort is cheap).
    dispatches: DashMap<u64, InflightDispatch>,
}

impl InflightPrefixes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Single source of truth for per-dispatch picker state on
    /// `endpoint`, oldest-first by `dispatched_at`. The caller
    /// projects to whichever view it needs:
    /// - intersect `remaining` against query hashes for layered
    ///   expected-overlap
    /// - feed `(dispatched_at, uncached_at_dispatch)` (via
    ///   `DispatchSnapshot::summary`) into the queue-cost math
    /// - derive per-dispatch progress for the cache-credit refinement
    ///
    /// Reading once and projecting both views guarantees a consistent
    /// snapshot — separate `dispatches_for` + `workload_for` calls
    /// could disagree if a `BlockStored` event drained an entry
    /// between them.
    pub fn dispatches_for(&self, endpoint: &str) -> Vec<DispatchSnapshot> {
        let mut matching: Vec<DispatchSnapshot> = self
            .dispatches
            .iter()
            .filter(|e| e.value().endpoint == endpoint)
            .map(|e| DispatchSnapshot {
                remaining: e.value().remaining.clone(),
                dispatched_at: e.value().dispatched_at,
                uncached_at_dispatch: e.value().uncached_at_dispatch,
                total_at_dispatch: e.value().total_at_dispatch,
            })
            .collect();
        matching.sort_by_key(|d| d.dispatched_at);
        matching
    }

    /// Record a dispatch. Called from `decide()` after pick, inside
    /// the `pick_lock` critical section so the next concurrent pick
    /// sees the entry. `uncached_at_dispatch` is the prefill workload
    /// (in blocks) that this prompt brought to the worker — read at
    /// scoring time by `workload_for()` to estimate queue cost ahead
    /// of subsequent picks.
    pub fn insert(
        &self,
        request_id: u64,
        endpoint: String,
        hashes: HashSet<u64>,
        uncached_at_dispatch: u32,
        total_at_dispatch: u32,
    ) {
        self.dispatches.insert(
            request_id,
            InflightDispatch {
                endpoint,
                remaining: hashes,
                dispatched_at: Instant::now(),
                uncached_at_dispatch,
                total_at_dispatch,
            },
        );
    }

    /// On every `BlockStored` event for `endpoint`, walk all
    /// dispatches and remove the stored hashes from each `remaining`
    /// set. Drained dispatches drop out of the map.
    pub fn on_stored(&self, endpoint: &str, stored: &[u64]) {
        if stored.is_empty() {
            return;
        }
        let mut drained: Vec<u64> = Vec::new();
        for mut entry in self.dispatches.iter_mut() {
            if entry.value().endpoint != endpoint {
                continue;
            }
            let remaining = &mut entry.value_mut().remaining;
            for h in stored {
                remaining.remove(h);
            }
            if remaining.is_empty() {
                drained.push(*entry.key());
            }
        }
        // Two-phase to avoid mutating-while-iterating.
        for id in drained {
            self.dispatches.remove(&id);
        }
    }

    /// Drop the dispatch for `request_id`. Idempotent. Called at TTFT
    /// observation and at stream EOF — mirrors `InflightRegistry::dec`.
    pub fn forget(&self, request_id: u64) {
        self.dispatches.remove(&request_id);
    }

    /// Drop dispatches older than `max_age`. Backstop against lifecycle
    /// leaks (lost `BlockStored`, stream dropped without observation).
    pub fn sweep(&self, max_age: Duration) {
        let now = Instant::now();
        let stale: Vec<u64> = self
            .dispatches
            .iter()
            .filter(|e| now.duration_since(e.value().dispatched_at) > max_age)
            .map(|e| *e.key())
            .collect();
        for id in stale {
            self.dispatches.remove(&id);
        }
    }

    /// Current pending dispatch count across all endpoints. Diagnostic
    /// only — useful for dashboards / smoke verification that the
    /// lifecycle is wired correctly.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.dispatches.len()
    }
}

#[cfg(test)]
mod tests;
