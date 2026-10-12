//! Per-endpoint shadow counter of picks that haven't yet been
//! observed as first-token (TTFT). Shared by both the gRPC and
//! pick path via `KinferenceService`.
//!
//! Why this exists: scraped per-worker metrics in `MetricsRegistry`
//! lag by up to 1s + scrape latency. During request bursts the
//! picker can make 3+ picks before a scrape catches up to even the
//! first one. With multiple workers tied at the top of predicted-wait
//! (very common), uniform-random pool selection produces statistical
//! streaks that pile all picks on the same worker. This registry
//! tracks our own contribution to per-worker queue depth so the
//! picker can blend it with scraped metrics and break those streaks.
//!
//! Keyed by `endpoint` (the `host:port` string) — that identifier is
//! already on every `WorkerScore` so no plumbing of numeric worker
//! IDs through the picker is needed. Pod-IP churn across worker
//! restarts naturally orphans old entries: a count for a dead
//! endpoint just costs an unused map slot, and new endpoints start
//! clean.

use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;

#[derive(Default)]
pub struct InflightRegistry {
    counts: DashMap<String, AtomicU32>,
}

impl InflightRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bump the in-flight count for an endpoint. Called immediately
    /// after a successful pick in `decide()`.
    pub fn inc(&self, endpoint: &str) {
        // `entry().or_insert_with(...)` ensures the AtomicU32 exists,
        // then we bump it. DashMap's per-shard locking keeps this
        // O(1) and contention-free across distinct endpoints.
        self.counts
            .entry(endpoint.to_string())
            .or_insert_with(|| AtomicU32::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the in-flight count for an endpoint, saturating at
    /// zero (no underflow). Called at first ResponseBody (TTFT
    /// observation) or at stream EOF as a fallback when the request
    /// never produced a first token.
    pub fn dec(&self, endpoint: &str) {
        if let Some(entry) = self.counts.get(endpoint) {
            // `compare_exchange` loop to saturate at 0. Avoids the
            // race where `fetch_sub` would wrap from 0 → u32::MAX.
            let atom = entry.value();
            loop {
                let current = atom.load(Ordering::Relaxed);
                if current == 0 {
                    return;
                }
                if atom
                    .compare_exchange_weak(
                        current,
                        current - 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return;
                }
            }
        }
    }

    /// Current in-flight count for an endpoint. Returns 0 if the
    /// endpoint has never been seen (no entry in the map).
    pub fn get(&self, endpoint: &str) -> u32 {
        self.counts
            .get(endpoint)
            .map(|e| e.value().load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests;
