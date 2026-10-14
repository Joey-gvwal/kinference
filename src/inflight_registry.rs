//! Per-endpoint count of picks not yet observed as first-token.
//!
//! Scraped metrics lag, so during a burst several picks can land before a
//! scrape reflects the first. The picker uses this count to break ties and
//! avoid piling a burst onto one worker.
//!
//! Keyed by `endpoint` (`host:port`), which every `WorkerScore` carries. A
//! dead endpoint's entry just becomes an unused map slot.

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

    /// Bump the in-flight count for an endpoint, after a pick.
    pub fn inc(&self, endpoint: &str) {
        self.counts
            .entry(endpoint.to_string())
            .or_insert_with(|| AtomicU32::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the in-flight count for an endpoint, saturating at zero.
    /// Called when the pick's observation is released.
    pub fn dec(&self, endpoint: &str) {
        if let Some(entry) = self.counts.get(endpoint) {
            // Saturate at 0, where `fetch_sub` would wrap.
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

    /// Current in-flight count for an endpoint; 0 if never seen.
    pub fn get(&self, endpoint: &str) -> u32 {
        self.counts
            .get(endpoint)
            .map(|e| e.value().load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests;
