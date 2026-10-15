//! The 1 Hz rollup behind the fleet gauges on `/metrics`.
//!
//! Prometheus keeps the history. This task only turns live state into the
//! gauges that have no natural counter: the radix tree's size, which only a
//! throttled `dump_events` can measure, and per-worker reachability and
//! saturation, read off the scrape registry.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use dynamo_kv_router::indexer::KvIndexerInterface;
use dynamo_kv_router::protocols::KvCacheEventData;
use tokio_util::sync::CancellationToken;

use crate::pool::PoolRegistry;
use crate::prom::PromMetrics;
use crate::scrape::MetricsRegistry;

/// `dump_events` walks the whole tree, so the size gauges refresh every tenth
/// tick rather than every tick.
const DUMP_EVERY_TICKS: u64 = 10;

/// A worker whose last successful scrape is older than this counts as
/// unresponsive in `kinference_workers`.
const UNRESPONSIVE_AFTER_MS: u64 = 5000;

/// Spawn the rollup. Runs until `cancel` fires.
pub fn spawn(
    pools: Arc<PoolRegistry>,
    metrics: Arc<MetricsRegistry>,
    prom: Arc<PromMetrics>,
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
                    if n.is_multiple_of(DUMP_EVERY_TICKS)
                        && let Some((blocks, workers)) = index_size(&pools).await
                    {
                        prom.set_index_size(blocks, workers);
                    }
                    let (ok, unresponsive) = worker_health(&pools, &metrics);
                    prom.set_worker_states(ok, unresponsive);
                    prom.set_worker_states_by_model(&worker_health_by_model(&pools, &metrics));
                    prom.set_worker_saturation(&worker_saturation(&pools, &metrics));
                    n = n.wrapping_add(1);
                }
            }
        }
    });
}

/// Blocks and publishing workers summed over every pool's radix tree. `None`
/// when no pool answered. The pool snapshot is taken per call and not held, so
/// a pool removed since the last tick stops being counted and stops being
/// pinned.
async fn index_size(pools: &PoolRegistry) -> Option<(u64, u64)> {
    let mut blocks = 0u64;
    let mut workers = HashSet::new();
    let mut any = false;
    let current = pools.snapshot();
    for pool in current.iter() {
        if let Ok(events) = pool.indexer.dump_events().await {
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
}

fn responsive(metrics: &MetricsRegistry, worker_id_num: u64) -> bool {
    metrics.age_ms_for(worker_id_num) <= UNRESPONSIVE_AFTER_MS
}

/// Fleet-wide `(ok, unresponsive)`, over the routing state.
fn worker_health(pools: &PoolRegistry, metrics: &MetricsRegistry) -> (u64, u64) {
    let (mut ok, mut unresponsive) = (0u64, 0u64);
    for pool in pools.snapshot().iter() {
        for id in pool.registry.ids() {
            if responsive(metrics, id) {
                ok += 1;
            } else {
                unresponsive += 1;
            }
        }
    }
    (ok, unresponsive)
}

/// The same split per served model, for `kinference_workers_by_model`. Uses
/// the registry's `by_model` index, so this agrees with what routing sees. A
/// worker serving several models appears under each.
fn worker_health_by_model(
    pools: &PoolRegistry,
    metrics: &MetricsRegistry,
) -> Vec<(String, u64, u64)> {
    let mut acc: HashMap<String, (u64, u64)> = HashMap::new();
    for pool in pools.snapshot().iter() {
        for model in pool.registry.known_models() {
            let e = acc.entry(model.clone()).or_default();
            for (id, _) in pool.registry.workers_for_model(&model) {
                if responsive(metrics, id) {
                    e.0 += 1;
                } else {
                    e.1 += 1;
                }
            }
        }
    }
    acc.into_iter().map(|(m, (a, b))| (m, a, b)).collect()
}

/// The two saturation gauges per `(worker, model)`: the windowed means an
/// `InferencePolicy` predicate compares against, published so a threshold can
/// be calibrated rather than guessed. A worker serving several models appears
/// once per model with the same reading.
fn worker_saturation(
    pools: &PoolRegistry,
    metrics: &MetricsRegistry,
) -> Vec<(String, String, f64, f64)> {
    let mut out = Vec::new();
    for pool in pools.snapshot().iter() {
        for model in pool.registry.known_models() {
            for (id, entry) in pool.registry.workers_for_model(&model) {
                let m = metrics.latest(id);
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
