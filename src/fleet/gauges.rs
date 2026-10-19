//! The fleet gauges on `/metrics`, as a Prometheus collector.
//!
//! Worker health and saturation are computed when the registry gathers, so
//! a scrape sees them as they are at that instant. The radix tree's size can
//! only be measured by `dump_events`, which walks the whole tree, so the
//! `/metrics` handler refreshes it through [`FleetGauges::refresh_index`] at
//! most once per [`INDEX_DUMP_INTERVAL`] and the gauges hold the last dump
//! in between.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dynamo_kv_router::indexer::KvIndexerInterface;
use dynamo_kv_router::protocols::KvCacheEventData;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{GaugeVec, IntGauge, IntGaugeVec, Opts, Registry};
use tokio::time::Instant;

use crate::fleet::pool::PoolRegistry;
use crate::fleet::scrape::MetricsRegistry;

/// How long one tree dump serves `kinference_index_blocks` and
/// `kinference_index_workers`.
pub const INDEX_DUMP_INTERVAL: Duration = Duration::from_secs(10);

/// A worker whose last successful scrape is older than this counts as
/// unresponsive in `kinference_workers`.
const UNRESPONSIVE_AFTER_MS: u64 = 5000;

/// The six fleet gauge families and the state they are computed from.
///
/// Clones share everything, so one clone is registered as the collector and
/// another is kept to call `refresh_index`.
#[derive(Clone)]
pub struct FleetGauges {
    pools: Arc<PoolRegistry>,
    scrape: Arc<MetricsRegistry>,
    workers: IntGaugeVec,
    workers_by_model: IntGaugeVec,
    queued_prompts_mean: GaugeVec,
    kv_cache_usage_mean: GaugeVec,
    index_blocks: IntGauge,
    index_workers: IntGauge,
    /// Serializes `collect`, so two concurrent scrapes cannot interleave one
    /// family's reset with the other's read.
    collect_lock: Arc<Mutex<()>>,
    /// When the tree was last dumped. Held across the dump, so concurrent
    /// scrapes inside one interval share it.
    last_dump: Arc<tokio::sync::Mutex<Option<Instant>>>,
}

impl FleetGauges {
    /// Build the families and register them as one collector in `registry`.
    pub fn new(
        pools: Arc<PoolRegistry>,
        scrape: Arc<MetricsRegistry>,
        registry: &Registry,
    ) -> Self {
        let workers = IntGaugeVec::new(
            Opts::new(
                "kinference_workers",
                "Workers by reachability: ok, or unresponsive when its /metrics \
                 scrape has been stale for over 5s. An unresponsive worker is still \
                 routable — it is scored on its last known sample",
            ),
            &["state"],
        )
        .expect("workers gauge opts are valid");
        let workers_by_model = IntGaugeVec::new(
            Opts::new(
                "kinference_workers_by_model",
                "Worker reachability by served model and state. A worker serving \
                 several models is counted under EACH, so summing across models \
                 can exceed the fleet — use kinference_workers for the total",
            ),
            &["model", "state"],
        )
        .expect("workers_by_model gauge opts are valid");
        let queued_prompts_mean = GaugeVec::new(
            Opts::new(
                "kinference_worker_queued_prompts_mean30s",
                "Mean queued prompts per worker over a 30s window — the exact \
                 value `InferencePolicy`'s `queuedPrompts` predicate reads. \
                 Fractional: a worker that queued 5 prompts for two seconds \
                 out of thirty reads about 0.3. Plot it to pick a threshold",
            ),
            &["worker", "model"],
        )
        .expect("worker_queued_prompts_mean gauge opts are valid");
        let kv_cache_usage_mean = GaugeVec::new(
            Opts::new(
                "kinference_worker_kv_cache_usage_mean30s",
                "Mean KV-cache utilization per worker (0..1) over the same 30s \
                 window, as read by the `kvCacheUsage` predicate. The headroom \
                 signal: it rises monotonically with pressure, where the queue \
                 can read low at high pressure",
            ),
            &["worker", "model"],
        )
        .expect("worker_kv_cache_usage_mean gauge opts are valid");
        let index_blocks = IntGauge::new(
            "kinference_index_blocks",
            "Blocks currently in the prefix radix tree, summed across pools. \
             Measured by a tree dump at most once every 10s, since the dump is \
             O(tree)",
        )
        .expect("index_blocks gauge opts are valid");
        let index_workers = IntGauge::new(
            "kinference_index_workers",
            "Workers appearing in the prefix radix tree, summed across pools. \
             Compare against the configured worker count: a worker missing here \
             can never win a cache-affinity pick, so it silently gets no prefix \
             credit. Same refresh as kinference_index_blocks",
        )
        .expect("index_workers gauge opts are valid");
        let gauges = Self {
            pools,
            scrape,
            workers,
            workers_by_model,
            queued_prompts_mean,
            kv_cache_usage_mean,
            index_blocks,
            index_workers,
            collect_lock: Arc::new(Mutex::new(())),
            last_dump: Arc::new(tokio::sync::Mutex::new(None)),
        };
        // The names are static, so this can only fail on a second
        // registration into the same registry.
        registry
            .register(Box::new(gauges.clone()))
            .expect("register the fleet gauges once per registry");
        gauges
    }

    /// Dump the trees and set the index gauges, unless a dump younger than
    /// `INDEX_DUMP_INTERVAL` exists. Call before rendering `/metrics`.
    pub async fn refresh_index(&self) {
        let mut last = self.last_dump.lock().await;
        if last.is_some_and(|at| at.elapsed() < INDEX_DUMP_INTERVAL) {
            return;
        }
        if let Some((blocks, workers)) = index_size(&self.pools).await {
            self.index_blocks.set(blocks as i64);
            self.index_workers.set(workers as i64);
        }
        *last = Some(Instant::now());
    }

    fn families(&self) -> [&dyn Collector; 6] {
        [
            &self.workers,
            &self.workers_by_model,
            &self.queued_prompts_mean,
            &self.kv_cache_usage_mean,
            &self.index_blocks,
            &self.index_workers,
        ]
    }
}

impl Collector for FleetGauges {
    fn desc(&self) -> Vec<&Desc> {
        self.families().iter().flat_map(|f| f.desc()).collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let _serial = self.collect_lock.lock().unwrap_or_else(|e| e.into_inner());
        let (ok, unresponsive) = worker_health(&self.pools, &self.scrape);
        self.workers.with_label_values(&["ok"]).set(ok as i64);
        self.workers
            .with_label_values(&["unresponsive"])
            .set(unresponsive as i64);
        self.workers_by_model.reset();
        for (model, ok, unresponsive) in worker_health_by_model(&self.pools, &self.scrape) {
            self.workers_by_model
                .with_label_values(&[&model, "ok"])
                .set(ok as i64);
            self.workers_by_model
                .with_label_values(&[&model, "unresponsive"])
                .set(unresponsive as i64);
        }
        self.queued_prompts_mean.reset();
        self.kv_cache_usage_mean.reset();
        for (worker, model, queued, kv) in worker_saturation(&self.pools, &self.scrape) {
            self.queued_prompts_mean
                .with_label_values(&[&worker, &model])
                .set(queued);
            self.kv_cache_usage_mean
                .with_label_values(&[&worker, &model])
                .set(kv);
        }
        self.families().iter().flat_map(|f| f.collect()).collect()
    }
}

/// Blocks and publishing workers summed over every pool's radix tree. `None`
/// when no pool answered. The pool snapshot is taken per call and not held, so
/// a pool removed since the last dump stops being counted and stops being
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

#[cfg(test)]
mod tests;

/// A `Stored` event for `worker_id` with one root block per hash.
#[cfg(test)]
pub(crate) fn test_stored_event(
    worker_id: u64,
    hashes: &[u64],
) -> dynamo_kv_router::protocols::RouterEvent {
    use dynamo_kv_router::protocols::{
        ExternalSequenceBlockHash, KvCacheEvent, KvCacheStoreData, KvCacheStoredBlockData,
        LocalBlockHash, RouterEvent,
    };
    RouterEvent::new(
        worker_id,
        KvCacheEvent {
            event_id: 0,
            dp_rank: 0,
            data: KvCacheEventData::Stored(KvCacheStoreData {
                parent_hash: None,
                start_position: None,
                blocks: hashes
                    .iter()
                    .map(|h| KvCacheStoredBlockData {
                        block_hash: ExternalSequenceBlockHash(*h),
                        tokens_hash: LocalBlockHash(*h),
                        mm_extra_info: None,
                    })
                    .collect(),
            }),
        },
    )
}
