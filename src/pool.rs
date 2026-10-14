//! A `Pool` is the runtime state of exactly one worker set: what a scoring
//! decision consults and commits into.
//!
//! A worker set is declared as an `InferenceWorkerSet` or in TOML, both
//! resolve to a `PoolSpec`, and a `Pool` is built from that: a radix tree at
//! one block size, the worker registry that populates it, the inflight
//! bookkeeping that must stay consistent with both, and the lock that
//! serializes commits into them.
//!
//! Pools are independent, 1:1 with worker sets, with no shared radix trees.
//! A single `Pick` may score against several, but they are never merged.
//!
//! Per pool:
//! - `predictor` / `picker`: coefficients and routing policy.
//! - `block_size`: KV hash granularity.
//! - `indexer`: a `KvIndexer` built with `block_size`.
//! - `registry`: workers, plus homogeneity enforcement (`canonical_models`).
//! - `inflight_prefixes` / `inflight`: shadow tracking of
//!   dispatched-but-unobserved work.
//! - `pick_lock`: so picks in different pools run concurrently.
//!
//! The metrics registry, observation CSV sink, tokenization and response-id
//! minting are shared, on `KinferenceService`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use dynamo_kv_router::indexer::{KvIndexer, KvIndexerMetrics};
use tokio_util::sync::CancellationToken;

use crate::config::{PickerConfig, PoolSpec, PredictorConfig, TokenizeMode};
use crate::inflight_prefixes::InflightPrefixes;
use crate::inflight_registry::InflightRegistry;
use crate::policy::{PoolId, PoolKey};
use crate::workers::WorkerRegistry;

/// An immutable view of the fleet's pools.
///
/// A snapshot rather than a concurrent map, so consumers that iterate (the
/// rollup, `gather_facts`, `/readyz`) see one coherent pool set.
#[derive(Default)]
pub struct PoolMap {
    by_id: HashMap<PoolId, Arc<Pool>>,
    ordered: Vec<Arc<Pool>>,
}

#[allow(dead_code)]
impl PoolMap {
    pub fn from_pools(pools: Vec<Arc<Pool>>) -> Self {
        Self {
            by_id: pools.iter().map(|p| (p.id, p.clone())).collect(),
            ordered: pools,
        }
    }

    /// `None` once a pool is gone. Callers must degrade rather than panic, since
    /// an id can outlive its pool (a pending observation, a queued eviction).
    pub fn get(&self, id: PoolId) -> Option<&Arc<Pool>> {
        self.by_id.get(&id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<Pool>> {
        self.ordered.iter()
    }

    pub fn as_slice(&self) -> &[Arc<Pool>] {
        &self.ordered
    }

    pub fn len(&self) -> usize {
        self.ordered.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ordered.is_empty()
    }
}

/// The published pool set. Read on the pick hot path: take the read lock, clone
/// the `Arc`, release.
pub struct PoolRegistry {
    current: RwLock<Arc<PoolMap>>,
    /// Bumped on every publish. Policy resolution depends on which pools exist
    /// as well as on CRD events, so it re-resolves on this.
    generation: tokio::sync::watch::Sender<u64>,
}

impl Default for PoolRegistry {
    fn default() -> Self {
        Self {
            current: RwLock::new(Arc::new(PoolMap::default())),
            generation: tokio::sync::watch::channel(0).0,
        }
    }
}

impl PoolRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Wake up whenever the pool set changes.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.generation.subscribe()
    }

    pub fn from_pools(pools: Vec<Arc<Pool>>) -> Arc<Self> {
        let r = Self::new();
        r.publish(Arc::new(PoolMap::from_pools(pools)));
        r
    }

    pub fn snapshot(&self) -> Arc<PoolMap> {
        self.current.read().expect("pool registry poisoned").clone()
    }

    pub fn publish(&self, map: Arc<PoolMap>) {
        *self.current.write().expect("pool registry poisoned") = map;
        // After the write, so a woken consumer reads the new set rather than
        // racing the one it was told about.
        self.generation.send_modify(|g| *g = g.wrapping_add(1));
    }
}

/// The self-contained routing world for one homogeneous worker pool.
///
/// Built from a [`PoolSpec`] by [`crate::pool_manager::PoolManager`] and
/// published into the [`PoolRegistry`]. A dropped pool survives as long as
/// requests still hold it, which lets the reaper tell exactly when it is safe
/// to cancel the token.
pub struct Pool {
    /// This pool's opaque, never-reused handle. See [`PoolId`].
    pub id: PoolId,
    /// The declaration this pool came from, stable across restarts and matched
    /// on by reconciles. See [`PoolKey`].
    pub key: PoolKey,
    /// Operator-facing name (from `[[workerset]].name`, or the
    /// `InferenceWorkerSet`'s). Unique across the fleet.
    pub name: String,
    /// The `[[profile]]` this pool's tuning came from, carried into the
    /// observation CSV.
    pub profile: String,
    /// KV-cache hash block size for every worker in this pool.
    pub block_size: u32,
    /// Latency-model coefficients for this pool.
    pub predictor: PredictorConfig,
    /// Routing policy for this pool.
    pub picker: PickerConfig,
    /// This pool's worker set + `by_model` index + homogeneity guard.
    pub registry: Arc<WorkerRegistry>,
    /// This pool's KV radix-tree index, built with `block_size`.
    pub indexer: Arc<KvIndexer>,
    /// Dispatched-but-not-yet-stored prefix tracker for this pool.
    pub inflight_prefixes: Arc<InflightPrefixes>,
    /// Per-endpoint shadow pick counter for this pool.
    pub inflight: Arc<InflightRegistry>,
    /// Serializes the score → pick → record critical section for this
    /// pool. Per-pool so picks in different pools don't contend.
    pub pick_lock: Arc<Mutex<()>>,
    /// This pool's primary tokenization path (Render/Local/Compare).
    pub tokenize_mode: TokenizeMode,
    /// This pool's dedicated `/render` URL, if configured. When `None`,
    /// render/compare rotate across the pool's own worker `http_url`s.
    pub tokenize_render_url: Option<String>,
    /// `/v1/models` scrape cadence for this pool's workers; `None` uses the
    /// fleet-wide setting.
    pub models_refresh_secs: Option<u64>,
    /// `Some` when this pool's model list is declared rather than discovered.
    /// Its workers are seeded with these names and never scraped for
    /// `/v1/models`.
    pub static_models: Option<Vec<String>>,
    /// Scopes everything this pool owns, including its `KvIndexer`'s OS
    /// thread. A child of the process token; see [`Pool::from_spec`].
    ///
    /// Cancelled by the reaper, not at retirement: `KvIndexer` unwraps a
    /// channel send, so anything still reaching it after cancellation panics.
    /// Discovery uses a separate token that does stop at retirement.
    pub cancel: CancellationToken,
}

impl Pool {
    /// Build a `Pool` from its config. Constructs a fresh `KvIndexer` sized to
    /// this pool's `block_size`, plus empty per-pool registries.
    ///
    /// `parent` is the process-wide token; the pool takes a child of it.
    /// `KvIndexer`'s `Drop` cancels the token it was given, so handing it the
    /// process token would shut down the whole process when a pool is dropped.
    /// Process shutdown still reaches every pool.
    pub fn from_spec(id: PoolId, spec: &PoolSpec, parent: &CancellationToken) -> Self {
        let cancel = parent.child_token();
        let kv_metrics = Arc::new(KvIndexerMetrics::new_unregistered());
        let indexer = Arc::new(KvIndexer::new(
            cancel.clone(),
            spec.kv_block_size,
            kv_metrics,
        ));
        Self {
            id,
            cancel,
            key: spec.key.clone(),
            name: spec.name.clone(),
            profile: spec.profile.clone(),
            block_size: spec.kv_block_size,
            predictor: spec.predictor.clone(),
            picker: spec.picker.clone(),
            registry: Arc::new(WorkerRegistry::new()),
            indexer,
            inflight_prefixes: Arc::new(InflightPrefixes::new()),
            inflight: Arc::new(InflightRegistry::new()),
            pick_lock: Arc::new(Mutex::new(())),
            tokenize_mode: spec.tokenize_mode,
            tokenize_render_url: spec.render_url.clone(),
            models_refresh_secs: spec.models_refresh_secs,
            static_models: spec.static_models.clone(),
        }
    }

    /// Rebuild this pool with new tuning, sharing every piece of live state.
    ///
    /// Keeps the same `PoolId`, `KvIndexer` (and so its warm prefix cache),
    /// worker registry, inflight trackers and token; only the tuning values
    /// change. Readers holding the old snapshot finish against the old values.
    ///
    /// `block_size` is baked into the `KvIndexer`, so changing it requires
    /// retire-and-create instead (debug-asserted).
    pub fn with_spec(&self, spec: &PoolSpec) -> Self {
        debug_assert_eq!(
            self.block_size, spec.kv_block_size,
            "block_size is baked into the KvIndexer; a change needs retire + create"
        );
        Self {
            id: self.id,
            key: spec.key.clone(),
            name: spec.name.clone(),
            profile: spec.profile.clone(),
            block_size: self.block_size,
            predictor: spec.predictor.clone(),
            picker: spec.picker.clone(),
            tokenize_mode: spec.tokenize_mode,
            tokenize_render_url: spec.render_url.clone(),
            models_refresh_secs: spec.models_refresh_secs,
            static_models: spec.static_models.clone(),
            // Shared, so the update keeps all live state.
            registry: self.registry.clone(),
            indexer: self.indexer.clone(),
            inflight_prefixes: self.inflight_prefixes.clone(),
            inflight: self.inflight.clone(),
            pick_lock: self.pick_lock.clone(),
            cancel: self.cancel.clone(),
        }
    }
}

/// One `Pool` for tests, from a minimal static spec.
#[cfg(test)]
pub(crate) fn test_pool(name: &str) -> Arc<Pool> {
    test_pool_with_block_size(name, 16)
}

/// A test pool with a given block size, which sets `total_blocks` and so the
/// prefill term, making pools predict differently for one prompt.
#[cfg(test)]
pub(crate) fn test_pool_with_block_size(name: &str, block_size: u32) -> Arc<Pool> {
    Arc::new(Pool::from_spec(
        PoolId::next(),
        &crate::config::test_spec(name, block_size),
        &CancellationToken::new(),
    ))
}

#[cfg(test)]
mod tests;
