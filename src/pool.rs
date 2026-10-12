//! A `Pool` is the **runtime instance of exactly one worker set** — the state a
//! scoring decision consults and commits into.
//!
//! Three layers are worth keeping distinct:
//!
//! 1. The **declaration** — an `InferenceWorkerSet` in Kubernetes, or a worker
//!    set block in TOML. Two spellings of one idea.
//! 2. The **resolved spec** — what both declarations normalize to, so pool
//!    creation has a single code path regardless of source.
//! 3. The **pool** — this type: a radix tree at one block size, the worker
//!    registry that populates it, the inflight bookkeeping that must stay
//!    consistent with both, and the lock that serializes commits into them.
//!
//! So a pool is not "the config for a worker set" — everything declarative is
//! moving out to profiles and the worker set itself. What is left is mutable
//! state that has to stay self-consistent.
//!
//! **Pools are independent of each other, 1:1 with worker sets.** No shared
//! radix trees, even between pools that agree on block size. The only place
//! several are considered together is a policy decision spanning worker sets,
//! and that ranges *over* pools rather than merging them — so a pool is the unit
//! of scoring STATE, not the unit of a routing decision. A single `Pick` can
//! score against several.
//!
//! Everything that must differ between heterogeneous pools lives here:
//! - `predictor` / `picker` — per-pool coefficients + routing policy.
//! - `block_size` — per-pool KV hash granularity.
//! - `indexer` — a `KvIndexer` built with this pool's `block_size`; two
//!   pools with different block sizes cannot share one radix tree.
//! - `registry` — per-pool worker set + homogeneity enforcement
//!   (`canonical_models`).
//! - `inflight_prefixes` / `inflight` — per-pool shadow tracking of
//!   dispatched-but-unobserved work.
//! - `pick_lock` — per-pool, so picks in different pools run
//!   concurrently (the pre-pool design had one global lock).
//!
//! Cross-cutting infrastructure (telemetry, metrics registry, the
//! observation CSV sink, tokenization, response-id minting) stays on
//! `KinferenceService` and is shared across pools.

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
/// `ordered` exists so the consumers that *iterate* — the telemetry rollup,
/// `gather_facts`, `/readyz`, the dashboard — see the same pools in the same
/// order within a tick. That coherence is why this is a snapshot rather than a
/// concurrent map: a torn view would count a pool in one place and miss it in
/// another during the same pass.
#[derive(Default)]
pub struct PoolMap {
    by_id: HashMap<PoolId, Arc<Pool>>,
    ordered: Vec<Arc<Pool>>,
}

// Every consumer that iterates the pool set now goes through here — the
// telemetry rollup's index gauge, `/readyz`, and `gather_facts` — so each reads
// one coherent view and none of them pins a pool the fleet has dropped.
#[allow(dead_code)] // `get`/`len` have no caller until pools become removable
impl PoolMap {
    pub fn from_pools(pools: Vec<Arc<Pool>>) -> Self {
        Self {
            by_id: pools.iter().map(|p| (p.id, p.clone())).collect(),
            ordered: pools,
        }
    }

    /// `None` once a pool is gone. Callers must degrade rather than panic: an id
    /// can legitimately outlive its pool — a pending observation, a queued
    /// eviction — and that is ordinary, not a bug.
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
/// the `Arc`, release — the same shape as `policy::store::PolicyStore` and
/// `model_router`.
pub struct PoolRegistry {
    current: RwLock<Arc<PoolMap>>,
    /// Bumped on every publish, so a consumer whose own inputs did not change
    /// still knows to recompute.
    ///
    /// Policy resolution needs this: it is driven by CRD watch events, but its
    /// answer also depends on which pools exist. Without a signal, a worker set
    /// whose pool was created just AFTER the last CRD event stays reported as
    /// unresolved indefinitely — the runtime routes to it correctly while
    /// status says it is broken, which is the worst of both.
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
/// published into the [`PoolRegistry`] as an `Arc`. Reachable only through a
/// registry snapshot, so a pool the fleet has dropped survives exactly as long
/// as the requests still holding it — which is what lets the reaper decide,
/// exactly rather than heuristically, when it is safe to cancel the token.
pub struct Pool {
    /// This pool's opaque, never-reused handle. See [`PoolId`].
    pub id: PoolId,
    /// The declaration this pool came from — stable across restarts, and what a
    /// reconcile matches on. See [`PoolKey`].
    pub key: PoolKey,
    /// Operator-facing name (from `[[workerset]].name`, or the
    /// `InferenceWorkerSet`'s). Unique across the fleet.
    pub name: String,
    /// The `[[profile]]` this pool's tuning came from. A label — carried into
    /// the observation CSV so a row can be traced back to the coefficients that
    /// produced it, which is the point of naming tuning at all.
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
    /// This pool's dedicated `/render` URL, if configured. `None` →
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
    /// thread. A CHILD of the process token, so cancelling it stops this pool
    /// and nothing else — see [`Pool::from_spec`].
    ///
    /// Cancelled by the REAPER, not by retirement: `KvIndexer` unwraps a
    /// channel send, so anything still reaching it after cancellation panics.
    /// A pool's discovery is scoped by a separate token that does stop at
    /// retirement.
    pub cancel: CancellationToken,
}

impl Pool {
    /// Build a `Pool` from its config. Constructs a fresh `KvIndexer` sized to
    /// this pool's `block_size`, plus empty per-pool registries.
    ///
    /// `parent` is the process-wide token; the pool takes a **child** of it.
    /// That distinction is load-bearing rather than tidy: `KvIndexer` keeps
    /// whatever token it is handed and its `Drop` cancels that token when the
    /// last `Arc` goes. Handing it the process token — as this did — means
    /// dropping any single pool cancels the token every other subsystem is
    /// watching, i.e. **it shuts down the whole process**. Nothing drops a pool
    /// today, so the bug is latent; it becomes a live one the moment pools are
    /// created and destroyed at runtime.
    ///
    /// `child_token` propagates parent → child only, so process shutdown still
    /// reaches every pool.
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
    /// A settings change must not cost a pool its prefix cache. The new `Pool`
    /// keeps the same `PoolId`, the same `KvIndexer` (so the radix tree and its
    /// warm contents survive), the same worker registry, the same inflight
    /// trackers, and the same cancellation token — only the values change. It is
    /// published over the old one, and readers holding the old snapshot finish
    /// against the old values. No interior mutability, no hot-path lock, no
    /// re-warm.
    ///
    /// `block_size` is the exception and is NOT applied: it is baked into the
    /// `KvIndexer` at construction, so changing it is a retire-and-create, which
    /// the caller must do instead. Debug-asserted rather than silently ignored.
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
            // Shared, deliberately: this is what makes an update free.
            registry: self.registry.clone(),
            indexer: self.indexer.clone(),
            inflight_prefixes: self.inflight_prefixes.clone(),
            inflight: self.inflight.clone(),
            pick_lock: self.pick_lock.clone(),
            cancel: self.cancel.clone(),
        }
    }
}

/// One `Pool` for tests, from a minimal static spec. Shared so dashboard,
/// telemetry, and monitor tests build pools the same way.
#[cfg(test)]
pub(crate) fn test_pool(name: &str) -> Arc<Pool> {
    test_pool_with_block_size(name, 16)
}

/// The block size is what makes two pools predict differently for one prompt —
/// it sets `total_blocks`, and so the prefill term. Named so a test can pin
/// that two sets are not sharing one predictor.
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
