//! Pool lifecycle: create, update, retire, reap.
//!
//! Pools used to be built once at startup and never touched. Once a worker set
//! can appear and disappear while traffic flows — a `[[workerset]]` today, an
//! `InferenceWorkerSet` next — something has to own that transition, and the
//! transitions are not symmetric.
//!
//! ## One owner, no lock
//!
//! `PoolManager` is owned by a single reconcile task and takes `&mut self`, so
//! the live map needs no lock at all. What the rest of the process reads is the
//! [`PoolRegistry`] snapshot the manager publishes — the same
//! `RwLock<Arc<_>>`-behind-a-pointer-write shape as `policy::store::PolicyStore`.
//!
//! ## Teardown is two-phase, and that is not fastidiousness
//!
//! The vendored `KvIndexer::apply_event` and `remove_worker` **`.unwrap()` a
//! channel send**, so they panic once the indexer's token is cancelled. A
//! one-shot "cancel and drop" teardown would therefore race every in-flight
//! event against the cancellation and panic the ingest task that lost.
//!
//! So:
//!
//! 1. **Retire**, synchronous: publish a snapshot without the pool, drop its
//!    `model_router` claims, stop its discovery, evict its workers *without*
//!    touching the indexer, and move it to `retired`. The token stays live.
//! 2. **Reap**, on a sweep: once nothing else holds the pool and a linger has
//!    passed, cancel the token — the indexer thread exits — and drop.
//!
//! The strong-count test is exact rather than a heuristic, and only because of
//! the earlier `InflightHandles` work: nothing outlives `pick_and_record`
//! holding an `Arc<Pool>`, so `strong_count == 1` really does mean "no request
//! is inside this pool". A pending observation carries the two registries
//! directly and does not pin the pool.
//!
//! ## Resurrection
//!
//! A watch reconnect, or a `delete --all && apply`, retires and recreates the
//! same worker set within seconds. Recreating from scratch would dump every
//! prefix in the fleet and re-warm from cold. So create first checks `retired`
//! for the same [`PoolKey`] with an identical spec and un-retires in place:
//! same `PoolId`, same radix tree, same registry.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::PoolSpec;
use crate::policy::PoolKey;
use crate::pool::{Pool, PoolId, PoolMap, PoolRegistry};

/// Side effects the manager cannot perform itself, because they belong to
/// subsystems it must not depend on (discovery, telemetry, the coordinator
/// channel). Implemented in `main.rs`, where those handles live.
pub trait PoolHooks: Send + Sync {
    /// Start everything that feeds a new pool: its `Discovery` task and the
    /// forwarder that tags its `WorkerChange`s onto the shared coordinator
    /// channel.
    ///
    /// An `Err` means the pool is NOT published — better an absent pool with a
    /// loud error than a visible one nothing feeds.
    fn on_create(&self, pool: &Arc<Pool>, spec: &PoolSpec) -> anyhow::Result<()>;

    /// Drop every fleet-wide trace of this pool's workers: telemetry slots,
    /// metrics samples, per-worker tasks.
    ///
    /// MUST NOT call into the pool's `KvIndexer` — its subtree goes away with
    /// the whole tree at reap, and calling `remove_worker` here would race the
    /// cancellation the reaper is about to do. MUST NOT cancel the pool token.
    fn on_retire(&self, pool: &Arc<Pool>);
}

/// When a retired pool may be reaped.
#[derive(Debug, Clone, Copy)]
pub struct ReapPolicy {
    /// Grace period after retirement before reaping is even considered, so a
    /// worker set that flaps can be resurrected with its cache intact.
    pub linger: Duration,
    /// Hard backstop. A leaked `Arc<Pool>` would otherwise pin an OS thread
    /// forever; past this point the pool is reaped regardless, with a WARN
    /// naming the strong count so the leak is attributable.
    pub backstop: Duration,
}

impl Default for ReapPolicy {
    fn default() -> Self {
        Self {
            linger: Duration::from_secs(30),
            backstop: Duration::from_secs(600),
        }
    }
}

enum CreateOutcome {
    Created,
    /// A retired pool with an identical spec came back: same `PoolId`, same
    /// radix tree, no re-warm.
    Resurrected,
    /// `on_create` refused; nothing was published.
    Failed,
}

struct Retired {
    pool: Arc<Pool>,
    spec: PoolSpec,
    at: Instant,
}

/// What one reconcile did. Returned for logging and for tests; the manager
/// itself does not act on it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub created: Vec<PoolKey>,
    pub resurrected: Vec<PoolKey>,
    /// Tuning changed; same pool, same cache, new coefficients.
    pub updated: Vec<PoolKey>,
    /// `kv_block_size` changed, which the indexer cannot absorb — retire +
    /// create, and the cache is genuinely lost.
    pub replaced: Vec<PoolKey>,
    pub retired: Vec<PoolKey>,
    /// `on_create` failed; nothing was published.
    pub failed: Vec<PoolKey>,
}

impl ReconcileReport {
    #[allow(dead_code)] // asserted in tests; a caller wanting "did anything change?"
    pub fn is_noop(&self) -> bool {
        self == &Self::default()
    }
}

pub struct PoolManager {
    registry: Arc<PoolRegistry>,
    /// The live set, ordered so the published `PoolMap` has a stable order and
    /// two replicas that saw their pools appear in different orders still rank
    /// candidates identically.
    live: BTreeMap<PoolKey, (Arc<Pool>, PoolSpec)>,
    retired: Vec<Retired>,
    hooks: Arc<dyn PoolHooks>,
    /// Process-wide token; every pool takes a child of it.
    parent: tokio_util::sync::CancellationToken,
    model_router: Arc<
        std::sync::RwLock<std::collections::HashMap<String, std::collections::BTreeSet<PoolId>>>,
    >,
    reap: ReapPolicy,
}

impl PoolManager {
    pub fn new(
        registry: Arc<PoolRegistry>,
        hooks: Arc<dyn PoolHooks>,
        parent: tokio_util::sync::CancellationToken,
        model_router: Arc<
            std::sync::RwLock<
                std::collections::HashMap<String, std::collections::BTreeSet<PoolId>>,
            >,
        >,
        reap: ReapPolicy,
    ) -> Self {
        Self {
            registry,
            live: BTreeMap::new(),
            retired: Vec::new(),
            hooks,
            parent,
            model_router,
            reap,
        }
    }

    /// Bring the live set in line with `desired`.
    ///
    /// The caller decides what `desired` is; this only applies it. In
    /// particular an EMPTY `desired` retires everything, which is correct when
    /// the operator really removed every worker set and catastrophic when it
    /// just means "the watch has not listed yet" — gating on that is the
    /// caller's job, not this function's.
    pub fn reconcile(&mut self, desired: Vec<PoolSpec>) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        let wanted: BTreeMap<PoolKey, PoolSpec> =
            desired.into_iter().map(|s| (s.key.clone(), s)).collect();

        // Retire first, so a replace frees its workers' fleet-wide telemetry
        // slots before the replacement asks for them. `telemetry_max_workers` is a hard
        // cap and `handle_added` skips silently once it is hit.
        let going: Vec<PoolKey> = self
            .live
            .keys()
            .filter(|k| !wanted.contains_key(*k))
            .cloned()
            .collect();
        for key in going {
            self.retire(&key);
            report.retired.push(key);
        }

        for (key, spec) in wanted {
            match self.live.get(&key) {
                Some((_, live_spec)) if *live_spec == spec => {}
                Some((pool, _)) if pool.block_size != spec.kv_block_size => {
                    // The indexer is built around block_size and cannot be
                    // rebased. Loud, because the whole prefix cache for this set
                    // is about to be discarded.
                    tracing::warn!(
                        workerset = %key,
                        from = pool.block_size,
                        to = spec.kv_block_size,
                        "kv_block_size changed; replacing the worker set and discarding \
                         its prefix cache (the KvIndexer is built around block size)"
                    );
                    self.retire(&key);
                    match self.create(spec) {
                        CreateOutcome::Failed => report.failed.push(key),
                        // A replace can still land on a matching retired entry
                        // from an earlier cycle, which is a saving, not a bug.
                        _ => report.replaced.push(key),
                    }
                }
                Some((pool, live_spec)) => {
                    // A DECLARED model list is per-worker state, and the update
                    // below shares the registry — so a change to it has to be
                    // pushed into the existing workers here. Left alone it would
                    // sit inert until a worker churned, and that worker would
                    // then be refused for disagreeing with the stale pin.
                    if live_spec.static_models != spec.static_models {
                        let touched = pool
                            .registry
                            .reset_declared_models(spec.static_models.as_deref());
                        crate::monitor::release_models(&self.model_router, pool.id, &pool.name);
                        if let Some(models) = spec.static_models.as_deref() {
                            crate::monitor::associate_models(
                                &self.model_router,
                                pool.id,
                                &pool.name,
                                models,
                            );
                        }
                        tracing::info!(
                            workerset = %key,
                            workers = touched,
                            models = ?spec.static_models,
                            "declared model list changed; re-seeded this set's workers"
                        );
                    }
                    // Copy-on-write: new values, same id, same warm cache.
                    let next = Arc::new(pool.with_spec(&spec));
                    tracing::info!(
                        workerset = %key,
                        profile = %spec.profile,
                        "worker set retuned in place; prefix cache retained"
                    );
                    self.live.insert(key.clone(), (next, spec));
                    self.publish();
                    report.updated.push(key);
                }
                None => match self.create(spec) {
                    CreateOutcome::Created => report.created.push(key),
                    CreateOutcome::Resurrected => report.resurrected.push(key),
                    CreateOutcome::Failed => report.failed.push(key),
                },
            }
        }
        report
    }

    /// Reap retired pools that nothing is using any more.
    ///
    /// Returns how many were reaped. Cheap enough to call on a 10s tick: it
    /// touches only the retired list, which is empty in the steady state.
    pub fn sweep(&mut self) -> usize {
        let (linger, backstop) = (self.reap.linger, self.reap.backstop);
        let mut reaped = 0;
        self.retired.retain(|r| {
            let age = r.at.elapsed();
            // `strong_count == 1` means this list holds the only reference:
            // no pick is inside the pool and no snapshot still lists it.
            let idle = Arc::strong_count(&r.pool) == 1;
            if age < linger || (!idle && age < backstop) {
                return true;
            }
            if !idle {
                tracing::warn!(
                    workerset = %r.pool.key,
                    strong_count = Arc::strong_count(&r.pool),
                    age_secs = age.as_secs(),
                    "reaping a retired worker set past the backstop while handles are \
                     still outstanding; something leaked an Arc<Pool>"
                );
            }
            // Cancelling here and not at retire is the whole point of the two
            // phases: the KvIndexer's channel send unwraps, so anything still
            // feeding it would panic rather than stop.
            r.pool.cancel.cancel();
            tracing::info!(workerset = %r.pool.key, age_secs = age.as_secs(), "worker set reaped");
            reaped += 1;
            false
        });
        reaped
    }

    /// Live pools, for logging and tests.
    #[allow(dead_code)] // the watch-driven reconcile logs against this
    pub fn live_keys(&self) -> Vec<PoolKey> {
        self.live.keys().cloned().collect()
    }

    #[cfg(test)]
    fn retired_keys(&self) -> Vec<PoolKey> {
        self.retired.iter().map(|r| r.pool.key.clone()).collect()
    }

    #[cfg(test)]
    fn live_pool(&self, key: &PoolKey) -> Option<&Arc<Pool>> {
        self.live.get(key).map(|(p, _)| p)
    }

    /// Bring a worker set into service, reusing a retired pool when one matches
    /// exactly.
    fn create(&mut self, spec: PoolSpec) -> CreateOutcome {
        let (pool, resurrected) = match self.take_resurrectable(&spec) {
            Some(pool) => (pool, true),
            None => (
                Arc::new(Pool::from_spec(PoolId::next(), &spec, &self.parent)),
                false,
            ),
        };
        // Hooks BEFORE publishing: "visible in the snapshot" must imply "fully
        // wired", or a pick can land on a pool whose discovery never started.
        if let Err(err) = self.hooks.on_create(&pool, &spec) {
            tracing::error!(
                workerset = %spec.key,
                error = %err,
                "failed to start this worker set's discovery; not publishing it"
            );
            if resurrected {
                // Put it back, or nothing ever reaps it and its indexer thread
                // outlives the process's interest in it.
                self.retired.push(Retired {
                    pool,
                    spec,
                    at: Instant::now(),
                });
            }
            return CreateOutcome::Failed;
        }
        tracing::info!(
            workerset = %spec.key,
            profile = %spec.profile,
            block_size = spec.kv_block_size,
            resurrected,
            "worker set created"
        );
        self.live.insert(spec.key.clone(), (pool, spec));
        self.publish();
        if resurrected {
            CreateOutcome::Resurrected
        } else {
            CreateOutcome::Created
        }
    }

    /// Pull a retired pool back if its spec is unchanged.
    ///
    /// A spec that CHANGED must not resurrect: the retired pool's indexer is
    /// built around the old block size and its registry holds the old set's
    /// workers, so reusing it would be a silent mismatch rather than a saving.
    fn take_resurrectable(&mut self, spec: &PoolSpec) -> Option<Arc<Pool>> {
        let idx = self
            .retired
            .iter()
            .position(|r| r.pool.key == spec.key && r.spec == *spec)?;
        Some(self.retired.remove(idx).pool)
    }

    fn retire(&mut self, key: &PoolKey) {
        let Some((pool, spec)) = self.live.remove(key) else {
            return;
        };
        // 1. Stop routing to it. Publishing first means no NEW pick can enter
        //    the pool while we tear its workers down.
        self.publish();
        // 2. Drop its model claims, or a replacement worker set serving the
        //    same model is ignored forever.
        crate::monitor::release_models(&self.model_router, pool.id, &pool.name);
        // 3. Fleet-wide worker state (telemetry slots, metrics, per-worker
        //    tasks). Deliberately no indexer call — see `PoolHooks::on_retire`.
        self.hooks.on_retire(&pool);
        tracing::info!(
            workerset = %key,
            strong_count = Arc::strong_count(&pool),
            "worker set retired; awaiting reap"
        );
        self.retired.push(Retired {
            pool,
            spec,
            at: Instant::now(),
        });
    }

    fn publish(&self) {
        let pools: Vec<Arc<Pool>> = self.live.values().map(|(p, _)| p.clone()).collect();
        self.registry.publish(Arc::new(PoolMap::from_pools(pools)));
    }
}

#[cfg(test)]
mod tests;
