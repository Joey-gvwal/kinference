//! The model index: which pools serve each model name. Written by the
//! scrape and the pool manager, read on the pick path.

use std::collections::{BTreeSet, HashMap};
use std::sync::RwLock;

use kinference_policy::PoolId;

#[derive(Default)]
pub struct ModelIndex {
    claims: RwLock<HashMap<String, BTreeSet<PoolId>>>,
}

impl ModelIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `pool_id` serves `models`. Several pools may serve one
    /// model; ranking happens at pick time. Claims leave via [`Self::release`].
    pub fn associate(&self, pool_id: PoolId, pool_name: &str, models: &[String]) {
        if models.is_empty() {
            return;
        }
        // Fast path for the steady state, so concurrent picks aren't blocked by
        // the write lock.
        {
            let map = self.claims.read().unwrap();
            if models
                .iter()
                .all(|m| map.get(m).is_some_and(|set| set.contains(&pool_id)))
            {
                return;
            }
        }
        let mut map = self.claims.write().unwrap();
        for m in models {
            if map.entry(m.clone()).or_default().insert(pool_id) {
                let serving = map[m].len();
                if serving > 1 {
                    tracing::info!(
                        model = %m,
                        pool = %pool_name,
                        pools_serving = serving,
                        "model also served by this pool; both are pick candidates"
                    );
                } else {
                    tracing::info!(model = %m, pool = %pool_name, "model routed to pool");
                }
            }
        }
    }

    /// Drop every claim held by `pool_id`, and forget models nobody serves.
    pub fn release(&self, pool_id: PoolId, pool_name: &str) {
        let mut map = self.claims.write().unwrap();
        map.retain(|model, set| {
            if !set.remove(&pool_id) {
                return true;
            }
            if set.is_empty() {
                tracing::info!(
                    model = %model,
                    workerset = %pool_name,
                    "last worker set serving this model went away"
                );
                false
            } else {
                true
            }
        });
    }

    /// Pools serving `model`, in id order. Empty when none has claimed it.
    pub fn pools_for(&self, model: &str) -> Vec<PoolId> {
        self.claims
            .read()
            .unwrap()
            .get(model)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Every model some pool serves.
    pub fn models(&self) -> Vec<String> {
        self.claims.read().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests;
