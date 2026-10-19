//! The model index: which pools serve each model name. Written by the
//! scrape and the pool manager, read on the pick path.
//!
//! A new claim is published after the map holds it, so a subscriber that
//! looks the claim up sees it.

use std::collections::{BTreeSet, HashMap};
use std::sync::RwLock;

use kinference_policy::PoolId;

use crate::fleet::sync::{read, write};
use tokio::sync::broadcast;

/// A (model, pool) pair the index did not hold before. A repeat claim sends
/// nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelClaim {
    pub pool: PoolId,
    pub model: String,
}

pub struct ModelIndex {
    claims: RwLock<HashMap<String, BTreeSet<PoolId>>>,
    changes: broadcast::Sender<ModelClaim>,
}

impl Default for ModelIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelIndex {
    pub fn new() -> Self {
        Self {
            claims: RwLock::new(HashMap::new()),
            changes: broadcast::channel(256).0,
        }
    }

    /// New claims, from the point of subscription on. A receiver that falls
    /// behind sees `Lagged` and misses the oldest claims.
    pub fn subscribe(&self) -> broadcast::Receiver<ModelClaim> {
        self.changes.subscribe()
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
            let map = read(&self.claims);
            if models
                .iter()
                .all(|m| map.get(m).is_some_and(|set| set.contains(&pool_id)))
            {
                return;
            }
        }
        let mut added = Vec::new();
        {
            let mut map = write(&self.claims);
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
                    added.push(m.clone());
                }
            }
        }
        for model in added {
            // `Err` means nobody is listening.
            let _ = self.changes.send(ModelClaim {
                pool: pool_id,
                model,
            });
        }
    }

    /// Drop every claim held by `pool_id`, and forget models nobody serves.
    pub fn release(&self, pool_id: PoolId, pool_name: &str) {
        let mut map = write(&self.claims);
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
        read(&self.claims)
            .get(model)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Every model some pool serves.
    pub fn models(&self) -> Vec<String> {
        read(&self.claims).keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests;
