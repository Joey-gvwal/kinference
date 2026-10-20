//! The policy snapshot: the controller publishes it, the router and the
//! admin API read it.

use std::sync::{Arc, RwLock};

use crate::resolve::PolicyIndex;

/// The published snapshot. Read on the pick hot path: take the read lock,
/// clone the `Arc`, release. A poisoned lock is recovered, since a publish
/// is one pointer swap and cannot leave the value torn.
#[derive(Default)]
pub struct PolicyStore {
    current: RwLock<Arc<PolicyIndex>>,
}

impl PolicyStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> Arc<PolicyIndex> {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn publish(&self, index: Arc<PolicyIndex>) {
        *self.current.write().unwrap_or_else(|p| p.into_inner()) = index;
    }
}
