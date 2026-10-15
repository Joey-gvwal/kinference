//! The Kubernetes control plane for CRD-driven routing policy.
//!
//! The resource types and the pure policy logic live in the
//! `kinference-policy` crate, shared with `kinfctl`. Import them as
//! `kinference_policy::…`. This module holds only the code that talks to the
//! API server:
//!
//! - `store`: the reflectors that feed `resolve`, and the published snapshot.
//! - `worker_sets`: `InferenceWorkerSet` to `PoolSpec`, the watch that
//!   reconciles them, and the store of rejected sets.
//! - `status`: publishes `status` on the policy resources.
//! - `binding`: publishes one `BackendBinding` per pool, so a gateway can
//!   resolve an `InferencePool` backendRef it has no schema for.

use serde::Deserialize;

pub mod binding;
pub mod status;
pub mod store;
pub mod worker_sets;

/// `[policy]`: routing policy from `kinference.buoyant.io` CRDs.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct PolicyConfig {
    /// Watch the CRDs and honor `PickRequest.inference_pool`. When off, a
    /// pool-naming request falls back to model-based routing. Requires the RBAC
    /// in `charts/kinference/templates/rbac.yaml`.
    #[serde(default)]
    pub enabled: bool,
    /// Namespace to watch; `None` watches all.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Publish one `BackendBinding` per `InferencePool`.
    #[serde(default = "default_true")]
    pub backend_bindings: bool,
    /// The `Extension` a published binding lists. Empty lists none, for routes
    /// carrying their own `extensionRef` filter. kinference doesn't create or
    /// read this object.
    #[serde(default = "default_binding_extension")]
    pub backend_binding_extension: String,
}

fn default_true() -> bool {
    true
}

fn default_binding_extension() -> String {
    "chat-picker".to_string()
}

impl PolicyConfig {
    /// The extension name to publish, or `None` when empty or whitespace.
    pub fn binding_extension(&self) -> Option<&str> {
        let name = self.backend_binding_extension.trim();
        (!name.is_empty()).then_some(name)
    }
}

// Not derived, so `backend_bindings` defaults to true like an absent `[policy]`.
impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            namespace: None,
            backend_bindings: default_true(),
            backend_binding_extension: default_binding_extension(),
        }
    }
}
