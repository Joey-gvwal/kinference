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

pub mod binding;
pub mod status;
pub mod store;
pub mod worker_sets;
