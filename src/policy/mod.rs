//! The router side of CRD-driven routing policy.
//!
//! The resource types and the pure policy logic live in the
//! `kinference-policy` crate, which `kinfctl` shares. They are re-exported here
//! so `crate::policy::…` names both halves; see that crate's header for how
//! policy relates to the picker and where the topology comes from.
//!
//! ## Layout
//!
//! - `store` — the reflectors that feed `resolve`, and the published snapshot.
//! - `worker_sets` — `InferenceWorkerSet` → `PoolSpec`, plus the watch that
//!   reconciles them and the store of the ones that were rejected. Lives here
//!   rather than at the crate root because its input is a CRD type and its
//!   output is consumed only by `store` and `status`; as a top-level module it
//!   formed a cycle with them.
//! - `status` — the controller side.
//! - `binding` — the other half of the controller side: one `BackendBinding`
//!   per pool, which is what lets a gateway resolve an `InferencePool`
//!   backendRef it has no schema for.

pub use kinference_policy::*;

pub mod binding;
pub mod status;
pub mod store;
pub mod worker_sets;
