//! The control plane: everything that talks to the Kubernetes API server.
//!
//! [`start`](mod@start) connects, then runs the CRD watch, the worker-set
//! reconcile, the status and binding writers and the elector. The one crate
//! that depends on `kube`; every call goes through [`api::ApiServer`], which
//! has a live implementation and a recorded one for tests. Only the leader
//! writes.
//!
//! - [`api`][]: `ApiServer`, the one door to the API server; `KubeApi` over a
//!   client, `Recorded` over objects a test supplies.
//! - [`discovery`][]: `KubeDiscovery`, the EndpointSlice watch the fleet runs,
//!   and the factory that binds the in-cluster client into it.
//! - [`error`][]: the control plane's error type.
//! - [`leader`][]: Lease-based leader election.
//! - [`metrics`][]: `ControllerMetrics`.
//! - [`policy`][]: the CRD watch that feeds the snapshot, the worker-set
//!   reconcile, the status writer and the binding writer.
//! - [`start`](mod@start): `start` and `Controller`, whose `shutdown` waits
//!   for the Lease to be vacated.
//! - `sync`: lock helpers that recover from poisoning.

pub mod api;
pub mod discovery;
pub mod error;
pub mod leader;
pub mod metrics;
pub mod policy;
pub mod start;
mod sync;

pub use error::Error;
pub use start::{ControllerConfig, ControllerHandles, start};
