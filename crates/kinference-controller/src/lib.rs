//! The control plane: the CRD watch, the status and binding writers, the
//! worker-set reconcile, leader election and EndpointSlice discovery, all
//! behind `api`, the one door to the API server. `start` runs it.

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
