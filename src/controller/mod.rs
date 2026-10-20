//! The control plane: the CRD watch, the status and binding writers, the
//! worker-set reconcile, leader election and EndpointSlice discovery, all
//! behind `api`, the one door to the API server.

pub mod api;
pub mod discovery;
pub mod leader;
pub mod metrics;
pub mod policy;
