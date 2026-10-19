//! The control plane: the CRD watch, the status and binding writers, the
//! worker-set reconcile, leader election and EndpointSlice discovery.

pub mod discovery;
pub mod leader;
pub mod metrics;
pub mod policy;
