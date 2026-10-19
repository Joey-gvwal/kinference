//! The control plane: the CRD watch, the status and binding writers, the
//! worker-set reconcile and leader election.

pub mod leader;
pub mod metrics;
pub mod policy;
