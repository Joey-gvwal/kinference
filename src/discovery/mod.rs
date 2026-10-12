//! Worker discovery layer.
//!
//! A `Discovery` is the source-of-truth for "which workers are live right
//! now." It emits `WorkerChange::{Added, Removed}` events on a channel; the
//! coordinator in `main.rs` reacts by registering/unregistering workers in
//! the various per-worker state stores and spawning/cancelling their
//! per-worker tasks.
//!
//! Two implementations:
//! - [`static_config::StaticDiscovery`] wraps the existing
//!   `Config.workers: Vec<WorkerConfig>`. Emits one Added per worker at
//!   startup, then waits on `cancel`. Bit-for-bit equivalent to the
//!   pre-discovery static-config behavior.
//! - `kubernetes::KubeDiscovery` (Phase D) watches Pods by label selector
//!   and emits Added/Removed as pods reach/leave Ready.

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::WorkerConfig;

pub mod kubernetes;
pub mod static_config;

/// One discovery event. Sent by a `Discovery` impl, consumed by the
/// coordinator in `main.rs`.
// `Added(WorkerConfig)` dwarfs `Removed(u64)`, but discovery events are rare
// and short-lived, so the size gap costs nothing; boxing the config would
// churn every constructor and match arm for it.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum WorkerChange {
    /// A new worker has appeared. The coordinator should register it in
    /// every state store and spawn its ZMQ listener + metrics poller.
    Added(WorkerConfig),
    /// A previously-Added worker is gone. The coordinator should cancel
    /// its per-worker tasks and unregister it from every state store.
    // Not constructed until Phase D adds `KubeDiscovery`.
    #[allow(dead_code)]
    Removed(u64),
}

/// A source of worker-membership events. Implementations run as a single
/// task driven by `tokio::spawn(discovery.run(...))`.
///
/// Contract:
/// - `run` MUST send an `Added` for each worker it knows about at startup
///   and any time a previously-removed worker reappears.
/// - `run` MUST send `Removed(worker_id_num)` exactly once per worker that
///   leaves the set. Duplicate `Removed` for an unknown worker is
///   tolerated (the coordinator drops them).
/// - `run` MUST exit cleanly when `cancel.cancelled()` fires.
/// - `run` MAY drop events if the channel is full (sender's responsibility
///   to back-pressure or coalesce). The default coordinator channel
///   capacity is 64, which is plenty for typical fleet sizes.
#[async_trait]
pub trait Discovery: Send + Sync + 'static {
    async fn run(
        self: std::sync::Arc<Self>,
        tx: mpsc::Sender<WorkerChange>,
        cancel: CancellationToken,
    );
}
