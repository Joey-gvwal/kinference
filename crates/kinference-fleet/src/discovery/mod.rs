//! Worker discovery layer.
//!
//! A `Discovery` is the source of truth for which workers in one worker set
//! are live. It emits `WorkerChange::{Added, Removed}` events;
//! `fleet::coordinator` registers or unregisters each worker and spawns or
//! cancels its per-worker tasks.
//!
//! One implementation, `controller::discovery::KubeDiscovery`, watches a
//! Service's EndpointSlices and emits `Added`/`Removed` as endpoints become
//! or stop being Ready. The server builds it through a [`DiscoveryFactory`],
//! so the fleet never holds a Kubernetes client.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// One discovery event, consumed by `fleet::coordinator`.
// `Added(WorkerConfig)` dwarfs `Removed(u64)`, but discovery events are rare,
// so boxing isn't worth it.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum WorkerChange {
    /// A new worker has appeared. The coordinator should register it in
    /// every state store and spawn its ZMQ listener + metrics poller.
    Added(WorkerConfig),
    /// A previously-Added worker is gone. The coordinator should cancel
    /// its per-worker tasks and unregister it from every state store.
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
/// - `run` MAY drop events if the channel is full. The coordinator channel
///   holds 64.
#[async_trait]
pub trait Discovery: Send + Sync + 'static {
    async fn run(
        self: std::sync::Arc<Self>,
        tx: mpsc::Sender<WorkerChange>,
        cancel: CancellationToken,
    );
}

/// Builds one pool's Kubernetes discovery from its config. The server binds
/// the in-cluster client once; the fleet never sees `kube`.
pub type DiscoveryFactory = Arc<dyn Fn(DiscoveryConfig) -> Arc<dyn Discovery> + Send + Sync>;

/// `[workerset.discovery]`: where a pool's workers come from.
#[derive(Debug, Clone, Deserialize, Default, PartialEq)]
pub struct DiscoveryConfig {
    /// Namespace of the Service; `None` means all namespaces.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Service fronting the workers. Each Ready endpoint in its EndpointSlices
    /// becomes a worker, dialed by pod IP, so any Service type works. Required.
    #[serde(default)]
    pub service_name: Option<String>,
    /// `namespace/name` of the owning `InferenceWorkerSet`, for logs only,
    /// since two sets can share a Service.
    #[serde(skip)]
    pub workerset: Option<String>,
    /// Last-resort ports, used when the Pod has neither a matching named port
    /// nor a `kinference.buoyant.io/*` annotation.
    #[serde(default)]
    pub defaults: DiscoveryDefaults,
    /// Explicit ports from an `InferenceWorkerSet`. Always empty for TOML
    /// worker sets.
    #[serde(skip)]
    pub ports: PortRefs,
}

/// Explicit per-role ports. `None` means resolve by conventional name, then
/// fall back to [`DiscoveryDefaults`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PortRefs {
    pub http: Option<PortRef>,
    pub metrics: Option<PortRef>,
    pub zmq: Option<PortRef>,
    pub replay: Option<PortRef>,
    /// The workers publish no KV events, so no SUB socket is opened. Unlike
    /// `zmq: None`, which falls back to a default port: a ZMQ `connect` to a
    /// port nobody publishes on succeeds and stays silent forever.
    pub kv_events_declared_absent: bool,
}

/// A port named by a CR, either by Service port name or by number.
///
/// Names are matched against the EndpointSlice's ports. A number is a Service
/// port, but kinference dials pods directly, so it is matched against the
/// slice's target ports first and only used verbatim (with a warning) if
/// nothing matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortRef {
    Name(String),
    Number(u16),
}

/// Last-resort per-pod ports. See [`DiscoveryConfig::defaults`].
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DiscoveryDefaults {
    /// OpenAI-compatible HTTP port, also used for `/metrics`.
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    /// ZMQ PUB port for KV-cache events.
    #[serde(default = "default_zmq_port")]
    pub zmq_port: u16,
    /// ZMQ DEALER replay port. `None` disables replay unless a named port or
    /// annotation supplies one.
    #[serde(default)]
    pub replay_port: Option<u16>,
}

impl Default for DiscoveryDefaults {
    fn default() -> Self {
        Self {
            http_port: default_http_port(),
            zmq_port: default_zmq_port(),
            replay_port: None,
        }
    }
}

fn default_http_port() -> u16 {
    8000
}

fn default_zmq_port() -> u16 {
    5557
}

#[derive(Debug, Clone, Default)]
pub struct WorkerConfig {
    pub worker_id_str: String,
    pub worker_id_num: u64,
    /// ZMQ PUB endpoint for KV-cache events. `None` when the set declares no
    /// KV events.
    pub zmq_endpoint: Option<String>,
    /// DEALER endpoint for replaying event history from seq=0 at startup.
    pub replay_endpoint: Option<String>,
    /// The engine's Prometheus `/metrics` URL.
    pub metrics_url: Option<String>,
    /// Base URL (no path) of the engine's OpenAI-compatible API, used for
    /// scrapes and tokenization, and as the fallback for `endpoint`.
    pub http_url: Option<String>,
    /// Routable `ip:port` returned to clients (`WorkerScore.endpoint` and
    /// `x-gateway-destination-endpoint`). Takes precedence over `http_url`'s
    /// `host:port`; needed when `http_url` uses a hostname, since the gateways
    /// don't resolve names here.
    pub endpoint: Option<String>,
    pub model_name: String,

    // Placement metadata. Not used yet.
    pub pod_name: Option<String>,
    pub namespace: Option<String>,
    pub node_name: Option<String>,
    pub node_ip: Option<String>,
    pub pod_ip: Option<String>,
    pub availability_zone: Option<String>,
    pub instance_type: Option<String>,
}
