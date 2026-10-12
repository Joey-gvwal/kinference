//! Worker eviction signaling and teardown.
//!
//! Two pathways funnel into the same teardown sequence:
//!
//! 1. **Discovery removal** — `WorkerChange::Removed` from the static
//!    or kubernetes discovery source. The pod went away on its own.
//! 2. **Homogeneity violation** — the ingest path observes an engine
//!    reporting a different `block_size` than the fleet-wide
//!    configured value, or the metrics path observes a `/v1/models`
//!    list that disagrees with the canonical set. In both cases the
//!    misbehaving worker needs to be removed so the rest of the pool
//!    keeps serving on a single set of coherent assumptions.
//!
//! Subsystems (ingest listener, metrics poller) request eviction by
//! sending an [`EvictionRequest`] on the shared
//! [`tokio::sync::mpsc::UnboundedSender`]. The main coordinator owns
//! the receiver and drives the actual teardown — this keeps
//! lifecycle logic in a single place and avoids each subsystem
//! reaching into shared state on its own.
//!
//! [`EvictionReason`] carries a human-readable description so the
//! INFO line emitted from `evict_worker` (in `main.rs`) tells the
//! operator exactly why a worker disappeared.

use tokio::sync::mpsc;

/// One worker to evict, with the reason that triggered it.
#[derive(Debug, Clone)]
pub struct EvictionRequest {
    pub worker_id_num: u64,
    pub reason: EvictionReason,
}

/// Why a worker is being evicted.
#[derive(Debug, Clone)]
pub enum EvictionReason {
    /// The configured fleet-wide `kv_block_size` does not match the
    /// engine's reported value seen on the first `BlockStored`
    /// event. Carrier of the two numbers so the WARN log makes the
    /// fix obvious.
    BlockSizeMismatch { expected: u32, observed: u32 },
    /// The worker's `/v1/models` list differs from the canonical
    /// set pinned by the first non-empty report from the pool.
    /// Carrier of both lists for diff-printing.
    ModelMismatch {
        expected: Vec<String>,
        observed: Vec<String>,
    },
}

impl EvictionReason {
    /// The `reason` label on `kinference_worker_evictions_total`.
    ///
    /// Separate from `Display`, which carries the two mismatched VALUES so the
    /// WARN log names the fix. Those values are unbounded, so they must not
    /// reach a label.
    pub fn as_label(&self) -> &'static str {
        match self {
            EvictionReason::BlockSizeMismatch { .. } => "block_size_mismatch",
            EvictionReason::ModelMismatch { .. } => "model_mismatch",
        }
    }
}

impl std::fmt::Display for EvictionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionReason::BlockSizeMismatch { expected, observed } => write!(
                f,
                "engine block_size={observed} differs from fleet-wide configured kv_block_size={expected}"
            ),
            EvictionReason::ModelMismatch { expected, observed } => write!(
                f,
                "served models {observed:?} differ from canonical fleet models {expected:?}"
            ),
        }
    }
}

/// Type alias for the eviction channel's sender. Each subsystem
/// that can request an eviction (ingest listener, metrics poller)
/// holds a clone.
pub type EvictionTx = mpsc::UnboundedSender<EvictionRequest>;

/// Type alias for the eviction channel's receiver. The main
/// coordinator holds one and drains it inside its select! loop.
pub type EvictionRx = mpsc::UnboundedReceiver<EvictionRequest>;

/// Build a fresh eviction channel. Called once in `main()`.
pub fn channel() -> (EvictionTx, EvictionRx) {
    mpsc::unbounded_channel()
}
