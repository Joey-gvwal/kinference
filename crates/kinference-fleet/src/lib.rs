//! The fleet: which workers exist and what state they are in.
//!
//! A worker set resolves to a `PoolSpec`; a [`pool::Pool`] is its runtime
//! state. The coordinator adds and drops workers from discovery and
//! eviction events, and each worker gets a scrape task and a ZMQ ingest
//! task. [`Fleet`] builds it all and [`FleetHandles`] is what the router and
//! the control plane read. The fleet holds no Kubernetes client: discovery
//! is a task the server supplies through [`discovery::DiscoveryFactory`].
//!
//! - [`coordinator`][]: the worker lifecycle loop, and the hooks that start
//!   and stop a pool's discovery.
//! - [`discovery`][]: the discovery config types, `WorkerChange`, and the
//!   factory the server supplies.
//! - [`error`][]: the fleet's error type.
//! - [`eviction`][]: the channel that asks the coordinator to drop a worker.
//! - [`gauges`][]: the Prometheus collector for worker health, saturation and
//!   index size, computed at gather.
//! - [`inflight`][]: per-endpoint picks not yet observed, and prefixes
//!   dispatched but not yet in the index.
//! - [`ingest`][]: the ZMQ KV-event listener per worker, replay, and
//!   block-size checks.
//! - [`metrics`][]: `FleetMetrics`, and the helpers every crate registers its
//!   families with.
//! - [`models`][]: `ModelIndex`, which pools serve each model.
//! - [`pool`][]: `Pool`, `WorkerContext`, `PoolRegistry` and `PoolManager`.
//! - [`profile`][]: the tuning a pool carries, as data only.
//! - [`scrape`][]: the scrape of worker `/metrics` and `/v1/models`, and the
//!   registry of samples.
//! - [`start`][]: `Fleet`, which builds and runs the above.
//! - `sync`: lock helpers that recover from poisoning.
//! - [`workers`][]: `WorkerRegistry`, per-worker entries, served models,
//!   readiness.

pub mod coordinator;
pub mod discovery;
pub mod error;
pub mod eviction;
pub mod gauges;
pub mod inflight;
pub mod ingest;
pub mod metrics;
pub mod models;
pub mod pool;
pub mod profile;
pub mod scrape;
pub mod start;
mod sync;
pub mod workers;

pub use error::Error;
pub use start::{Fleet, FleetConfig, FleetHandles};
