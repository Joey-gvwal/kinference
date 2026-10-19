//! The fleet: pools, workers, discovery, eviction, the scrape of engine
//! metrics, ZMQ ingest, in-flight state and the model index.

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
mod sync;
pub mod workers;
