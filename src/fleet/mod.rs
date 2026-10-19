//! The fleet: pools, workers, discovery, eviction, the scrape of engine
//! metrics, ZMQ ingest, in-flight state and the model index.

pub mod coordinator;
pub mod discovery;
pub mod eviction;
pub mod inflight;
pub mod ingest;
pub mod models;
pub mod pool;
pub mod profile;
pub mod rollup;
pub mod scrape;
pub mod workers;
