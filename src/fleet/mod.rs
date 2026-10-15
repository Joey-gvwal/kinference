//! The fleet: pools, workers, discovery, eviction, the scrape of engine
//! metrics, ZMQ ingest and in-flight state.

pub mod discovery;
pub mod eviction;
pub mod ingest;
pub mod pool;
pub mod scrape;
pub mod workers;
