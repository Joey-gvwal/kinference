//! kinference: a KV-cache-aware router for inference requests.
//!
//! The `kinference` binary (`src/main.rs`) runs [`server::run`], the only
//! public item. `kinfctl` is its own crate (`crates/kinfctl`), built on the
//! `kinference-policy` crate this one re-exports as `policy`.

mod admin;
mod body_store;
mod config;
mod discovery;
mod eviction;
mod grpc;
mod inflight_prefixes;
mod inflight_registry;
mod ingest;
mod leader;
mod local_tokenize;
mod metrics;
mod mock;
mod monitor;
mod observation_sink;
mod observations;
mod pick_body;
mod picker;
mod policy;
mod pool;
mod pool_manager;
mod prom;
mod proto;
mod rollup;
mod tokenize;
mod tokenize_client;
mod version;
mod workers;

pub mod server;
