//! kinference: a KV-cache-aware router for inference requests.
//!
//! The `kinference` binary (`src/main.rs`) runs [`server::run`], the only
//! public item. `kinfctl` is its own crate (`crates/kinfctl`), built on the
//! `kinference-policy` crate this one imports too.

mod admin;
mod body_store;
mod config;
mod eviction;
mod fleet;
mod grpc;
mod inflight;
mod ingest;
mod leader;
mod local_tokenize;
mod metrics;
mod mock;
mod observations;
mod pick_body;
mod picker;
mod policy;
mod proto;
mod rollup;
mod scrape;
mod service;
mod tokenize;
mod tokenize_client;
mod version;

pub mod server;
