//! kinference — a KV-cache-aware router for inference requests.
//!
//! The server binary, `kinference` (`src/main.rs`), runs [`server::run`].
//! `kinfctl`, the command-line tool for checking `InferencePolicy` manifests
//! before they are applied, is its own crate (`crates/kinfctl`), built on the
//! same `kinference-policy` crate this one re-exports as `policy`.
//!
//! Every other module is private to the crate. The entry point is the only
//! public surface, which is what lets the server's internals stay
//! `pub(crate)`-free without the binary reaching past what it needs.

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
mod telemetry;
mod tokenize;
mod tokenize_client;
mod version;
mod workers;

pub mod server;
