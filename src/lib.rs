//! kinference — a KV-cache-aware router for inference requests.
//!
//! One library, two binaries:
//!
//! - `kinference` (`src/main.rs`), the server, runs [`server::run`].
//! - `kinfctl` (`src/bin/kinfctl.rs`), the command-line tool for checking
//!   `InferencePolicy` manifests before they are applied, runs [`cli::main`].
//!
//! Every other module is private to the crate. The two entry points are the
//! only public surface, which is what lets the server's internals stay
//! `pub(crate)`-free without either binary reaching past what it needs.

mod body_store;
mod config;
mod dashboard;
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

pub mod cli;
pub mod server;
