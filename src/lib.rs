//! kinference: a KV-cache-aware router for inference requests.
//!
//! The `kinference` binary (`src/main.rs`) runs [`server::run`], the only
//! public item. `kinfctl` is its own crate (`crates/kinfctl`), built on the
//! `kinference-policy` crate this one imports too.

mod admin;
mod config;
mod fleet;
mod grpc;
mod leader;
mod local_tokenize;
mod metrics;
mod mock;
mod observations;
mod policy;
mod proto;
mod router;
mod version;

pub mod server;
