//! kinference: a KV-cache-aware router for inference requests.
//!
//! The `kinference` binary (`src/main.rs`) runs [`server::run`], the only
//! public item. `kinfctl` is its own crate (`crates/kinfctl`), built on the
//! `kinference-policy` crate this one imports too.

mod admin;
mod config;
mod controller;
mod metrics;
mod router;
mod version;

pub mod server;
