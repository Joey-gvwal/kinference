//! kinference: a KV-cache-aware router for inference requests.
//!
//! This crate is the operator surface; the subsystems are the
//! `kinference-fleet`, `kinference-router`, `kinference-controller` and
//! `kinference-policy` crates. The `kinference` binary (`src/main.rs`) runs
//! [`server::run`], the only public item. `kinfctl` is its own crate.
//!
//! - `admin`: `/readyz`, `/metrics` and the read-only policy API.
//! - `config`: `Config`, load, validation, and the one read of the
//!   environment at startup.
//! - `metrics`: the one Prometheus registry and the text exposition.
//! - [`server`][]: `run`, the wiring from config to shutdown.
//! - `version`: the build version string.

mod admin;
mod config;
mod metrics;
mod version;

pub mod server;
