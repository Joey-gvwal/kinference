//! Build-time version string surfaced by `--version`, the dashboard, and
//! `/api/snapshot`'s `version` field.
//!
//! Falls back to `CARGO_PKG_VERSION` (i.e. whatever's in `Cargo.toml`)
//! when the env var isn't set — which is the case for local
//! `cargo build` invocations. The Makefile in
//! `k8s/kinference/` sets `KINFERENCE_VERSION=$(TAG)` so
//! container images advertise the same identity as their tag.
//!
//! `option_env!` is a compile-time read; we use the `match` form so this
//! resolves to a `&'static str` constant (clap's `version = …` attribute
//! needs that). `unwrap_or` on `Option<&str>` is not const yet on stable.

pub const VERSION: &str = match option_env!("KINFERENCE_VERSION") {
    Some(v) if !v.is_empty() => v,
    _ => env!("CARGO_PKG_VERSION"),
};
