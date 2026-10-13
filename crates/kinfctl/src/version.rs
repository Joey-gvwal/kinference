//! Build-time version string surfaced by `kinfctl --version`.
//!
//! Same rule as the router's `version` module, so a CLI and a server built
//! together report the same identity: `KINFERENCE_VERSION` when set, else
//! `CARGO_PKG_VERSION` (from the workspace). The `justfile` sets it for
//! release builds, and the release workflow checks that `--version` prints it.
//!
//! The `match` form keeps this a `&'static str` constant, which clap's
//! `version` attribute needs; `Option::unwrap_or` isn't const on stable.

pub const VERSION: &str = match option_env!("KINFERENCE_VERSION") {
    Some(v) if !v.is_empty() => v,
    _ => env!("CARGO_PKG_VERSION"),
};
