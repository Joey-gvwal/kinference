//! Build-time version string surfaced by `kinfctl --version`.
//!
//! The same rule as the router's own `version` module, so a CLI and a server
//! built together report the same identity: `KINFERENCE_VERSION` when set,
//! else `CARGO_PKG_VERSION`, which both crates take from the workspace. The
//! `justfile` sets it for release builds, and the release workflow checks that
//! `--version` prints it.
//!
//! `option_env!` is a compile-time read; we use the `match` form so this
//! resolves to a `&'static str` constant (clap's `version = …` attribute
//! needs that). `unwrap_or` on `Option<&str>` is not const yet on stable.

pub const VERSION: &str = match option_env!("KINFERENCE_VERSION") {
    Some(v) if !v.is_empty() => v,
    _ => env!("CARGO_PKG_VERSION"),
};
