//! Build-time version string surfaced by `--version` and the
//! `kinference_build_info` metric.
//!
//! `KINFERENCE_VERSION` when set, else `CARGO_PKG_VERSION`. The `justfile`
//! and the Dockerfile set `KINFERENCE_VERSION` from the image tag, so images
//! report the same identity as their tag.
//!
//! The `match` form keeps this a `&'static str` constant, which clap's
//! `version` attribute needs; `Option::unwrap_or` isn't const on stable.

pub const VERSION: &str = match option_env!("KINFERENCE_VERSION") {
    Some(v) if !v.is_empty() => v,
    _ => env!("CARGO_PKG_VERSION"),
};
