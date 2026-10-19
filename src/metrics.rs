//! Prometheus exposition for kinference's own metrics, served on `GET /metrics`
//! by the admin listener.
//!
//! Unrelated to `kinference_fleet::scrape`, which scrapes each worker's `/metrics`.
//!
//! The server creates the one `Registry` here and lends it to each group,
//! which registers its own families: `fleet::metrics`, `router::metrics`,
//! `controller::metrics`.

use prometheus::{Encoder, Registry, TextEncoder};

use kinference_fleet::metrics::int_gauge_vec;

/// The Prometheus text-format content type, as `TextEncoder::format_type()`
/// returns.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// A registry carrying `kinference_build_info`. `Registry` is `Clone` and
/// shares its collectors, so every holder sees every group's families.
pub fn registry() -> Registry {
    let registry = Registry::new();
    // Build identity, the conventional Prometheus shape: a gauge pinned to
    // 1 whose labels carry the information, for `group_left` joins. Set
    // here since `VERSION` is a compile-time constant.
    let build_info = int_gauge_vec(
        &registry,
        "kinference_build_info",
        "Build identity of this router process. Always 1; read the labels. \
         Matches the `--version` output, so a scrape and a human agree on \
         which build is running",
        &["version"],
    );
    build_info
        .with_label_values(&[crate::version::VERSION])
        .set(1);
    registry
}

/// Render the current metric set in Prometheus text-exposition format.
/// Sorted by metric name, so registration order does not show. Cheap enough
/// to run per scrape.
pub fn render(registry: &Registry) -> String {
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    // Unreachable in practice: writing to a Vec doesn't fail.
    if encoder.encode(&registry.gather(), &mut buf).is_err() {
        return String::new();
    }
    String::from_utf8(buf).unwrap_or_default()
}

#[cfg(test)]
mod tests;
