//! Prometheus exposition for kinference's own metrics, served on `GET /metrics`
//! by the admin listener.
//!
//! Unrelated to `src/fleet/scrape.rs`, which scrapes each worker's `/metrics`.
//!
//! `PromMetrics` owns the one `Registry`. Each group registers its own
//! families in it: `fleet::metrics`, `router::metrics`, `controller::metrics`.

use prometheus::{Encoder, Registry, TextEncoder};

use crate::fleet::metrics::int_gauge_vec;

/// The registry behind `/metrics`. Clone-free: share via `Arc`.
pub struct PromMetrics {
    registry: Registry,
}

impl PromMetrics {
    /// A registry carrying `kinference_build_info`.
    pub fn new() -> Self {
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
        Self { registry }
    }

    /// The shared exposition registry, lent to subsystems that own their own
    /// metrics.
    ///
    /// Registration order doesn't affect [`Self::render`], which is sorted by
    /// metric name.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Render the current metric set in Prometheus text-exposition
    /// format. Cheap enough to run per scrape.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        // Unreachable in practice: writing to a Vec doesn't fail.
        if encoder.encode(&self.registry.gather(), &mut buf).is_err() {
            return String::new();
        }
        String::from_utf8(buf).unwrap_or_default()
    }

    /// The Prometheus text-format content type, as
    /// `TextEncoder::format_type()` returns.
    pub fn content_type() -> &'static str {
        "text/plain; version=0.0.4"
    }
}

impl Default for PromMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
