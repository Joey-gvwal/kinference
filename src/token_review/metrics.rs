//! This subsystem's Prometheus metrics.
//!
//! Registered into the process-wide `Registry` that `crate::prom` owns and the
//! dashboard's `/metrics` route renders. Adding a metric here is one field,
//! one `register` call and one recorder — all in this file, none of them in a
//! struct five other subsystems share.
//!
//! Registration ORDER does not matter: `Registry::gather` collects families
//! into a map keyed by name, so the exposition text is byte-identical
//! regardless of where a family was registered.

use prometheus::{IntCounterVec, Opts, Registry};

/// Every `outcome` value [`Metrics::record`] can be called with, from the call
/// sites in `super`. Seeded at construction so the family is complete from
/// startup. Note the split this preserves: `denied` is an answered review,
/// while `transport` and `timeout` are failures to reach the apiserver at all
/// — an all-zero `denied` is meaningful, and it cannot be read as such if the
/// series is simply missing.
const OUTCOMES: [&str; 7] = [
    "authenticated",
    "denied",
    "cached_authenticated",
    "cached_denied",
    "invalid",
    "timeout",
    "transport",
];

pub(super) struct Metrics {
    /// `kinference_token_review_total{outcome}`.
    token_review: IntCounterVec,
}

impl Metrics {
    /// Registers this subsystem's families into `registry`.
    ///
    /// Borrows the registry rather than owning one, for the same reason
    /// `WorkerSetWatcher::run` takes its kube client instead of holding it: a
    /// test hands over `&Registry::new()` and needs nothing else.
    ///
    /// Panics if called twice against the same registry — `register` rejects a
    /// duplicate collector. That can only be a startup-wiring bug, and it is
    /// the same failure class as the `.expect` calls it replaces.
    pub(super) fn new(registry: &Registry) -> Self {
        let token_review = IntCounterVec::new(
            Opts::new(
                "kinference_token_review_total",
                "Kubernetes TokenReview calls, by outcome",
            ),
            &["outcome"],
        )
        .expect("token_review counter opts are valid");
        registry
            .register(Box::new(token_review.clone()))
            .expect("register token_review");
        for outcome in OUTCOMES {
            token_review.with_label_values(&[outcome]);
        }
        Self { token_review }
    }

    /// One TokenReview call. `outcome` separates a cached answer from a fresh
    /// one, and a DENIAL from a failure to determine — the cache-hit ratio and
    /// the apiserver error rate are different operational questions.
    pub(super) fn record(&self, outcome: &str) {
        self.token_review.with_label_values(&[outcome]).inc();
    }
}
