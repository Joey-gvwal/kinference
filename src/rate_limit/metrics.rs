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
/// startup rather than filling in as each case first occurs.
const OUTCOMES: [&str; 4] = ["allowed", "invalid", "commit_ok", "commit_not_found"];

pub(super) struct Metrics {
    /// `kinference_rate_limit_total{outcome}`.
    rate_limit: IntCounterVec,
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
        let rate_limit = IntCounterVec::new(
            Opts::new(
                "kinference_rate_limit_total",
                "RateLimiter Reserve/Commit calls, by outcome",
            ),
            &["outcome"],
        )
        .expect("rate_limit counter opts are valid");
        registry
            .register(Box::new(rate_limit.clone()))
            .expect("register rate_limit");
        // Seed every outcome at zero. This family is registered ONLY when
        // `[rate_limit] enabled = true`, so seeding sharpens the distinction a
        // reader actually needs: absent now means "the subsystem is off", and
        // an all-zero series means "on, and nothing has been limited". Without
        // it those two look the same on a dashboard.
        for outcome in OUTCOMES {
            rate_limit.with_label_values(&[outcome]);
        }
        Self { rate_limit }
    }

    /// One `Reserve` or `Commit`. This service sees every admitted request, so
    /// the counters are the ONLY per-call signal — the handler deliberately logs
    /// nothing, since a line per call would cost more than the limiting it
    /// stands in for.
    pub(super) fn record(&self, outcome: &str) {
        self.rate_limit.with_label_values(&[outcome]).inc();
    }
}
