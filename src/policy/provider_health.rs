//! What kinference learns about a provider from the caller, and nothing else.
//!
//! A local endpoint is observable: kinference scrapes its metrics, subscribes to its
//! KV events, and counts its own dispatches. A provider is not. kinference does not
//! read the `GatewayBackend`, holds no credential, and never speaks to the
//! vendor — so every fact in this module arrives second-hand, over
//! `ReportFirstToken` and `Abort`.
//!
//! Two facts, and they are the only ones a provider has:
//!
//! - **Latency**, as a trailing-window p95 of caller-reported TTFT. Published as
//!   `status.observed.observedTTFTP95` and read by `EstimatedTTFT`. An aggregate,
//!   with no visibility into the vendor's queue, which is why the CRD calls it a
//!   gate rather than a ranking key and why tiering exists as the alternative.
//! - **Whether it works at all.** This is the load-bearing one, and it exists
//!   because of the split: a dangling `backendRef`, a Secret with the wrong key,
//!   a revoked API key, a `ReferenceGrant` the caller declines to honor — kinference
//!   cannot see ANY of these. Without the caller reporting them, a misconfigured
//!   provider keeps winning picks forever while every object kinference can read looks
//!   healthy. There is no dispatch retry anywhere in this design, so every one of
//!   those picks is a failed request.
//!
//! Suppression is deliberately local to the replica and short. It is not a
//! circuit breaker with a considered failure model — it is the minimum that stops
//! a broken destination from being a black hole, and it has to expire on its own
//! because the recovery signal (the ref now resolves) is invisible here too.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use super::engine::ObjectKey;

/// Samples kept per provider for the p95. Small on purpose: this is a gate, and
/// a long window would hold a stale figure across a vendor's own bad patch.
const WINDOW: usize = 128;

/// How long a sample stays in the window. Stated because an aggregate with no
/// window is uninterpretable — and because `EstimatedTTFT` ranks a per-request
/// prediction against it, so a reader has to know what they are comparing.
const SAMPLE_TTL: Duration = Duration::from_secs(300);

/// Consecutive dispatch failures before a provider stops producing candidates.
///
/// Consecutive, not a rate: a single 500 from a busy vendor is noise, while three
/// in a row with nothing in between is a destination that is not working. One
/// success resets the count, so a provider that is merely flaky keeps serving.
const FAILURES_BEFORE_SUPPRESS: u32 = 3;

/// How long suppression lasts. Short, because recovery is unobservable from here
/// — the only way to find out that a fixed `backendRef` now resolves is to try it
/// again.
const SUPPRESS_FOR: Duration = Duration::from_secs(30);

/// Why a provider stopped being dispatched to. Ordered by how actionable it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// `Abort(BACKEND_UNRESOLVED)` — the caller could not resolve the
    /// `GatewayBackend` at all. A configuration error, and the one kinference is
    /// structurally blind to.
    Unresolved,
    /// `Abort(BACKEND_UNAUTHORIZED)`, or a 401/403 from the vendor.
    Unauthorized,
    /// A 5xx from the vendor, or no response at all.
    Upstream,
}

impl FailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unresolved => "BackendUnresolved",
            Self::Unauthorized => "BackendUnauthorized",
            Self::Upstream => "UpstreamError",
        }
    }

    /// Whether an operator has to change something for this to clear.
    ///
    /// Drives the condition on the CR: a config error is reported as
    /// `ResolvedRefs=False`, which says "this object is wrong", while an upstream
    /// error is `Ready=False`, which says "the far end is unwell". Sending an
    /// operator to edit a manifest over a vendor's outage wastes their time, and
    /// the reverse leaves a typo looking like someone else's problem.
    pub fn is_configuration_error(self) -> bool {
        matches!(self, Self::Unresolved | Self::Unauthorized)
    }
}

#[derive(Debug, Default)]
struct Entry {
    /// `(when, ttft_ms)`, oldest first.
    samples: Vec<(Instant, f32)>,
    consecutive_failures: u32,
    /// Set while this provider is producing no candidates.
    suppressed_until: Option<Instant>,
    /// The most recent failure, retained past suppression so status can say what
    /// went wrong rather than only that something did.
    last_failure: Option<(Instant, FailureKind, String)>,
}

/// What a provider's status should say, derived rather than stored.
#[derive(Debug, Clone)]
pub struct ProviderHealth {
    pub suppressed: bool,
    pub last_failure: Option<(FailureKind, String)>,
}

/// Per-provider caller feedback.
///
/// One lock over a small map, taken on the feedback path and once per candidate
/// generation. Not a `DashMap`: the status writer iterates every provider each
/// tick, and a torn view would publish one provider's health against another's.
#[derive(Debug, Default)]
pub struct ProviderHealthStore {
    inner: RwLock<HashMap<ObjectKey, Entry>>,
}

impl ProviderHealthStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// A successful first token WITH a usable TTFT sample.
    ///
    /// Streaming responses only. See [`Self::record_success_untimed`] for why
    /// the two are separate.
    pub fn record_success(&self, provider: &ObjectKey, ttft_ms: f32) {
        let now = Instant::now();
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let e = g.entry(provider.clone()).or_default();
        e.samples.push((now, ttft_ms));
        prune(&mut e.samples, now);
        clear_failures(e);
    }

    /// A success whose timing is NOT a time-to-first-token.
    ///
    /// For a non-streaming response the caller's first byte arrives at
    /// completion, so the interval it reports is total latency. Folding that
    /// into `samples` would inflate the p95 — and that p95 is not merely
    /// published: `EstimatedTTFT` RANKS providers by it and
    /// `backend.observedTTFTP95` predicates compare against it, so a vendor
    /// serving non-streaming traffic would lose picks it should win and could
    /// be excluded outright.
    ///
    /// The recovery half still applies, and that is the whole reason this
    /// exists rather than simply skipping the call: the request demonstrably
    /// worked, so the failure streak must clear. Without it a provider serving
    /// only non-streaming traffic would stay suppressed forever after one
    /// transient failure.
    ///
    /// A provider with no timed sample at all stays eligible under
    /// `EstimatedTTFT` and sorts last — see the `unobserved_provider` case in
    /// `policy::engine` — so contributing no sample degrades gracefully instead
    /// of removing the provider from consideration. The end-to-end figure is
    /// still recorded, by `kinference_request_duration_ms`.
    pub fn record_success_untimed(&self, provider: &ObjectKey) {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let e = g.entry(provider.clone()).or_default();
        clear_failures(e);
    }

    /// A failed dispatch. Returns `true` when this one crossed the threshold, so
    /// the caller can log the transition once rather than on every failure.
    pub fn record_failure(&self, provider: &ObjectKey, kind: FailureKind, detail: &str) -> bool {
        let now = Instant::now();
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let e = g.entry(provider.clone()).or_default();
        e.consecutive_failures = e.consecutive_failures.saturating_add(1);
        e.last_failure = Some((now, kind, detail.to_string()));
        if e.consecutive_failures >= FAILURES_BEFORE_SUPPRESS {
            let already = e.suppressed_until.is_some_and(|t| t > now);
            e.suppressed_until = Some(now + SUPPRESS_FOR);
            return !already;
        }
        false
    }

    /// Trailing-window p95 in milliseconds, or `None` with no samples.
    ///
    /// Returned as a number here and formatted only at the status boundary, so
    /// the ranking path never parses a string it just printed.
    pub fn observed_ttft_ms(&self, provider: &ObjectKey) -> Option<f32> {
        let now = Instant::now();
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let e = g.get(provider)?;
        p95(&e.samples, now)
    }

    /// Whether this provider should currently produce no candidates.
    pub fn is_suppressed(&self, provider: &ObjectKey) -> bool {
        let now = Instant::now();
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        g.get(provider)
            .and_then(|e| e.suppressed_until)
            .is_some_and(|t| t > now)
    }

    /// Everything the status writer needs for one provider.
    pub fn health(&self, provider: &ObjectKey) -> ProviderHealth {
        let now = Instant::now();
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let Some(e) = g.get(provider) else {
            return ProviderHealth {
                suppressed: false,
                last_failure: None,
            };
        };
        ProviderHealth {
            suppressed: e.suppressed_until.is_some_and(|t| t > now),
            // Retained past suppression, but not forever: a failure older than
            // the sample window is history, and reporting it would leave a
            // recovered provider looking broken.
            last_failure: e
                .last_failure
                .as_ref()
                .filter(|(at, _, _)| now.duration_since(*at) < SAMPLE_TTL)
                .map(|(_, kind, detail)| (*kind, detail.clone())),
        }
    }

    /// Drop providers with nothing left to say, so a deleted CR does not pin an
    /// entry. Called from the same sweep as the other TTL'd maps.
    pub fn sweep(&self) -> usize {
        let now = Instant::now();
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let before = g.len();
        g.retain(|_, e| {
            prune(&mut e.samples, now);
            !e.samples.is_empty()
                || e.suppressed_until.is_some_and(|t| t > now)
                || e.consecutive_failures > 0
                || e.last_failure
                    .as_ref()
                    .is_some_and(|(at, _, _)| now.duration_since(*at) < SAMPLE_TTL)
        });
        before - g.len()
    }
}

fn prune(samples: &mut Vec<(Instant, f32)>, now: Instant) {
    samples.retain(|(at, _)| now.duration_since(*at) < SAMPLE_TTL);
    if samples.len() > WINDOW {
        let drop = samples.len() - WINDOW;
        samples.drain(..drop);
    }
}

/// Nearest-rank p95 over the live samples.
/// Recovery: a provider that just worked is neither failing nor suppressed.
///
/// Shared by both success paths so a timed and an untimed success cannot drift
/// apart on what "recovered" means.
fn clear_failures(e: &mut Entry) {
    e.consecutive_failures = 0;
    e.suppressed_until = None;
}

fn p95(samples: &[(Instant, f32)], now: Instant) -> Option<f32> {
    let mut live: Vec<f32> = samples
        .iter()
        .filter(|(at, _)| now.duration_since(*at) < SAMPLE_TTL)
        .map(|(_, ms)| *ms)
        .collect();
    if live.is_empty() {
        return None;
    }
    live.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    // Nearest-rank rather than interpolated: with a handful of samples an
    // interpolated p95 invents a value between two observations, and this figure
    // is meant to be a real one a request actually saw.
    let idx = (((live.len() as f64) * 0.95).ceil() as usize).saturating_sub(1);
    Some(live[idx.min(live.len() - 1)])
}

#[cfg(test)]
mod tests;
