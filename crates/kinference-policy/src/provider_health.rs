//! What kinference learns about a provider from the caller, and nothing else.
//!
//! kinference never talks to a provider or reads its `GatewayBackend`, so
//! everything here arrives from the caller, over `ReportFirstToken` and
//! `Abort`:
//!
//! - Latency, as a trailing-window p95 of caller-reported TTFT, used by
//!   `EstimatedTTFT` and `backend.observedTTFTP95` predicates.
//! - Whether it works at all. A dangling `backendRef` or a bad credential is
//!   invisible to kinference, so without these reports a misconfigured provider
//!   would keep winning picks.
//!
//! Repeated failures suppress a provider briefly, per replica. Suppression
//! expires on its own, since recovery is also invisible here.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use super::engine::ObjectKey;

/// Samples kept per provider for the p95. Small, so the figure tracks recent
/// behavior.
const WINDOW: usize = 128;

/// How long a sample stays in the window.
const SAMPLE_TTL: Duration = Duration::from_secs(300);

/// Consecutive dispatch failures before a provider stops producing candidates.
///
/// Consecutive, not a rate: one success resets the count, so a merely flaky
/// provider keeps serving.
const FAILURES_BEFORE_SUPPRESS: u32 = 3;

/// How long suppression lasts. Short, since the only way to detect recovery is
/// to try again.
const SUPPRESS_FOR: Duration = Duration::from_secs(30);

/// Why a provider stopped being dispatched to. Ordered by how actionable it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// `Abort(BACKEND_UNRESOLVED)`: the caller could not resolve the
    /// `GatewayBackend`.
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
    /// A configuration error is reported as `ResolvedRefs=False`, an upstream
    /// error as `Ready=False`.
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
    /// The most recent failure, retained past suppression for status.
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
/// generation.
#[derive(Debug, Default)]
pub struct ProviderHealthStore {
    inner: RwLock<HashMap<ObjectKey, Entry>>,
}

impl ProviderHealthStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// A successful first token with a usable TTFT sample. Streaming responses
    /// only; see [`Self::record_success_untimed`].
    pub fn record_success(&self, provider: &ObjectKey, ttft_ms: f32) {
        let now = Instant::now();
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let e = g.entry(provider.clone()).or_default();
        e.samples.push((now, ttft_ms));
        prune(&mut e.samples, now);
        clear_failures(e);
    }

    /// A success whose timing is not a time-to-first-token.
    ///
    /// For a non-streaming response the first byte arrives at completion, so
    /// recording it would inflate the p95. The failure streak still clears.
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
            // Failures older than the sample window are no longer reported.
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

/// Recovery: a provider that just worked is neither failing nor suppressed.
fn clear_failures(e: &mut Entry) {
    e.consecutive_failures = 0;
    e.suppressed_until = None;
}

/// Nearest-rank p95 over the live samples.
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
    // Nearest-rank, so the result is a value a request actually saw.
    let idx = (((live.len() as f64) * 0.95).ceil() as usize).saturating_sub(1);
    Some(live[idx.min(live.len() - 1)])
}

#[cfg(test)]
mod tests;
