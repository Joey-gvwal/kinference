//! Provider feedback: what the caller reports back about an off-cluster
//! destination.
//!
//! kinference never talks to a provider; the caller does, and reports back
//! over the feedback RPCs. `record_provider_pick` writes the labels that the
//! report handlers read back.
//!
//! `record_provider_abort` is `service`'s one direct dependency on the proto
//! types, kept in this small file.

use std::time::Instant;

use super::{KinferenceService, ProviderPick, Qos};

impl KinferenceService {
    /// Stash what a provider pick needs for its feedback to land.
    ///
    /// Only the labels map: there is no `PendingObservation` (built from
    /// per-endpoint scoring state) and nothing pool-scoped for a provider.
    ///
    /// `qos`, `tenant` and `streaming` describe the request, not the
    /// [`ProviderPick`]. `streaming` lets
    /// [`Self::record_provider_first_token`] tell a real TTFT from a
    /// non-streaming response's total latency.
    pub(super) fn record_provider_pick(
        &self,
        response_id: u64,
        pick: &ProviderPick,
        qos: Qos,
        tenant: &str,
        streaming: bool,
    ) {
        self.request_labels.insert(
            response_id,
            crate::observations::RequestMeta {
                // The model the body names, as for a local pick.
                model: pick.upstream_model.clone(),
                qos,
                tenant: tenant.to_string(),
                streaming,
                picked_at: Instant::now(),
                provider: Some(pick.provider.clone()),
            },
        );
    }

    /// Fold a first-token report into a provider's health, when the pick went
    /// off-cluster.
    ///
    /// `upstream_status` is how kinference learns about failures like a revoked
    /// API key, which it can't see otherwise.
    ///
    /// Returns whether `response_id` was a known provider pick, so the reply
    /// can acknowledge it despite there being no `PendingObservation`.
    pub(crate) fn record_provider_first_token(
        &self,
        response_id: u64,
        observed_ttft_ms: u32,
        upstream_status: i32,
    ) -> bool {
        let Some(labels) = self.request_labels.provider_pick_for(response_id) else {
            return false;
        };
        let (provider, model) = (labels.provider, labels.model);
        use kinference_policy::provider_health::FailureKind;
        let failure = match upstream_status {
            // No response at all.
            0 => Some((FailureKind::Upstream, "no response".to_string())),
            401 | 403 => Some((
                FailureKind::Unauthorized,
                format!("vendor returned {upstream_status}"),
            )),
            s if s >= 500 => Some((FailureKind::Upstream, format!("vendor returned {s}"))),
            _ => None,
        };
        match failure {
            Some((kind, detail)) => self.note_provider_failure(&provider, kind, &detail),
            None if labels.streaming => {
                self.provider_health
                    .record_success(&provider, observed_ttft_ms as f32);
                // Success only: a failure's time is time-to-error.
                self.prom
                    .record_provider_ttft(&provider.label(), &model, observed_ttft_ms);
            }
            None => {
                // Non-streaming: `observed_ttft_ms` is total latency, so record
                // the success without a sample.
                self.provider_health.record_success_untimed(&provider);
            }
        }
        true
    }

    /// Fold an `Abort` into provider health, when the pick went off-cluster.
    ///
    /// `NOT_DISPATCHED` (the caller changed its mind) and `PULL_FAILED`
    /// (kinference's body stream broke) are not provider failures.
    pub(crate) fn record_provider_abort(
        &self,
        response_id: u64,
        reason: crate::proto::abort_request::Reason,
        detail: &str,
    ) {
        use crate::proto::abort_request::Reason;
        use kinference_policy::provider_health::FailureKind;
        let kind = match reason {
            Reason::BackendUnresolved => FailureKind::Unresolved,
            Reason::BackendUnauthorized => FailureKind::Unauthorized,
            Reason::Unspecified | Reason::NotDispatched | Reason::PullFailed => return,
        };
        let Some(labels) = self.request_labels.provider_pick_for(response_id) else {
            return;
        };
        let provider = labels.provider;
        self.note_provider_failure(&provider, kind, detail);
    }

    /// Record a failure and log the moment a provider stops being dispatched to.
    ///
    /// Logged on the transition only.
    pub(crate) fn note_provider_failure(
        &self,
        provider: &kinference_policy::engine::ObjectKey,
        kind: kinference_policy::provider_health::FailureKind,
        detail: &str,
    ) {
        if self.provider_health.record_failure(provider, kind, detail) {
            tracing::warn!(
                provider = %provider.label(),
                reason = kind.as_str(),
                detail,
                "provider: suppressing candidates after repeated dispatch failures. \
                 kinference cannot see the GatewayBackend, so this is the caller's report"
            );
        }
    }
}
