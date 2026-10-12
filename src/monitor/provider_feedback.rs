//! Provider feedback — what the caller reports back about an off-cluster
//! destination.
//!
//! kinference never talks to a provider itself: it hands the caller a
//! `ProviderTarget` and the caller does the request. So everything this module
//! records arrives over the feedback RPCs, and it is the ONLY way kinference learns
//! whether a provider worked. That is why the pick side and the report side
//! live together here — `record_provider_pick` writes the labels that
//! `record_provider_first_token` later reads back.
//!
//! Split out of the policy driver rather than filed with it because it is also
//! where the one proto dependency in `monitor` sits: `record_provider_abort`
//! names `crate::proto::abort_request::Reason` directly, which contradicts the
//! invariant stated in `crate::policy` (that `monitor` stays proto-agnostic).
//! Quarantining it in one small file makes that checkable by inspection
//! instead of buried in a 2,900-line module. See the note on
//! `record_provider_abort`.

use std::time::Instant;

use super::{KinferenceService, ProviderPick, Qos};

impl KinferenceService {
    /// Stash what a provider pick needs for its feedback to land.
    ///
    /// Only the labels map, which is deliberate: `PendingObservation` is a CSV
    /// row assembled from per-endpoint scoring state, and a provider has none of
    /// it. Nothing pool-scoped is touched here at all — no inflight count, no
    /// prefix insert — because there is no pool.
    ///
    /// `qos`, `tenant` and `streaming` are passed in rather than read off
    /// `pick`: [`ProviderPick`] describes the DESTINATION, and all three
    /// describe the request. Threading them keeps an off-cluster pick's metrics
    /// labelled the same way a local one's are — previously `qos` was hardcoded
    /// here, so every provider request reported `qos="interactive"` whatever
    /// the caller actually sent.
    ///
    /// `streaming` is the one with teeth: without it
    /// [`Self::record_provider_first_token`] cannot tell a real TTFT from a
    /// non-streaming response's total latency, and a local pick's
    /// `PendingObservation` — which is where that flag normally lives — does
    /// not exist for a provider.
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
                // The provider row's `name`: what the body was rewritten to
                // say, as a local pick reports the model its body names. So
                // `model` means the same on both paths, and one alias served
                // both ways is two models — which it is. The name the client
                // asked for stays in `canonical_model`, for logs.
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
    /// `upstream_status` is what makes this worth doing: kinference cannot tell a
    /// working vendor from a revoked API key on its own, and a 401 here is the
    /// single most likely first-day failure — a Secret whose key name does not
    /// match what the `GatewayBackend` asked for. Without reading it, that
    /// presents as every request failing while every object kinference can see reads
    /// healthy.
    /// Returns whether this `response_id` was a known PROVIDER pick, so the
    /// caller's `acknowledged` can be true for one. A provider pick holds no
    /// `PendingObservation` — there is no pool, so nothing pool-scoped to
    /// release — and without this the reply would say the id was unknown, which
    /// reads to a well-behaved caller as "you sent a stale id".
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
        use crate::policy::provider_health::FailureKind;
        let failure = match upstream_status {
            // No response at all. The proto says to treat this as
            // Abort(NOT_DISPATCHED), and for a provider that means the far end
            // never answered.
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
                // The same sample the router ranks on, published. Success only:
                // a 401 or a 5xx carries a time-to-ERROR, and folding that into
                // a latency distribution would make a failing vendor look fast.
                self.prom
                    .record_provider_ttft(&provider.label(), &model, observed_ttft_ms);
            }
            None => {
                // Non-streaming: the caller's first byte arrived at completion,
                // so `observed_ttft_ms` is total latency, not a TTFT. It is
                // excluded from BOTH the ranking samples and the histogram —
                // that p95 is what `EstimatedTTFT` sorts on and what
                // `backend.observedTTFTP95` compares against, so admitting it
                // would cost the provider picks it should win.
                //
                // The recovery half still applies: the request worked, so the
                // failure streak clears. The end-to-end figure is not lost —
                // `kinference_request_duration_ms` covers provider picks.
                self.provider_health.record_success_untimed(&provider);
            }
        }
        true
    }

    /// Fold an `Abort` into provider health, when the pick went off-cluster.
    ///
    /// `NOT_DISPATCHED` and `PULL_FAILED` are NOT failures of the provider —
    /// the first is the caller changing its mind, the second is kinference's own body
    /// stream breaking. Counting either would suppress a working destination
    /// because a client hung up, which is why the proto grew reason codes that
    /// distinguish them.
    pub(crate) fn record_provider_abort(
        &self,
        response_id: u64,
        reason: crate::proto::abort_request::Reason,
        detail: &str,
    ) {
        use crate::policy::provider_health::FailureKind;
        use crate::proto::abort_request::Reason;
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
    /// Logged on the TRANSITION only. One line per failed request would bury the
    /// state change in the noise it caused, and the state change is the thing an
    /// operator needs to see.
    pub(crate) fn note_provider_failure(
        &self,
        provider: &crate::policy::engine::ObjectKey,
        kind: crate::policy::provider_health::FailureKind,
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
