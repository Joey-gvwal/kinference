//! Thin tonic glue over [`crate::router::service::KinferenceService`].
//!
//! Implements the `Picker` gRPC service (`PushBody`, `Pick`, `PullBody`,
//! `ReportFirstToken`, `ReportFinalToken`, `Abort`) by marshaling to and from
//! `crate::router::service` and `crate::router::body_store`.
//!
//! Error model, per the proto:
//! - `Pick` never fails with a `Status` for a routing outcome; `NO_WORKERS`,
//!   `DEGRADED` and the like come back inside a `PickResponse`. `Status` is
//!   for transport and protocol failures.
//! - `PushBody`, `PullBody` and `Abort` have no routing semantics, so they
//!   report failures as gRPC status codes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::router::metrics::PickOutcome;
use crate::router::observations::Qos;
use crate::router::service::{KinferenceService, PickAndRecordOutcome};

use crate::router::proto;
use crate::router::proto::picker_server::Picker;
use crate::router::proto::{
    AbortRequest, AbortResponse, BodyChunk, PickRequest, PickResponse, PullBodyRequest,
    PushBodyResponse, ReportFinalTokenRequest, ReportFinalTokenResponse, ReportFirstTokenRequest,
    ReportFirstTokenResponse, pick_response::Outcome as WireOutcome,
};

/// Metadata header carrying the caller's log-correlation id on `PushBody`,
/// sent early so an abandoned body is traceable before any `Pick` names it.
const MD_CLIENT_REQUEST_ID: &str = "kinference-client-request-id";
/// Metadata header carrying the client's Content-Length when known. Enforced:
/// it allows early rejection of an over-budget body, and is the only way to
/// catch a body truncated by an early half-close.
pub(crate) const MD_DECLARED_LENGTH: &str = "kinference-declared-length";

/// Why a `PushBody` drain stopped short.
#[derive(Debug)]
pub(crate) enum PushFailure {
    /// No chunk arrived within the inter-chunk budget.
    Idle(Duration),
    /// A byte budget was exceeded; carries the metric label.
    Budget(Status, crate::router::body_store::RejectReason),
    /// The stream reset before half-close, so the body is incomplete.
    Transport(Status),
    /// The caller sent `kinference-declared-length` and delivered a different
    /// number of bytes, e.g. a body truncated by an early half-close.
    LengthMismatch { declared: u64, received: u64 },
}

impl PushFailure {
    pub(crate) fn reject_reason(&self) -> Option<crate::router::body_store::RejectReason> {
        match self {
            PushFailure::Budget(_, reason) => Some(*reason),
            _ => None,
        }
    }

    pub(crate) fn into_status(self) -> Status {
        match self {
            PushFailure::Idle(idle) => Status::deadline_exceeded(format!(
                "no body chunk received within {}s",
                idle.as_secs()
            )),
            PushFailure::Budget(status, _) | PushFailure::Transport(status) => status,
            PushFailure::LengthMismatch { declared, received } => {
                Status::invalid_argument(format!(
                    "body length {received} does not match the declared \
                     kinference-declared-length of {declared}"
                ))
            }
        }
    }
}

/// Drain a client-streaming body into one buffer, enforcing the idle budget and
/// the byte budgets as chunks arrive.
///
/// Generic over the stream so it is testable without a gRPC transport.
///
/// The timeout applies per chunk, not to the call, since a push spans the
/// client's upload. Stream end is a clean half-close: the body is complete. An
/// `Err` item is a reset stream, and the bytes are discarded.
///
/// `declared`, the caller's `kinference-declared-length`, is enforced: a
/// mismatch either way is rejected.
pub(crate) async fn accumulate_body<S>(
    stream: &mut S,
    reservation: &mut crate::router::body_store::Reservation,
    idle: Duration,
    declared: Option<u64>,
) -> Result<Vec<u8>, PushFailure>
where
    S: futures::Stream<Item = Result<BodyChunk, Status>> + Unpin,
{
    use futures::StreamExt;
    // `declared` is only an allocation hint here, clamped.
    let mut buf: Vec<u8> = Vec::with_capacity(declared.unwrap_or(0).min(1024 * 1024) as usize);
    loop {
        match tokio::time::timeout(idle, stream.next()).await {
            Err(_) => return Err(PushFailure::Idle(idle)),
            Ok(None) => {
                // Clean half-close. If a length was declared, this is where a
                // truncated body is caught.
                if let Some(declared) = declared
                    && buf.len() as u64 != declared
                {
                    return Err(PushFailure::LengthMismatch {
                        declared,
                        received: buf.len() as u64,
                    });
                }
                return Ok(buf);
            }
            Ok(Some(Ok(chunk))) => {
                if let Err((status, reason)) = reservation.charge(chunk.chunk.len() as u64) {
                    return Err(PushFailure::Budget(status, reason));
                }
                buf.extend_from_slice(&chunk.chunk);
                // Fail fast on an over-long body rather than buffering past
                // what the caller said it would send.
                if let Some(declared) = declared
                    && buf.len() as u64 > declared
                {
                    return Err(PushFailure::LengthMismatch {
                        declared,
                        received: buf.len() as u64,
                    });
                }
            }
            Ok(Some(Err(e))) => return Err(PushFailure::Transport(e)),
        }
    }
}

/// Chunk size for `PullBody`, well under gRPC message limits.
const PULL_CHUNK_BYTES: usize = 64 * 1024;
/// Outbound chunk queue depth. Bounded, so a slow caller applies
/// backpressure.
const PULL_CHANNEL_DEPTH: usize = 4;

#[tonic::async_trait]
impl Picker for KinferenceService {
    /// Buffer one request body streamed in by the caller, and hand back the id
    /// that names it.
    ///
    /// The call spans the client's upload, so it has no overall deadline. It
    /// is bounded by the gap between chunks (`body_idle_timeout_secs`,
    /// `DEADLINE_EXCEEDED`) and by the byte budgets (`RESOURCE_EXHAUSTED`).
    ///
    /// A clean half-close means the body is complete; on a reset stream the
    /// bytes are dropped and no id is handed out.
    async fn push_body(
        &self,
        request: Request<Streaming<BodyChunk>>,
    ) -> Result<Response<PushBodyResponse>, Status> {
        // Read metadata BEFORE `into_inner()` consumes the request envelope.
        let md = request.metadata();
        let client_request_id = md
            .get(MD_CLIENT_REQUEST_ID)
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let declared_length = md
            .get(MD_DECLARED_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        let req_label = client_request_id.clone().unwrap_or_else(|| "-".to_string());

        // Refuse an over-budget body on its declared length alone, before
        // reading a single chunk.
        if let Some(declared) = declared_length
            && let Err((status, reason)) = self.bodies.store.check_declared_length(declared)
        {
            self.prom.record_body_rejected(reason.as_label());
            tracing::warn!(
                req = %req_label,
                declared,
                "push_body: refused on declared length"
            );
            return Err(status);
        }

        let mut inbound = request.into_inner();
        let mut reservation = self
            .bodies
            .store
            .reservation()
            .with_client_request_id(client_request_id);
        let idle = self.bodies.idle_timeout;
        let started = Instant::now();

        let buf = match accumulate_body(&mut inbound, &mut reservation, idle, declared_length).await
        {
            Ok(buf) => buf,
            Err(failure) => {
                if let Some(reason) = failure.reject_reason() {
                    self.prom.record_body_rejected(reason.as_label());
                }
                let status = failure.into_status();
                tracing::warn!(
                    req = %req_label,
                    code = ?status.code(),
                    "push_body: aborted before half-close; discarding"
                );
                return Err(status);
            }
        };

        let total_bytes = buf.len() as u64;
        let stream_id = self.bodies.store.commit(reservation, buf);
        self.prom.record_body_pushed(total_bytes);
        tracing::debug!(
            req = %req_label,
            stream_id,
            total_bytes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "push_body: buffered"
        );
        Ok(Response::new(PushBodyResponse {
            stream_id,
            total_bytes,
        }))
    }

    /// Pick a worker for a body already pushed: claim the body, run
    /// `pick_and_record`, and re-key the body to the minted `response_id` for
    /// `PullBody`.
    ///
    /// `Err(Status)` only for body-lookup failures; routing outcomes come back
    /// in the `PickResponse`.
    async fn pick(&self, request: Request<PickRequest>) -> Result<Response<PickResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();
        // Claim the body first: no point minting ids or scoring if the caller
        // named a body we don't have.
        let handle = self.bodies.store.begin_pick(req.body_stream_id)?;
        let pushed = handle.body().clone();

        let response_id = self.mint_response_id();
        let client_request_id = if req.client_request_id.is_empty() {
            None
        } else {
            Some(req.client_request_id.as_str())
        };
        let qos = Qos::from_proto(req.qos_class);
        // An absent `inference_pool` means pick across everything served.
        let pool_ref = req
            .inference_pool
            .as_ref()
            .map(|p| kinference_policy::PoolRef {
                namespace: p.namespace.clone(),
                name: p.name.clone(),
                resource_version: p.resource_version.clone(),
            });
        // Verbatim, including "", which a policy can match.
        let identity = kinference_policy::RequestIdentity { tenant: req.tenant };
        let outcome = self
            .pick_and_record(
                &pushed,
                response_id,
                client_request_id,
                qos,
                pool_ref.as_ref(),
                &identity,
            )
            .await;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        // `Degraded` counts as `Success` in the pick metric: a worker was
        // chosen, on load alone.
        let (response, pick_outcome) = match outcome {
            PickAndRecordOutcome::Ok {
                endpoint,
                response_id,
                body,
                model,
            } => (
                commit_picked_body(handle, response_id, body, &pushed, |total| PickResponse {
                    outcome: WireOutcome::Ok as i32,
                    destination: Some(local_destination(endpoint)),
                    response_id,
                    reason: String::new(),
                    body_total_bytes: total,
                    requested_model: model,
                }),
                PickOutcome::Success,
            ),
            PickAndRecordOutcome::Degraded {
                endpoint,
                response_id,
                reason,
                body,
                model,
            } => (
                commit_picked_body(handle, response_id, body, &pushed, |total| PickResponse {
                    outcome: WireOutcome::Degraded as i32,
                    destination: Some(local_destination(endpoint)),
                    response_id,
                    reason,
                    body_total_bytes: total,
                    requested_model: model,
                }),
                PickOutcome::Success,
            ),
            // An off-cluster destination. `OK`, not `DEGRADED`, which is about
            // the prefix-cache signal a provider never has.
            PickAndRecordOutcome::Provider {
                pick,
                response_id,
                body,
            } => {
                let canonical = pick.canonical_model.clone();
                (
                    commit_picked_body(handle, response_id, body, &pushed, |total| {
                        PickResponse {
                            outcome: WireOutcome::Ok as i32,
                            destination: Some(provider_destination(&pick)),
                            response_id,
                            reason: String::new(),
                            body_total_bytes: total,
                            // The name the client asked for; the body has the
                            // vendor's.
                            requested_model: canonical,
                        }
                    }),
                    PickOutcome::Success,
                )
            }
            // Non-OK outcomes release the body now; callers don't `Abort` a
            // failed `Pick`.
            PickAndRecordOutcome::NoWorkers { model } => {
                handle.discard();
                (
                    PickResponse {
                        outcome: WireOutcome::NoWorkers as i32,
                        destination: None,
                        response_id: 0,
                        reason: format!("no workers serve model `{model}`"),
                        body_total_bytes: 0,
                        requested_model: model,
                    },
                    PickOutcome::NotFound,
                )
            }
            // Distinct from NoWorkers: workers may be available, but a rule
            // declined to use them.
            PickAndRecordOutcome::PolicyRejected { reason, model } => {
                handle.discard();
                (
                    PickResponse {
                        outcome: WireOutcome::PolicyRejected as i32,
                        destination: None,
                        response_id: 0,
                        reason,
                        body_total_bytes: 0,
                        requested_model: model,
                    },
                    PickOutcome::NotFound,
                )
            }
            PickAndRecordOutcome::BadRequest { reason, model } => {
                handle.discard();
                (
                    PickResponse {
                        outcome: WireOutcome::BadRequest as i32,
                        destination: None,
                        response_id: 0,
                        reason,
                        body_total_bytes: 0,
                        requested_model: model,
                    },
                    PickOutcome::NoOpinion,
                )
            }
        };
        self.prom.record_pick(pick_outcome.as_label(), latency_ms);
        Ok(Response::new(response))
    }

    type PullBodyStream = ReceiverStream<Result<BodyChunk, Status>>;

    /// Stream the body to forward upstream back out to the caller.
    ///
    /// Single-shot: the body is released as the stream is handed out, so a
    /// second `PullBody` for the same `response_id` is `NOT_FOUND`. Exactly
    /// `PickResponse.body_total_bytes` bytes are streamed.
    async fn pull_body(
        &self,
        request: Request<PullBodyRequest>,
    ) -> Result<Response<Self::PullBodyStream>, Status> {
        let req = request.into_inner();
        let lease = self.bodies.store.take_outbound(req.response_id)?;
        let total = lease.len();
        tracing::debug!(
            response_id = req.response_id,
            total_bytes = total,
            "pull_body: streaming"
        );

        let (tx, rx) = mpsc::channel(PULL_CHANNEL_DEPTH);
        tokio::spawn(async move {
            // The body stays charged to the budget until this task ends.
            let body = lease.body().clone();
            for slice in body.chunks(PULL_CHUNK_BYTES) {
                // `send` awaits when the queue is full (backpressure). An error
                // means the caller went away.
                if tx
                    .send(Ok(BodyChunk {
                        chunk: slice.to_vec(),
                    }))
                    .await
                    .is_err()
                {
                    tracing::debug!(
                        response_id = req.response_id,
                        "pull_body: caller stopped consuming; dropping the rest"
                    );
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// First-token report for an earlier `Pick()`. Records the TTFT and CSV row
    /// (streaming only) and releases the inflight state.
    ///
    /// `upstream_status == 0` means no response at all, handled like
    /// `Abort(NOT_DISPATCHED)`.
    ///
    /// `acknowledged = false` means the `response_id` was unknown (swept, or
    /// never picked).
    async fn report_first_token(
        &self,
        request: Request<ReportFirstTokenRequest>,
    ) -> Result<Response<ReportFirstTokenResponse>, Status> {
        let req = request.into_inner();
        // Before releasing, which clears the label this is attributed by. A
        // no-op for a local pick.
        let was_provider = self.observations.record_provider_first_token(
            req.response_id,
            req.observed_ttft_ms,
            req.upstream_status,
        );
        let acknowledged = if req.upstream_status == 0 {
            tracing::debug!(
                response_id = req.response_id,
                "report_first_token: upstream_status=0 (no response reached the caller); \
                 treating as not-dispatched"
            );
            let (released, _) = self.abort_request(req.response_id);
            released
        } else {
            self.observations
                .finalize(req.response_id, req.observed_ttft_ms, req.upstream_status)
        };
        Ok(Response::new(ReportFirstTokenResponse {
            // A provider pick is known even though it holds no pending
            // observation for `Observations::finalize` to find.
            acknowledged: acknowledged || was_provider,
        }))
    }

    /// Final-token report for an earlier `Pick()`. Carries the served model,
    /// prompt/completion token counts, and per-bucket inter-token-latency
    /// counts. Feeds the per-(model,qos) token counters, length histograms, and
    /// ITL histogram on `/metrics`. The `qos` label comes from the pick's
    /// `RequestLabels` entry (the request's own `model` is a fallback when that
    /// entry was already TTL-swept).
    ///
    /// `acknowledged = false` means the `response_id` was unknown (swept or
    /// never picked); metrics are still recorded best-effort.
    async fn report_final_token(
        &self,
        request: Request<ReportFinalTokenRequest>,
    ) -> Result<Response<ReportFinalTokenResponse>, Status> {
        let req = request.into_inner();
        let meta = self.observations.labels.take(req.response_id);
        // An unknown or expired `response_id` leaves nothing to attribute this
        // to. The empty tenant maps to the default bucket.
        let (model, qos, tenant) = match &meta {
            Some(m) => (m.model.clone(), m.qos, m.tenant.clone()),
            // `Unknown`, rather than a guess.
            None => (req.model.clone(), Qos::Unknown, String::new()),
        };
        if meta.is_none() {
            self.prom.record_orphaned_report();
        }
        let qos_label = qos.as_label();
        tracing::debug!(
            response_id = req.response_id,
            model = %model,
            qos = qos_label,
            prompt_tokens = req.prompt_tokens,
            completion_tokens = req.completion_tokens,
            itl_layout = req.itl_bucket_layout,
            itl_buckets = req.itl_bucket_counts.len(),
            known = meta.is_some(),
            "report_final_token"
        );
        // End-to-end duration, when we know when the pick happened.
        if let Some(m) = &meta {
            self.prom.record_request_duration(
                &model,
                qos_label,
                &tenant,
                m.picked_at.elapsed().as_millis() as u64,
            );
        }
        self.prom.record_usage(
            &model,
            qos_label,
            &tenant,
            req.prompt_tokens,
            req.completion_tokens,
        );
        // Engine-measured prefix-cache hit; `None` is counted as unknown.
        self.prom
            .record_cached_prompt_tokens(&model, qos_label, &tenant, req.cached_tokens);
        self.prom.record_itl(
            &model,
            qos_label,
            &tenant,
            req.itl_bucket_layout,
            &req.itl_bucket_counts,
        );
        Ok(Response::new(ReportFinalTokenResponse {
            acknowledged: meta.is_some(),
        }))
    }

    /// Release state for a request that will never complete.
    ///
    /// Best-effort and idempotent: an unknown or already-released id comes back
    /// `released: false` rather than `NOT_FOUND`, because callers legitimately
    /// send this zero, one, or several times. Frees the buffered body and
    /// un-counts the inflight dispatch.
    async fn abort(
        &self,
        request: Request<AbortRequest>,
    ) -> Result<Response<AbortResponse>, Status> {
        let req = request.into_inner();
        let reason = proto::abort_request::Reason::try_from(req.reason)
            .unwrap_or(proto::abort_request::Reason::Unspecified);
        // Before releasing, which clears the label this is attributed by.
        if let Some(kind) = abort_failure_kind(reason) {
            self.observations
                .record_provider_abort(req.response_id, kind, &req.detail);
        }
        let (released, bytes_released) = self.abort_request(req.response_id);
        self.prom.record_body_aborted(abort_reason_label(reason));
        tracing::debug!(
            response_id = req.response_id,
            req = %req.client_request_id,
            reason = abort_reason_label(reason),
            detail = %req.detail,
            released,
            bytes_released,
            "abort"
        );
        Ok(Response::new(AbortResponse { released }))
    }
}

/// Re-key a successfully-picked body to `response_id` and build the response
/// with the exact outbound length.
///
/// `wire` is `pick_and_record`'s rewritten body, or empty when unchanged, in
/// which case the pushed body is reused.
fn commit_picked_body(
    handle: crate::router::body_store::PickHandle,
    response_id: u64,
    wire: Vec<u8>,
    pushed: &Arc<Vec<u8>>,
    build: impl FnOnce(u64) -> PickResponse,
) -> PickResponse {
    let outbound = if wire.is_empty() {
        pushed.clone()
    } else {
        Arc::new(wire)
    };
    let total = outbound.len() as u64;
    handle.commit(response_id, outbound);
    build(total)
}

/// The provider failure an abort reason reports, if any.
///
/// `NOT_DISPATCHED` (the caller changed its mind) and `PULL_FAILED`
/// (kinference's body stream broke) are not provider failures.
fn abort_failure_kind(
    reason: proto::abort_request::Reason,
) -> Option<kinference_policy::provider_health::FailureKind> {
    use kinference_policy::provider_health::FailureKind;
    use proto::abort_request::Reason;
    match reason {
        Reason::BackendUnresolved => Some(FailureKind::Unresolved),
        Reason::BackendUnauthorized => Some(FailureKind::Unauthorized),
        Reason::Unspecified | Reason::NotDispatched | Reason::PullFailed => None,
    }
}

fn abort_reason_label(reason: proto::abort_request::Reason) -> &'static str {
    match reason {
        proto::abort_request::Reason::Unspecified => "unspecified",
        proto::abort_request::Reason::NotDispatched => "not_dispatched",
        proto::abort_request::Reason::PullFailed => "pull_failed",
        proto::abort_request::Reason::BackendUnresolved => "backend_unresolved",
        proto::abort_request::Reason::BackendUnauthorized => "backend_unauthorized",
    }
}

/// A local endpoint, as the `destination` oneof.
fn local_destination(endpoint: String) -> proto::pick_response::Destination {
    proto::pick_response::Destination::Local(proto::LocalTarget { endpoint })
}

/// An off-cluster provider, as the `destination` oneof.
///
/// The backend to dispatch through, and the path to dispatch on (empty means
/// the client's path). The model is in the rewritten body. Group and Kind are
/// implied, as on `InferencePoolRef`.
fn provider_destination(
    pick: &crate::router::service::ProviderPick,
) -> proto::pick_response::Destination {
    proto::pick_response::Destination::Provider(proto::ProviderTarget {
        backend_ref: Some(proto::BackendRef {
            namespace: pick.backend_ref.namespace.clone(),
            name: pick.backend_ref.name.clone(),
        }),
        path: pick.path.clone().unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests;
