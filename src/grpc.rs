//! Thin tonic glue over [`crate::monitor::KinferenceService`].
//!
//! Owns:
//! - The `proto` module (tonic-generated bindings for the gRPC `PushBody` /
//!   `Pick` / `PullBody` / `ReportFirstToken` / `ReportFinalToken` / `Abort`
//!   RPCs).
//! - The `impl Picker for KinferenceService` trait impl that marshals
//!   `pick_and_record` / `finalize_observation` outcomes onto the wire.
//!
//! All actual scoring + picking + observation logic lives in
//! `crate::monitor`; body buffering lives in `crate::body_store`; HTTP
//! tokenize plumbing lives in `crate::tokenize_client`.
//!
//! ERROR MODEL — note the deliberate split, which the proto specifies:
//! - `Pick` never fails with a `Status` for a routing outcome. `NO_WORKERS`,
//!   `DEGRADED`, and `BAD_REQUEST` all come back inside a `PickResponse` so
//!   clients always get structured observability. `Status` is reserved for
//!   transport/protocol failures.
//! - `PushBody` / `PullBody` / `Abort` are the opposite: they're protocol
//!   operations with no routing semantics, so they report failures as gRPC
//!   status codes directly (NOT_FOUND, FAILED_PRECONDITION,
//!   RESOURCE_EXHAUSTED, DEADLINE_EXCEEDED, INVALID_ARGUMENT).

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::monitor::{KinferenceService, PickAndRecordOutcome, Qos};
use crate::prom::PickOutcome;

use crate::proto;
use crate::proto::picker_server::Picker;
use crate::proto::{
    AbortRequest, AbortResponse, BodyChunk, PickRequest, PickResponse, PullBodyRequest,
    PushBodyResponse, ReportFinalTokenRequest, ReportFinalTokenResponse, ReportFirstTokenRequest,
    ReportFirstTokenResponse, pick_response::Outcome as WireOutcome,
};

/// Metadata header carrying the caller's log-correlation id on `PushBody`,
/// sent early so an abandoned body is traceable before any `Pick` names it.
const MD_CLIENT_REQUEST_ID: &str = "kinference-client-request-id";
/// Metadata header carrying the client's Content-Length when known. Enforced,
/// not advisory: it buys early rejection of an over-budget body AND — because a
/// clean half-close proves only that the caller stopped, not that it sent
/// everything — it's the one check that catches a truncated body.
pub(crate) const MD_DECLARED_LENGTH: &str = "kinference-declared-length";

/// Why a `PushBody` drain stopped short.
#[derive(Debug)]
pub(crate) enum PushFailure {
    /// No chunk arrived within the inter-chunk budget.
    Idle(Duration),
    /// A byte budget was exceeded; carries the metric label.
    Budget(Status, crate::body_store::RejectReason),
    /// The stream reset before half-close, so the body is incomplete.
    Transport(Status),
    /// The caller sent `kinference-declared-length` and delivered a different
    /// number of bytes. Catches a body truncated by an EARLY half-close —
    /// which the transport cannot distinguish from a complete one, since a
    /// clean END_STREAM is a clean END_STREAM either way.
    LengthMismatch { declared: u64, received: u64 },
}

impl PushFailure {
    pub(crate) fn reject_reason(&self) -> Option<crate::body_store::RejectReason> {
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
/// Generic over the stream so this — the part with the actual rules in it — is
/// unit-testable without a live gRPC transport (`tonic::Streaming` can only be
/// built by the transport layer).
///
/// The timeout is applied PER CHUNK, never to the call as a whole: a push spans
/// the end client's upload and may legitimately run for minutes.
/// `Ok(None)`/stream end is a clean half-close and means the body is complete;
/// an `Err` item is a reset stream, and the bytes are discarded — a partial body
/// never reaches the store, because the caller only mints a `stream_id` on the
/// `Ok` path.
///
/// `declared` is the caller's `kinference-declared-length` when it sent one. When
/// present it is ENFORCED, not merely advisory: a mismatch either way is
/// rejected. That is the only defense against a body truncated by an early
/// half-close, which is otherwise indistinguishable from a complete one — and
/// silently forwarding a truncated body upstream is worse than any error,
/// because it can parse as a valid-but-wrong request.
pub(crate) async fn accumulate_body<S>(
    stream: &mut S,
    reservation: &mut crate::body_store::Reservation,
    idle: Duration,
    declared: Option<u64>,
) -> Result<Vec<u8>, PushFailure>
where
    S: futures::Stream<Item = Result<BodyChunk, Status>> + Unpin,
{
    use futures::StreamExt;
    // Trust `declared` only as an allocation hint, clamped so a bogus header
    // can't make us allocate a huge buffer up front. (An over-budget declared
    // length is already refused before we get here.)
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

/// Chunk size for `PullBody`. Small enough to stay far under any gRPC
/// per-message limit and to keep the first byte moving promptly; large enough
/// that a multi-MB body doesn't cost thousands of messages.
const PULL_CHUNK_BYTES: usize = 64 * 1024;
/// Outbound chunk queue depth. Bounded on purpose: a caller that consumes
/// slowly should apply backpressure (its pace is set by the upstream inference
/// server), not make us buffer a second copy of the body.
const PULL_CHANNEL_DEPTH: usize = 4;

#[tonic::async_trait]
impl Picker for KinferenceService {
    /// Buffer one request body streamed in by the caller, and hand back the id
    /// that names it.
    ///
    /// The call stays open for the whole of the end client's upload, which may
    /// be minutes over a slow link — so it gets NO overall deadline. It's
    /// bounded two other ways, per the proto: the gap between chunks
    /// (`body_idle_timeout_secs` → `DEADLINE_EXCEEDED`) and total bytes
    /// (per-request + server-wide budgets → `RESOURCE_EXHAUSTED`).
    ///
    /// A clean half-close means the body is complete. A reset stream means it
    /// is not, and the bytes are discarded — the local buffer and its byte
    /// reservation are simply dropped, with no id ever handed out and no TTL
    /// involvement.
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
            && let Err((status, reason)) = self.body_store.check_declared_length(declared)
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
            .body_store
            .reservation()
            .with_client_request_id(client_request_id);
        let idle = self.body_idle_timeout();
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
        let stream_id = self.body_store.commit(reservation, buf);
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

    /// Pick a worker for a body already pushed. Claims the buffered body,
    /// runs `pick_and_record` (the full pick critical section incl. inflight +
    /// inflight-prefix mutation), and re-keys the body to the minted
    /// `response_id` for `PullBody` to stream.
    ///
    /// Returns `Err(Status)` only for body-lookup failures (the pushed body is
    /// unknown, already consumed, or the id is 0). Every routing-level outcome
    /// comes back as a structured `PickResponse`.
    async fn pick(&self, request: Request<PickRequest>) -> Result<Response<PickResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();
        // Claim the body first: no point minting ids or scoring if the caller
        // named a body we don't have.
        let handle = self.body_store.begin_pick(req.body_stream_id)?;
        let pushed = handle.body().clone();

        let response_id = self.mint_response_id();
        let client_request_id = if req.client_request_id.is_empty() {
            None
        } else {
            Some(req.client_request_id.as_str())
        };
        let qos = Qos::from_proto(req.qos_class);
        // An absent `inference_pool` means "pick across everything I serve" —
        // the pre-field behavior, which the proto requires keep working.
        let pool_ref = req.inference_pool.as_ref().map(|p| crate::policy::PoolRef {
            namespace: p.namespace.clone(),
            name: p.name.clone(),
            resource_version: p.resource_version.clone(),
        });
        // Moved, not cloned: nothing downstream reads `req` again. Taken
        // verbatim, including "" — an unattributed request carries the empty
        // tenant as a value rather than as an absence, so a policy can name it.
        let identity = crate::policy::RequestIdentity { tenant: req.tenant };
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

        // Telemetry note: `Degraded` is mapped to `Success` for now — the
        // worker did get chosen, just on load-only signal.
        let (response, pick_outcome) = match outcome {
            PickAndRecordOutcome::Ok {
                endpoint,
                response_id,
                body,
                model,
            } => (
                self.commit_picked_body(handle, response_id, body, &pushed, |total| PickResponse {
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
                self.commit_picked_body(handle, response_id, body, &pushed, |total| PickResponse {
                    outcome: WireOutcome::Degraded as i32,
                    destination: Some(local_destination(endpoint)),
                    response_id,
                    reason,
                    body_total_bytes: total,
                    requested_model: model,
                }),
                PickOutcome::Success,
            ),
            // An off-cluster destination. `OK`, not `DEGRADED`: the body is
            // complete and the choice was made on everything a provider has.
            // DEGRADED is about kinference's own prefix-cache signal being
            // unavailable, and a provider never had one to lose.
            PickAndRecordOutcome::Provider {
                pick,
                response_id,
                body,
            } => {
                let canonical = pick.canonical_model.clone();
                (
                    self.commit_picked_body(handle, response_id, body, &pushed, |total| {
                        PickResponse {
                            outcome: WireOutcome::Ok as i32,
                            destination: Some(provider_destination(&pick)),
                            response_id,
                            reason: String::new(),
                            body_total_bytes: total,
                            // The name the CLIENT asked for, as everywhere else
                            // in this field. The vendor's spelling is already in
                            // the body PullBody returns.
                            requested_model: canonical,
                        }
                    }),
                    PickOutcome::Success,
                )
            }
            // Non-OK outcomes retain nothing: no destination means nobody will
            // ever pull this body. The proto makes releasing it the server's
            // own job (callers don't Abort a failed Pick), so do it now rather
            // than leaving it for the TTL.
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
            // Kept distinct from NoWorkers on the wire because it says something
            // different about the cluster: workers may be idle and perfectly able
            // to serve, and a rule declined to use them. Reporting it as an
            // absence sends the operator to look at Deployments over an edit to a
            // policy.
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
    /// `PickResponse.body_total_bytes` bytes are streamed — callers are
    /// entitled to enforce that.
    async fn pull_body(
        &self,
        request: Request<PullBodyRequest>,
    ) -> Result<Response<Self::PullBodyStream>, Status> {
        let req = request.into_inner();
        let lease = self.body_store.take_outbound(req.response_id)?;
        let total = lease.len();
        tracing::debug!(
            response_id = req.response_id,
            total_bytes = total,
            "pull_body: streaming"
        );

        let (tx, rx) = mpsc::channel(PULL_CHANNEL_DEPTH);
        tokio::spawn(async move {
            // `lease` moves in, so the body's bytes stay charged to the budget
            // until this task ends — however it ends.
            let body = lease.body().clone();
            for slice in body.chunks(PULL_CHUNK_BYTES) {
                // `send` awaits when the queue is full: that IS the
                // backpressure. An error means the caller went away.
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

    /// First-token report for an earlier `Pick()`. Records the TTFT (streaming
    /// only) + per-request CSV row, and clears the inflight + inflight-prefix
    /// state.
    ///
    /// `upstream_status == 0` means the caller never got a response at all, so
    /// there is no TTFT to record — the proto says treat it like
    /// `Abort(NOT_DISPATCHED)`, which is what we do: release the dispatch (and
    /// any body still held) instead of writing a bogus observation.
    ///
    /// `acknowledged = false` means the `response_id` was unknown — either
    /// already TTL'd by the sweep, or never picked.
    async fn report_first_token(
        &self,
        request: Request<ReportFirstTokenRequest>,
    ) -> Result<Response<ReportFirstTokenResponse>, Status> {
        let req = request.into_inner();
        // Before anything is released, since releasing clears the label this is
        // attributed by. For a LOCAL pick this is a no-op; for a provider it is
        // the only latency kinference will ever have, and the only way it learns that a
        // vendor is rejecting its credentials.
        let was_provider = self.record_provider_first_token(
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
            self.finalize_observation(req.response_id, req.observed_ttft_ms, req.upstream_status)
        };
        Ok(Response::new(ReportFirstTokenResponse {
            // A provider pick is known even though it holds no pending
            // observation for `finalize_observation` to find.
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
        let meta = self.request_labels.take(req.response_id);
        // An unknown `response_id` (or one whose labels aged out of the 600s
        // TTL) leaves nothing to attribute this report to. The empty tenant is
        // not a special case: `record_*` maps it to the default bucket, the
        // same as a caller that sent none.
        let (model, qos, tenant) = match &meta {
            Some(m) => (m.model.clone(), m.qos, m.tenant.clone()),
            // `Unknown`, not a fabricated `Interactive`: there is no meta to
            // read the real class from, and asserting one silently loads every
            // orphan into whichever class we guessed.
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
        // End-to-end duration, when we know when the pick happened. An unknown
        // `response_id` has no `picked_at` to measure from, so it is skipped
        // rather than guessed — a fabricated zero would sit in the lowest
        // bucket and drag the distribution down exactly when reports are being
        // lost.
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
        // Engine-measured prefix-cache hit for this prompt. Passed through as
        // `Option` so a caller that does not report it is counted as unknown
        // rather than as a zero hit.
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
    /// send this zero, one, or several times. Frees the buffered body AND
    /// un-counts the inflight dispatch — the latter matters more, since a
    /// dispatch left counted makes the endpoint look permanently loaded.
    async fn abort(
        &self,
        request: Request<AbortRequest>,
    ) -> Result<Response<AbortResponse>, Status> {
        let req = request.into_inner();
        let reason = proto::abort_request::Reason::try_from(req.reason)
            .unwrap_or(proto::abort_request::Reason::Unspecified);
        // Read the provider BEFORE releasing, since releasing clears the label
        // this is attributed by. A BACKEND_* abort is the only way kinference ever
        // learns that a destination it chose does not work — it never reads the
        // GatewayBackend, so an unresolvable ref or a rejected credential is
        // otherwise invisible and the provider keeps winning picks forever.
        self.record_provider_abort(req.response_id, reason, &req.detail);
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

impl KinferenceService {
    /// Configured idle budget between `PushBody` chunks.
    fn body_idle_timeout(&self) -> Duration {
        self.body_idle_timeout
    }

    /// Re-key a successfully-picked body to `response_id` and build the
    /// response with the exact outbound length.
    ///
    /// `wire` is `pick_and_record`'s rewritten body, or EMPTY when it left the
    /// body alone — in which case we reuse the pushed `Arc` rather than copying
    /// megabytes to say "unchanged".
    fn commit_picked_body(
        &self,
        handle: crate::body_store::PickHandle,
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
///
/// `workerset` is `None` for a statically configured worker set, which has no
/// namespaced identity to report — the field is left unset rather than filled
/// with a name that would not resolve.
/// A local endpoint, as the `destination` oneof.
fn local_destination(endpoint: String) -> proto::pick_response::Destination {
    proto::pick_response::Destination::Local(proto::LocalTarget { endpoint })
}

/// An off-cluster provider, as the `destination` oneof.
///
/// The backend to dispatch through, and the path to dispatch on. The model
/// rides in the body, which kinference already rewrote to the vendor's own name, and
/// with one supported wire format there is nothing else to declare — so nothing
/// here duplicates what the bytes say. Group and Kind are implied, as on
/// `InferencePoolRef`.
///
/// The path is the one thing here the bytes CANNOT say: a body rewrite renames
/// the model, and the request line is where the vendor disagrees about where
/// that model lives. Empty means "dispatch on the path the client sent".
fn provider_destination(pick: &crate::monitor::ProviderPick) -> proto::pick_response::Destination {
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
