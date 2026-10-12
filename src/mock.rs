//! Deterministic stand-in for `KinferenceService` used when
//! `Config.mock_mode = true`.
//!
//! Speaks the same gRPC interface (`Picker::{PushBody, Pick, PullBody,
//! ReportFirstToken, ReportFinalToken, Abort}`) but doesn't touch any of the
//! live-mode subsystems:
//! - no ZMQ subscriber (so no `KvIndexer`);
//! - no Prometheus `/metrics` scrapes (so no `MetricsRegistry`);
//! - no per-request HTTP `/tokenize` call.
//!
//! Body buffering IS real: the mock owns its own [`crate::body_store::BodyStore`]
//! and implements the same push → pick → pull lifecycle, so a client integrating
//! against mock mode exercises the true wire contract (ids, single-shot pull,
//! `body_total_bytes`, abort idempotency). Only the routing decision is
//! synthetic: `Pick` parses the body just enough to extract `model`, looks up
//! the workers serving it, and chooses one by hashing `(model, body)`. Same
//! input → same chosen worker across runs and machines. The report RPCs always
//! acknowledge — there's no pending-observation table to clear in mock mode.
//!
//! Pre-Pick/Observe migration this file also drove a deterministic
//! `Score()` response with hashed-in load metrics; that's gone now.
//! With `Pick()` the caller dispatches based on the returned endpoint
//! alone, so per-worker telemetry is no longer load-bearing for mock
//! behavior.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::body_store::BodyStore;
use crate::proto::picker_server::Picker;
use crate::proto::{
    AbortRequest, AbortResponse, BodyChunk, PickRequest, PickResponse, PullBodyRequest,
    PushBodyResponse, ReportFinalTokenRequest, ReportFinalTokenResponse, ReportFirstTokenRequest,
    ReportFirstTokenResponse, pick_response::Outcome as PickOutcome,
};
use crate::workers::WorkerRegistry;

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

pub struct MockKinferenceService {
    registry: Arc<WorkerRegistry>,
    /// Mock mode gets its own body store — same type, same semantics as real
    /// mode, so the push/pull contract is genuinely exercised.
    body_store: Arc<BodyStore>,
}

impl MockKinferenceService {
    pub fn new(registry: Arc<WorkerRegistry>) -> Self {
        Self {
            registry,
            // Generous fixed budgets: mock mode is for demos and client
            // integration tests, not memory-pressure testing.
            body_store: crate::body_store::shared(
                256 * 1024 * 1024,
                8 * 1024 * 1024,
                Duration::from_secs(60),
            ),
        }
    }

    /// Random non-zero response_id, matching the real service's contract (see
    /// `KinferenceService::mint_response_id`).
    fn mint_response_id(&self) -> u64 {
        loop {
            let mut buf = [0u8; 8];
            let id = match getrandom::getrandom(&mut buf) {
                Ok(()) => u64::from_le_bytes(buf),
                Err(_) => 1,
            };
            if id != 0 {
                return id;
            }
        }
    }
}

/// Minimal body parse — only the `model` field is needed for the
/// mock's pick logic.
#[derive(serde::Deserialize, Default)]
struct ChatBody {
    #[serde(default)]
    model: String,
}

#[tonic::async_trait]
impl Picker for MockKinferenceService {
    /// Same buffering contract as the real service, minus the byte-budget
    /// tuning: accumulate until half-close, then hand back a random id.
    async fn push_body(
        &self,
        request: Request<Streaming<BodyChunk>>,
    ) -> Result<Response<PushBodyResponse>, Status> {
        // Honor `kinference-declared-length` exactly as the real service does — both
        // the early over-budget rejection and the received-vs-declared check —
        // so a client integrating against mock mode sees the same contract.
        let declared = request
            .metadata()
            .get(crate::grpc::MD_DECLARED_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());
        if let Some(declared) = declared {
            self.body_store
                .check_declared_length(declared)
                .map_err(|(status, _)| status)?;
        }
        let mut inbound = request.into_inner();
        let mut reservation = self.body_store.reservation();
        let buf = crate::grpc::accumulate_body(
            &mut inbound,
            &mut reservation,
            Duration::from_secs(30),
            declared,
        )
        .await
        .map_err(|f| f.into_status())?;
        let total_bytes = buf.len() as u64;
        let stream_id = self.body_store.commit(reservation, buf);
        Ok(Response::new(PushBodyResponse {
            stream_id,
            total_bytes,
        }))
    }

    async fn pick(&self, request: Request<PickRequest>) -> Result<Response<PickResponse>, Status> {
        let req = request.into_inner();
        let handle = self.body_store.begin_pick(req.body_stream_id)?;
        let pushed = handle.body().clone();
        // The body is the only source for `model`, matching the real service.
        let mut model = {
            let parsed: ChatBody = serde_json::from_slice(&pushed).unwrap_or_default();
            parsed.model
        };
        if model.is_empty() {
            handle.discard();
            return Ok(Response::new(PickResponse {
                outcome: PickOutcome::BadRequest as i32,
                destination: None,
                response_id: 0,
                reason: "request body has an empty or absent `model`".to_string(),
                body_total_bytes: 0,
                requested_model: String::new(),
            }));
        }
        // "auto" pseudo-model: pick a concrete model (placeholder: random
        // among known models), mirroring the real KinferenceService path. When we
        // resolve it, `force_model` drives the same body rewrite the real
        // service does, so a mock-backed caller never forwards "auto".
        let mut force_model: Option<String> = None;
        if model == crate::monitor::AUTO_MODEL {
            let candidates: Vec<String> = self
                .registry
                .known_models()
                .into_iter()
                .filter(|m| m != crate::monitor::AUTO_MODEL)
                .collect();
            if candidates.is_empty() {
                handle.discard();
                return Ok(Response::new(PickResponse {
                    outcome: PickOutcome::NoWorkers as i32,
                    destination: None,
                    response_id: 0,
                    reason: "auto: no candidate models available".to_string(),
                    body_total_bytes: 0,
                    requested_model: model,
                }));
            }
            let mut buf = [0u8; 8];
            let idx = match getrandom::getrandom(&mut buf) {
                Ok(()) => (u64::from_le_bytes(buf) as usize) % candidates.len(),
                Err(_) => 0,
            };
            model = candidates[idx].clone();
            force_model = Some(model.clone());
        }
        let workers = self.registry.workers_for_model(&model);
        if workers.is_empty() {
            handle.discard();
            return Ok(Response::new(PickResponse {
                outcome: PickOutcome::NoWorkers as i32,
                destination: None,
                response_id: 0,
                reason: format!("no workers serve model {model}"),
                body_total_bytes: 0,
                requested_model: model,
            }));
        }
        // Deterministic chosen-worker selection: hash (model, pushed body) and
        // index into the sorted-by-id worker list. Same input always
        // returns the same endpoint across runs and machines.
        let mut buf = Vec::with_capacity(model.len() + pushed.len() + 1);
        buf.extend_from_slice(model.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&pushed);
        let h = fnv1a(&buf);
        let chosen = &workers[(h as usize) % workers.len()];
        let response_id = self.mint_response_id();
        // Mirror the real service's Pick-time body rewrite, then retain the
        // outbound body for PullBody — reusing the pushed Arc when unchanged.
        let outbound = {
            let mut obj: serde_json::Value =
                serde_json::from_slice(&pushed).unwrap_or(serde_json::Value::Null);
            if crate::pick_body::apply_pick_body_mutations(&mut obj, force_model.as_deref()) {
                Arc::new(serde_json::to_vec(&obj).unwrap_or_default())
            } else {
                pushed.clone()
            }
        };
        let body_total_bytes = outbound.len() as u64;
        let endpoint = chosen.1.score_addr.clone();
        handle.commit(response_id, outbound);
        Ok(Response::new(PickResponse {
            outcome: PickOutcome::Ok as i32,
            // The mock serves from its own static registry and has no notion of
            // an off-cluster destination, so this arm is always local.
            destination: Some(crate::proto::pick_response::Destination::Local(
                crate::proto::LocalTarget { endpoint },
            )),
            response_id,
            reason: String::new(),
            body_total_bytes,
            requested_model: model,
        }))
    }

    type PullBodyStream = ReceiverStream<Result<BodyChunk, Status>>;

    async fn pull_body(
        &self,
        request: Request<PullBodyRequest>,
    ) -> Result<Response<Self::PullBodyStream>, Status> {
        let req = request.into_inner();
        let lease = self.body_store.take_outbound(req.response_id)?;
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let body = lease.body().clone();
            for slice in body.chunks(64 * 1024) {
                if tx
                    .send(Ok(BodyChunk {
                        chunk: slice.to_vec(),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn abort(
        &self,
        request: Request<AbortRequest>,
    ) -> Result<Response<AbortResponse>, Status> {
        let req = request.into_inner();
        let (released, _bytes) = self.body_store.abort(req.response_id);
        Ok(Response::new(AbortResponse { released }))
    }

    async fn report_first_token(
        &self,
        _request: Request<ReportFirstTokenRequest>,
    ) -> Result<Response<ReportFirstTokenResponse>, Status> {
        // Mock holds no pending-observation state — every report is
        // treated as acknowledging a known-but-stateless pick.
        Ok(Response::new(ReportFirstTokenResponse {
            acknowledged: true,
        }))
    }

    async fn report_final_token(
        &self,
        _request: Request<ReportFinalTokenRequest>,
    ) -> Result<Response<ReportFinalTokenResponse>, Status> {
        // Mock records no metrics; acknowledge and move on.
        Ok(Response::new(ReportFinalTokenResponse {
            acknowledged: true,
        }))
    }
}

#[cfg(test)]
mod tests;
