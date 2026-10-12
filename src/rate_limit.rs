//! `RateLimiter` gRPC service — token-budget reserve/reconcile for callers.
//!
//! Mirrors `btrl.v1.RateLimiter` in gw-pc's token_rate_limiter plugin. A caller
//! cannot know a request's true token cost before running it, only its upper
//! bound, so it reserves the bound and commits the actual usage afterwards.
//! Holding the bound is what makes the limit safe under concurrency: N
//! simultaneous requests cannot collectively overrun a window while all of them
//! are in flight and none has reported usage.
//!
//! # THIS IMPLEMENTATION LIMITS NOTHING
//!
//! [`NoopRateLimiter`] admits every reservation. It is a wiring placeholder so
//! callers can be built against a real endpoint before the sliding-window
//! accounting lands.
//!
//! Unlike a stubbed authenticator, this fails OPEN in the direction absence
//! already points — a caller with no rate limiter at all is equally unlimited —
//! so the risk is not a security hole but a false sense of protection, and the
//! bill that follows. Two guards:
//!
//! 1. Off unless `[rate_limit] enabled = true`.
//! 2. Turning it on logs a `WARN` at startup saying it enforces nothing.
//!
//! There is deliberately **no per-call logging**. This service is on the
//! per-request path of everything the gateway admits — a line per call would
//! cost more than the limiting it is standing in for. The counters carry the
//! per-call signal instead.
//!
//! ## What the stub reports, and why those values
//!
//! `limit = 0` and `window_ms = 0`, which the proto defines as "no window is
//! enforced". The alternative of reporting a large fake budget would have
//! callers render a confident `X-RateLimit-Remaining` for a window that does not
//! exist; reporting `remaining = 0` alone would read as *throttled*, the exact
//! opposite of the truth. Zero limit is the one combination that says "not
//! applicable" rather than lying in either direction.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};

use tonic::{Request, Response, Status};

use crate::proto::rate_limiter_server::{RateLimiter, RateLimiterServer};
use crate::proto::{
    CommitRequest, CommitResponse, CommitStatus, DenialReason, ReserveRequest, ReserveResponse,
};

mod config;
mod metrics;

pub use config::RateLimitConfig;
use metrics::Metrics;

/// Build the service, or `None` when `[rate_limit] enabled = false`.
///
/// The toggle is read here rather than at the call site so `main` never has to
/// know this subsystem has one; `add_optional_service` takes the `Option`
/// unchanged. Needs no client and no external dependency, so unlike
/// `TokenReview` there is nothing here that can fail at startup — hence no
/// `Result`.
///
/// `listen` is used only for the log line, so an operator reading startup
/// output can see which listener grew a service.
pub fn start(
    cfg: &RateLimitConfig,
    registry: &prometheus::Registry,
    listen: SocketAddr,
) -> Option<RateLimiterServer<NoopRateLimiter>> {
    if !cfg.enabled {
        return None;
    }
    NoopRateLimiter::warn_on_startup();
    tracing::info!(
        listen = %listen,
        "RateLimiter service enabled on the gRPC listener"
    );
    Some(RateLimiterServer::new(NoopRateLimiter::new(Metrics::new(
        registry,
    ))))
}

/// Admits everything. See the module docs.
pub struct NoopRateLimiter {
    /// Handles for `Commit` to quote back.
    ///
    /// A counter rather than a random id: the stub tracks no reservations, so a
    /// handle carries no authority and guessability buys an attacker nothing.
    /// The real implementation will need unguessable ids, since a handle will
    /// then name someone's budget.
    next_request_id: AtomicI64,
    metrics: Metrics,
}

impl NoopRateLimiter {
    fn new(metrics: Metrics) -> Self {
        Self {
            // Starts at 1: 0 is reserved on the wire for "not allowed", so it
            // must never be handed out as a usable handle.
            next_request_id: AtomicI64::new(1),
            metrics,
        }
    }

    /// Announce that nothing is being enforced.
    fn warn_on_startup() {
        tracing::warn!(
            "RateLimiter is served by the NO-OP implementation: every reservation is \
             admitted and no token budget is tracked. A caller cannot tell the \
             difference between this and a working limiter, so do not rely on it to \
             bound spend."
        );
    }

    fn mint_request_id(&self) -> i64 {
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        // Wrapping past i64::MAX would eventually hand out 0, which means "not
        // allowed" on the wire. Unreachable in practice at any real request
        // rate; cheap to make impossible rather than argue about.
        if id <= 0 {
            self.next_request_id.store(1, Ordering::Relaxed);
            return 1;
        }
        id
    }
}

#[tonic::async_trait]
impl RateLimiter for NoopRateLimiter {
    async fn reserve(
        &self,
        request: Request<ReserveRequest>,
    ) -> Result<Response<ReserveResponse>, Status> {
        let req = request.into_inner();

        // Enforced even by the stub, because both are caller bugs that need no
        // accounting to detect — and catching them now means the behavior does
        // not change under the caller when the real limiter lands.
        if req.tenant_id.trim().is_empty() {
            self.metrics.record("invalid");
            return Err(Status::invalid_argument("tenant_id must not be empty"));
        }
        if req.max_tokens < 0 {
            self.metrics.record("invalid");
            return Err(Status::invalid_argument("max_tokens must not be negative"));
        }

        self.metrics.record("allowed");
        Ok(Response::new(ReserveResponse {
            allowed: true,
            request_id: self.mint_request_id(),
            retry_after_ms: 0,
            // `limit = 0` is the proto's "no window is enforced" — see the
            // module docs for why this beats a fake budget.
            limit: 0,
            remaining: 0,
            window_ms: 0,
            reason: DenialReason::Unspecified as i32,
        }))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let req = request.into_inner();

        // 0 is the wire value for "Reserve did not allow this", so committing
        // against it means the caller skipped or misread the reservation.
        // NOT_FOUND is both truthful and the thing that surfaces the bug, and it
        // is what the real implementation will say too.
        let status = if req.request_id <= 0 {
            self.metrics.record("commit_not_found");
            CommitStatus::NotFound
        } else {
            // Any other handle is accepted. The stub holds no reservations, so
            // it has nothing to fail to find, and answering NOT_FOUND for every
            // commit would make a correctly-written caller log an error per
            // request.
            self.metrics.record("commit_ok");
            CommitStatus::Ok
        };

        Ok(Response::new(CommitResponse {
            status: status as i32,
        }))
    }
}

#[cfg(test)]
mod tests;
