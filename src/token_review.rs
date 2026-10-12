//! `TokenReview` gRPC service — validate a Kubernetes bearer token for a caller.
//!
//! Mirrors `gateway:filters/token-review` in gw-pc, whose host implementation
//! this is modelled on. The caller is typically a wasi-p2 gateway plugin, which
//! has neither outbound HTTPS nor crypto and so cannot make the
//! `authentication.k8s.io/v1` round-trip itself; kinference already holds an
//! in-cluster ServiceAccount and CA, so it can.
//!
//! `TokenReview` is a cluster-scoped, non-persisted "virtual" resource: you
//! `create` one and the apiserver hands back the same object with `status`
//! filled in. Nothing is stored, so there is no cleanup and no conflict.
//!
//! ## Failing to authenticate is not an error
//!
//! `authenticated = false` is a successful call that reached the apiserver and
//! got a "no". A gRPC error means kinference could not determine the answer at all.
//! Those are different instructions to the caller — "reject this request" versus
//! "retry, or fail open, your policy" — so they never collapse into one.
//!
//! ## Why the cache is bounded, unlike the reference
//!
//! Bound ServiceAccount tokens ROTATE. An unbounded map keyed by token would
//! therefore grow without limit in steady state, one entry per rotation per
//! caller, and never shrink — a slow leak that only shows up in long-lived
//! processes. [`ReviewCache`] evicts expired entries as it goes and caps its
//! size, so a rotating fleet reaches a stable footprint.
//!
//! ## The cache key is a CRYPTOGRAPHIC hash, and that is not fussiness
//!
//! SHA-256 of the token, never the token itself — the process should not hold
//! credentials it does not need. A fast non-cryptographic hash would be wrong
//! for a second reason: an attacker who can present tokens could construct a
//! collision against a cached positive and be handed someone else's
//! authenticated identity.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use k8s_openapi::api::authentication::v1::{TokenReview as K8sTokenReview, TokenReviewSpec};
use kube::api::{Api, PostParams};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::proto::token_review_server::{TokenReview, TokenReviewServer};
use crate::proto::{ReviewTokenRequest, ReviewTokenResponse};

mod config;
mod metrics;

pub use config::TokenReviewConfig;
use metrics::Metrics;

/// Build the service, or `None` when `[token_review] enabled = false`.
///
/// The toggle is read here rather than at the call site so `main` never has to
/// know this subsystem has one; `add_optional_service` takes the `Option`
/// unchanged.
///
/// This is the one place a kube client may be built, and it deliberately holds
/// no logic beyond acquiring one and logging — [`KubeTokenReview`] takes the
/// client as an argument, so everything with behaviour in it stays drivable
/// without credentials. `listen` is used only for the log line.
///
/// The client is built HERE, at startup, and a failure is fatal. Building one
/// does not contact the apiserver — it reads credentials — so this is
/// deterministic: if it fails, the deployment is misconfigured and every call
/// would return UNAVAILABLE forever. Better to refuse to start than to serve
/// an auth endpoint that can only ever fail.
pub async fn start(
    cfg: &TokenReviewConfig,
    registry: &prometheus::Registry,
    listen: std::net::SocketAddr,
) -> anyhow::Result<Option<TokenReviewServer<KubeTokenReview>>> {
    if !cfg.enabled {
        return Ok(None);
    }
    // `infer()` rather than `incluster_env()`, matching the policy watcher:
    // this only talks to the apiserver, so kubeconfig is safe and makes the
    // whole path testable against a local cluster. (The strict variant exists
    // for DISCOVERY, where honoring kubeconfig would hand back pod IPs that are
    // unreachable.)
    let client = kube::Client::try_from(kube::Config::infer().await.map_err(|e| {
        anyhow::anyhow!(
            "[token_review] enabled = true but no Kubernetes credentials are \
             available (no in-cluster service-account token and no usable \
             kubeconfig). Set enabled = false to run without it. ({e})"
        )
    })?)?;
    tracing::info!(
        listen = %listen,
        positive_ttl_secs = cfg.positive_ttl_secs,
        negative_ttl_secs = cfg.negative_ttl_secs,
        max_cache_entries = cfg.max_cache_entries,
        "READY: TokenReview validating bearer tokens against the apiserver"
    );
    Ok(Some(TokenReviewServer::new(KubeTokenReview::new(
        client,
        cfg,
        Metrics::new(registry),
    ))))
}

/// What the apiserver said. Small and `Clone` so the cache can hand out copies.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Review {
    authenticated: bool,
    username: String,
}

impl Review {
    fn denied() -> Self {
        Self {
            authenticated: false,
            username: String::new(),
        }
    }
}

/// TTL cache of recent review results, keyed by `(sha256(token), audiences)`.
///
/// Audiences are part of the key because the same token can be valid for one
/// audience and not another — keying on the token alone would let a hit for
/// audience A answer a question about audience B.
struct ReviewCache {
    entries: HashMap<(String, Vec<String>), (Instant, Review)>,
    positive_ttl: Duration,
    negative_ttl: Duration,
    max_entries: usize,
}

impl ReviewCache {
    fn new(positive_ttl: Duration, negative_ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            positive_ttl,
            negative_ttl,
            // Zero would make every insert evict everything; treat it as "no
            // caching" rather than as a footgun.
            max_entries: max_entries.max(1),
        }
    }

    /// A still-valid result, or `None`.
    ///
    /// Takes `now` so expiry is testable without sleeping.
    fn get(&self, key: &(String, Vec<String>), now: Instant) -> Option<Review> {
        self.entries
            .get(key)
            .filter(|(expires_at, _)| *expires_at > now)
            .map(|(_, review)| review.clone())
    }

    /// Store a result, dropping anything expired on the way through.
    ///
    /// A negative gets a SHORTER TTL than a positive: a token that has just
    /// become valid should start working promptly, while a token that is simply
    /// wrong is usually presented repeatedly and is worth absorbing.
    fn insert(&mut self, key: (String, Vec<String>), review: Review, now: Instant) {
        let ttl = if review.authenticated {
            self.positive_ttl
        } else {
            self.negative_ttl
        };
        // Opportunistic sweep, so a rotating fleet does not accumulate dead
        // entries between the rare moments the cap is hit.
        self.entries.retain(|_, (expires_at, _)| *expires_at > now);
        if self.entries.len() >= self.max_entries {
            // At the cap with nothing expired: drop the soonest-to-expire, which
            // is the entry with the least remaining value.
            if let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, (expires_at, _))| *expires_at)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&victim);
            }
        }
        self.entries.insert(key, (now + ttl, review));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Validates tokens against the cluster apiserver.
pub struct KubeTokenReview {
    client: kube::Client,
    cache: Mutex<ReviewCache>,
    default_deadline: Duration,
    metrics: Metrics,
}

impl KubeTokenReview {
    fn new(client: kube::Client, cfg: &TokenReviewConfig, metrics: Metrics) -> Self {
        Self {
            client,
            cache: Mutex::new(ReviewCache::new(
                Duration::from_secs(cfg.positive_ttl_secs),
                Duration::from_secs(cfg.negative_ttl_secs),
                cfg.max_cache_entries,
            )),
            default_deadline: Duration::from_millis(cfg.default_deadline_ms),
            metrics,
        }
    }

    /// One apiserver round-trip. No caching, no validation — the caller does
    /// both.
    async fn ask_apiserver(
        &self,
        token: &str,
        audiences: &[String],
        deadline: Duration,
    ) -> Result<Review, Status> {
        let review = K8sTokenReview {
            spec: TokenReviewSpec {
                token: token.to_string(),
                // `Some(vec![])` and `None` are NOT the same to the apiserver:
                // an empty list means "check against the apiserver's own
                // audience", which is a weaker check than a gateway wants. Send
                // `None` only when the caller genuinely named none.
                audiences: (!audiences.is_empty()).then(|| audiences.to_vec()),
            },
            ..Default::default()
        };

        let api: Api<K8sTokenReview> = Api::all(self.client.clone());
        let created =
            match tokio::time::timeout(deadline, api.create(&PostParams::default(), &review)).await
            {
                Ok(Ok(created)) => created,
                Ok(Err(err)) => {
                    // The error may quote the request body, which contains the
                    // token. Log the TYPE and let the operator correlate; never
                    // interpolate the error into a response the caller sees either.
                    tracing::warn!(
                        error = %err,
                        "TokenReview: apiserver rejected the review request"
                    );
                    self.metrics.record("transport");
                    return Err(Status::unavailable(
                        "could not complete a TokenReview against the apiserver",
                    ));
                }
                Err(_) => {
                    self.metrics.record("timeout");
                    return Err(Status::deadline_exceeded(
                        "the apiserver did not answer the TokenReview within the deadline",
                    ));
                }
            };

        // A response with no status at all is not an authentication. Treated as
        // a denial rather than an error: the call succeeded, and the apiserver
        // declining to say yes is a no.
        Ok(created.status.map_or_else(Review::denied, |status| {
            let authenticated = status.authenticated.unwrap_or(false);
            // The apiserver's own words, and the difference between "wrong
            // audience" and "expired". Not the token, so it is safe to log —
            // but it stays out of the RESPONSE, since the caller only needs yes
            // or no.
            if !authenticated && let Some(reason) = status.error.as_deref() {
                tracing::debug!(reason, "TokenReview: apiserver reported a reason");
            }
            Review {
                authenticated,
                // Only carry an identity when there is one to carry — a username
                // beside `authenticated: false` invites a caller to use it.
                username: if authenticated {
                    status.user.and_then(|u| u.username).unwrap_or_default()
                } else {
                    String::new()
                },
            }
        }))
    }
}

#[tonic::async_trait]
impl TokenReview for KubeTokenReview {
    async fn review_token(
        &self,
        request: Request<ReviewTokenRequest>,
    ) -> Result<Response<ReviewTokenResponse>, Status> {
        let req = request.into_inner();

        if req.token.trim().is_empty() {
            self.metrics.record("invalid");
            return Err(Status::invalid_argument("token must not be empty"));
        }

        // Normalized so callers that send the same audiences in a different
        // order share a cache entry rather than each paying a round-trip.
        let mut audiences = req.audiences.clone();
        audiences.sort();
        audiences.dedup();

        let key = (hash_token(&req.token), audiences.clone());
        let now = Instant::now();

        if let Some(hit) = self.cache.lock().await.get(&key, now) {
            self.metrics.record(if hit.authenticated {
                "cached_authenticated"
            } else {
                "cached_denied"
            });
            return Ok(Response::new(ReviewTokenResponse {
                authenticated: hit.authenticated,
                username: hit.username,
            }));
        }

        // One budget, from config. A gRPC deadline the client set is enforced by
        // the transport independently — it drops the future — so the effective
        // budget is still the shorter of the two.
        let review = self
            .ask_apiserver(&req.token, &audiences, self.default_deadline)
            .await?;

        self.metrics.record(if review.authenticated {
            "authenticated"
        } else {
            "denied"
        });
        if !review.authenticated {
            // Audiences, never the token. A denial is the interesting event for
            // an operator debugging a misminted audience.
            tracing::debug!(
                ?audiences,
                "TokenReview: apiserver did not authenticate this token"
            );
        }

        self.cache.lock().await.insert(key, review.clone(), now);
        Ok(Response::new(ReviewTokenResponse {
            authenticated: review.authenticated,
            username: review.username,
        }))
    }
}

/// Hex SHA-256 of the token. The cache key never holds the raw credential, and
/// the hash must be cryptographic — see the module docs.
fn hash_token(token: &str) -> String {
    use std::fmt::Write;
    let digest = Sha256::digest(token.as_bytes());
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests;
