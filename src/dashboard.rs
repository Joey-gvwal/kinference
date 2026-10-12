//! In-process live dashboard HTTP server — the admin listener.
//!
//! Serves the bundled SPA (embedded from `dashboard/dist`); the JSON API
//! under `/api` — `GET /api/snapshot`, the telemetry snapshot the frontend
//! polls, and the read-only policy API (`GET /api/policy/context`,
//! `POST /api/policy/simulate`, see [`policy_api`]); `GET /metrics`; and
//! `GET /readyz`, used by the K8s readinessProbe. Only started when
//! `admin_listen` is configured — deployments that disable the dashboard also
//! lose the readiness probe, so leave it enabled in production.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{FromRef, Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use rust_embed::RustEmbed;
use tokio_util::sync::CancellationToken;

use crate::metrics::MetricsRegistry;
use crate::pool::PoolRegistry;
use crate::prom::PromMetrics;
use crate::telemetry::Telemetry;

mod policy_api;

#[derive(RustEmbed)]
#[folder = "dashboard/dist"]
struct Assets;

/// State threaded through every axum route via `FromRef`. Holds the
/// Arcs the handlers below need (telemetry for `/api/snapshot`, the pool
/// set and policy store for `/readyz`, Prometheus registry for `/metrics`).
#[derive(Clone)]
struct AppState {
    telemetry: Arc<Telemetry>,
    /// The live pool set, not a startup snapshot of its registries — so
    /// `/readyz` can report how much of the fleet has resolved, and pools that
    /// appear after the server started still count.
    pools: Arc<PoolRegistry>,
    /// CRD policy, when enabled. `/readyz` gates on this having LISTED, which
    /// is the one startup state where answering a Pick is worse than refusing
    /// it — see [`readyz`].
    policy: Option<Arc<crate::policy::store::PolicyStore>>,
    /// Prometheus exposition registry for `/metrics` (same `Arc` the
    /// gRPC `report_final_token` handler records into).
    prom: Arc<PromMetrics>,
    /// The policy API's own state — snapshot readers plus the semaphore that
    /// bounds concurrent simulations. Built once, so every request shares one
    /// semaphore; see [`policy_api::PolicyApiState`].
    policy_api: policy_api::PolicyApiState,
}

impl FromRef<AppState> for Arc<Telemetry> {
    fn from_ref(state: &AppState) -> Self {
        state.telemetry.clone()
    }
}

impl FromRef<AppState> for Arc<PoolRegistry> {
    fn from_ref(state: &AppState) -> Self {
        state.pools.clone()
    }
}

/// What `/readyz` needs: the fleet state to report, and the policy store to
/// gate on. One extractor because axum allows a handler only one `State`.
#[derive(Clone)]
struct ReadyzState {
    pools: Arc<PoolRegistry>,
    policy: Option<Arc<crate::policy::store::PolicyStore>>,
}

impl FromRef<AppState> for ReadyzState {
    fn from_ref(state: &AppState) -> Self {
        Self {
            pools: state.pools.clone(),
            policy: state.policy.clone(),
        }
    }
}

impl FromRef<AppState> for Arc<PromMetrics> {
    fn from_ref(state: &AppState) -> Self {
        state.prom.clone()
    }
}

/// Every route on the admin listener.
///
/// Everything under `/api` answers in JSON, failures included. Without the
/// two fallbacks below, an unknown `/api/...` GET fell through to `/*path` and
/// got the SPA's `index.html` with a 200 — a typo'd endpoint read as a
/// successful response that would not parse — and a wrong method got axum's
/// empty 405. Callers here are increasingly programs, and a program should
/// never have to sniff a content type to learn it called the wrong URL.
fn router(state: AppState) -> Router {
    let get_only = || async { policy_api::method_not_allowed("GET, HEAD") };
    Router::new()
        .route("/api/snapshot", get(snapshot).fallback(get_only))
        .route(
            "/api/policy/context",
            get(policy_api::context).fallback(get_only),
        )
        .route(
            "/api/policy/simulate",
            post(policy_api::simulate)
                .fallback(|| async { policy_api::method_not_allowed("POST") }),
        )
        .route("/api", any(policy_api::not_found))
        .route("/api/*rest", any(policy_api::not_found))
        .route("/metrics", get(metrics))
        .route("/readyz", get(readyz))
        .route("/", get(|| async { serve_asset("index.html") }))
        .route("/*path", get(asset))
        // Whatever the routes above cannot match. Not only truly unknown
        // paths: matchit does not backtrack from a static prefix into a
        // catch-all, so `/api/policy/context/` — a trailing slash — matched
        // neither the endpoint nor `/api/*rest`, and got axum's default: a 404
        // with an empty body.
        .fallback(fallback)
        // Applies to every route, but only `simulate` reads a body.
        .layer(axum::extract::DefaultBodyLimit::max(
            policy_api::MAX_BODY_BYTES,
        ))
        .with_state(state)
}

/// The router-wide fallback: JSON for anything under `/api`, the SPA for the
/// rest — the same split the routes themselves make.
async fn fallback(uri: axum::http::Uri) -> Response {
    let path = uri.path();
    if path == "/api" || path.starts_with("/api/") {
        policy_api::not_found(uri).await.into_response()
    } else {
        serve_asset(path.trim_start_matches('/'))
    }
}

pub async fn serve(
    addr: SocketAddr,
    telemetry: Arc<Telemetry>,
    pools: Arc<PoolRegistry>,
    policy: Option<Arc<crate::policy::store::PolicyStore>>,
    prom: Arc<PromMetrics>,
    metrics_registry: Arc<MetricsRegistry>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let state = AppState {
        policy_api: policy_api::PolicyApiState::new(
            policy.clone(),
            pools.clone(),
            metrics_registry,
        ),
        telemetry,
        pools,
        policy,
        prom,
    };
    let app = router(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Dashboard READY: serving live dashboard over HTTP");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;
    Ok(())
}

async fn snapshot(State(tel): State<Arc<Telemetry>>) -> Json<serde_json::Value> {
    Json(tel.snapshot_json())
}

/// Prometheus scrape target. Renders kinference's own metric set (currently
/// per-model token counters fed by `ReportFinalToken`) in the
/// text-exposition format.
async fn metrics(State(prom): State<Arc<PromMetrics>>) -> Response {
    (
        [(header::CONTENT_TYPE, PromMetrics::content_type())],
        prom.render(),
    )
        .into_response()
}

/// K8s readinessProbe target. Returns 200 once at least one worker has
/// resolved both its model list (via `/v1/models` scrape) AND its
/// initial ZMQ event-stream sync. Returns 503 otherwise so K8s holds
/// client traffic off the pod through the cold-start window. See
/// [`crate::workers::WorkerEntry::is_ready`] for the per-worker
/// definition.
/// A pool set that is empty and one whose workers are all still cold both
/// return 503, but they are different operator problems — "kinference has nothing
/// to route to" versus "the engines are still warming" — so they say so.
/// Readiness: can this process answer a `Pick`?
///
/// Deliberately NOT "is there anywhere to route to". A `Pick` for a model
/// nothing serves has a correct answer — `NO_WORKERS` — and refusing traffic
/// rather than giving it is worse on both ends: the caller cannot distinguish a
/// cold router from a broken one, and a provider-only deployment (which has no
/// workers at all, since a provider is not backed by a pool) would never become
/// Ready despite being able to route perfectly well.
///
/// One gate, and it is the only startup state where answering is worse than
/// refusing: with policy enabled but the CRD reflectors not yet listed, a `Pick`
/// naming an `InferencePool` cannot be told from one naming a pool that does not
/// exist — so it routes on load alone and SILENTLY BYPASSES the policy. Serving
/// that is a wrong answer, not an empty one. Everything else about startup —
/// no worker sets, no endpoints, no tokenizer yet — degrades honestly.
async fn readyz(State(st): State<ReadyzState>) -> Response {
    if let Some(store) = st.policy.as_ref()
        && !store.snapshot().listed()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "policy is enabled but the CRD watch has not listed yet; a Pick naming a pool \
             would silently bypass its policy\n",
        )
            .into_response();
    }

    // Ready either way; the body carries the fleet state that used to decide it,
    // because `curl /readyz` was a useful way to see it.
    let snapshot = st.pools.snapshot();
    let pools_n = snapshot.len();
    let ready: usize = snapshot.iter().filter(|p| p.registry.any_ready()).count();
    (
        StatusCode::OK,
        format!("ready\nworker sets: {pools_n}, with ready endpoints: {ready}\n"),
    )
        .into_response()
}

async fn asset(Path(path): Path<String>) -> Response {
    serve_asset(&path)
}

/// Serve an embedded asset by path. Falls back to `index.html` for unknown
/// non-asset routes (SPA client-side routing), and 404s only if assets aren't
/// embedded at all (dist missing at build time).
fn serve_asset(path: &str) -> Response {
    let p = path.trim_start_matches('/');
    let p = if p.is_empty() { "index.html" } else { p };
    if let Some(content) = Assets::get(p) {
        let mime = mime_guess::from_path(p).first_or_octet_stream();
        return (
            [(header::CONTENT_TYPE, mime.as_ref())],
            content.data.into_owned(),
        )
            .into_response();
    }
    match Assets::get("index.html") {
        Some(content) => (
            [(header::CONTENT_TYPE, "text/html")],
            content.data.into_owned(),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "dashboard assets not embedded; run `cd dashboard && npm run build`",
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests;
