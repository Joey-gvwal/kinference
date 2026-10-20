//! Fakes for the tokenizer tests: a `/render` on a loopback port and the
//! worker entries that point at it.

use std::sync::{Arc, Mutex};

use super::{TokenizeInput, TokenizeRequest};
use kinference_fleet::workers::WorkerEntry;

/// What the fake `/render` answers.
#[derive(Clone)]
pub(crate) enum Render {
    /// Every request tokenizes to these ids.
    Tokens(Vec<u32>),
    /// Every request gets a 500.
    Failing,
    /// No worker has an `http_url`, so nothing is tried.
    Absent,
}

/// One request the fake `/render` was sent.
#[derive(Clone)]
pub(crate) struct Seen {
    pub path: String,
    pub body: Vec<u8>,
}

/// A `/render` on a loopback port that gives every request the same answer
/// and remembers what it was asked. Stops when dropped.
pub(crate) struct FakeRender {
    base: String,
    requests: Arc<Mutex<Vec<Seen>>>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeRender {
    pub(crate) async fn start(answer: Render) -> Self {
        use axum::response::IntoResponse as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        // Any path: a worker is POSTed `{http_url}/v1/chat/completions/render`.
        let app =
            axum::Router::new().fallback(move |uri: axum::http::Uri, body: axum::body::Bytes| {
                seen.lock().expect("requests").push(Seen {
                    path: uri.path().to_string(),
                    body: body.to_vec(),
                });
                let answer = answer.clone();
                async move {
                    match answer {
                        Render::Tokens(ids) => {
                            axum::Json(serde_json::json!({ "token_ids": ids })).into_response()
                        }
                        Render::Failing => {
                            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "render down")
                                .into_response()
                        }
                        Render::Absent => unreachable!("no server is started for Absent"),
                    }
                }
            });
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve the fake /render");
        });
        Self {
            base,
            requests,
            task,
        }
    }

    /// The `http_url` a worker needs to reach this fake.
    pub(crate) fn base(&self) -> &str {
        &self.base
    }

    pub(crate) fn calls(&self) -> usize {
        self.requests.lock().expect("requests").len()
    }

    /// Every request so far, oldest first.
    pub(crate) fn requests(&self) -> Vec<Seen> {
        self.requests.lock().expect("requests").clone()
    }
}

impl Drop for FakeRender {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A worker serving `model` at `addr`, tokenizing at `http_url`.
pub(crate) fn worker(
    model: &str,
    addr: &str,
    ready: bool,
    http_url: Option<String>,
) -> WorkerEntry {
    WorkerEntry {
        worker_id_str: addr.into(),
        model_name_hint: Some(model.into()),
        current_models: vec![model.into()],
        http_url,
        block_size: 16,
        initial_sync_complete: ready,
        kv_events_observed: true,
        score_addr: addr.into(),
        engine: None,
        pod_name: None,
        namespace: None,
        node_name: None,
        node_ip: None,
        pod_ip: None,
        availability_zone: None,
        instance_type: None,
    }
}

/// A request over `workers` with no dedicated URL, raw body or request id.
pub(crate) fn request<'a>(
    workers: &'a [(u64, WorkerEntry)],
    model: &'a str,
    input: TokenizeInput,
) -> TokenizeRequest<'a> {
    TokenizeRequest {
        render_url: None,
        workers,
        model,
        input,
        raw_body: None,
        req_id: None,
    }
}
