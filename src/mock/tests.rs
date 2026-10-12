use super::*;
use crate::workers::WorkerEntry;

fn registry_with_model(model: &str) -> Arc<WorkerRegistry> {
    let reg = Arc::new(WorkerRegistry::new());
    reg.register(
        1,
        WorkerEntry {
            worker_id_str: "w1".to_string(),
            model_name_hint: Some(model.to_string()),
            current_models: vec![model.to_string()],
            http_url: Some("http://10.0.0.1:8000/".to_string()),
            block_size: 16,
            initial_sync_complete: true,
            kv_events_observed: true,
            score_addr: "10.0.0.1:8000".to_string(),
            engine: None,
            pod_name: None,
            namespace: None,
            node_name: None,
            node_ip: None,
            pod_ip: None,
            availability_zone: None,
            instance_type: None,
        },
    );
    reg
}

/// Seed a pushed body and return its `stream_id`.
///
/// Goes through the store rather than the `push_body` RPC because
/// `tonic::Streaming` can only be built by the transport layer. The drain
/// loop itself (idle budget, byte budgets, half-close vs reset) is unit
/// tested in `crate::grpc`, and the full streaming RPC is covered by the
/// mock-mode grpcurl e2e.
async fn push(svc: &MockKinferenceService, body: &str) -> u64 {
    let mut r = svc.body_store.reservation();
    r.charge(body.len() as u64).expect("within budget");
    svc.body_store.commit(r, body.as_bytes().to_vec())
}

/// Drain a `PullBody` stream into the reassembled body.
async fn pull(svc: &MockKinferenceService, response_id: u64) -> Result<Vec<u8>, Status> {
    use tokio_stream::StreamExt;
    let mut stream = svc
        .pull_body(Request::new(PullBodyRequest { response_id }))
        .await?
        .into_inner();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?.chunk);
    }
    Ok(out)
}

/// Full round-trip: push → pick → pull, returning the pick response and the
/// reassembled outbound body.
async fn round_trip(svc: &MockKinferenceService, body: &str) -> (PickResponse, Vec<u8>) {
    let stream_id = push(svc, body).await;
    let resp = svc
        .pick(Request::new(PickRequest {
            body_stream_id: stream_id,
            client_request_id: String::new(),
            qos_class: 0, // INTERACTIVE
            inference_pool: None,
            tenant: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    let pulled = pull(svc, resp.response_id).await.unwrap_or_default();
    (resp, pulled)
}

// The mock runs the real body store and the real `pick_body` rewrites, so
// these lock in the whole push/pick/pull wire contract.

#[tokio::test]
async fn auto_plus_stream_rewrites_model_and_adds_include_usage() {
    let svc = MockKinferenceService::new(registry_with_model("qwen-test"));
    let (resp, body) = round_trip(
        &svc,
        r#"{"model":"auto","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(resp.outcome, PickOutcome::Ok as i32);
    assert_eq!(resp.requested_model, "qwen-test");
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["model"], serde_json::json!("qwen-test"));
    assert_eq!(
        v["stream_options"]["include_usage"],
        serde_json::json!(true)
    );
    // The advertised length must match the REWRITTEN body exactly.
    assert_eq!(resp.body_total_bytes, body.len() as u64);
}

#[tokio::test]
async fn unchanged_body_round_trips_byte_for_byte() {
    let svc = MockKinferenceService::new(registry_with_model("qwen-test"));
    let original = r#"{"model":"qwen-test","messages":[{"role":"user","content":"hi"}]}"#;
    let (resp, body) = round_trip(&svc, original).await;
    assert_eq!(resp.outcome, PickOutcome::Ok as i32);
    // Nothing to rewrite → PullBody still returns a COMPLETE body (the old
    // "empty means unchanged" wire contract is gone).
    assert_eq!(body, original.as_bytes());
    assert_eq!(resp.body_total_bytes, original.len() as u64);
}

#[tokio::test]
async fn concrete_model_stream_adds_include_usage_only() {
    let svc = MockKinferenceService::new(registry_with_model("qwen-test"));
    let (resp, body) = round_trip(
        &svc,
        r#"{"model":"qwen-test","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(resp.outcome, PickOutcome::Ok as i32);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["model"], serde_json::json!("qwen-test"));
    assert_eq!(
        v["stream_options"]["include_usage"],
        serde_json::json!(true)
    );
}

#[tokio::test]
async fn pull_body_is_single_shot() {
    let svc = MockKinferenceService::new(registry_with_model("qwen-test"));
    let (resp, _) = round_trip(&svc, r#"{"model":"qwen-test","messages":[]}"#).await;
    // round_trip already pulled once; a second pull must not find it.
    let err = pull(&svc, resp.response_id).await.expect_err("single-shot");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn pick_on_unknown_stream_is_not_found() {
    let svc = MockKinferenceService::new(registry_with_model("qwen-test"));
    let err = svc
        .pick(Request::new(PickRequest {
            body_stream_id: 123456,
            client_request_id: String::new(),
            qos_class: 0,
            inference_pool: None,
            tenant: String::new(),
        }))
        .await
        .expect_err("unknown body");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn abort_releases_then_reports_already_released() {
    let svc = MockKinferenceService::new(registry_with_model("qwen-test"));
    let stream_id = push(&svc, r#"{"model":"qwen-test","messages":[]}"#).await;
    let resp = svc
        .pick(Request::new(PickRequest {
            body_stream_id: stream_id,
            client_request_id: String::new(),
            qos_class: 0,
            inference_pool: None,
            tenant: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    let abort = |id| {
        svc.abort(Request::new(AbortRequest {
            response_id: id,
            reason: 1, // NOT_DISPATCHED
            detail: String::new(),
            client_request_id: String::new(),
        }))
    };
    let first = abort(resp.response_id).await.unwrap().into_inner();
    assert!(first.released);
    // Idempotent: a duplicate abort is `released: false`, never an error.
    let second = abort(resp.response_id).await.unwrap().into_inner();
    assert!(!second.released);
}

#[tokio::test]
async fn no_workers_releases_the_body_and_reports_the_model() {
    let svc = MockKinferenceService::new(registry_with_model("qwen-test"));
    let stream_id = push(&svc, r#"{"model":"nope","messages":[]}"#).await;
    let resp = svc
        .pick(Request::new(PickRequest {
            body_stream_id: stream_id,
            client_request_id: String::new(),
            qos_class: 0,
            inference_pool: None,
            tenant: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.outcome, PickOutcome::NoWorkers as i32);
    assert_eq!(resp.requested_model, "nope");
    assert_eq!(resp.response_id, 0, "no id when nothing was retained");
    assert_eq!(resp.body_total_bytes, 0);
    // A non-OK pick frees the body immediately rather than at TTL.
    assert_eq!(svc.body_store.stats().1, 0);
}

/// End-to-end through the real pick path, not just the helper: a client that
/// sends `stream` as a STRING (which Pydantic accepts, so the engine really
/// does stream) must still get `include_usage` forced on. Covers the second
/// reader of the flag too — `RequestMeta.streaming` shares `stream_flag`, so
/// a regression that reverted one site would show up here.
#[tokio::test]
async fn string_valued_stream_still_adds_include_usage() {
    let svc = MockKinferenceService::new(registry_with_model("qwen-test"));
    let (resp, body) = round_trip(
        &svc,
        r#"{"model":"qwen-test","stream":"true","messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(resp.outcome, PickOutcome::Ok as i32);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v["stream_options"]["include_usage"],
        serde_json::json!(true)
    );
    assert_eq!(resp.body_total_bytes, body.len() as u64);
}
