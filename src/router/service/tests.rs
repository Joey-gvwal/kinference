use super::*;
use crate::router::service::test_support::{ServiceBuilder, TestService};

use kinference_fleet::pool::test_pool;

fn score(endpoint: &str, queue_waiting: u32) -> WorkerScore {
    WorkerScore {
        name: endpoint.to_string(),
        endpoint: endpoint.to_string(),
        matched_blocks: 0,
        completion_rate_per_sec: 10.0,
        queue_waiting,
        queue_running: 0,
        queue_swapped: 0,
        kv_cache_usage: 0.0,
        block_size: 16,
        total_blocks: 4,
    }
}

fn group(pool: Arc<Pool>, scores: Vec<WorkerScore>) -> ScoredGroup {
    ScoredGroup {
        pool,
        model: "gemma".to_string(),
        scores,
        side: ScoreSideData {
            hashes_by_block_size: HashMap::new(),
            tokens: Vec::new(),
        },
    }
}

/// With two worker sets serving one model, the no-policy path chooses on
/// predicted wait, so the deeper queue loses even when scored first.
#[tokio::test]
async fn the_less_loaded_pool_wins() {
    let busy = group(test_pool("busy"), vec![score("10.0.0.1:8000", 64)]);
    let idle = group(test_pool("idle"), vec![score("10.0.0.2:8000", 0)]);
    let winner = best_group(vec![busy, idle], "req", None).expect("a pool must win");
    assert_eq!(winner.pool.name, "idle");
}

/// A group is ranked by its BEST endpoint, not its average: a pool with one
/// free worker and three saturated ones is a fine place to send a request.
#[tokio::test]
async fn a_pool_is_ranked_by_its_best_endpoint() {
    let mixed = group(
        test_pool("mixed"),
        vec![
            score("10.0.0.1:8000", 512),
            score("10.0.0.2:8000", 0),
            score("10.0.0.3:8000", 512),
        ],
    );
    let steady = group(test_pool("steady"), vec![score("10.0.0.4:8000", 8)]);
    // `steady` goes first, so returning the head or ranking on the mean
    // would both pick it.
    let winner = best_group(vec![steady, mixed], "req", None).expect("a pool must win");
    assert_eq!(winner.pool.name, "mixed");
}

/// A pool that scored no workers cannot win, and doesn't shadow one that did.
#[tokio::test]
async fn a_pool_with_no_scores_is_skipped() {
    let empty = group(test_pool("empty"), Vec::new());
    let live = group(test_pool("live"), vec![score("10.0.0.1:8000", 4)]);
    let winner = best_group(vec![empty, live], "req", None).expect("a pool must win");
    assert_eq!(winner.pool.name, "live");
    assert!(best_group(vec![group(test_pool("empty"), Vec::new())], "req", None).is_none());
}

/// Ties keep the first group, and `resolve_pools` yields pool-id order, so
/// replicas agree.
#[tokio::test]
async fn a_tie_keeps_the_first_group() {
    let first = group(test_pool("first"), vec![score("10.0.0.1:8000", 3)]);
    let second = group(test_pool("second"), vec![score("10.0.0.2:8000", 3)]);
    let winner = best_group(vec![first, second], "req", None).expect("a pool must win");
    assert_eq!(winner.pool.name, "first");
}

/// The two degradations have distinct labels and messages.
#[test]
fn the_two_degradations_are_told_apart() {
    assert_ne!(
        Degraded::Tokenize.as_label(),
        Degraded::PrefixIndex.as_label()
    );
    assert_ne!(Degraded::Tokenize.reason(), Degraded::PrefixIndex.reason());
    // Labels become a Prometheus label value; keep them metric-shaped.
    for d in [Degraded::Tokenize, Degraded::PrefixIndex] {
        assert!(
            d.as_label()
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '_'),
            "{} is not a usable metric label",
            d.as_label()
        );
    }
}

// --- alias fallback -------------------------------------------------
//
// The model index only holds names engines report, so a declared alias is
// translated before the "nothing serves this model" check.

fn ws(aliases: &[(&str, &str)]) -> kinference_policy::resolve::ResolvedWorkerSet {
    kinference_policy::resolve::ResolvedWorkerSet {
        name: "set-a".into(),
        namespace: "default".into(),
        labels: std::collections::BTreeMap::new(),
        pool_id: kinference_policy::PoolId::from_raw(0),
        service_name: "svc".into(),
        service_port: None,
        kv_events_declared: false,
        aliases: aliases
            .iter()
            // Fixtures pass (canonical, upstream); the canonical becomes an
            // alias of the upstream.
            .map(|(n, u)| kinference_policy::crd::ModelAlias {
                name: Some(u.to_string()),
                aliases: if n == u { vec![] } else { vec![n.to_string()] },
            })
            .collect(),
    }
}

#[test]
fn an_alias_resolves_to_the_upstream_an_engine_actually_serves() {
    let d = [ws(&[("qwen-simple", "Qwen/Qwen2.5-32B-Instruct-AWQ")])];
    let got = first_served_upstream(&[&d[0]], "qwen-simple", |m| {
        m == "Qwen/Qwen2.5-32B-Instruct-AWQ"
    });
    assert_eq!(got.as_deref(), Some("Qwen/Qwen2.5-32B-Instruct-AWQ"));
}

/// A declared alias whose upstream nothing serves is not rewritten.
#[test]
fn an_alias_whose_upstream_is_unserved_does_not_resolve() {
    let d = [ws(&[("qwen-simple", "Qwen/Qwen2.5-32B-Instruct-AWQ")])];
    assert_eq!(
        first_served_upstream(&[&d[0]], "qwen-simple", |_| false),
        None
    );
}

/// The canonical name itself, which `upstreams_for` appends, is never
/// returned.
#[test]
fn the_canonical_name_is_never_returned_as_its_own_upstream() {
    let d = [ws(&[])];
    assert_eq!(
        first_served_upstream(&[&d[0]], "some-model", |_| true),
        None
    );
}

/// Two upstream models sharing one alias resolve to one fixed choice, not
/// candidates the policy ranks.
#[test]
fn several_upstreams_take_the_first_that_is_served() {
    let d = [ws(&[("bar", "llama-70B"), ("bar", "llama-70B-FP8")])];
    // Only the FP8 build is up, so the first declared one is skipped: first
    // served, not first declared.
    assert_eq!(
        first_served_upstream(&[&d[0]], "bar", |m| m == "llama-70B-FP8").as_deref(),
        Some("llama-70B-FP8")
    );
    // Both up: the earlier alias wins, deterministically.
    assert_eq!(
        first_served_upstream(&[&d[0]], "bar", |_| true).as_deref(),
        Some("llama-70B")
    );
}

/// Aliases live per worker set, so a pool spanning several sets searches all
/// of them.
#[test]
fn aliases_on_a_later_destination_are_found() {
    let d = [ws(&[]), ws(&[("short", "vendor/long-name")])];
    assert_eq!(
        first_served_upstream(&[&d[0], &d[1]], "short", |m| m == "vendor/long-name").as_deref(),
        Some("vendor/long-name")
    );
}

// --- provider candidates and the health gate ---------------------------

fn resolved_provider(
    name: &str,
    models: &[(&str, &str, Option<u32>)],
) -> kinference_policy::resolve::ResolvedProvider {
    kinference_policy::resolve::ResolvedProvider {
        uid: None,
        name: name.into(),
        namespace: "default".into(),
        labels: std::collections::BTreeMap::new(),
        backend_ref: kinference_policy::engine::ObjectKey::new("default", format!("{name}-be")),
        path: None,
        models: models
            .iter()
            // (canonical, upstream, ctx); the canonical becomes an alias of
            // the upstream.
            .map(|(n, u, ctx)| kinference_policy::crd::ProviderModel {
                name: Some(u.to_string()),
                aliases: if n == u { vec![] } else { vec![n.to_string()] },
                context_window: *ctx,
            })
            .collect(),
    }
}

/// Every per-endpoint signal is `None`, not a default.
#[test]
fn a_provider_candidate_carries_no_per_endpoint_signal() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider("together", &[("bar", "acme/llama-70b", Some(131072))]);
    let cands = provider_candidates_for(&health, &prov, "bar");
    assert_eq!(cands.len(), 1);
    let c = &cands[0];

    assert_eq!(c.canonical_model, "bar");
    // The vendor's own spelling.
    assert_eq!(c.upstream_model, "acme/llama-70b");
    assert_eq!(c.backend, "together");
    assert_eq!(
        c.backend_type(),
        kinference_policy::crd::BackendType::Provider
    );

    assert_eq!(c.attrs.predicted_ttft_ms, None);
    assert_eq!(c.attrs.prefix_cache_hit_ratio, None);
    assert_eq!(c.attrs.queued_prompts, None);
    assert_eq!(c.attrs.backend_ready_endpoints, None);
    // Declared rather than observed.
    assert_eq!(c.attrs.context_window, Some(131072));
}

/// The path is carried on the candidate, since `selection` only sees the
/// `CandidateTarget`.
#[test]
fn a_provider_candidate_carries_the_dispatch_path() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let mut prov = resolved_provider("together", &[("bar", "up", None)]);
    prov.path = Some("/serving-endpoints/chat/completions".into());

    let target = &provider_candidates_for(&health, &prov, "bar")[0].target;
    let kinference_policy::engine::CandidateTarget::Provider { path, .. } = target else {
        panic!("a provider candidate");
    };
    assert_eq!(path.as_deref(), Some("/serving-endpoints/chat/completions"));

    // A provider that declares none leaves the path alone.
    prov.path = None;
    let target = &provider_candidates_for(&health, &prov, "bar")[0].target;
    let kinference_policy::engine::CandidateTarget::Provider { path, .. } = target else {
        panic!("a provider candidate");
    };
    assert_eq!(*path, None);
}

#[test]
fn a_declaration_the_crd_would_reject_yields_no_candidate() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    // A canonical name nobody declared.
    let prov = resolved_provider("together", &[("bar", "up", None)]);
    assert!(provider_candidates_for(&health, &prov, "other").is_empty());
}

#[test]
fn two_rows_under_one_name_are_two_candidates() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider(
        "together",
        &[("bar", "acme/v1", None), ("bar", "acme/v2", None)],
    );
    let cands = provider_candidates_for(&health, &prov, "bar");
    // Interchangeable upstream models become separate candidates.
    assert_eq!(cands.len(), 2);
}

/// A provider the caller keeps failing to reach stops being offered.
#[test]
fn a_suppressed_provider_produces_no_candidates_at_all() {
    use kinference_policy::provider_health::FailureKind;
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider("together", &[("bar", "up", None)]);
    let key = kinference_policy::engine::ObjectKey::new("default", "together");

    assert_eq!(provider_candidates_for(&health, &prov, "bar").len(), 1);

    for _ in 0..3 {
        health.record_failure(&key, FailureKind::Unresolved, "no such GatewayBackend");
    }
    assert!(
        provider_candidates_for(&health, &prov, "bar").is_empty(),
        "a provider the caller cannot dispatch to must not be a candidate"
    );

    // One success brings it back.
    health.record_success(&key, 200.0);
    assert_eq!(provider_candidates_for(&health, &prov, "bar").len(), 1);
}

#[test]
fn an_observed_provider_carries_its_p95_as_both_signal_and_objective() {
    let health = kinference_policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider("together", &[("bar", "up", None)]);
    let key = kinference_policy::engine::ObjectKey::new("default", "together");
    health.record_success(&key, 840.0);

    let c = &provider_candidates_for(&health, &prov, "bar")[0];
    // Once as the `observedTTFTP95` a predicate compares against...
    assert_eq!(c.attrs.backend_observed_ttft_ms, Some(840.0));
    // ...and once as the value EstimatedTTFT ranks on, which must agree.
    assert_eq!(
        c.objective_ms(&kinference_policy::compile::Rank::MinimizeEstimatedTtft),
        Some(840.0)
    );
    // But never as PredictedTTFT, which is Local-only by construction.
    assert_eq!(
        c.objective_ms(&kinference_policy::compile::Rank::MinimizePredictedTtft),
        None
    );
}

/// `worker_score` maps every input to the field it names.
///
/// Five of its parameters are `u32`, so a transposition would type-check.
#[test]
fn worker_score_maps_each_input_to_its_own_field() {
    let entry = kinference_fleet::workers::WorkerEntry {
        worker_id_str: "w-7".into(),
        model_name_hint: None,
        current_models: vec![],
        http_url: None,
        block_size: 999, // must NOT be what lands in the score
        initial_sync_complete: true,
        kv_events_observed: true,
        score_addr: "10.0.0.7:8000".into(),
        engine: None,
        pod_name: None,
        namespace: None,
        node_name: None,
        node_ip: None,
        pod_ip: None,
        availability_zone: None,
        instance_type: None,
    };
    // Distinct values throughout, so any swap changes the assertion that fails.
    let sample = kinference_fleet::scrape::MetricsSample {
        completion_rate_per_sec: 1.5,
        waiting: 11,
        running: 22,
        swapped: 33,
        kv_cache_usage: 0.75,
        ..Default::default()
    };

    let s = worker_score(&entry, &sample, 16, 44, 55);

    assert_eq!(s.name, "w-7");
    assert_eq!(s.endpoint, "10.0.0.7:8000");
    assert_eq!(s.block_size, 16, "block_size comes from the argument");
    assert_eq!(s.matched_blocks, 44);
    assert_eq!(s.total_blocks, 55);
    assert_eq!(s.queue_waiting, 11);
    assert_eq!(s.queue_running, 22);
    assert_eq!(s.queue_swapped, 33);
    assert!((s.completion_rate_per_sec - 1.5).abs() < f32::EPSILON);
    assert!((s.kv_cache_usage - 0.75).abs() < f32::EPSILON);
}

// --- policy_select, end to end ----------------------------------------------
//
// Through the real service with live pools; the builder's fake `/render`
// tokenizes, so scoring yields a score per worker.

/// The scored group a pick starts with: `model` on `pool`, one score per
/// `(address, queued)`.
fn primary(pool: &Arc<Pool>, model: &str, workers: &[(&str, u32)]) -> ScoredGroup {
    ScoredGroup {
        pool: pool.clone(),
        model: model.into(),
        scores: workers
            .iter()
            .map(|(addr, queued)| crate::router::picker::WorkerScore {
                name: addr.to_string(),
                endpoint: addr.to_string(),
                matched_blocks: 0,
                completion_rate_per_sec: 0.0,
                queue_waiting: *queued,
                queue_running: 0,
                queue_swapped: 0,
                kv_cache_usage: 0.0,
                block_size: 16,
                total_blocks: 4,
            })
            .collect(),
        side: ScoreSideData {
            hashes_by_block_size: HashMap::new(),
            tokens: Vec::new(),
        },
    }
}

async fn select(
    svc: &KinferenceService,
    store: &Arc<crate::controller::policy::store::PolicyStore>,
    start: ScoredGroup,
    model: &str,
) -> Result<PolicySelection, PickAndRecordOutcome> {
    let pref = kinference_policy::PoolRef {
        namespace: "default".into(),
        name: "pool".into(),
        resource_version: String::new(),
    };
    let spare = Some((
        vec![serde_json::json!({"role": "user", "content": "hi"})],
        ChatExtras::default(),
        b"{}".to_vec(),
    ));
    let attrs = kinference_policy::predicate::RequestAttrs {
        model: vec![model.into()],
        ..Default::default()
    };
    svc.policy_select(&pref, store, Some(start), model, spare, attrs, "test")
        .await
}

/// When every matching rule falls through, the unmatched route serves the
/// requested model, not a group an earlier rule scored (here, `gemma`).
#[tokio::test]
async fn a_fall_through_serves_the_requested_model_only() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .pool("gemma-ws", "gemma", &["10.0.0.2:8000"])
        .policy(
            serde_json::json!({"rules": [{"name": "try-gemma", "route": {
            "consider": [{"models": {"names": ["gemma"]}}],
            "exclude": [{"name": "not-gemma", "drop": {"backend": {"name": "gemma-ws"}}}],
            "select": [{"minimize": "PredictedTTFT"}],
            "onEmpty": "FallThrough"}}]}),
        )
        .build()
        .await;
    // A deep queue on qwen, so gemma would win any comparison it was let into.
    let start = primary(&t.pools[0], "qwen", &[("10.0.0.1:8000", 60)]);
    let Ok(PolicySelection::Local { group, endpoint }) =
        select(&t.svc, t.store(), start, "qwen").await
    else {
        panic!("expected a local pick");
    };
    assert_eq!(group.model, "qwen");
    assert_eq!(endpoint, "10.0.0.1:8000");
}

/// A worker never scraped has no `queuedPrompts`, so a `drop` on it can't
/// fire. The scraped idle worker beside it is dropped.
#[tokio::test]
async fn an_unscraped_worker_has_no_saturation_signals() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000", "10.0.0.2:8000"])
        .policy(
            serde_json::json!({"rules": [{"name": "busy-only", "route": {
            "exclude": [{"name": "idle", "drop": {
                "backend": {"type": "Local"}, "queuedPrompts": {"lessThan": 1}}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]}),
        )
        .build()
        .await;
    // Worker 2 was scraped and is idle; worker 1 never was.
    t.svc.metrics.record_sample_for_test(
        2,
        kinference_fleet::scrape::MetricsSample {
            scraped_at: Some(Instant::now()),
            ..Default::default()
        },
    );
    let start = primary(
        &t.pools[0],
        "qwen",
        &[("10.0.0.1:8000", 0), ("10.0.0.2:8000", 0)],
    );
    let Ok(PolicySelection::Local { endpoint, .. }) =
        select(&t.svc, t.store(), start, "qwen").await
    else {
        panic!("the unscraped worker must survive the exclusion");
    };
    assert_eq!(endpoint, "10.0.0.1:8000");
}

/// A service where `qwen` is served by worker set `qwen-ws` and by provider
/// `cloud`, whose row names it `vendor/qwen`, after two picks for tenant
/// `acme`: response 1 interactive, which is unmatched and stays local, and
/// response 2 batch, which a rule sends to the provider.
async fn local_and_cloud_picks() -> TestService {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .provider(
            "cloud",
            serde_json::json!([{"name": "vendor/qwen", "aliases": ["qwen"]}]),
        )
        .policy(serde_json::json!({"rules": [{"name": "batch-to-cloud",
            "matches": [{"qos": ["batch"]}],
            "route": {"consider": [{"type": "Provider"}], "select": [{"order": ["cloud"]}]}}]}))
        .build()
        .await;
    let pool = TestService::pool_ref();

    let local = t.pick(BODY, 1, Qos::Interactive, Some(&pool)).await;
    assert!(
        matches!(local, PickAndRecordOutcome::Ok { .. }),
        "{local:?}"
    );
    let cloud = t.pick(BODY, 2, Qos::Batch, Some(&pool)).await;
    assert!(
        matches!(cloud, PickAndRecordOutcome::Provider { .. }),
        "{cloud:?}"
    );
    t
}

/// Every pick that routes is counted by tenant and destination.
#[tokio::test]
async fn a_pick_is_counted_by_tenant_and_destination() {
    let t = local_and_cloud_picks().await;
    let out = t.metrics();
    for line in [
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#,
        r#"kinference_routed_requests_total{destination="cloud",tenant="default",type="Provider"} 1"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
}

/// The token families' `model` is the name each body was SENT with: the
/// worker's model on a local pick, the provider row's `name` on a provider pick.
/// Not the alias the client asked for, nor what the report says.
#[tokio::test]
async fn token_metrics_name_the_model_each_body_was_sent_with() {
    use crate::router::proto::picker_server::Picker;

    let t = local_and_cloud_picks().await;
    for (response_id, prompt_tokens) in [(1, 10), (2, 7)] {
        t.svc
            .report_final_token(tonic::Request::new(
                crate::router::proto::ReportFinalTokenRequest {
                    response_id,
                    model: "what-the-vendor-said".into(),
                    prompt_tokens,
                    completion_tokens: 1,
                    ..Default::default()
                },
            ))
            .await
            .expect("report accepted");
    }
    let out = t.metrics();
    for line in [
        r#"kinference_prompt_tokens_total{model="qwen",qos="interactive",tenant="default"} 10"#,
        r#"kinference_prompt_tokens_total{model="vendor/qwen",qos="batch",tenant="default"} 7"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
    assert!(!out.contains("what-the-vendor-said"), "{out}");
}

// --- the pick path, end to end ----------------------------------------------

/// An OpenAI chat request for `qwen`.
const BODY: &[u8] = br#"{"model":"qwen","messages":[{"role":"user","content":"hi"}]}"#;

/// The samples of counter `family` in `out` that are not zero. Families are
/// registered with every label up front, so absence must be checked by value.
fn nonzero<'a>(out: &'a str, family: &str) -> Vec<&'a str> {
    out.lines()
        .filter(|l| l.starts_with(&format!("{family}{{")) && !l.ends_with(" 0"))
        .collect()
}

/// The `model` a body names.
fn model_in(body: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(body).expect("a JSON body")["model"]
        .as_str()
        .expect("a string model")
        .to_string()
}

/// A good pick names the worker and reserves its in-flight state.
#[tokio::test]
async fn a_good_pick_returns_the_worker_and_reserves_its_state() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .build()
        .await;

    let out = t.pick(BODY, 7, Qos::Interactive, None).await;
    let PickAndRecordOutcome::Ok {
        endpoint,
        response_id,
        body,
        model,
    } = out
    else {
        panic!("{out:?}");
    };
    assert_eq!(endpoint, "10.0.0.1:8000");
    assert_eq!(response_id, 7);
    assert!(body.is_empty(), "an unchanged body comes back empty");
    assert_eq!(model, "qwen");
    assert_eq!(t.render_calls(), 1);

    // What ReportFirstToken and ReportFinalToken will find.
    assert_eq!(t.svc.pending_observations.len(), 1);
    let meta = t
        .svc
        .request_labels
        .take(7)
        .expect("labels for the final report");
    assert_eq!(meta.model, "qwen");
    assert_eq!(meta.qos, Qos::Interactive);
    assert_eq!(meta.tenant, "acme");
    assert!(!meta.streaming);
    assert!(meta.provider.is_none());
    // What the next pick on this pool sees.
    assert_eq!(t.pools[0].inflight.get("10.0.0.1:8000"), 1);
    let dispatches = t.pools[0].inflight_prefixes.dispatches_for("10.0.0.1:8000");
    assert_eq!(dispatches.len(), 1);
    assert_eq!(
        dispatches[0].total_at_dispatch, 2,
        "32 tokens in blocks of 16"
    );

    let out = t.metrics();
    for line in [
        r#"kinference_routed_requests_total{destination="qwen-ws",tenant="default",type="Local"} 1"#,
        r#"kinference_prefix_requested_blocks_total{model="qwen",qos="interactive"} 2"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
    assert!(nonzero(&out, "kinference_score_degraded_total").is_empty());
    assert!(nonzero(&out, "kinference_policy_decisions_total").is_empty());
}

/// A streaming request comes back rewritten with `stream_options.include_usage`
/// even though its model did not change.
#[tokio::test]
async fn a_streaming_pick_returns_the_rewritten_body() {
    let t = ServiceBuilder::new()
        .pool("qwen-ws", "qwen", &["10.0.0.1:8000"])
        .build()
        .await;
    let body = br#"{"model":"qwen","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

    let out = t.pick(body, 1, Qos::Interactive, None).await;
    let PickAndRecordOutcome::Ok { body, .. } = out else {
        panic!("{out:?}");
    };
    let rewritten: serde_json::Value = serde_json::from_slice(&body).expect("a JSON body");
    assert_eq!(rewritten["stream_options"]["include_usage"], true);
    assert_eq!(model_in(&body), "qwen");
    assert!(
        t.svc.request_labels.take(1).expect("labels").streaming,
        "the labels remember that the request streams"
    );
}
