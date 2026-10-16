use super::*;

fn router() -> RwLock<HashMap<String, BTreeSet<PoolId>>> {
    RwLock::new(HashMap::new())
}

fn serving(r: &RwLock<HashMap<String, BTreeSet<PoolId>>>, model: &str) -> Vec<PoolId> {
    r.read()
        .unwrap()
        .get(model)
        .map(|s| s.iter().copied().collect())
        .unwrap_or_default()
}

#[test]
fn a_claim_is_recorded() {
    let r = router();
    associate_models(&r, PoolId::from_raw(0), "a", &["gemma".to_string()]);
    assert_eq!(serving(&r, "gemma"), vec![PoolId::from_raw(0)]);
}

/// A second pool's claim is recorded alongside the first.
#[test]
fn a_second_pool_joins_rather_than_being_ignored() {
    let r = router();
    associate_models(&r, PoolId::from_raw(0), "a", &["gemma".to_string()]);
    associate_models(&r, PoolId::from_raw(1), "b", &["gemma".to_string()]);
    assert_eq!(
        serving(&r, "gemma"),
        vec![PoolId::from_raw(0), PoolId::from_raw(1)]
    );
}

/// Both arrival orders produce the same set, iterated in the same order.
#[test]
fn claim_order_does_not_affect_the_result() {
    let forward = router();
    associate_models(&forward, PoolId::from_raw(0), "a", &["gemma".to_string()]);
    associate_models(&forward, PoolId::from_raw(2), "c", &["gemma".to_string()]);
    let reverse = router();
    associate_models(&reverse, PoolId::from_raw(2), "c", &["gemma".to_string()]);
    associate_models(&reverse, PoolId::from_raw(0), "a", &["gemma".to_string()]);
    assert_eq!(serving(&forward, "gemma"), serving(&reverse, "gemma"));
}

#[test]
fn re_claiming_is_idempotent() {
    let r = router();
    for _ in 0..3 {
        associate_models(&r, PoolId::from_raw(0), "a", &["gemma".to_string()]);
    }
    assert_eq!(serving(&r, "gemma"), vec![PoolId::from_raw(0)]);
}

#[test]
fn distinct_models_route_to_distinct_pools() {
    let r = router();
    associate_models(&r, PoolId::from_raw(0), "a", &["gemma".to_string()]);
    associate_models(&r, PoolId::from_raw(1), "b", &["llama".to_string()]);
    assert_eq!(serving(&r, "gemma"), vec![PoolId::from_raw(0)]);
    assert_eq!(serving(&r, "llama"), vec![PoolId::from_raw(1)]);
}

/// A departed pool's claim is released.
#[test]
fn releasing_a_pool_frees_the_model_for_its_replacement() {
    let r = router();
    associate_models(&r, PoolId::from_raw(0), "a", &["gemma".to_string()]);
    release_models(&r, PoolId::from_raw(0), "x");
    assert!(
        !r.read().unwrap().contains_key("gemma"),
        "a model nobody serves should not linger as an empty entry"
    );
    associate_models(&r, PoolId::from_raw(7), "a-v2", &["gemma".to_string()]);
    assert_eq!(serving(&r, "gemma"), vec![PoolId::from_raw(7)]);
}

#[test]
fn releasing_one_of_two_pools_leaves_the_other_serving() {
    let r = router();
    associate_models(&r, PoolId::from_raw(0), "a", &["gemma".to_string()]);
    associate_models(&r, PoolId::from_raw(1), "b", &["gemma".to_string()]);
    release_models(&r, PoolId::from_raw(0), "x");
    assert_eq!(serving(&r, "gemma"), vec![PoolId::from_raw(1)]);
}

use crate::fleet::pool::test_pool;

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

#[test]
fn releasing_an_unknown_pool_is_a_noop() {
    let r = router();
    associate_models(&r, PoolId::from_raw(0), "a", &["gemma".to_string()]);
    release_models(&r, PoolId::from_raw(9), "x");
    assert_eq!(serving(&r, "gemma"), vec![PoolId::from_raw(0)]);
}

#[test]
fn empty_model_list_is_noop() {
    let r = router();
    associate_models(&r, PoolId::from_raw(0), "a", &[]);
    assert!(r.read().unwrap().is_empty());
}

// --- alias fallback -------------------------------------------------
//
// `model_router` only holds names engines report, so a declared alias is
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
    let entry = crate::fleet::workers::WorkerEntry {
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
    let sample = crate::fleet::scrape::MetricsSample {
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
// Through the real service with live pools and no network: workers have no
// HTTP URL, so tokenization fails and scoring degrades to load only, which
// still yields a score per worker.

fn offline_worker(model: &str, addr: &str) -> crate::fleet::workers::WorkerEntry {
    crate::fleet::workers::WorkerEntry {
        worker_id_str: addr.into(),
        model_name_hint: Some(model.into()),
        current_models: vec![model.into()],
        http_url: None,
        block_size: 16,
        initial_sync_complete: true,
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

fn cr<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> Arc<T> {
    Arc::new(serde_json::from_value(v).expect("valid fixture"))
}

/// A service over InferencePool `default/pool` under policy `spec`, with one
/// worker set per `(name, model, worker addresses)`, each a live pool.
fn policy_fleet(
    sets: &[(&str, &str, &[&str])],
    spec: serde_json::Value,
) -> (
    KinferenceService,
    Arc<crate::controller::policy::store::PolicyStore>,
    Vec<Arc<Pool>>,
) {
    policy_fleet_with(sets, &[], spec)
}

/// [`policy_fleet`], with an InferenceProvider per `(name, models[])` among the
/// pool's destinations too.
fn policy_fleet_with(
    sets: &[(&str, &str, &[&str])],
    providers: &[(&str, serde_json::Value)],
    spec: serde_json::Value,
) -> (
    KinferenceService,
    Arc<crate::controller::policy::store::PolicyStore>,
    Vec<Arc<Pool>>,
) {
    let metrics = Arc::new(MetricsRegistry::new());
    let registry = crate::fleet::pool::PoolRegistry::new();
    let (mut live, mut worker_sets, mut dests) = (Vec::new(), Vec::new(), Vec::new());
    let mut bound = std::collections::BTreeMap::new();
    let mut id = 0;
    for (name, model, addrs) in sets {
        // Named as a CRD worker set's pool is: keyed by namespace, with `name`
        // the key's `namespace/name` form (see `policy::worker_sets`).
        let mut spec = crate::fleet::pool::test_spec(name, 16);
        spec.key = kinference_policy::PoolKey::namespaced("default", *name);
        spec.name = spec.key.to_string();
        let p = Arc::new(Pool::from_spec(
            PoolId::next(),
            &spec,
            &tokio_util::sync::CancellationToken::new(),
        ));
        for addr in *addrs {
            id += 1;
            p.registry.register(id, offline_worker(model, addr));
        }
        bound.insert(
            kinference_policy::PoolKey::namespaced("default", *name),
            p.id,
        );
        worker_sets.push(cr::<kinference_policy::crd::InferenceWorkerSet>(
            serde_json::json!({
                "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferenceWorkerSet",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {"serviceRef": {"name": name}}
            }),
        ));
        dests.push(serde_json::json!({"kind": "InferenceWorkerSet", "name": name}));
        live.push(p);
    }
    registry.publish(Arc::new(crate::fleet::pool::PoolMap::from_pools(
        live.clone(),
    )));
    let mut provs = Vec::new();
    for (name, models) in providers {
        provs.push(cr::<kinference_policy::crd::InferenceProvider>(
            serde_json::json!({
                "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferenceProvider",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {"backendRef": {"name": format!("{name}-backend")}, "api": "OpenAI",
                         "models": models}
            }),
        ));
        dests.push(serde_json::json!({"kind": "InferenceProvider", "name": name}));
    }
    let pool = cr::<kinference_policy::crd::InferencePool>(serde_json::json!({
        "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferencePool",
        "metadata": {"name": "pool", "namespace": "default"},
        "spec": {"destinations": dests, "policyRef": {"name": "p"}}
    }));
    let policy = cr::<kinference_policy::crd::InferencePolicy>(serde_json::json!({
        "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferencePolicy",
        "metadata": {"name": "p", "namespace": "default"},
        "spec": spec
    }));
    let store = crate::controller::policy::store::PolicyStore::new();
    store.publish(Arc::new(kinference_policy::resolve::resolve(
        &[pool],
        &[policy],
        &worker_sets,
        &provs,
        &[],
        &[],
        &bound,
        &Default::default(),
    )));
    let ttl = std::time::Duration::from_secs(30);
    let svc = KinferenceService::new(
        registry,
        Arc::new(router()),
        metrics.clone(),
        Arc::new(reqwest::Client::new()),
        None,
        Arc::new(PendingObservations::new(ttl)),
        Arc::new(RequestLabels::new(ttl)),
        Arc::new(crate::router::body_store::BodyStore::new(
            1 << 20,
            1 << 20,
            ttl,
        )),
        Some(store.clone()),
        Arc::new(kinference_policy::provider_health::ProviderHealthStore::new()),
        ttl,
        Arc::new(ObservationSink::open(None)),
        Arc::new(crate::metrics::PromMetrics::new()),
    );
    (svc, store, live)
}

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
    let (svc, store, live) = policy_fleet(
        &[
            ("qwen-ws", "qwen", &["10.0.0.1:8000"]),
            ("gemma-ws", "gemma", &["10.0.0.2:8000"]),
        ],
        serde_json::json!({"rules": [{"name": "try-gemma", "route": {
            "consider": [{"models": {"names": ["gemma"]}}],
            "exclude": [{"name": "not-gemma", "drop": {"backend": {"name": "gemma-ws"}}}],
            "select": [{"minimize": "PredictedTTFT"}],
            "onEmpty": "FallThrough"}}]}),
    );
    // A deep queue on qwen, so gemma would win any comparison it was let into.
    let start = primary(&live[0], "qwen", &[("10.0.0.1:8000", 60)]);
    let Ok(PolicySelection::Local { group, endpoint }) = select(&svc, &store, start, "qwen").await
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
    let (svc, store, live) = policy_fleet(
        &[("qwen-ws", "qwen", &["10.0.0.1:8000", "10.0.0.2:8000"])],
        serde_json::json!({"rules": [{"name": "busy-only", "route": {
            "exclude": [{"name": "idle", "drop": {
                "backend": {"type": "Local"}, "queuedPrompts": {"lessThan": 1}}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]}),
    );
    // Worker 2 was scraped and is idle; worker 1 never was.
    svc.metrics.record_sample_for_test(
        2,
        crate::fleet::scrape::MetricsSample {
            scraped_at: Some(Instant::now()),
            ..Default::default()
        },
    );
    let start = primary(
        &live[0],
        "qwen",
        &[("10.0.0.1:8000", 0), ("10.0.0.2:8000", 0)],
    );
    let Ok(PolicySelection::Local { endpoint, .. }) = select(&svc, &store, start, "qwen").await
    else {
        panic!("the unscraped worker must survive the exclusion");
    };
    assert_eq!(endpoint, "10.0.0.1:8000");
}

/// Names a pick outcome for assertion messages; the enum has no `Debug`.
fn outcome_name(o: &PickAndRecordOutcome) -> String {
    match o {
        PickAndRecordOutcome::Ok { .. } => "Ok".into(),
        PickAndRecordOutcome::Degraded { reason, .. } => format!("Degraded({reason})"),
        PickAndRecordOutcome::Provider { .. } => "Provider".into(),
        PickAndRecordOutcome::NoWorkers { model } => format!("NoWorkers({model})"),
        PickAndRecordOutcome::PolicyRejected { reason, .. } => format!("PolicyRejected({reason})"),
        PickAndRecordOutcome::BadRequest { reason, .. } => format!("BadRequest({reason})"),
    }
}

/// A service where `qwen` is served by worker set `qwen-ws` and by provider
/// `cloud`, whose row names it `vendor/qwen`, after two picks for tenant
/// `acme`: response 1 interactive, which is unmatched and stays local, and
/// response 2 batch, which a rule sends to the provider.
async fn local_and_cloud_picks() -> KinferenceService {
    let (svc, _store, live) = policy_fleet_with(
        &[("qwen-ws", "qwen", &["10.0.0.1:8000"])],
        &[(
            "cloud",
            serde_json::json!([{"name": "vendor/qwen", "aliases": ["qwen"]}]),
        )],
        serde_json::json!({"rules": [{"name": "batch-to-cloud",
            "matches": [{"qos": ["batch"]}],
            "route": {"consider": [{"type": "Provider"}], "select": [{"order": ["cloud"]}]}}]}),
    );
    associate_models(
        &svc.model_router,
        live[0].id,
        "qwen-ws",
        &["qwen".to_string()],
    );
    let pool = kinference_policy::PoolRef {
        namespace: "default".into(),
        name: "pool".into(),
        resource_version: String::new(),
    };
    let tenant = kinference_policy::RequestIdentity {
        tenant: "acme".into(),
    };
    let body = br#"{"model":"qwen","messages":[{"role":"user","content":"hi"}]}"#;

    let local = svc
        .pick_and_record(body, 1, None, Qos::Interactive, Some(&pool), &tenant)
        .await;
    assert!(
        matches!(
            local,
            PickAndRecordOutcome::Ok { .. } | PickAndRecordOutcome::Degraded { .. }
        ),
        "{}",
        outcome_name(&local)
    );
    let cloud = svc
        .pick_and_record(body, 2, None, Qos::Batch, Some(&pool), &tenant)
        .await;
    assert!(
        matches!(cloud, PickAndRecordOutcome::Provider { .. }),
        "{}",
        outcome_name(&cloud)
    );
    svc
}

/// Every pick that routes is counted by tenant and destination.
#[tokio::test]
async fn a_pick_is_counted_by_tenant_and_destination() {
    let svc = local_and_cloud_picks().await;
    let out = svc.prom.render();
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

    let svc = local_and_cloud_picks().await;
    for (response_id, prompt_tokens) in [(1, 10), (2, 7)] {
        svc.report_final_token(tonic::Request::new(
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
    let out = svc.prom.render();
    for line in [
        r#"kinference_prompt_tokens_total{model="qwen",qos="interactive",tenant="default"} 10"#,
        r#"kinference_prompt_tokens_total{model="vendor/qwen",qos="batch",tenant="default"} 7"#,
    ] {
        assert!(out.contains(line), "missing {line}:\n{out}");
    }
    assert!(!out.contains("what-the-vendor-said"), "{out}");
}
