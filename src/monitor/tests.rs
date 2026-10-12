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

/// The behavior this replaced: a second pool's claim used to be logged and
/// DROPPED, which left that worker set dead for the model on the no-policy
/// path even though it was serving it perfectly well.
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

/// Both arrival orders produce the same set, and `BTreeSet` fixes the
/// iteration order — so two replicas that observed their pools in opposite
/// orders still rank candidates identically.
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

/// The R2 regression: without a release path a departed pool's claim is
/// permanent, so a replacement worker set is ignored FOREVER and requests
/// 404 while healthy workers idle.
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

use crate::pool::test_pool;

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

/// The whole point of the index: two worker sets serve one model, and the
/// no-policy path has to choose. It chooses on predicted wait, so the pool
/// with the deeper queue loses even though it was scored first.
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
    // `steady` goes first, so returning the head — or ranking on the mean
    // queue depth — would both pick it.
    let winner = best_group(vec![steady, mixed], "req", None).expect("a pool must win");
    assert_eq!(winner.pool.name, "mixed");
}

/// A pool that scored no workers cannot win, and must not shadow one that
/// did — this is what makes an empty `ScoreOutcome::NoWorkers` from one pool
/// non-fatal now that several may serve a model.
#[tokio::test]
async fn a_pool_with_no_scores_is_skipped() {
    let empty = group(test_pool("empty"), Vec::new());
    let live = group(test_pool("live"), vec![score("10.0.0.1:8000", 4)]);
    let winner = best_group(vec![empty, live], "req", None).expect("a pool must win");
    assert_eq!(winner.pool.name, "live");
    assert!(best_group(vec![group(test_pool("empty"), Vec::new())], "req", None).is_none());
}

/// Ties keep the first group. `resolve_pools` yields pool-id order, so two
/// replicas that saw their pools appear in opposite orders still agree —
/// which matters because inflight tracking is split between them and a
/// coin-flip per replica would smear one request stream across both pools.
#[tokio::test]
async fn a_tie_keeps_the_first_group() {
    let first = group(test_pool("first"), vec![score("10.0.0.1:8000", 3)]);
    let second = group(test_pool("second"), vec![score("10.0.0.2:8000", 3)]);
    let winner = best_group(vec![first, second], "req", None).expect("a pool must win");
    assert_eq!(winner.pool.name, "first");
}

/// The two degradations must not share a label or a message. They were
/// reported identically, so an operator chasing "tokenize unavailable"
/// would go and inspect a perfectly healthy engine while kinference's own radix
/// tree was the thing failing.
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
// The gate these guard: `model_router` only holds names an engine REPORTED,
// so a declared alias resolved to nothing and the request was refused
// before the policy stage — the only place that knew the alias table — was
// reached. A request for `qwen-simple` 404'd while the worker set's own
// status listed it.

fn ws(aliases: &[(&str, &str)]) -> crate::policy::store::ResolvedWorkerSet {
    crate::policy::store::ResolvedWorkerSet {
        name: "set-a".into(),
        namespace: "default".into(),
        labels: std::collections::BTreeMap::new(),
        pool_id: crate::pool::PoolId::from_raw(0),
        service_name: "svc".into(),
        service_port: None,
        kv_events_declared: false,
        aliases: aliases
            .iter()
            // Fixtures still pass (canonical, upstream); under the new shape
            // the canonical is an ALIAS of the upstream, so every existing
            // case asks the same question.
            .map(|(n, u)| crate::policy::crd::ModelAlias {
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

/// A declared alias whose upstream nothing serves must NOT be rewritten to.
/// Doing so would move the 404 from kinference to the engine and lose the
/// operator-facing reason.
#[test]
fn an_alias_whose_upstream_is_unserved_does_not_resolve() {
    let d = [ws(&[("qwen-simple", "Qwen/Qwen2.5-32B-Instruct-AWQ")])];
    assert_eq!(
        first_served_upstream(&[&d[0]], "qwen-simple", |_| false),
        None
    );
}

/// `upstreams_for` appends the canonical name as an identity fallback. We
/// only reach here because no pool serves it, so returning it would rewrite
/// the model to itself and loop the caller straight back to NoWorkers.
#[test]
fn the_canonical_name_is_never_returned_as_its_own_upstream() {
    let d = [ws(&[])];
    assert_eq!(
        first_served_upstream(&[&d[0]], "some-model", |_| true),
        None
    );
}

/// Documents the accepted limitation of the minimal fix: two upstream models
/// declared interchangeable under one name resolve to ONE fixed choice
/// rather than becoming candidates the policy ranks.
#[test]
fn several_upstreams_take_the_first_that_is_served() {
    let d = [ws(&[("bar", "llama-70B"), ("bar", "llama-70B-FP8")])];
    // Only the FP8 build is up — the first declared one is skipped, so the
    // ordering rule is "first SERVED", not "first declared".
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
/// of them — the alias may be declared on a set other than the first.
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
) -> crate::policy::store::ResolvedProvider {
    crate::policy::store::ResolvedProvider {
        uid: None,
        name: name.into(),
        namespace: "default".into(),
        labels: std::collections::BTreeMap::new(),
        backend_ref: crate::policy::engine::ObjectKey::new("default", format!("{name}-be")),
        path: None,
        models: models
            .iter()
            // The fixture tuple is still (canonical, upstream, ctx); under
            // the new shape the canonical is an ALIAS of the upstream, which
            // keeps every existing case asking the same question.
            .map(|(n, u, ctx)| crate::policy::crd::ProviderModel {
                name: Some(u.to_string()),
                aliases: if n == u { vec![] } else { vec![n.to_string()] },
                context_window: *ctx,
            })
            .collect(),
    }
}

/// Every per-endpoint signal must be `None`, not a default.
///
/// This is the property the whole design leans on: `Matcher::eval` treats
/// absence as non-matching, so a Local-only exclusion cannot silently drop a
/// provider — and a defaulted latency of zero would have been the BEST score
/// under every `minimize`, handing a vendor all the traffic.
#[test]
fn a_provider_candidate_carries_no_per_endpoint_signal() {
    let health = crate::policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider("together", &[("bar", "acme/llama-70b", Some(131072))]);
    let cands = provider_candidates_for(&health, &prov, "bar");
    assert_eq!(cands.len(), 1);
    let c = &cands[0];

    assert_eq!(c.canonical_model, "bar");
    // The vendor's own spelling, which is what the body must end up saying.
    assert_eq!(c.upstream_model, "acme/llama-70b");
    assert_eq!(c.backend, "together");
    assert_eq!(c.backend_type(), crate::policy::crd::BackendType::Provider);

    assert_eq!(c.attrs.predicted_ttft_ms, None);
    assert_eq!(c.attrs.prefix_cache_hit_ratio, None);
    assert_eq!(c.attrs.queued_prompts, None);
    assert_eq!(c.attrs.backend_ready_endpoints, None);
    // Declared rather than observed, and the one signal a provider does have.
    assert_eq!(c.attrs.context_window, Some(131072));
}

/// The path reaches the candidate, because by the time one WINS there is no
/// `ResolvedProvider` left to ask: `selection` sees only the `CandidateTarget`.
#[test]
fn a_provider_candidate_carries_the_dispatch_path() {
    let health = crate::policy::provider_health::ProviderHealthStore::new();
    let mut prov = resolved_provider("together", &[("bar", "up", None)]);
    prov.path = Some("/serving-endpoints/chat/completions".into());

    let target = &provider_candidates_for(&health, &prov, "bar")[0].target;
    let crate::policy::engine::CandidateTarget::Provider { path, .. } = target else {
        panic!("a provider candidate");
    };
    assert_eq!(path.as_deref(), Some("/serving-endpoints/chat/completions"));

    // And a provider that declares none leaves the request line alone, which
    // is what a mixed pool needs: a local win must not be rewritten.
    prov.path = None;
    let target = &provider_candidates_for(&health, &prov, "bar")[0].target;
    let crate::policy::engine::CandidateTarget::Provider { path, .. } = target else {
        panic!("a provider candidate");
    };
    assert_eq!(*path, None);
}

#[test]
fn a_declaration_the_crd_would_reject_yields_no_candidate() {
    let health = crate::policy::provider_health::ProviderHealthStore::new();
    // A canonical name nobody declared.
    let prov = resolved_provider("together", &[("bar", "up", None)]);
    assert!(provider_candidates_for(&health, &prov, "other").is_empty());
}

#[test]
fn two_rows_under_one_name_are_two_candidates() {
    let health = crate::policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider(
        "together",
        &[("bar", "acme/v1", None), ("bar", "acme/v2", None)],
    );
    let cands = provider_candidates_for(&health, &prov, "bar");
    // Exactly as two worker-set aliases would be: interchangeable upstream
    // models become a routing choice rather than a fixed pick.
    assert_eq!(cands.len(), 2);
}

/// A destination the caller keeps failing to reach must stop being offered.
///
/// There is no dispatch retry anywhere in this design, so every pick a broken
/// provider wins is a failed request — and kinference cannot see the cause, only
/// the reports.
#[test]
fn a_suppressed_provider_produces_no_candidates_at_all() {
    use crate::policy::provider_health::FailureKind;
    let health = crate::policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider("together", &[("bar", "up", None)]);
    let key = crate::policy::engine::ObjectKey::new("default", "together");

    assert_eq!(provider_candidates_for(&health, &prov, "bar").len(), 1);

    for _ in 0..3 {
        health.record_failure(&key, FailureKind::Unresolved, "no such GatewayBackend");
    }
    assert!(
        provider_candidates_for(&health, &prov, "bar").is_empty(),
        "a provider the caller cannot dispatch to must not be a candidate"
    );

    // One success is enough to bring it back — recovery is unobservable from
    // here, so it can only be learned by something working again.
    health.record_success(&key, 200.0);
    assert_eq!(provider_candidates_for(&health, &prov, "bar").len(), 1);
}

#[test]
fn an_observed_provider_carries_its_p95_as_both_signal_and_objective() {
    let health = crate::policy::provider_health::ProviderHealthStore::new();
    let prov = resolved_provider("together", &[("bar", "up", None)]);
    let key = crate::policy::engine::ObjectKey::new("default", "together");
    health.record_success(&key, 840.0);

    let c = &provider_candidates_for(&health, &prov, "bar")[0];
    // Once as the `observedTTFTP95` a predicate compares against...
    assert_eq!(c.attrs.backend_observed_ttft_ms, Some(840.0));
    // ...and once as the value EstimatedTTFT ranks on. The two must agree, or
    // a policy could exclude on a figure different from the one that ranked.
    assert_eq!(
        c.objective_ms(&crate::policy::compile::Rank::MinimizeEstimatedTtft),
        Some(840.0)
    );
    // But never as PredictedTTFT, which is Local-only by construction.
    assert_eq!(
        c.objective_ms(&crate::policy::compile::Rank::MinimizePredictedTtft),
        None
    );
}

/// `worker_score` maps every input to the field it names.
///
/// Five of its parameters are `u32`, so a transposition — `matched_blocks` for
/// `total_blocks`, say — type-checks silently, and no other test in the suite
/// would notice: the picker's tests build `WorkerScore` directly, and the
/// scoring path is not driven at unit level. This is cheap only because the
/// projection now happens in exactly one place.
#[test]
fn worker_score_maps_each_input_to_its_own_field() {
    let entry = crate::workers::WorkerEntry {
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
    let sample = crate::metrics::MetricsSample {
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
// HTTP URL, so tokenization fails at once and scoring degrades to load only —
// which still yields a score per worker, enough to rank.

fn offline_worker(model: &str, addr: &str) -> crate::workers::WorkerEntry {
    crate::workers::WorkerEntry {
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
    Arc<crate::policy::store::PolicyStore>,
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
    Arc<crate::policy::store::PolicyStore>,
    Vec<Arc<Pool>>,
) {
    let metrics = Arc::new(MetricsRegistry::new());
    let registry = crate::pool::PoolRegistry::new();
    let (mut live, mut worker_sets, mut dests) = (Vec::new(), Vec::new(), Vec::new());
    let mut bound = std::collections::BTreeMap::new();
    let mut id = 0;
    for (name, model, addrs) in sets {
        // Named as a CRD worker set's pool is: keyed by namespace, with `name`
        // the key's `namespace/name` form (see `policy::worker_sets`).
        let mut spec = crate::config::test_spec(name, 16);
        spec.key = crate::config::PoolKey::namespaced("default", *name);
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
        bound.insert(crate::config::PoolKey::namespaced("default", *name), p.id);
        worker_sets.push(cr::<crate::policy::crd::InferenceWorkerSet>(
            serde_json::json!({
                "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferenceWorkerSet",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {"serviceRef": {"name": name}}
            }),
        ));
        dests.push(serde_json::json!({"kind": "InferenceWorkerSet", "name": name}));
        live.push(p);
    }
    registry.publish(Arc::new(crate::pool::PoolMap::from_pools(live.clone())));
    let mut provs = Vec::new();
    for (name, models) in providers {
        provs.push(cr::<crate::policy::crd::InferenceProvider>(
            serde_json::json!({
                "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferenceProvider",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {"backendRef": {"name": format!("{name}-backend")}, "api": "OpenAI",
                         "models": models}
            }),
        ));
        dests.push(serde_json::json!({"kind": "InferenceProvider", "name": name}));
    }
    let pool = cr::<crate::policy::crd::InferencePool>(serde_json::json!({
        "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferencePool",
        "metadata": {"name": "pool", "namespace": "default"},
        "spec": {"destinations": dests, "policyRef": {"name": "p"}}
    }));
    let policy = cr::<crate::policy::crd::InferencePolicy>(serde_json::json!({
        "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferencePolicy",
        "metadata": {"name": "p", "namespace": "default"},
        "spec": spec
    }));
    let store = crate::policy::store::PolicyStore::new();
    store.publish(Arc::new(crate::policy::store::resolve(
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
        crate::telemetry::Telemetry::new(4, metrics),
        Arc::new(reqwest::Client::new()),
        None,
        Arc::new(PendingObservations::new(ttl)),
        Arc::new(RequestLabels::new(ttl)),
        Arc::new(crate::body_store::BodyStore::new(1 << 20, 1 << 20, ttl)),
        Some(store.clone()),
        Arc::new(crate::policy::provider_health::ProviderHealthStore::new()),
        ttl,
        Arc::new(ObservationSink::from_env()),
        Arc::new(crate::prom::PromMetrics::new()),
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
            .map(|(addr, queued)| crate::picker::WorkerScore {
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
    store: &Arc<crate::policy::store::PolicyStore>,
    start: ScoredGroup,
    model: &str,
) -> Result<PolicySelection, PickAndRecordOutcome> {
    let pref = crate::policy::PoolRef {
        namespace: "default".into(),
        name: "pool".into(),
        resource_version: String::new(),
    };
    let spare = Some((
        vec![serde_json::json!({"role": "user", "content": "hi"})],
        ChatExtras::default(),
        b"{}".to_vec(),
    ));
    let attrs = crate::policy::predicate::RequestAttrs {
        model: vec![model.into()],
        ..Default::default()
    };
    svc.policy_select(&pref, store, Some(start), model, spare, attrs, "test")
        .await
}

/// When every matching rule falls through, the unmatched route serves the
/// model the request asked for. It used to reuse every group earlier rules had
/// scored — here the faster `gemma` group a rule considered and then excluded
/// — so a request for `qwen` could be answered by `gemma`.
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

/// A worker never scraped has no `queuedPrompts` at all, so a `drop` on it
/// cannot fire — absence never matches. It used to read as an idle worker
/// with an empty queue. The scraped idle worker beside it is dropped.
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
        crate::metrics::MetricsSample {
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
    let pool = crate::policy::PoolRef {
        namespace: "default".into(),
        name: "pool".into(),
        resource_version: String::new(),
    };
    let tenant = crate::policy::RequestIdentity {
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

/// Every pick that routes is counted by tenant and destination, so a dashboard
/// can show where each tenant's traffic went, on the cluster or off it.
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
/// Not the alias the client asked for, which would merge two different models,
/// and not whatever the report says, which is the vendor's own spelling.
#[tokio::test]
async fn token_metrics_name_the_model_each_body_was_sent_with() {
    use crate::proto::picker_server::Picker;

    let svc = local_and_cloud_picks().await;
    for (response_id, prompt_tokens) in [(1, 10), (2, 7)] {
        svc.report_final_token(tonic::Request::new(crate::proto::ReportFinalTokenRequest {
            response_id,
            model: "what-the-vendor-said".into(),
            prompt_tokens,
            completion_tokens: 1,
            ..Default::default()
        }))
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
