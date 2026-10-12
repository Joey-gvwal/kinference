use super::*;
use crate::config::{PickerConfig, PredictorConfig, ProfileTokenizeConfig, TokenizeMode};
use crate::policy::crd::{InferenceWorkerSetSpec, KvEvents, ServiceRef};

fn profile(name: &str) -> ProfileConfig {
    ProfileConfig {
        name: name.to_string(),
        predictor: PredictorConfig::default(),
        picker: PickerConfig::default(),
        tokenize: ProfileTokenizeConfig {
            mode: TokenizeMode::Local,
        },
    }
}

fn iws(ns: &str, name: &str, spec: InferenceWorkerSetSpec) -> InferenceWorkerSet {
    let mut o = InferenceWorkerSet::new(name, spec);
    o.metadata.namespace = Some(ns.to_string());
    o
}

fn full_spec() -> InferenceWorkerSetSpec {
    InferenceWorkerSetSpec {
        service_ref: ServiceRef {
            name: "vllm".into(),
            port: None,
        },
        profile: Some("h100".into()),
        kv_block_size: Some(16),
        metrics_port: None,
        kv_events: None,
        model_discovery: None,
        models: Vec::new(),
    }
}

#[test]
fn a_complete_set_resolves_against_its_profile() {
    let (out, rejected) = desired_pools(
        &[iws("infra", "workers-h100", full_spec())],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(rejected.is_empty(), "{rejected:?}");
    let spec = out
        .get(&PoolKey::namespaced("infra", "workers-h100"))
        .unwrap();
    assert_eq!(spec.profile, "h100");
    assert_eq!(spec.kv_block_size, 16);
    assert_eq!(spec.tokenize_mode, TokenizeMode::Local);
    assert_eq!(spec.discovery.mode, DiscoveryMode::Kubernetes);
    assert_eq!(spec.discovery.service_name.as_deref(), Some("vllm"));
    assert_eq!(
        spec.discovery.namespace.as_deref(),
        Some("infra"),
        "the slice watch must be scoped to the set's own namespace"
    );
}

/// R13. Two same-named Services in different namespaces are different
/// fleets; collapsing them into one pool merges their prefix models
/// silently, and every request still succeeds while the routing is wrong.
#[test]
fn same_named_sets_in_two_namespaces_stay_separate() {
    let (out, rejected) = desired_pools(
        &[
            iws("a", "workers", full_spec()),
            iws("b", "workers", full_spec()),
        ],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(rejected.is_empty());
    assert_eq!(out.len(), 2);
    let a = out.get(&PoolKey::namespaced("a", "workers")).unwrap();
    let b = out.get(&PoolKey::namespaced("b", "workers")).unwrap();
    assert_ne!(a.name, b.name, "names must not collide either");
    assert_eq!(a.discovery.namespace.as_deref(), Some("a"));
    assert_eq!(b.discovery.namespace.as_deref(), Some("b"));
}

/// The fix for an unknown profile is editing the ROUTER's TOML; for a
/// missing one it is editing the CR. Different actions, so different
/// reasons — and the unknown case lists what is defined, because it is
/// almost always a typo.
#[test]
fn an_unknown_profile_is_rejected_and_lists_the_defined_ones() {
    let mut spec = full_spec();
    spec.profile = Some("h1OO".into());
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100"), profile("a100")],
        &DiscoveryDefaults::default(),
    );
    assert!(out.is_empty());
    assert_eq!(rejected.len(), 1);
    match &rejected[0].reason {
        RejectReason::UnknownProfile { defined } => {
            assert_eq!(defined, &["a100".to_string(), "h100".to_string()]);
        }
        other => panic!("expected UnknownProfile, got {other:?}"),
    }
    assert_eq!(rejected[0].reason.as_condition_reason(), "UnknownProfile");
}

/// The whole reason these fields are `Option` in Rust: an object missing
/// them must be REJECTED individually. If it failed deserialization the
/// whole list page would fail, and discovery would be down for every set in
/// the namespace, not just this one.
#[test]
fn a_set_missing_required_fields_is_rejected_without_taking_the_others_down() {
    let mut no_profile = full_spec();
    no_profile.profile = None;
    let mut no_block = full_spec();
    no_block.kv_block_size = None;
    let mut zero_block = full_spec();
    zero_block.kv_block_size = Some(0);
    let mut no_svc = full_spec();
    no_svc.service_ref.name = String::new();

    let (out, rejected) = desired_pools(
        &[
            iws("infra", "bad-profile", no_profile),
            iws("infra", "bad-block", no_block),
            iws("infra", "zero-block", zero_block),
            iws("infra", "bad-svc", no_svc),
            iws("infra", "good", full_spec()),
        ],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(out.len(), 1, "the healthy set must still materialize");
    assert!(out.contains_key(&PoolKey::namespaced("infra", "good")));
    let reasons: Vec<&str> = rejected
        .iter()
        .map(|r| r.reason.as_condition_reason())
        .collect();
    assert_eq!(
        reasons,
        [
            "MissingProfile",
            "MissingKvBlockSize",
            "InvalidKvBlockSize",
            "MissingServiceRef"
        ]
    );
}

/// The outage this closed, at the level the parser sees it.
///
/// `ModelAlias.name` was a bare `String`. When the schema changed under a
/// running router the API server pruned the field it no longer declared,
/// and because `watcher` deserializes list pages WHOLESALE, every worker set
/// in the namespace failed as one page — discovery down until the binary
/// caught up, on objects that were individually fine. It must PARSE.
#[test]
fn a_models_row_missing_its_name_parses_rather_than_failing_the_page() {
    let spec: InferenceWorkerSetSpec = serde_json::from_value(serde_json::json!({
        "serviceRef": {"name": "vllm", "port": "http"},
        "profile": "h100",
        "kvBlockSize": 16,
        // What pruning leaves behind when a schema drops the field a row was
        // keyed on: the row survives, its identity does not.
        "models": [{"aliases": ["qwen-2.5"]}]
    }))
    .expect("must parse: failing here fails every set in the list page");
    assert_eq!(spec.models.len(), 1);
    assert!(spec.models[0].name.is_none());
}

/// ...and is then refused, because an alias mapped onto nothing is a routing
/// lie: the name a client writes would silently resolve to no model.
#[test]
fn a_models_row_missing_its_name_is_rejected() {
    let mut spec = full_spec();
    spec.models = vec![crate::policy::crd::ModelAlias {
        name: None,
        aliases: vec!["qwen-2.5".into()],
    }];
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(out.is_empty(), "no pool: the set is not routable");
    assert_eq!(rejected[0].reason, RejectReason::ModelMissingName);
    assert_eq!(rejected[0].reason.as_condition_reason(), "ModelMissingName");
}

/// A row with a name and no aliases is the ordinary case and must survive —
/// `aliases` is optional, and `name` alone is routable.
#[test]
fn a_models_row_with_no_aliases_is_fine() {
    let mut spec = full_spec();
    spec.models = vec![crate::policy::crd::ModelAlias {
        name: Some("meta/llama-3.3-70b".into()),
        aliases: vec![],
    }];
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(rejected.is_empty(), "{rejected:?}");
    assert_eq!(out.len(), 1);
}

/// The hole this closed: `KvEvents.port` was the one non-`Option` leaf in the
/// spec, so `kvEvents: {}` failed the whole object — and `watcher`
/// deserializes list pages WHOLESALE, so one such object took worker-set
/// discovery down for the namespace. It must PARSE.
#[test]
fn an_empty_kv_events_block_parses_rather_than_failing_the_page() {
    let spec: InferenceWorkerSetSpec = serde_json::from_value(serde_json::json!({
        "serviceRef": {"name": "vllm", "port": "http"},
        "profile": "h100",
        "kvBlockSize": 16,
        "kvEvents": {}
    }))
    .expect("must parse: failing here fails every set in the list page");
    assert!(spec.kv_events.is_some(), "the block is present");
    assert!(spec.kv_events.unwrap().port.is_none(), "but says no port");
}

/// ...and is then refused, rather than read either silent way: connecting to
/// a default socket that never speaks, or downgrading to "publishes nothing"
/// and reporting a healthy pool with a permanently empty prefix model.
#[test]
fn a_kv_events_block_with_no_port_is_rejected() {
    let mut spec = full_spec();
    spec.kv_events = Some(KvEvents {
        port: None,
        replay_port: None,
    });
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(out.is_empty(), "no pool: the set is not routable");
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].reason, RejectReason::KvEventsMissingPort);
    assert_eq!(
        rejected[0].reason.as_condition_reason(),
        "KvEventsMissingPort"
    );
    assert!(
        rejected[0]
            .reason
            .to_string()
            .contains("omit the whole block"),
        "the message must name the fix: {}",
        rejected[0].reason
    );
}

/// End to end for the portless-`kvEvents` case: `desired_pools` refuses it,
/// `rejection_index` projects the reason, and `bind` hands back `Incomplete`
/// — the same shape as a provider missing its `backendRef`, because in both
/// the object simply does not say enough to route to.
#[test]
fn a_portless_kv_events_block_binds_as_incomplete() {
    use crate::policy::crd::{Destination, InferencePool, InferencePoolSpec};
    use crate::policy::resolve::{PoolIndex, UnresolvedReason, rejection_index, resolve};

    let mut spec = full_spec();
    spec.kv_events = Some(KvEvents {
        port: None,
        replay_port: None,
    });
    let ws = iws("infra", "workers-h100", spec);
    let (pools_built, rejections) = desired_pools(
        std::slice::from_ref(&ws),
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(pools_built.is_empty(), "refused, so it produced no pool");

    let mut pool = InferencePool::new(
        "chat",
        InferencePoolSpec {
            destinations: vec![Destination {
                kind: Some("InferenceWorkerSet".into()),
                name: Some("workers-h100".into()),
                ..Default::default()
            }],
            policy_ref: None,
        },
    );
    pool.metadata.namespace = Some("infra".into());
    let idx = resolve(
        &[std::sync::Arc::new(pool)],
        &[],
        &[std::sync::Arc::new(ws)],
        &[],
        &[],
        &[],
        // The pool map is what `desired_pools` actually built: empty.
        &PoolIndex::new(),
        &rejection_index(&rejections),
    );

    // On the pool: reported, not silently dropped.
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.unresolved.len(), 1);
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::Incomplete);

    // And on the set's own entry, which is where an operator looks.
    let e = idx.worker_sets().next().unwrap();
    assert_eq!(e.bound.as_ref().err(), Some(&UnresolvedReason::Incomplete));
}

/// Omitting the block entirely stays legal — that is the supported way to say
/// these engines publish nothing, and it must not be confused with the case
/// above.
#[test]
fn an_omitted_kv_events_block_is_not_a_rejection() {
    let mut spec = full_spec();
    spec.kv_events = None;
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(rejected.len(), 0);
    assert_eq!(out.len(), 1);
    assert!(
        out.values()
            .next()
            .unwrap()
            .discovery
            .ports
            .kv_events_declared_absent
    );
}

#[test]
fn declared_ports_reach_discovery_by_name_or_number() {
    let mut spec = full_spec();
    spec.service_ref.port = Some(IntOrString::Str("openai".into()));
    spec.metrics_port = Some(IntOrString::Int(9090));
    spec.kv_events = Some(KvEvents {
        port: Some(IntOrString::Str("kv-pub".into())),
        replay_port: Some(IntOrString::Str("kv-replay".into())),
    });

    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let ports = &out.values().next().unwrap().discovery.ports;
    assert_eq!(ports.http, Some(PortRef::Name("openai".into())));
    assert_eq!(ports.metrics, Some(PortRef::Number(9090)));
    assert_eq!(ports.replay, Some(PortRef::Name("kv-replay".into())));
    assert_eq!(ports.zmq, Some(PortRef::Name("kv-pub".into())));
}

/// Unset ports must stay unset rather than becoming a number, so discovery
/// keeps its by-name-then-default chain — which is what makes an ordinary
/// vLLM Service work with no port fields at all.
#[test]
fn unset_ports_leave_the_conventional_resolution_alone() {
    let mut spec = full_spec();
    spec.kv_events = Some(KvEvents {
        port: Some(IntOrString::Str("kv-pub".into())),
        replay_port: None,
    });
    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let ports = &out.values().next().unwrap().discovery.ports;
    assert_eq!(ports.http, None);
    assert_eq!(ports.metrics, None);
    assert_eq!(ports.replay, None);
    assert!(!ports.kv_events_declared_absent);
}

/// An absent `kvEvents` is a DECLARATION — these engines don't publish —
/// not a gap to fill with a default. Connecting anyway would look healthy
/// forever, because ZMQ `connect` always succeeds: the socket would sit
/// silent and the worker would never finish its initial sync.
#[test]
fn an_absent_kv_events_block_is_honored_rather_than_defaulted() {
    let (out, _) = desired_pools(
        &[iws("infra", "ws", full_spec())],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let ports = &out.values().next().unwrap().discovery.ports;
    assert!(ports.kv_events_declared_absent);
    assert_eq!(ports.zmq, None);
}

#[test]
fn model_discovery_sets_the_interval_and_the_static_list() {
    let mut spec = full_spec();
    // Two upstream models sharing one alias — the interchangeable case, which
    // under the new shape is two rows with distinct `name`s rather than two
    // rows with a duplicated one.
    spec.models = vec![
        crate::policy::crd::ModelAlias {
            name: Some("meta-llama/Llama-3.3-70B-Instruct".into()),
            aliases: vec!["chat".into()],
        },
        crate::policy::crd::ModelAlias {
            name: Some("neuralmagic/Llama-3.3-70B-Instruct-FP8".into()),
            aliases: vec!["chat".into()],
        },
    ];
    spec.model_discovery = Some(crate::policy::crd::ModelDiscovery {
        mode: ModelDiscoveryMode::Static,
        interval: "2m".into(),
    });
    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let resolved = out.values().next().unwrap();
    assert_eq!(resolved.models_refresh_secs, Some(120));
    assert_eq!(
        resolved.static_models.as_deref(),
        Some(
            [
                "meta-llama/Llama-3.3-70B-Instruct".to_string(),
                "neuralmagic/Llama-3.3-70B-Instruct-FP8".to_string()
            ]
            .as_slice()
        ),
        "Static mode seeds the UPSTREAM names, which is what the engine serves"
    );
}

/// Endpoint mode is the default and must not produce a static list, or the
/// pool would stop discovering models it was meant to poll for.
#[test]
fn endpoint_mode_leaves_discovery_on() {
    let mut spec = full_spec();
    spec.model_discovery = Some(crate::policy::crd::ModelDiscovery::default());
    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(out.values().next().unwrap().static_models, None);
}

/// A bad interval is a tuning knob gone wrong, not a reason to refuse to
/// route: fall back to the fleet default rather than rejecting the set.
#[test]
fn an_unparseable_interval_falls_back_instead_of_rejecting() {
    let mut spec = full_spec();
    spec.model_discovery = Some(crate::policy::crd::ModelDiscovery {
        mode: ModelDiscoveryMode::Endpoint,
        interval: "soon".into(),
    });
    let (out, rejected) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert!(rejected.is_empty(), "a bad interval must not kill the set");
    assert_eq!(out.values().next().unwrap().models_refresh_secs, None);
}

/// A port number that cannot be a port must not be truncated into a
/// different, valid-looking one.
#[test]
fn an_out_of_range_port_is_dropped_not_truncated() {
    let mut spec = full_spec();
    spec.metrics_port = Some(IntOrString::Int(70000));
    let (out, _) = desired_pools(
        &[iws("infra", "ws", spec)],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(out.values().next().unwrap().discovery.ports.metrics, None);
}

/// Order-independence: the desired set must not depend on the order the
/// watch delivered objects, or two replicas build differently-ordered
/// fleets and rank tied candidates differently.
#[test]
fn the_desired_set_is_order_independent() {
    let forward = desired_pools(
        &[
            iws("infra", "a", full_spec()),
            iws("infra", "b", full_spec()),
        ],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    let reverse = desired_pools(
        &[
            iws("infra", "b", full_spec()),
            iws("infra", "a", full_spec()),
        ],
        &[profile("h100")],
        &DiscoveryDefaults::default(),
    );
    assert_eq!(
        forward.0.keys().collect::<Vec<_>>(),
        reverse.0.keys().collect::<Vec<_>>()
    );
}

// --- watch → reconcile, driven against hand-seeded reflector stores. ---
//
// Same approach as `policy::store`'s tests and `discovery::kubernetes`'s:
// a `reflector::Store` is populated by hand, so the interesting behavior is
// exercised without a cluster or an HTTP mock.

fn watcher_with(
    sets: &[InferenceWorkerSet],
    profiles: Vec<ProfileConfig>,
) -> (WorkerSetWatcher, std::sync::Arc<crate::pool::PoolRegistry>) {
    let (store, mut writer) = reflector::store::<InferenceWorkerSet>();
    for s in sets {
        writer.apply_watcher_event(&watcher::Event::Apply(s.clone()));
    }
    let registry = crate::pool::PoolRegistry::new();
    let manager = crate::pool_manager::PoolManager::new(
        registry.clone(),
        std::sync::Arc::new(NoopHooks),
        CancellationToken::new(),
        std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        crate::pool_manager::ReapPolicy {
            linger: Duration::ZERO,
            backstop: Duration::from_secs(600),
        },
    );
    let cfg = crate::config::KubernetesConfig {
        enabled: true,
        namespace: Some("infra".into()),
        selector: None,
        discovery_defaults: DiscoveryDefaults::default(),
    };
    (
        WorkerSetWatcher::new(
            &cfg,
            profiles,
            manager,
            RejectionStore::new(),
            std::sync::Arc::new(crate::prom::PromMetrics::new()),
        )
        .with_store(store),
        registry,
    )
}

struct NoopHooks;
impl crate::pool_manager::PoolHooks for NoopHooks {
    fn on_create(
        &self,
        _pool: &std::sync::Arc<crate::pool::Pool>,
        _spec: &PoolSpec,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn on_retire(&self, _pool: &std::sync::Arc<crate::pool::Pool>) {}
}

#[tokio::test]
async fn a_listed_store_materializes_its_sets() {
    let (mut w, registry) = watcher_with(&[iws("infra", "a", full_spec())], vec![profile("h100")]);
    w.reconcile_from_store();
    assert_eq!(registry.snapshot().len(), 1);
    assert_eq!(registry.snapshot().iter().next().unwrap().name, "infra/a");
}

/// The gate, at pool granularity. `desired_pools` is TOTAL, so reconciling
/// against a store that has not finished its initial list retires EVERY
/// pool in the fleet — and then rebuilds them a moment later, having thrown
/// away every prefix cache in between. A watch reconnect must be invisible,
/// not a fleet-wide cache flush.
///
/// This test asserts the consequence directly: an empty store, reconciled,
/// really does retract everything. The production guard is that
/// `run_once` does not call this until it has seen `InitDone`.
#[tokio::test]
async fn reconciling_against_an_empty_store_would_retract_every_pool() {
    let (mut w, registry) = watcher_with(
        &[
            iws("infra", "a", full_spec()),
            iws("infra", "b", full_spec()),
        ],
        vec![profile("h100")],
    );
    w.reconcile_from_store();
    assert_eq!(registry.snapshot().len(), 2);

    let (empty, _) = reflector::store::<InferenceWorkerSet>();
    w.store = Some(empty);
    w.reconcile_from_store();
    assert_eq!(
        registry.snapshot().len(),
        0,
        "an empty store retracts everything — which is exactly why the \
         reconcile is gated on InitDone"
    );
}

/// A set losing its profile must not take the healthy ones with it.
#[tokio::test]
async fn one_rejected_set_does_not_disturb_the_others() {
    let mut bad = full_spec();
    bad.profile = Some("nope".into());
    let (mut w, registry) = watcher_with(
        &[iws("infra", "good", full_spec()), iws("infra", "bad", bad)],
        vec![profile("h100")],
    );
    w.reconcile_from_store();
    assert_eq!(registry.snapshot().len(), 1);
    assert_eq!(
        registry.snapshot().iter().next().unwrap().name,
        "infra/good"
    );
}

/// Re-reconciling an unchanged store must be a no-op, or every watch event
/// on an unrelated object would churn the whole fleet.
#[tokio::test]
async fn re_reconciling_an_unchanged_store_keeps_the_same_pools() {
    let (mut w, registry) = watcher_with(&[iws("infra", "a", full_spec())], vec![profile("h100")]);
    w.reconcile_from_store();
    let id = registry.snapshot().iter().next().unwrap().id;
    w.reconcile_from_store();
    assert_eq!(registry.snapshot().iter().next().unwrap().id, id);
}
