//! Resolution is driven directly against hand-seeded reflector stores — no
//! cluster, no HTTP mock — the same approach as
//! `discovery::kubernetes`'s tests.
use super::*;
use crate::policy::crd::{
    BackendObjectRef, Destination, HTTPRouteRule, InferencePolicySpec, InferencePoolSpec,
    InferenceProviderSpec, InferenceWorkerSetSpec, LabelSelector, PolicyRef, ProviderModel,
    ReferenceGrantFrom, ReferenceGrantSpec, ReferenceGrantTo, ServiceRef,
};
use kube::api::ObjectMeta;

/// The Nth destination as a worker set. Every test written before providers
/// existed is asserting about a local one, and panicking here beats an
/// `unwrap` that would read as incidental.
fn local(p: &ResolvedPool, i: usize) -> &ResolvedWorkerSet {
    p.destinations[i]
        .as_local()
        .expect("test expects a local destination")
}

/// `resolve` with no providers and no grants — the shape every pre-provider
/// test wants, and saying so beats two `&[]` at each call site.
fn resolve_local(
    pools: &[Arc<InferencePool>],
    policies: &[Arc<InferencePolicy>],
    worker_sets: &[Arc<InferenceWorkerSet>],
    routes: &[Arc<HTTPRoute>],
    pools_by_key: &PoolIndex,
    rejections: &RejectionIndex,
) -> PolicyIndex {
    resolve(
        pools,
        policies,
        worker_sets,
        &[],
        routes,
        &[],
        pools_by_key,
        rejections,
    )
}

fn meta(name: &str, ns: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(ns.into()),
        resource_version: Some("100".into()),
        ..Default::default()
    }
}

fn worker_set(name: &str, service: &str, labels: &[(&str, &str)]) -> Arc<InferenceWorkerSet> {
    let mut m = meta(name, "infra");
    if !labels.is_empty() {
        m.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
    }
    Arc::new(InferenceWorkerSet {
        metadata: m,
        spec: InferenceWorkerSetSpec {
            service_ref: ServiceRef {
                name: service.into(),
                port: None,
            },
            // Valid by default: the CRD requires both, and a fixture that
            // omitted them would make every test resolve as a REJECTION
            // rather than exercising the thing it means to.
            profile: Some("h100".into()),
            kv_block_size: Some(16),
            ..Default::default()
        },
        status: None,
    })
}

fn pool(name: &str, dests: Vec<Destination>, policy: Option<&str>) -> Arc<InferencePool> {
    Arc::new(InferencePool {
        metadata: meta(name, "infra"),
        spec: InferencePoolSpec {
            destinations: dests,
            policy_ref: policy.map(|p| PolicyRef {
                group: None,
                kind: None,
                name: p.into(),
            }),
        },
        status: None,
    })
}

fn provider(
    name: &str,
    ns: &str,
    backend: &str,
    backend_ns: Option<&str>,
    models: Vec<(&str, &str, Option<u32>)>,
) -> Arc<InferenceProvider> {
    Arc::new(InferenceProvider {
        metadata: meta(name, ns),
        spec: InferenceProviderSpec {
            backend_ref: Some(BackendObjectRef {
                group: None,
                kind: None,
                name: Some(backend.into()),
                namespace: backend_ns.map(str::to_string),
            }),
            api: Some(ApiFormat::OpenAI),
            // Set per-test; the overwhelming case is a provider that speaks
            // the path the client already sent.
            path: None,
            models: models
                .into_iter()
                // (canonical, upstream, ctx) — the canonical is now an
                // alias of the upstream, so existing cases are unchanged.
                .map(|(n, up, ctx)| ProviderModel {
                    name: Some(up.into()),
                    aliases: if n == up { vec![] } else { vec![n.into()] },
                    context_window: ctx,
                })
                .collect(),
        },
        status: None,
    })
}

fn dest_provider(name: &str, ns: Option<&str>) -> Destination {
    Destination {
        kind: Some("InferenceProvider".into()),
        name: Some(name.into()),
        namespace: ns.map(str::to_string),
        ..Default::default()
    }
}

/// A grant in `to_ns` letting `from_ns`'s InferenceProviders reach
/// GatewayBackends: what a cross-namespace `backendRef` needs.
fn grant(to_ns: &str, from_ns: &str, name: Option<&str>) -> Arc<ReferenceGrant> {
    grant_from(
        "InferenceProvider",
        from_ns,
        to_ns,
        (GATEWAY_BACKEND_GROUP, GATEWAY_BACKEND_KIND),
        name,
    )
}

/// A grant in `to_ns` letting `from_kind` objects in `from_ns` reach the
/// `to` group and kind there, by `name` or all of them.
fn grant_from(
    from_kind: &str,
    from_ns: &str,
    to_ns: &str,
    to: (&str, &str),
    name: Option<&str>,
) -> Arc<ReferenceGrant> {
    Arc::new(ReferenceGrant {
        metadata: meta("g", to_ns),
        spec: ReferenceGrantSpec {
            from: vec![ReferenceGrantFrom {
                group: Some("kinference.buoyant.io".into()),
                kind: Some(from_kind.into()),
                namespace: Some(from_ns.into()),
            }],
            to: vec![ReferenceGrantTo {
                group: Some(to.0.into()),
                kind: Some(to.1.into()),
                name: name.map(str::to_string),
            }],
        },
    })
}

/// A one-rule InferencePolicy. `type: Local` on the tier because the CRD's
/// Local-only rule requires it for `PredictedTTFT`, and a fixture that would
/// be rejected on the cluster is a poor stand-in for one.
fn policy(name: &str) -> Arc<InferencePolicy> {
    Arc::new(InferencePolicy {
        metadata: meta(name, "infra"),
        spec: serde_json::from_value::<InferencePolicySpec>(serde_json::json!({
            "rules": [{
                "name": "r",
                "route": {"select": [{"type": "Local", "minimize": "PredictedTTFT"}]}
            }]
        }))
        .unwrap(),
        status: None,
    })
}

fn dest_named(name: &str) -> Destination {
    Destination {
        kind: Some("InferenceWorkerSet".into()),
        name: Some(name.into()),
        ..Default::default()
    }
}

/// The identity index the resolver consults: (namespace, name) → pool.
///
/// In production this comes off the live pool set, where every entry exists
/// because a worker set materialized into it. These tests build it from
/// every name the module uses, so resolution failures in a test mean the
/// thing under test failed rather than the fixture being incomplete — the
/// cases that WANT a miss build their own index.
fn pools() -> PoolIndex {
    [
        "a",
        "b",
        "private",
        "shared-1",
        "workers-h100",
        "workers-a100",
    ]
    .into_iter()
    .enumerate()
    .map(|(i, name)| {
        (
            crate::policy::PoolKey::namespaced("infra", name),
            PoolId::from_raw(i as u64),
        )
    })
    .collect()
}

/// Prove the reflector-store path works end to end, not just `resolve`.
#[test]
fn seeding_a_reflector_store_feeds_resolve() {
    let (store, mut writer) = reflector::store::<InferenceWorkerSet>();
    writer.apply_watcher_event(&watcher::Event::Apply(
        (*worker_set("workers-h100", "vllm-h100", &[])).clone(),
    ));
    let sets = store.state();
    assert_eq!(sets.len(), 1);
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &sets,
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool resolved");
    assert_eq!(p.destinations.len(), 1);
    assert_eq!(
        local(p, 0).pool_id,
        pools()[&crate::policy::PoolKey::namespaced("infra", "workers-h100")],
        "the destination resolves to the pool the set became"
    );
}

/// A `lookup` miss means two opposite things — "no such pool" and "I have
/// not listed yet" — and the pick path reacts differently to each. Only a
/// snapshot built from listed stores may be read as authoritative.
#[test]
fn the_default_snapshot_is_not_listed_but_a_resolved_one_is() {
    assert!(
        !PolicyIndex::default().listed(),
        "before the first list, a miss must not be read as 'no such pool'"
    );
    let idx = resolve_local(&[], &[], &[], &[], &pools(), &RejectionIndex::new());
    assert!(
        idx.listed(),
        "anything resolve_local() builds came from listed stores"
    );
    assert!(idx.is_empty(), "listed and empty are independent facts");
}

/// `InitDone` is what promotes a fresh store from partial to whole. The run
/// loop gates publishing on it, so misclassifying it would either publish a
/// snapshot with every pool missing, or never publish at all.
#[test]
fn only_init_done_reports_listed() {
    let ev = |e: watcher::Event<InferencePool>| {
        matches!(
            step::<InferencePool>(Ok(Some(e)), "InferencePool"),
            Step::Listed
        )
    };
    assert!(ev(watcher::Event::InitDone));
    assert!(!ev(watcher::Event::Init));
    assert!(!ev(watcher::Event::Apply(
        (*pool("chat", vec![], None)).clone()
    )));

    // An ended stream must be rebuildable, never terminal.
    assert!(matches!(
        step::<InferencePool>(Ok(None), "InferencePool"),
        Step::Ended
    ));
}

#[test]
fn worker_set_binds_to_the_pool_with_the_matching_service() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(local(p, 0).service_name, "vllm-h100");
    assert!(p.unresolved.is_empty());
}

/// A worker set the router has not materialized into a pool. Normally the
/// brief window between the object appearing and the reconcile running, so
/// it fails open — the pool is still served by whatever else resolved —
/// and says so on status rather than silently listing one destination
/// fewer.
#[test]
fn a_worker_set_with_no_pool_yet_is_unresolved() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-a100")], None)],
        &[],
        &[worker_set("workers-a100", "vllm-a100", &[])],
        &[],
        // The set is valid; the router just has not created its pool.
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert!(p.destinations.is_empty());
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::NoMatchingPool);
    assert_eq!(p.unresolved[0].name, "workers-a100");
}

#[test]
fn missing_destination_is_not_found() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("gone")], None)],
        &[],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::NotFound);
}

#[test]
fn empty_destinations_means_every_worker_set_in_the_namespace() {
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[
            worker_set("a", "vllm-h100", &[]),
            worker_set("b", "vllm-h100", &[]),
        ],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    assert_eq!(idx.lookup("infra", "chat").unwrap().destinations.len(), 2);
}

#[test]
fn selector_destination_matches_by_label() {
    let dest = Destination {
        kind: Some("InferenceWorkerSet".into()),
        selector: Some(LabelSelector {
            match_labels: [("tier".to_string(), "shared".to_string())]
                .into_iter()
                .collect(),
        }),
        ..Default::default()
    };
    let idx = resolve_local(
        &[pool("chat", vec![dest], None)],
        &[],
        &[
            worker_set("shared-1", "vllm-h100", &[("tier", "shared")]),
            worker_set("private", "vllm-h100", &[("tier", "private")]),
        ],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.destinations.len(), 1);
    assert_eq!(local(p, 0).name, "shared-1");
}

#[test]
fn a_set_matched_twice_is_only_added_once() {
    // A name and a selector can both cover the same worker set.
    let by_label = Destination {
        kind: Some("InferenceWorkerSet".into()),
        selector: Some(LabelSelector {
            match_labels: [("tier".to_string(), "shared".to_string())]
                .into_iter()
                .collect(),
        }),
        ..Default::default()
    };
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("shared-1"), by_label], None)],
        &[],
        &[worker_set("shared-1", "vllm-h100", &[("tier", "shared")])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    assert_eq!(
        idx.lookup("infra", "chat").unwrap().destinations.len(),
        1,
        "destinationCount must not double-count"
    );
}

/// End to end for the portless-`kvEvents` case: `desired_pools` refuses it,
/// `rejection_index` projects the reason, and `bind` hands back `Incomplete`
/// — the same shape as a provider missing its `backendRef`, because in both
/// the object simply does not say enough to route to.
#[test]
fn a_portless_kv_events_block_binds_as_incomplete() {
    use crate::policy::crd::KvEvents;

    let mut ws = worker_set("workers-h100", "vllm-h100", &[]);
    Arc::get_mut(&mut ws).unwrap().spec.kv_events = Some(KvEvents {
        port: None,
        replay_port: None,
    });

    let (pools_built, rejections) = crate::policy::worker_sets::desired_pools(
        std::slice::from_ref(&*ws),
        &[crate::config::ProfileConfig {
            name: "h100".into(),
            predictor: crate::config::PredictorConfig::default(),
            picker: crate::config::PickerConfig::default(),
            tokenize: crate::config::ProfileTokenizeConfig {
                mode: crate::config::TokenizeMode::Local,
            },
        }],
        &crate::config::DiscoveryDefaults::default(),
    );
    assert!(pools_built.is_empty(), "refused, so it produced no pool");

    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        std::slice::from_ref(&ws),
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

/// The `dummy` case: an `InferenceWorkerSet` no `InferencePool` names must
/// still appear in the index, or the status writer never sees it and the
/// object keeps no `status` key at all.
#[test]
fn a_worker_set_no_pool_names_is_still_published() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[
            worker_set("workers-h100", "vllm-h100", &[]),
            worker_set("workers-a100", "vllm-a100", &[]),
        ],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let sets: Vec<_> = idx.worker_sets().collect();
    assert_eq!(
        sets.len(),
        2,
        "both sets published, not just the referenced one"
    );

    let named = sets.iter().find(|e| e.name == "workers-h100").unwrap();
    assert_eq!(named.referenced_by, vec!["chat".to_string()]);
    assert!(named.bound.is_ok());

    let orphan = sets.iter().find(|e| e.name == "workers-a100").unwrap();
    assert!(
        orphan.referenced_by.is_empty(),
        "no pool names it, so nothing to report"
    );
    assert!(
        orphan.bound.is_ok(),
        "it bound to a live pool — being unreferenced is not being broken"
    );
}

/// Two pools routing to one set both get named. A bool would have lost the
/// second, and which pool is the actionable half of the report.
#[test]
fn a_worker_set_reached_by_two_pools_lists_both_sorted() {
    let idx = resolve_local(
        &[
            pool("zeta", vec![dest_named("workers-h100")], None),
            pool("alpha", vec![dest_named("workers-h100")], None),
        ],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let e = idx.worker_sets().next().unwrap();
    assert_eq!(
        e.referenced_by,
        vec!["alpha".to_string(), "zeta".to_string()]
    );
}

/// A pool with no `destinations` means "every set in this namespace", and a
/// set reached that way is as referenced as one named outright — which is
/// why `referenced_by` is accumulated from the RESOLVED destinations rather
/// than from `spec.destinations`.
#[test]
fn an_implicitly_reached_worker_set_counts_as_referenced() {
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let e = idx.worker_sets().next().unwrap();
    assert_eq!(e.referenced_by, vec!["chat".to_string()]);
}

/// A set kinference has no pool for is published too, carrying the reason. Its
/// own object is the only place that can be said when no pool names it.
#[test]
fn a_worker_set_with_no_pool_is_published_with_its_reason() {
    let idx = resolve_local(
        &[],
        &[],
        &[worker_set("no-such-pool", "vllm-missing", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let e = idx.worker_sets().next().unwrap();
    assert_eq!(
        e.bound.as_ref().err(),
        Some(&UnresolvedReason::NoMatchingPool)
    );
}

/// `policyRef.group`/`kind` were parsed and then discarded, so
/// `{kind: ConfigMap, name: x}` quietly resolved to the InferencePolicy named
/// `x`. Either the fields mean something or they should not be in the CRD.
#[test]
fn a_policy_ref_naming_the_wrong_kind_dangles() {
    let mut p = pool("chat", vec![dest_named("workers-h100")], Some("real"));
    Arc::get_mut(&mut p).unwrap().spec.policy_ref = Some(PolicyRef {
        group: None,
        kind: Some("ConfigMap".into()),
        name: "real".into(),
    });
    let idx = resolve_local(
        std::slice::from_ref(&p),
        &[policy("real")],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let r = idx.lookup("infra", "chat").unwrap();
    assert_eq!(
        r.dangling_policy,
        Some(DanglingPolicy::WrongKind {
            name: "real".into(),
            group: "kinference.buoyant.io".into(),
            kind: "ConfigMap".into(),
        }),
        "the policy EXISTS; the ref is what is wrong"
    );
    assert!(r.policy.rules.is_empty(), "fails open, serving as asked");
    // And the policy it wrongly named must not count as referenced.
    let pol = idx.policies().next().unwrap();
    assert!(pol.referenced_by.is_empty());
}

/// The default group/kind are the right ones, so omitting them still resolves.
#[test]
fn an_omitted_group_and_kind_still_resolve() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], Some("real"))],
        &[policy("real")],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let r = idx.lookup("infra", "chat").unwrap();
    assert_eq!(r.dangling_policy, None);
}

#[test]
fn pool_without_policy_ref_serves_as_asked() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert!(p.policy.rules.is_empty());
    assert!(p.dangling_policy.is_none());
}

#[test]
fn dangling_policy_ref_fails_open_and_is_reported() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], Some("nope"))],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.dangling_policy.as_ref().map(|d| d.name()), Some("nope"));
    // Fail open: still routable, just unfiltered.
    assert_eq!(p.destinations.len(), 1);
    assert!(p.policy.rules.is_empty());
}

#[test]
fn policy_ref_resolves_and_compiles() {
    let policy = Arc::new(InferencePolicy {
        metadata: meta("chat-policy", "infra"),
        spec: serde_json::from_value::<InferencePolicySpec>(serde_json::json!({
            "rules": [{"name": "r", "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
        }))
        .unwrap(),
        status: None,
    });
    let idx = resolve_local(
        &[pool(
            "chat",
            vec![dest_named("workers-h100")],
            Some("chat-policy"),
        )],
        &[policy],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert!(p.dangling_policy.is_none());
    assert_eq!(p.policy.rules.len(), 1);
    assert_eq!(p.policy.rules[0].name, "r");
}

#[test]
fn parents_come_from_httproutes_naming_the_pool() {
    let route = Arc::new(HTTPRoute {
        metadata: meta("llm", "infra"),
        spec: serde_json::from_value(serde_json::json!({
            "parentRefs": [{"kind": "Gateway", "name": "inference-gateway"}],
            "rules": [{"backendRefs": [
                {"group": "kinference.buoyant.io", "kind": "InferencePool", "name": "chat"}]}]
        }))
        .unwrap(),
    });
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[],
        &[route],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.parents.len(), 1);
    assert_eq!(p.parents[0].name, "inference-gateway");
    // The namespace defaults to the route's, so status names the Gateway
    // unambiguously.
    assert_eq!(p.parents[0].namespace.as_deref(), Some("infra"));
}

#[test]
fn a_route_targeting_a_different_pool_is_not_a_parent() {
    let route = Arc::new(HTTPRoute {
        metadata: meta("other", "infra"),
        spec: serde_json::from_value(serde_json::json!({
            "parentRefs": [{"name": "gw"}],
            "rules": [{"backendRefs": [
                {"group": "kinference.buoyant.io", "kind": "InferencePool", "name": "elsewhere"}]}]
        }))
        .unwrap(),
    });
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[],
        &[route],
        &pools(),
        &RejectionIndex::new(),
    );
    assert!(idx.lookup("infra", "chat").unwrap().parents.is_empty());
}

#[test]
fn a_service_backend_ref_is_not_a_parent() {
    // Only kinference.buoyant.io InferencePool refs count.
    let route = Arc::new(HTTPRoute {
        metadata: meta("svc-route", "infra"),
        spec: HTTPRouteSpecForTest::service_backend("chat"),
    });
    let idx = resolve_local(
        &[pool("chat", vec![], None)],
        &[],
        &[],
        &[route],
        &pools(),
        &RejectionIndex::new(),
    );
    assert!(idx.lookup("infra", "chat").unwrap().parents.is_empty());
}

struct HTTPRouteSpecForTest;
impl HTTPRouteSpecForTest {
    fn service_backend(name: &str) -> crate::policy::crd::HTTPRouteSpec {
        crate::policy::crd::HTTPRouteSpec {
            parent_refs: vec![ParentRef {
                name: "gw".into(),
                ..Default::default()
            }],
            rules: vec![HTTPRouteRule {
                backend_refs: vec![crate::policy::crd::BackendRef {
                    // No group/kind → a core Service, not our pool.
                    name: name.into(),
                    ..Default::default()
                }],
            }],
        }
    }
}

#[test]
fn unsupported_destination_kind_is_reported() {
    let dest = Destination {
        kind: Some("Service".into()),
        name: Some("svc".into()),
        ..Default::default()
    };
    let idx = resolve_local(
        &[pool("chat", vec![dest], None)],
        &[],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::UnsupportedKind);
}

/// Identity carries the namespace, so a same-named set in another namespace
/// is a different pool. Getting this wrong would merge two fleets' prefix
/// models while every request still succeeded.
#[test]
fn a_pool_in_another_namespace_does_not_claim_this_worker_set() {
    let elsewhere: PoolIndex = [(
        crate::policy::PoolKey::namespaced("other", "workers-h100"),
        PoolId::from_raw(0),
    )]
    .into_iter()
    .collect();
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &elsewhere,
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(
        p.unresolved[0].reason,
        UnresolvedReason::NoMatchingPool,
        "a pool in another namespace must not claim this worker set"
    );
}

/// A pool must report WHY a destination is unusable, not just that it is.
///
/// The reason comes from the resolver, which is the only thing that knows
/// whether a profile exists — this module cannot see the router's TOML. A
/// generic `NoMatchingPool` here would send an operator to inspect Services
/// over a typo in a config file.
#[test]
fn an_unresolved_destination_reports_the_resolvers_reason() {
    use crate::policy::rejection::{RejectReason, Rejection};
    let sets = [
        worker_set("workers-h100", "vllm-h100", &[]),
        worker_set("workers-a100", "vllm-a100", &[]),
    ];
    let rejections = rejection_index(&[
        Rejection {
            key: crate::policy::PoolKey::namespaced("infra", "workers-h100"),
            reason: RejectReason::UnknownProfile {
                defined: vec!["h100".into()],
            },
        },
        Rejection {
            key: crate::policy::PoolKey::namespaced("infra", "workers-a100"),
            reason: RejectReason::MissingKvBlockSize,
        },
    ]);
    let idx = resolve_local(
        &[pool(
            "chat",
            vec![dest_named("workers-h100"), dest_named("workers-a100")],
            None,
        )],
        &[],
        &sets,
        &[],
        // Neither produced a pool, which is why they were rejected.
        &PoolIndex::new(),
        &rejections,
    );
    let p = idx.lookup("infra", "chat").unwrap();
    let reasons: Vec<UnresolvedReason> = p.unresolved.iter().map(|u| u.reason).collect();
    assert!(
        reasons.contains(&UnresolvedReason::UnknownProfile),
        "got {reasons:?}"
    );
    assert!(
        reasons.contains(&UnresolvedReason::InvalidKvBlockSize),
        "got {reasons:?}"
    );
}

/// With nothing recorded against it, a set with no pool is simply "no pool
/// yet" — the ordinary window between the object appearing and the
/// reconcile running, which must not be dressed up as a misconfiguration.
#[test]
fn an_unrecorded_miss_stays_no_matching_pool() {
    let idx = resolve_local(
        &[pool("chat", vec![dest_named("workers-h100")], None)],
        &[],
        &[worker_set("workers-h100", "vllm-h100", &[])],
        &[],
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").unwrap();
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::NoMatchingPool);
}

// --- alias resolution -------------------------------------------------

fn set_with_aliases(aliases: &[(&str, &str)]) -> ResolvedWorkerSet {
    ResolvedWorkerSet {
        name: "ws".into(),
        namespace: "infra".into(),
        labels: BTreeMap::new(),
        pool_id: PoolId::from_raw(0),
        service_name: "svc".into(),
        service_port: None,
        kv_events_declared: false,
        aliases: aliases
            .iter()
            // Fixtures still pass (canonical, upstream); under the new shape
            // the canonical is an ALIAS of the upstream, so every existing
            // case asks the same question.
            .map(|(n, u)| ModelAlias {
                name: Some(u.to_string()),
                aliases: if n == u { vec![] } else { vec![n.to_string()] },
            })
            .collect(),
    }
}

#[test]
fn two_models_sharing_an_alias_yield_two_upstreams() {
    let ws = set_with_aliases(&[
        ("bar", "meta-llama/Llama-3.3-70B-Instruct"),
        ("bar", "neuralmagic/Llama-3.3-70B-Instruct-FP8"),
    ]);
    let up = ws.upstreams_for("bar");
    assert!(up.contains(&"meta-llama/Llama-3.3-70B-Instruct".to_string()));
    assert!(up.contains(&"neuralmagic/Llama-3.3-70B-Instruct-FP8".to_string()));
    // Plus the canonical name itself, in case it is also served directly.
    assert!(up.contains(&"bar".to_string()));
}

#[test]
fn no_aliases_means_identity() {
    let ws = set_with_aliases(&[]);
    assert_eq!(ws.upstreams_for("gemma"), vec!["gemma".to_string()]);
}

#[test]
fn aliases_are_additive_not_a_whitelist() {
    // Declaring an alias for one model must not hide the others.
    let ws = set_with_aliases(&[("foo", "meta-llama/Llama-3.1-8B-Instruct")]);
    assert_eq!(ws.upstreams_for("qwen"), vec!["qwen".to_string()]);
}

#[test]
fn canonical_for_inverts_the_alias_table() {
    let ws = set_with_aliases(&[("bar", "neuralmagic/Llama-3.3-70B-Instruct-FP8")]);
    assert_eq!(
        ws.canonical_for("neuralmagic/Llama-3.3-70B-Instruct-FP8"),
        "bar"
    );
    assert_eq!(
        ws.canonical_for("unlisted"),
        "unlisted",
        "identity fallback"
    );
}

// --- providers ---------------------------------------------------------

#[test]
fn a_provider_destination_resolves_to_a_backend_reference() {
    let prov = provider(
        "together",
        "infra",
        "together-backend",
        None,
        vec![("bar", "acme-llama-70b", Some(131072))],
    );
    let idx = resolve(
        &[pool("chat", vec![dest_provider("together", None)], None)],
        &[],
        &[],
        &[prov],
        &[],
        &[],
        // Deliberately EMPTY: a provider resolves without any pool at all.
        // It is not backed by one, which is the whole difference.
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert!(p.unresolved.is_empty(), "{:?}", p.unresolved);
    let got = p.destinations[0].as_provider().expect("provider");
    assert_eq!(got.name, "together");
    // The namespace is defaulted HERE, so the pick response is always fully
    // qualified and the caller never has to guess one.
    assert_eq!(got.backend_ref, ObjectKey::new("infra", "together-backend"));
    assert_eq!(
        got.upstreams_for("bar"),
        vec![("acme-llama-70b".to_string(), Some(131072))]
    );
}

/// The path is carried, not interpreted: kinference has no URL to check it against.
///
/// Absence and emptiness collapse to the same `None`, because the wire spells
/// "no rewrite" as an empty field and a value that means exactly what absence
/// means should not travel four hops pretending to be a decision.
#[test]
fn a_provider_path_travels_verbatim_and_empty_means_absent() {
    let mut prov = provider("together", "infra", "b", None, vec![("bar", "up", None)]);
    Arc::get_mut(&mut prov).unwrap().spec.path =
        Some("/serving-endpoints/chat/completions?api-version=2024-10-21".into());
    let r = resolve_provider(&prov, &[]).expect("resolves");
    assert_eq!(
        r.path.as_deref(),
        Some("/serving-endpoints/chat/completions?api-version=2024-10-21"),
        "the query string is part of the path the caller dispatches on"
    );

    // Whitespace is not a path either, and an operator who clears the field by
    // blanking it means the same as one who deletes it.
    for blank in ["", "   "] {
        Arc::get_mut(&mut prov).unwrap().spec.path = Some(blank.into());
        assert_eq!(resolve_provider(&prov, &[]).expect("resolves").path, None);
    }

    Arc::get_mut(&mut prov).unwrap().spec.path = None;
    assert_eq!(resolve_provider(&prov, &[]).expect("resolves").path, None);
}

/// A path is never a reason to refuse a destination.
///
/// Unlike `backendRef` and `api`, nothing downstream needs it to be well-formed
/// to produce a candidate, and a provider that would otherwise serve is worth
/// more than the typo it was applied with — which the schema's `^/` catches
/// anyway, for everything not applied with `--validate=false`.
#[test]
fn a_malformed_provider_path_does_not_unresolve_the_provider() {
    let mut prov = provider("together", "infra", "b", None, vec![("bar", "up", None)]);
    Arc::get_mut(&mut prov).unwrap().spec.path = Some("no-leading-slash".into());
    let r = resolve_provider(&prov, &[]).expect("still resolves");
    assert_eq!(r.path.as_deref(), Some("no-leading-slash"));
}

#[test]
fn a_provider_missing_a_required_field_is_incomplete_not_dropped() {
    // Required by the CRD schema, `Option` in Rust on purpose — so this is the
    // layer that has to enforce it, and it must report rather than skip.
    let mut prov = provider("together", "infra", "b", None, vec![("bar", "up", None)]);
    Arc::get_mut(&mut prov).unwrap().spec.api = None;
    let idx = resolve(
        &[pool("chat", vec![dest_provider("together", None)], None)],
        &[],
        &[],
        &[prov],
        &[],
        &[],
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert!(p.destinations.is_empty());
    assert_eq!(p.unresolved.len(), 1);
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::Incomplete);
    assert_eq!(p.unresolved[0].kind, "InferenceProvider");
}

#[test]
fn a_provider_with_no_usable_model_row_is_incomplete() {
    let prov = provider("together", "infra", "b", None, vec![]);
    let idx = resolve(
        &[pool("chat", vec![dest_provider("together", None)], None)],
        &[],
        &[],
        &[prov],
        &[],
        &[],
        &PoolIndex::new(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::Incomplete);
}

#[test]
fn a_destination_with_no_kind_is_rejected_rather_than_assumed_local() {
    // `kind` has no default now that a destination can be either class.
    // Guessing between a pod fleet and a paid vendor is not a guess to make
    // silently.
    let dest = Destination {
        kind: None,
        name: Some("a".into()),
        ..Default::default()
    };
    let idx = resolve_local(
        &[pool("chat", vec![dest], None)],
        &[],
        &[worker_set("a", "vllm-a", &[])],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert!(p.destinations.is_empty());
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::UnsupportedKind);
    assert_eq!(p.unresolved[0].kind, "<unset>");
}

#[test]
fn a_name_shared_across_kinds_is_ambiguous_and_neither_is_admitted() {
    // A policy names a destination by bare string, so admitting both would
    // make `backends.names: [a]` mean two different things at once and leave
    // `order` unable to express a preference between them.
    let idx = resolve(
        &[pool(
            "chat",
            vec![dest_named("a"), dest_provider("a", None)],
            None,
        )],
        &[],
        &[worker_set("a", "vllm-a", &[])],
        &[provider("a", "infra", "b", None, vec![("bar", "up", None)])],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert_eq!(p.destinations.len(), 1, "the first one stands");
    assert_eq!(p.unresolved.len(), 1);
    assert_eq!(p.unresolved[0].reason, UnresolvedReason::AmbiguousName);
}

#[test]
fn a_cross_namespace_backend_ref_needs_a_grant_and_fails_closed() {
    let prov = provider(
        "together",
        "infra",
        "shared-backend",
        Some("vendors"),
        vec![("bar", "up", None)],
    );
    let call = |grants: &[Arc<ReferenceGrant>]| {
        resolve(
            &[pool("chat", vec![dest_provider("together", None)], None)],
            &[],
            &[],
            std::slice::from_ref(&prov),
            &[],
            grants,
            &PoolIndex::new(),
            &RejectionIndex::new(),
        )
    };

    // No grant: refused. This is the boundary that stops one namespace
    // spending another's vendor credential, so an unseen grant has to read as
    // an absent one.
    let p = call(&[]);
    let p = p.lookup("infra", "chat").expect("pool");
    assert!(p.destinations.is_empty());
    assert_eq!(
        p.unresolved[0].reason,
        UnresolvedReason::CrossNamespaceNotGranted
    );

    // A grant in the wrong namespace does not help either.
    let p = call(&[grant("elsewhere", "infra", None)]);
    assert_eq!(
        p.lookup("infra", "chat").unwrap().unresolved[0].reason,
        UnresolvedReason::CrossNamespaceNotGranted
    );

    // A grant naming a DIFFERENT backend does not help.
    let p = call(&[grant("vendors", "infra", Some("other-backend"))]);
    assert_eq!(
        p.lookup("infra", "chat").unwrap().unresolved[0].reason,
        UnresolvedReason::CrossNamespaceNotGranted
    );

    // Nor does one for InferencePools. `from` names the kind holding the
    // reference, and here that is the provider: the namespace's owner granted
    // pools something, not providers its credential.
    let p = call(&[grant_from(
        "InferencePool",
        "infra",
        "vendors",
        (GATEWAY_BACKEND_GROUP, GATEWAY_BACKEND_KIND),
        None,
    )]);
    assert_eq!(
        p.lookup("infra", "chat").unwrap().unresolved[0].reason,
        UnresolvedReason::CrossNamespaceNotGranted
    );

    // The grant that actually covers it, both wildcard and by name.
    for g in [
        grant("vendors", "infra", None),
        grant("vendors", "infra", Some("shared-backend")),
    ] {
        let p = call(&[g]);
        let p = p.lookup("infra", "chat").expect("pool");
        assert!(p.unresolved.is_empty(), "{:?}", p.unresolved);
        assert_eq!(
            p.destinations[0].as_provider().unwrap().backend_ref,
            ObjectKey::new("vendors", "shared-backend")
        );
    }
}

/// A pool's destination in another namespace needs a grant there from
/// InferencePools: the pool holds that reference. One from InferenceProviders,
/// as a provider's `backendRef` needs, does not cover it.
#[test]
fn a_cross_namespace_destination_needs_a_grant_for_pools() {
    // Provider and its backend both live in `vendors`; only the pool crosses.
    let prov = provider(
        "together",
        "vendors",
        "shared-backend",
        None,
        vec![("bar", "up", None)],
    );
    let call = |grants: &[Arc<ReferenceGrant>]| {
        resolve(
            &[pool(
                "chat",
                vec![dest_provider("together", Some("vendors"))],
                None,
            )],
            &[],
            &[],
            std::slice::from_ref(&prov),
            &[],
            grants,
            &PoolIndex::new(),
            &RejectionIndex::new(),
        )
    };
    let to_providers = ("kinference.buoyant.io", "InferenceProvider");
    let reason = |grants: &[Arc<ReferenceGrant>]| {
        call(grants)
            .lookup("infra", "chat")
            .expect("pool")
            .unresolved
            .first()
            .map(|u| u.reason)
    };

    assert_eq!(
        reason(&[]),
        Some(UnresolvedReason::CrossNamespaceNotGranted)
    );
    assert_eq!(
        reason(&[grant_from(
            "InferenceProvider",
            "infra",
            "vendors",
            to_providers,
            None
        )]),
        Some(UnresolvedReason::CrossNamespaceNotGranted)
    );
    assert_eq!(
        reason(&[grant_from(
            "InferencePool",
            "infra",
            "vendors",
            to_providers,
            Some("together")
        )]),
        None
    );
}

#[test]
fn an_omitted_destinations_list_never_picks_up_a_provider() {
    // "Omit to mean every InferenceWorkerSet in this namespace" — Local only,
    // so reaching something that costs money always requires naming it.
    let idx = resolve(
        &[pool("chat", vec![], None)],
        &[],
        &[worker_set("a", "vllm-a", &[])],
        &[provider(
            "together",
            "infra",
            "b",
            None,
            vec![("bar", "up", None)],
        )],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool");
    assert_eq!(p.destinations.len(), 1);
    assert!(p.destinations[0].as_local().is_some());
}

/// The shape: `name` is the VENDOR's spelling and matches on its own;
/// `aliases` add client-facing names on top. A row with no aliases is still
/// reachable, which is what removes the need for a separate `upstreamName`.
#[test]
fn a_provider_model_matches_its_own_name_and_every_alias() {
    use crate::policy::crd::ProviderModel;
    let mut prov = provider("vendor", "infra", "b", None, vec![]);
    Arc::get_mut(&mut prov).unwrap().spec.models = vec![
        ProviderModel {
            name: Some("Qwen/Qwen2.5-32B-Instruct-Turbo".into()),
            aliases: vec!["qwen-2.5".into(), "qwen-simple".into()],
            context_window: Some(32768),
        },
        // No aliases at all — the case the old shape could not express
        // without repeating the name twice.
        ProviderModel {
            name: Some("meta/llama-3.3-70b".into()),
            aliases: vec![],
            context_window: None,
        },
    ];
    let r = resolve_provider(&prov, &[]).expect("resolves");

    let upstream =
        |n: &str| -> Vec<String> { r.upstreams_for(n).into_iter().map(|(u, _)| u).collect() };
    // Every alias reaches the vendor's model, and so does its own name.
    for n in ["qwen-2.5", "qwen-simple", "Qwen/Qwen2.5-32B-Instruct-Turbo"] {
        assert_eq!(
            upstream(n),
            vec!["Qwen/Qwen2.5-32B-Instruct-Turbo".to_string()],
            "{n} must resolve to the vendor's spelling"
        );
    }
    // An aliasless row is reachable under its own name.
    assert_eq!(
        upstream("meta/llama-3.3-70b"),
        vec!["meta/llama-3.3-70b".to_string()]
    );
    // And nothing else is. `models[]` is exhaustive for a provider — there is
    // no discovery to widen it.
    assert!(upstream("gpt-4").is_empty());

    // The context window travels with the row, so every alias inherits it.
    assert_eq!(r.upstreams_for("qwen-simple")[0].1, Some(32768));

    assert_eq!(
        r.served_names(),
        vec![
            "Qwen/Qwen2.5-32B-Instruct-Turbo",
            "meta/llama-3.3-70b",
            "qwen-2.5",
            "qwen-simple",
        ]
    );
}

#[test]
fn provider_served_names_report_what_is_declared() {
    let prov = provider(
        "together",
        "infra",
        "b",
        None,
        vec![
            ("bar", "up-1", None),
            ("foo", "up-2", None),
            ("bar", "up-3", None),
        ],
    );
    let r = resolve_provider(&prov, &[]).expect("resolves");
    // Both spellings of every row: the vendor's own name matches with no
    // alias involved, so `up-1` is as routable as the `bar` that aliases it.
    assert_eq!(r.served_names(), vec!["bar", "foo", "up-1", "up-2", "up-3"]);
    // Two rows sharing one alias are two candidates, exactly as two
    // worker-set aliases would be.
    assert_eq!(r.upstreams_for("bar").len(), 2);
    // And the upstream name resolves to itself — the identity case a row
    // with no aliases relies on.
    assert_eq!(
        r.upstreams_for("up-2"),
        vec![("up-2".to_string(), None)],
        "a request naming the vendor's own model must reach it"
    );
}

// --- the publish gate vs. an uninstalled CRD ---------------------------

fn api_err(code: u16) -> kube::Error {
    kube::Error::Api(Box::new(kube::core::Status {
        code,
        message: "the server could not find the requested resource".into(),
        ..Default::default()
    }))
}

/// A missing CRD must not hold the publish gate shut.
///
/// Before this, an uninstalled optional CRD errored forever, its `listed`
/// flag never flipped, and the all-kinds gate then blocked EVERY publish — so
/// a cluster without the Gateway API CRDs silently had no policy routing at
/// all, with only a repeating warning to say so. Two of the six watched kinds
/// are genuinely optional, so it was a real configuration to fall into.
#[test]
fn a_404_on_the_initial_list_reads_as_an_empty_store() {
    assert!(crd_not_installed(&watcher::Error::InitialListFailed(
        api_err(404)
    )));
    assert!(crd_not_installed(&watcher::Error::WatchStartFailed(
        api_err(404)
    )));
}

/// Everything else stays a retry. A 403 is bad RBAC and a 500 is a bad
/// moment; both are fixable without a manifest change and must NOT be read as
/// "there are no objects" — for ReferenceGrant that would turn a permissions
/// problem into a silently permissive one.
#[test]
fn other_failures_are_still_transient() {
    for code in [401, 403, 409, 429, 500, 503] {
        assert!(
            !crd_not_installed(&watcher::Error::InitialListFailed(api_err(code))),
            "{code} must be retried, not treated as an absent CRD"
        );
    }
    assert!(!crd_not_installed(&watcher::Error::WatchFailed(api_err(
        404
    ))));
    assert!(!crd_not_installed(&watcher::Error::NoResourceVersion));
}

/// With no grants visible, no cross-namespace reference is permitted.
///
/// The direction matters: an absent ReferenceGrant CRD and an empty one are
/// indistinguishable here, and both have to mean "not granted". A check whose
/// job is to stop one namespace spending another's vendor credential cannot
/// fail open on its own unavailability.
#[test]
fn an_absent_grant_store_permits_nothing() {
    assert!(!grant_permits(
        &[],
        "vendors",
        GATEWAY_BACKEND_GROUP,
        GATEWAY_BACKEND_KIND,
        Some("shared-backend"),
        "infra",
        "InferenceProvider",
    ));
}

// --- every policy is published, referenced or not -----------------------

#[test]
fn a_policy_no_pool_references_is_still_published() {
    let idx = resolve_local(
        // The pool names `attached`; `orphan` exists and nothing points at it.
        &[pool("chat", vec![], Some("attached"))],
        &[policy("attached"), policy("orphan")],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let got: Vec<(&str, usize)> = idx
        .policies()
        .map(|p| (p.name.as_str(), p.referenced_by.len()))
        .collect();
    // Both appear. Reaching policies only through pools is what left an
    // unattached one with no status at all, and therefore indistinguishable
    // from one that had failed to compile.
    assert_eq!(got, vec![("attached", 1), ("orphan", 0)]);
}

#[test]
fn referenced_by_names_the_pools() {
    let idx = resolve_local(
        &[
            pool("chat", vec![], Some("shared")),
            pool("batch", vec![], Some("shared")),
        ],
        &[policy("shared")],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let p = idx.policies().next().expect("policy");
    let mut by = p.referenced_by.clone();
    by.sort();
    // Naming the pools is what makes the status actionable, and a policy
    // reached by two pools is legal rather than an error.
    assert_eq!(by, vec!["batch", "chat"]);
}

#[test]
fn a_dangling_policy_ref_leaves_no_policy_to_publish() {
    let idx = resolve_local(
        &[pool("chat", vec![], Some("typo"))],
        &[],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    // Nothing to publish: there is no object named `typo`. The pool reports it
    // instead, which is the only place it CAN be reported.
    assert_eq!(idx.policies().count(), 0);
    let p = idx.lookup("infra", "chat").expect("pool");
    assert_eq!(p.dangling_policy.as_ref().map(|d| d.name()), Some("typo"));
}

#[test]
fn the_published_order_is_stable() {
    // The status writer's last-written comparison is what breaks the write
    // loop; an unstable order would make it miss and rewrite every tick.
    let call = || {
        resolve_local(
            &[pool("chat", vec![], Some("b"))],
            &[policy("c"), policy("a"), policy("b")],
            &[],
            &[],
            &pools(),
            &RejectionIndex::new(),
        )
    };
    let names = |i: &PolicyIndex| -> Vec<String> { i.policies().map(|p| p.name.clone()).collect() };
    assert_eq!(names(&call()), vec!["a", "b", "c"]);
    assert_eq!(names(&call()), names(&call()));
}

#[test]
fn a_policy_shared_by_two_pools_is_compiled_once() {
    let idx = resolve_local(
        &[
            pool("chat", vec![], Some("shared")),
            pool("batch", vec![], Some("shared")),
        ],
        &[policy("shared")],
        &[],
        &[],
        &pools(),
        &RejectionIndex::new(),
    );
    let a = idx.lookup("infra", "chat").unwrap().policy.clone();
    let b = idx.lookup("infra", "batch").unwrap().policy.clone();
    assert!(
        Arc::ptr_eq(&a, &b),
        "both pools should share one compilation"
    );
    assert!(Arc::ptr_eq(&a, &idx.policies().next().unwrap().policy));
}

// --- OpenAI-compatible only --------------------------------------------

/// The scope gate. A format nothing translates to must not become a
/// destination, because every request routed there would fail.
#[test]
fn a_non_openai_api_is_refused() {
    for api in [
        ApiFormat::Anthropic,
        ApiFormat::Bedrock,
        ApiFormat::VertexAI,
        ApiFormat::AzureOpenAI,
    ] {
        let mut prov = provider("p", "infra", "b", None, vec![("bar", "up", None)]);
        Arc::get_mut(&mut prov).unwrap().spec.api = Some(api);
        let idx = resolve(
            &[pool("chat", vec![dest_provider("p", None)], None)],
            &[],
            &[],
            std::slice::from_ref(&prov),
            &[],
            &[],
            &PoolIndex::new(),
            &RejectionIndex::new(),
        );
        let p = idx.lookup("infra", "chat").expect("pool");
        assert!(p.destinations.is_empty(), "{api:?} must not resolve");
        assert_eq!(
            p.unresolved[0].reason,
            UnresolvedReason::UnsupportedApiFormat,
            "{api:?}"
        );
    }
}

/// Enforced at RESOLVE, not only in the CRD enum.
///
/// The schema stops a new write; this stops an object that predates the
/// narrowing, or was applied with `--validate=false`. And `ApiFormat` keeps
/// every variant on purpose so such an object still deserializes — failing to
/// parse it would fail the whole watcher list page and take provider
/// discovery down for the namespace. Parse it, then refuse it.
#[test]
fn a_bypassed_object_still_parses_before_it_is_refused() {
    let spec: crate::policy::crd::InferenceProviderSpec =
        serde_json::from_value(serde_json::json!({
            "backendRef": {"name": "b"},
            "api": "Anthropic",
            "models": [{"name": "up", "aliases": ["bar"]}]
        }))
        .expect("a value the CRD no longer admits must still DESERIALIZE");
    assert_eq!(spec.api, Some(ApiFormat::Anthropic));

    let prov = Arc::new(InferenceProvider {
        metadata: meta("p", "infra"),
        spec,
        status: None,
    });
    assert_eq!(
        resolve_provider(&prov, &[]).unwrap_err(),
        UnresolvedReason::UnsupportedApiFormat
    );
}

/// `api` is validated and then discarded — the wire carries no format, so the
/// value has one legal setting and nothing downstream reads it.
#[test]
fn openai_still_resolves() {
    let prov = provider("p", "infra", "b", None, vec![("bar", "up", None)]);
    let r = resolve_provider(&prov, &[]).expect("resolves");
    assert_eq!(r.name, "p");
    assert_eq!(r.backend_ref, ObjectKey::new("infra", "b"));
}

/// The `Service` view is ATTACHED to a snapshot, not resolved into it — so a
/// bare `resolve` carries an unlisted one, and a consumer that reads a miss as
/// an absence would be wrong to trust it.
#[test]
fn a_resolved_snapshot_carries_no_services_until_the_watcher_attaches_them() {
    let idx = resolve_local(&[], &[], &[], &[], &pools(), &RejectionIndex::new());
    assert!(idx.listed(), "the CRD side of this snapshot is complete");
    assert!(
        !idx.services().listed(),
        "and the Service side is not, because nothing here read one"
    );

    let attached = resolve_local(&[], &[], &[], &[], &pools(), &RejectionIndex::new())
        .with_services(crate::policy::services::ServiceIndex::build(&[]));
    assert!(attached.services().listed());
    assert!(attached.services().is_empty());
}
