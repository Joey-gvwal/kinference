use super::*;

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};

use kinference_policy::PoolId;
use kinference_policy::compile::CompiledPolicy;
use kinference_policy::crd::{Condition, IntOrString};
use kinference_policy::engine::ObjectKey;
use kinference_policy::resolve::{ResolvedProvider, ResolvedWorkerSet};

/// The default picker extension name.
const EXT: Option<&str> = Some("chat-picker");

fn ws(name: &str, service: &str, namespace: &str) -> ResolvedDestination {
    ws_port(name, service, namespace, None)
}

fn ws_port(
    name: &str,
    service: &str,
    namespace: &str,
    port: Option<IntOrString>,
) -> ResolvedDestination {
    ResolvedDestination::Local(ResolvedWorkerSet {
        name: name.into(),
        namespace: namespace.into(),
        labels: BTreeMap::new(),
        pool_id: PoolId::from_raw(0),
        service_name: service.into(),
        service_port: port,
        kv_events_declared: false,
        aliases: Vec::new(),
    })
}

fn service(ns: &str, name: &str, ports: &[(&str, i32)]) -> Arc<Service> {
    Arc::new(Service {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(ns.into()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(
                ports
                    .iter()
                    .map(|(n, p)| ServicePort {
                        name: (!n.is_empty()).then(|| n.to_string()),
                        port: *p,
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }),
        status: None,
    })
}

/// A single-port Service for every worker set the fixtures name. Single-port
/// because `ws` omits `serviceRef.port`, which only resolves in that case.
fn index() -> ServiceIndex {
    ServiceIndex::build(&[
        service("infra", "vllm-h100", &[("", 8080)]),
        service("infra", "vllm-a100", &[("", 8080)]),
        service("gpu", "vllm-a100", &[("", 8080)]),
    ])
}

/// `desired_binding` against that index, asserting nothing was omitted.
fn desired(pool: &ResolvedPool, extension: Option<&str>) -> BackendBinding {
    let d = desired_binding(pool, extension, &index());
    assert!(d.omitted.is_empty(), "fixture ports: {:?}", d.omitted);
    d.binding
}

fn provider(name: &str, backend_ns: &str, backend: &str) -> ResolvedDestination {
    ResolvedDestination::Provider(ResolvedProvider {
        name: name.into(),
        namespace: "infra".into(),
        uid: None,
        labels: BTreeMap::new(),
        backend_ref: ObjectKey::new(backend_ns, backend),
        path: None,
        models: Vec::new(),
    })
}

fn pool(dests: Vec<ResolvedDestination>) -> ResolvedPool {
    ResolvedPool {
        uid: Some("pool-uid".into()),
        namespace: "infra".into(),
        name: "chat".into(),
        resource_version: "100".into(),
        generation: Some(3),
        policy: Arc::new(CompiledPolicy::serve_as_asked("<implicit>")),
        dangling_policy: None,
        destinations: dests,
        unresolved: Vec::new(),
        parents: Vec::new(),
    }
}

/// The claim is what a route's `InferencePool` backendRef is matched against.
#[test]
fn claims_the_pool_by_group_kind_name() {
    let b = desired(&pool(vec![]), EXT);
    assert_eq!(b.spec.claim.group, "kinference.buoyant.io");
    assert_eq!(b.spec.claim.kind, "InferencePool");
    assert_eq!(b.spec.claim.name, "chat");
    // The gateway matches a claim against refs in the binding's namespace.
    assert_eq!(b.metadata.namespace.as_deref(), Some("infra"));
    assert_eq!(b.metadata.name.as_deref(), Some("chat"));
}

/// A worker set becomes its Service, on the port the Service exposes.
#[test]
fn worker_set_becomes_a_service_ref_on_its_service_port() {
    let b = desired(&pool(vec![ws("workers-h100", "vllm-h100", "infra")]), EXT);
    assert_eq!(b.spec.backend_refs.len(), 1);
    let r = &b.spec.backend_refs[0];
    // The entry name is the destination's.
    assert_eq!(r.name, "workers-h100");
    assert_eq!(r.target.name, "vllm-h100");
    // Unset group and kind mean a core Service.
    assert_eq!(r.target.group, None);
    assert_eq!(r.target.kind, None);
    // The Service's only port, since the fixture's `serviceRef` names none.
    assert_eq!(r.target.port, Some(8080));
    // Same namespace as the binding, so it is left to default.
    assert_eq!(r.target.namespace, None);
}

/// A port named by `serviceRef` is resolved to its number.
#[test]
fn a_named_service_ref_port_resolves_to_its_number() {
    let idx = ServiceIndex::build(&[service(
        "infra",
        "vllm-h100",
        &[("http", 80), ("metrics", 9090)],
    )]);
    let p = pool(vec![ws_port(
        "workers-h100",
        "vllm-h100",
        "infra",
        Some(IntOrString::Str("http".into())),
    )]);
    let d = desired_binding(&p, EXT, &idx);
    assert!(d.omitted.is_empty());
    // 80, the Service port, not the pod's target port.
    assert_eq!(d.binding.spec.backend_refs[0].target.port, Some(80));
}

/// A Service in another namespace is looked up there, not in the pool's
/// namespace.
#[test]
fn a_cross_namespace_port_is_read_from_the_targets_own_namespace() {
    let idx = ServiceIndex::build(&[
        service("infra", "vllm-a100", &[("http", 80)]),
        service("gpu", "vllm-a100", &[("http", 9000)]),
    ]);
    let p = pool(vec![ws_port(
        "remote",
        "vllm-a100",
        "gpu",
        Some(IntOrString::Str("http".into())),
    )]);
    let d = desired_binding(&p, EXT, &idx);
    let r = &d.binding.spec.backend_refs[0];
    assert_eq!(r.target.namespace.as_deref(), Some("gpu"));
    assert_eq!(r.target.port, Some(9000));
}

/// A ref whose port can't be resolved is left out and reported, never written
/// portless.
#[test]
fn a_destination_whose_port_does_not_resolve_is_omitted_with_its_reason() {
    let idx = ServiceIndex::build(&[service(
        "infra",
        "vllm-h100",
        &[("http", 80), ("metrics", 9090)],
    )]);

    // Two ports and no name: ambiguous, so kinference won't guess.
    let d = desired_binding(
        &pool(vec![ws("workers-h100", "vllm-h100", "infra")]),
        EXT,
        &idx,
    );
    assert!(d.binding.spec.backend_refs.is_empty());
    assert_eq!(
        d.omitted,
        [("workers-h100".to_string(), UnresolvedPort::AmbiguousPort)]
    );

    // A typo'd port name, and a Service that is not there at all.
    let p = pool(vec![
        ws_port(
            "workers-h100",
            "vllm-h100",
            "infra",
            Some(IntOrString::Str("htpp".into())),
        ),
        ws_port(
            "workers-a100",
            "vllm-a100",
            "infra",
            Some(IntOrString::Str("http".into())),
        ),
    ]);
    let d = desired_binding(&p, EXT, &idx);
    assert!(d.binding.spec.backend_refs.is_empty());
    assert_eq!(
        d.omitted,
        [
            ("workers-h100".to_string(), UnresolvedPort::UnknownPortName),
            ("workers-a100".to_string(), UnresolvedPort::ServiceNotFound),
        ]
    );
}

/// One destination's unresolvable port doesn't drop the pool's other refs.
#[test]
fn an_omitted_destination_leaves_the_rest_of_the_binding_intact() {
    let idx = ServiceIndex::build(&[
        service("infra", "vllm-h100", &[("", 8080)]),
        service("infra", "vllm-a100", &[("http", 80), ("metrics", 9090)]),
    ]);
    let p = pool(vec![
        ws("workers-h100", "vllm-h100", "infra"),
        ws("workers-a100", "vllm-a100", "infra"),
        provider("openai", "infra", "openai-backend"),
    ]);
    let d = desired_binding(&p, EXT, &idx);
    let names: Vec<&str> = d
        .binding
        .spec
        .backend_refs
        .iter()
        .map(|r| r.name.as_str())
        .collect();
    assert_eq!(names, ["workers-h100", "openai"]);
    assert_eq!(d.omitted.len(), 1);
}

/// Against an index that has not listed, every worker set is omitted as
/// `NotListed` rather than written portless.
#[test]
fn an_unlisted_index_omits_every_service_ref() {
    let p = pool(vec![
        ws("workers-h100", "vllm-h100", "infra"),
        provider("openai", "infra", "openai-backend"),
    ]);
    let d = desired_binding(&p, EXT, &ServiceIndex::unlisted());
    // Providers don't need a Service.
    assert_eq!(d.binding.spec.backend_refs.len(), 1);
    assert_eq!(d.binding.spec.backend_refs[0].name, "openai");
    assert_eq!(
        d.omitted,
        [("workers-h100".to_string(), UnresolvedPort::NotListed)]
    );
}

/// A provider becomes the `GatewayBackend` it names, without a port.
#[test]
fn provider_becomes_a_gateway_backend_ref() {
    let b = desired(
        &pool(vec![provider("openai", "infra", "openai-backend")]),
        EXT,
    );
    let r = &b.spec.backend_refs[0];
    assert_eq!(r.name, "openai");
    assert_eq!(r.target.group.as_deref(), Some("gateway.linkerd.io"));
    assert_eq!(r.target.kind.as_deref(), Some("GatewayBackend"));
    assert_eq!(r.target.name, "openai-backend");
    assert_eq!(r.target.port, None);
}

/// A target's namespace is set only when it differs from the binding's.
#[test]
fn only_cross_namespace_targets_carry_a_namespace() {
    let b = desired(
        &pool(vec![
            ws("local", "vllm-h100", "infra"),
            ws("remote", "vllm-a100", "gpu"),
            provider("openai", "vendors", "openai-backend"),
        ]),
        EXT,
    );
    assert_eq!(b.spec.backend_refs[0].target.namespace, None);
    assert_eq!(
        b.spec.backend_refs[1].target.namespace.as_deref(),
        Some("gpu")
    );
    assert_eq!(
        b.spec.backend_refs[2].target.namespace.as_deref(),
        Some("vendors")
    );
}

/// Destination order is the pool's, and both classes land in one list.
#[test]
fn both_classes_share_one_ordered_list() {
    let b = desired(
        &pool(vec![
            ws("workers-h100", "vllm-h100", "infra"),
            provider("openai", "infra", "openai-backend"),
            ws("workers-a100", "vllm-a100", "infra"),
        ]),
        EXT,
    );
    let names: Vec<&str> = b
        .spec
        .backend_refs
        .iter()
        .map(|r| r.name.as_str())
        .collect();
    assert_eq!(names, ["workers-h100", "openai", "workers-a100"]);
}

/// A pool with no resolved destinations still gets an empty binding.
#[test]
fn a_pool_with_no_resolved_destination_still_gets_a_binding() {
    let b = desired(&pool(vec![]), EXT);
    assert!(b.spec.backend_refs.is_empty());
}

/// The binding is owned by its pool and labeled as ours.
#[test]
fn labels_and_owner_reference_mark_the_binding_as_ours() {
    let b = desired(&pool(vec![]), EXT);
    assert!(is_ours(&b));
    assert_eq!(b.labels().get(POOL_LABEL).map(String::as_str), Some("chat"));
    let owner = &b.metadata.owner_references.as_ref().unwrap()[0];
    assert_eq!(owner.kind, "InferencePool");
    assert_eq!(owner.api_version, "kinference.buoyant.io/v1alpha1");
    assert_eq!(owner.uid, "pool-uid");
    assert_eq!(owner.controller, Some(true));
}

/// A pool without a known UID still gets a binding, just an unowned one.
#[test]
fn a_pool_without_a_uid_gets_no_owner_reference() {
    let mut p = pool(vec![]);
    p.uid = None;
    let b = desired(&p, EXT);
    assert!(b.metadata.owner_references.is_none());
}

/// An object without our label is someone else's, whatever it claims.
#[test]
fn an_unlabelled_binding_is_not_ours() {
    let mut b = desired(&pool(vec![]), EXT);
    b.metadata.labels = None;
    assert!(!is_ours(&b));
}

/// Equal specs compare in sync, so the writer doesn't patch every tick.
#[test]
fn identical_bindings_are_in_sync() {
    let want = desired(&pool(vec![ws("workers-h100", "vllm-h100", "infra")]), EXT);
    assert!(in_sync(&want.clone(), &want));
}

/// ...and a drifted one doesn't, in either direction.
#[test]
fn drift_is_not_in_sync() {
    let want = desired(&pool(vec![ws("workers-h100", "vllm-h100", "infra")]), EXT);

    let fewer = desired(&pool(vec![]), EXT);
    assert!(!in_sync(&fewer, &want));

    let mut edited = want.clone();
    edited.spec.backend_refs[0].target.name = "someone-elses-service".into();
    assert!(!in_sync(&edited, &want));

    // An extension added out of band is drift too.
    let mut extended = want.clone();
    extended.spec.extensions = vec!["kinference-selector".into()];
    assert!(!in_sync(&extended, &want));
}

/// Owners compare by UID, since a recreated pool keeps its name.
#[test]
fn an_owner_reference_to_a_dead_pool_is_not_in_sync() {
    let want = desired(&pool(vec![]), EXT);
    let mut stale = want.clone();
    stale.metadata.owner_references.as_mut().unwrap()[0].uid = "older-pool-uid".into();
    assert!(!in_sync(&stale, &want));
}

/// Fields kinference does not set are not compared.
#[test]
fn foreign_metadata_does_not_count_as_drift() {
    let want = desired(&pool(vec![]), EXT);
    let mut annotated = want.clone();
    annotated.metadata.annotations = Some(BTreeMap::from([(
        "example.com/owner".to_string(),
        "platform".to_string(),
    )]));
    annotated
        .metadata
        .labels
        .as_mut()
        .unwrap()
        .insert("example.com/team".into(), "platform".into());
    assert!(in_sync(&annotated, &want));
}

/// The binding lists the picker extension.
#[test]
fn the_configured_extension_is_listed() {
    let b = desired(&pool(vec![]), EXT);
    assert_eq!(b.spec.extensions, ["chat-picker"]);

    let renamed = desired(&pool(vec![]), Some("infra-picker"));
    assert_eq!(renamed.spec.extensions, ["infra-picker"]);
}

/// A cleared extension lists none, for routes carrying their own
/// `extensionRef` filter.
#[test]
fn no_configured_extension_lists_none() {
    let b = desired(&pool(vec![]), None);
    assert!(b.spec.extensions.is_empty());
}

/// A blank `backend_binding_extension` reads as none.
#[test]
fn a_blank_configured_name_is_no_extension() {
    let cfg = crate::controller::policy::PolicyConfig::default();
    assert_eq!(cfg.binding_extension(), Some("chat-picker"));

    let cleared = crate::controller::policy::PolicyConfig {
        backend_binding_extension: "  ".into(),
        ..Default::default()
    };
    assert_eq!(cleared.binding_extension(), None);
}

/// Renaming the extension registers as drift on existing bindings.
#[test]
fn a_changed_extension_is_drift() {
    let want = desired(&pool(vec![]), EXT);
    let stale = desired(&pool(vec![]), Some("infra-picker"));
    assert!(!in_sync(&stale, &want));
    // And so does clearing it.
    let cleared = desired(&pool(vec![]), None);
    assert!(!in_sync(&cleared, &want));
}

fn condition(status: &str) -> Condition {
    Condition {
        r#type: "Accepted".into(),
        status: status.into(),
        observed_generation: None,
        last_transition_time: "2026-01-01T00:00:00Z".into(),
        reason: "NoNativeTarget".into(),
        message: "backendRefs is empty".into(),
    }
}

/// Only an explicit `Accepted=False` counts as a rejection.
#[test]
fn only_a_false_accepted_condition_reads_as_rejected() {
    let mut b = desired(&pool(vec![]), EXT);
    assert_eq!(
        rejected_reason(&b),
        None,
        "no status yet is not a rejection"
    );

    b.status = Some(kinference_policy::crd::BackendBindingStatus {
        conditions: vec![condition("True")],
    });
    assert_eq!(rejected_reason(&b), None);

    b.status = Some(kinference_policy::crd::BackendBindingStatus {
        conditions: vec![condition("False")],
    });
    let reason = rejected_reason(&b).expect("rejected");
    assert!(reason.contains("NoNativeTarget"), "{reason}");
    assert!(reason.contains("backendRefs is empty"), "{reason}");
}
