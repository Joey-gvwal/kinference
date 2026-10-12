//! Driven against hand-built `Service` objects — the same no-cluster approach
//! as `store`'s and `discovery::kubernetes`'s tests.
use super::*;
use k8s_openapi::api::core::v1::ServiceSpec;
use kube::api::ObjectMeta;

/// `(name, port)` pairs, in declaration order. An unnamed port is `""`, which
/// is how a single-port Service is usually written.
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

fn index(services: &[Arc<Service>]) -> ServiceIndex {
    ServiceIndex::build(services)
}

fn name(s: &str) -> IntOrString {
    IntOrString::Str(s.into())
}

#[test]
fn a_named_port_resolves_to_the_service_port() {
    let idx = index(&[service(
        "infra",
        "workers-h100",
        &[("http", 80), ("metrics", 9090)],
    )]);
    // 80, not the pod's 8000: the whole reason to read the Service.
    assert_eq!(
        idx.port_number("infra", "workers-h100", Some(&name("http"))),
        Ok(80)
    );
    assert_eq!(
        idx.port_number("infra", "workers-h100", Some(&name("metrics"))),
        Ok(9090)
    );
}

#[test]
fn an_unknown_port_name_is_reported_rather_than_guessed() {
    let idx = index(&[service("infra", "workers-h100", &[("http", 80)])]);
    assert_eq!(
        idx.port_number("infra", "workers-h100", Some(&name("htpp"))),
        Err(UnresolvedPort::UnknownPortName)
    );
}

/// The case that motivates reading the Service at all. A number kinference cannot
/// see on the Service must NOT be passed through: a `BackendBinding` carrying
/// it would resolve, program a route, and fail every request.
#[test]
fn a_numeric_port_must_be_one_the_service_exposes() {
    let idx = index(&[service("infra", "workers-h100", &[("http", 80)])]);
    assert_eq!(
        idx.port_number("infra", "workers-h100", Some(&IntOrString::Int(80))),
        Ok(80)
    );
    assert_eq!(
        idx.port_number("infra", "workers-h100", Some(&IntOrString::Int(8000))),
        Err(UnresolvedPort::PortNotExposed)
    );
}

/// No legal Service port name is all digits, so a quoted number was meant as a
/// number.
#[test]
fn a_numeric_string_is_read_as_a_number() {
    let idx = index(&[service("infra", "workers-h100", &[("http", 80)])]);
    assert_eq!(
        idx.port_number("infra", "workers-h100", Some(&name("80"))),
        Ok(80)
    );
}

#[test]
fn an_omitted_port_resolves_only_when_there_is_exactly_one() {
    let single = index(&[service("infra", "workers-h100", &[("", 80)])]);
    assert_eq!(single.port_number("infra", "workers-h100", None), Ok(80));

    let multi = index(&[service(
        "infra",
        "workers-h100",
        &[("http", 80), ("metrics", 9090)],
    )]);
    assert_eq!(
        multi.port_number("infra", "workers-h100", None),
        Err(UnresolvedPort::AmbiguousPort),
        "picking one of two would silently route inference traffic at /metrics"
    );
}

#[test]
fn a_service_with_no_ports_has_none_to_resolve() {
    let idx = index(&[service("infra", "workers-h100", &[])]);
    assert_eq!(
        idx.port_number("infra", "workers-h100", Some(&name("http"))),
        Err(UnresolvedPort::NoUsablePort)
    );
}

/// Namespace is part of the key. Getting this wrong would let one namespace's
/// Service answer for another's — the same collapse the pool index avoids by
/// keying on identity.
#[test]
fn the_namespace_is_part_of_the_identity() {
    let idx = index(&[
        service("infra", "workers-h100", &[("http", 80)]),
        service("other", "workers-h100", &[("http", 8080)]),
    ]);
    assert_eq!(
        idx.port_number("infra", "workers-h100", Some(&name("http"))),
        Ok(80)
    );
    assert_eq!(
        idx.port_number("other", "workers-h100", Some(&name("http"))),
        Ok(8080)
    );
    assert_eq!(
        idx.port_number("nowhere", "workers-h100", Some(&name("http"))),
        Err(UnresolvedPort::ServiceNotFound)
    );
}

/// The distinction the publish gate does NOT make for us: an index that has
/// not listed must not report a Service missing.
#[test]
fn an_unlisted_index_says_so_instead_of_reporting_absence() {
    let idx = ServiceIndex::unlisted();
    assert!(!idx.listed());
    assert_eq!(
        idx.port_number("infra", "workers-h100", Some(&name("http"))),
        Err(UnresolvedPort::NotListed),
        "a consumer must be able to tell startup from a missing Service"
    );
    assert!(index(&[]).listed(), "an empty cluster still lists");
}

#[test]
fn the_whole_object_is_kept_for_later_consumers() {
    let idx = index(&[service("infra", "workers-h100", &[("http", 80)])]);
    assert!(idx.contains("infra", "workers-h100"));
    assert_eq!(idx.len(), 1);
    let svc = idx.get("infra", "workers-h100").unwrap();
    assert_eq!(svc.name_any(), "workers-h100");
    assert_eq!(idx.ports("infra", "workers-h100").unwrap().len(), 1);
}
