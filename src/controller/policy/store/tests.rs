//! Event classification and the reflector stores that feed `resolve`.
use super::*;
use kinference_policy::crd::{Destination, InferencePoolSpec, InferenceWorkerSetSpec, ServiceRef};
use kinference_policy::resolve::RejectionIndex;
use kube::api::ObjectMeta;

fn meta(name: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some("infra".into()),
        ..Default::default()
    }
}

fn worker_set(name: &str) -> InferenceWorkerSet {
    InferenceWorkerSet {
        metadata: meta(name),
        spec: InferenceWorkerSetSpec {
            service_ref: ServiceRef {
                name: name.into(),
                port: None,
            },
            profile: Some("h100".into()),
            kv_block_size: Some(16),
            ..Default::default()
        },
        status: None,
    }
}

fn pool(name: &str, dests: Vec<Destination>) -> InferencePool {
    InferencePool {
        metadata: meta(name),
        spec: InferencePoolSpec {
            destinations: dests,
            policy_ref: None,
        },
        status: None,
    }
}

/// The reflector-store path works end to end, not just `resolve`.
#[test]
fn seeding_a_reflector_store_feeds_resolve() {
    let (store, mut writer) = reflector::store::<InferenceWorkerSet>();
    writer.apply_watcher_event(&watcher::Event::Apply(worker_set("workers-h100")));
    let sets = store.state();
    assert_eq!(sets.len(), 1);
    let key = kinference_policy::PoolKey::namespaced("infra", "workers-h100");
    let id = kinference_policy::PoolId::from_raw(1);
    let idx = resolve(
        &[Arc::new(pool(
            "chat",
            vec![Destination {
                kind: Some("InferenceWorkerSet".into()),
                name: Some("workers-h100".into()),
                ..Default::default()
            }],
        ))],
        &[],
        &sets,
        &[],
        &[],
        &[],
        &PoolIndex::from([(key, id)]),
        &RejectionIndex::new(),
    );
    let p = idx.lookup("infra", "chat").expect("pool resolved");
    assert_eq!(p.destinations.len(), 1);
    assert_eq!(
        p.destinations[0]
            .as_local()
            .expect("a local destination")
            .pool_id,
        id,
        "the destination resolves to the pool the set became"
    );
}

/// The run loop gates publishing on `InitDone`; misclassifying it would
/// publish a snapshot missing every pool, or never publish.
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
    assert!(!ev(watcher::Event::Apply(pool("chat", vec![]))));

    // An ended stream must be rebuildable, never terminal.
    assert!(matches!(
        step::<InferencePool>(Ok(None), "InferencePool"),
        Step::Ended
    ));
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
#[test]
fn a_404_on_the_initial_list_reads_as_an_empty_store() {
    assert!(crd_not_installed(&watcher::Error::InitialListFailed(
        api_err(404)
    )));
    assert!(crd_not_installed(&watcher::Error::WatchStartFailed(
        api_err(404)
    )));
}

/// Everything else is retried rather than read as "no objects".
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
