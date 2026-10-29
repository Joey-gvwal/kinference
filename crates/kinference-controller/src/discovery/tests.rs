//! Reconciliation tests, driving `reconcile()` against a hand-populated
//! `reflector::Store<EndpointSlice>`.
use super::*;
use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::api::discovery::v1::{EndpointConditions, EndpointPort};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

fn default_defaults() -> DiscoveryDefaults {
    DiscoveryDefaults::default()
}

fn ready_endpoint(pod_uid: &str, pod_name: &str, address: &str) -> Endpoint {
    Endpoint {
        addresses: vec![address.to_string()],
        conditions: Some(EndpointConditions {
            ready: Some(true),
            serving: Some(true),
            terminating: Some(false),
        }),
        target_ref: Some(ObjectReference {
            kind: Some("Pod".to_string()),
            name: Some(pod_name.to_string()),
            namespace: Some("default".to_string()),
            uid: Some(pod_uid.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn not_ready_endpoint(pod_uid: &str, pod_name: &str, address: &str) -> Endpoint {
    let mut ep = ready_endpoint(pod_uid, pod_name, address);
    ep.conditions.as_mut().unwrap().ready = Some(false);
    ep
}

fn slice(slice_uid: &str, name: &str, endpoints: Vec<Endpoint>) -> EndpointSlice {
    EndpointSlice {
        address_type: "IPv4".to_string(),
        endpoints: Some(endpoints),
        metadata: ObjectMeta {
            uid: Some(slice_uid.to_string()),
            name: Some(name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        ports: Some(vec![EndpointPort {
            name: Some("http".to_string()),
            port: Some(8000),
            ..Default::default()
        }]),
    }
}

fn drain(rx: &mut mpsc::Receiver<WorkerChange>) -> Vec<WorkerChange> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        out.push(msg);
    }
    out
}

/// A slice with a caller-chosen port list, for the port-resolution tests.
/// `Some(vec![])` is the state Kubernetes leaves once the last pod goes.
fn slice_with_ports(
    slice_uid: &str,
    name: &str,
    endpoints: Vec<Endpoint>,
    ports: Vec<EndpointPort>,
) -> EndpointSlice {
    let mut s = slice(slice_uid, name, endpoints);
    s.ports = Some(ports);
    s
}

fn port(name: &str, n: i32) -> EndpointPort {
    EndpointPort {
        name: Some(name.to_string()),
        port: Some(n),
        ..Default::default()
    }
}

/// Replay is scoped to the kvEvents declaration, like the publish socket.
#[test]
fn no_kv_events_declaration_resolves_neither_socket() {
    let sl = slice_with_ports(
        "u1",
        "svc",
        vec![],
        vec![
            port("http", 8000),
            port("kv-pub", 5557),
            port("kv-replay", 5558),
        ],
    );
    let refs = PortRefs {
        kv_events_declared_absent: true,
        ..PortRefs::default()
    };
    let got = extract_named_ports(
        &sl,
        &DiscoveryDefaults::default(),
        &refs,
        &mut HashSet::new(),
    );
    assert_eq!(got.zmq, None, "declared absent, so no publish socket");
    assert_eq!(
        got.replay, None,
        "and nothing to replay: the Service HAS a kv-replay port, and it must \
         still not be resolved"
    );
    assert_eq!(got.http, 8000, "the rest of the Service resolves normally");
}

/// With the block present and no ports given, both resolve by conventional
/// name.
#[test]
fn a_kv_events_declaration_resolves_both_sockets_by_convention() {
    let sl = slice_with_ports(
        "u1",
        "svc",
        vec![],
        vec![
            port("http", 8000),
            port("kv-pub", 5557),
            port("kv-replay", 5558),
        ],
    );
    let got = extract_named_ports(
        &sl,
        &DiscoveryDefaults::default(),
        &PortRefs::default(),
        &mut HashSet::new(),
    );
    assert_eq!(got.zmq, Some(5557));
    assert_eq!(got.replay, Some(5558));
}

fn refs_declaring_http(name: &str) -> PortRefs {
    PortRefs {
        http: Some(PortRef::Name(name.to_string())),
        ..PortRefs::default()
    }
}

/// Collects WARN-and-above output emitted on this thread.
#[derive(Clone, Default)]
struct LogBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl LogBuf {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).expect("log output is UTF-8")
    }
}

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// `reconcile` never leaves the calling thread (it awaits only on
/// `tx.send`), so a thread-local subscriber captures all of it.
fn capture_warnings() -> (LogBuf, tracing::subscriber::DefaultGuard) {
    let buf = LogBuf::default();
    let guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .without_time()
            .finish(),
    );
    (buf, guard)
}

/// An empty placeholder slice (e.g. after scaling to zero) has no `ports[]`,
/// and must not warn about declared port names.
#[tokio::test]
async fn an_endpoint_less_slice_warns_about_nothing() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    let (tx, mut rx) = mpsc::channel(16);
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();
    let defaults = default_defaults();
    let refs = refs_declaring_http("http");

    // Both shapes K8s leaves behind: no endpoints at all, and endpoints that
    // have all gone NotReady. Neither has a resolvable `ports[]`.
    writer.apply_watcher_event(&watcher::Event::Apply(slice_with_ports(
        "slice-empty",
        "svc-abc",
        vec![],
        vec![],
    )));
    writer.apply_watcher_event(&watcher::Event::Apply(slice_with_ports(
        "slice-cold",
        "svc-def",
        vec![not_ready_endpoint("pod-1", "pod-1", "10.0.0.1")],
        vec![],
    )));

    let logs = {
        let (buf, _guard) = capture_warnings();
        reconcile(
            &store,
            &defaults,
            &refs,
            &tx,
            Some("default/test-set"),
            &mut announced,
            &mut HashSet::new(),
        )
        .await;
        buf.contents()
    };

    assert!(
        drain(&mut rx).is_empty(),
        "an empty slice yields no workers"
    );
    assert!(
        logs.is_empty(),
        "an endpoint-less slice must produce no warnings; got: {logs}"
    );
}

/// A slice with a Ready endpoint that lacks the declared port name still
/// warns.
#[tokio::test]
async fn a_declared_port_missing_from_a_live_slice_still_warns() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    let (tx, mut rx) = mpsc::channel(16);
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();
    let defaults = default_defaults();
    let refs = refs_declaring_http("openai");

    writer.apply_watcher_event(&watcher::Event::Apply(slice_with_ports(
        "slice-live",
        "svc-abc",
        vec![ready_endpoint("pod-1", "pod-1", "10.0.0.1")],
        vec![EndpointPort {
            name: Some("http".to_string()),
            port: Some(8000),
            ..Default::default()
        }],
    )));

    let logs = {
        let (buf, _guard) = capture_warnings();
        reconcile(
            &store,
            &defaults,
            &refs,
            &tx,
            Some("default/test-set"),
            &mut announced,
            &mut HashSet::new(),
        )
        .await;
        buf.contents()
    };

    assert!(
        logs.contains("declared port name is not on this EndpointSlice"),
        "a genuinely absent port name must still warn; got: {logs}"
    );
    // Still routable: the fallback found the conventional `http` name.
    assert_eq!(drain(&mut rx).len(), 1, "the worker is still announced");
}

/// A standing port miss warns on the first reconcile only.
#[tokio::test]
async fn a_standing_port_miss_warns_once() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    let (tx, _rx) = mpsc::channel(16);
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();
    let mut warned: HashSet<String> = HashSet::new();
    let defaults = default_defaults();
    let refs = refs_declaring_http("openai");

    writer.apply_watcher_event(&watcher::Event::Apply(slice_with_ports(
        "slice-live",
        "svc-abc",
        vec![ready_endpoint("pod-1", "pod-1", "10.0.0.1")],
        vec![EndpointPort {
            name: Some("http".to_string()),
            port: Some(8000),
            ..Default::default()
        }],
    )));

    let (buf, _guard) = capture_warnings();
    for _ in 0..2 {
        reconcile(
            &store,
            &defaults,
            &refs,
            &tx,
            Some("default/test-set"),
            &mut announced,
            &mut warned,
        )
        .await;
    }
    let logs = buf.contents();

    assert_eq!(logs.lines().count(), 1, "got: {logs}");
}

#[tokio::test]
async fn ready_endpoint_yields_added() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    writer.apply_watcher_event(&watcher::Event::Apply(slice(
        "slice-uid-1",
        "svc-abc",
        vec![ready_endpoint("pod-a", "pod-a", "10.0.0.1")],
    )));

    let (tx, mut rx) = mpsc::channel(16);
    let defaults = default_defaults();
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();

    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    let events = drain(&mut rx);

    assert_eq!(events.len(), 1);
    match &events[0] {
        WorkerChange::Added(cfg) => {
            assert_eq!(cfg.pod_ip.as_deref(), Some("10.0.0.1"));
            assert!(cfg.http_url.as_deref().unwrap().contains("10.0.0.1:8000"));
        }
        other => panic!("expected Added, got {other:?}"),
    }
    assert_eq!(announced.len(), 1);
    assert!(announced.contains_key("pod-a"));
}

#[tokio::test]
async fn endpoint_going_not_ready_yields_removed() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    writer.apply_watcher_event(&watcher::Event::Apply(slice(
        "slice-uid-1",
        "svc-abc",
        vec![ready_endpoint("pod-a", "pod-a", "10.0.0.1")],
    )));

    let (tx, mut rx) = mpsc::channel(16);
    let defaults = default_defaults();
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();

    // First reconcile: emit Added.
    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    drain(&mut rx);
    assert_eq!(announced.len(), 1);

    // Endpoint flips to not-ready.
    writer.apply_watcher_event(&watcher::Event::Apply(slice(
        "slice-uid-1",
        "svc-abc",
        vec![not_ready_endpoint("pod-a", "pod-a", "10.0.0.1")],
    )));

    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], WorkerChange::Removed(_)));
    assert!(announced.is_empty());
}

#[tokio::test]
async fn deleted_slice_yields_removed() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    let s = slice(
        "slice-uid-1",
        "svc-abc",
        vec![ready_endpoint("pod-a", "pod-a", "10.0.0.1")],
    );
    writer.apply_watcher_event(&watcher::Event::Apply(s.clone()));

    let (tx, mut rx) = mpsc::channel(16);
    let defaults = default_defaults();
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();

    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    drain(&mut rx);
    assert_eq!(announced.len(), 1);

    writer.apply_watcher_event(&watcher::Event::Delete(s));
    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], WorkerChange::Removed(_)));
    assert!(announced.is_empty());
}

/// An empty store retracts the whole pool, which is why the run loop doesn't
/// reconcile before `InitDone` after a rebuild. (This test calls `reconcile`
/// directly, so it documents the hazard rather than testing the gate.)
#[tokio::test]
async fn reconciling_against_an_unlisted_store_would_retract_the_pool() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    let s = slice(
        "slice-uid-1",
        "svc-abc",
        vec![
            ready_endpoint("pod-a", "pod-a", "10.0.0.1"),
            ready_endpoint("pod-b", "pod-b", "10.0.0.2"),
        ],
    );
    writer.apply_watcher_event(&watcher::Event::Apply(s.clone()));

    let (tx, mut rx) = mpsc::channel(16);
    let defaults = default_defaults();
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();
    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    assert_eq!(drain(&mut rx).len(), 2, "both workers announced");

    // Stand in for the post-rebuild store: fresh, hence empty, while
    // `announced` still holds both workers.
    let (empty, _writer2) = reflector::store::<EndpointSlice>();
    reconcile(
        &empty,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    let events = drain(&mut rx);
    assert_eq!(events.len(), 2, "an empty store retracts everything");
    assert!(events.iter().all(|e| matches!(e, WorkerChange::Removed(_))));
    assert!(announced.is_empty());
}

#[tokio::test]
async fn steady_state_is_no_op() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    writer.apply_watcher_event(&watcher::Event::Apply(slice(
        "slice-uid-1",
        "svc-abc",
        vec![ready_endpoint("pod-a", "pod-a", "10.0.0.1")],
    )));

    let (tx, mut rx) = mpsc::channel(16);
    let defaults = default_defaults();
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();

    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    drain(&mut rx);

    // No store change; a second reconcile should emit nothing.
    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    let events = drain(&mut rx);
    assert!(
        events.is_empty(),
        "expected no events; got {}",
        events.len()
    );
}

#[tokio::test]
async fn endpoint_moving_between_slices_is_steady_state() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    writer.apply_watcher_event(&watcher::Event::Apply(slice(
        "slice-uid-1",
        "svc-abc",
        vec![ready_endpoint("pod-a", "pod-a", "10.0.0.1")],
    )));

    let (tx, mut rx) = mpsc::channel(16);
    let defaults = default_defaults();
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();

    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    drain(&mut rx);
    assert_eq!(announced.len(), 1);

    // Same pod_uid now appears in a different slice (slice-1 empty,
    // slice-2 has it). This can happen when the EndpointSlice
    // controller repartitions slices.
    writer.apply_watcher_event(&watcher::Event::Apply(slice(
        "slice-uid-1",
        "svc-abc",
        vec![],
    )));
    writer.apply_watcher_event(&watcher::Event::Apply(slice(
        "slice-uid-2",
        "svc-abc-1",
        vec![ready_endpoint("pod-a", "pod-a", "10.0.0.1")],
    )));

    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    let events = drain(&mut rx);
    assert!(
        events.is_empty(),
        "expected steady state; got {} events",
        events.len()
    );
    assert_eq!(announced.len(), 1);
}

#[tokio::test]
async fn two_ready_endpoints_yield_two_added() {
    let (store, mut writer) = reflector::store::<EndpointSlice>();
    writer.apply_watcher_event(&watcher::Event::Apply(slice(
        "slice-uid-1",
        "svc-abc",
        vec![
            ready_endpoint("pod-a", "pod-a", "10.0.0.1"),
            ready_endpoint("pod-b", "pod-b", "10.0.0.2"),
        ],
    )));

    let (tx, mut rx) = mpsc::channel(16);
    let defaults = default_defaults();
    let mut announced: HashMap<String, (u64, String)> = HashMap::new();

    reconcile(
        &store,
        &defaults,
        &PortRefs::default(),
        &tx,
        Some("default/test-set"),
        &mut announced,
        &mut HashSet::new(),
    )
    .await;
    let events = drain(&mut rx);
    assert_eq!(events.len(), 2);
    for ev in &events {
        assert!(matches!(ev, WorkerChange::Added(_)));
    }
    assert_eq!(announced.len(), 2);
}

/// `run` over a recorded API server: only the named Service's slices count,
/// and each Ready endpoint is announced once.
#[tokio::test]
async fn run_announces_each_ready_endpoint_of_the_named_service() {
    use crate::api::recorded::Recorded;
    use std::time::Duration;

    let of_service = |mut s: EndpointSlice, svc: &str| {
        s.metadata.labels = Some([(SVC_NAME_LABEL.to_string(), svc.to_string())].into());
        s
    };
    let api = Recorded::default().with([
        of_service(
            slice(
                "s1",
                "vllm-abc",
                vec![
                    ready_endpoint("u1", "p1", "10.0.0.1"),
                    not_ready_endpoint("u2", "p2", "10.0.0.2"),
                ],
            ),
            "vllm",
        ),
        of_service(
            slice(
                "s2",
                "other-abc",
                vec![ready_endpoint("u3", "p3", "10.0.0.3")],
            ),
            "other",
        ),
    ]);
    let cfg = DiscoveryConfig {
        namespace: Some("default".into()),
        service_name: Some("vllm".into()),
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(KubeDiscovery::new(cfg, api).run(tx, cancel.clone()));

    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the listed endpoint is announced")
        .expect("the task holds the sender");
    match first {
        WorkerChange::Added(w) => assert_eq!(w.pod_ip.as_deref(), Some("10.0.0.1")),
        other => panic!("expected Added, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "the not-ready endpoint and the other Service's are not announced"
    );
    cancel.cancel();
    task.await.expect("discovery exits on cancel");
}
