//! A `KinferenceService` for tests, with every dependency defaulted.
//!
//! Workers tokenize against a fake `/render` on a loopback port, so a pick
//! runs the same path as in production without a network.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::*;
use crate::controller::policy::store::PolicyStore;
use kinference_fleet::pool::{PoolMap, PoolRegistry, test_spec};
use kinference_fleet::workers::WorkerEntry;
use kinference_policy::crd::{
    InferencePolicy, InferencePool, InferenceProvider, InferenceWorkerSet,
};
use kinference_policy::resolve::PolicyIndex;
use kinference_policy::{PoolId, PoolKey, PoolRef, RequestIdentity};

/// What the fake `/render` answers.
#[derive(Clone)]
pub(crate) enum Render {
    /// Every request tokenizes to these ids.
    Tokens(Vec<u32>),
    /// Every request gets a 500.
    Failing,
    /// No worker has an `http_url`, so nothing is tried.
    Absent,
}

/// One worker set: a live pool with one worker per address.
struct PoolFixture {
    name: String,
    model: String,
    addrs: Vec<String>,
    /// `initial_sync_complete` on every worker.
    ready: bool,
    /// `(upstream, aliases)` rows for the worker set's `spec.models`.
    aliases: Vec<(String, Vec<String>)>,
}

enum Policy {
    /// `policy: None`, as with policy disabled.
    Off,
    /// A listed snapshot with no pools, so a named pool is unknown.
    ListedEmpty,
    /// `InferencePool default/pool` over every fixture, under this spec.
    Spec(serde_json::Value),
}

pub(crate) struct ServiceBuilder {
    pools: Vec<PoolFixture>,
    providers: Vec<(String, serde_json::Value)>,
    policy: Policy,
    render: Render,
}

impl ServiceBuilder {
    pub(crate) fn new() -> Self {
        Self {
            pools: Vec::new(),
            providers: Vec::new(),
            policy: Policy::ListedEmpty,
            // Two full blocks at the test pools' block size of 16.
            render: Render::Tokens((1..=32).collect()),
        }
    }

    /// One worker set serving `model`, with one ready worker per address.
    pub(crate) fn pool(mut self, name: &str, model: &str, addrs: &[&str]) -> Self {
        self.pools.push(PoolFixture {
            name: name.into(),
            model: model.into(),
            addrs: addrs.iter().map(|a| a.to_string()).collect(),
            ready: true,
            aliases: Vec::new(),
        });
        self
    }

    /// The last pool's workers have not finished their initial sync.
    pub(crate) fn still_syncing(mut self) -> Self {
        self.pools.last_mut().expect("a pool to mark").ready = false;
        self
    }

    /// `models: [{name: upstream, aliases}]` on worker set `set`.
    pub(crate) fn alias(mut self, set: &str, upstream: &str, aliases: &[&str]) -> Self {
        let f = self
            .pools
            .iter_mut()
            .find(|f| f.name == set)
            .expect("an alias names a pool the builder has");
        f.aliases.push((
            upstream.into(),
            aliases.iter().map(|a| a.to_string()).collect(),
        ));
        self
    }

    pub(crate) fn render(mut self, render: Render) -> Self {
        self.render = render;
        self
    }

    pub(crate) fn without_policy(mut self) -> Self {
        self.policy = Policy::Off;
        self
    }

    /// An `InferenceProvider` among the pool's destinations; `models` is its
    /// `spec.models`.
    pub(crate) fn provider(mut self, name: &str, models: serde_json::Value) -> Self {
        self.providers.push((name.into(), models));
        self
    }

    /// The `InferencePolicy` spec; the snapshot then holds `default/pool`
    /// over every worker set and provider.
    pub(crate) fn policy(mut self, spec: serde_json::Value) -> Self {
        self.policy = Policy::Spec(spec);
        self
    }

    pub(crate) async fn build(self) -> TestService {
        let render = match self.render {
            Render::Absent => None,
            answer => Some(FakeRender::start(answer).await),
        };
        let http_url = render.as_ref().map(|r| r.base.clone());
        let registry = prometheus::Registry::new();
        let pool_registry = PoolRegistry::new();
        let models = Arc::new(ModelIndex::new());
        let (mut pools, mut worker_sets, mut dests) = (Vec::new(), Vec::new(), Vec::new());
        let mut bound = BTreeMap::new();
        // Numbered across pools, so the first worker of the first pool is 1.
        let mut id = 0;
        for f in &self.pools {
            // Keyed as a CRD worker set's pool is, so the policy snapshot can
            // bind to it.
            let mut spec = test_spec(&f.name, 16);
            spec.key = PoolKey::namespaced("default", &f.name);
            spec.name = spec.key.to_string();
            let p = Arc::new(Pool::from_spec(
                PoolId::next(),
                &spec,
                &CancellationToken::new(),
            ));
            for addr in &f.addrs {
                id += 1;
                p.registry
                    .register(id, worker(&f.model, addr, f.ready, http_url.clone()));
            }
            models.associate(p.id, &f.name, std::slice::from_ref(&f.model));
            bound.insert(spec.key.clone(), p.id);
            let aliases: Vec<serde_json::Value> = f
                .aliases
                .iter()
                .map(|(u, a)| serde_json::json!({"name": u, "aliases": a}))
                .collect();
            worker_sets.push(cr::<InferenceWorkerSet>(serde_json::json!({
                "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferenceWorkerSet",
                "metadata": {"name": f.name, "namespace": "default"},
                "spec": {"serviceRef": {"name": f.name}, "models": aliases}
            })));
            dests.push(serde_json::json!({"kind": "InferenceWorkerSet", "name": f.name}));
            pools.push(p);
        }
        pool_registry.publish(Arc::new(PoolMap::from_pools(pools.clone())));

        let store = match self.policy {
            Policy::Off => None,
            Policy::ListedEmpty => {
                let store = PolicyStore::new();
                store.publish(Arc::new(PolicyIndex::listed_empty()));
                Some(store)
            }
            Policy::Spec(spec) => {
                let mut provs = Vec::new();
                for (name, models) in &self.providers {
                    provs.push(cr::<InferenceProvider>(serde_json::json!({
                        "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferenceProvider",
                        "metadata": {"name": name, "namespace": "default"},
                        "spec": {"backendRef": {"name": format!("{name}-backend")}, "api": "OpenAI",
                                 "models": models}
                    })));
                    dests.push(serde_json::json!({"kind": "InferenceProvider", "name": name}));
                }
                let pool = cr::<InferencePool>(serde_json::json!({
                    "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferencePool",
                    "metadata": {"name": "pool", "namespace": "default"},
                    "spec": {"destinations": dests, "policyRef": {"name": "p"}}
                }));
                let policy = cr::<InferencePolicy>(serde_json::json!({
                    "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferencePolicy",
                    "metadata": {"name": "p", "namespace": "default"},
                    "spec": spec
                }));
                let store = PolicyStore::new();
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
                Some(store)
            }
        };

        let ttl = Duration::from_secs(30);
        let svc = KinferenceService::new(
            pool_registry,
            models,
            Arc::new(MetricsRegistry::new()),
            Arc::new(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(2))
                    .build()
                    .expect("a reqwest client"),
            ),
            Arc::new(
                crate::router::tokenizer::local::LocalTokenizerRegistry::new(Default::default())
                    .expect("a local tokenizer registry"),
            ),
            Arc::new(PendingObservations::new(ttl)),
            Arc::new(RequestLabels::new(ttl)),
            Arc::new(crate::router::body_store::BodyStore::new(
                1 << 20,
                1 << 20,
                ttl,
            )),
            store.clone(),
            Arc::new(kinference_policy::provider_health::ProviderHealthStore::new()),
            ttl,
            Arc::new(ObservationSink::open(None)),
            Arc::new(crate::router::metrics::RouterMetrics::new(
                &registry,
                &Default::default(),
            )),
        );
        TestService {
            svc,
            pools,
            store,
            registry,
            render,
        }
    }
}

/// A built service and the handles tests assert through.
pub(crate) struct TestService {
    pub svc: KinferenceService,
    /// In the order the builder was given them.
    pub pools: Vec<Arc<Pool>>,
    store: Option<Arc<PolicyStore>>,
    registry: prometheus::Registry,
    render: Option<FakeRender>,
}

impl TestService {
    /// `default/pool`, the `InferencePool` every policy fixture publishes.
    pub(crate) fn pool_ref() -> PoolRef {
        PoolRef {
            namespace: "default".into(),
            name: "pool".into(),
            resource_version: String::new(),
        }
    }

    /// The policy store; panics for a service built `without_policy`.
    pub(crate) fn store(&self) -> &Arc<PolicyStore> {
        self.store
            .as_ref()
            .expect("the service was built with policy")
    }

    /// `pick_and_record` for tenant `acme`, with no client request id.
    pub(crate) async fn pick(
        &self,
        body: &[u8],
        request_id: u64,
        qos: Qos,
        pool: Option<&PoolRef>,
    ) -> PickAndRecordOutcome {
        let identity = RequestIdentity {
            tenant: "acme".into(),
        };
        self.svc
            .pick_and_record(body, request_id, None, qos, pool, &identity)
            .await
    }

    /// The router's metrics in Prometheus text form.
    pub(crate) fn metrics(&self) -> String {
        use prometheus::Encoder as _;
        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("encode the registry");
        String::from_utf8(buf).expect("utf-8 exposition")
    }

    /// How many requests the fake `/render` has answered.
    pub(crate) fn render_calls(&self) -> usize {
        self.render
            .as_ref()
            .map_or(0, |r| r.calls.load(Ordering::SeqCst))
    }
}

/// A worker serving `model` at `addr`, tokenizing at `http_url`.
fn worker(model: &str, addr: &str, ready: bool, http_url: Option<String>) -> WorkerEntry {
    WorkerEntry {
        worker_id_str: addr.into(),
        model_name_hint: Some(model.into()),
        current_models: vec![model.into()],
        http_url,
        block_size: 16,
        initial_sync_complete: ready,
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

/// A `/render` on a loopback port that gives every request the same answer.
/// Stops when dropped.
struct FakeRender {
    base: String,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeRender {
    async fn start(answer: Render) -> Self {
        use axum::response::IntoResponse as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        let calls = Arc::new(AtomicUsize::new(0));
        let hits = calls.clone();
        // Any path: a worker is POSTed `{http_url}/v1/chat/completions/render`.
        let app = axum::Router::new().fallback(move || {
            hits.fetch_add(1, Ordering::SeqCst);
            let answer = answer.clone();
            async move {
                match answer {
                    Render::Tokens(ids) => {
                        axum::Json(serde_json::json!({ "token_ids": ids })).into_response()
                    }
                    Render::Failing => {
                        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "render down")
                            .into_response()
                    }
                    Render::Absent => unreachable!("no server is started for Absent"),
                }
            }
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve the fake /render");
        });
        Self { base, calls, task }
    }
}

impl Drop for FakeRender {
    fn drop(&mut self) {
        self.task.abort();
    }
}
