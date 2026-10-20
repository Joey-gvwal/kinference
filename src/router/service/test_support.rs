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

/// One worker set: a live pool with one worker per address.
struct PoolFixture {
    name: String,
    model: String,
    addrs: Vec<String>,
}

enum Policy {
    /// A listed snapshot with no pools, so a named pool is unknown.
    ListedEmpty,
    /// `InferencePool default/pool` over every fixture, under this spec.
    Spec(serde_json::Value),
}

pub(crate) struct ServiceBuilder {
    pools: Vec<PoolFixture>,
    providers: Vec<(String, serde_json::Value)>,
    policy: Policy,
    /// What the fake `/render` answers for every request.
    tokens: Vec<u32>,
}

impl ServiceBuilder {
    pub(crate) fn new() -> Self {
        Self {
            pools: Vec::new(),
            providers: Vec::new(),
            policy: Policy::ListedEmpty,
            // Two full blocks at the test pools' block size of 16.
            tokens: (1..=32).collect(),
        }
    }

    /// One worker set serving `model`, with one ready worker per address.
    pub(crate) fn pool(mut self, name: &str, model: &str, addrs: &[&str]) -> Self {
        self.pools.push(PoolFixture {
            name: name.into(),
            model: model.into(),
            addrs: addrs.iter().map(|a| a.to_string()).collect(),
        });
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
        let render = FakeRender::start(self.tokens).await;
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
                    .register(id, worker(&f.model, addr, Some(render.base.clone())));
            }
            models.associate(p.id, &f.name, std::slice::from_ref(&f.model));
            bound.insert(spec.key.clone(), p.id);
            worker_sets.push(cr::<InferenceWorkerSet>(serde_json::json!({
                "apiVersion": "kinference.buoyant.io/v1alpha1", "kind": "InferenceWorkerSet",
                "metadata": {"name": f.name, "namespace": "default"},
                "spec": {"serviceRef": {"name": f.name}}
            })));
            dests.push(serde_json::json!({"kind": "InferenceWorkerSet", "name": f.name}));
            pools.push(p);
        }
        pool_registry.publish(Arc::new(PoolMap::from_pools(pools.clone())));

        let store = PolicyStore::new();
        match self.policy {
            Policy::ListedEmpty => store.publish(Arc::new(PolicyIndex::listed_empty())),
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
            }
        }

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
    store: Arc<PolicyStore>,
    registry: prometheus::Registry,
    render: FakeRender,
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

    pub(crate) fn store(&self) -> &Arc<PolicyStore> {
        &self.store
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
        self.render.calls.load(Ordering::SeqCst)
    }
}

/// A ready worker serving `model` at `addr`, tokenizing at `http_url`.
fn worker(model: &str, addr: &str, http_url: Option<String>) -> WorkerEntry {
    WorkerEntry {
        worker_id_str: addr.into(),
        model_name_hint: Some(model.into()),
        current_models: vec![model.into()],
        http_url,
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

/// A `/render` on a loopback port that answers every request with the same
/// token ids. Stops when dropped.
struct FakeRender {
    base: String,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeRender {
    async fn start(tokens: Vec<u32>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        let calls = Arc::new(AtomicUsize::new(0));
        let hits = calls.clone();
        // Any path: a worker is POSTed `{http_url}/v1/chat/completions/render`.
        let app = axum::Router::new().fallback(move || {
            hits.fetch_add(1, Ordering::SeqCst);
            let tokens = tokens.clone();
            async move { axum::Json(serde_json::json!({ "token_ids": tokens })) }
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
