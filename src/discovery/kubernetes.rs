//! `KubeDiscovery` — watch the EndpointSlices of a named K8s Service and
//! emit one worker per Ready endpoint.
//!
//! Operates in-cluster only: `kube::Config::incluster_env()` reads the
//! auto-mounted service-account token. We deliberately refuse to fall
//! back to `~/.kube/config` — even if auth succeeded that way, pod IPs
//! returned by EndpointSlice would not be reachable from outside the
//! cluster's pod network.
//!
//! The Service does NOT need to be headless. K8s stamps every EndpointSlice
//! it creates for a selector-based Service — ClusterIP, headless,
//! LoadBalancer, all of them — with `kubernetes.io/service-name=<svc>` and
//! lists the backing pods' individual IPs. We read those per-pod IPs and
//! connect to each pod DIRECTLY (HTTP `/v1/models` + `/render` and the ZMQ
//! event ports); we never route through the Service VIP, so its type and
//! kube-proxy's round-robin are irrelevant to prefix-aware routing. (The
//! VIP never appears in an EndpointSlice.)
//!
//! Per-endpoint port resolution order:
//!   1. Named port on the EndpointSlice (`http`, `metrics`, `kv-pub` or
//!      `kv-events`, `kv-replay`) — inherited from the Service's
//!      `spec.ports[].name`.
//!   2. `discovery.defaults` from config (last resort).
//!
//! Worker identity = `fnv1a_u64(endpoint.target_ref.uid)`. Same UID
//! semantics as the original Pod-mode discovery: a restarted pod gets
//! a fresh UID → a fresh `worker_id_num` → a fresh prefix-cache view in
//! the dynamo router. Correct semantics.
//!
//! ## Reconciliation model
//!
//! We drive off `kube::runtime::reflector`, which maintains a deduped
//! in-memory `Store<EndpointSlice>` that stays in sync with the API
//! server (including correctly pruning objects that vanished during a
//! disconnect at relist time). Each event on the reflector stream is
//! just a "state may have changed" wakeup: we re-derive the current
//! set of Ready endpoints from the store and diff it against
//! `announced` (what we've already emitted `Added` for). No per-event
//! Apply/Delete dispatch, no `Init*` state machine — the store owns
//! that complexity.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::TryStreamExt;
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointSlice};
use kube::Api;
use kube::runtime::WatchStreamExt;
use kube::runtime::reflector;
use kube::runtime::watcher::{self, watcher as start_watcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::{DiscoveryConfig, DiscoveryDefaults, PortRef, PortRefs, WorkerConfig};
use crate::discovery::{Discovery, WorkerChange};

/// The K8s standard label that the EndpointSlice controller stamps on
/// every slice it generates for a Service. Documented at
/// https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/.
const SVC_NAME_LABEL: &str = "kubernetes.io/service-name";

/// FNV-1a 64-bit hash. Same algorithm as `src/mock.rs::fnv1a`.
fn fnv1a_u64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub struct KubeDiscovery {
    cfg: DiscoveryConfig,
    client: kube::Client,
}

impl KubeDiscovery {
    /// Build in-cluster credentials once, so pool creation can be synchronous
    /// and infallible.
    ///
    /// `new` does two things: acquire credentials (fallible, and a fatal
    /// startup condition) and bind a config (trivial). Doing both per pool made
    /// creation `async` and fallible, which is exactly what a reconcile loop
    /// cannot tolerate — a transient apiserver hiccup would turn "add a worker
    /// set" into a failure with nothing to retry it. Acquire once here, at
    /// startup, where failing hard is the right answer.
    pub async fn client() -> Result<kube::Client> {
        let kube_cfg = kube::Config::incluster_env().context(
            "failed to read in-cluster Kubernetes service-account credentials. \
             KubeDiscovery only works when the monitor runs as a pod inside the \
             cluster it should watch (~/.kube/config is intentionally NOT honored — \
             discovered pod IPs would not be reachable from outside the cluster \
             network). If you're running locally, use discovery.mode = \"static\" \
             with [[workers]].",
        )?;
        kube::Client::try_from(kube_cfg)
            .context("failed to build Kubernetes client from in-cluster config")
    }

    /// Bind a config to an already-acquired client. Synchronous and infallible:
    /// `service_name` is validated at config load and, in Kubernetes mode, by
    /// the desired-set resolution.
    pub fn from_client(cfg: DiscoveryConfig, client: kube::Client) -> Self {
        debug_assert!(
            !cfg.service_name.as_deref().unwrap_or("").is_empty(),
            "service_name is validated before a pool spec reaches here"
        );
        Self { cfg, client }
    }
}

#[async_trait]
impl Discovery for KubeDiscovery {
    async fn run(self: Arc<Self>, tx: mpsc::Sender<WorkerChange>, cancel: CancellationToken) {
        let slices: Api<EndpointSlice> = match self.cfg.namespace.as_deref() {
            Some(ns) => Api::namespaced(self.client.clone(), ns),
            None => Api::all(self.client.clone()),
        };
        // SAFETY: KubeDiscovery::new validated this.
        let service_name = self.cfg.service_name.clone().unwrap_or_default();
        // Watch only the EndpointSlices belonging to the named Service.
        // K8s's EndpointSlice controller stamps every slice it creates with
        // this label, so this is the canonical selector.
        let wcfg = watcher::Config::default().labels(&format!("{SVC_NAME_LABEL}={service_name}"));

        tracing::info!(
            namespace = ?self.cfg.namespace,
            service_name = %service_name,
            "starting EndpointSlice watcher"
        );

        // `announced` tracks what we've already told the rest of the
        // system about. The reflector's `Store` is the source of truth
        // for what CURRENTLY exists in K8s; on each stream wakeup we
        // diff the two and emit deltas.
        //
        // Hoisted ABOVE the rebuild loop deliberately: it must survive a
        // rebuild, or the new generation would re-`Added` every worker under an
        // id the registry already holds, which `handle_added` treats as a
        // duplicate and rolls back.
        let mut announced: HashMap<String, (u64, String)> = HashMap::new();

        // Rebuild the watcher rather than exiting when its stream ends. This
        // used to `return`, which left the pool permanently frozen: no new
        // worker ever discovered, no departure ever noticed, and no error after
        // the one warning. Unlike the policy watcher there is no fallback here,
        // so the pool simply stops tracking reality.
        const REBUILD_DELAY: std::time::Duration = std::time::Duration::from_secs(2);
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let (store, writer) = reflector::store::<EndpointSlice>();
            // `.default_backoff()` chains a `StreamBackoff` so transient API
            // errors retry with exponential backoff. Wrap it with `reflector`
            // so every event also updates `store` (in-band, before the event
            // yields to us — so `store.state()` always reflects the current
            // event when we handle it).
            let stream = reflector::reflector(writer, start_watcher(slices.clone(), wcfg.clone()))
                .default_backoff();
            tokio::pin!(stream);

            // This generation's store starts EMPTY, so reconciling before the
            // initial list completes would diff a populated `announced` against
            // nothing and retract the entire pool, only to re-add it moments
            // later. `InitDone` is the first point `store.state()` is whole.
            let mut listed = false;

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!("cancellation received, exiting");
                        return;
                    }
                    next = stream.try_next() => {
                        match next {
                            Ok(Some(watcher::Event::InitDone)) => {
                                listed = true;
                                reconcile(
                                    &store,
                                    &self.cfg.defaults,
                                    &self.cfg.ports,
                                    &tx,
                                    self.cfg.workerset.as_deref(),
                                    &mut announced,
                                )
                                .await;
                            }
                            Ok(Some(_)) if listed => {
                                reconcile(
                                    &store,
                                    &self.cfg.defaults,
                                    &self.cfg.ports,
                                    &tx,
                                    self.cfg.workerset.as_deref(),
                                    &mut announced,
                                )
                                .await;
                            }
                            // Still listing: the store is partial, so there is
                            // nothing safe to diff against yet.
                            Ok(Some(_)) => {}
                            Ok(None) => {
                                tracing::warn!(
                                    "watcher stream ended; rebuilding in {:?}",
                                    REBUILD_DELAY
                                );
                                break;
                            }
                            Err(err) => {
                                tracing::warn!(error = %err, "watcher error; continuing");
                            }
                        }
                    }
                }
            }

            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(REBUILD_DELAY) => {}
            }
        }
    }
}

/// Diff the reflector's cached slices against `announced` and emit
/// `Added` / `Removed` for every pod that just transitioned in or out
/// of the Ready-across-any-slice set.
///
/// Idempotent: safe to call after every event (including no-op events).
/// Store lookups are cheap read-locks — a full re-derivation costs
/// O(slices × endpoints_per_slice) which is bounded and small.
async fn reconcile(
    store: &reflector::Store<EndpointSlice>,
    defaults: &DiscoveryDefaults,
    ports: &PortRefs,
    tx: &mpsc::Sender<WorkerChange>,
    // `namespace/name` of the InferenceWorkerSet these endpoints belong to, so a
    // churn line says WHICH declared fleet moved. Two sets can front the same
    // Service, so the Service name alone does not answer that. `None` in static
    // mode, where there is no CR.
    workerset: Option<&str>,
    // pod_uid -> (worker id, k8s coordinate). The coordinate is stored rather
    // than re-derived because the Removed path fires exactly when the endpoint
    // has left the store, leaving nothing to derive it from.
    announced: &mut HashMap<String, (u64, String)>,
) {
    let ws = workerset.unwrap_or("-");
    // Snapshot: pod_uid → WorkerConfig for every Ready endpoint the
    // store currently knows about. A pod that's Ready in multiple
    // slices dedupes to one entry (last write wins; the WorkerConfig
    // is identical modulo slice_name, which is only used in
    // debug logging).
    let mut current: HashMap<String, WorkerConfig> = HashMap::new();
    for slice in store.state() {
        // A slice with no Ready endpoint contributes nothing, and resolving its
        // ports first is not merely wasted work — it WARNs. K8s keeps an empty
        // placeholder slice after the last pod goes but empties `ports[]`, since
        // a port NAME has no container port left to resolve against. Every
        // declared name then "misses" and reports a typo in a CR that was
        // correct all along. Scaling a GPU node pool to zero produced exactly
        // that: one bogus WARN per declared port, on every reconcile.
        if !slice.endpoints.iter().flatten().any(is_endpoint_ready) {
            continue;
        }
        let ports = extract_named_ports(&slice, defaults, ports);
        let slice_name = slice.metadata.name.as_deref().unwrap_or("");
        // `endpoints` became `Option<Vec<_>>` in the k8s 1.36 API, so flatten
        // rather than iterating the Option itself. A slice with the field
        // absent is simply an empty slice.
        for ep in slice.endpoints.iter().flatten() {
            if !is_endpoint_ready(ep) {
                continue;
            }
            let Some(pod_uid) = ep.target_ref.as_ref().and_then(|t| t.uid.clone()) else {
                continue;
            };
            match synthesize_worker_from_endpoint(ep, &ports, slice_name, defaults) {
                Some(cfg) => {
                    current.insert(pod_uid, cfg);
                }
                None => {
                    tracing::warn!(
                        workerset = ws,
                        pod = ep
                            .target_ref
                            .as_ref()
                            .and_then(|t| t.name.as_deref())
                            .unwrap_or("-"),
                        slice = slice_name,
                        "endpoint Ready but missing required fields \
                         (address or targetRef); skipping. Will retry on next update."
                    );
                }
            }
        }
    }

    // Newly Ready → emit Added.
    for (pod_uid, cfg) in current.iter() {
        if announced.contains_key(pod_uid) {
            continue;
        }
        let worker_id_num = cfg.worker_id_num;
        tracing::info!(
            workerset = ws,
            worker_id = cfg.worker_id_str,
            zmq_endpoint = ?cfg.zmq_endpoint,
            http_url = ?cfg.http_url,
            "endpoint became Ready; emitting Added"
        );
        announced.insert(pod_uid.clone(), (worker_id_num, cfg.worker_id_str.clone()));
        let _ = tx.send(WorkerChange::Added(cfg.clone())).await;
    }

    // No longer Ready anywhere → emit Removed. Covers three cases the
    // hand-rolled reconciler handled separately: endpoint dropped from
    // a slice, slice deleted, and endpoint vanished during a
    // reflector-driven relist.
    let vanished: Vec<String> = announced
        .keys()
        .filter(|u| !current.contains_key(*u))
        .cloned()
        .collect();
    for pod_uid in vanished {
        if let Some((num, worker_id)) = announced.remove(&pod_uid) {
            tracing::info!(
                workerset = ws,
                worker_id,
                "endpoint no longer Ready in any slice; emitting Removed"
            );
            let _ = tx.send(WorkerChange::Removed(num)).await;
        }
    }
}

/// Ready iff `endpoint.conditions.ready == Some(true)`. K8s's
/// EndpointSlice controller only sets this when the backing Pod passes
/// its readiness probe AND isn't terminating — equivalent to the
/// is_pod_ready check we used to do ourselves, just without inspecting
/// the Pod.
fn is_endpoint_ready(ep: &Endpoint) -> bool {
    ep.conditions
        .as_ref()
        .and_then(|c| c.ready)
        .unwrap_or(false)
}

/// Slice-level port resolution. Each EndpointSlice carries a single
/// `ports[]` (inherited from the Service) that applies to all its
/// endpoints. We look up `http`, `metrics`, `kv-pub`/`kv-events`, `kv-replay`
/// by name and fall back to `discovery.defaults` for any that are absent.
struct ResolvedPorts {
    http: u16,
    metrics: u16,
    /// `None` when the declaration says these engines publish no KV events.
    /// The worker is still routable; it just contributes no prefix signal.
    zmq: Option<u16>,
    replay: Option<u16>,
}

/// Resolve one explicitly-declared port against the slice.
///
/// A NAME is looked up in the slice's port list; a miss returns `None` so the
/// caller falls back to its conventional-name chain, with a WARN, because a
/// name that resolves to nothing is a typo or a Service that changed under the
/// CR — silently dialing a default would serve traffic against the wrong port.
/// `reconcile` skips endpoint-less slices before calling this, so the WARN
/// cannot fire for a slice that is merely empty.
///
/// A NUMBER is matched against the slice's ports first: the CR names a SERVICE
/// port, the slice carries TARGET ports, and they coincide in the ordinary case
/// but need not. No match means using it verbatim, with a WARN naming the
/// hazard, because refusing outright would be worse — the number is usually
/// right.
fn resolve_port_ref(slice: &EndpointSlice, role: &str, want: &PortRef) -> Option<u16> {
    let ports = slice.ports.as_deref().unwrap_or(&[]);
    match want {
        PortRef::Name(name) => {
            let hit = ports.iter().find_map(|p| {
                (p.name.as_deref() == Some(name.as_str()))
                    .then(|| p.port.and_then(|v| u16::try_from(v).ok()))
                    .flatten()
            });
            if hit.is_none() {
                tracing::warn!(
                    role,
                    port_name = %name,
                    slice = slice.metadata.name.as_deref().unwrap_or("-"),
                    "declared port name is not on this EndpointSlice; \
                     falling back to the conventional name"
                );
            }
            hit
        }
        PortRef::Number(n) => {
            let matched = ports
                .iter()
                .any(|p| p.port.and_then(|v| u16::try_from(v).ok()) == Some(*n));
            if !matched {
                tracing::warn!(
                    role,
                    port = n,
                    slice = slice.metadata.name.as_deref().unwrap_or("-"),
                    "declared numeric port matches no EndpointSlice port; dialing it \
                     verbatim. A number in a CR is a SERVICE port and endpoints carry \
                     TARGET ports — prefer a port NAME, which is always unambiguous"
                );
            }
            Some(*n)
        }
    }
}

fn extract_named_ports(
    slice: &EndpointSlice,
    defaults: &DiscoveryDefaults,
    refs: &PortRefs,
) -> ResolvedPorts {
    let find = |name: &str| -> Option<u16> {
        slice.ports.as_ref()?.iter().find_map(|p| {
            if p.name.as_deref() == Some(name) {
                p.port.and_then(|v| u16::try_from(v).ok())
            } else {
                None
            }
        })
    };
    let declared = |role: &str, want: &Option<PortRef>| -> Option<u16> {
        want.as_ref().and_then(|w| resolve_port_ref(slice, role, w))
    };
    let http = declared("http", &refs.http)
        .or_else(|| find("http"))
        .unwrap_or(defaults.http_port);
    let metrics = declared("metrics", &refs.metrics)
        .or_else(|| find("metrics"))
        .unwrap_or(http);
    // `kv-events` is the name the InferenceWorkerSet CRD's examples use for the
    // publish socket; `kv-pub` is what kinference's own configs have always used.
    // Accept both, because the failure mode of accepting only one is silent: the
    // lookup misses, the ZMQ default is used, the SUB socket connects to
    // nothing (ZMQ connect always succeeds), and the pool serves traffic with a
    // permanently empty prefix model.
    let zmq = if refs.kv_events_declared_absent {
        None
    } else {
        Some(
            declared("kv-events", &refs.zmq)
                .or_else(|| find("kv-pub"))
                .or_else(|| find("kv-events"))
                .unwrap_or(defaults.zmq_port),
        )
    };
    // Gated on the same declaration as `zmq`: replay reads back the stream the
    // publish socket emits, so a set that publishes nothing has nothing to
    // replay. Before `replayPort` moved under `kvEvents` this resolved
    // unconditionally, and the value was carried on every endpoint of an
    // events-free set and never read.
    let replay = if refs.kv_events_declared_absent {
        None
    } else {
        declared("kv-replay", &refs.replay)
            .or_else(|| find("kv-replay"))
            .or(defaults.replay_port)
    };
    ResolvedPorts {
        http,
        metrics,
        zmq,
        replay,
    }
}

/// Build a `WorkerConfig` from one endpoint. Returns `None` if we can't
/// extract the minimum (address + UID).
fn synthesize_worker_from_endpoint(
    ep: &Endpoint,
    ports: &ResolvedPorts,
    slice_name: &str,
    _defaults: &DiscoveryDefaults,
) -> Option<WorkerConfig> {
    let target = ep.target_ref.as_ref()?;
    let pod_uid = target.uid.as_deref()?;
    let pod_name = target.name.clone()?;
    let namespace = target.namespace.clone().unwrap_or_default();
    let address = ep.addresses.first().cloned()?;

    let worker_id_str = if namespace.is_empty() {
        pod_name.clone()
    } else {
        format!("{namespace}/{pod_name}")
    };
    let worker_id_num = fnv1a_u64(pod_uid.as_bytes());

    let http_url = Some(format!("http://{address}:{}/", ports.http));
    let metrics_url = Some(format!("http://{address}:{}/metrics", ports.metrics));
    let zmq_endpoint = ports.zmq.map(|p| format!("tcp://{address}:{p}"));
    let replay_endpoint = ports.replay.map(|p| format!("tcp://{address}:{p}"));

    let node_name = ep.node_name.clone();
    let availability_zone = ep.zone.clone();

    tracing::debug!(
        slice = slice_name,
        worker_id = worker_id_str,
        http_port = ports.http,
        metrics_port = ports.metrics,
        zmq_port = ports.zmq,
        replay_port = ?ports.replay,
        "synthesized WorkerConfig from endpoint"
    );

    Some(WorkerConfig {
        worker_id_str,
        worker_id_num,
        // Empty hint — the warmup-fired /v1/models scrape populates the
        // reactive by_model index sub-second after registration.
        model_name: String::new(),
        zmq_endpoint,
        replay_endpoint,
        metrics_url,
        http_url,
        // K8s mode derives the routable address from `http_url`
        // (which is already `http://<pod-ip>:<port>/`), so no need
        // for an explicit override.
        endpoint: None,
        pod_name: Some(pod_name),
        namespace: Some(namespace),
        node_name,
        node_ip: None, // not exposed by EndpointSlice; we don't currently route on this anyway
        pod_ip: Some(address),
        availability_zone,
        instance_type: None, // would require Node fetch + extra RBAC; out of scope
    })
}

#[cfg(test)]
mod tests;
