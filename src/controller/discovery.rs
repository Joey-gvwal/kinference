//! `KubeDiscovery`: watch the EndpointSlices of a named Kubernetes Service and
//! emit one worker per Ready endpoint.
//!
//! In-cluster only: `~/.kube/config` is not honored, since pod IPs from an
//! EndpointSlice aren't reachable from outside the cluster network.
//!
//! Any selector-based Service works, headless or not: its EndpointSlices are
//! labeled `kubernetes.io/service-name=<svc>` and list the pods' own IPs, which
//! kinference dials directly, never the Service VIP.
//!
//! Per-endpoint port resolution order:
//!   1. A port the `InferenceWorkerSet` declares, by name or number.
//!   2. The conventional port name on the EndpointSlice (`http`, `metrics`,
//!      `kv-pub` or `kv-events`, `kv-replay`).
//!   3. `discovery.defaults`.
//!
//! Worker identity is `fnv1a_u64(endpoint.target_ref.uid)`, so a restarted
//! pod gets a fresh `worker_id_num` and a fresh prefix-cache view.
//!
//! Each event on the reflector stream is a wakeup: the current set of Ready
//! endpoints is re-derived from the store and diffed against `announced` (what
//! has been emitted as `Added`).

use std::collections::HashMap;

use anyhow::{Context, Result};
use futures::TryStreamExt;
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointSlice};
use kube::Api;
use kube::runtime::WatchStreamExt;
use kube::runtime::reflector;
use kube::runtime::watcher::{self, watcher as start_watcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use kinference_fleet::discovery::{
    DiscoveryConfig, DiscoveryDefaults, PortRef, PortRefs, WorkerChange, WorkerConfig,
};

/// The K8s standard label that the EndpointSlice controller stamps on
/// every slice it generates for a Service. Documented at
/// https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/.
const SVC_NAME_LABEL: &str = "kubernetes.io/service-name";

/// FNV-1a 64-bit hash.
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
    /// Called once at startup, where failing hard is right; [`Self::from_client`]
    /// then binds each pool's config without I/O.
    pub async fn client() -> Result<kube::Client> {
        let kube_cfg = kube::Config::incluster_env().context(
            "failed to read in-cluster Kubernetes service-account credentials. \
             KubeDiscovery only works when the monitor runs as a pod inside the \
             cluster it should watch (~/.kube/config is intentionally NOT honored — \
             discovered pod IPs would not be reachable from outside the cluster \
             network).",
        )?;
        kube::Client::try_from(kube_cfg)
            .context("failed to build Kubernetes client from in-cluster config")
    }

    /// Bind a config to an already-acquired client. Synchronous and infallible:
    /// `service_name` is validated at config load or by the desired-set
    /// resolution.
    pub fn from_client(cfg: DiscoveryConfig, client: kube::Client) -> Self {
        debug_assert!(
            !cfg.service_name.as_deref().unwrap_or("").is_empty(),
            "service_name is validated before a pool spec reaches here"
        );
        Self { cfg, client }
    }

    /// Emit worker-membership events until `cancel` fires, under the contract
    /// of `kinference_fleet::discovery::DiscoveryTask`.
    pub async fn run(self, tx: mpsc::Sender<WorkerChange>, cancel: CancellationToken) {
        let slices: Api<EndpointSlice> = match self.cfg.namespace.as_deref() {
            Some(ns) => Api::namespaced(self.client.clone(), ns),
            None => Api::all(self.client.clone()),
        };
        // Validated before the pool spec reaches here; see `from_client`.
        let service_name = self.cfg.service_name.clone().unwrap_or_default();
        // Only the named Service's EndpointSlices.
        let wcfg = watcher::Config::default().labels(&format!("{SVC_NAME_LABEL}={service_name}"));

        tracing::info!(
            namespace = ?self.cfg.namespace,
            service_name = %service_name,
            "starting EndpointSlice watcher"
        );

        // What has been announced as `Added`. Outside the rebuild loop so it
        // survives a rebuild; otherwise every worker would be re-`Added`, which
        // `handle_added` treats as a duplicate and rolls back.
        let mut announced: HashMap<String, (u64, String)> = HashMap::new();

        // Rebuild the watcher when its stream ends, or the pool would stop
        // tracking membership.
        const REBUILD_DELAY: std::time::Duration = std::time::Duration::from_secs(2);
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let (store, writer) = reflector::store::<EndpointSlice>();
            // The reflector updates `store` before each event is yielded, so
            // `store.state()` reflects the event being handled.
            let stream = reflector::reflector(writer, start_watcher(slices.clone(), wcfg.clone()))
                .default_backoff();
            tokio::pin!(stream);

            // This generation's store starts empty, so reconciling before
            // `InitDone` would retract the whole pool and re-add it moments
            // later.
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
/// Idempotent, and cheap enough to run on every event.
async fn reconcile(
    store: &reflector::Store<EndpointSlice>,
    defaults: &DiscoveryDefaults,
    ports: &PortRefs,
    tx: &mpsc::Sender<WorkerChange>,
    // `namespace/name` of the InferenceWorkerSet, for logs, since two sets
    // can front one Service. `None` without a CR.
    workerset: Option<&str>,
    // pod_uid -> (worker id num, worker id string). Stored because a removed
    // endpoint is no longer in the store.
    announced: &mut HashMap<String, (u64, String)>,
) {
    let ws = workerset.unwrap_or("-");
    // pod_uid -> WorkerConfig for every Ready endpoint. A pod Ready in
    // several slices dedupes to one entry.
    let mut current: HashMap<String, WorkerConfig> = HashMap::new();
    for slice in store.state() {
        // Skip slices with no Ready endpoint before resolving ports. An empty
        // placeholder slice has no `ports[]`, so every declared port name
        // would warn as a miss.
        if !slice.endpoints.iter().flatten().any(is_endpoint_ready) {
            continue;
        }
        let ports = extract_named_ports(&slice, defaults, ports);
        let slice_name = slice.metadata.name.as_deref().unwrap_or("");
        // `endpoints` is optional; absent means empty.
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

    // No longer Ready anywhere: emit Removed. Covers an endpoint dropped from
    // a slice, a deleted slice, and an endpoint gone after a relist.
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

/// Ready iff `endpoint.conditions.ready == Some(true)`, which the
/// EndpointSlice controller sets only when the Pod passes its readiness probe
/// and isn't terminating.
fn is_endpoint_ready(ep: &Endpoint) -> bool {
    ep.conditions
        .as_ref()
        .and_then(|c| c.ready)
        .unwrap_or(false)
}

/// Ports resolved for a slice. Each EndpointSlice carries one `ports[]` for
/// all its endpoints. See the module docs for the resolution order.
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
/// A name is looked up in the slice's ports; a miss warns (likely a typo or a
/// changed Service) and returns `None`, so the caller falls back to the
/// conventional name.
///
/// A number is a Service port while the slice carries target ports; they
/// usually coincide. With no match it is used verbatim, with a warning.
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
    // Accept both `kv-pub` and `kv-events` (the CRD examples' name). A miss
    // would silently fall back to the default port, and since ZMQ connect
    // always succeeds, the prefix model would stay empty.
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
    // A set that publishes nothing has nothing to replay.
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
        // Filled in by the warmup /v1/models scrape after registration.
        model_name: String::new(),
        zmq_endpoint,
        replay_endpoint,
        metrics_url,
        http_url,
        // Derived from `http_url`, which already uses the pod IP.
        endpoint: None,
        pod_name: Some(pod_name),
        namespace: Some(namespace),
        node_name,
        node_ip: None, // not exposed by EndpointSlice
        pod_ip: Some(address),
        availability_zone,
        instance_type: None, // would need a Node fetch and extra RBAC
    })
}

#[cfg(test)]
mod tests;
