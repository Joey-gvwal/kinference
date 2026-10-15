//! `InferenceWorkerSet` CRs to [`PoolSpec`]s, and the watch that keeps them
//! current.
//!
//! In Kubernetes mode the CR supplies the topology (Service, ports, block size)
//! and TOML supplies only the `[[profile]]` tuning.
//!
//! The CR fields are `Option` in Rust although the schema requires them:
//! `kube::runtime::watcher` deserializes list pages wholesale, so one
//! nonconforming object would fail the whole page and stall the watch for every
//! worker set. The types parse permissively and [`desired_pools`] turns a
//! violation into a [`Rejection`].

use std::collections::BTreeMap;
use std::time::Duration;

use futures::TryStreamExt;
use kinference_policy::PoolKey;
use kinference_policy::crd::{InferenceWorkerSet, IntOrString, ModelDiscoveryMode};
use kinference_policy::rejection::{RejectReason, Rejection};
use kube::Api;
use kube::api::ResourceExt;
use kube::runtime::watcher::{self, watcher as start_watcher};
use kube::runtime::{WatchStreamExt, reflector};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::discovery::{DiscoveryConfig, DiscoveryDefaults, DiscoveryMode, PortRef, PortRefs};
use crate::pool::{PoolSpec, ProfileConfig};

/// Worker sets that produced no pool, published for the status writer. A
/// rejected set appears in no `PolicyIndex`, so this is the only way its status
/// gets updated.
#[derive(Default)]
pub struct RejectionStore {
    current: std::sync::RwLock<std::sync::Arc<Vec<Rejection>>>,
}

impl RejectionStore {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> std::sync::Arc<Vec<Rejection>> {
        self.current
            .read()
            .expect("rejection store poisoned")
            .clone()
    }

    fn publish(&self, v: Vec<Rejection>) {
        *self.current.write().expect("rejection store poisoned") = std::sync::Arc::new(v);
    }
}

/// Resolve every `InferenceWorkerSet` into a `PoolSpec`, or a `Rejection`.
///
/// The result is total: `PoolManager::reconcile` retires every pool absent from
/// it. It is a `BTreeMap` so replicas build identical fleets regardless of the
/// order the watch delivered objects in.
pub fn desired_pools(
    sets: &[InferenceWorkerSet],
    profiles: &[ProfileConfig],
    defaults: &DiscoveryDefaults,
) -> (BTreeMap<PoolKey, PoolSpec>, Vec<Rejection>) {
    let by_name: BTreeMap<&str, &ProfileConfig> =
        profiles.iter().map(|p| (p.name.as_str(), p)).collect();
    let mut out = BTreeMap::new();
    let mut rejected = Vec::new();

    for iws in sets {
        // Always namespaced: same-named sets in different namespaces are
        // different fleets, and merging them would silently mix their prefix
        // models.
        let key = PoolKey::namespaced(iws.namespace().unwrap_or_default(), iws.name_any());
        let spec = &iws.spec;

        let Some(profile_name) = spec.profile.as_deref().filter(|p| !p.trim().is_empty()) else {
            rejected.push(Rejection {
                key,
                reason: RejectReason::MissingProfile,
            });
            continue;
        };
        let Some(profile) = by_name.get(profile_name) else {
            let mut defined: Vec<String> = by_name.keys().map(|s| s.to_string()).collect();
            defined.sort();
            rejected.push(Rejection {
                key,
                reason: RejectReason::UnknownProfile { defined },
            });
            continue;
        };
        let Some(block_size) = spec.kv_block_size else {
            rejected.push(Rejection {
                key,
                reason: RejectReason::MissingKvBlockSize,
            });
            continue;
        };
        if block_size == 0 {
            rejected.push(Rejection {
                key,
                reason: RejectReason::InvalidKvBlockSize { got: block_size },
            });
            continue;
        }
        if spec.service_ref.name.trim().is_empty() {
            rejected.push(Rejection {
                key,
                reason: RejectReason::MissingServiceRef,
            });
            continue;
        }
        if spec.models.iter().any(|m| m.name.is_none()) {
            rejected.push(Rejection {
                key,
                reason: RejectReason::ModelMissingName,
            });
            continue;
        }
        // A `kvEvents` block without a port is refused. Defaulting the port or
        // treating it as "publishes nothing" would both yield a healthy-looking
        // pool whose prefix model stays empty.
        if spec.kv_events.as_ref().is_some_and(|k| k.port.is_none()) {
            rejected.push(Rejection {
                key,
                reason: RejectReason::KvEventsMissingPort,
            });
            continue;
        }

        out.insert(
            key.clone(),
            PoolSpec {
                name: key.to_string(),
                profile: profile.name.clone(),
                kv_block_size: block_size,
                predictor: profile.predictor.clone(),
                picker: profile.picker.clone(),
                tokenize_mode: profile.tokenize.mode,
                // Renders rotate across the pool's own workers.
                render_url: None,
                discovery: DiscoveryConfig {
                    mode: DiscoveryMode::Kubernetes,
                    workers_file: None,
                    // Never `None`, which would watch every namespace and merge
                    // same-named Services into one pool.
                    namespace: Some(key.namespace.clone().unwrap_or_default()),
                    service_name: Some(spec.service_ref.name.clone()),
                    workerset: Some(key.to_string()),
                    defaults: defaults.clone(),
                    ports: PortRefs {
                        http: spec.service_ref.port.as_ref().and_then(port_ref),
                        metrics: spec.metrics_port.as_ref().and_then(port_ref),
                        zmq: spec
                            .kv_events
                            .as_ref()
                            .and_then(|k| k.port.as_ref())
                            .and_then(port_ref),
                        replay: spec
                            .kv_events
                            .as_ref()
                            .and_then(|k| k.replay_port.as_ref())
                            .and_then(port_ref),
                        // No `kvEvents` block means these engines don't publish.
                        kv_events_declared_absent: spec.kv_events.is_none(),
                    },
                },
                models_refresh_secs: spec
                    .model_discovery
                    .as_ref()
                    .and_then(|m| refresh_secs(&m.interval, &key)),
                static_models: spec
                    .model_discovery
                    .as_ref()
                    .filter(|m| m.mode == ModelDiscoveryMode::Static)
                    .map(|_| {
                        // `name` is what the engine reports, i.e. what discovery
                        // would otherwise have found.
                        spec.models.iter().filter_map(|m| m.name.clone()).collect()
                    }),
                key,
            },
        );
    }
    (out, rejected)
}

/// `spec.modelDiscovery.interval` in whole seconds.
///
/// An unparseable or non-positive value falls back to the fleet default with a
/// warning rather than rejecting the set. Sub-second values are clamped to 1s.
fn refresh_secs(interval: &str, key: &PoolKey) -> Option<u64> {
    match kinference_policy::predicate::parse_duration_ms(interval) {
        Ok(ms) if ms >= 1.0 => Some(((ms / 1000.0).round() as u64).max(1)),
        Ok(ms) => {
            tracing::warn!(
                workerset = %key,
                interval,
                ms,
                "spec.modelDiscovery.interval is not a positive duration; \
                 using the fleet-wide models_refresh_secs"
            );
            None
        }
        Err(err) => {
            tracing::warn!(
                workerset = %key,
                interval,
                error = %err,
                "spec.modelDiscovery.interval did not parse; \
                 using the fleet-wide models_refresh_secs"
            );
            None
        }
    }
}

/// A CR port reference in the form discovery consumes. A number that isn't a
/// valid port is dropped rather than truncated, so the role falls back to its
/// conventional name.
fn port_ref(v: &IntOrString) -> Option<PortRef> {
    match v {
        IntOrString::Str(s) if !s.trim().is_empty() => Some(PortRef::Name(s.clone())),
        IntOrString::Str(_) => None,
        IntOrString::Int(n) => u16::try_from(*n)
            .ok()
            .filter(|p| *p > 0)
            .map(PortRef::Number),
    }
}

/// `[kubernetes]`: worker sets derived from `InferenceWorkerSet` CRs. Profiles
/// still come from TOML.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct KubernetesConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Namespace to watch; `None` watches all (needs cluster-scoped RBAC).
    #[serde(default)]
    pub namespace: Option<String>,
    /// Label selector an `InferenceWorkerSet` must match, e.g.
    /// `kinference.buoyant.io/router=edge`. `None` matches everything, which is
    /// only right when a single router runs in the cluster; leader election
    /// doesn't prevent every router from building every pool.
    #[serde(default)]
    pub selector: Option<String>,
    /// Last-resort ports, used when neither the Pod nor the CR names one.
    #[serde(default)]
    pub discovery_defaults: DiscoveryDefaults,
}

/// Watch `InferenceWorkerSet`s and drive [`crate::pool::PoolManager`].
///
/// Owns the manager, so the live map needs no lock; the periodic reap sweep
/// runs here for the same reason.
pub struct WorkerSetWatcher {
    namespace: Option<String>,
    selector: Option<String>,
    profiles: Vec<ProfileConfig>,
    defaults: DiscoveryDefaults,
    manager: crate::pool::PoolManager,
    /// Sets that resolved to nothing, kept so a rejection is logged once per
    /// transition rather than on every watch tick.
    rejected: Vec<Rejection>,
    /// The current generation's store. Rebuilt with the watch.
    store: Option<reflector::Store<InferenceWorkerSet>>,
    rejection_store: std::sync::Arc<RejectionStore>,
    prom: std::sync::Arc<crate::metrics::PromMetrics>,
}

impl WorkerSetWatcher {
    /// The kube client is passed to [`Self::run`] instead, so tests can drive
    /// reconciles without one.
    pub fn new(
        kube_cfg: &KubernetesConfig,
        profiles: Vec<ProfileConfig>,
        manager: crate::pool::PoolManager,
        rejection_store: std::sync::Arc<RejectionStore>,
        prom: std::sync::Arc<crate::metrics::PromMetrics>,
    ) -> Self {
        Self {
            namespace: kube_cfg.namespace.clone(),
            selector: kube_cfg.selector.clone(),
            profiles,
            defaults: kube_cfg.discovery_defaults.clone(),
            manager,
            rejected: Vec::new(),
            store: None,
            rejection_store,
            prom,
        }
    }

    /// Run until cancelled, rebuilding the watch whenever its stream ends.
    pub async fn run(mut self, client: kube::Client, cancel: CancellationToken) {
        const REBUILD_DELAY: Duration = Duration::from_secs(2);
        loop {
            if cancel.is_cancelled() {
                return;
            }
            self.run_once(&client, &cancel).await;
            if cancel.is_cancelled() {
                return;
            }
            tracing::warn!("worker sets: rebuilding the InferenceWorkerSet watch");
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(REBUILD_DELAY) => {}
            }
        }
    }

    async fn run_once(&mut self, client: &kube::Client, cancel: &CancellationToken) {
        let api: Api<InferenceWorkerSet> = match self.namespace.as_deref() {
            Some(ns) => Api::namespaced(client.clone(), ns),
            None => Api::all(client.clone()),
        };
        let mut cfg = watcher::Config::default();
        if let Some(sel) = self.selector.as_deref() {
            cfg = cfg.labels(sel);
        }
        let sets = reflector::reflector(
            {
                let (store, writer) = reflector::store::<InferenceWorkerSet>();
                self.store = Some(store);
                writer
            },
            start_watcher(api, cfg),
        )
        .default_backoff();
        tokio::pin!(sets);

        tracing::info!(
            namespace = ?self.namespace,
            selector = ?self.selector,
            profiles = self.profiles.len(),
            "worker sets: watching InferenceWorkerSets"
        );

        let mut sweep = tokio::time::interval(Duration::from_secs(10));
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Until the initial list completes the store is partial, and since
        // `desired_pools` is total, reconciling would retire every pool and
        // discard its prefix cache. Only reconciling is gated; reaping is safe.
        let mut listed = false;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = sweep.tick() => { self.manager.sweep(); }
                ev = sets.try_next() => {
                    match ev {
                        Ok(Some(watcher::Event::InitDone)) => listed = true,
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            tracing::warn!("worker sets: watch stream ended; will rebuild");
                            return;
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "worker sets: watch error; continuing");
                            continue;
                        }
                    }
                    if !listed {
                        continue;
                    }
                    self.reconcile_from_store();
                }
            }
        }
    }

    #[cfg(test)]
    fn with_store(mut self, store: reflector::Store<InferenceWorkerSet>) -> Self {
        self.store = Some(store);
        self
    }

    fn reconcile_from_store(&mut self) {
        let Some(store) = self.store.as_ref() else {
            return;
        };
        let sets = store.state();
        let (desired, rejected) = desired_pools(
            &sets.iter().map(|s| (**s).clone()).collect::<Vec<_>>(),
            &self.profiles,
            &self.defaults,
        );
        // Log each rejection once per transition, not on every watch event.
        for r in &rejected {
            if !self.rejected.contains(r) {
                tracing::warn!(
                    workerset = %r.key,
                    reason = r.reason.as_condition_reason(),
                    "worker set not materialized: {}",
                    r.reason
                );
                self.prom
                    .record_workerset_rejected(r.reason.as_condition_reason());
            }
        }
        self.rejected = rejected.clone();
        self.rejection_store.publish(rejected);

        let report = self.manager.reconcile(desired.into_values().collect());
        if !report.is_noop() {
            tracing::info!(
                created = ?report.created,
                resurrected = ?report.resurrected,
                updated = ?report.updated,
                replaced = ?report.replaced,
                retired = ?report.retired,
                failed = ?report.failed,
                live = self.manager.live_keys().len(),
                "worker sets reconciled"
            );
        }
    }
}

#[cfg(test)]
mod tests;
