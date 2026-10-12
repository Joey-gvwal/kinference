//! `InferenceWorkerSet` CRs → [`PoolSpec`]s, and the watch that keeps them current.
//!
//! In Kubernetes mode the CR is the source of truth for TOPOLOGY — which
//! Service, which ports, which block size — and TOML keeps only the
//! `[[profile]]` blocks, because routing tuning is the router's own calibration
//! rather than a property of the fleet. An operator creates an
//! `InferenceWorkerSet`, names a profile, and kinference routes to it. No TOML change
//! per fleet.
//!
//! ## Why the CR fields are `Option` in Rust and required in the schema
//!
//! `kube::runtime::watcher` deserializes list pages **wholesale**. A single
//! object predating the schema change — or applied with `--validate=false` —
//! fails the entire page, producing a `watcher::Error`, backoff, and retry
//! forever. That is not "one bad object is skipped": it is **discovery down for
//! every worker set in the namespace**. So the types parse permissively,
//! [`desired_pools`] enforces, and a violation becomes a [`Rejection`] rather
//! than a dead watch.
//!
//! ## Why the listed gate matters more here than anywhere else
//!
//! [`desired_pools`] is total: whatever it returns IS the desired set, and
//! `PoolManager::reconcile` retires everything absent from it. Reconciling
//! against a store that has not finished its initial list therefore deletes
//! **every pool in the fleet** — and then recreates them a moment later, having
//! discarded every prefix cache in between. The gate is what makes a watch
//! reconnect invisible instead of a fleet-wide cache flush.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::TryStreamExt;
use kube::Api;
use kube::api::ResourceExt;
use kube::runtime::watcher::{self, watcher as start_watcher};
use kube::runtime::{WatchStreamExt, reflector};
use tokio_util::sync::CancellationToken;

use super::crd::{InferenceWorkerSet, IntOrString, ModelDiscoveryMode};
use crate::config::{
    DiscoveryConfig, DiscoveryDefaults, DiscoveryMode, PoolKey, PoolSpec, PortRef, PortRefs,
    ProfileConfig,
};

/// Why a worker set could not be materialized.
///
/// Every variant is an operator-fixable misconfiguration, and they are kept
/// distinct because the FIX differs: an unknown profile means editing the
/// router's TOML, a missing one means editing the CR. Reporting them alike
/// would send an operator to the wrong file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// `spec.profile` absent. Required by the schema, so this means the object
    /// predates it or bypassed validation.
    MissingProfile,
    /// `spec.profile` names a profile the router does not define. Carries the
    /// defined names, because in practice this is a typo and seeing the list
    /// resolves it immediately.
    UnknownProfile { defined: Vec<String> },
    /// `spec.kvBlockSize` absent, same provenance as `MissingProfile`.
    MissingKvBlockSize,
    /// `spec.kvBlockSize` is zero. The schema forbids it; a `KvIndexer` built
    /// on it would divide by zero.
    InvalidKvBlockSize { got: u32 },
    /// `spec.serviceRef.name` is empty, so there is nothing to watch.
    MissingServiceRef,
    /// A `spec.models` row carries no `name`.
    ///
    /// The row maps an alias onto nothing, so the alias silently would not
    /// resolve. Refused rather than dropped: the schema requires `name`, so this
    /// is only reachable by an object that bypassed validation, and a declared
    /// alias that does not work is a routing lie rather than a cosmetic slip.
    ModelMissingName,
    /// `spec.kvEvents` is present but carries no `port`.
    ///
    /// A contradiction rather than an omission: the block IS the declaration
    /// that these engines publish events, and it does not say where. The schema
    /// requires `port`, so this is only reachable by an object that bypassed
    /// validation — but it has to be reachable, because `port` is `Option` in
    /// Rust precisely so one such object cannot fail a whole watcher page.
    KvEventsMissingPort,
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingProfile => write!(f, "spec.profile is required"),
            Self::UnknownProfile { defined } => write!(
                f,
                "spec.profile names an undefined routing profile; the router defines [{}]",
                defined.join(", ")
            ),
            Self::MissingKvBlockSize => write!(f, "spec.kvBlockSize is required"),
            Self::InvalidKvBlockSize { got } => {
                write!(f, "spec.kvBlockSize must be > 0, got {got}")
            }
            Self::MissingServiceRef => write!(f, "spec.serviceRef.name is required"),
            Self::ModelMissingName => write!(
                f,
                "a spec.models row has no name; every row needs the name the \
                 engines report for that model"
            ),
            Self::KvEventsMissingPort => write!(
                f,
                "spec.kvEvents is present but has no port; omit the whole block if these \
                 engines publish no KV events"
            ),
        }
    }
}

/// Short machine-readable form, for a status condition's `reason` and for a
/// metric label. Kubernetes conditions want CamelCase here.
impl RejectReason {
    pub fn as_condition_reason(&self) -> &'static str {
        match self {
            Self::MissingProfile => "MissingProfile",
            Self::UnknownProfile { .. } => "UnknownProfile",
            Self::MissingKvBlockSize => "MissingKvBlockSize",
            Self::InvalidKvBlockSize { .. } => "InvalidKvBlockSize",
            Self::MissingServiceRef => "MissingServiceRef",
            Self::ModelMissingName => "ModelMissingName",
            Self::KvEventsMissingPort => "KvEventsMissingPort",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub key: PoolKey,
    pub reason: RejectReason,
}

/// Worker sets that produced no pool, published for the status writer.
///
/// A rejected set gets no pool, so it appears in no `PolicyIndex` and the
/// status writer would otherwise never touch it — leaving the object with a
/// stale `Accepted=True` from before the break, or no condition at all. An
/// operator would then have to read the router's logs to find out why nothing
/// routes to their worker set, which is exactly what status exists to avoid.
///
/// Same publish/snapshot shape as `PolicyStore`: take the lock, clone the
/// `Arc`, release.
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
/// Pure: no I/O, no clock, no cluster. That is what makes the interesting cases
/// — an unknown profile, a set with no `kvBlockSize`, two same-named Services in
/// different namespaces — testable against hand-seeded reflector stores instead
/// of against a cluster.
///
/// The result is a `BTreeMap`, so the desired set has one stable order
/// regardless of the order the watch happened to deliver objects in. Two
/// replicas that connected at different times then build identical fleets.
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
        // A namespaced key, always. Two same-named Services in different
        // namespaces are different fleets, and collapsing them into one pool
        // would merge their prefix models silently — the worst kind of wrong,
        // because every request still succeeds.
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
        // Refused rather than read one of the two silent ways: connecting to a
        // default socket that will never speak, or downgrading the set to
        // "publishes nothing" and reporting a healthy pool whose prefix model
        // stays permanently empty. Both serve traffic while lying about it.
        if spec.models.iter().any(|m| m.name.is_none()) {
            rejected.push(Rejection {
                key,
                reason: RejectReason::ModelMissingName,
            });
            continue;
        }
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
                // Deliberately none: a render URL is a per-endpoint fact and a
                // CR-driven pool has no dedicated render box. Render/compare
                // rotate across the pool's own workers, which is right
                // in-cluster.
                render_url: None,
                discovery: DiscoveryConfig {
                    mode: DiscoveryMode::Kubernetes,
                    workers_file: None,
                    // ALWAYS the set's own namespace, never `None` (which would
                    // mean "watch every namespace"). Otherwise two same-named
                    // Services in different namespaces merge into one pool.
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
                        // The CR is authoritative here: no `kvEvents` block
                        // means these engines don't publish, and honoring that
                        // beats connecting to a default that will never speak.
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
                        // `name` IS the upstream now — what the engine reports —
                        // so a Static set declares exactly the strings discovery
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
/// An unparseable or sub-second value falls back to the fleet default with a
/// WARN rather than failing the whole set: the interval is a tuning knob, and
/// refusing to route over it would be wildly out of proportion. Sub-second is
/// clamped to 1s — the scrape is an HTTP round-trip per worker, and a fleet
/// hammering `/v1/models` at 100Hz is a worse outcome than a slow refresh.
fn refresh_secs(interval: &str, key: &PoolKey) -> Option<u64> {
    match super::predicate::parse_duration_ms(interval) {
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

/// A CR port reference in the form discovery consumes. A number that cannot be
/// a port (negative, or above 65535) is dropped rather than truncated, so the
/// role falls back to its conventional name instead of dialing something
/// arbitrary.
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

/// Watch `InferenceWorkerSet`s and drive [`crate::pool_manager::PoolManager`].
///
/// Owns the manager outright — the plan's "one reconcile task, so the live map
/// needs no lock" — and is also where the periodic reap sweep lives, since both
/// want `&mut PoolManager`.
pub struct WorkerSetWatcher {
    namespace: Option<String>,
    selector: Option<String>,
    profiles: Vec<ProfileConfig>,
    defaults: DiscoveryDefaults,
    manager: crate::pool_manager::PoolManager,
    /// Sets that resolved to nothing, kept so a rejection is logged once per
    /// transition rather than on every watch tick.
    rejected: Vec<Rejection>,
    /// The current generation's store. Rebuilt with the watch.
    store: Option<reflector::Store<InferenceWorkerSet>>,
    /// Published for the status writer, which cannot see rejections any other
    /// way — a rejected set has no pool and so appears in no policy snapshot.
    rejection_store: std::sync::Arc<RejectionStore>,
    prom: std::sync::Arc<crate::prom::PromMetrics>,
}

impl WorkerSetWatcher {
    /// The kube client is handed to [`Self::run`] rather than held here: it is
    /// needed only by the watch, and keeping it out means the resolution and
    /// reconcile logic can be driven in tests without one.
    pub fn new(
        kube_cfg: &crate::config::KubernetesConfig,
        profiles: Vec<ProfileConfig>,
        manager: crate::pool_manager::PoolManager,
        rejection_store: std::sync::Arc<RejectionStore>,
        prom: std::sync::Arc<crate::prom::PromMetrics>,
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
    ///
    /// A stream that ends is ordinary — an apiserver restart, a rolled
    /// connection — and must not be terminal: the pool set would freeze at
    /// whatever it last saw with nothing to say so. The sweep keeps running
    /// across rebuilds, so a retired pool is still reaped while the watch is
    /// reconnecting.
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
        // Until the initial list completes the store is partial, and
        // `desired_pools` is TOTAL — so reconciling now would retire every pool
        // in the fleet and rebuild them a moment later, having thrown away every
        // prefix cache in between. Reaping is safe meanwhile; only reconciling
        // is gated.
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
        // Log each rejection ONCE per transition, not per event: a set naming a
        // missing profile stays broken until an operator acts, and re-logging it
        // on every watch tick buries everything else.
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
