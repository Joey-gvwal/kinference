//! The CRD watch and the resolved snapshot the pick path reads.
//!
//! Six reflectors feed one rebuild — `InferencePool`, `InferencePolicy`,
//! `InferenceWorkerSet`, `InferenceProvider`, and (read-only) `HTTPRoute` and
//! `ReferenceGrant`. This follows [`crate::discovery::kubernetes`]
//! deliberately: watch events are treated as bare wakeups and ALL state is
//! re-derived from `store.state()`, so there is no incremental-update logic to
//! get subtly wrong. The resolution itself is a free function over those
//! stores, which is what makes it testable without a cluster.
//!
//! A seventh reflector, on core `Service`, rides along on the same task but is
//! NOT an input to [`resolve`] and does not hold the publish gate — see
//! [`super::services`] for both halves of that reasoning. Its output is
//! attached to the published snapshot ([`PolicyIndex::services`]) so a consumer
//! reading pools and Service ports sees one consistent instant.
//!
//! Consumers want current state, not a change log, so the output is an
//! immutable `Arc<PolicyIndex>` published into a cell — "latest snapshot wins".
//!
//! The resolution itself lives in [`super::resolve`]; this module only feeds
//! it and publishes what it returns.

use std::sync::{Arc, RwLock};

use futures::TryStreamExt;
use k8s_openapi::api::core::v1::Service;
use kube::runtime::WatchStreamExt;
use kube::runtime::reflector;
use kube::runtime::watcher::{self, watcher as start_watcher};
use kube::{Api, Resource};
use tokio_util::sync::CancellationToken;

use super::crd::{
    HTTPRoute, InferencePolicy, InferencePool, InferenceProvider, InferenceWorkerSet,
    ReferenceGrant,
};
use super::resolve::{PolicyIndex, PoolIndex, rejection_index, resolve};
use super::services::ServiceIndex;

/// The published snapshot. Read on the pick hot path: take the read lock, clone
/// the `Arc`, release — same shape as the existing `model_router`.
#[derive(Default)]
pub struct PolicyStore {
    current: RwLock<Arc<PolicyIndex>>,
}

impl PolicyStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> Arc<PolicyIndex> {
        self.current.read().expect("policy store poisoned").clone()
    }

    pub fn publish(&self, index: Arc<PolicyIndex>) {
        *self.current.write().expect("policy store poisoned") = index;
    }
}

/// Build the identity index from a live pool snapshot.
pub fn pool_index(pools: &crate::pool::PoolMap) -> PoolIndex {
    pools.iter().map(|p| (p.key.clone(), p.id)).collect()
}

/// Watch the CRDs and keep [`PolicyStore`] current.
pub struct PolicyWatcher {
    client: kube::Client,
    namespace: Option<String>,
    /// Why worker sets were refused, read on every resolve so an unresolved
    /// destination reports the actual cause rather than a guess.
    rejections: Arc<super::worker_sets::RejectionStore>,
    /// The live pool set, read on every resolve.
    ///
    /// Held as the registry rather than a startup snapshot because pools come
    /// and go now: a captured index would resolve worker sets to pools that no
    /// longer exist and miss ones that do.
    pools: Arc<crate::pool::PoolRegistry>,
    store: Arc<PolicyStore>,
}

impl PolicyWatcher {
    /// Build a watcher.
    ///
    /// Unlike [`crate::discovery::kubernetes::KubeDiscovery`], this honors
    /// `~/.kube/config` when there is no service-account token. Discovery
    /// refuses to, and for a good reason — the pod IPs it hands back are
    /// unreachable from outside the cluster network, so a kubeconfig-based run
    /// would authenticate happily and then route into a void. Policy has no
    /// such hazard: it only reads and writes CRDs. Honoring kubeconfig makes
    /// the whole policy layer testable against a local cluster.
    pub async fn new(
        namespace: Option<String>,
        pools: Arc<crate::pool::PoolRegistry>,
        rejections: Arc<super::worker_sets::RejectionStore>,
        store: Arc<PolicyStore>,
    ) -> anyhow::Result<Self> {
        let kube_cfg = kube::Config::infer().await.map_err(|e| {
            anyhow::anyhow!(
                "failed to build a Kubernetes client for the policy watcher \
                 ([policy] enabled = true): no in-cluster service-account token and no usable \
                 kubeconfig. Set `[policy] enabled = false` to run without CRD policy. ({e})"
            )
        })?;
        let client = kube::Client::try_from(kube_cfg)?;
        Ok(Self {
            client,
            namespace,
            pools,
            rejections,
            store,
        })
    }

    fn api<K>(&self) -> Api<K>
    where
        K: Resource<Scope = kube::core::NamespaceResourceScope>,
        K::DynamicType: Default,
    {
        match self.namespace.as_deref() {
            Some(ns) => Api::namespaced(self.client.clone(), ns),
            None => Api::all(self.client.clone()),
        }
    }

    /// Run until cancelled. Every watch event triggers a full re-resolve, which
    /// keeps the logic single-path; CRD churn is low enough that recomputing is
    /// cheaper than maintaining incremental state correctly.
    /// Rebuild the whole watcher set whenever a stream ends, rather than
    /// exiting.
    ///
    /// A `watcher` stream yielding `Ok(None)` used to return from `run`, which
    /// killed all four reflectors permanently — the process kept serving with a
    /// snapshot frozen at whatever it last saw, and nothing said so. One ended
    /// stream must not be terminal.
    pub async fn run(self, cancel: CancellationToken) {
        // Backoff between rebuilds so a stream that ends immediately (bad RBAC,
        // for instance) cannot spin against the apiserver.
        const REBUILD_DELAY: std::time::Duration = std::time::Duration::from_secs(2);
        loop {
            if cancel.is_cancelled() {
                return;
            }
            self.run_once(&cancel).await;
            if cancel.is_cancelled() {
                return;
            }
            tracing::warn!("policy: rebuilding the CRD watcher set");
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(REBUILD_DELAY) => {}
            }
        }
    }

    /// One generation of the watcher set. Returns when cancelled or when any
    /// stream ends; the caller rebuilds.
    ///
    /// Publishing is gated on every kind having completed its initial list. The
    /// stores here are fresh, so resolving mid-list would see an empty world and
    /// publish a snapshot claiming every pool had vanished. `PolicyStore` keeps
    /// serving the previous snapshot until this generation has a complete view,
    /// which is what makes a rebuild invisible to the pick path.
    async fn run_once(&self, cancel: &CancellationToken) {
        let (pool_store, pool_writer) = reflector::store::<InferencePool>();
        let (policy_store, policy_writer) = reflector::store::<InferencePolicy>();
        let (ws_store, ws_writer) = reflector::store::<InferenceWorkerSet>();
        let (prov_store, prov_writer) = reflector::store::<InferenceProvider>();
        let (route_store, route_writer) = reflector::store::<HTTPRoute>();
        let (grant_store, grant_writer) = reflector::store::<ReferenceGrant>();
        let (svc_store, svc_writer) = reflector::store::<Service>();

        let cfg = watcher::Config::default();
        let pools = reflector::reflector(pool_writer, start_watcher(self.api(), cfg.clone()))
            .default_backoff();
        let policies = reflector::reflector(policy_writer, start_watcher(self.api(), cfg.clone()))
            .default_backoff();
        let sets = reflector::reflector(ws_writer, start_watcher(self.api(), cfg.clone()))
            .default_backoff();
        let provs = reflector::reflector(prov_writer, start_watcher(self.api(), cfg.clone()))
            .default_backoff();
        let routes = reflector::reflector(route_writer, start_watcher(self.api(), cfg.clone()))
            .default_backoff();
        let grants = reflector::reflector(grant_writer, start_watcher(self.api(), cfg.clone()))
            .default_backoff();
        // Core `Service`, and the only watch here that is not a CRD.
        //
        // Cluster-wide when `discovery.namespace` is unset, which is the one
        // cost worth naming: a big cluster has far more Services than inference
        // CRs, and this holds every one of them. Narrowing it is not free either
        // way — a label selector would need a label kinference does not own, and
        // watching only the Services worker sets name would mean tearing down
        // and rebuilding a watch on every CR edit — so scope it with
        // `discovery.namespace` when the router serves one namespace.
        let services =
            reflector::reflector(svc_writer, start_watcher(self.api(), cfg)).default_backoff();
        tokio::pin!(pools, policies, sets, provs, routes, grants, services);

        tracing::info!(
            namespace = ?self.namespace,
            pools = self.pools.snapshot().len(),
            "policy: watching kinference.buoyant.io CRDs + HTTPRoutes + ReferenceGrants + Services"
        );

        // One flag per kind, set when that kind finishes its initial list.
        //
        // Providers and grants join the gate rather than being allowed to lag:
        // resolving before the provider store has listed would report every
        // provider destination unresolved, and resolving before the GRANT store
        // has listed would retract every cross-namespace reference — a check that
        // fails closed must not be consulted before it can answer.
        //
        // Services do NOT join it, and that is the load-bearing part of adding
        // them: nothing in `resolve` reads a Service, so holding every publish
        // until they list would mean a cluster that upgraded kinference without the
        // `services` grant lost policy routing entirely — a 403 is `Step::Ignore`
        // forever, so the flag would never flip and the symptom would point
        // nowhere near the cause. Their listed-ness rides on the snapshot
        // instead (`ServiceIndex::listed`), where the consumer that actually
        // needs it can wait on it and nothing else is affected.
        let mut listed = [false; SVC + 1];
        // Resolution depends on the POOL SET as well as the CRDs, and the two
        // change independently: a worker set materializes into a pool some
        // moment after the CRD event that asked for it. Without this arm, that
        // pool's destination stays reported unresolved until the next unrelated
        // CRD event — the router routing to it correctly the whole time.
        let mut pools_changed = self.pools.subscribe();
        loop {
            let (idx, name, kind) = tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("policy: cancellation received, exiting");
                    return;
                }
                _ = pools_changed.changed() => (0, "", Step::Changed),
                r = pools.try_next() => (0, "InferencePool", step(r, "InferencePool")),
                r = policies.try_next() => (1, "InferencePolicy", step(r, "InferencePolicy")),
                r = sets.try_next() => (2, "InferenceWorkerSet", step(r, "InferenceWorkerSet")),
                r = provs.try_next() => (3, "InferenceProvider", step(r, "InferenceProvider")),
                r = routes.try_next() => (4, "HTTPRoute", step(r, "HTTPRoute")),
                r = grants.try_next() => (5, "ReferenceGrant", step(r, "ReferenceGrant")),
                r = services.try_next() => (SVC, "Service", step(r, "Service")),
            };
            match kind {
                Step::Listed => listed[idx] = true,
                // A kind whose CRD is not installed. Counts as LISTED with an
                // empty store, and that is not a shortcut: a resource that does
                // not exist has no objects, which is exactly what an empty store
                // says.
                //
                // The alternative is what this replaces, and it was severe. An
                // uninstallable kind returned an error forever, its `listed` flag
                // never flipped, and the all-kinds gate below then blocked EVERY
                // publish — so a missing optional CRD silently disabled policy
                // routing entirely, with nothing but a repeating warning to say
                // so. Two of these six kinds are genuinely optional
                // (`HTTPRoute` only decorates pool status; `ReferenceGrant` only
                // permits cross-namespace refs), so that was a real
                // configuration to fall into.
                //
                // Note the direction this fails in for grants: no grants visible
                // means no cross-namespace reference is permitted, which is the
                // conservative answer for a check whose job is to stop one
                // namespace spending another's credential.
                Step::Absent => {
                    if !listed[idx] {
                        tracing::warn!(
                            kind = name,
                            "policy: this CRD is not installed on the cluster; treating it as \
                             having no objects. Cross-namespace references need \
                             ReferenceGrant, and pool status `parents[]` needs HTTPRoute — \
                             install the Gateway API CRDs if you want either"
                        );
                    }
                    listed[idx] = true;
                }
                Step::Changed => {}
                Step::Ignore => continue,
                Step::Ended => return,
            }
            // A Service event arriving BEFORE the Service list completes changes
            // nothing that would be published — the index is unlisted either
            // way — so it is not worth a full CRD re-resolve. Without this, a
            // cluster with a few thousand Services drives one complete resolve
            // and publish per `InitApply` at startup, all of them identical.
            if idx == SVC && !listed[SVC] {
                continue;
            }
            // Until every GATED kind has listed, the stores are partial and any
            // snapshot built from them would be wrong in the direction that
            // matters: pools missing rather than pools stale. `Service` is
            // outside the gate on purpose (see above), so it is excluded here
            // rather than merely happening to be last.
            if !listed[..SVC].iter().all(|&l| l) {
                continue;
            }
            let index = resolve(
                &pool_store.state(),
                &policy_store.state(),
                &ws_store.state(),
                &prov_store.state(),
                &route_store.state(),
                &grant_store.state(),
                &pool_index(&self.pools.snapshot()),
                &rejection_index(&self.rejections.snapshot()),
            );
            // Attached, not resolved into: an unlisted index is published as
            // unlisted so the reader can tell "no such Service" from "not read
            // yet" instead of inferring an absence from an empty map.
            let index = index.with_services(if listed[SVC] {
                ServiceIndex::build(&svc_store.state())
            } else {
                ServiceIndex::unlisted()
            });
            let unresolved: usize = index.pools().map(|p| p.unresolved.len()).sum();
            tracing::debug!(
                pools = index.len(),
                unresolved_destinations = unresolved,
                services = index.services().len(),
                services_listed = index.services().listed(),
                "policy: snapshot rebuilt"
            );
            self.store.publish(Arc::new(index));
        }
    }
}

/// Index of the `Service` flag in `run_once`'s `listed` array, and therefore
/// also the count of kinds that DO hold the publish gate — everything below it.
/// Named so the gate check cannot silently start including Services when the
/// next kind is added.
const SVC: usize = 6;

enum Step {
    /// This kind just completed its initial list; its store is now whole.
    Listed,
    /// This kind's CRD is not installed. Definitive rather than transient, so it
    /// counts as an empty store instead of an error to retry past.
    Absent,
    Changed,
    Ignore,
    Ended,
}

fn step<T>(r: Result<Option<watcher::Event<T>>, watcher::Error>, kind: &str) -> Step {
    match r {
        // `InitDone` is the only event that tells us the store went from
        // partially-populated to complete. The `Writer` buffers `InitApply`
        // objects and swaps them in atomically here, so this is the first
        // moment `store.state()` is trustworthy for a fresh reflector.
        Ok(Some(watcher::Event::InitDone)) => Step::Listed,
        Ok(Some(_)) => Step::Changed,
        Ok(None) => {
            tracing::warn!(kind, "policy: watcher stream ended; will rebuild");
            Step::Ended
        }
        Err(err) if crd_not_installed(&err) => Step::Absent,
        Err(err) => {
            tracing::warn!(kind, error = %err, "policy: watcher error; continuing");
            Step::Ignore
        }
    }
}

/// Whether a watcher error means "this resource type does not exist here".
///
/// Distinguished from every other failure because the right response is the
/// opposite: a 403 or a connection reset is transient and must be retried, while
/// a 404 on the collection is a standing fact about the cluster that retrying
/// cannot change. Conflating them is what let a missing optional CRD hold the
/// publish gate shut forever.
///
/// Deliberately narrow — only the initial list and the watch start, and only
/// `404`. A `NotFound` mid-watch means something else.
fn crd_not_installed(err: &watcher::Error) -> bool {
    let inner = match err {
        watcher::Error::InitialListFailed(e) | watcher::Error::WatchStartFailed(e) => e,
        _ => return false,
    };
    matches!(inner, kube::Error::Api(resp) if resp.code == 404)
}

#[cfg(test)]
mod tests;
