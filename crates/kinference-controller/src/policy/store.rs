//! The CRD watch, which publishes the resolved snapshot the pick path reads.
//!
//! Six reflectors feed [`resolve`]: `InferencePool`, `InferencePolicy`,
//! `InferenceWorkerSet`, `InferenceProvider`, and (read-only) `HTTPRoute` and
//! `ReferenceGrant`. As in [`crate::discovery`], watch events are
//! bare wakeups and all state is re-derived from the stores on each one.
//!
//! A seventh reflector, on core `Service`, is not an input to [`resolve`] and
//! does not hold the publish gate (see [`kinference_policy::services`]). Its output is
//! attached to the published snapshot ([`PolicyIndex::services`]) so pools and
//! Service ports are read from one consistent instant.

use std::sync::Arc;

use futures::TryStreamExt;
use k8s_openapi::api::core::v1::Service;
use kinference_policy::crd::{
    HTTPRoute, InferencePolicy, InferencePool, InferenceProvider, InferenceWorkerSet,
    ReferenceGrant,
};
use kinference_policy::resolve::{PoolIndex, rejection_index, resolve};
use kinference_policy::services::ServiceIndex;
use kinference_policy::store::PolicyStore;
use kube::runtime::WatchStreamExt;
use kube::runtime::reflector;
use kube::runtime::watcher;
use tokio_util::sync::CancellationToken;

use crate::api::ApiServer;

/// Build the identity index from a live pool snapshot.
pub fn pool_index(pools: &kinference_fleet::pool::PoolMap) -> PoolIndex {
    pools.iter().map(|p| (p.key.clone(), p.id)).collect()
}

/// Watch the CRDs and keep [`PolicyStore`] current.
pub struct PolicyWatcher<A: ApiServer> {
    api: A,
    namespace: Option<String>,
    /// Why worker sets were refused, read on every resolve so an unresolved
    /// destination reports the actual cause rather than a guess.
    rejections: Arc<super::worker_sets::RejectionStore>,
    /// The live pool set, read on every resolve since pools come and go.
    pools: Arc<kinference_fleet::pool::PoolRegistry>,
    store: Arc<PolicyStore>,
}

impl<A: ApiServer> PolicyWatcher<A> {
    pub fn new(
        api: A,
        namespace: Option<String>,
        pools: Arc<kinference_fleet::pool::PoolRegistry>,
        rejections: Arc<super::worker_sets::RejectionStore>,
        store: Arc<PolicyStore>,
    ) -> Self {
        Self {
            api,
            namespace,
            pools,
            rejections,
            store,
        }
    }

    fn watch<K: crate::api::Object>(
        &self,
        cfg: &watcher::Config,
    ) -> futures::stream::BoxStream<'static, Result<watcher::Event<K>, watcher::Error>> {
        self.api.watch(self.namespace.as_deref(), cfg.clone())
    }

    /// Run until cancelled. Every watch event triggers a full re-resolve. When
    /// any stream ends, the whole watcher set is rebuilt; otherwise the
    /// snapshot would silently freeze.
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
    /// Publishing is gated on every kind having completed its initial list.
    /// The stores here are fresh, so resolving mid-list would publish a
    /// snapshot missing pools. Until then `PolicyStore` keeps serving the
    /// previous snapshot, so a rebuild is invisible to the pick path.
    async fn run_once(&self, cancel: &CancellationToken) {
        let (pool_store, pool_writer) = reflector::store::<InferencePool>();
        let (policy_store, policy_writer) = reflector::store::<InferencePolicy>();
        let (ws_store, ws_writer) = reflector::store::<InferenceWorkerSet>();
        let (prov_store, prov_writer) = reflector::store::<InferenceProvider>();
        let (route_store, route_writer) = reflector::store::<HTTPRoute>();
        let (grant_store, grant_writer) = reflector::store::<ReferenceGrant>();
        let (svc_store, svc_writer) = reflector::store::<Service>();

        let cfg = watcher::Config::default();
        let pools = reflector::reflector(pool_writer, self.watch(&cfg)).default_backoff();
        let policies = reflector::reflector(policy_writer, self.watch(&cfg)).default_backoff();
        let sets = reflector::reflector(ws_writer, self.watch(&cfg)).default_backoff();
        let provs = reflector::reflector(prov_writer, self.watch(&cfg)).default_backoff();
        let routes = reflector::reflector(route_writer, self.watch(&cfg)).default_backoff();
        let grants = reflector::reflector(grant_writer, self.watch(&cfg)).default_backoff();
        // Cluster-wide when the namespace is unset, so on a big cluster this
        // holds every Service. There is no label kinference owns to select on;
        // scope by namespace when the router serves only one.
        let services = reflector::reflector(svc_writer, self.watch(&cfg)).default_backoff();
        tokio::pin!(pools, policies, sets, provs, routes, grants, services);

        tracing::info!(
            namespace = ?self.namespace,
            pools = self.pools.snapshot().len(),
            "policy: watching kinference.buoyant.io CRDs + HTTPRoutes + ReferenceGrants + Services"
        );

        // One flag per kind, set when that kind finishes its initial list.
        //
        // Providers and grants are gated too: before they list, every provider
        // destination would be unresolved and every cross-namespace reference
        // retracted.
        //
        // Services are not gated, since `resolve` doesn't read them. Gating
        // them would let a missing `services` RBAC grant (a 403, so
        // `Step::Ignore` forever) disable policy routing entirely. Their
        // listed state rides on the snapshot instead (`ServiceIndex::listed`).
        let mut listed = [false; SVC + 1];
        // Resolution also depends on the pool set, which changes independently
        // of the CRDs: a worker set becomes a pool some time after its CRD
        // event. Re-resolve then, or that pool's destination stays reported
        // unresolved until the next unrelated CRD event.
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
                // A kind whose CRD is not installed counts as listed with an
                // empty store; otherwise it would hold the gate shut forever.
                // `HTTPRoute` and `ReferenceGrant` are genuinely optional. For
                // grants this fails closed: no grants means no cross-namespace
                // reference is permitted.
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
            // Service events before the Service list completes change nothing
            // published (the index is unlisted either way), so skip the
            // re-resolve rather than run one per `InitApply` at startup.
            if idx == SVC && !listed[SVC] {
                continue;
            }
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
            // Published as unlisted until listed, so readers can tell "no such
            // Service" from "not read yet".
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

/// Index of the `Service` flag in `run_once`'s `listed` array. Every kind
/// below it holds the publish gate.
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
        // The `Writer` buffers `InitApply` objects and swaps them in at
        // `InitDone`, so this is the first moment `store.state()` is complete.
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
/// Unlike a 403 or a connection reset, a 404 on the collection won't go away
/// with retries. Only the initial list and the watch start count; a `NotFound`
/// mid-watch means something else.
fn crd_not_installed(err: &watcher::Error) -> bool {
    let inner = match err {
        watcher::Error::InitialListFailed(e) | watcher::Error::WatchStartFailed(e) => e,
        _ => return false,
    };
    matches!(inner, kube::Error::Api(resp) if resp.code == 404)
}

#[cfg(test)]
mod tests;
