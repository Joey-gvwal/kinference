//! The controller side: publishing a `BackendBinding` per `InferencePool`.
//!
//! A gateway cannot resolve an `InferencePool` backendRef — it has no schema
//! for the kind and never reads the object. What it does understand is a
//! `BackendBinding` (`gateway.linkerd.io/v1alpha1`): an object that CLAIMS a
//! foreign `(group, kind, name)` and states what that ref resolves to in native
//! kinds. kinference already resolves every pool down to Services and
//! `GatewayBackend`s in order to route, so it is the only component that can
//! write that translation down — and, being the router-controller for the kind,
//! the one that should.
//!
//! One binding per pool, in the pool's own namespace, named after it. The
//! namespace is not a convenience: the gateway matches a claim against route
//! refs in the BINDING's namespace, and a route naming a pool defaults the
//! ref's namespace to its own — so a binding anywhere else answers for nothing.
//!
//! What each destination becomes:
//!
//! - an `InferenceWorkerSet` — its `serviceRef` name, as a core `Service`, on
//!   the port that ref resolves to.
//! - an `InferenceProvider` — its `backendRef`, as a `GatewayBackend`, with no
//!   port: that one is in the backend's `baseURL`, and the schema REJECTS a ref
//!   that also gives one.
//!
//! The Service port is read off the Service itself
//! ([`super::services::ServiceIndex`]) rather than passed through from
//! `serviceRef.port`, because the two are not the same question: the ref may
//! name a port (`http`, the legible spelling a worker set should use) or omit
//! one entirely, and only `spec.ports[]` turns either into the number a
//! backendRef must carry. A ref kinference cannot resolve to a number is LEFT OUT of
//! the binding with a warning naming the reason, rather than written portless:
//! a portless Service ref resolves to no endpoints, so it would be an entry
//! that programs a route and then fails every request through it, and rather
//! than guessed at, because guessing sends chat traffic to the metrics port.
//! For the same reason nothing is published at all until the Service watch has
//! listed — an empty index cannot tell a missing Service from an unread one,
//! and a tick that acted on it would strip the port off every binding in the
//! cluster.
//!
//! The binding also lists an `Extension`
//! (`[policy] backend_binding_extension`, `chat-picker` by default), which the
//! gateway prepends to the filter chain of every rule that routes to the pool;
//! that extension asks kinference per request and names the endpoint. The refs say
//! WHICH backends the pool spans and where to dial them; the extension says
//! which one this request goes to.
//!
//! Destinations come from the resolved snapshot rather than from
//! `spec.destinations`, so a binding lists exactly what kinference itself would pick
//! among: a worker set that bound to no pool, a provider missing its
//! `backendRef` and a destination naming nothing are all absent here for the
//! same reason they produce no candidate. A pool whose destinations ALL fail
//! keeps an empty binding rather than losing it — the gateway then reports
//! `Accepted=False` on an object that exists, which is a far shorter path to
//! the cause than a ref that resolves through nothing.
//!
//! Two hazards, and neither is the status writer's:
//!
//! 1. **Clobbering.** These are ordinary objects in a user's namespace. A
//!    binding this writer did not create is never touched, whatever its name.
//! 2. **Write amplification.** Every write is compared against the LIVE object
//!    listed this tick, not against a private record of what we last wrote, so
//!    an out-of-band edit is corrected rather than cached over.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{DeleteParams, ListParams, ObjectMeta, Patch, PatchParams};
use kube::{Api, Resource, ResourceExt};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::crd::{
    BackendBinding, BackendBindingSpec, BindingBackendRef, BindingClaim, BindingObjectReference,
    InferencePool,
};
use super::services::{ServiceIndex, UnresolvedPort};
use super::store::{
    GATEWAY_BACKEND_GROUP, GATEWAY_BACKEND_KIND, POLICY_GROUP, PolicyStore, ResolvedDestination,
    ResolvedPool,
};

/// Server-side-apply field manager. Every field this writer sets is owned by
/// this name, so a field it STOPS setting is removed on the next apply — which
/// is what makes "keep in sync" mean removal as well as addition.
const FIELD_MANAGER: &str = "kinference";

/// Marks a binding as this writer's. Its absence is what protects an operator's
/// own binding from being adopted, overwritten or pruned.
const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
const MANAGED_BY_VALUE: &str = "kinference";

/// The pool a binding was derived from. Redundant with `spec.claim.name` and
/// worth the duplication: it is what makes `kubectl get backendbindings -l` a
/// usable question.
const POOL_LABEL: &str = "kinference.buoyant.io/inference-pool";

/// The kind bindings written here claim. The group is [`POLICY_GROUP`], shared
/// by every `kinference.buoyant.io` kind.
const POOL_KIND: &str = "InferencePool";

/// A pool's desired binding, plus the destinations that did not make it into
/// it.
///
/// The two travel together because they come from one pass over the pool and
/// mean nothing apart: a ref missing from `binding` is only intelligible
/// alongside the reason it is missing, and the reason is only actionable while
/// the binding it belongs to is in hand.
pub struct Desired {
    pub binding: BackendBinding,
    /// `(destination name, why)` for every worker set left out because its
    /// Service port did not resolve. Reported, never written: see the module
    /// header on why a portless ref is worse than an absent one.
    pub omitted: Vec<(String, UnresolvedPort)>,
}

/// The `BackendBinding` a pool should have, metadata included.
///
/// `extension` is the picker `Extension` the operator configured, `None` when
/// they cleared it. `services` supplies the Service port numbers; pass a
/// [`ServiceIndex`] that has listed, or every worker set comes back omitted as
/// [`UnresolvedPort::NotListed`].
///
/// Pure over the resolved pool and the index, so the whole translation is
/// testable without a cluster; only applying it is async.
pub fn desired_binding(
    pool: &ResolvedPool,
    extension: Option<&str>,
    services: &ServiceIndex,
) -> Desired {
    let mut backend_refs = Vec::with_capacity(pool.destinations.len());
    let mut omitted = Vec::new();

    for dest in &pool.destinations {
        match dest {
            // A worker set is a Service — group and kind stay unset, which IS
            // the core Service in a backendRef.
            //
            // The Service is looked up in the WORKER SET's namespace, not the
            // pool's: a cross-namespace destination names a Service beside
            // itself, and resolving the ref against the pool's namespace would
            // read a same-named Service in the wrong place.
            ResolvedDestination::Local(ws) => {
                let port = match services.port_number(
                    &ws.namespace,
                    &ws.service_name,
                    ws.service_port.as_ref(),
                ) {
                    Ok(port) => port,
                    Err(why) => {
                        omitted.push((ws.name.clone(), why));
                        continue;
                    }
                };
                backend_refs.push(BindingBackendRef {
                    name: ws.name.clone(),
                    target: BindingObjectReference {
                        group: None,
                        kind: None,
                        name: ws.service_name.clone(),
                        namespace: qualify(&pool.namespace, &ws.namespace),
                        port: Some(port),
                    },
                });
            }
            // A provider is the `GatewayBackend` it names. kinference never reads
            // that object — it holds the vendor credential — and does not need
            // to: the binding hands the gateway a reference, and the gateway
            // resolves it.
            ResolvedDestination::Provider(prov) => backend_refs.push(BindingBackendRef {
                name: prov.name.clone(),
                target: BindingObjectReference {
                    group: Some(GATEWAY_BACKEND_GROUP.to_string()),
                    kind: Some(GATEWAY_BACKEND_KIND.to_string()),
                    name: prov.backend_ref.name.clone(),
                    namespace: qualify(&pool.namespace, &prov.backend_ref.namespace),
                    port: None,
                },
            }),
        }
    }

    let binding = BackendBinding {
        metadata: ObjectMeta {
            name: Some(binding_name(pool)),
            namespace: Some(pool.namespace.clone()),
            labels: Some(BTreeMap::from([
                (MANAGED_BY_LABEL.to_string(), MANAGED_BY_VALUE.to_string()),
                (POOL_LABEL.to_string(), pool.name.clone()),
            ])),
            // Garbage collection does the deleting whenever the API server can:
            // the binding is meaningless without the pool it translates. The
            // reconcile's own pruning is the fallback for the pool whose UID
            // the snapshot never carried.
            owner_references: pool.uid.as_ref().map(|uid| {
                vec![OwnerReference {
                    api_version: InferencePool::api_version(&()).to_string(),
                    kind: InferencePool::kind(&()).to_string(),
                    name: pool.name.clone(),
                    uid: uid.clone(),
                    controller: Some(true),
                    // Not set: blocking the pool's deletion on this object
                    // would need `pools/finalizers`, and a binding that
                    // outlives its pool for one GC cycle harms nothing.
                    block_owner_deletion: None,
                }]
            }),
            ..Default::default()
        },
        spec: BackendBindingSpec {
            claim: BindingClaim {
                group: POLICY_GROUP.to_string(),
                kind: POOL_KIND.to_string(),
                name: pool.name.clone(),
            },
            backend_refs,
            // The picker. One entry, or none where the operator cleared the
            // name — and the empty list is as meaningful as the populated one:
            // this writer owns the field under server-side apply, so clearing
            // the config removes the extension from every binding rather than
            // leaving the old name behind.
            extensions: extension.map(|e| vec![e.to_string()]).unwrap_or_default(),
        },
        status: None,
    };

    Desired { binding, omitted }
}

/// The binding's name: the pool's own.
///
/// One binding per pool and pool names are unique in a namespace, so this
/// cannot collide with another pool's. It CAN collide with an operator's own
/// binding of the same name, which is why every write checks ownership first.
pub fn binding_name(pool: &ResolvedPool) -> String {
    pool.name.clone()
}

/// A target's namespace, omitted when it is the binding's own — the default the
/// CRD documents. Writing it out anyway would work, but it makes two identical
/// bindings compare unequal depending on how the destination was spelled.
fn qualify(binding_ns: &str, target_ns: &str) -> Option<String> {
    (binding_ns != target_ns).then(|| target_ns.to_string())
}

/// Whether this writer created the binding. Only these are updated or pruned.
fn is_ours(binding: &BackendBinding) -> bool {
    binding.labels().get(MANAGED_BY_LABEL).map(String::as_str) == Some(MANAGED_BY_VALUE)
}

/// Whether the live object already says what it should.
///
/// Compares the spec, the two labels and the owner reference — everything this
/// writer sets. Anything else on the object (an operator's annotation, another
/// controller's field) is not ours and not compared, so it never provokes a
/// write.
fn in_sync(current: &BackendBinding, desired: &BackendBinding) -> bool {
    let spec_matches =
        serde_json::to_value(&current.spec).ok() == serde_json::to_value(&desired.spec).ok();
    let labels_match = desired
        .labels()
        .iter()
        .all(|(k, v)| current.labels().get(k) == Some(v));
    let owners_match = match desired.metadata.owner_references.as_ref() {
        // Compare by UID: a recreated pool is a different owner under the same
        // name, and a binding still pointing at the dead one is GC'd out from
        // under us the moment the API server notices.
        Some(want) => want.iter().all(|w| {
            current
                .metadata
                .owner_references
                .iter()
                .flatten()
                .any(|c| c.uid == w.uid)
        }),
        None => true,
    };
    spec_matches && labels_match && owners_match
}

/// Creates, updates and prunes the `BackendBinding`s, on a tick.
pub struct BindingWriter {
    client: kube::Client,
    /// Watch scope, mirroring `PolicyWatcher`'s: `None` is every namespace.
    /// Bindings are listed in the same scope the pools were resolved in, or
    /// pruning would judge a binding against a snapshot that never contained
    /// its pool.
    namespace: Option<String>,
    store: Arc<PolicyStore>,
    /// The picker `Extension` every published binding lists. `None` where the
    /// operator cleared it; see `PolicyConfig::backend_binding_extension`.
    extension: Option<String>,
    /// Whether this replica may write. Same gate as the status writer, for the
    /// same reason: N writers of one object thrash it.
    leader: watch::Receiver<bool>,
    /// Conditions already reported, so a standing complaint is logged once
    /// rather than every tick. Replaced wholesale each reconcile, which is what
    /// lets a resolved-then-recurring problem be reported twice.
    warned: Mutex<HashSet<String>>,
}

impl BindingWriter {
    pub fn new(
        client: kube::Client,
        namespace: Option<String>,
        store: Arc<PolicyStore>,
        extension: Option<String>,
        leader: watch::Receiver<bool>,
    ) -> Self {
        Self {
            client,
            namespace,
            store,
            extension,
            leader,
            warned: Mutex::new(HashSet::new()),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("backend binding writer: cancelled");
                    return;
                }
                r = self.leader.changed() => {
                    match r {
                        Ok(()) => {
                            if *self.leader.borrow_and_update() {
                                tracing::info!(
                                    "backend binding writer: promoted to leader; \
                                     publishing bindings from the next tick"
                                );
                            } else {
                                tracing::info!(
                                    "backend binding writer: demoted; stopping writes"
                                );
                            }
                        }
                        // Elector gone. Hold the last known state rather than
                        // assuming either way — the same call the status
                        // writer makes, and for the same reasons.
                        Err(_) => tracing::error!(
                            "backend binding writer: leader channel closed; \
                             holding the last known leadership state"
                        ),
                    }
                }
                _ = interval.tick() => {
                    if !*self.leader.borrow() {
                        continue;
                    }
                    if let Err(e) = self.reconcile_once().await {
                        // Never fatal. A pool that cannot be published as a
                        // binding still routes for anything already programmed.
                        tracing::warn!(error = %e, "backend binding writer: reconcile failed");
                    }
                }
            }
        }
    }

    fn api(&self) -> Api<BackendBinding> {
        match self.namespace.as_deref() {
            Some(ns) => Api::namespaced(self.client.clone(), ns),
            None => Api::all(self.client.clone()),
        }
    }

    async fn reconcile_once(&self) -> anyhow::Result<()> {
        let snapshot = self.store.snapshot();
        // Before the first complete list every pool looks deleted, and pruning
        // on that view would delete every binding in the cluster.
        if !snapshot.listed() {
            return Ok(());
        }

        // The Service watch rides outside the publish gate (see
        // `super::services`), so a snapshot can be complete while the index is
        // not. Writing on that view would publish every binding portless and
        // then correct it a tick later — a route programmed against no
        // endpoints in between — so hold instead, and say so: this is also what
        // a missing `services` RBAC grant looks like, and nothing else in kinference
        // would report it.
        if !snapshot.services().listed() {
            self.warn_new(HashSet::from([
                "backend bindings: waiting for the Service watch to list before \
                 publishing; a backendRef needs the Service's port (if this does \
                 not clear, check kinference's `services` get/list/watch grant)"
                    .to_string(),
            ]));
            return Ok(());
        }

        let mut port_warnings: HashSet<String> = HashSet::new();
        let desired: HashMap<(String, String), BackendBinding> = snapshot
            .pools()
            .map(|pool| {
                let Desired { binding, omitted } =
                    desired_binding(pool, self.extension.as_deref(), snapshot.services());
                for (dest, why) in omitted {
                    // Named per destination rather than per pool: the fix is a
                    // field on one worker set's `serviceRef`, and a message
                    // that only named the pool would send the operator looking
                    // through all of them.
                    port_warnings.insert(format!(
                        "backend bindings: {}/{}: destination {dest} is left out — \
                         its Service port did not resolve ({}: {why})",
                        pool.namespace,
                        pool.name,
                        why.as_str(),
                    ));
                }
                ((pool.namespace.clone(), binding_name(pool)), binding)
            })
            .collect();

        let existing = match self.api().list(&ListParams::default()).await {
            Ok(list) => list.items,
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // The CRD is not installed. Expected wherever kinference runs
                // without this gateway, and not an error — but silence would
                // leave "my routes never program" with nothing to go on.
                self.warn_new(HashSet::from([
                    "backend bindings: the BackendBinding CRD is not installed; \
                     not publishing any (install the gateway's \
                     gateway.linkerd.io CRDs, or set \
                     `[policy] backend_bindings = false`)"
                        .to_string(),
                ]));
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let mut by_key: HashMap<(String, String), &BackendBinding> = HashMap::new();
        for b in &existing {
            let Some(ns) = b.namespace() else { continue };
            by_key.insert((ns, b.name_any()), b);
        }

        let mut warnings: HashSet<String> = port_warnings;
        let mut applied = 0usize;

        for (key, want) in &desired {
            if let Some(current) = by_key.get(key) {
                if !is_ours(current) {
                    // Someone else's object under the name we want. Refuse
                    // rather than adopt: it may claim something entirely
                    // different, and taking it over would break whatever
                    // depends on it with no trace of what happened.
                    warnings.insert(format!(
                        "backend bindings: {}/{} exists and is not managed by kinference; \
                         leaving it alone — the pool's routes will not resolve until it \
                         is removed or renamed",
                        key.0, key.1
                    ));
                    continue;
                }
                if let Some(reason) = rejected_reason(current) {
                    warnings.insert(format!(
                        "backend bindings: the gateway rejected {}/{}: {reason}",
                        key.0, key.1
                    ));
                }
                if in_sync(current, want) {
                    continue;
                }
            }
            let api: Api<BackendBinding> = Api::namespaced(self.client.clone(), &key.0);
            // Server-side apply, so create and update are one path and the
            // fields this writer owns are exactly the ones it sets. `force`
            // takes back a field another manager grabbed — the alternative is a
            // conflict error every ten seconds and a binding that never
            // converges.
            match api
                .patch(
                    &key.1,
                    &PatchParams::apply(FIELD_MANAGER).force(),
                    &Patch::Apply(want),
                )
                .await
            {
                Ok(_) => {
                    applied += 1;
                    tracing::debug!(namespace = %key.0, name = %key.1, "backend binding: applied");
                }
                Err(e) => {
                    // Logged, not returned: one namespace's RBAC gap must not
                    // stop the rest of the cluster from being published.
                    tracing::warn!(
                        namespace = %key.0,
                        name = %key.1,
                        error = %e,
                        "backend binding: apply failed"
                    );
                }
            }
        }

        // Prune. Owner-reference GC already covers the deleted pool wherever
        // the UID was known; this catches the rest — a pool that left the watch
        // scope, or one whose binding was created before its UID was.
        for binding in &existing {
            let Some(ns) = binding.namespace() else {
                continue;
            };
            let name = binding.name_any();
            if !is_ours(binding) || desired.contains_key(&(ns.clone(), name.clone())) {
                continue;
            }
            // Already on its way out; deleting again just logs noise.
            if binding.meta().deletion_timestamp.is_some() {
                continue;
            }
            let api: Api<BackendBinding> = Api::namespaced(self.client.clone(), &ns);
            match api.delete(&name, &DeleteParams::default()).await {
                Ok(_) => tracing::info!(
                    namespace = %ns,
                    name = %name,
                    "backend binding: deleted; its InferencePool is gone"
                ),
                Err(e) => tracing::warn!(
                    namespace = %ns, name = %name, error = %e,
                    "backend binding: delete failed"
                ),
            }
        }

        if applied > 0 {
            tracing::debug!(applied, "backend bindings: reconciled");
        }
        self.warn_new(warnings);
        Ok(())
    }

    /// Log the conditions that were not standing at the last reconcile, and
    /// remember the current set as the baseline for the next one.
    fn warn_new(&self, warnings: HashSet<String>) {
        let mut warned = self.warned.lock().expect("binding warn set poisoned");
        for w in &warnings {
            if !warned.contains(w) {
                tracing::warn!("{w}");
            }
        }
        *warned = warnings;
    }
}

/// The gateway's verdict on a binding, when it refused it. `None` while the
/// gateway has said nothing — which is also what a cluster with no gateway
/// looks like, and is not a problem to report.
fn rejected_reason(binding: &BackendBinding) -> Option<String> {
    let conditions = &binding.status.as_ref()?.conditions;
    let accepted = conditions.iter().find(|c| c.r#type == "Accepted")?;
    (accepted.status == "False").then(|| format!("{}: {}", accepted.reason, accepted.message))
}

/// Convenience for `main`: build the writer with its own client.
///
/// `leader` gates writing, exactly as it does for the status writer: with
/// election disabled the caller passes a channel pre-set to `true`.
pub async fn spawn(
    namespace: Option<String>,
    store: Arc<PolicyStore>,
    extension: Option<String>,
    leader: watch::Receiver<bool>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // Same client policy as the rest of the controller side: CRDs only, so a
    // kubeconfig is as good as an in-cluster token.
    let cfg = kube::Config::infer().await?;
    let client = kube::Client::try_from(cfg)?;
    let writer = BindingWriter::new(client, namespace, store, extension, leader);
    tokio::spawn(async move { writer.run(cancel).await });
    Ok(())
}

#[cfg(test)]
mod tests;
