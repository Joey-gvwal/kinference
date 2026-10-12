//! Resolving the CRDs into the snapshot the pick path reads.
//!
//! [`resolve`] is a free function over the contents of the CRD stores, with no
//! client and no watch: `store::PolicyWatcher` calls it on every rebuild, and
//! `kinfctl check` calls it over manifests read from disk, so the CLI cannot
//! disagree with a running kinference about what a name means.
//!
//! ## Resolving destinations to pools
//!
//! By IDENTITY. An `InferenceWorkerSet` becomes a pool (see
//! `worker_sets`), so a destination naming that set is a `(namespace,
//! name)` lookup against the live pool set — see [`PoolIndex`].
//!
//! This replaced a Service-name match, which existed only while pools came from
//! TOML and a CR had to be *associated* with one. Identity is both simpler and
//! stricter: two same-named Services in different namespaces can no longer
//! collapse into one pool, and the index is rebuilt from the current pool set
//! on every resolve rather than captured at startup — closing a race where
//! policy resolved a worker set to a pool the manager had not created yet, or
//! one it had already retired.
//!
//! A miss is reported as precisely as the CR allows: a set that could never
//! have produced a pool says WHY (`UnknownProfile`, `InvalidKvBlockSize`), so
//! an operator is not sent to look at Services over a typo in TOML. Anything
//! else is `NoMatchingPool`, normally the brief window between the object
//! appearing and the reconcile running.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use kube::{Resource, ResourceExt};

use super::PoolId;
use super::compile::CompiledPolicy;
use super::crd::{
    ApiFormat, BackendType, HTTPRoute, InferencePolicy, InferencePool, InferenceProvider,
    InferenceWorkerSet, IntOrString, ModelAlias, ParentRef, ProviderModel, ReferenceGrant,
};
use super::engine::ObjectKey;
use super::services::ServiceIndex;

/// `(namespace, name)`.
pub type ObjKey = (String, String);

/// Why a destination didn't resolve. Mirrors
/// `status.unresolvedDestinations[].reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnresolvedReason {
    /// The named `InferenceWorkerSet` doesn't exist.
    NotFound,
    /// It exists, but kinference has no pool for it — so there is nothing to route
    /// to. Usually transient: the set was just created and the reconcile has
    /// not run yet. Persistent means the set was REJECTED, and the reason is
    /// one of the two below.
    NoMatchingPool,
    /// It names a routing profile the router does not define.
    ///
    /// Its own reason rather than folding into `NoMatchingPool`, because the
    /// fix is completely different: edit the ROUTER's TOML, not the CR. An
    /// operator told "no matching pool" would go looking at Services.
    UnknownProfile,
    /// Its `kvBlockSize` is missing or not a usable block size.
    InvalidKvBlockSize,
    /// `kind` was something other than `InferenceWorkerSet` or
    /// `InferenceProvider`, or was absent — which is not a default, because a
    /// destination that could be either class must say which.
    UnsupportedKind,
    /// Two destinations of DIFFERENT kinds resolved to the same name.
    ///
    /// Its own reason because it silently corrupts policy: `backends.names`
    /// selects a destination by bare string, so a worker set and a provider
    /// sharing a name would make one entry mean both, and `order` could not
    /// express a preference between them. Rejecting is the only reading that
    /// does not guess.
    AmbiguousName,
    /// A cross-namespace reference with no `ReferenceGrant` permitting it in the
    /// target namespace.
    ///
    /// For a provider's `backendRef` this is the boundary that stops one
    /// namespace spending another's vendor credential, so it fails CLOSED — a
    /// grant kinference cannot see is a grant that does not exist.
    CrossNamespaceNotGranted,
    /// The object is missing a field kinference needs. Distinct from a rejected
    /// worker set: nothing here is derived, the object simply does not say
    /// enough to route to.
    Incomplete,
    /// An `InferenceProvider` declared a wire format nothing translates to.
    ///
    /// Enforced here as well as in the CRD enum, and both are load-bearing. The
    /// schema stops a NEW object being written; this stops one that predates the
    /// narrowing — or was applied with `--validate=false` — from being routed to.
    /// The Rust `ApiFormat` deliberately keeps every variant so such an object
    /// still DESERIALIZES, because failing to parse it would fail the whole
    /// watcher list page and take provider discovery down for the namespace.
    /// Parse it, then refuse it.
    UnsupportedApiFormat,
}

impl UnresolvedReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            UnresolvedReason::NotFound => "NotFound",
            UnresolvedReason::NoMatchingPool => "NoMatchingPool",
            UnresolvedReason::UnknownProfile => "UnknownProfile",
            UnresolvedReason::InvalidKvBlockSize => "InvalidKvBlockSize",
            UnresolvedReason::UnsupportedKind => "UnsupportedKind",
            UnresolvedReason::AmbiguousName => "AmbiguousName",
            UnresolvedReason::CrossNamespaceNotGranted => "CrossNamespaceNotGranted",
            UnresolvedReason::Incomplete => "Incomplete",
            UnresolvedReason::UnsupportedApiFormat => "UnsupportedApiFormat",
        }
    }
}

/// Why a pool's `policyRef` did not resolve.
///
/// Two reasons rather than one bool, for the same purpose `UnknownProfile` is
/// separate from `NoMatchingPool`: the fixes are in different files. `NotFound`
/// sends an operator to look for a missing `InferencePolicy`; `WrongKind` sends
/// them to the `policyRef` they just wrote. Told only "does not exist", someone
/// whose policy plainly does exist has nowhere to go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DanglingPolicy {
    /// No `InferencePolicy` by that name in the pool's own namespace. A
    /// same-namespace lookup, which is why a cross-namespace ref lands here.
    NotFound { name: String },
    /// `group`/`kind` name something that is not an `kinference.buoyant.io`
    /// `InferencePolicy`.
    ///
    /// These carry schema defaults, so they read as a typed reference — and were
    /// parsed and then ignored, which meant `{kind: ConfigMap, name: x}` quietly
    /// resolved to the InferencePolicy named `x`. Either the fields mean
    /// something or they should not be in the CRD.
    WrongKind {
        name: String,
        group: String,
        kind: String,
    },
}

impl DanglingPolicy {
    pub fn name(&self) -> &str {
        match self {
            Self::NotFound { name } | Self::WrongKind { name, .. } => name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Unresolved {
    pub kind: String,
    pub name: String,
    /// UID of the offending object, when one exists to point at. `None` for a
    /// destination naming something that is not there at all. Internal — the
    /// status writer uses it as a cache key; `status.unresolvedDestinations`
    /// does not carry it.
    pub uid: Option<String>,
    pub reason: UnresolvedReason,
}

/// An `InferenceWorkerSet` that resolved to a kinference pool.
#[derive(Debug, Clone)]
pub struct ResolvedWorkerSet {
    /// The worker set's own name — this is `backend.name` in policy predicates.
    pub name: String,
    pub namespace: String,
    /// Object labels, for `backends.selector.matchLabels`.
    pub labels: BTreeMap<String, String>,
    /// The kinference pool whose `discovery.service_name` matched `serviceRef.name`.
    pub pool_id: PoolId,
    pub service_name: String,
    /// `serviceRef.port` as written — a name, a number, or absent. Carried
    /// unresolved because the answer lives on the Service, which is watched
    /// outside the publish gate: resolving it here would either hold the gate
    /// or bake a `None` into every snapshot taken before the Service watch
    /// listed. [`super::services::ServiceIndex::port_number`] resolves it, in
    /// THIS set's namespace, for the consumer that needs a number.
    pub service_port: Option<IntOrString>,
    /// Whether `spec.kvEvents` was declared. Reported as
    /// `status.kvEvents.configured`; kinference cannot infer it from runtime state,
    /// since a set with no publisher looks identical to one whose publisher is
    /// merely idle.
    pub kv_events_declared: bool,
    /// `spec.models` aliases. Empty means every discovered model is routable
    /// under the exact name `/v1/models` reports.
    pub aliases: Vec<ModelAlias>,
}

impl ResolvedWorkerSet {
    /// Upstream model names this set could serve for a canonical (client-facing)
    /// name.
    ///
    /// Aliases are ADDITIVE, not a whitelist: the canonical name itself stays
    /// routable (the guide's "omit `models` and every discovered model is
    /// routable under the exact string /v1/models reports" — declaring an alias
    /// for one model shouldn't hide the others). Two aliases sharing a `name`
    /// yield two upstreams, which is exactly how interchangeable upstream
    /// models become two candidates.
    ///
    /// Whether an upstream is ACTUALLY served is checked later against the
    /// worker registry; this only says what to look for.
    pub fn upstreams_for(&self, canonical: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .aliases
            .iter()
            .filter(|m| {
                m.name.as_deref() == Some(canonical) || m.aliases.iter().any(|a| a == canonical)
            })
            .filter_map(|m| m.name.clone())
            .collect();
        if !out.iter().any(|u| u == canonical) {
            out.push(canonical.to_string());
        }
        out
    }

    /// Every client-facing name declared for `upstream`, sorted.
    ///
    /// The forward direction of the alias table, and TOTAL where
    /// [`Self::canonical_for`] is partial: an upstream may have zero aliases
    /// (returns empty) or several (returns all of them). Status reporting wants
    /// this; `canonical_for` can express neither case.
    pub fn aliases_for(&self, upstream: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .aliases
            .iter()
            .filter(|m| m.name.as_deref() == Some(upstream))
            .flat_map(|m| m.aliases.iter().cloned())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Canonical name for an upstream name, inverting the alias table. Used when
    /// reporting what a pool can reach.
    pub fn canonical_for(&self, upstream: &str) -> String {
        self.aliases
            .iter()
            .find(|m| m.name.as_deref() == Some(upstream))
            .and_then(|m| m.aliases.first().cloned())
            .unwrap_or_else(|| upstream.to_string())
    }
}

/// An `InferenceProvider` that resolved far enough to route to.
///
/// "Far enough" is deliberately shallow: a `backendRef` kinference is willing to hand
/// out, an `api`, and a model table. The `GatewayBackend` itself is never read,
/// so this resolving says nothing about whether the destination WORKS — only that
/// the InferenceProvider is well-formed and permitted to name that backend. The
/// rest arrives as caller feedback.
#[derive(Debug, Clone)]
pub struct ResolvedProvider {
    /// The provider's own name — this is `backend.name` in policy predicates.
    pub name: String,
    pub namespace: String,
    /// The object's UID.
    ///
    /// Part of the status writer's cache key, so a delete-and-recreate under the
    /// same name is a cache MISS and gets written. Name alone let a recreated
    /// object inherit the cache entry of its predecessor and sit with empty
    /// status until its content happened to change or the process restarted.
    pub uid: Option<String>,
    /// Object labels, for `backends.selector.matchLabels`.
    pub labels: BTreeMap<String, String>,
    /// The `GatewayBackend`, fully qualified: the namespace is resolved HERE so
    /// the caller never has to default one. kinference holds no RBAC for the object
    /// and never reads it.
    pub backend_ref: ObjectKey,
    /// `spec.path`: the path the caller dispatches on instead of the one the
    /// client sent. `None` leaves the request line alone.
    ///
    /// Carried through unvalidated beyond emptiness, for the same reason the
    /// `GatewayBackend` is: kinference has no URL to check it against and never
    /// dispatches, so a path that 404s looks exactly like one that works until
    /// the caller reports otherwise.
    pub path: Option<String>,
    /// Declared model table. Required on the CR, because there is no
    /// `/v1/models` to poll.
    pub models: Vec<ProviderModel>,
}

impl ResolvedProvider {
    /// `(name, context_window)` for a requested name — one entry per
    /// declaration, so two rows sharing a `name` yield two candidates exactly as
    /// two worker-set aliases would.
    pub fn upstreams_for(&self, canonical: &str) -> Vec<(String, Option<u32>)> {
        self.models
            .iter()
            .filter(|m| {
                // The vendor's own name matches without an alias, so a row with
                // no aliases is still routable. Aliases ADD names; they never
                // replace the one the body will carry.
                m.name.as_deref() == Some(canonical) || m.aliases.iter().any(|a| a == canonical)
            })
            .filter_map(|m| m.name.clone().map(|n| (n, m.context_window)))
            .collect()
    }

    /// Every client-facing name this provider serves, sorted. The provider
    /// counterpart of [`ResolvedWorkerSet::aliases_for`], for status reporting.
    pub fn served_names(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .models
            .iter()
            .filter_map(|m| m.name.clone())
            .chain(self.models.iter().flat_map(|m| m.aliases.iter().cloned()))
            .collect();
        out.sort();
        out.dedup();
        out
    }
}

/// A destination a pool lists, of either class.
///
/// The two are NOT interchangeable behind a trait, and the enum is the point:
/// almost every routing signal exists for one and not the other, so code that
/// handles a destination has to say which case it is in. A trait with
/// `Option`-returning accessors would let a caller forget.
#[derive(Debug, Clone)]
pub enum ResolvedDestination {
    Local(ResolvedWorkerSet),
    Provider(ResolvedProvider),
}

impl ResolvedDestination {
    /// The name a policy's `backends.names` or `order` would use.
    pub fn name(&self) -> &str {
        match self {
            Self::Local(w) => &w.name,
            Self::Provider(p) => &p.name,
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            Self::Local(w) => &w.namespace,
            Self::Provider(p) => &p.namespace,
        }
    }

    pub fn labels(&self) -> &BTreeMap<String, String> {
        match self {
            Self::Local(w) => &w.labels,
            Self::Provider(p) => &p.labels,
        }
    }

    pub fn backend_type(&self) -> BackendType {
        match self {
            Self::Local(_) => BackendType::Local,
            Self::Provider(_) => BackendType::Provider,
        }
    }

    pub fn as_local(&self) -> Option<&ResolvedWorkerSet> {
        match self {
            Self::Local(w) => Some(w),
            Self::Provider(_) => None,
        }
    }

    pub fn as_provider(&self) -> Option<&ResolvedProvider> {
        match self {
            Self::Provider(p) => Some(p),
            Self::Local(_) => None,
        }
    }

    /// The CR kind, for `status.unresolvedDestinations[].kind` and logs.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Local(_) => "InferenceWorkerSet",
            Self::Provider(_) => "InferenceProvider",
        }
    }
}

/// One `InferencePolicy`, compiled — whether or not anything routes through it.
///
/// Held separately from the pools because status has to reach a policy NOBODY
/// references. Reaching policies only through pools was how an unattached policy
/// ended up with a permanently blank `RULES` column: never visited, so never
/// given status, so indistinguishable from one that had failed. Worse, a policy
/// that used to be attached kept its LAST-WRITTEN count forever, so three
/// detached policies read exactly like three live ones.
#[derive(Debug, Clone)]
pub struct ResolvedPolicy {
    pub namespace: String,
    pub name: String,
    /// The object's UID.
    ///
    /// Part of the status writer's cache key, so a delete-and-recreate under the
    /// same name is a cache MISS and gets written. Name alone let a recreated
    /// object inherit the cache entry of its predecessor and sit with empty
    /// status until its content happened to change or the process restarted.
    pub uid: Option<String>,
    /// For `observedGeneration`, so an operator can tell whether the status they
    /// are reading describes the edit they just made.
    pub generation: Option<i64>,
    pub policy: Arc<CompiledPolicy>,
    /// The spec exactly as applied.
    ///
    /// Kept because `CompiledPolicy` is lossy in the direction that matters to
    /// a reader: compilation normalizes, drops what it warned about, and has no
    /// inverse. Rendering it back would show something the operator never
    /// wrote and could not diff against their manifest. Shared, not cloned —
    /// this is the same allocation the watcher already holds.
    pub spec: Arc<crate::policy::crd::InferencePolicySpec>,
    /// Pools whose `policyRef` names this policy. EMPTY means nothing routes
    /// through it — reported rather than left to be inferred from a blank column.
    ///
    /// A list rather than a bool because naming the pool is what makes the status
    /// actionable, and because a policy reached by two pools is legal. Sorted, so
    /// a status written from it compares stable across snapshots.
    pub referenced_by: Vec<String>,
    /// Names in the policy that match nothing in the pools referencing it —
    /// see [`super::lint`]. Empty when nothing references the policy: there is
    /// nothing to check its names against.
    pub reference_warnings: Vec<crate::policy::warning::PolicyWarning>,
}

impl ResolvedPolicy {
    /// Compile and name warnings together, in that order — what
    /// `status.warnings` and the policy API report.
    pub fn all_warnings(&self) -> impl Iterator<Item = &crate::policy::warning::PolicyWarning> {
        self.policy.warnings.iter().chain(&self.reference_warnings)
    }
}

/// One `InferenceWorkerSet` in scope, whether or not a pool names it.
///
/// Published for EVERY set the store lists, because everything a set's status
/// reports — its endpoints, the models its engines actually serve, whether its
/// event stream is subscribed — is a fact about the SET. None of it needs a pool
/// to be true.
///
/// Reaching sets only through pool destinations meant an unreferenced one was
/// never patched at all. Not written as zero — never written, no `status` key on
/// the object. So a set happily serving traffic looked identical to one kinference
/// had never seen, and the only IWS with a ready endpoint could be the only one
/// with a blank row.
#[derive(Debug, Clone)]
pub struct ResolvedWorkerSetEntry {
    pub namespace: String,
    pub name: String,
    /// The object's UID.
    ///
    /// Part of the status writer's cache key, so a delete-and-recreate under the
    /// same name is a cache MISS and gets written. Name alone let a recreated
    /// object inherit the cache entry of its predecessor and sit with empty
    /// status until its content happened to change or the process restarted.
    pub uid: Option<String>,
    /// For `observedGeneration`, so an operator can tell whether the status they
    /// are reading describes the edit they just made.
    pub generation: Option<i64>,
    /// `Ok` once the set bound to a live kinference pool; `Err` is why it did not.
    /// The same reason a pool naming it would have reported under
    /// `unresolvedDestinations` — except an unreferenced set is in no pool, so
    /// its own object is the only place that can be said.
    pub bound: Result<ResolvedWorkerSet, UnresolvedReason>,
    /// Pools that route to this set, sorted. Empty means no pool names it, which
    /// is NOT the same as unreachable — see `InferenceWorkerSetStatus`.
    pub referenced_by: Vec<String>,
}

/// One `InferencePool`, fully resolved.
#[derive(Debug, Clone)]
pub struct ResolvedPool {
    pub namespace: String,
    pub name: String,
    /// The object's UID.
    ///
    /// Part of the status writer's cache key, so a delete-and-recreate under the
    /// same name is a cache MISS and gets written. Name alone let a recreated
    /// object inherit the cache entry of its predecessor and sit with empty
    /// status until its content happened to change or the process restarted.
    pub uid: Option<String>,
    /// The revision this snapshot was built from, for the freshness check
    /// against `PickRequest.inference_pool.resource_version`.
    pub resource_version: String,
    pub generation: Option<i64>,
    /// Compiled policy, or the implicit serve-as-asked policy when `policyRef`
    /// is absent.
    pub policy: Arc<CompiledPolicy>,
    /// `policyRef` did not resolve, and why. The pool still serves — fail-open,
    /// with `ResolvedRefs=False` — because refusing all traffic over a typo'd
    /// policy name is worse than serving it unfiltered.
    pub dangling_policy: Option<DanglingPolicy>,
    /// Destinations kinference can actually route to, of either class. The pool is a
    /// hard outer bound: a policy cannot reach anything outside this list.
    pub destinations: Vec<ResolvedDestination>,
    pub unresolved: Vec<Unresolved>,
    /// Gateways that route to this pool, from HTTPRoutes naming it.
    pub parents: Vec<ParentRef>,
}

impl ResolvedPool {
    /// Look up a destination by the name a policy would use. Names are unique
    /// across classes — a collision is rejected at resolve time as
    /// [`UnresolvedReason::AmbiguousName`] — so this cannot be ambiguous.
    pub fn backend(&self, name: &str) -> Option<&ResolvedDestination> {
        self.destinations.iter().find(|d| d.name() == name)
    }

    /// Local destinations only, for the callers that genuinely need a pool.
    pub fn local_destinations(&self) -> impl Iterator<Item = &ResolvedWorkerSet> {
        self.destinations.iter().filter_map(|d| d.as_local())
    }
}

/// Immutable snapshot of all resolved pools.
#[derive(Debug, Default)]
pub struct PolicyIndex {
    pools: HashMap<ObjKey, Arc<ResolvedPool>>,
    /// Every `InferencePolicy` in scope, compiled, referenced or not.
    policies: Vec<Arc<ResolvedPolicy>>,
    /// Every `InferenceWorkerSet` in scope, bound or not, referenced or not.
    worker_sets: Vec<Arc<ResolvedWorkerSetEntry>>,
    /// Every `Service` in scope, carried alongside rather than folded in.
    ///
    /// Nothing above this line is derived from a Service, so it is attached by
    /// the watcher after [`resolve`] rather than passed into it — which is also
    /// why a `resolve` call in a test gets an UNLISTED index and that is
    /// correct. It travels on the snapshot so a consumer that needs both a pool
    /// and its Service's port reads one consistent instant instead of two
    /// independently-updating cells.
    services: ServiceIndex,
    /// False only on the initial `Default` snapshot, i.e. before every reflector
    /// has completed its first list.
    ///
    /// This exists so a consumer can tell "no such pool" from "I have not
    /// listed yet" — two conditions that look identical through `lookup` and
    /// call for opposite reactions. The first is operator error worth a warning;
    /// the second is expected for the first moments of process life. Without the
    /// distinction, every request arriving during startup logs a spurious
    /// "unknown InferencePool".
    listed: bool,
}

impl PolicyIndex {
    pub fn lookup(&self, namespace: &str, name: &str) -> Option<&Arc<ResolvedPool>> {
        self.pools.get(&(namespace.to_string(), name.to_string()))
    }

    /// Whether this snapshot reflects a completed list of every watched kind.
    /// A `lookup` miss is only meaningful when this is true.
    /// A listed snapshot with nothing in it — the state a cluster with no
    /// `InferencePool`s reaches, and what `/readyz` needs to distinguish from
    /// "has not looked yet".
    pub fn listed_empty() -> Self {
        Self {
            listed: true,
            ..Default::default()
        }
    }

    pub fn listed(&self) -> bool {
        self.listed
    }

    pub fn pools(&self) -> impl Iterator<Item = &Arc<ResolvedPool>> {
        self.pools.values()
    }

    /// Every policy in scope, including ones no pool references.
    pub fn policies(&self) -> impl Iterator<Item = &Arc<ResolvedPolicy>> {
        self.policies.iter()
    }

    /// Every worker set in scope, including ones no pool references and ones
    /// that bound to no pool.
    pub fn worker_sets(&self) -> impl Iterator<Item = &Arc<ResolvedWorkerSetEntry>> {
        self.worker_sets.iter()
    }

    /// The `Service` view. Check [`ServiceIndex::listed`] before reading a miss
    /// as an absence: this index is outside the publish gate, so a snapshot can
    /// be complete in every other respect while Services are still listing (or
    /// unreadable, if the `services` RBAC grant is missing).
    pub fn services(&self) -> &ServiceIndex {
        &self.services
    }

    /// Attach the `Service` view. Called by the watcher once per rebuild;
    /// separate from [`resolve`] because Services are not an input to it.
    pub fn with_services(mut self, services: ServiceIndex) -> Self {
        self.services = services;
        self
    }

    pub fn len(&self) -> usize {
        self.pools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }
}

/// Which pool a worker set became, by identity.
///
/// This replaced a Service-name match. That match existed only while pools came
/// from TOML and an `InferenceWorkerSet` had to be *associated* with one; now a
/// worker set IS a pool, so the relation is identity and the lookup is a map
/// hit. Two consequences worth stating:
///
/// - Two same-named Services in different namespaces can no longer collapse
///   into one pool, because the key carries the namespace.
/// - The index is derived from the CURRENT pool set on every resolve rather
///   than captured at startup, which closes a race where policy resolved a
///   worker set to a pool the manager had not created yet — or, worse, to one
///   it had already retired.
pub type PoolIndex = BTreeMap<super::PoolKey, PoolId>;

/// Why each worker set that produced no pool was refused, as recorded by
/// `worker_sets::desired_pools` — the only place that knows.
pub type RejectionIndex = BTreeMap<super::PoolKey, UnresolvedReason>;

/// Project the resolver's rejections into the reasons status reports.
pub fn rejection_index(rejections: &[super::rejection::Rejection]) -> RejectionIndex {
    use super::rejection::RejectReason;
    rejections
        .iter()
        .map(|r| {
            let reason = match r.reason {
                RejectReason::MissingProfile | RejectReason::UnknownProfile { .. } => {
                    UnresolvedReason::UnknownProfile
                }
                RejectReason::MissingKvBlockSize | RejectReason::InvalidKvBlockSize { .. } => {
                    UnresolvedReason::InvalidKvBlockSize
                }
                // A set with no Service has nothing to watch, which from the
                // pool's side is indistinguishable from having no pool.
                RejectReason::MissingServiceRef => UnresolvedReason::NoMatchingPool,
                // The object does not say enough to route to — the same shape as
                // a provider missing its `backendRef`, and nothing here is
                // derived from cluster state.
                // Both are "the object does not say enough to route to" — the
                // same shape as a provider missing its backendRef.
                RejectReason::ModelMissingName | RejectReason::KvEventsMissingPort => {
                    UnresolvedReason::Incomplete
                }
            };
            (r.key.clone(), reason)
        })
        .collect()
}

/// Rebuild the whole index from the current contents of the CRD stores.
///
/// Pure over its inputs, which is what lets the tests drive it with hand-seeded
/// reflector stores and no cluster.
///
/// Takes no Services: none of pool, policy, destination or status resolution
/// reads one. The snapshot's `Service` view is attached afterwards with
/// [`PolicyIndex::with_services`], so what this returns carries an unlisted,
/// empty one.
#[allow(clippy::too_many_arguments)]
pub fn resolve(
    pools: &[Arc<InferencePool>],
    policies: &[Arc<InferencePolicy>],
    worker_sets: &[Arc<InferenceWorkerSet>],
    providers: &[Arc<InferenceProvider>],
    routes: &[Arc<HTTPRoute>],
    grants: &[Arc<ReferenceGrant>],
    pools_by_key: &PoolIndex,
    rejections: &RejectionIndex,
) -> PolicyIndex {
    // Anything `resolve` produces is derived from listed stores by construction;
    // only the `Default` snapshot is un-listed.
    let mut index = PolicyIndex {
        listed: true,
        ..Default::default()
    };

    // Compile every policy in scope up front, keyed by identity.
    //
    // Two reasons, and the second is the one that matters. A policy shared by two
    // pools is compiled once instead of twice — minor. And a policy NO pool
    // references still gets compiled, so status can report it rather than leaving
    // it blank. Compiling an unattached policy is not wasted work: its warnings
    // are exactly what you want to see BEFORE attaching it.
    let mut compiled: HashMap<ObjKey, Arc<CompiledPolicy>> = HashMap::new();
    let mut referenced_by: HashMap<ObjKey, Vec<String>> = HashMap::new();
    // Which pools route to which worker set. Accumulated from the RESOLVED
    // destinations rather than from `spec.destinations`, so a set reached by a
    // selector or by the implicit "every set in this namespace" is recorded the
    // same as one named outright.
    let mut ws_referenced_by: HashMap<ObjKey, Vec<String>> = HashMap::new();
    for p in policies {
        let Some(pns) = p.namespace() else { continue };
        let key = (pns, p.name_any());
        compiled.insert(
            key,
            Arc::new(CompiledPolicy::compile(&p.name_any(), &p.spec)),
        );
    }

    for pool in pools {
        let Some(ns) = pool.namespace() else { continue };
        let name = pool.name_any();

        // --- policy -----------------------------------------------------
        //
        // Same-namespace only, which is why a policyRef pointing at another
        // namespace dangles rather than resolving.
        let (policy, dangling_policy) = match pool.spec.policy_ref.as_ref() {
            None => (
                // "Omitting it means serve every request as asked."
                Arc::new(CompiledPolicy::serve_as_asked("<implicit>")),
                None,
            ),
            Some(pref) => {
                // `group`/`kind` are checked rather than assumed. They carry
                // schema defaults, so an omitted one is the right kind by
                // construction; a WRONG one used to be read and discarded.
                let group = pref.group.as_deref().unwrap_or(POLICY_GROUP);
                let kind = pref.kind.as_deref().unwrap_or(POLICY_KIND);
                if group != POLICY_GROUP || kind != POLICY_KIND {
                    (
                        Arc::new(CompiledPolicy::serve_as_asked("<wrong-kind>")),
                        Some(DanglingPolicy::WrongKind {
                            name: pref.name.clone(),
                            group: group.to_string(),
                            kind: kind.to_string(),
                        }),
                    )
                } else {
                    match compiled.get(&(ns.clone(), pref.name.clone())) {
                        Some(p) => {
                            referenced_by
                                .entry((ns.clone(), pref.name.clone()))
                                .or_default()
                                .push(name.clone());
                            (p.clone(), None)
                        }
                        None => (
                            // Fail open: serve unfiltered rather than refuse
                            // everything over a typo, and report it in status.
                            Arc::new(CompiledPolicy::serve_as_asked("<missing>")),
                            Some(DanglingPolicy::NotFound {
                                name: pref.name.clone(),
                            }),
                        ),
                    }
                }
            }
        };

        // --- destinations -----------------------------------------------
        let mut destinations: Vec<ResolvedDestination> = Vec::new();
        let mut unresolved = Vec::new();
        // A destination's `namespace` selects which namespace to look in; it
        // defaults to the pool's own, which is why same-namespace destinations
        // need no grant.
        let sets_in = |target_ns: &str| -> Vec<&Arc<InferenceWorkerSet>> {
            worker_sets
                .iter()
                .filter(|w| w.namespace().as_deref() == Some(target_ns))
                .collect()
        };
        let provs_in = |target_ns: &str| -> Vec<&Arc<InferenceProvider>> {
            providers
                .iter()
                .filter(|p| p.namespace().as_deref() == Some(target_ns))
                .collect()
        };

        if pool.spec.destinations.is_empty() {
            // "Omit to mean every InferenceWorkerSet in this namespace" — Local
            // only, so reaching a paid provider always requires naming it.
            for ws in sets_in(&ns) {
                match bind(ws, pools_by_key, rejections) {
                    Ok(rws) => destinations.push(ResolvedDestination::Local(rws)),
                    Err(reason) => unresolved.push(Unresolved {
                        kind: "InferenceWorkerSet".into(),
                        name: ws.name_any(),
                        uid: ws.uid(),
                        reason,
                    }),
                }
            }
        } else {
            for dest in &pool.spec.destinations {
                // `kind` is required and has NO default: a destination can be
                // either class now, and guessing between a pod fleet and a paid
                // vendor is not a guess worth making.
                let kind = dest.kind.as_deref().unwrap_or("");
                if kind != "InferenceWorkerSet" && kind != "InferenceProvider" {
                    unresolved.push(Unresolved {
                        kind: if kind.is_empty() {
                            "<unset>".into()
                        } else {
                            kind.to_string()
                        },
                        name: dest.name.clone().unwrap_or_default(),
                        // A destination naming no known kind points at no object.
                        uid: None,
                        reason: UnresolvedReason::UnsupportedKind,
                    });
                    continue;
                }
                let target_ns = dest.namespace.as_deref().unwrap_or(ns.as_str());
                let label = dest.name.clone().unwrap_or_else(|| "<selector>".into());

                // Cross-namespace needs a grant in the TARGET namespace, and
                // fails closed. In single-namespace mode kinference cannot see the
                // target at all, so it cannot see a grant either — which reads
                // as "not granted", not as "assume yes".
                if target_ns != ns.as_str()
                    && !grant_permits(
                        grants,
                        target_ns,
                        "kinference.buoyant.io",
                        kind,
                        dest.name.as_deref(),
                        &ns,
                        "InferencePool",
                    )
                {
                    unresolved.push(Unresolved {
                        kind: kind.to_string(),
                        name: label,
                        // Refused before looking, so no object was resolved.
                        uid: None,
                        reason: UnresolvedReason::CrossNamespaceNotGranted,
                    });
                    continue;
                }

                let matches_selector = |labels: &BTreeMap<String, String>| {
                    dest.selector.as_ref().is_some_and(|sel| {
                        sel.match_labels
                            .iter()
                            .all(|(k, v)| labels.get(k) == Some(v))
                    })
                };

                // Exactly one of name or selector is set (admission-enforced).
                let mut any_matched = false;
                if kind == "InferenceWorkerSet" {
                    for ws in sets_in(target_ns) {
                        let hit = match &dest.name {
                            Some(n) => &ws.name_any() == n,
                            None => matches_selector(&ws.labels().clone().into_iter().collect()),
                        };
                        if !hit {
                            continue;
                        }
                        any_matched = true;
                        push_destination(
                            &mut destinations,
                            &mut unresolved,
                            bind(ws, pools_by_key, rejections).map(ResolvedDestination::Local),
                            "InferenceWorkerSet",
                            &ws.name_any(),
                            ws.uid(),
                        );
                    }
                } else {
                    for prov in provs_in(target_ns) {
                        let hit = match &dest.name {
                            Some(n) => &prov.name_any() == n,
                            None => matches_selector(&prov.labels().clone().into_iter().collect()),
                        };
                        if !hit {
                            continue;
                        }
                        any_matched = true;
                        push_destination(
                            &mut destinations,
                            &mut unresolved,
                            resolve_provider(prov, grants).map(ResolvedDestination::Provider),
                            "InferenceProvider",
                            &prov.name_any(),
                            prov.uid(),
                        );
                    }
                }
                if !any_matched {
                    unresolved.push(Unresolved {
                        kind: kind.to_string(),
                        name: label,
                        // Nothing matched, so there is no object to point at.
                        uid: None,
                        reason: UnresolvedReason::NotFound,
                    });
                }
            }
        }

        // --- parents (from HTTPRoutes naming this pool) -----------------
        let mut parents: Vec<ParentRef> = Vec::new();
        for route in routes {
            let Some(route_ns) = route.namespace() else {
                continue;
            };
            let targets_us = route.spec.rules.iter().any(|r| {
                r.backend_refs.iter().any(|b| {
                    b.is_inference_pool()
                        && b.name == name
                        // backendRef namespace defaults to the route's own.
                        && b.namespace.as_deref().unwrap_or(route_ns.as_str()) == ns.as_str()
                })
            });
            if !targets_us {
                continue;
            }
            for p in &route.spec.parent_refs {
                let already = parents.iter().any(|q| {
                    q.name == p.name
                        && q.namespace.as_deref().unwrap_or(route_ns.as_str())
                            == p.namespace.as_deref().unwrap_or(route_ns.as_str())
                });
                if !already {
                    let mut p = p.clone();
                    // Default the parent's namespace to the route's, so status
                    // identifies the Gateway unambiguously.
                    if p.namespace.is_none() {
                        p.namespace = Some(route_ns.clone());
                    }
                    parents.push(p);
                }
            }
        }

        for rws in destinations.iter().filter_map(|d| d.as_local()) {
            ws_referenced_by
                .entry((rws.namespace.clone(), rws.name.clone()))
                .or_default()
                .push(name.clone());
        }

        let resolved = ResolvedPool {
            namespace: ns.clone(),
            name: name.clone(),
            uid: pool.uid(),
            resource_version: pool.resource_version().unwrap_or_default(),
            generation: pool.meta().generation,
            policy,
            dangling_policy,
            destinations,
            unresolved,
            parents,
        };
        index.pools.insert((ns, name), Arc::new(resolved));
    }

    // Publish every policy, referenced or not. Sorted so the snapshot is stable —
    // the status writer's last-written comparison is what breaks the write loop,
    // and an unstable ordering would make it miss.
    let resolved_policies: Vec<Arc<ResolvedPolicy>> = policies
        .iter()
        .filter_map(|p| {
            let namespace = p.namespace()?;
            let name = p.name_any();
            let key = (namespace.clone(), name.clone());
            let mut referenced_by = referenced_by.remove(&key).unwrap_or_default();
            referenced_by.sort();
            referenced_by.dedup();
            // Checked against exactly the pools that route through this policy:
            // a name is only wrong relative to where the policy is used.
            let attached: Vec<&ResolvedPool> = referenced_by
                .iter()
                .filter_map(|pool| index.pools.get(&(namespace.clone(), pool.clone())))
                .map(Arc::as_ref)
                .collect();
            let reference_warnings =
                crate::policy::lint::reference_warnings(&p.spec, &attached).warnings;
            Some(Arc::new(ResolvedPolicy {
                namespace,
                name,
                uid: p.uid(),
                generation: p.meta().generation,
                policy: compiled.get(&key)?.clone(),
                spec: Arc::new(p.spec.clone()),
                referenced_by,
                reference_warnings,
            }))
        })
        .collect();
    index.policies = resolved_policies;
    index
        .policies
        .sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

    // Publish every worker set, bound or not, referenced or not. Same sort
    // rationale as the policies above: the status writer breaks its write loop
    // on a last-written comparison, and an unstable order would defeat it.
    index.worker_sets = worker_sets
        .iter()
        .filter_map(|w| {
            let namespace = w.namespace()?;
            let name = w.name_any();
            let key = (namespace.clone(), name.clone());
            let mut referenced_by = ws_referenced_by.remove(&key).unwrap_or_default();
            referenced_by.sort();
            referenced_by.dedup();
            Some(Arc::new(ResolvedWorkerSetEntry {
                namespace,
                name,
                uid: w.uid(),
                generation: w.meta().generation,
                bound: bind(w, pools_by_key, rejections),
                referenced_by,
            }))
        })
        .collect();
    index
        .worker_sets
        .sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    index
}

/// Find the pool a worker set became.
///
/// A miss reports the reason the RESOLVER recorded, not one re-derived here.
/// That matters for `UnknownProfile` in particular: whether a profile exists is
/// a fact about the router's own TOML, which this module cannot see. Deriving
/// it here would mean either duplicating the profile list — two sources that
/// can drift — or reporting the generic `NoMatchingPool` and sending an
/// operator to inspect Services over a typo in a config file.
///
/// No recorded reason means `NoMatchingPool`, which is normally the brief
/// window between the object appearing and the reconcile running.
fn bind(
    ws: &Arc<InferenceWorkerSet>,
    pools_by_key: &PoolIndex,
    rejections: &RejectionIndex,
) -> Result<ResolvedWorkerSet, UnresolvedReason> {
    let namespace = ws.namespace().unwrap_or_default();
    let key = super::PoolKey::namespaced(&namespace, ws.name_any());
    let pool_id = *pools_by_key.get(&key).ok_or_else(|| {
        rejections
            .get(&key)
            .copied()
            .unwrap_or(UnresolvedReason::NoMatchingPool)
    })?;
    Ok(ResolvedWorkerSet {
        name: ws.name_any(),
        namespace,
        labels: ws.labels().clone().into_iter().collect(),
        pool_id,
        service_name: ws.spec.service_ref.name.clone(),
        service_port: ws.spec.service_ref.port.clone(),
        kv_events_declared: ws.spec.kv_events.is_some(),
        aliases: ws.spec.models.clone(),
    })
}

/// Add a resolved destination, or record why it did not resolve.
///
/// Two rejections happen here rather than at the call sites, because both are
/// about the destination LIST and not about any one object: a selector can match
/// something a previous entry already added, and two entries of different kinds
/// can land on the same name.
fn push_destination(
    destinations: &mut Vec<ResolvedDestination>,
    unresolved: &mut Vec<Unresolved>,
    resolved: Result<ResolvedDestination, UnresolvedReason>,
    kind: &str,
    name: &str,
    uid: Option<String>,
) {
    let reason = match resolved {
        Ok(d) => {
            match destinations.iter().find(|e| e.name() == d.name()) {
                // Same object reached twice — by name and by a selector, say.
                // Idempotent, so drop the duplicate silently.
                Some(e) if e.kind() == d.kind() => return,
                // Different KINDS under one name. A policy selects a destination
                // by bare string, so this would make `backends.names: [x]` mean
                // both and leave `order` unable to express a preference. Neither
                // is admitted.
                Some(_) => UnresolvedReason::AmbiguousName,
                None => {
                    destinations.push(d);
                    return;
                }
            }
        }
        Err(reason) => reason,
    };
    unresolved.push(Unresolved {
        kind: kind.to_string(),
        name: name.to_string(),
        uid,
        reason,
    });
}

/// Resolve an `InferenceProvider` to the shallow form kinference routes on.
///
/// Everything required by the schema is `Option` here for the reason given on
/// `InferenceProviderSpec`, so this is where required-ness is actually enforced.
/// A provider missing its `backendRef`, its `api` or a usable model row is
/// `Incomplete` — reported on the pool AND as a condition on the provider,
/// because the operator is looking at the provider.
///
/// Note what is NOT checked: whether the `GatewayBackend` exists, whether its
/// Secret has the named key, whether the vendor will accept the credential.
/// kinference cannot see any of that. It arrives as `Abort(BACKEND_*)`.
fn resolve_provider(
    prov: &Arc<InferenceProvider>,
    grants: &[Arc<ReferenceGrant>],
) -> Result<ResolvedProvider, UnresolvedReason> {
    let namespace = prov.namespace().unwrap_or_default();
    let backend = prov
        .spec
        .backend_ref
        .as_ref()
        .ok_or(UnresolvedReason::Incomplete)?;
    let backend_name = backend.name.clone().ok_or(UnresolvedReason::Incomplete)?;
    let api = prov.spec.api.ok_or(UnresolvedReason::Incomplete)?;
    // OpenAI-compatible only. Accepting another value would mean handing the
    // caller a format nothing translates to, which fails every request routed
    // here — so refuse the destination instead of producing candidates for it.
    if api != ApiFormat::OpenAI {
        return Err(UnresolvedReason::UnsupportedApiFormat);
    }

    // The group/kind the ref points at, defaulted as the CRD documents.
    let group = backend.group.as_deref().unwrap_or(GATEWAY_BACKEND_GROUP);
    let kind = backend.kind.as_deref().unwrap_or(GATEWAY_BACKEND_KIND);

    // Resolve the namespace HERE, so the pick response is always fully qualified
    // and the caller never defaults one of its own.
    let backend_ns = backend
        .namespace
        .clone()
        .unwrap_or_else(|| namespace.clone());

    // The credential boundary. A grant is in the TARGET namespace and describes
    // the reference rather than the referent, which is exactly why kinference can
    // enforce this without reading — or holding any access to — the backend
    // itself. The gateway could not: it does not know this object exists.
    if backend_ns != namespace
        && !grant_permits(
            grants,
            &backend_ns,
            group,
            kind,
            Some(backend_name.as_str()),
            &namespace,
            "InferenceProvider",
        )
    {
        return Err(UnresolvedReason::CrossNamespaceNotGranted);
    }

    // At least one usable row, or there is nothing this provider can serve.
    let models: Vec<ProviderModel> = prov
        .spec
        .models
        .iter()
        // `aliases` may be empty — the vendor's own name is always routable — so
        // a usable row is one that says what the vendor calls the model.
        .filter(|m| m.name.is_some())
        .cloned()
        .collect();
    if models.is_empty() {
        return Err(UnresolvedReason::Incomplete);
    }

    Ok(ResolvedProvider {
        name: prov.name_any(),
        namespace,
        uid: prov.uid(),
        labels: prov.labels().clone().into_iter().collect(),
        backend_ref: ObjectKey::new(backend_ns, backend_name),
        // An empty string is not a path, and the wire spells "no rewrite" as
        // the empty field either way — so it collapses to `None` here.
        // Everything else goes through verbatim: the schema's `^/` is the only
        // gate, and a provider is never REFUSED over its path, since a
        // destination that would otherwise serve is worth more than the typo it
        // was applied with.
        path: prov
            .spec
            .path
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string),
        models,
    })
}

pub(crate) const POLICY_GROUP: &str = "kinference.buoyant.io";
pub(crate) const POLICY_KIND: &str = "InferencePolicy";
pub(crate) const GATEWAY_BACKEND_GROUP: &str = "gateway.linkerd.io";
pub(crate) const GATEWAY_BACKEND_KIND: &str = "GatewayBackend";

/// Whether a `ReferenceGrant` in `target_ns` permits `from_kind` objects in
/// `from_ns` to reference `(group, kind, name)` there.
///
/// `from_kind` is the kind that holds the reference, as the Gateway API defines
/// `from`: `InferencePool` for a pool's destination, `InferenceProvider` for a
/// provider's `backendRef`. A grant for one does not cover the other — the
/// owner of the target namespace said which.
///
/// Fails closed by construction: it answers only from grants kinference can see, and
/// an unseen grant is indistinguishable from an absent one. That is the correct
/// bias for a check whose whole purpose is to stop one namespace spending
/// another's credential.
fn grant_permits(
    grants: &[Arc<ReferenceGrant>],
    target_ns: &str,
    group: &str,
    kind: &str,
    name: Option<&str>,
    from_ns: &str,
    from_kind: &str,
) -> bool {
    grants
        .iter()
        .filter(|g| g.namespace().as_deref() == Some(target_ns))
        .any(|g| {
            let from_ok = g.spec.from.iter().any(|f| {
                f.namespace.as_deref() == Some(from_ns)
                    && f.group.as_deref().unwrap_or("kinference.buoyant.io")
                        == "kinference.buoyant.io"
                    && f.kind.as_deref() == Some(from_kind)
            });
            // `to[].name` absent means every object of that kind in this
            // namespace — the grant-api default, not a wildcard we invented.
            let to_ok = g.spec.to.iter().any(|t| {
                t.group.as_deref().unwrap_or("") == group
                    && t.kind.as_deref() == Some(kind)
                    && match (&t.name, name) {
                        (None, _) => true,
                        (Some(n), Some(want)) => n == want,
                        (Some(_), None) => false,
                    }
            });
            from_ok && to_ok
        })
}

#[cfg(test)]
mod tests;
