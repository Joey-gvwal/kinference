//! CRD-driven routing policy (`kinference.buoyant.io/v1alpha1`).
//!
//! When a `Pick` names an `InferencePool`, kinference reads that pool, the
//! `InferencePolicy` it points at, and the `InferenceWorkerSet`s it lists, and
//! routes accordingly. A `Pick` with no pool is unaffected and behaves exactly
//! as it did before this module existed.
//!
//! ## How this relates to the existing picker
//!
//! An `InferencePolicy` does **not** override `PredictorConfig` /
//! `PickerConfig`. Those come from the worker set's `[[profile]]`, are
//! immutable for the pool's lifetime, and stay TOML-sourced. Policy
//! governs the layer *above* the picker — which candidates exist, which are
//! dropped, how survivors are ranked — and the only ranking v1 offers,
//! `minimize: PredictedTTFT`, is precisely what `picker::predicted_wait_ms`
//! already computes. So this composes with the existing machinery instead of
//! replacing it, and no hot-path structure changes shape.
//!
//! ## Where the topology comes from
//!
//! **The CRs.** An `InferenceWorkerSet` IS a pool: it carries its own
//! `serviceRef`, ports, `kvBlockSize` and `profile`, and
//! `worker_sets::desired_pools` turns it into a `PoolSpec` that
//! `PoolManager` materializes. A LOCAL destination therefore resolves by
//! IDENTITY — `(namespace, name)` against the live pool set — not by matching a
//! Service name against a TOML block.
//!
//! An `InferenceProvider` is the other kind of destination, and it is NOT a
//! pool. It has no Service, no endpoints, no KV events and no telemetry, because
//! it lives outside the cluster: resolving it produces a `GatewayBackend`
//! reference, a wire format and a declared model table, and the caller of `Pick`
//! dials it. kinference never reads that backend and holds no credential for it, so
//! everything it knows about how a provider is behaving arrives second-hand —
//! see [`provider_health`].
//!
//! What stays in TOML is the `[[profile]]` a set names: predictor coefficients,
//! picker policy, tokenize mode. That split is deliberate. Topology is a
//! property of the cluster and belongs in cluster objects; routing tuning is
//! the router's own calibration against hardware and workload, and putting it
//! in a CR would ask an operator to hand-edit numbers that come out of
//! `analysis/analyze-latency.py`.
//!
//! A destination that resolves to nothing is reported on the pool as an
//! `unresolvedDestinations` entry, and — when the worker set itself was
//! rejected — as an `Accepted=False` condition on the set, because that is the
//! object the operator is looking at.
//!
//! ## Layout
//!
//! - [`crd`] — the resource types. `charts/kinference-crds/templates/*.yaml` is the source of truth.
//! - `predicate` — the predicate grammar, compiled to a typed AST once per
//!   policy revision rather than interpreted per request.
//! - `compile` — `InferencePolicy` → `CompiledPolicy`, collecting the warnings
//!   the status writer surfaces.
//! - `classify` — the classifier registry and the `tag` set each request
//!   carries. The classifiers themselves are keyword STUBS, but they do
//!   inspect the request: a request with no user turn to read is tagged
//!   `pii-unknown` and `complexity-unknown`, everything else gets a real
//!   verdict. See that module's own header for the keyword table.
//! - `engine` — candidate generation, exclusion, selection.
//! - `store` — the reflectors and the resolved snapshot.
//! - `services` — the `Service` watch. Nothing in pool or policy resolution
//!   reads a Service; it is there for the things that must name one to a THIRD
//!   party, where the front-door port number is needed and only the Service
//!   carries it. See that module's header.
//! - `worker_sets` — `InferenceWorkerSet` → `PoolSpec`, plus the watch that
//!   reconciles them and the store of the ones that were rejected. Lives here
//!   rather than at the crate root because its input is a CRD type and its
//!   output is consumed only by `store` and `status`; as a top-level module it
//!   formed a cycle with them.
//! - `provider_health` — what the caller reports back about an off-cluster
//!   destination, which is the only way kinference learns anything about one.
//! - `status` — the controller side.
//! - `binding` — the other half of the controller side: one `BackendBinding`
//!   per pool, which is what lets a gateway resolve an `InferencePool`
//!   backendRef it has no schema for.

pub mod binding;
pub mod check;
pub mod classify;
pub mod compile;
pub mod crd;
pub mod engine;
pub mod explain;
pub mod lint;
pub mod predicate;
pub mod provider_health;
pub mod services;
pub mod status;
pub mod store;
pub mod warning;
pub mod worker_sets;

/// The pool a request named, lifted off the wire so `crate::monitor` stays
/// proto-agnostic (it references no generated type today, and this keeps it
/// that way).
#[derive(Debug, Clone)]
pub struct PoolRef {
    pub namespace: String,
    pub name: String,
    /// The revision the gateway held when it routed. Opaque — comparable for
    /// equality only, never ordinally.
    pub resource_version: String,
}

impl PoolRef {
    pub fn label(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// Request attributes the CALLER resolved, lifted off the wire.
///
/// kinference does not parse HTTP. It has no view of which header carries a tenant
/// id, no way to bound what a caller would hand it, and no reason to hold an
/// `Authorization` header in order to choose an endpoint — so identity arrives
/// already extracted, as typed components.
///
/// `tenant` is mandatory on the wire and has no presence bit, so `""` is the
/// only way to say "undetermined" — and it is a VALUE, matching a rule that
/// names the empty string and no other. That is what makes an exhaustive rule
/// set with `unmatched: Reject` refuse unattributed traffic instead of serving
/// it under someone else's rule.
///
/// There is deliberately no per-policy override for how this is extracted:
/// an `InferencePolicy` cannot name the header a tenant id arrives in, because
/// that resolution does not happen here. A deployment with its own header
/// conventions configures the caller.
///
/// Note also what is absent — `path`. The route already matched on it to reach
/// the pool, so matching it again in a policy would duplicate routing logic in
/// a weaker grammar.
#[derive(Debug, Clone, Default)]
pub struct RequestIdentity {
    pub tenant: String,
}
