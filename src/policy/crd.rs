//! Rust types for the `kinference.buoyant.io/v1alpha1` custom resources, plus the
//! sliver of Gateway API we need to read.
//!
//! These mirror `charts/kinference-crds/templates/*.yaml` field-for-field. **The YAML is the source of
//! truth** — it is hand-written and installed by the `kinference-crds` chart, and
//! we never generate a schema from these types. The `JsonSchema` derive exists
//! only because `kube`'s `CustomResource` macro requires it.
//!
//! Deserialization is deliberately lenient in one specific way: every predicate
//! value (`matches`, `if:`, `drop:`) arrives as untyped `serde_json::Value`,
//! because the CRD declares those fields
//! `x-kubernetes-preserve-unknown-fields: true` and the *shape* of the value is
//! what picks the comparison (scalar = equality, array = membership, hash =
//! operator). Turning that into a typed AST is `super::predicate`'s job, and it
//! happens once per policy revision — never per request.
//!
//! Statuses are modeled too, because kinference is the router-controller and writes
//! them (see `super::status`).

// These types are declared ahead of their consumers — the predicate compiler,
// engine, store and status writer land in later steps — so many are not
// constructed yet. Same convention as the config structs.
#![allow(dead_code)]

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Untyped predicate payload. See the module doc: the YAML shape selects the
/// operator, so this stays `Value` until `super::predicate` compiles it.
pub type RawPredicateValue = serde_json::Value;

// ---------------------------------------------------------------------------
// InferenceWorkerSet
// ---------------------------------------------------------------------------

/// A set of local model-server pods, named indirectly through the Service that
/// fronts them.
///
/// The Service reference (rather than a pod selector) is load-bearing: the
/// EndpointSlice controller already tracks the ready/serving/terminating
/// tri-state, and an endpoint that is `serving` but `terminating` must keep its
/// in-flight requests while producing no new candidates. kinference's existing
/// EndpointSlice discovery already keys on `ready` alone, so that behavior is
/// inherited rather than reimplemented.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kinference.buoyant.io",
    version = "v1alpha1",
    kind = "InferenceWorkerSet",
    plural = "inferenceworkersets",
    shortname = "iws",
    namespaced,
    status = "InferenceWorkerSetStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct InferenceWorkerSetSpec {
    /// The Service fronting these model servers. Same-namespace only, so no
    /// `ReferenceGrant` is involved.
    pub service_ref: ServiceRef,
    /// Which `[[profile]]` in kinference's TOML supplies this set's predictor
    /// coefficients, picker policy, and tokenize mode.
    ///
    /// **Required by the CRD schema, `Option` here** — and that asymmetry is
    /// deliberate, not backwards compatibility. `kube::runtime::watcher`
    /// deserializes list pages WHOLESALE: one object predating the schema
    /// change, or applied with `--validate=false`, fails the entire page and
    /// produces a `watcher::Error`, backoff, and retry forever. That is not
    /// "one bad object is skipped", it is discovery down for every worker set
    /// in the namespace. So parse permissively, enforce in
    /// `desired_pools`, and report violations as a status condition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// Tokens per block in the KV-cache events these engines PUBLISH.
    ///
    /// **Not necessarily the engine's `--block-size`.** Those coincide for
    /// standard vLLM (16), and do NOT for every model: Gemma 4 reports
    /// `block_size="64"` in `vllm:cache_config_info` while publishing
    /// 256-token blocks. SGLang `--page-size 1` publishes 1. Read the value off
    /// a real event rather than off the launch flag. The tell is `Block not
    /// published. Block size must be N tokens to be published. Block size is: M`
    /// — M is the truth, set this to M.
    ///
    /// A wrong value fails in two stages. Mismatched blocks are dropped before
    /// insertion, so events arrive and the tree stays EMPTY while picks degrade
    /// to load-only with no error; then after
    /// `BLOCK_SIZE_MISMATCH_EVICT_THRESHOLD` consecutive mismatched events the
    /// endpoint is evicted, and one matching event resets that count. A quiet
    /// fleet stays in the silent state — events fire on newly stored blocks, and
    /// cached prefixes store nothing — while a busy one drains.
    ///
    /// Lives on the worker set rather than the profile because it is a property
    /// of THIS fleet's engines, and kinference never adopts an observed value.
    /// Required in the schema, `Option` here, for the same
    /// wholesale-deserialization reason as [`Self::profile`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_block_size: Option<u32>,
    /// Where Prometheus metrics are served, if not on the OpenAI port.
    ///
    /// Unset derives today's behavior: the port named `metrics` if the Service
    /// has one, else `serviceRef.port`. vLLM and SGLang both serve `/metrics`
    /// on the OpenAI port, so unset is right for them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_port: Option<IntOrString>,
    /// Where these servers publish KV cache events, on the SAME Service as
    /// [`Self::service_ref`]. Optional in the schema, but absence is not a
    /// benign default: without events there is no prefix model, so
    /// `prefixCacheHitRatio` is 0 everywhere and `predictedTTFT` — the signal
    /// the whole `select` stage ranks on — loses most of its fidelity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_events: Option<KvEvents>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_discovery: Option<ModelDiscovery>,
    /// Optional aliases. Two entries sharing a `name` declare the upstream
    /// models interchangeable — which is what lets a request for one canonical
    /// name fan out into multiple candidates.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<ModelAlias>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRef {
    pub name: String,
    /// Service port NAME or number (`x-kubernetes-int-or-string`). The name is
    /// preferred; a worker typically exposes the OpenAI API, Prometheus metrics
    /// and a cache-event socket, and only the names keep those legible.
    ///
    /// This names the OpenAI port only. kinference additionally needs the KV-event
    /// ports, which it keeps resolving by name off the EndpointSlice
    /// (`kv-pub` / `kv-replay`) exactly as it does today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<IntOrString>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KvEvents {
    /// Port name or number on `serviceRef`'s Service, for the PUBLISH socket.
    ///
    /// **Required by the schema, `Option` here** — the same asymmetry as
    /// [`InferenceWorkerSetSpec::profile`], and this was the one leaf in the
    /// spec that broke it. `kube::runtime::watcher` deserializes list pages
    /// WHOLESALE, so a single object carrying `kvEvents: {}` — applied with
    /// `--validate=false`, or predating `required: [port]` — failed the entire
    /// page and took worker-set discovery down for the namespace. Parse it, then
    /// refuse it in `desired_pools` as `KvEventsMissingPort`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<IntOrString>,
    /// The engine's ZMQ event REPLAY socket, read once at startup to recover
    /// events published before the router connected.
    ///
    /// Nested here rather than sitting beside `metricsPort`, because it is the
    /// other half of THIS transport and not a separate one: it replays the very
    /// stream [`Self::port`] publishes, and it resolves from the port named
    /// `kv-replay`. Flat, it was possible to declare a replay socket on a set
    /// that publishes nothing — resolved, carried on every endpoint, and never
    /// read. Here that is unrepresentable.
    ///
    /// Unset resolves to the port named `kv-replay` if the Service has one, and
    /// otherwise disables replay: only live events are ingested, so an endpoint's
    /// existing cache contents stay invisible until they are re-stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_port: Option<IntOrString>,
}

/// Minimal stand-in for `k8s.io/apimachinery` `IntOrString`, which
/// `k8s-openapi` does expose but not with a `JsonSchema` impl.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum IntOrString {
    Int(i64),
    Str(String),
}

impl IntOrString {
    /// The port NAME, when the reference used one. `None` for a numeric port.
    pub fn as_name(&self) -> Option<&str> {
        match self {
            IntOrString::Str(s) => Some(s.as_str()),
            IntOrString::Int(_) => None,
        }
    }

    /// The port NUMBER, when the reference used one.
    pub fn as_number(&self) -> Option<u16> {
        match self {
            IntOrString::Int(n) => u16::try_from(*n).ok(),
            IntOrString::Str(s) => s.parse().ok(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelDiscovery {
    #[serde(default = "default_discovery_mode")]
    pub mode: ModelDiscoveryMode,
    /// Duration string (`30s`, `1m`). Parsed by `super::store`.
    #[serde(default = "default_models_interval")]
    pub interval: String,
}

impl Default for ModelDiscovery {
    fn default() -> Self {
        Self {
            mode: default_discovery_mode(),
            interval: default_models_interval(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum ModelDiscoveryMode {
    /// Poll `/v1/models` on each ready endpoint — what kinference already does.
    #[default]
    Endpoint,
    /// Trust `spec.models` alone; no polling.
    Static,
}

fn default_discovery_mode() -> ModelDiscoveryMode {
    ModelDiscoveryMode::Endpoint
}
fn default_models_interval() -> String {
    "30s".to_string()
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelAlias {
    /// What this set reports on `/v1/models`, and what gets written into the
    /// outbound body when this candidate wins.
    ///
    /// The row's identity AND its wire name, so the two cannot drift apart. It
    /// stays routable under itself with or without a row — declaring aliases for
    /// one model never hides the engine's own names for the others.
    ///
    /// **Required by the schema, `Option` here**, for the reason spelled out on
    /// [`InferenceWorkerSetSpec::profile`] — and this field is why that rule
    /// exists rather than being a formality. It was a bare `String`, and when
    /// the schema changed under a running router the API server pruned the field
    /// it no longer declared, every worker set in the namespace failed to
    /// deserialize as one page, and discovery went down until the binary caught
    /// up. One object should degrade one object. Parse it, then refuse it in
    /// `desired_pools` as `ModelMissingName`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Client-facing names that also resolve to [`Self::name`].
    ///
    /// Additive, never a whitelist. The same shape `InferenceProvider.models`
    /// uses, so one idea has one spelling across both kinds.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferenceWorkerSetStatus {
    /// At-a-glance state as comma-separated tokens: the primary verdict first,
    /// then any qualifier that is true.
    ///
    /// A materialized STRING, not a list, because a `[*]` printer column renders
    /// only the FIRST array element — verified against the API server — so a list
    /// would print one token and drop the rest silently. Never empty when status
    /// is written, which is what keeps a BLANK column meaning "no status at all".
    ///
    /// Exists because one condition cannot answer "how is this object doing".
    /// Reading `Accepted`'s reason into a column showed a set that was accepted
    /// and idle identically to one accepted and serving; the qualifiers are the
    /// facts that a column reading a single condition structurally cannot show.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    // NOTE on the list fields below, and on every status list in this file:
    // they deliberately do NOT carry `skip_serializing_if = "Vec::is_empty"`.
    //
    // `super::status` writes with `Patch::Merge` (JSON merge patch), where an
    // OMITTED key means "leave it as it is" — so skipping empty vecs makes
    // emptying a list unexpressible. Once populated, it could never be cleared:
    // a destination the operator fixed would stay listed as unresolved forever,
    // a model that left the fleet would linger, a policy warning would outlive
    // the mistake that caused it. Serializing `[]` is what actually clears them.
    //
    // Nested lists (`ParentStatus::conditions`, `RuleStatus`) may still skip,
    // since they ride inside a parent array that merge-patch replaces wholesale.
    #[serde(default)]
    pub models: Vec<WorkerSetModelStatus>,
    /// Pools listing this set in `spec.destinations`, sorted.
    ///
    /// EMPTY does NOT mean unreachable, and that is the distinction this field
    /// exists to make: a `Pick` naming no `InferencePool` routes across every
    /// set kinference serves, so an unreferenced set still takes traffic. What an
    /// empty list means is that no pool-scoped POLICY applies to it.
    #[serde(default)]
    pub referenced_by: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<EndpointCounts>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_events: Option<KvEventsStatus>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// Whether a prefix model actually exists for this set — otherwise invisible,
/// since a set with no event stream still serves traffic, just on a degraded
/// `predictedTTFT`.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KvEventsStatus {
    /// Mirrors `spec.kvEvents` being set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured: Option<bool>,
    /// Endpoints whose event stream kinference has actually observed traffic on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribed_endpoints: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSetModelStatus {
    /// What this set's engines report on `/v1/models` — the identity of this
    /// entry, and the only name here that was OBSERVED rather than declared.
    ///
    /// One entry per distinct model, so a set whose engines also serve LoRA
    /// adapters (or were launched with several `--served-model-name` values)
    /// reports one each, and `ready_endpoints` shows a partial rollout.
    pub upstream_name: String,
    /// Client-facing names `spec.models` maps onto this one, if any.
    ///
    /// PLURAL and secondary, which is the correction: an upstream can carry any
    /// number of aliases, and a singular field can only hold a lossy inverse
    /// lookup that silently reports the first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_endpoints: Option<i64>,
    /// EFFECTIVE window (vLLM `max_model_len`), as a range — a rollout that
    /// changes `--max-model-len` leaves endpoints disagreeing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<ContextWindow>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct ContextWindow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct EndpointCounts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<i64>,
}

// ---------------------------------------------------------------------------
// InferenceProvider
// ---------------------------------------------------------------------------

/// A destination outside the cluster: wire format and model aliasing.
///
/// The transport is deliberately somewhere else. `baseURL`, credentials and TLS
/// live on the `gateway.linkerd.io/GatewayBackend` this points at, which kinference
/// does **not** read and holds no RBAC for — it names that object by reference in
/// the pick response and the caller resolves it.
///
/// The split runs both ways: the caller understands GatewayBackends and not
/// InferenceProviders. So everything inference-specific it needs is flattened
/// into `ProviderTarget`, which buys two things worth having on purpose.
/// `kinference.buoyant.io` stays private to kinference, so this group can evolve without a
/// gateway release; and kinference sits outside the credential blast radius entirely,
/// holding no vendor key even transiently.
///
/// What follows from that is the one thing to keep in mind while reading the
/// resolution code: kinference cannot tell whether `backendRef` resolves. A dangling
/// ref, a missing Secret key and a denied `ReferenceGrant` all look identical
/// from here — healthy — until the caller reports otherwise via
/// `Abort(BACKEND_UNRESOLVED)`. That report is not an optimization; without it a
/// misconfigured provider is a silent black hole, because there is no dispatch
/// retry anywhere in this design.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kinference.buoyant.io",
    version = "v1alpha1",
    kind = "InferenceProvider",
    plural = "inferenceproviders",
    shortname = "infprov",
    namespaced,
    status = "InferenceProviderStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderSpec {
    /// The `GatewayBackend` carrying URL and credentials. Cross-namespace needs
    /// a `ReferenceGrant` in the target namespace — which kinference CAN check
    /// without reading the backend itself, since a grant lives in the target
    /// namespace and names `from`/`to` by group and kind. That is why the check
    /// belongs here and not in the gateway, which does not know this object
    /// exists.
    ///
    /// `Option` despite being schema-required: see the note on
    /// `InferenceWorkerSetSpec::profile`. One non-conformant object must not
    /// take provider discovery down for the whole namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_ref: Option<BackendObjectRef>,
    /// Wire format. Not cosmetic: Anthropic, Bedrock and Vertex do not speak the
    /// OpenAI format, and translating between them — streaming above all — is a
    /// real dataplane component. kinference only NAMES the format; the caller does
    /// the work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<ApiFormat>,
    /// Path the caller must dispatch on, replacing the one the client sent.
    /// Absent leaves the request line alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Required by the schema and declared rather than discovered: there is no
    /// `/v1/models` to poll, because reaching one would need the URL and the
    /// credential that deliberately live out of reach.
    ///
    /// The canonical `name` must agree with what the worker sets expose, since
    /// that agreement is the whole mechanism by which one candidate set spans
    /// local and remote. Nothing validates it;
    /// `InferencePool.status.models[].servedBy` aggregates every claim, so a
    /// mismatch is at least visible in one place.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<ProviderModel>,
}

/// A reference to an object in another API group, so `group` and `kind` are
/// meaningful rather than implied.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendObjectRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Wire format an upstream speaks.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum ApiFormat {
    /// The only pass-through.
    #[default]
    OpenAI,
    Anthropic,
    Bedrock,
    VertexAI,
    AzureOpenAI,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderModel {
    /// What this vendor calls it — an endpoint name, an ARN, a repo path.
    ///
    /// The string written into the body when this provider wins, AND routable
    /// under itself: a request naming it matches with no alias involved. That
    /// self-match is why there is no separate `upstreamName` — the row's identity
    /// and its wire name are the same string, so they cannot drift apart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Client-facing names that also resolve to [`Self::name`].
    ///
    /// Optional and plural. This is where a canonical name shared with a local
    /// worker set goes, which is what makes failover between them possible —
    /// vendors spell models their own way, and an alias is how one request name
    /// reaches both.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// Declared, not discovered: vendors rarely advertise one, and the built-in
    /// admission check needs it. Set it below a local set's effective window and
    /// long requests can never fail over here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderStatus {
    /// At-a-glance state as comma-separated tokens: the primary verdict first,
    /// then any qualifier that is true.
    ///
    /// A materialized STRING, not a list, because a `[*]` printer column renders
    /// only the FIRST array element — verified against the API server — so a list
    /// would print one token and drop the rest silently. Never empty when status
    /// is written, which is what keeps a BLANK column meaning "no status at all".
    ///
    /// Exists because one condition cannot answer "how is this object doing".
    /// Reading `Accepted`'s reason into a column showed a set that was accepted
    /// and idle identically to one accepted and serving; the qualifiers are the
    /// facts that a column reading a single condition structurally cannot show.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Not `skip_serializing_if` — see the note on `InferenceWorkerSetStatus`.
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

// No `observed.observedTTFTP95`. The router still computes that p95 — it is the
// `EstimatedTTFT` ranking input for a provider and what a
// `backend.observedTTFTP95` predicate compares against — but as a NUMBER on the
// routing path, never formatted. Status reported it as a duration string that
// nothing read back, on a field that was absent until the first report and so
// blank on any provider yet to serve.

// ---------------------------------------------------------------------------
// ReferenceGrant (gateway.networking.k8s.io/v1beta1) — READ-ONLY, minimal
// ---------------------------------------------------------------------------

/// Just enough `ReferenceGrant` to answer "may namespace A reference this
/// object in namespace B".
///
/// Read for two things, both of which cross a namespace: a pool's
/// `destinations[].namespace`, and an `InferenceProvider`'s
/// `backendRef.namespace`. The second is the load-bearing one — it is what stops
/// one namespace spending another's vendor credential — and kinference can enforce it
/// without ever reading a `GatewayBackend`, because the grant lives in the target
/// namespace and describes the reference rather than the referent.
///
/// Deliberately partial, like `HTTPRoute`: `from[].namespace` and `to[]` are all
/// that a decision needs.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1beta1",
    kind = "ReferenceGrant",
    plural = "referencegrants",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceGrantSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<ReferenceGrantFrom>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<ReferenceGrantTo>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceGrantFrom {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceGrantTo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Absent means every object of that kind in this namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

// ---------------------------------------------------------------------------
// InferencePool
// ---------------------------------------------------------------------------

/// The HTTPRoute backendRef: which destinations traffic may reach, and which
/// policy chooses among them.
///
/// The pool is a **hard outer bound** — a policy cannot reach a destination the
/// pool does not list. That is what makes it safe to give tenants write access
/// to policies while keeping pools platform-owned.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kinference.buoyant.io",
    version = "v1alpha1",
    kind = "InferencePool",
    plural = "inferencepools",
    shortname = "infpool",
    namespaced,
    status = "InferencePoolStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct InferencePoolSpec {
    /// Omit to mean every `InferenceWorkerSet` in this namespace.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub destinations: Vec<Destination>,
    /// At most one policy. Omitting it means "serve every request as asked".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<PolicyRef>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Destination {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Exactly one of `name` or `selector` is set (admission-enforced).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub match_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferencePoolStatus {
    /// At-a-glance state as comma-separated tokens: the primary verdict first,
    /// then any qualifier that is true.
    ///
    /// A materialized STRING, not a list, because a `[*]` printer column renders
    /// only the FIRST array element — verified against the API server — so a list
    /// would print one token and drop the rest silently. Never empty when status
    /// is written, which is what keeps a BLANK column meaning "no status at all".
    ///
    /// Exists because one condition cannot answer "how is this object doing".
    /// Reading `Accepted`'s reason into a column showed a set that was accepted
    /// and idle identically to one accepted and serving; the qualifiers are the
    /// facts that a column reading a single condition structurally cannot show.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_count: Option<i64>,
    /// The pool's OWN admission state.
    ///
    /// Separate from `parents[].conditions`, which is the per-parent handshake a
    /// gateway waits on. A pool with no `HTTPRoute` attached has no parents, so
    /// writing these only per-parent left a pool with zero destinations and a
    /// dangling `policyRef` looking exactly like a healthy one — its admission
    /// state existed nowhere. Whether the router accepted this pool is a fact
    /// about the POOL, and every other kind in this group reports it top-level.
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// Aggregated across destinations — "what can traffic on this route
    /// reach", which no single worker set can answer.
    #[serde(default)]
    pub models: Vec<PoolModelStatus>,
    /// Must serialize when empty — see the note on `InferenceWorkerSetStatus`.
    /// This is the field that exposed the bug: fixing a dangling destination
    /// left it listed as unresolved indefinitely.
    #[serde(default)]
    pub unresolved_destinations: Vec<UnresolvedDestination>,
    /// Gateway API status contract. `Accepted` lives in here, per-parent, which
    /// is why kinference must also read HTTPRoutes to know its parents at all.
    #[serde(default)]
    pub parents: Vec<ParentStatus>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolModelStatus {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub served_by: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<ContextWindow>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedDestination {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Why it didn't resolve — `NotFound`, `NoMatchingPool`, etc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParentStatus {
    pub parent_ref: ParentRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParentRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// `metav1.Condition`. Hand-rolled rather than reused from `k8s-openapi`
/// because that type has no `JsonSchema` impl.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    pub r#type: String,
    /// `"True"` | `"False"` | `"Unknown"`.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    pub last_transition_time: String,
    pub reason: String,
    pub message: String,
}

// ---------------------------------------------------------------------------
// InferencePolicy
// ---------------------------------------------------------------------------

/// How to choose, given the worker sets an `InferencePool` allows. The only
/// object a tenant needs write access to. Rules are first-match-wins in list
/// order.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kinference.buoyant.io",
    version = "v1alpha1",
    kind = "InferencePolicy",
    plural = "inferencepolicies",
    shortname = "infpol",
    namespaced,
    status = "InferencePolicyStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct InferencePolicySpec {
    /// `ServeLocal` (default) is fail-open. A rule set meant to enumerate must
    /// say `Reject`, or an unlisted tenant is served rather than refused.
    #[serde(default)]
    pub unmatched: Unmatched,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<Rule>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum Unmatched {
    #[default]
    ServeLocal,
    Reject,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    pub name: String,
    /// Entries are ORed; fields within one entry are ANDed. No `matches` means
    /// "matches everything".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matches: Vec<RequestPredicate>,
    pub route: Route,
}

/// Request-scope predicate. Every value is untyped on purpose — see the module
/// doc.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RequestPredicate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<RawPredicateValue>,
    /// Always present on the request, so a predicate here always evaluates.
    /// `tenant: ""` matches traffic the caller could not attribute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<RawPredicateValue>,
    /// Array-only, over the closed set `interactive | batch | deferrable` — the
    /// schema enumerates the domain, which costs the scalar and operator forms
    /// and buys API-server rejection of a typo. Deliberately still untyped here:
    /// a non-conformant shape must degrade to a warning on ONE predicate, not
    /// fail deserialization of the whole `InferencePolicy`, which would drop the
    /// policy from the store and silently fall the pool back to serve-as-asked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos: Option<RawPredicateValue>,
    /// Classifier output, as a SET of opaque tags. A scalar RHS means CONTAINS,
    /// an array means INTERSECTS, `{containsAll: [...]}` means all of them, and
    /// `{not: t}` means `t` is absent from the set.
    ///
    /// The tag's prefix selects which classifier has to run, so a rule
    /// referencing `pii-*` pays the PII classifier's latency.
    ///
    /// Untyped and unvalidated, so a typo matches nothing silently. The
    /// authoritative set of emittable tags is [`super::classify::CLASSIFIERS`],
    /// and `compile` warns into `status.warnings` on a tag none of them can
    /// produce — which is where a typo now surfaces, the per-rule classifier
    /// metadata having been dropped from status as router reference material
    /// rather than per-object state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<RawPredicateValue>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    /// Candidate generation. Defaults to the requested model on every
    /// destination the pool allows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consider: Vec<ConsiderGroup>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<Exclusion>,
    /// Required. Ordered tiers; the first with a surviving candidate wins.
    pub select: Vec<SelectTier>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tie_break: Vec<TieBreak>,
    #[serde(default)]
    pub on_empty: OnEmpty,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsiderGroup {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Shorthand for `backends.type`; setting both is admission-rejected.
    ///
    /// The same spelling `SelectTier` accepts, because constraining a group to
    /// one backend class is the commonest thing written in either list and
    /// having it only in `select` meant the same idea had two spellings
    /// depending on which list you were in.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<BackendType>,
    /// Relative, not a percentage. Groups are sampled by weight FIRST, before
    /// exclusions and selection run over the survivors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub models: Option<ModelSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<BackendSelector>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelSelector {
    /// Exactly one of `from_request` or `names` (admission-enforced).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_request: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendSelector {
    /// Semantic class, not a Kind name. `Local` means `InferenceWorkerSet`
    /// today and could later cover an imported peer-cluster set without every
    /// policy needing an edit — which is the reason it is not spelled `kind`.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<BackendType>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,
}

/// Which class of destination a selector or tier admits.
///
/// The two differ in what can be *known* about them, not merely in where they
/// sit: a local candidate carries live per-endpoint telemetry, a provider
/// candidate carries an empirical aggregate and nothing per-request. That is why
/// signals are scoped by this and not by namespace or ownership.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BackendType {
    #[default]
    Local,
    Provider,
}

impl BackendType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "Local",
            Self::Provider => "Provider",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Exclusion {
    pub name: String,
    /// Optional request-scope guard. Exists because rules are first-match-wins
    /// and therefore do NOT compose exclusions.
    #[serde(default, rename = "if", skip_serializing_if = "Option::is_none")]
    pub if_: Option<RequestPredicate>,
    pub drop: CandidatePredicate,
}

/// Candidate-scope predicate. A predicate over a signal the candidate does not
/// carry does not match.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CandidatePredicate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<RawPredicateValue>,
    /// Renamed EXPLICITLY, like its `observedTTFTP95` sibling below.
    /// `rename_all = "camelCase"` would give `predictedTtft`, which disagrees
    /// with both the CRD schema in `charts/kinference-crds/templates/` and the name
    /// `CandidateAttr::name()` prints in traces — and the disagreement was
    /// silent: the structural schema admits `predictedTTFT`, serde ignored it,
    /// and the predicate simply vanished, so the rule matched everything it
    /// was written to exclude. Pinned by
    /// `every_candidate_attr_name_round_trips_through_serde`.
    #[serde(
        default,
        rename = "predictedTTFT",
        skip_serializing_if = "Option::is_none"
    )]
    pub predicted_ttft: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_cache_hit_ratio: Option<RawPredicateValue>,
    /// Mean queued prompts over the last 30s — FRACTIONAL, so `greaterThan:
    /// 0` means "any queueing at all in the window", not "queued right now".
    /// A worker that queued 5 prompts for two seconds reads about 0.3, so pick
    /// a threshold that says how much sustained queueing you will tolerate.
    ///
    /// Sheds load in v1 — it does not route around it. Excluding every
    /// backed-up endpoint empties the candidate set and `onEmpty` fires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_prompts: Option<RawPredicateValue>,
    /// Mean KV-cache utilization over the last 30s, 0.0..1.0.
    ///
    /// The headroom signal. Unlike `queuedPrompts` it rises monotonically with
    /// pressure, so it is the one to threshold for "this GPU is nearly full"
    /// — measured on a real fleet, a moment at 93% KV showed only one queued
    /// prompt while a moment at 31% showed five.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_cache_usage: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<CandidateModelPredicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<CandidateBackendPredicate>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CandidateModelPredicate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<RawPredicateValue>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CandidateBackendPredicate {
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<RawPredicateValue>,
    /// Local-only, so it never drops a provider candidate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_endpoints: Option<RawPredicateValue>,
    /// Provider-only. An aggregate, so it is a gate and never a ranking key.
    #[serde(
        default,
        rename = "observedTTFTP95",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_ttft_p95: Option<RawPredicateValue>,
    /// Provider-only, and sourced from a `UsageBudget`, which does not exist
    /// yet. Parsed so that using it produces a policy WARNING rather than being
    /// silently dropped by serde and compiling to an exclusion that never fires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_exhausted: Option<RawPredicateValue>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SelectTier {
    /// Shorthand for `backends.type`; setting both is admission-rejected.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<BackendType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<BackendSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimize: Option<Minimize>,
    /// Destinations, in preference order. Exactly one of `minimize` or `order`
    /// (admission-enforced).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order: Vec<String>,
}

impl ConsiderGroup {
    /// The group's backend-class constraint, from either spelling.
    pub fn backend_type(&self) -> Option<BackendType> {
        self.type_
            .or_else(|| self.backends.as_ref().and_then(|b| b.type_))
    }
}

impl SelectTier {
    /// The tier's backend-class constraint, from either spelling.
    pub fn backend_type(&self) -> Option<BackendType> {
        self.type_
            .or_else(|| self.backends.as_ref().and_then(|b| b.type_))
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum Minimize {
    /// A calibrated per-request prediction over prompt, prefix overlap and
    /// worker state. Local-only — there is nothing to compute it from for a
    /// provider — which is why the CRD rejects it on a tier that could span
    /// both classes.
    #[default]
    PredictedTTFT,
    /// `PredictedTTFT` where available, `observedTTFTP95` otherwise. Its
    /// fidelity is heterogeneous BY CONSTRUCTION: it ranks a per-request
    /// prediction against an empirical aggregate with no visibility into the
    /// vendor's queue. That is the price of ordering across classes in one tier,
    /// and it is why tiering exists as the alternative.
    EstimatedTTFT,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TieBreak {
    /// Prefer a local candidate over a provider one. Only reachable inside a
    /// tier that spans both classes — order tiers by class instead if that is
    /// what you want, since a tier boundary is unconditional and this is not.
    PreferLocal,
    #[default]
    Random,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum OnEmpty {
    /// Fail the request when every candidate has been excluded.
    #[default]
    Reject,
    /// Continue evaluating subsequent rules; if none yields a candidate, apply
    /// `spec.unmatched`. The CRD guide flagged this value as unspecified; this
    /// is the reading that composes with first-match-wins.
    FallThrough,
}

// ---------------------------------------------------------------------------
// HTTPRoute (gateway.networking.k8s.io/v1) — READ-ONLY, minimal
// ---------------------------------------------------------------------------

/// Just enough `HTTPRoute` to answer "which Gateways route to this pool".
///
/// kinference reads this for one reason: `InferencePool`'s `Accepted` condition lives
/// inside `status.parents[]`, keyed by parent Gateway, so there is no way to
/// publish it without knowing the routes that name the pool. We never write
/// HTTPRoute status — that belongs to the gateway controller.
///
/// Deliberately partial: `matches`, `filters`, `hostnames` and everything else
/// are irrelevant here, and `serde` ignores unknown fields.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "HTTPRoute",
    plural = "httproutes",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct HTTPRouteSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_refs: Vec<ParentRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<HTTPRouteRule>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HTTPRouteRule {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backend_refs: Vec<BackendRef>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

impl BackendRef {
    /// True when this ref targets an `kinference.buoyant.io` `InferencePool`.
    pub fn is_inference_pool(&self) -> bool {
        self.group.as_deref() == Some("kinference.buoyant.io")
            && self.kind.as_deref() == Some("InferencePool")
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferencePolicyStatus {
    /// At-a-glance state, as comma-separated tokens: exactly one of `InForce` /
    /// `NotReferenced`, then `HasWarnings` if there are any.
    ///
    /// A materialized STRING rather than a list, because a `[*]` printer column
    /// renders only the FIRST element — verified against the API server — so an
    /// array would show `NotReferenced` and silently drop `HasWarnings`. The
    /// router joins it here so the column can print all of it.
    ///
    /// Not folded into the `Accepted` condition's reason either, though commas
    /// are legal there: warnings never affect acceptance (a policy in force with
    /// two warnings is `Accepted=True`), so `HasWarnings` is not a reason for
    /// anything and putting it in that field would make the condition lie.
    ///
    /// Always at least one token when status is written, which is what keeps a
    /// BLANK column meaning "no status at all" — a different problem from any
    /// value this can hold.
    ///
    /// This replaced `ruleCount`. That field was `len(spec.rules)` and could
    /// never be anything else — compilation is a straight map with no filter and
    /// no reorder — so it duplicated the manifest, and the column it fed said
    /// only "this compiled", which every policy in the namespace does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Pools whose `policyRef` names this policy. EMPTY means nothing routes
    /// through it.
    ///
    /// `state` carries the at-a-glance answer; this field is the detail behind it
    /// — WHICH pools, not just whether any.
    ///
    /// Not the printer column itself, deliberately. A `[*]` JSONPath renders only
    /// the first element, so a policy shared by two pools would silently report
    /// one, and the un-starred form prints raw JSON — `[]` on every idle policy.
    #[serde(default)]
    pub referenced_by: Vec<String>,
    // Must serialize when empty — see the note on `InferenceWorkerSetStatus`.
    // `warnings` matters most here: a warning that outlives the mistake it
    // describes actively misleads whoever just fixed it.
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

// ---------------------------------------------------------------------------
// BackendBinding (gateway.linkerd.io)
// ---------------------------------------------------------------------------

/// The gateway's stand-in for a backendRef of a kind it has no schema for.
///
/// A `BackendBinding` claims a `(group, kind, name)` triple and says what that
/// ref resolves to in kinds the gateway DOES resolve — a core `Service` or a
/// `GatewayBackend`. The claimed object is never read by the gateway and need
/// not exist, which is precisely why an `InferencePool` can be a route's
/// backendRef at all: the gateway knows nothing about `kinference.buoyant.io`.
///
/// kinference is the router-controller for `InferencePool`, so it is the component
/// that knows what a pool resolves to — and therefore the one that owns these
/// objects. See `super::binding`.
///
/// The one kind in this file whose YAML is NOT in `charts/kinference-crds/templates/`: the schema ships
/// with the gateway (`deploy/chart/gateway/crds/backendbinding.yaml` there),
/// which is what owns and validates it. This mirrors that schema
/// field-for-field, and drifts from it the moment the gateway's own moves —
/// there is no shared artifact to catch it, so check against the chart when
/// touching either.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "gateway.linkerd.io",
    version = "v1alpha1",
    kind = "BackendBinding",
    plural = "backendbindings",
    namespaced,
    status = "BackendBindingStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct BackendBindingSpec {
    /// The foreign backendRef this binding answers for, matched as opaque
    /// strings against route refs in the binding's OWN namespace. That
    /// namespace scoping is why kinference creates the binding beside the pool: a
    /// route naming the pool defaults the ref's namespace to its own.
    pub claim: BindingClaim,
    /// What the claimed ref resolves to. Native kinds only.
    pub backend_refs: Vec<BindingBackendRef>,
    /// `Extension` objects in the binding's namespace, prepended to the filter
    /// chain of any rule whose ref this binding claims. kinference never sets this;
    /// it is modeled so the field is visible when reading one back.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
}

/// The `(group, kind, name)` of the backendRef a binding answers for, held as
/// opaque strings.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct BindingClaim {
    pub group: String,
    pub kind: String,
    pub name: String,
}

/// One destination a binding resolves to.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BindingBackendRef {
    /// Identifies the entry WITHIN the binding — in the gateway's status
    /// messages, and as the stable key for anything per-entry added later.
    /// Unrelated to the target's own name, so kinference uses the destination's
    /// name (the string a policy's `backends.names` would use), which is
    /// unique across both classes within one pool.
    pub name: String,
    /// The target. `group` and `kind` absent mean the core `Service`, exactly
    /// as in a route backendRef.
    #[serde(rename = "ref")]
    pub target: BindingObjectReference,
}

/// A typed reference to a native backend kind.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BindingObjectReference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    /// Absent means the binding's own namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Service port the traffic goes to. Set for a core `Service` target — the
    /// number `serviceRef.port` resolves to against the Service's own
    /// `spec.ports` — and left absent for a `GatewayBackend`, which carries its
    /// port in `baseURL` and is REJECTED if given one here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// Status of one `BackendBinding`. Written by the GATEWAY, never by kinference —
/// modeled so the writer can report an `Accepted=False` verdict on an object it
/// created, which is otherwise invisible from kinference's side.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendBindingStatus {
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

#[cfg(test)]
mod tests;
