//! The controller side: publishing `status` on the kinference.buoyant.io resources.
//!
//! kinference is the router-controller for these kinds, so it owns their status. That
//! matters beyond observability: a conforming gateway **must not program a route
//! until the referenced pool reports `Accepted=True`**, so nothing routes until
//! this runs.
//!
//! Two hazards shape the design.
//!
//! 1. **Write amplification / self-triggering.** We watch the same objects we
//!    patch, so an unconditional periodic write would wake our own watch, which
//!    rebuilds the snapshot, which writes again — forever, at whatever the tick
//!    rate is. The fix is comparing the desired status against what we last
//!    wrote and patching only on a real change. That comparison is what makes
//!    the loop terminate, not an optimization.
//! 2. **Condition churn.** `lastTransitionTime` must only move when the
//!    condition actually transitions. We reuse the previous timestamp whenever
//!    `(type, status, reason)` is unchanged; otherwise every write would look
//!    like a state change to anything watching.
//!
//! Status computation is pure over `(snapshot, PoolFacts)` so it is testable
//! without a cluster; only the patching is async.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use kube::Api;
use kube::api::{Patch, PatchParams};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::crd::{
    Condition, ContextWindow, EndpointCounts, InferencePolicy, InferencePolicyStatus,
    InferencePool, InferencePoolStatus, InferenceProvider, InferenceProviderStatus,
    InferenceWorkerSet, InferenceWorkerSetStatus, KvEventsStatus, ParentStatus, PoolModelStatus,
    UnresolvedDestination, WorkerSetModelStatus,
};
use super::provider_health::{FailureKind, ProviderHealth};
use super::store::{
    DanglingPolicy, PolicyStore, ResolvedPolicy, ResolvedPool, ResolvedWorkerSet, UnresolvedReason,
};
use crate::metrics::MetricsRegistry;
use crate::pool::{Pool, PoolId};

/// The controller identity written into `status.parents[].controllerName`.
pub const CONTROLLER_NAME: &str = "kinference.buoyant.io/router";

/// What one worker set's pool actually has right now, from the live registries.
#[derive(Debug, Clone, Default)]
pub struct PoolFacts {
    /// Endpoints that passed the readiness gate (models resolved AND initial KV
    /// sync done) — the same definition `/readyz` uses.
    pub ready_endpoints: i64,
    /// Endpoints kinference has actually decoded a KV cache event from.
    ///
    /// Counted independently of readiness, and it has to be: readiness keys on
    /// the SUB socket reaching steady state, which a ZMQ `connect()` reports
    /// even when nothing is publishing. So a set can be fully ready and
    /// serving while its prefix model is empty — the exact condition
    /// `status.kvEvents` exists to make visible.
    pub event_endpoints: i64,
    /// Discovered models, keyed by the UPSTREAM name the engine reports.
    pub models: Vec<ModelFact>,
}

#[derive(Debug, Clone)]
pub struct ModelFact {
    pub upstream: String,
    pub ready_endpoints: i64,
    /// Effective window across ready endpoints. A range, because a rollout that
    /// changes `--max-model-len` leaves endpoints disagreeing.
    pub ctx_min: Option<i64>,
    pub ctx_max: Option<i64>,
}

/// Gather live facts for every pool. Cheap: one registry snapshot per pool plus
/// a metrics lookup per worker.
///
/// Takes a slice rather than the registry so the caller decides WHEN the
/// snapshot is taken. That matters: every fact in one status write must come
/// from one view of the fleet, or a pool can be counted in `destinationCount`
/// and missing from `models` in the same object.
pub fn gather_facts(pools: &[Arc<Pool>], metrics: &MetricsRegistry) -> HashMap<PoolId, PoolFacts> {
    let mut out = HashMap::new();
    for pool in pools {
        let mut ready_endpoints = 0i64;
        let mut event_endpoints = 0i64;
        // upstream model -> (count, min, max)
        let mut per_model: BTreeMap<String, (i64, Option<i64>, Option<i64>)> = BTreeMap::new();
        for (num, entry) in pool.registry.iter_snapshot() {
            // Counted before the readiness gate: an endpoint publishing events
            // while still waiting on its first `/v1/models` scrape is genuinely
            // subscribed, and reporting otherwise would look like a dead
            // publisher during every rollout.
            if entry.kv_events_observed {
                event_endpoints += 1;
            }
            if !entry.is_ready() {
                continue;
            }
            ready_endpoints += 1;
            let info = metrics.models(num);
            for m in &entry.current_models {
                let window = info
                    .models
                    .iter()
                    .find(|e| &e.name == m)
                    .and_then(|e| e.max_model_len)
                    .map(|v| v as i64);
                let slot = per_model.entry(m.clone()).or_insert((0, None, None));
                slot.0 += 1;
                if let Some(w) = window {
                    slot.1 = Some(slot.1.map_or(w, |cur: i64| cur.min(w)));
                    slot.2 = Some(slot.2.map_or(w, |cur: i64| cur.max(w)));
                }
            }
        }
        out.insert(
            pool.id,
            PoolFacts {
                ready_endpoints,
                event_endpoints,
                models: per_model
                    .into_iter()
                    .map(|(upstream, (n, lo, hi))| ModelFact {
                        upstream,
                        ready_endpoints: n,
                        ctx_min: lo,
                        ctx_max: hi,
                    })
                    .collect(),
            },
        );
    }
    out
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Build a condition, reusing `previous`'s timestamp when nothing transitioned.
fn condition(
    r#type: &str,
    ok: bool,
    reason: &str,
    message: &str,
    generation: Option<i64>,
    previous: Option<&Condition>,
) -> Condition {
    let status = if ok { "True" } else { "False" };
    let last_transition_time = previous
        .filter(|p| p.r#type == r#type && p.status == status && p.reason == reason)
        .map(|p| p.last_transition_time.clone())
        .unwrap_or_else(now_rfc3339);
    Condition {
        r#type: r#type.to_string(),
        status: status.to_string(),
        observed_generation: generation,
        last_transition_time,
        reason: reason.to_string(),
        message: message.to_string(),
    }
}

/// Join the STATE tokens for a printer column.
///
/// One string rather than a list because a `[*]` printer column renders only the
/// first array element, so a list loses every token after the first with no
/// trace. `primary` is always present, which is what keeps a blank column
/// meaning "no status was written at all" rather than "nothing to report".
fn state(primary: &str, qualifiers: &[(bool, &str)]) -> Option<String> {
    let mut out = String::from(primary);
    for (present, tok) in qualifiers {
        if *present {
            out.push(',');
            out.push_str(tok);
        }
    }
    Some(out)
}

fn find<'a>(conds: Option<&'a [Condition]>, ty: &str) -> Option<&'a Condition> {
    conds?.iter().find(|c| c.r#type == ty)
}

/// Desired `InferencePool` status.
pub fn desired_pool_status(
    resolved: &ResolvedPool,
    facts: &HashMap<PoolId, PoolFacts>,
    previous: Option<&InferencePoolStatus>,
) -> InferencePoolStatus {
    // Aggregate what this route can actually reach, across destinations. This
    // is the field to check first when debugging, and no single worker set can
    // answer it.
    //
    // Value is (destinations serving the model, min context window, max
    // context window); the windows stay `None` when no destination declares one.
    type ModelAgg = (Vec<String>, Option<i64>, Option<i64>);
    let mut by_model: BTreeMap<String, ModelAgg> = BTreeMap::new();
    for dest in &resolved.destinations {
        // A provider reports what it DECLARES rather than what it was observed
        // to serve — there is nothing to scrape. That makes this field the one
        // place a canonical-name mismatch between a provider and a worker set
        // becomes visible, since nothing validates that they agree.
        if let Some(prov) = dest.as_provider() {
            for m in &prov.models {
                let (Some(name), Some(window)) = (m.name.clone(), m.context_window) else {
                    let Some(name) = m.name.clone() else { continue };
                    let slot = by_model
                        .entry(name)
                        .or_insert_with(|| (Vec::new(), None, None));
                    if !slot.0.contains(&prov.name) {
                        slot.0.push(prov.name.clone());
                    }
                    continue;
                };
                let slot = by_model
                    .entry(name)
                    .or_insert_with(|| (Vec::new(), None, None));
                if !slot.0.contains(&prov.name) {
                    slot.0.push(prov.name.clone());
                }
                let w = window as i64;
                slot.1 = Some(slot.1.map_or(w, |c: i64| c.min(w)));
                slot.2 = Some(slot.2.map_or(w, |c: i64| c.max(w)));
            }
            continue;
        }
        let Some(dest) = dest.as_local() else {
            continue;
        };
        let Some(f) = facts.get(&dest.pool_id) else {
            continue;
        };
        for m in &f.models {
            // A pool answers "what can a client ask for", so it keys on the
            // client-facing name — the opposite of the worker set, which reports
            // what it observed. Every alias gets its OWN entry rather than one
            // entry under an arbitrarily-chosen alias: two names for one
            // upstream model are two things a client can legitimately send, and
            // collapsing them hides one. `servedBy` and the context window
            // duplicate across them, which is accurate — it is the same fleet.
            //
            // An upstream with no alias falls back to itself, which is exactly
            // the unaliased behavior.
            let names = {
                let a = dest.aliases_for(&m.upstream);
                if a.is_empty() {
                    vec![m.upstream.clone()]
                } else {
                    a
                }
            };
            for name in names {
                let slot = by_model
                    .entry(name)
                    .or_insert_with(|| (Vec::new(), None, None));
                if !slot.0.contains(&dest.name) {
                    slot.0.push(dest.name.clone());
                }
                if let Some(lo) = m.ctx_min {
                    slot.1 = Some(slot.1.map_or(lo, |c: i64| c.min(lo)));
                }
                if let Some(hi) = m.ctx_max {
                    slot.2 = Some(slot.2.map_or(hi, |c: i64| c.max(hi)));
                }
            }
        }
    }

    let models: Vec<PoolModelStatus> = by_model
        .into_iter()
        .map(|(name, (served_by, lo, hi))| PoolModelStatus {
            name,
            served_by,
            context_window: (lo.is_some() || hi.is_some())
                .then_some(ContextWindow { min: lo, max: hi }),
        })
        .collect();

    // Two indirection hops, two ways to dangle: a destination that doesn't
    // resolve, or a policyRef naming a missing policy.
    let refs_ok = resolved.unresolved.is_empty() && resolved.dangling_policy.is_none();
    let refs_reason = if !resolved.unresolved.is_empty() {
        "UnresolvedDestinations"
    } else {
        match &resolved.dangling_policy {
            Some(DanglingPolicy::NotFound { .. }) => "PolicyNotFound",
            // Its own reason, because it sends the operator somewhere else: the
            // policy may well exist, and the thing to fix is the ref.
            Some(DanglingPolicy::WrongKind { .. }) => "PolicyRefWrongKind",
            None => "ResolvedRefs",
        }
    };
    let refs_msg = match (&resolved.dangling_policy, resolved.unresolved.len()) {
        (Some(DanglingPolicy::NotFound { name }), _) => {
            format!("policyRef `{name}` does not exist; serving without policy")
        }
        (Some(DanglingPolicy::WrongKind { name, group, kind }), _) => format!(
            "policyRef `{name}` names {group}/{kind}, not {}/{}; serving without policy",
            super::store::POLICY_GROUP,
            super::store::POLICY_KIND
        ),
        (None, 0) => "all destinations and the policyRef resolved".to_string(),
        (None, n) => format!("{n} destination(s) did not resolve"),
    };

    // Built once, then written BOTH top-level and per-parent: the pool-level
    // answer and the per-parent handshake are the same judgement, and computing
    // them twice would let them drift.
    let accepted_ok = !resolved.destinations.is_empty();
    let accepted_reason = if accepted_ok {
        "Accepted"
    } else {
        "NoDestinations"
    };
    let accepted_msg = format!(
        "{} destination(s) resolved to a routable pool",
        resolved.destinations.len()
    );

    let parents: Vec<ParentStatus> = resolved
        .parents
        .iter()
        .map(|p| {
            let prev = previous.and_then(|s| {
                s.parents
                    .iter()
                    .find(|q| q.parent_ref.name == p.name)
                    .map(|q| q.conditions.as_slice())
            });
            ParentStatus {
                parent_ref: p.clone(),
                controller_name: Some(CONTROLLER_NAME.to_string()),
                conditions: vec![
                    // Accepted reflects "the router has this pool and can route
                    // it" — the handshake the gateway waits on. Unresolved
                    // destinations don't retract it: a pool with one good
                    // destination still routes.
                    condition(
                        "Accepted",
                        accepted_ok,
                        accepted_reason,
                        &accepted_msg,
                        resolved.generation,
                        find(prev, "Accepted"),
                    ),
                    condition(
                        "ResolvedRefs",
                        refs_ok,
                        refs_reason,
                        &refs_msg,
                        resolved.generation,
                        find(prev, "ResolvedRefs"),
                    ),
                ],
            }
        })
        .collect();

    let own = previous.map(|s| s.conditions.as_slice());
    InferencePoolStatus {
        // The two dangle reasons are qualifiers because the pool still ROUTES
        // through either: an unresolved destination leaves the others usable,
        // and a bad policyRef fails open to serve-as-asked.
        state: state(
            accepted_reason,
            &[
                (!resolved.unresolved.is_empty(), "UnresolvedDestinations"),
                (
                    matches!(
                        resolved.dangling_policy,
                        Some(DanglingPolicy::NotFound { .. })
                    ),
                    "PolicyNotFound",
                ),
                (
                    matches!(
                        resolved.dangling_policy,
                        Some(DanglingPolicy::WrongKind { .. })
                    ),
                    "PolicyRefWrongKind",
                ),
            ],
        ),
        destination_count: Some(resolved.destinations.len() as i64),
        conditions: vec![
            condition(
                "Accepted",
                accepted_ok,
                accepted_reason,
                &accepted_msg,
                resolved.generation,
                find(own, "Accepted"),
            ),
            condition(
                "ResolvedRefs",
                refs_ok,
                refs_reason,
                &refs_msg,
                resolved.generation,
                find(own, "ResolvedRefs"),
            ),
        ],
        models,
        unresolved_destinations: resolved
            .unresolved
            .iter()
            .map(|u| UnresolvedDestination {
                kind: Some(u.kind.clone()),
                name: Some(u.name.clone()),
                reason: Some(u.reason.as_str().to_string()),
            })
            .collect(),
        parents,
    }
}

/// Status for a worker set the router refused to materialize.
///
/// Everything derived from a pool is cleared, not omitted: this object may have
/// been healthy a moment ago, and leaving a stale `ready: 3` next to
/// `Accepted=False` is worse than saying nothing — it reads as "still serving,
/// just unhappy" when in fact no traffic can reach it.
pub fn rejected_worker_set_status(
    reason: &super::worker_sets::RejectReason,
    referenced_by: &[String],
    previous: Option<&InferenceWorkerSetStatus>,
) -> InferenceWorkerSetStatus {
    let prev = previous.map(|s| s.conditions.as_slice());
    InferenceWorkerSetStatus {
        // The rejection IS the verdict, so it leads. No `NotReady` qualifier —
        // a refused set has no endpoints by construction, and saying so twice
        // would read as two problems.
        state: state(reason.as_condition_reason(), &[]),
        models: Vec::new(),
        // NOT cleared with the rest. Who names this set is a fact about the
        // POOLS, unaffected by this object being broken — and it is what tells
        // an operator which pool just lost a destination.
        referenced_by: referenced_by.to_vec(),
        endpoints: Some(EndpointCounts { ready: Some(0) }),
        kv_events: None,
        conditions: vec![
            condition(
                "Accepted",
                false,
                reason.as_condition_reason(),
                &reason.to_string(),
                None,
                find(prev, "Accepted"),
            ),
            condition(
                "Ready",
                false,
                "NotAccepted",
                "the worker set was not accepted, so it has no endpoints",
                None,
                find(prev, "Ready"),
            ),
        ],
    }
}

/// Desired `InferenceProvider` status.
///
/// Everything here comes from the caller. That is not a shortcut — it is what the
/// split means: kinference never dials the vendor, never reads the `GatewayBackend`,
/// and holds no credential, so a provider's own object can only say what it has
/// been told.
///
/// The two conditions are deliberately different questions, and which one fires
/// decides where an operator goes next:
///
/// - `ResolvedRefs=False` means **this object is wrong** — a `backendRef` that
///   does not resolve, or credentials the vendor rejected. Go edit a manifest.
/// - `Ready=False` means **the far end is unwell** — 5xx, or no response. Nothing
///   here to fix.
///
/// Getting that backwards wastes real time: sending someone to re-check a Secret
/// during a vendor outage, or leaving a typo looking like somebody else's problem.
pub fn desired_provider_status(
    health: &ProviderHealth,
    previous: Option<&InferenceProviderStatus>,
) -> InferenceProviderStatus {
    let prev = previous.map(|s| s.conditions.as_slice());
    let config_error = health
        .last_failure
        .as_ref()
        .filter(|(kind, _)| kind.is_configuration_error());
    let upstream_error = health
        .last_failure
        .as_ref()
        .filter(|(kind, _)| !kind.is_configuration_error());

    let refs = match config_error {
        Some((kind, detail)) => condition(
            "ResolvedRefs",
            false,
            kind.as_str(),
            &format!(
                "the caller could not dispatch through this provider's backendRef: {detail}. \
                 kinference does not read GatewayBackends, so this is reported rather than \
                 observed"
            ),
            None,
            find(prev, "ResolvedRefs"),
        ),
        // True is the honest answer and a weak one: it means the
        // InferenceProvider is well-formed and permitted to name its backend,
        // NOT that the backend exists. Nothing kinference can see distinguishes the
        // two, which is exactly why the caller's report is load-bearing.
        None => condition(
            "ResolvedRefs",
            true,
            "Resolved",
            "backendRef is well-formed and permitted; whether it resolves is the \
             caller's to report",
            None,
            find(prev, "ResolvedRefs"),
        ),
    };

    let ready = if health.suppressed {
        let detail = health
            .last_failure
            .as_ref()
            .map(|(_, d)| d.clone())
            .unwrap_or_else(|| "repeated dispatch failures".to_string());
        condition(
            "Ready",
            false,
            "DispatchFailing",
            &format!("producing no candidates after repeated dispatch failures: {detail}"),
            None,
            find(prev, "Ready"),
        )
    } else if let Some((_, detail)) = upstream_error {
        // Not suppressed, so it is still serving — but a recent upstream error is
        // worth surfacing rather than hiding behind a bare `Ready=True`.
        condition(
            "Ready",
            true,
            "Serving",
            &format!("serving, with a recent upstream error: {detail}"),
            None,
            find(prev, "Ready"),
        )
    } else {
        condition(
            "Ready",
            true,
            "Serving",
            "producing candidates",
            None,
            find(prev, "Ready"),
        )
    };

    // Written on the HEALTHY path too, not only by `rejected_provider_status`.
    // Omitting it here did not leave a stale `Accepted=False` behind — status is
    // a merge patch and `conditions` is replaced wholesale — it did something
    // quieter and worse: a provider the operator FIXED lost the condition
    // entirely instead of flipping to True, and a provider that was never broken
    // never had one. So the question "is this object admitted" had no answer for
    // exactly the providers that were fine, which is the same blank-status shape
    // that hid an unattached policy and an unreferenced worker set.
    let accepted = condition(
        "Accepted",
        true,
        "Accepted",
        "resolved into a pool destination",
        None,
        find(prev, "Accepted"),
    );

    InferenceProviderStatus {
        // `Suppressed` is the one an operator needs at a glance: the provider is
        // well-formed and still producing NO candidates. The backend tokens come
        // from the caller's reports, which is the only channel that can see them.
        state: state(
            "Accepted",
            &[
                (
                    matches!(config_error, Some((FailureKind::Unresolved, _))),
                    "BackendUnresolved",
                ),
                (
                    matches!(config_error, Some((FailureKind::Unauthorized, _))),
                    "BackendUnauthorized",
                ),
                (health.suppressed, "Suppressed"),
            ],
        ),
        // `Accepted` first, matching every other kind: it is the condition a
        // printer column reads, and the one an operator checks before the rest.
        conditions: vec![accepted, refs, ready],
    }
}

/// Desired `InferenceProvider` status for one kinference REFUSED.
///
/// `observed` is cleared rather than left stale: a provider producing no
/// candidates has no live latency, and leaving the last p95 in place would let a
/// refused destination keep advertising a figure a policy could rank on.
///
/// `Accepted=False` rather than `ResolvedRefs=False` — nothing was resolved far
/// enough to have a ref problem. The spec itself is the thing to fix.
pub fn rejected_provider_status(
    reason: super::store::UnresolvedReason,
    previous: Option<&InferenceProviderStatus>,
) -> InferenceProviderStatus {
    use super::store::UnresolvedReason as R;
    let prev = previous.map(|s| s.conditions.as_slice());
    let msg = match reason {
        R::UnsupportedApiFormat => {
            "spec.api names a wire format nothing translates to. Only OpenAI is \
             supported; point baseURL at the vendor's OpenAI-compatible endpoint, or \
             front it with a proxy that exposes one"
        }
        R::Incomplete => {
            "spec is missing something required: backendRef.name, api, or at least \
             one models[] row naming what the vendor calls a model"
        }
        R::CrossNamespaceNotGranted => {
            "backendRef crosses namespaces, and no ReferenceGrant in the target \
             namespace lets InferenceProviders from this namespace reference it"
        }
        R::AmbiguousName => {
            "another destination of a different kind in this pool has the same name, \
             so a policy naming it would be ambiguous"
        }
        _ => "the pool refused this destination; see the pool's unresolvedDestinations",
    };
    InferenceProviderStatus {
        state: state(reason.as_str(), &[]),
        conditions: vec![
            condition(
                "Accepted",
                false,
                reason.as_str(),
                msg,
                None,
                find(prev, "Accepted"),
            ),
            condition(
                "Ready",
                false,
                "NotAccepted",
                "refused, so it produces no candidates",
                None,
                find(prev, "Ready"),
            ),
        ],
    }
}

/// Desired `InferenceWorkerSet` status.
pub fn desired_worker_set_status(
    dest: &ResolvedWorkerSet,
    facts: &HashMap<PoolId, PoolFacts>,
    referenced_by: &[String],
    generation: Option<i64>,
    previous: Option<&InferenceWorkerSetStatus>,
) -> InferenceWorkerSetStatus {
    let f = facts.get(&dest.pool_id).cloned().unwrap_or_default();
    let models: Vec<WorkerSetModelStatus> = f
        .models
        .iter()
        .map(|m| WorkerSetModelStatus {
            // Keyed on what the engines actually reported. Aliases hang off it
            // rather than replacing it, so an undeclared model still reports
            // cleanly and a multi-alias one loses nothing.
            upstream_name: m.upstream.clone(),
            aliases: dest.aliases_for(&m.upstream),
            ready_endpoints: Some(m.ready_endpoints),
            context_window: (m.ctx_min.is_some() || m.ctx_max.is_some()).then_some(ContextWindow {
                min: m.ctx_min,
                max: m.ctx_max,
            }),
        })
        .collect();
    let prev = previous.map(|s| s.conditions.as_slice());
    // Reaching here means the serviceRef resolved to a pool kinference watches; an
    // unresolvable one never becomes a destination and is reported on the POOL
    // instead, as `unresolvedDestinations`.
    let ready = f.ready_endpoints > 0;
    // Deliberately NOT folded into `Accepted`, the way an unreferenced POLICY is.
    // A policy that no pool names does nothing at all, so `Accepted=False` is the
    // truth there. A worker set that no pool names still serves every `Pick` that
    // names no pool, so saying `Accepted=False` would report a set that is
    // carrying traffic as one nothing can reach. Separate question, separate
    // condition.
    let referenced = !referenced_by.is_empty();
    let referenced_msg = if referenced {
        format!("destination of pool(s) {}", referenced_by.join(", "))
    } else {
        format!(
            "no InferencePool lists this set in spec.destinations, so no pool-scoped policy \
             applies to it. It still serves a Pick that names no pool. Attach it with: \
             kubectl -n {} patch infpool <pool> --type=json -p \
             '[{{\"op\":\"add\",\"path\":\"/spec/destinations/-\",\"value\":\
{{\"kind\":\"InferenceWorkerSet\",\"name\":\"{}\"}}}}]'",
            dest.namespace, dest.name
        )
    };
    InferenceWorkerSetStatus {
        // `Unreferenced` is a qualifier, not a verdict: the set is accepted and
        // serving, it just has no pool-scoped policy over it.
        state: state(
            "Accepted",
            &[(!ready, "NotReady"), (!referenced, "Unreferenced")],
        ),
        models,
        referenced_by: referenced_by.to_vec(),
        endpoints: Some(EndpointCounts {
            ready: Some(f.ready_endpoints),
        }),
        // Always reported, including `configured: false` — the whole point is
        // that a missing event stream is otherwise invisible, so omitting the
        // block for the case that needs it most would defeat it.
        kv_events: Some(KvEventsStatus {
            configured: Some(dest.kv_events_declared),
            subscribed_endpoints: Some(f.event_endpoints),
        }),
        conditions: vec![
            condition(
                "Accepted",
                true,
                "Accepted",
                &format!("materialized as a pool; Service `{}`", dest.service_name),
                generation,
                find(prev, "Accepted"),
            ),
            condition(
                "Ready",
                ready,
                if ready {
                    "EndpointsReady"
                } else {
                    "NoReadyEndpoints"
                },
                &format!("{} ready endpoint(s)", f.ready_endpoints),
                generation,
                find(prev, "Ready"),
            ),
            condition(
                "Referenced",
                referenced,
                if referenced {
                    "InPool"
                } else {
                    "NoPoolNamesIt"
                },
                &referenced_msg,
                generation,
                find(prev, "Referenced"),
            ),
        ],
    }
}

/// Desired status for a set kinference has no pool for, and that no rejection
/// explains.
///
/// Normally the brief window between the object appearing and the next
/// reconcile. Persistent means `serviceRef` names a Service that does not exist.
/// A pool naming this set would report that under `unresolvedDestinations` — but
/// an unreferenced set is in no pool, so without this the object says nothing at
/// all and the operator has only the router's logs.
pub fn unbound_worker_set_status(
    reason: UnresolvedReason,
    referenced_by: &[String],
    generation: Option<i64>,
    previous: Option<&InferenceWorkerSetStatus>,
) -> InferenceWorkerSetStatus {
    let prev = previous.map(|s| s.conditions.as_slice());
    InferenceWorkerSetStatus {
        state: state(reason.as_str(), &[]),
        models: Vec::new(),
        referenced_by: referenced_by.to_vec(),
        endpoints: Some(EndpointCounts { ready: Some(0) }),
        kv_events: None,
        conditions: vec![
            condition(
                "Accepted",
                false,
                reason.as_str(),
                "kinference has no pool for this worker set, so nothing routes to it",
                generation,
                find(prev, "Accepted"),
            ),
            condition(
                "Ready",
                false,
                "NotAccepted",
                "the worker set was not accepted, so it has no endpoints",
                generation,
                find(prev, "Ready"),
            ),
        ],
    }
}

/// Desired `InferencePolicy` status.
///
/// Written for EVERY policy in scope, not only the ones a pool references. An
/// unreferenced policy is still compiled and still reports its warnings — that
/// is the state you want to inspect before attaching it — and the
/// `Accepted` condition is what says whether anything routes through it.
///
/// Reaching policies only through pools was how an unattached policy ended up
/// with a permanently blank `RULES` column, indistinguishable from one that had
/// failed to compile; and a DETACHED policy kept its last-written count forever,
/// so several dead policies read exactly like several live ones.
pub fn desired_policy_status(
    resolved: &ResolvedPolicy,
    previous: Option<&InferencePolicyStatus>,
) -> InferencePolicyStatus {
    let policy = &resolved.policy;
    let generation = resolved.generation;
    let prev = previous.map(|s| s.conditions.as_slice());
    // Compile warnings and name warnings, reported as one list: an operator
    // asking "what is wrong with this policy" should not need to know which
    // stage noticed. Warnings never make a policy unusable — each describes a
    // part that does less than it says, and the rest still applies.
    let warnings: Vec<String> = resolved.all_warnings().map(ToString::to_string).collect();
    let compiled = if warnings.is_empty() {
        format!("{} rule(s) compiled", policy.rules.len())
    } else {
        format!(
            "{} rule(s) compiled with {} warning(s)",
            policy.rules.len(),
            warnings.len()
        )
    };
    // `Accepted` here answers "does anything route through this", which is the
    // question a policy object cannot otherwise answer. Compiling cleanly is not
    // the same as being in force, and conflating them is what made a detached
    // policy look live.
    let accepted = !resolved.referenced_by.is_empty();
    let (reason, msg) = if accepted {
        (
            "Accepted",
            format!(
                "{compiled}; in force on pool(s) {}",
                resolved.referenced_by.join(", ")
            ),
        )
    } else {
        (
            "NotReferenced",
            format!(
                "{compiled}, but no InferencePool names it in spec.policyRef, so it is not in force. \
                 Attach it with: kubectl -n {} patch infpool <pool> --type=merge \
                 -p '{{\"spec\":{{\"policyRef\":{{\"name\":\"{}\"}}}}}}'",
                resolved.namespace, resolved.name
            ),
        )
    };
    // Joined here rather than left as a list: a `[*]` printer column renders only
    // the first element, so an array would print `NotReferenced` and drop
    // `HasWarnings` without a trace. Ordered most-decisive first, and never
    // empty — a blank column has to keep meaning "no status written at all".
    let mut state = vec![if accepted { "InForce" } else { "NotReferenced" }];
    if !warnings.is_empty() {
        state.push("HasWarnings");
    }

    InferencePolicyStatus {
        state: Some(state.join(",")),
        referenced_by: resolved.referenced_by.clone(),
        warnings: fit_status_warnings(warnings),
        conditions: vec![condition(
            "Accepted",
            accepted,
            reason,
            &msg,
            generation,
            find(prev, "Accepted"),
        )],
    }
}

/// `status.warnings` limits from the InferencePolicy CRD schema
/// (`charts/kinference-crds/templates/kinference.buoyant.io_inferencepolicies.yaml`).
const STATUS_WARNINGS_MAX_ITEMS: usize = 32;
const STATUS_WARNING_MAX_CHARS: usize = 2048;

/// Fit warnings inside what the CRD schema admits.
///
/// Going over EITHER limit makes the API server reject the whole status patch,
/// not just the warnings — so `state`, `referencedBy` and the conditions all go
/// stale, the writer retries every tick, and the only trace is a WARN log line.
/// A policy may have 32 rules, each able to raise several warnings, so an
/// ordinary policy can reach this; it is not a pathological case.
///
/// So: keep the first 31, then say how many were cut and where the full list
/// lives. Each entry is truncated on a character boundary — the schema counts
/// characters, and slicing bytes could split a multi-byte one.
fn fit_status_warnings(all: Vec<String>) -> Vec<String> {
    let clip = |w: String| {
        if w.chars().count() <= STATUS_WARNING_MAX_CHARS {
            w
        } else {
            let mut t: String = w.chars().take(STATUS_WARNING_MAX_CHARS - 1).collect();
            t.push('…');
            t
        }
    };
    if all.len() <= STATUS_WARNINGS_MAX_ITEMS {
        return all.into_iter().map(clip).collect();
    }
    let kept = STATUS_WARNINGS_MAX_ITEMS - 1;
    let cut = all.len() - kept;
    let mut out: Vec<String> = all.into_iter().take(kept).map(clip).collect();
    out.push(format!(
        "…and {cut} more warning(s), not shown because status holds at most \
         {STATUS_WARNINGS_MAX_ITEMS}. The full list is in GET /api/policy/context on the admin \
         port, or run `kinfctl check` on the manifest"
    ));
    out
}

type CacheKey = (&'static str, String, String, Option<String>);

/// The status writer's last-written cache key: kind, namespace, name, UID.
///
/// The UID is part of it, and that is the point. Keyed on name alone, a
/// delete-and-recreate under the same name inherited its predecessor's cache
/// entry: the recreated object starts with EMPTY status, the writer computes the
/// same value it had cached, sees a hit, and skips the patch — leaving the object
/// blank until its content happened to change or the process restarted. The cache
/// is in-memory and never reconciled against the API server, so nothing else
/// would have noticed.
fn cache_key(kind: &'static str, ns: &str, name: &str, uid: Option<&str>) -> CacheKey {
    (
        kind,
        ns.to_string(),
        name.to_string(),
        uid.map(str::to_string),
    )
}

/// Patches status onto the CRDs, on a tick, only when something changed.
pub struct StatusWriter {
    client: kube::Client,
    store: Arc<PolicyStore>,
    /// The live pool set. Held as the registry rather than a startup snapshot:
    /// status is a report about what kinference can route to right now, so a writer
    /// describing the pools that existed at boot is worse than one that
    /// describes nothing.
    pools: Arc<crate::pool::PoolRegistry>,
    /// Worker sets that produced no pool. They appear in no `PolicyIndex`, so
    /// without this the object an operator is actually looking at says nothing
    /// about why nothing routes to it.
    rejections: Arc<super::worker_sets::RejectionStore>,
    metrics: Arc<MetricsRegistry>,
    /// Caller-reported provider health. The only source for a provider's status,
    /// since kinference observes nothing about one directly.
    provider_health: Arc<super::provider_health::ProviderHealthStore>,
    /// Last status we wrote, per `(kind, namespace, name)`. Both the loop-breaker
    /// and the source of stable `lastTransitionTime`s.
    last: Mutex<HashMap<CacheKey, serde_json::Value>>,
    /// Whether this replica may write. Always `true` when leader election is
    /// disabled (the caller hands over a channel pre-set to `true` and spawns no
    /// elector), so single-replica behavior is unchanged.
    leader: watch::Receiver<bool>,
    prom: Arc<crate::prom::PromMetrics>,
    /// Policy warnings already logged, as `ns/name: warning`. A warning is
    /// logged once, when it first appears, rather than every tick — the same
    /// contract as `binding::warn_new`.
    warned: Mutex<HashSet<String>>,
}

impl StatusWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: kube::Client,
        store: Arc<PolicyStore>,
        pools: Arc<crate::pool::PoolRegistry>,
        rejections: Arc<super::worker_sets::RejectionStore>,
        metrics: Arc<MetricsRegistry>,
        provider_health: Arc<super::provider_health::ProviderHealthStore>,
        leader: watch::Receiver<bool>,
        prom: Arc<crate::prom::PromMetrics>,
    ) -> Self {
        Self {
            client,
            store,
            pools,
            rejections,
            metrics,
            provider_health,
            leader,
            prom,
            last: Mutex::new(HashMap::new()),
            warned: Mutex::new(HashSet::new()),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("policy status writer: cancelled");
                    return;
                }
                // Awaiting the transition rather than polling `borrow()` each
                // tick is what makes promotion observable at the moment it
                // happens, which the cache reset below depends on.
                r = self.leader.changed() => {
                    match r {
                        Ok(()) => {
                            let now_leader = *self.leader.borrow_and_update();
                            self.prom.set_leader(now_leader);
                            if now_leader {
                                self.on_promotion();
                            } else {
                                self.on_demotion();
                            }
                        }
                        // Elector gone. Hold the current state rather than
                        // assuming either way: promoting ourselves could create
                        // a second writer, and demoting could leave nobody
                        // writing at all.
                        Err(_) => {
                            tracing::error!(
                                "policy status writer: leader channel closed; \
                                 holding the last known leadership state"
                            );
                            self.prom.record_leader_election_error();
                        }
                    }
                }
                _ = interval.tick() => {
                    if !*self.leader.borrow() {
                        continue;
                    }
                    if let Err(e) = self.reconcile_once().await {
                        // Never fatal: status is reporting, and losing it must
                        // not take routing down with it.
                        tracing::warn!(error = %e, "policy status writer: reconcile failed");
                    }
                }
            }
        }
    }

    /// Drop the last-written cache on becoming leader.
    ///
    /// The cache is this replica's private record of what IT wrote. After a
    /// failover that record describes the world before the previous leader took
    /// over, so trusting it means comparing desired status against a stale
    /// baseline and skipping patches that are genuinely needed — status silently
    /// frozen, with no error anywhere to point at it. Clearing means "I don't
    /// know what my predecessor left; re-assert everything", which costs one
    /// extra round of patches per promotion and is the only safe assumption.
    ///
    /// It also resets `lastTransitionTime` provenance: with no `previous` to
    /// carry forward, the next write stamps fresh timestamps. That is correct —
    /// this replica genuinely has not observed those conditions before.
    fn on_promotion(&self) {
        let dropped = {
            let mut last = self.last.lock().expect("status cache poisoned");
            let n = last.len();
            last.clear();
            n
        };
        tracing::info!(
            dropped_cache_entries = dropped,
            "policy status writer: promoted to leader; re-asserting all status"
        );
    }

    /// Withdraw the policy warning counts on losing the lease.
    ///
    /// Only `reconcile_once` sets `kinference_policy_warnings`, and a follower
    /// never runs it, so nothing else would clear them: a demoted replica kept
    /// exporting the counts it had at demotion, beside the new leader's, and a
    /// warning fixed after a failover stayed visible on it indefinitely.
    fn on_demotion(&self) {
        self.prom.set_policy_warnings(&[]);
        tracing::info!("policy status writer: demoted; stopping writes");
    }

    async fn reconcile_once(&self) -> anyhow::Result<()> {
        let snapshot = self.store.snapshot();
        // NOT `snapshot.is_empty()`, which asks only about POOLS. Worker sets
        // and policies are patched outside the pool loop now, so a cluster with
        // worker sets but no pool still has objects to describe — and bailing
        // here was one of the two ways an unreferenced set ended up statusless.
        if !snapshot.listed() {
            return Ok(());
        }
        // One snapshot for the whole reconcile: every CR patched below then
        // describes the same fleet.
        let facts = gather_facts(self.pools.snapshot().as_slice(), &self.metrics);

        for resolved in snapshot.pools() {
            // --- pool -----------------------------------------------------
            let prev = self.previous::<InferencePoolStatus>(
                "InferencePool",
                &resolved.namespace,
                &resolved.name,
                resolved.uid.as_deref(),
            );
            let desired = desired_pool_status(resolved, &facts, prev.as_ref());
            self.patch::<InferencePool>(
                "InferencePool",
                &resolved.namespace,
                &resolved.name,
                resolved.uid.as_deref(),
                &desired,
            )
            .await;

            // --- providers -------------------------------------------------
            //
            // Everything here is second-hand. kinference never reads the
            // GatewayBackend, so a provider's own object is the only place the
            // caller's reports become visible to an operator — and it is where
            // they will look when requests to a vendor start failing.
            for prov in resolved.destinations.iter().filter_map(|d| d.as_provider()) {
                let key = crate::policy::engine::ObjectKey::new(&prov.namespace, &prov.name);
                let prev = self.previous::<InferenceProviderStatus>(
                    "InferenceProvider",
                    &prov.namespace,
                    &prov.name,
                    prov.uid.as_deref(),
                );
                let desired =
                    desired_provider_status(&self.provider_health.health(&key), prev.as_ref());
                self.patch::<InferenceProvider>(
                    "InferenceProvider",
                    &prov.namespace,
                    &prov.name,
                    prov.uid.as_deref(),
                    &desired,
                )
                .await;
            }

            // --- rejected providers ----------------------------------------
            //
            // A rejected provider never becomes a destination, so the loop above
            // never sees it. The pool reports it under
            // `unresolvedDestinations`, but the operator is looking at the
            // PROVIDER, and a blank status there reads as "nothing happened"
            // rather than "refused, and here is why".
            for u in resolved
                .unresolved
                .iter()
                .filter(|u| u.kind == "InferenceProvider" && !u.name.starts_with('<'))
            {
                let prev = self.previous::<InferenceProviderStatus>(
                    "InferenceProvider",
                    &resolved.namespace,
                    &u.name,
                    u.uid.as_deref(),
                );
                let desired = rejected_provider_status(u.reason, prev.as_ref());
                self.patch::<InferenceProvider>(
                    "InferenceProvider",
                    &resolved.namespace,
                    &u.name,
                    u.uid.as_deref(),
                    &desired,
                )
                .await;
            }
        }

        // --- worker sets --------------------------------------------------
        //
        // Outside the pool loop, and over the SETS rather than over pool
        // destinations. Reaching them through pools meant a set no pool named
        // was never patched at all — not written as zero, never written — so a
        // set serving traffic happily was indistinguishable from an object kinference
        // had never seen, and the one set with a ready endpoint could be the one
        // with a blank row. A set's endpoints, models and event subscription are
        // facts about the SET; none of them need a pool to be true.
        //
        // Three outcomes, each answering a different operator question: rejected
        // (this object is wrong), unbound (kinference has no pool for it), bound
        // (here is what it serves, and whether a pool routes to it).
        let rejected = self.rejections.snapshot();
        for entry in snapshot.worker_sets() {
            let prev = self.previous::<InferenceWorkerSetStatus>(
                "InferenceWorkerSet",
                &entry.namespace,
                &entry.name,
                entry.uid.as_deref(),
            );
            // A rejection wins over the bind failure it causes: it names the
            // unknown profile or the unusable block size, where the bind error
            // can only say the pool is missing. Same object, and the specific
            // message is the one worth writing.
            let rejection = rejected.iter().find(|r| {
                r.key.name == entry.name
                    && r.key.namespace.as_deref().unwrap_or_default() == entry.namespace
            });
            let desired = match (rejection, &entry.bound) {
                (Some(r), _) => {
                    rejected_worker_set_status(&r.reason, &entry.referenced_by, prev.as_ref())
                }
                (None, Ok(dest)) => desired_worker_set_status(
                    dest,
                    &facts,
                    &entry.referenced_by,
                    entry.generation,
                    prev.as_ref(),
                ),
                (None, Err(reason)) => unbound_worker_set_status(
                    *reason,
                    &entry.referenced_by,
                    entry.generation,
                    prev.as_ref(),
                ),
            };
            self.patch::<InferenceWorkerSet>(
                "InferenceWorkerSet",
                &entry.namespace,
                &entry.name,
                entry.uid.as_deref(),
                &desired,
            )
            .await;
        }

        // --- policies -----------------------------------------------------
        //
        // Outside the pool loop, deliberately. A policy no pool references would
        // never be reached from in there, and that was the whole bug: an
        // unattached policy sat with a blank RULES column forever, and a DETACHED
        // one kept its last-written count, so a dead policy was indistinguishable
        // from a live one. Iterating the policies themselves means every one gets
        // an answer, and `Accepted` carries which it is.
        self.report_policy_warnings(&snapshot);
        for resolved in snapshot.policies() {
            let prev = self.previous::<InferencePolicyStatus>(
                "InferencePolicy",
                &resolved.namespace,
                &resolved.name,
                resolved.uid.as_deref(),
            );
            let desired = desired_policy_status(resolved, prev.as_ref());
            self.patch::<InferencePolicy>(
                "InferencePolicy",
                &resolved.namespace,
                &resolved.name,
                resolved.uid.as_deref(),
                &desired,
            )
            .await;
        }
        Ok(())
    }

    /// Log warnings that are new since the last pass, and publish per-policy
    /// counts to `kinference_policy_warnings`.
    ///
    /// Logged because the people most likely to have just introduced a warning
    /// are watching `kubectl apply` and the logs, not the status subresource.
    /// Once per appearance, because a WARN line repeated every ten seconds is
    /// one nobody reads.
    fn report_policy_warnings(&self, snapshot: &super::store::PolicyIndex) {
        let mut current: HashSet<String> = HashSet::new();
        let mut rows: Vec<(String, &'static str, i64)> = Vec::new();
        for resolved in snapshot.policies() {
            let key = format!("{}/{}", resolved.namespace, resolved.name);
            let mut by_code: BTreeMap<&'static str, i64> = BTreeMap::new();
            for w in resolved.all_warnings() {
                *by_code.entry(w.code.as_str()).or_default() += 1;
                current.insert(format!("{key}: {w}"));
            }
            rows.extend(by_code.into_iter().map(|(code, n)| (key.clone(), code, n)));
        }
        self.prom.set_policy_warnings(&rows);
        let mut warned = self.warned.lock().expect("policy warn set poisoned");
        for line in &current {
            if !warned.contains(line) {
                tracing::warn!("policy warning: {line}");
            }
        }
        *warned = current;
    }

    fn previous<T: serde::de::DeserializeOwned>(
        &self,
        kind: &'static str,
        ns: &str,
        name: &str,
        uid: Option<&str>,
    ) -> Option<T> {
        let last = self.last.lock().expect("status cache poisoned");
        let v = last.get(&cache_key(kind, ns, name, uid))?;
        serde_json::from_value(v.clone()).ok()
    }

    /// Patch `status` if it differs from what we last wrote.
    ///
    /// The comparison is load-bearing: we watch what we patch, so writing
    /// unconditionally would drive an endless rebuild/write cycle.
    async fn patch<K>(
        &self,
        kind: &'static str,
        ns: &str,
        name: &str,
        uid: Option<&str>,
        desired: &impl serde::Serialize,
    ) where
        K: kube::Resource<Scope = kube::core::NamespaceResourceScope>
            + serde::de::DeserializeOwned
            + Clone
            + std::fmt::Debug,
        K::DynamicType: Default,
    {
        let Ok(value) = serde_json::to_value(desired) else {
            return;
        };
        let key = cache_key(kind, ns, name, uid);
        {
            let last = self.last.lock().expect("status cache poisoned");
            if last.get(&key) == Some(&value) {
                return;
            }
        }
        let api: Api<K> = Api::namespaced(self.client.clone(), ns);
        let patch = serde_json::json!({ "status": value });
        match api
            .patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => {
                tracing::debug!(kind, ns, name, "policy status: patched");
                self.last
                    .lock()
                    .expect("status cache poisoned")
                    .insert(key, value);
            }
            Err(e) => {
                // Don't cache on failure, so the next tick retries.
                tracing::warn!(kind, ns, name, error = %e, "policy status: patch failed");
            }
        }
    }
}

/// Convenience for `main`: build the writer with its own client.
///
/// `leader` gates writing. When leader election is disabled the caller passes a
/// channel pre-set to `true`, so this replica always writes and behavior matches
/// the single-replica case exactly.
#[allow(clippy::too_many_arguments)]
pub async fn spawn(
    namespace_hint: Option<&str>,
    store: Arc<PolicyStore>,
    pools: Arc<crate::pool::PoolRegistry>,
    rejections: Arc<super::worker_sets::RejectionStore>,
    metrics: Arc<MetricsRegistry>,
    provider_health: Arc<super::provider_health::ProviderHealthStore>,
    leader: watch::Receiver<bool>,
    prom: Arc<crate::prom::PromMetrics>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let _ = namespace_hint;
    // Same reasoning as `PolicyWatcher::new`: status is CRD-only, so kubeconfig
    // is safe here even though discovery rejects it.
    let cfg = kube::Config::infer().await?;
    let client = kube::Client::try_from(cfg)?;
    // Seed the gauge so a follower reports 0 from the start rather than being
    // absent from /metrics until its first transition.
    prom.set_leader(*leader.borrow());
    let writer = StatusWriter::new(
        client,
        store,
        pools,
        rejections,
        metrics,
        provider_health,
        leader,
        prom,
    );
    tokio::spawn(async move { writer.run(cancel).await });
    Ok(())
}

#[cfg(test)]
mod tests;
