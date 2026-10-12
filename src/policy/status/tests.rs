use super::*;
use crate::policy::compile::CompiledPolicy;
use crate::policy::crd::{InferencePolicySpec, ModelAlias, ParentRef};
use crate::policy::store::{Unresolved, UnresolvedReason};

fn dest(name: &str, aliases: &[(&str, &str)]) -> ResolvedWorkerSet {
    ResolvedWorkerSet {
        name: name.into(),
        namespace: "infra".into(),
        labels: BTreeMap::new(),
        pool_id: PoolId::from_raw(0),
        service_name: "vllm-h100".into(),
        service_port: None,
        kv_events_declared: false,
        aliases: aliases
            .iter()
            // Fixtures still pass (canonical, upstream); under the new shape
            // the canonical is an ALIAS of the upstream, so every existing
            // case asks the same question.
            .map(|(n, u)| ModelAlias {
                name: Some(u.to_string()),
                aliases: if n == u { vec![] } else { vec![n.to_string()] },
            })
            .collect(),
    }
}

fn facts(
    ready: i64,
    models: &[(&str, i64, Option<i64>, Option<i64>)],
) -> HashMap<PoolId, PoolFacts> {
    let mut m = HashMap::new();
    m.insert(
        PoolId::from_raw(0),
        PoolFacts {
            ready_endpoints: ready,
            event_endpoints: ready,
            models: models
                .iter()
                .map(|(u, n, lo, hi)| ModelFact {
                    upstream: u.to_string(),
                    ready_endpoints: *n,
                    ctx_min: *lo,
                    ctx_max: *hi,
                })
                .collect(),
        },
    );
    m
}

/// A `ResolvedPolicy` wrapper for the status tests. `referenced_by` non-empty
/// by default, since that is the case most of these assert about.
fn rp(policy: CompiledPolicy) -> ResolvedPolicy {
    ResolvedPolicy {
        uid: None,
        namespace: "infra".into(),
        name: "chat-policy".into(),
        generation: Some(7),
        policy: Arc::new(policy),
        // Status never reads the raw spec; these fixtures start from a
        // `CompiledPolicy`, so there is no spec to carry.
        spec: Default::default(),
        referenced_by: vec!["chat".into()],
        reference_warnings: Vec::new(),
    }
}

/// The same policy with nothing pointing at it.
fn rp_unreferenced(policy: CompiledPolicy) -> ResolvedPolicy {
    ResolvedPolicy {
        referenced_by: Vec::new(),
        ..rp(policy)
    }
}

fn resolved(dests: Vec<ResolvedWorkerSet>, unresolved: Vec<Unresolved>) -> ResolvedPool {
    ResolvedPool {
        uid: None,
        namespace: "infra".into(),
        name: "chat".into(),
        resource_version: "100".into(),
        generation: Some(3),
        policy: Arc::new(CompiledPolicy::serve_as_asked("<implicit>")),
        dangling_policy: None,
        destinations: dests
            .into_iter()
            .map(crate::policy::store::ResolvedDestination::Local)
            .collect(),
        unresolved,
        parents: vec![ParentRef {
            kind: Some("Gateway".into()),
            name: "inference-gateway".into(),
            namespace: Some("infra".into()),
            ..Default::default()
        }],
    }
}

/// A rejected worker set has to say so on ITS OWN object. It appears in no
/// policy snapshot, so without this the operator sees either nothing or —
/// worse — a stale `Accepted=True` with a ready count from before the
/// break, which reads as "serving, just unhappy".
fn pcond<'a>(s: &'a InferenceProviderStatus, ty: &str) -> &'a Condition {
    s.conditions
        .iter()
        .find(|c| c.r#type == ty)
        .expect("condition present")
}

/// No condition message may carry a run of spaces.
///
/// These are read in `kubectl describe` and `-o yaml`, so a squashed line
/// continuation shows up verbatim as `the caller's to              report`.
/// Six of them had exactly that, and nothing caught it — the strings compile,
/// the tests that assert on them use `contains`, and only a human reading
/// live output notices.
#[test]
fn no_condition_message_has_a_double_space() {
    let d = dest("workers-h100", &[]);
    let health = |suppressed, fail| ProviderHealth {
        suppressed,
        last_failure: fail,
    };
    let mut msgs: Vec<String> = Vec::new();
    let mut collect = |conds: &[Condition]| {
        msgs.extend(conds.iter().map(|c| c.message.clone()));
    };
    collect(&desired_worker_set_status(&d, &facts(0, &[]), &[], None, None).conditions);
    collect(
        &desired_worker_set_status(&d, &facts(2, &[]), &["p".to_string()], None, None).conditions,
    );
    collect(
        &rejected_worker_set_status(
            &crate::policy::rejection::RejectReason::MissingKvBlockSize,
            &[],
            None,
        )
        .conditions,
    );
    collect(
        &unbound_worker_set_status(UnresolvedReason::NoMatchingPool, &[], None, None).conditions,
    );
    collect(
        &desired_pool_status(
            &resolved(vec![dest("w", &[])], vec![]),
            &facts(1, &[]),
            None,
        )
        .conditions,
    );
    collect(&desired_provider_status(&health(false, None), None).conditions);
    collect(
        &desired_provider_status(
            &health(true, Some((FailureKind::Unauthorized, "401".into()))),
            None,
        )
        .conditions,
    );
    for r in [
        UnresolvedReason::UnsupportedApiFormat,
        UnresolvedReason::Incomplete,
        UnresolvedReason::CrossNamespaceNotGranted,
        UnresolvedReason::AmbiguousName,
    ] {
        collect(&rejected_provider_status(r, None).conditions);
    }
    collect(&desired_policy_status(&rp_unreferenced(one_rule()), None).conditions);

    assert!(
        !msgs.is_empty(),
        "the harness must actually collect messages"
    );
    for m in &msgs {
        assert!(
            !m.contains("  "),
            "condition message has a run of spaces: {m:?}"
        );
    }
}

/// One column, every kind, same grammar: primary verdict then qualifiers.
/// A `[*]` column renders only the first array element, so each of these
/// would lose everything after the comma if `state` were a list.
#[test]
fn state_tokens_compose_across_the_kinds() {
    let d = dest("workers-h100", &[]);

    // Worker set: accepted, but idle AND in no pool. Neither is a fault, and
    // a column reading only `Accepted` showed none of it.
    let idle = desired_worker_set_status(&d, &facts(0, &[]), &[], None, None);
    assert_eq!(
        idle.state.as_deref(),
        Some("Accepted,NotReady,Unreferenced")
    );

    let serving = desired_worker_set_status(&d, &facts(3, &[]), &["chat".to_string()], None, None);
    assert_eq!(serving.state.as_deref(), Some("Accepted"));

    // A rejection IS the verdict, so it leads and stands alone — a refused
    // set has no endpoints by construction, and `NotReady` too would read as
    // a second, separate problem.
    let broken = rejected_worker_set_status(
        &crate::policy::rejection::RejectReason::MissingKvBlockSize,
        &[],
        None,
    );
    assert_eq!(broken.state.as_deref(), Some("MissingKvBlockSize"));

    // Pool: routes fine, but a destination dangles and the policyRef names
    // the wrong kind. Both are qualifiers because it still serves.
    let mut r = resolved(vec![dest("workers-h100", &[])], vec![]);
    r.unresolved = vec![Unresolved {
        kind: "InferenceWorkerSet".into(),
        name: "gone".into(),
        uid: None,
        reason: UnresolvedReason::NotFound,
    }];
    r.dangling_policy = Some(DanglingPolicy::WrongKind {
        name: "p".into(),
        group: "kinference.buoyant.io".into(),
        kind: "ConfigMap".into(),
    });
    let pool = desired_pool_status(&r, &facts(1, &[]), None);
    assert_eq!(
        pool.state.as_deref(),
        Some("Accepted,UnresolvedDestinations,PolicyRefWrongKind")
    );

    // Provider: well-formed and still producing nothing. `Suppressed` is the
    // token an operator needs, and it is invisible in the Accepted condition.
    let suppressed = desired_provider_status(
        &ProviderHealth {
            suppressed: true,
            last_failure: Some((FailureKind::Unauthorized, "401".into())),
        },
        None,
    );
    assert_eq!(
        suppressed.state.as_deref(),
        Some("Accepted,BackendUnauthorized,Suppressed")
    );
}

/// Never empty, on any kind — that is what keeps a blank column meaning "no
/// status was written at all" rather than "nothing to report".
#[test]
fn state_always_carries_at_least_one_token() {
    let d = dest("workers-h100", &[]);
    for st in [
        desired_worker_set_status(&d, &facts(1, &[]), &["p".to_string()], None, None)
            .state
            .clone(),
        desired_pool_status(
            &resolved(vec![dest("w", &[])], vec![]),
            &facts(1, &[]),
            None,
        )
        .state
        .clone(),
        desired_provider_status(
            &ProviderHealth {
                suppressed: false,
                last_failure: None,
            },
            None,
        )
        .state
        .clone(),
    ] {
        let st = st.expect("state is always written");
        assert!(!st.is_empty(), "and never the empty string");
    }
}

/// A healthy provider must carry `Accepted=True`. Without it, a provider the
/// operator FIXED lost the condition entirely rather than flipping — status
/// is a merge patch, so the array is replaced wholesale — and one that was
/// never broken never had it at all. Every other kind answers this question.
#[test]
fn a_healthy_provider_reports_accepted() {
    let s = desired_provider_status(
        &ProviderHealth {
            suppressed: false,
            last_failure: None,
        },
        None,
    );
    assert_eq!(pcond(&s, "Accepted").status, "True");
    assert_eq!(pcond(&s, "Accepted").reason, "Accepted");
    assert_eq!(
        s.conditions[0].r#type, "Accepted",
        "first, like every other kind, since a printer column reads it"
    );
    assert_eq!(pcond(&s, "ResolvedRefs").status, "True");
    assert_eq!(pcond(&s, "Ready").status, "True");
}

/// And the round trip: rejected then fixed must land on True, not on a
/// missing condition.
#[test]
fn a_provider_that_recovers_flips_accepted_to_true() {
    let broken = rejected_provider_status(UnresolvedReason::Incomplete, None);
    assert_eq!(pcond(&broken, "Accepted").status, "False");
    let fixed = desired_provider_status(
        &ProviderHealth {
            suppressed: false,
            last_failure: None,
        },
        Some(&broken),
    );
    assert_eq!(pcond(&fixed, "Accepted").status, "True");
}

fn cond<'a>(s: &'a InferenceWorkerSetStatus, ty: &str) -> &'a Condition {
    s.conditions
        .iter()
        .find(|c| c.r#type == ty)
        .expect("condition present")
}

/// The heart of it: a set no pool names is ACCEPTED, not rejected. It still
/// serves every `Pick` that names no pool, so `Accepted=False` would report
/// a set carrying traffic as one nothing can reach — which is why this does
/// not copy the unreferenced-POLICY shape, where `Accepted=False` is true.
#[test]
fn an_unreferenced_worker_set_is_accepted_but_not_referenced() {
    let d = dest("workers-h100", &[]);
    let s = desired_worker_set_status(&d, &facts(2, &[]), &[], None, None);

    assert_eq!(cond(&s, "Accepted").status, "True");
    assert_eq!(cond(&s, "Accepted").reason, "Accepted");
    assert_eq!(cond(&s, "Ready").status, "True", "2 ready endpoints");

    let r = cond(&s, "Referenced");
    assert_eq!(r.status, "False");
    assert_eq!(r.reason, "NoPoolNamesIt");
    assert!(r.message.contains("no pool-scoped policy"));
    assert!(
        r.message.contains("still serves a Pick that names no pool"),
        "must not read as unreachable"
    );
    assert!(
        r.message.contains("patch infpool"),
        "the message carries the fix: {}",
        r.message
    );
    assert!(s.referenced_by.is_empty());
}

#[test]
fn a_referenced_worker_set_names_the_pools_routing_to_it() {
    let d = dest("workers-h100", &[]);
    let refs = vec!["chat".to_string(), "batch".to_string()];
    let s = desired_worker_set_status(&d, &facts(1, &[]), &refs, None, None);

    let r = cond(&s, "Referenced");
    assert_eq!(r.status, "True");
    assert_eq!(r.reason, "InPool");
    assert!(r.message.contains("chat, batch"), "{}", r.message);
    assert_eq!(
        s.referenced_by, refs,
        "reported as a field, not only in prose"
    );
}

/// Unreferenced and unready are independent. The `dummy` case was the one
/// set with a READY endpoint and the only one with no status at all, so a
/// test that let them move together would not have caught it.
#[test]
fn referenced_and_ready_are_independent() {
    let d = dest("workers-h100", &[]);
    let unref_ready = desired_worker_set_status(&d, &facts(3, &[]), &[], None, None);
    assert_eq!(cond(&unref_ready, "Ready").status, "True");
    assert_eq!(cond(&unref_ready, "Referenced").status, "False");

    let refd_unready =
        desired_worker_set_status(&d, &facts(0, &[]), &["chat".to_string()], None, None);
    assert_eq!(cond(&refd_unready, "Ready").status, "False");
    assert_eq!(cond(&refd_unready, "Referenced").status, "True");
}

#[test]
fn an_unbound_worker_set_reports_the_reason_it_did_not_bind() {
    let s = unbound_worker_set_status(UnresolvedReason::NoMatchingPool, &[], Some(7), None);
    assert_eq!(cond(&s, "Accepted").status, "False");
    assert_eq!(cond(&s, "Accepted").reason, "NoMatchingPool");
    assert_eq!(cond(&s, "Accepted").observed_generation, Some(7));
    assert_eq!(s.endpoints.as_ref().unwrap().ready, Some(0));
    assert!(s.models.is_empty());
}

/// A broken object does not erase who was pointing at it — that is the pool
/// an operator now has to go fix.
#[test]
fn a_rejected_worker_set_still_reports_who_named_it() {
    let reason = crate::policy::rejection::RejectReason::MissingKvBlockSize;
    let s = rejected_worker_set_status(&reason, &["chat".to_string()], None);
    assert_eq!(s.referenced_by, vec!["chat".to_string()]);
    assert_eq!(cond(&s, "Accepted").status, "False");
}

/// Same inputs, same bytes — the writer breaks its patch loop on a
/// last-written comparison, so an unstable message would rewrite forever.
#[test]
fn unreferenced_status_is_stable_across_reconciles() {
    let d = dest("workers-h100", &[]);
    let a = desired_worker_set_status(&d, &facts(1, &[]), &[], None, None);
    let b = desired_worker_set_status(&d, &facts(1, &[]), &[], None, Some(&a));
    assert_eq!(
        serde_json::to_value(&a).unwrap(),
        serde_json::to_value(&b).unwrap()
    );
}

#[test]
fn a_rejected_worker_set_reports_why_and_clears_its_counts() {
    let reason = crate::policy::rejection::RejectReason::UnknownProfile {
        defined: vec!["h100".into(), "a100".into()],
    };
    let st = rejected_worker_set_status(&reason, &[], None);
    let accepted = st
        .conditions
        .iter()
        .find(|c| c.r#type == "Accepted")
        .unwrap();
    assert_eq!(accepted.status, "False");
    assert_eq!(accepted.reason, "UnknownProfile");
    assert!(
        accepted.message.contains("h100"),
        "the message must list the defined profiles: {}",
        accepted.message
    );
    assert_eq!(
        st.endpoints.as_ref().unwrap().ready,
        Some(0),
        "ready must be CLEARED, not left stale"
    );
    assert!(st.models.is_empty());
}

/// `lastTransitionTime` is stable while the condition is unchanged — the
/// same loop-breaker every other condition here relies on, or a permanently
/// broken worker set rewrites its status every tick.
#[test]
fn a_persistent_rejection_does_not_rewrite_its_transition_time() {
    let reason = crate::policy::rejection::RejectReason::MissingKvBlockSize;
    let first = rejected_worker_set_status(&reason, &[], None);
    let second = rejected_worker_set_status(&reason, &[], Some(&first));
    assert_eq!(
        first.conditions[0].last_transition_time,
        second.conditions[0].last_transition_time
    );
}

#[test]
fn pool_status_reports_count_models_and_accepted() {
    let r = resolved(vec![dest("workers-h100", &[])], vec![]);
    let s = desired_pool_status(
        &r,
        &facts(4, &[("gemma", 4, Some(32768), Some(131072))]),
        None,
    );
    assert_eq!(s.destination_count, Some(1));
    assert_eq!(s.models.len(), 1);
    assert_eq!(s.models[0].name, "gemma");
    assert_eq!(s.models[0].served_by, vec!["workers-h100"]);
    let cw = s.models[0].context_window.as_ref().unwrap();
    assert_eq!((cw.min, cw.max), (Some(32768), Some(131072)));
    // The handshake the gateway waits on.
    let accepted = s.parents[0]
        .conditions
        .iter()
        .find(|c| c.r#type == "Accepted")
        .unwrap();
    assert_eq!(accepted.status, "True");
    assert_eq!(
        s.parents[0].controller_name.as_deref(),
        Some(CONTROLLER_NAME)
    );
    assert_eq!(accepted.observed_generation, Some(3));
}

#[test]
fn pool_status_reports_models_under_their_canonical_names() {
    // The alias table is the whole reason status must translate: a client
    // asks for `bar`, the engine reports the upstream model.
    let r = resolved(
        vec![dest(
            "workers-h100",
            &[("bar", "meta-llama/Llama-3.3-70B-Instruct")],
        )],
        vec![],
    );
    let s = desired_pool_status(
        &r,
        &facts(2, &[("meta-llama/Llama-3.3-70B-Instruct", 2, None, None)]),
        None,
    );
    assert_eq!(s.models[0].name, "bar");
}

/// A pool answers "what can a client ask for", so every alias is its own
/// askable name and gets its own entry. `servedBy` duplicates across them,
/// which is accurate — it is one fleet reachable under two names. The old
/// inverse lookup collapsed them into whichever alias sorted first.
#[test]
fn a_pool_lists_every_alias_as_its_own_askable_model() {
    let r = resolved(
        vec![dest(
            "qwen-2-5-32b",
            &[
                ("qwen-2.5", "Qwen/Qwen2.5-32B"),
                ("qwen-simple", "Qwen/Qwen2.5-32B"),
            ],
        )],
        vec![],
    );
    let s = desired_pool_status(&r, &facts(2, &[("Qwen/Qwen2.5-32B", 2, None, None)]), None);
    let names: Vec<&str> = s.models.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["qwen-2.5", "qwen-simple"]);
    for m in &s.models {
        assert_eq!(
            m.served_by,
            vec!["qwen-2-5-32b"],
            "same fleet under both names"
        );
    }
}

/// Unaliased models still report under the upstream name, so enabling the
/// alias expansion did not change the common case.
#[test]
fn a_pool_falls_back_to_the_upstream_name_when_nothing_is_aliased() {
    let r = resolved(vec![dest("workers-h100", &[])], vec![]);
    let s = desired_pool_status(
        &r,
        &facts(1, &[("google/gemma-4-12B-it", 1, None, None)]),
        None,
    );
    assert_eq!(s.models[0].name, "google/gemma-4-12B-it");
}

#[test]
fn unresolved_destination_flips_resolved_refs_but_not_accepted() {
    // A pool with one good destination still routes.
    let r = resolved(
        vec![dest("good", &[])],
        vec![Unresolved {
            kind: "InferenceWorkerSet".into(),
            name: "gone".into(),
            uid: None,
            reason: UnresolvedReason::NotFound,
        }],
    );
    let s = desired_pool_status(&r, &facts(1, &[]), None);
    let c = &s.parents[0].conditions;
    assert_eq!(
        c.iter().find(|c| c.r#type == "Accepted").unwrap().status,
        "True"
    );
    let refs = c.iter().find(|c| c.r#type == "ResolvedRefs").unwrap();
    assert_eq!(refs.status, "False");
    assert_eq!(refs.reason, "UnresolvedDestinations");
    assert_eq!(
        s.unresolved_destinations[0].reason.as_deref(),
        Some("NotFound")
    );
}

#[test]
fn dangling_policy_ref_flips_resolved_refs() {
    let mut r = resolved(vec![dest("good", &[])], vec![]);
    r.dangling_policy = Some(DanglingPolicy::NotFound {
        name: "nope".into(),
    });
    let s = desired_pool_status(&r, &facts(1, &[]), None);
    let refs = s.parents[0]
        .conditions
        .iter()
        .find(|c| c.r#type == "ResolvedRefs")
        .unwrap();
    assert_eq!(refs.status, "False");
    assert_eq!(refs.reason, "PolicyNotFound");
    assert!(refs.message.contains("nope"));
}

#[test]
fn no_destinations_means_not_accepted() {
    let r = resolved(vec![], vec![]);
    let s = desired_pool_status(&r, &HashMap::new(), None);
    let accepted = s.parents[0]
        .conditions
        .iter()
        .find(|c| c.r#type == "Accepted")
        .unwrap();
    assert_eq!(accepted.status, "False");
    assert_eq!(accepted.reason, "NoDestinations");
}

#[test]
fn last_transition_time_is_stable_while_the_condition_holds() {
    // Otherwise every tick looks like a state change to anything watching.
    let r = resolved(vec![dest("good", &[])], vec![]);
    let first = desired_pool_status(&r, &facts(1, &[]), None);
    let t0 = first.parents[0].conditions[0].last_transition_time.clone();
    let second = desired_pool_status(&r, &facts(1, &[]), Some(&first));
    assert_eq!(second.parents[0].conditions[0].last_transition_time, t0);
}

#[test]
fn last_transition_time_moves_when_the_condition_flips() {
    // Seeded with an obviously old timestamp: `metav1.Time` is
    // second-granularity, so comparing against a just-generated one would
    // pass or fail depending on which side of a second boundary the test
    // ran. Anchoring to a fixed past value tests the invariant directly.
    const OLD: &str = "2020-01-01T00:00:00Z";
    let good = resolved(vec![dest("good", &[])], vec![]);
    let mut first = desired_pool_status(&good, &facts(1, &[]), None);
    for c in &mut first.parents[0].conditions {
        c.last_transition_time = OLD.to_string();
    }

    // Unchanged condition: the old timestamp is carried forward.
    let same = desired_pool_status(&good, &facts(1, &[]), Some(&first));
    assert_eq!(same.parents[0].conditions[0].last_transition_time, OLD);

    // Flipped condition: a fresh timestamp replaces it.
    let empty = resolved(vec![], vec![]);
    let flipped = desired_pool_status(&empty, &HashMap::new(), Some(&first));
    assert_eq!(flipped.parents[0].conditions[0].status, "False");
    assert_ne!(
        flipped.parents[0].conditions[0].last_transition_time, OLD,
        "a real transition must restamp the time"
    );
}

/// A `StatusWriter` whose client is never used — `Config::new` performs no
/// I/O, so this needs no cluster. Lets the cache logic be tested directly.
fn writer_with_leader(leader: bool) -> (StatusWriter, watch::Sender<bool>) {
    let cfg = kube::Config::new("http://127.0.0.1:1".parse().unwrap());
    let client = kube::Client::try_from(cfg).expect("dummy client");
    let (tx, rx) = watch::channel(leader);
    let w = StatusWriter::new(
        client,
        crate::policy::store::PolicyStore::new(),
        crate::pool::PoolRegistry::new(),
        crate::policy::worker_sets::RejectionStore::new(),
        Arc::new(MetricsRegistry::new()),
        Arc::new(crate::policy::provider_health::ProviderHealthStore::new()),
        rx,
        Arc::new(crate::prom::PromMetrics::new()),
    );
    (w, tx)
}

/// Promotion must drop the last-written cache.
///
/// That cache is this replica's private record of what IT wrote. After a
/// failover it describes the world as of before the previous leader took
/// over, so trusting it means comparing desired status against a stale
/// baseline and skipping patches that are genuinely needed. The symptom is
/// the nastiest kind: status silently frozen, with nothing logged and no
/// error to point at.
// `kube::Client::try_from` spawns a tower buffer worker, so this needs a
// runtime even though no request is ever issued.
/// The bug this key fixes, at the level the writer sees it.
///
/// Delete a CR and re-apply it under the same name while the router keeps
/// running: the recreated object starts with EMPTY status, but the writer had
/// cached what it wrote for the PREDECESSOR. Keyed on name alone the computed
/// status matched the cache, the patch was skipped, and the object sat blank
/// until its content happened to change or the process restarted. The cache is
/// in-memory and never reconciled against the API server, so nothing else
/// noticed.
#[tokio::test]
async fn a_recreated_object_is_a_cache_miss_even_with_identical_status() {
    let (w, _tx) = writer_with_leader(true);
    let status = serde_json::json!({"destinationCount": 1});

    // What the writer wrote for the first incarnation.
    w.last.lock().unwrap().insert(
        (
            "InferencePool",
            "infra".into(),
            "chat".into(),
            Some("uid-1".into()),
        ),
        status.clone(),
    );
    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", Some("uid-1"))
            .is_some(),
        "the original is still cached"
    );

    // Same namespace, same name, same computed status — different object.
    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", Some("uid-2"))
            .is_none(),
        "a recreated object must MISS, or its patch is skipped and it keeps \
         the empty status it was created with"
    );

    // And an object whose UID we could not determine must not collide with
    // either incarnation.
    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", None)
            .is_none()
    );
}

#[tokio::test]
async fn promotion_drops_the_last_written_cache_so_status_is_re_asserted() {
    let (w, _tx) = writer_with_leader(false);

    // Stand in for "we wrote this while we were previously leader".
    w.last.lock().unwrap().insert(
        ("InferencePool", "infra".into(), "chat".into(), None),
        serde_json::json!({"destinationCount": 1}),
    );
    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", None)
            .is_some(),
        "cache should be warm before promotion"
    );

    w.on_promotion();

    assert!(
        w.previous::<InferencePoolStatus>("InferencePool", "infra", "chat", None)
            .is_none(),
        "after promotion the cache must be empty, so the next reconcile \
         re-asserts rather than trusting a baseline a different leader has \
         since overwritten"
    );
}

/// Demotion must withdraw `kinference_policy_warnings`. Only the leader's
/// reconcile sets it, so otherwise a demoted replica keeps exporting the
/// counts it had when it lost the lease, beside the new leader's.
///
/// Driven through `run`, so it also fails if the loop stops calling
/// `on_demotion`. The store is never listed, so no reconcile clears the
/// gauge on the demotion's behalf.
#[tokio::test]
async fn demotion_withdraws_the_policy_warning_counts() {
    let (w, tx) = writer_with_leader(true);
    let prom = w.prom.clone();
    // Stand in for "we published these while we were leader".
    prom.set_policy_warnings(&[("infra/routing".into(), "UnknownBackend", 2)]);
    assert!(prom.render().contains("kinference_policy_warnings{"));

    let cancel = CancellationToken::new();
    let task = tokio::spawn(w.run(cancel.clone()));
    tx.send(false).expect("the writer holds the receiver");

    let cleared = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while prom.render().contains("kinference_policy_warnings") {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    cancel.cancel();
    task.await.expect("the writer exits on cancel");
    assert!(
        cleared.is_ok(),
        "a demoted replica must stop exporting policy warning counts"
    );
}

/// The gauge must be seeded at construction, not only on the first
/// transition — otherwise a follower is absent from /metrics entirely, and
/// "no pod reports kinference_leader" is exactly the alert condition.
#[test]
fn leader_gauge_distinguishes_follower_from_absent() {
    let prom = crate::prom::PromMetrics::new();
    prom.set_leader(false);
    assert!(
        prom.render().contains("kinference_leader "),
        "gauge must be present at 0"
    );
    prom.set_leader(true);
    assert!(prom.render().contains("kinference_leader 1"));
}

/// Emptying a status list must be expressible on the wire.
///
/// We patch with `Patch::Merge`, where an omitted key means "leave it
/// alone". So `skip_serializing_if = "Vec::is_empty"` on a status list makes
/// clearing it impossible: the field simply vanishes from the patch and the
/// stale value survives. Found on a live cluster — after a dangling
/// destination was fixed, `unresolvedDestinations` kept naming it even
/// though `ResolvedRefs` had already flipped back to True, so the object
/// contradicted itself.
#[test]
fn emptied_status_lists_serialize_so_a_merge_patch_can_clear_them() {
    // A pool that was unresolved and is now fully resolved.
    let r = resolved(vec![dest("workers-h100", &[])], vec![]);
    let s = desired_pool_status(&r, &facts(1, &[]), None);
    assert!(
        s.unresolved_destinations.is_empty(),
        "fixture should have nothing unresolved"
    );
    let json = serde_json::to_value(&s).unwrap();
    for key in ["unresolvedDestinations", "models"] {
        assert_eq!(
            json.get(key),
            Some(&serde_json::json!([])),
            "`{key}` must appear as [] so the merge patch clears it; \
             omitting it leaves the previous value in place"
        );
    }

    // Same contract on the other two status types.
    let ws = desired_worker_set_status(&dest("workers-h100", &[]), &facts(0, &[]), &[], None, None);
    let wj = serde_json::to_value(&ws).unwrap();
    assert_eq!(wj.get("models"), Some(&serde_json::json!([])));
    // Same contract: a set that was a pool destination and has just been
    // removed from every pool must be able to say so. Omitting the key would
    // leave it listed under a pool that no longer routes to it.
    assert_eq!(
        wj.get("referencedBy"),
        Some(&serde_json::json!([])),
        "`referencedBy` must appear as [] so dropping the last pool clears it"
    );

    let pol = crate::policy::compile::CompiledPolicy::serve_as_asked("p");
    let pj = serde_json::to_value(desired_policy_status(&rp(pol), None)).unwrap();
    assert_eq!(
        pj.get("warnings"),
        Some(&serde_json::json!([])),
        "`warnings` must be clearable — one outliving the mistake it describes \
         misleads whoever just fixed it"
    );
}

#[test]
fn identical_inputs_produce_identical_status_so_no_patch_is_issued() {
    // This equality is what stops the write→watch→rebuild→write loop.
    let r = resolved(vec![dest("good", &[])], vec![]);
    let a = desired_pool_status(&r, &facts(2, &[("m", 2, Some(8), Some(8))]), None);
    let b = desired_pool_status(&r, &facts(2, &[("m", 2, Some(8), Some(8))]), Some(&a));
    assert_eq!(
        serde_json::to_value(&a).unwrap(),
        serde_json::to_value(&b).unwrap()
    );
}

/// The reason the entry is keyed on the upstream rather than a name: two
/// aliases for one upstream model are BOTH reported. The old singular `name`
/// held a lossy inverse lookup and silently dropped every alias but the
/// first, so `qwen-simple` existed on the wire and appeared nowhere.
#[test]
fn every_alias_for_a_model_is_reported() {
    let d = dest(
        "workers-h100",
        &[
            ("qwen-2.5", "Qwen/Qwen2.5-32B"),
            ("qwen-simple", "Qwen/Qwen2.5-32B"),
        ],
    );
    let s = desired_worker_set_status(
        &d,
        &facts(2, &[("Qwen/Qwen2.5-32B", 2, None, None)]),
        &[],
        None,
        None,
    );
    assert_eq!(s.models.len(), 1, "one entry per OBSERVED model");
    assert_eq!(s.models[0].upstream_name, "Qwen/Qwen2.5-32B");
    assert_eq!(
        s.models[0].aliases,
        ["qwen-2.5", "qwen-simple"],
        "sorted, both present"
    );
}

/// A model nobody declared an alias for reports just its upstream name —
/// `aliases` is skipped when empty, so the common case stays terse instead
/// of echoing the same string into two fields.
#[test]
fn an_undeclared_model_reports_no_aliases() {
    let d = dest("workers-h100", &[]);
    let s = desired_worker_set_status(
        &d,
        &facts(1, &[("google/gemma-4-12B-it", 1, None, None)]),
        &[],
        None,
        None,
    );
    assert_eq!(s.models[0].upstream_name, "google/gemma-4-12B-it");
    assert!(s.models[0].aliases.is_empty());
    let json = serde_json::to_value(&s.models[0]).unwrap();
    assert!(
        json.get("aliases").is_none(),
        "empty aliases must not serialize"
    );
}

/// Several models on one fleet — LoRA adapters, or several
/// `--served-model-name` values — get an entry each, and `readyEndpoints`
/// makes a partial rollout visible. This is the cardinality the old shape
/// degraded worst at: three entries, three independent inverse lookups.
#[test]
fn each_observed_model_gets_its_own_entry_with_its_own_endpoint_count() {
    let d = dest("workers-h100", &[("base", "Qwen/Qwen2.5-32B")]);
    let s = desired_worker_set_status(
        &d,
        &facts(
            2,
            &[
                ("Qwen/Qwen2.5-32B", 2, Some(32768), Some(32768)),
                ("my-adapter", 1, Some(32768), Some(32768)),
            ],
        ),
        &[],
        None,
        None,
    );
    assert_eq!(s.models.len(), 2);
    let adapter = s
        .models
        .iter()
        .find(|m| m.upstream_name == "my-adapter")
        .unwrap();
    assert_eq!(adapter.ready_endpoints, Some(1), "loaded on one pod of two");
    assert!(adapter.aliases.is_empty());
    let base = s
        .models
        .iter()
        .find(|m| m.upstream_name == "Qwen/Qwen2.5-32B")
        .unwrap();
    assert_eq!(base.ready_endpoints, Some(2));
    assert_eq!(base.aliases, ["base"]);
}

#[test]
fn worker_set_status_counts_ready_endpoints_and_models() {
    let d = dest("workers-h100", &[("bar", "upstream/bar-fp8")]);
    let s = desired_worker_set_status(
        &d,
        &facts(3, &[("upstream/bar-fp8", 3, Some(32768), Some(32768))]),
        &[],
        None,
        None,
    );
    assert_eq!(s.endpoints.unwrap().ready, Some(3));
    // Keyed on the OBSERVED model; the declared name is a secondary list.
    assert_eq!(s.models[0].upstream_name, "upstream/bar-fp8");
    assert_eq!(s.models[0].aliases, ["bar"]);
    assert_eq!(s.models[0].ready_endpoints, Some(3));
    assert_eq!(
        s.conditions
            .iter()
            .find(|c| c.r#type == "Ready")
            .unwrap()
            .status,
        "True"
    );
}

/// The condition `status.kvEvents` exists to expose: a worker set that is
/// fully ready and serving traffic while its prefix model is empty. Nothing
/// else surfaces it — readiness keys on the SUB socket reaching steady
/// state, and ZMQ reports that whether or not a publisher exists.
#[test]
fn kv_events_status_separates_declared_from_actually_flowing() {
    // Declared, and events arriving from all three: the healthy case.
    let mut d = dest("workers-h100", &[]);
    d.kv_events_declared = true;
    let mut f = facts(3, &[("m", 3, None, None)]);
    let s = desired_worker_set_status(&d, &f, &[], None, None);
    let kv = s.kv_events.unwrap();
    assert_eq!(kv.configured, Some(true));
    assert_eq!(kv.subscribed_endpoints, Some(3));

    // Declared, ready, serving — but no endpoint has ever delivered an
    // event. `Ready=True` and `subscribedEndpoints: 0` together are the
    // signature of a misconfigured publisher port.
    f.get_mut(&PoolId::from_raw(0)).unwrap().event_endpoints = 0;
    let s = desired_worker_set_status(&d, &f, &[], None, None);
    assert_eq!(s.kv_events.unwrap().subscribed_endpoints, Some(0));
    assert_eq!(
        s.conditions
            .iter()
            .find(|c| c.r#type == "Ready")
            .unwrap()
            .status,
        "True",
        "no prefix model degrades ranking; it does not take the set out of service"
    );

    // Not declared: still reported, because `configured: false` is the
    // whole message. Omitting the block for this case would defeat it.
    let plain = dest("workers-a100", &[]);
    let s = desired_worker_set_status(&plain, &facts(1, &[]), &[], None, None);
    assert_eq!(s.kv_events.unwrap().configured, Some(false));
}

#[test]
fn worker_set_with_no_ready_endpoints_is_not_ready_but_still_accepted() {
    let d = dest("workers-h100", &[]);
    let s = desired_worker_set_status(&d, &facts(0, &[]), &[], None, None);
    assert_eq!(
        s.conditions
            .iter()
            .find(|c| c.r#type == "Accepted")
            .unwrap()
            .status,
        "True",
        "it bound to a pool; it just has no ready pods"
    );
    let ready = s.conditions.iter().find(|c| c.r#type == "Ready").unwrap();
    assert_eq!(ready.status, "False");
    assert_eq!(ready.reason, "NoReadyEndpoints");
}

#[test]
fn policy_status_composes_state_tokens_and_reports_an_inert_tag() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [
            {"name": "pii", "matches": [{"tag": "pii-detected"}],
             "route": {"select": [{"minimize": "PredictedTTFT"}]}},
            {"name": "plain", "route": {"select": [{"minimize": "PredictedTTFT"}]}}
        ]
    }))
    .unwrap();
    let p = CompiledPolicy::compile("chat-policy", &spec);
    let s = desired_policy_status(&rp(p), None);
    // Two ORTHOGONAL facts in one column, joined because a `[*]` column would
    // render only the first token. This fixture is referenced, so it is in
    // force AND carrying warnings — which is exactly why `HasWarnings` is not
    // in the `Accepted` condition's reason: acceptance is unaffected by them.
    assert_eq!(s.state.as_deref(), Some("InForce,HasWarnings"));
    // `tag` is untyped, so a typo matches nothing silently. With the
    // per-rule `emittableTags` gone, this WARNING is the only thing that
    // tells an author `pii-detected` is not a tag any classifier emits —
    // which makes it the load-bearing half of what status still reports.
    let inert = s
        .warnings
        .iter()
        .find(|w| w.contains("never matches"))
        .unwrap_or_else(|| panic!("inert-rule warning missing: {:?}", s.warnings));
    assert!(
        inert.contains("pii-detected"),
        "the warning must name the offending tag: {inert}"
    );
    assert!(
        inert.contains("pii-confirmed"),
        "and what CAN be emitted instead: {inert}"
    );
    // Warnings never make a policy unusable.
    assert_eq!(s.conditions[0].status, "True");
    assert_eq!(s.conditions[0].observed_generation, Some(7));
}

#[test]
fn clean_policy_has_no_warnings() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [{"name": "r", "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
    }))
    .unwrap();
    let s = desired_policy_status(&rp(CompiledPolicy::compile("p", &spec)), None);
    assert!(s.warnings.is_empty());
    assert!(s.conditions[0].message.contains("1 rule(s) compiled"));
}

#[test]
fn two_destinations_serving_one_model_both_appear_in_served_by() {
    let mut d2 = dest("workers-a100", &[]);
    d2.pool_id = PoolId::from_raw(1);
    let r = resolved(vec![dest("workers-h100", &[]), d2], vec![]);
    let mut f = facts(2, &[("gemma", 2, Some(8192), Some(8192))]);
    f.insert(
        PoolId::from_raw(1),
        PoolFacts {
            ready_endpoints: 1,
            event_endpoints: 1,
            models: vec![ModelFact {
                upstream: "gemma".into(),
                ready_endpoints: 1,
                ctx_min: Some(4096),
                ctx_max: Some(4096),
            }],
        },
    );
    let s = desired_pool_status(&r, &f, None);
    assert_eq!(s.models.len(), 1);
    assert_eq!(s.models[0].served_by, vec!["workers-h100", "workers-a100"]);
    // The range spans both destinations — the operator sees the spread.
    let cw = s.models[0].context_window.as_ref().unwrap();
    assert_eq!((cw.min, cw.max), (Some(4096), Some(8192)));
}

// --- referenced vs not --------------------------------------------------

fn one_rule() -> CompiledPolicy {
    let spec: InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [{
            "name": "r",
            "route": {"select": [{"type": "Local", "minimize": "PredictedTTFT"}]}
        }]
    }))
    .expect("valid fixture");
    CompiledPolicy::compile("p", &spec)
}

/// The bug this replaces: an unattached policy got no status at all, so its
/// RULES column stayed blank — which reads as "failed to compile" when it
/// means "nothing points at me".
#[test]
fn an_unreferenced_policy_reports_why_rather_than_nothing() {
    let s = desired_policy_status(&rp_unreferenced(one_rule()), None);

    // `state` is published for a draft too, so the column fills in and you can
    // see where the policy stands BEFORE attaching it. Warnings likewise —
    // those are most useful while it is still a draft. A clean draft carries
    // exactly one token, never an empty string, so a blank column keeps
    // meaning "no status was written at all".
    assert_eq!(s.state.as_deref(), Some("NotReferenced"));
    assert!(s.warnings.is_empty(), "so no HasWarnings token");

    let c = &s.conditions[0];
    assert_eq!(c.r#type, "Accepted");
    assert_eq!(c.status, "False");
    assert_eq!(c.reason, "NotReferenced");
    assert!(
        c.message.contains("no InferencePool names it"),
        "the message must say what is wrong: {}",
        c.message
    );
    // And how to fix it, since the fix is a patch command nobody remembers.
    assert!(c.message.contains("policyRef"), "{}", c.message);
}

#[test]
fn a_referenced_policy_names_the_pools_it_serves() {
    let s = desired_policy_status(&rp(one_rule()), None);
    let c = &s.conditions[0];
    assert_eq!(c.status, "True");
    assert_eq!(c.reason, "Accepted");
    assert!(
        c.message.contains("in force on pool(s) chat"),
        "{}",
        c.message
    );
}

/// Detaching must actively flip the condition, not leave the old one standing.
/// A frozen `Accepted=True` on three detached policies was the misleading part.
#[test]
fn detaching_flips_accepted_rather_than_freezing_it() {
    let mut attached = desired_policy_status(&rp(one_rule()), None);
    assert_eq!(attached.conditions[0].status, "True");

    // Backdate it, so "did the timestamp move" is answerable. Asserting
    // against a freshly-minted one cannot work: `lastTransitionTime` is
    // second-resolution, and both calls land in the same second.
    attached.conditions[0].last_transition_time = "2020-01-01T00:00:00Z".to_string();

    let detached = desired_policy_status(&rp_unreferenced(one_rule()), Some(&attached));
    assert_eq!(detached.conditions[0].status, "False");
    assert_eq!(detached.conditions[0].reason, "NotReferenced");
    // A real state change, so the stale timestamp is NOT carried forward —
    // that is what makes the transition visible in `kubectl describe`.
    assert_ne!(
        detached.conditions[0].last_transition_time,
        "2020-01-01T00:00:00Z"
    );

    // And the converse: an unchanged condition keeps its timestamp, which is
    // what stops every tick from looking like a fresh transition.
    let still_attached = desired_policy_status(&rp(one_rule()), Some(&attached));
    assert_eq!(
        still_attached.conditions[0].last_transition_time,
        "2020-01-01T00:00:00Z"
    );
}

/// observedGeneration was never published for policies, so there was no way to
/// tell whether the status you were reading described the edit you just made.
#[test]
fn the_policy_status_reports_the_generation_it_describes() {
    let s = desired_policy_status(&rp(one_rule()), None);
    assert_eq!(s.conditions[0].observed_generation, Some(7));
}

/// Unchanged state must produce a byte-identical status, or the writer's
/// last-written comparison never breaks the write loop.
#[test]
fn an_unchanged_policy_produces_an_identical_status() {
    let first = desired_policy_status(&rp_unreferenced(one_rule()), None);
    let again = desired_policy_status(&rp_unreferenced(one_rule()), Some(&first));
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&again).unwrap()
    );
}

/// A refused provider must say so on its OWN object.
///
/// It never becomes a destination, so nothing else writes to it — and a blank
/// status reads as "nothing happened" rather than "refused, and here is why".
/// Same blind spot the unreferenced-policy case had.
#[test]
fn a_refused_provider_reports_the_reason_and_the_fix() {
    let s = rejected_provider_status(
        crate::policy::store::UnresolvedReason::UnsupportedApiFormat,
        None,
    );
    let accepted = &s.conditions[0];
    assert_eq!(accepted.r#type, "Accepted");
    assert_eq!(accepted.status, "False");
    assert_eq!(accepted.reason, "UnsupportedApiFormat");
    // The message has to carry the way out, not just the verdict.
    assert!(
        accepted.message.contains("Only OpenAI"),
        "{}",
        accepted.message
    );
    assert!(
        accepted.message.contains("OpenAI-compatible endpoint"),
        "{}",
        accepted.message
    );

    assert_eq!(s.conditions[1].r#type, "Ready");
    assert_eq!(s.conditions[1].status, "False");

    // A refused provider's status carries NOTHING beyond its verdict. Pinned
    // by key set rather than field-by-field, so a field that starts leaking
    // stale data through the refusal path fails here — which is what the old
    // `observed.is_none()` assertion was really guarding.
    let keys: Vec<String> = serde_json::to_value(&s)
        .unwrap()
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(keys, ["conditions", "state"], "unexpected key on a refusal");
}

#[test]
fn every_refusal_reason_gets_its_own_message() {
    use crate::policy::store::UnresolvedReason as R;
    let msgs: Vec<String> = [
        R::UnsupportedApiFormat,
        R::Incomplete,
        R::CrossNamespaceNotGranted,
        R::AmbiguousName,
    ]
    .into_iter()
    .map(|r| {
        rejected_provider_status(r, None).conditions[0]
            .message
            .clone()
    })
    .collect();
    // Distinct guidance per reason — a shared "it failed" would send the
    // operator to the wrong file.
    let unique: std::collections::BTreeSet<&String> = msgs.iter().collect();
    assert_eq!(unique.len(), msgs.len(), "{msgs:?}");
}

// --- policy warnings in status ------------------------------------------------

use crate::policy::warning::{PolicyWarning, WarningCode};

fn warning(i: usize) -> PolicyWarning {
    PolicyWarning::new(
        WarningCode::EmptyDrop,
        format!("spec.rules[{i}].route.exclude[0].drop"),
        "removes nothing",
    )
}

/// The CRD caps `status.warnings` at 32 entries of 2048 characters, and going
/// over rejects the WHOLE patch — state and conditions included. A long list is
/// cut to 31 plus a pointer to the rest; an over-long entry is clipped.
#[test]
fn status_warnings_fit_the_crd_limits() {
    let mut compiled = CompiledPolicy::serve_as_asked("p");
    compiled.warnings = (0..40).map(warning).collect();
    compiled.warnings.push(PolicyWarning::new(
        WarningCode::EmptyDrop,
        "spec",
        "ü".repeat(5000),
    ));
    let s = desired_policy_status(&rp(compiled), None);
    assert_eq!(s.warnings.len(), 32);
    assert!(
        s.warnings[31].contains("and 10 more warning(s)"),
        "{}",
        s.warnings[31]
    );
    assert!(
        s.warnings[31].contains("/api/policy/context"),
        "{}",
        s.warnings[31]
    );
    assert!(s.warnings.iter().all(|w| w.chars().count() <= 2048));
    // The full count still reaches the condition, so nothing is understated.
    let accepted = s
        .conditions
        .iter()
        .find(|c| c.r#type == "Accepted")
        .unwrap();
    assert!(
        accepted.message.contains("41 warning(s)"),
        "{}",
        accepted.message
    );

    let mut long = CompiledPolicy::serve_as_asked("p");
    long.warnings = vec![PolicyWarning::new(
        WarningCode::EmptyDrop,
        "spec",
        "ü".repeat(5000),
    )];
    let s = desired_policy_status(&rp(long), None);
    assert_eq!(
        s.warnings[0].chars().count(),
        2048,
        "clipped on a char boundary"
    );
    assert!(s.warnings[0].ends_with('…'));
}

/// Status renders each warning as `Code: path: message`, so `kubectl get -o
/// yaml` shows the same code an agent matches on.
#[test]
fn status_warnings_carry_code_and_path() {
    let mut compiled = CompiledPolicy::serve_as_asked("p");
    compiled.warnings = vec![warning(0)];
    let s = desired_policy_status(&rp(compiled), None);
    assert_eq!(
        s.warnings,
        ["EmptyDrop: spec.rules[0].route.exclude[0].drop: removes nothing"]
    );
}

/// A name warning alone is enough for `HasWarnings`: a policy whose spec
/// compiles clean but names a backend its pool lacks is not healthy.
#[test]
fn reference_warnings_alone_raise_has_warnings() {
    let mut r = rp(CompiledPolicy::serve_as_asked("p"));
    r.reference_warnings = vec![PolicyWarning::new(
        WarningCode::UnknownBackend,
        "spec.rules[0].route.select[0].order[0]",
        "`databriks` is not a destination",
    )];
    let s = desired_policy_status(&r, None);
    assert_eq!(s.state.as_deref(), Some("InForce,HasWarnings"));
    assert_eq!(s.warnings.len(), 1);
    assert!(s.warnings[0].starts_with("UnknownBackend: "));
}
