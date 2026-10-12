use super::*;
use crate::policy::PoolId;
use crate::policy::compile::CompiledPolicy;
use crate::policy::crd::InferencePolicySpec;
use crate::policy::engine::{CandidateTarget, ObjectKey};
use crate::policy::predicate::CandidateAttrs;

/// Fixtures are JSON, not YAML, following the convention `crd/tests.rs`
/// established. These mirror the demo policies in `src/policy/check/tests.rs`.
fn policy(json: serde_json::Value) -> CompiledPolicy {
    let spec: InferencePolicySpec =
        serde_json::from_value(json).expect("fixture is a valid InferencePolicySpec");
    CompiledPolicy::compile("test", &spec)
}

/// One local candidate, with the two duplicated latency fields written from a
/// SINGLE input.
///
/// `target.predicted_ttft_ms` (f32) drives ranking via `objective_ms`, while
/// `attrs.predicted_ttft_ms` (f64) drives `drop:` predicates. They are
/// independent fields, so a helper that took them separately would let a test —
/// and the dashboard handler — rank on one number while displaying another.
fn local(backend: &str, model: &str, ttft: f32, queued: i64) -> Candidate {
    Candidate {
        canonical_model: model.to_string(),
        upstream_model: model.to_string(),
        backend: backend.to_string(),
        labels: Default::default(),
        target: CandidateTarget::Local {
            pool_id: PoolId::from_raw(0),
            endpoint: format!("{backend}-ep:8000"),
            score_idx: 0,
            predicted_ttft_ms: ttft,
        },
        attrs: CandidateAttrs {
            model_name: Some(model.to_string()),
            backend_name: Some(backend.to_string()),
            backend_type: Some("Local".to_string()),
            predicted_ttft_ms: Some(ttft as f64),
            queued_prompts: Some(queued as f64),
            ..Default::default()
        },
    }
}

fn provider(name: &str, model: &str, observed: Option<f32>) -> Candidate {
    Candidate {
        canonical_model: model.to_string(),
        upstream_model: model.to_string(),
        backend: name.to_string(),
        labels: Default::default(),
        target: CandidateTarget::Provider {
            provider: ObjectKey::new("default", name),
            backend_ref: ObjectKey::new("default", name),
            path: None,
            observed_ttft_ms: observed,
        },
        attrs: CandidateAttrs {
            model_name: Some(model.to_string()),
            backend_name: Some(name.to_string()),
            backend_type: Some("Provider".to_string()),
            backend_observed_ttft_ms: observed.map(|v| v as f64),
            ..Default::default()
        },
    }
}

fn attrs(model: &str, qos: &str, tags: &[&str]) -> RequestAttrs {
    RequestAttrs {
        model: vec![model.to_string()],
        qos: Some(qos.to_string()),
        tag: tags.iter().map(|t| t.to_string()).collect(),
        input_tokens: Some(1024),
        max_tokens: Some(256),
        tenant: String::new(),
    }
}

/// The duplicated latency fields must agree, or ranking and `drop:` predicates
/// describe different worlds.
///
/// Cheap to assert and expensive to get wrong: an explorer that ranks on a
/// stale f32 while showing an edited f64 would be confidently wrong.
#[test]
fn the_two_predicted_ttft_copies_stay_in_sync() {
    let c = local("ws-a", "m", 412.0, 3);
    let from_target = match &c.target {
        CandidateTarget::Local {
            predicted_ttft_ms, ..
        } => *predicted_ttft_ms as f64,
        _ => unreachable!(),
    };
    assert_eq!(from_target, c.attrs.predicted_ttft_ms.unwrap());
    assert_eq!(
        c.attrs.backend_name.as_deref(),
        Some(c.backend.as_str()),
        "`Rank::Order` matches candidate.backend, `drop: {{backend:{{name}}}}` matches attrs"
    );
    assert_eq!(
        c.attrs.backend_type.as_deref(),
        Some(c.backend_type().as_str()),
        "tier filtering uses backend_type(), predicates use attrs.backend_type"
    );
}

/// PARITY: the explainer's winner is whatever a direct `evaluate()` says.
///
/// The point is to pin the SEQUENCING, which is the only part the explainer
/// re-implements. The candidate filtering is done by hand here rather than by
/// calling `apply_consider`, so this is an independent check and not a
/// tautology.
#[test]
fn parity_with_a_direct_evaluate_call() {
    // policy-2: matches on model+tag, considers two models, minimizes TTFT.
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "simple-can-go-to-gemma-if-faster",
            "matches": [{ "model": "qwen-2.5", "tag": "complexity-low" }],
            "route": {
                "consider": [{ "models": { "names": ["gemma-4", "qwen-2.5"] } }],
                "select": [{ "type": "Local", "minimize": "PredictedTTFT" }]
            }
        }]
    }));
    let a = attrs("qwen-2.5", "interactive", &["complexity-low"]);
    let cands = vec![
        local("ws-gemma", "gemma-4", 900.0, 0),
        local("ws-qwen", "qwen-2.5", 300.0, 0),
    ];

    let got = explain(&p, &a, &cands);
    assert_eq!(got.verdict, Verdict::Chosen, "steps: {:#?}", got.steps);

    // Independent: take the one matching rule and evaluate it directly, doing
    // the `consider` model filter by hand.
    let rules = engine::matching_rules(&p, &a);
    assert_eq!(rules.len(), 1);
    let want_models = ["gemma-4", "qwen-2.5"];
    let filtered: Vec<Candidate> = cands
        .iter()
        .filter(|c| want_models.contains(&c.canonical_model.as_str()))
        .cloned()
        .collect();
    let direct = engine::evaluate(&rules[0].route, Some(&rules[0].name), &a, filtered);
    let want = match direct {
        Outcome::Chosen { candidate, .. } => candidate.label(),
        Outcome::Empty { .. } => panic!("direct evaluate found nothing"),
    };
    assert_eq!(got.winner, Some(want));

    // Second case: TWO rules match and both could decide. First-match-wins, so
    // parity here also pins the ORDER — with one matching rule it could not.
    let p2 = policy(serde_json::json!({
        "rules": [
            { "name": "first-wins", "route": {
                "consider": [{ "models": { "names": ["gemma-4"] } }],
                "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] } },
            { "name": "would-also-match", "route": {
                "consider": [{ "models": { "names": ["qwen-2.5"] } }],
                "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] } }
        ]
    }));
    let got2 = explain(&p2, &a, &cands);
    let rules2 = engine::matching_rules(&p2, &a);
    assert_eq!(rules2.len(), 2, "both are catch-alls");
    // By hand: the FIRST matching rule, with its own `consider` filter.
    let filtered2: Vec<Candidate> = cands
        .iter()
        .filter(|c| c.canonical_model == "gemma-4")
        .cloned()
        .collect();
    let direct2 = engine::evaluate(&rules2[0].route, Some(&rules2[0].name), &a, filtered2);
    let want2 = match direct2 {
        Outcome::Chosen { candidate, .. } => candidate.label(),
        Outcome::Empty { .. } => panic!("direct evaluate found nothing"),
    };
    assert_eq!(
        got2.winner,
        Some(want2),
        "rule 0 decides even though rule 1 would have picked the faster worker"
    );
}

/// A non-matching rule must say WHICH term failed — the whole reason this
/// module exists rather than reusing `CompiledRequestPredicate::matches`.
#[test]
fn a_rule_that_does_not_match_names_the_failing_term() {
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "simple-goes-to-gemma",
            "matches": [{ "model": "qwen-2.5", "tag": "complexity-low" }],
            "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] }
        }]
    }));
    // Right model, wrong tag.
    let a = attrs("qwen-2.5", "interactive", &["complexity-high"]);
    let got = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 0)]);

    let step = &got.steps[0];
    assert!(!step.matched);
    let pred = &step.predicates[0];
    assert!(!pred.matched);
    let model_term = pred.terms.iter().find(|t| t.attr == "model").unwrap();
    let tag_term = pred.terms.iter().find(|t| t.attr == "tag").unwrap();
    assert!(model_term.matched, "model qwen-2.5 does match");
    assert!(
        !tag_term.matched,
        "complexity-low is absent, so the tag fails"
    );
    assert!(
        step.trace.is_none() && step.consider.is_none(),
        "a rule that did not match was never evaluated"
    );
}

/// `consider` filters during candidate GENERATION in the driver, so a `Trace`
/// never shows it. The explainer has to apply it or `consider` looks inert.
#[test]
fn consider_filters_by_model_and_reports_why() {
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "gemma-only",
            "route": {
                "consider": [{ "models": { "names": ["gemma-4"] } }],
                "select": [{ "type": "Local", "minimize": "PredictedTTFT" }]
            }
        }]
    }));
    let a = attrs("qwen-2.5", "interactive", &[]);
    let got = explain(
        &p,
        &a,
        &[
            local("ws-gemma", "gemma-4", 900.0, 0),
            local("ws-qwen", "qwen-2.5", 300.0, 0),
        ],
    );

    let c = got.steps[0].consider.as_ref().expect("rule matched");
    assert_eq!(c.admitted.len(), 1, "only gemma-4 is in `consider`");
    assert!(c.admitted[0].contains("gemma-4"));
    assert_eq!(c.rejected.len(), 1);
    assert!(
        c.rejected[0].1.contains("qwen-2.5"),
        "the reason names the model that was not considered: {:?}",
        c.rejected[0]
    );
    // The faster qwen worker lost despite being faster, because `consider`
    // never offered it.
    assert!(got.winner.unwrap().contains("gemma-4"));
}

/// policy-5: batch-only-where-nothing-is-queued, with `onEmpty: Reject`.
///
/// The pair below is the whole feature in miniature — matching, an exclusion
/// reading a candidate attribute, and the verdict flipping on an edited
/// override.
#[test]
fn on_empty_reject_flips_when_a_candidate_is_edited() {
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "batch-only-where-nothing-is-queued",
            "matches": [{ "qos": ["batch"] }],
            "route": {
                "consider": [{ "type": "Local" }],
                "exclude": [{
                    "name": "require-an-empty-queue",
                    "drop": { "backend": { "type": "Local" }, "queuedPrompts": { "greaterThan": 0 } }
                }],
                "select": [{ "minimize": "PredictedTTFT" }],
                "onEmpty": "Reject"
            }
        }]
    }));
    let a = attrs("qwen-2.5", "batch", &[]);

    let idle = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 0)]);
    assert_eq!(idle.verdict, Verdict::Chosen, "an idle worker is allowed");

    let busy = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 1)]);
    assert_eq!(
        busy.verdict,
        Verdict::RejectedOnEmpty,
        "one queued prompt is enough: {:#?}",
        busy.steps
    );
    let trace = busy.steps[0].trace.as_ref().unwrap();
    assert!(
        trace.eliminated[0].1.contains("require-an-empty-queue"),
        "the exclusion is named: {:?}",
        trace.eliminated
    );
}

/// `onEmpty: FallThrough` must advance to the NEXT matching rule. This is the
/// sequencing the parity test cannot reach with a single rule.
#[test]
fn fall_through_advances_to_the_next_rule() {
    let p = policy(serde_json::json!({
        "rules": [
            {
                "name": "prefer-gemma-if-idle",
                "route": {
                    "exclude": [{
                        "name": "only-idle",
                        "drop": { "backend": { "type": "Local" }, "queuedPrompts": { "greaterThan": 0 } }
                    }],
                    "select": [{ "type": "Local", "minimize": "PredictedTTFT" }],
                    "onEmpty": "FallThrough"
                }
            },
            {
                "name": "anywhere-local",
                "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] }
            }
        ]
    }));
    let a = attrs("qwen-2.5", "interactive", &[]);
    // The only worker is busy, so rule 0 empties and hands on.
    let got = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 5)]);

    assert_eq!(got.verdict, Verdict::Chosen);
    assert!(
        got.steps[0].fell_through,
        "rule 0 should have fallen through"
    );
    assert_eq!(got.steps[0].winner, None);
    assert!(got.steps[1].winner.is_some(), "rule 1 decided");
}

/// A `select` tier that subsets by class silently skips the other class, with
/// no trace entry. The explorer reconstructs that.
#[test]
fn tier_subset_misses_are_reported_though_the_trace_omits_them() {
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "local-first-then-provider",
            "route": {
                "select": [
                    { "type": "Local", "minimize": "PredictedTTFT" },
                    { "type": "Provider", "order": ["databricks"] }
                ]
            }
        }]
    }));
    let a = attrs("qwen-2.5", "interactive", &[]);
    let got = explain(
        &p,
        &a,
        &[
            local("ws-qwen", "qwen-2.5", 300.0, 0),
            provider("databricks", "qwen-2.5", Some(800.0)),
        ],
    );

    assert_eq!(got.verdict, Verdict::Chosen);
    let step = &got.steps[0];
    assert!(step.winner.as_ref().unwrap().contains("ws-qwen"));
    assert_eq!(step.tier_rank.as_deref(), Some("minimize PredictedTTFT"));
    assert!(
        step.tier_subset_drops
            .iter()
            .any(|(c, why)| c.contains("databricks") && why.contains("tier 0")),
        "the provider was outside tier 0's subset: {:?}",
        step.tier_subset_drops
    );
    assert!(
        step.trace.as_ref().unwrap().eliminated.is_empty(),
        "and the trace itself records nothing about it — which is why we recompute"
    );
}

/// `unmatched: Reject` with no matching rule.
#[test]
fn unmatched_reject_is_distinct_from_on_empty() {
    let p = policy(serde_json::json!({
        "unmatched": "Reject",
        "rules": [{
            "name": "batch-only",
            "matches": [{ "qos": ["batch"] }],
            "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] }
        }]
    }));
    let a = attrs("qwen-2.5", "interactive", &[]);
    let got = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 0)]);
    assert_eq!(got.verdict, Verdict::RejectedUnmatched);
    assert!(!got.steps[0].matched);
    assert_eq!(got.winner, None);
}

/// The default `unmatched: ServeLocal` serves, and reports as its own verdict
/// rather than as `chosen` — the same split `kinference_policy_decisions_total` makes.
#[test]
fn unmatched_serve_local_reports_unmatched_served() {
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "batch-only",
            "matches": [{ "qos": ["batch"] }],
            "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] }
        }]
    }));
    let a = attrs("qwen-2.5", "interactive", &[]);
    let got = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 0)]);
    assert_eq!(got.verdict, Verdict::UnmatchedServed);
    assert!(got.winner.is_some());
    assert_eq!(
        got.steps.last().unwrap().name,
        "<unmatched>",
        "the fallback route gets its own step"
    );
}

/// Nondeterminism has to be declared before the user reads one roll as the
/// answer.
#[test]
fn nondeterminism_names_both_sources() {
    let random = policy(serde_json::json!({
        "unmatched": "Reject",
        "rules": [{
            "name": "coin-flip",
            "route": {
                "select": [{ "type": "Local", "minimize": "PredictedTTFT" }],
                "tieBreak": ["Random"]
            }
        }]
    }));
    let r = nondeterminism(&random);
    assert_eq!(r.len(), 1, "{r:?}");
    assert!(r[0].contains("tieBreak: Random"));
    assert_eq!(r[0].code, "RandomTieBreak");
    assert_eq!(r[0].rule.as_deref(), Some("coin-flip"));

    let weighted = policy(serde_json::json!({
        "rules": [{
            "name": "split",
            "route": {
                "consider": [
                    { "weight": 70, "models": { "names": ["gemma-4"] } },
                    { "weight": 30, "models": { "names": ["qwen-2.5"] } }
                ],
                "select": [{ "type": "Local", "minimize": "PredictedTTFT" }]
            }
        }]
    }));
    let w = nondeterminism(&weighted);
    assert!(w.iter().any(|s| s.contains("weighted `consider`")), "{w:?}");

    let plain = policy(serde_json::json!({
        "unmatched": "Reject",
        "rules": [{
            "name": "plain",
            "route": { "select": [{ "type": "Local", "order": ["a"] }], "tieBreak": ["PreferLocal"] }
        }]
    }));
    assert!(
        nondeterminism(&plain).is_empty(),
        "PreferLocal alone is deterministic"
    );
}

/// The route nobody wrote is a source too. `unmatched: ServeLocal` — the
/// default — serves with the built-in route, which breaks ties at random; a
/// policy that left it out claimed to be reproducible while idle, equally fast
/// endpoints traded wins between runs.
#[test]
fn the_unmatched_route_is_a_source_of_nondeterminism_unless_it_rejects() {
    let serve = policy(serde_json::json!({
        "rules": [{"name": "r", "route": {"select": [{"type": "Local", "order": ["a"]}]}}]
    }));
    let n = nondeterminism(&serve);
    assert_eq!(n.len(), 1, "{n:?}");
    assert_eq!(n[0].code, "UnmatchedRandomTieBreak");
    assert_eq!(n[0].rule, None);

    let reject = policy(serde_json::json!({
        "unmatched": "Reject",
        "rules": [{"name": "r", "route": {"select": [{"type": "Local", "order": ["a"]}]}}]
    }));
    assert!(nondeterminism(&reject).is_empty());
}

/// Every rule appears, matched or not. A rule that never fires is exactly what
/// an operator needs to see, and `matching_rules` alone would hide it.
#[test]
fn every_rule_is_reported_in_list_order() {
    let p = policy(serde_json::json!({
        "rules": [
            { "name": "first",  "matches": [{ "qos": ["batch"] }],
              "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] } },
            { "name": "second", "matches": [{ "qos": ["deferrable"] }],
              "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] } },
            { "name": "third",
              "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] } }
        ]
    }));
    let a = attrs("qwen-2.5", "interactive", &[]);
    let got = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 0)]);

    let names: Vec<&str> = got.steps.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, vec!["first", "second", "third"]);
    assert_eq!(
        got.steps.iter().filter(|s| s.matched).count(),
        1,
        "only the catch-all matched"
    );
    assert_eq!(got.verdict, Verdict::Chosen);
    assert_eq!(got.steps[2].index, 2, "index is the position in spec.rules");
}
