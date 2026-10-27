use super::*;
use crate::PoolId;
use crate::compile::CompiledPolicy;
use crate::crd::InferencePolicySpec;
use crate::engine::{CandidateTarget, ObjectKey};
use crate::predicate::CandidateAttrs;

/// Fixtures are JSON, not YAML, following the convention `crd/tests.rs`
/// established. These mirror the demo policies in `check/tests.rs`.
fn policy(json: serde_json::Value) -> CompiledPolicy {
    let spec: InferencePolicySpec =
        serde_json::from_value(json).expect("fixture is a valid InferencePolicySpec");
    CompiledPolicy::compile("test", &spec)
}

/// One local candidate, with the two duplicated latency fields written from a
/// SINGLE input.
///
/// `target.predicted_ttft_ms` (f32) drives ranking, while
/// `attrs.predicted_ttft_ms` (f64) drives `drop:` predicates; they must agree.
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
            model_names: vec![model.to_string()],
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
            model_names: vec![model.to_string()],
            backend_name: Some(name.to_string()),
            backend_type: Some("Provider".to_string()),
            backend_observed_ttft_ms: observed.map(|v| v as f64),
            ..Default::default()
        },
    }
}

fn attrs(model: &str, qos: &str) -> RequestAttrs {
    RequestAttrs {
        model: vec![model.to_string()],
        requested_model: model.to_string(),
        qos: Some(qos.to_string()),
        input_tokens: Some(1024),
        max_tokens: Some(256),
        tenant: String::new(),
        tag: Vec::new(),
    }
}

/// The duplicated latency fields agree.
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

/// Parity: the explainer's winner is whatever a direct `evaluate()` says. The
/// `consider` filtering is done by hand rather than via `apply_consider`, so
/// this independently checks the sequencing.
#[test]
fn parity_with_a_direct_evaluate_call() {
    // policy-2: matches on model+qos, considers two models, minimizes TTFT.
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "interactive-can-go-to-gemma-if-faster",
            "matches": [{ "model": "qwen-2.5", "qos": ["interactive"] }],
            "route": {
                "consider": [{ "models": { "names": ["gemma-4", "qwen-2.5"] } }],
                "select": [{ "type": "Local", "minimize": "PredictedTTFT" }]
            }
        }]
    }));
    let a = attrs("qwen-2.5", "interactive");
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

    // Two rules match and both could decide, so this also checks rule order.
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

/// A non-matching rule says which term failed.
#[test]
fn a_rule_that_does_not_match_names_the_failing_term() {
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "batch-goes-to-gemma",
            "matches": [{ "model": "qwen-2.5", "qos": ["batch"] }],
            "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] }
        }]
    }));
    // Right model, wrong QoS.
    let a = attrs("qwen-2.5", "interactive");
    let got = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 0)]);

    let step = &got.steps[0];
    assert!(!step.matched);
    let pred = &step.predicates[0];
    assert!(!pred.matched);
    let model_term = pred.terms.iter().find(|t| t.attr == "model").unwrap();
    let qos_term = pred.terms.iter().find(|t| t.attr == "qos").unwrap();
    assert!(model_term.matched, "model qwen-2.5 does match");
    assert!(!qos_term.matched, "the request is interactive, not batch");
    assert!(
        step.trace.is_none() && step.consider.is_none(),
        "a rule that did not match was never evaluated"
    );
}

/// The explainer applies `consider`, which the driver applies during
/// candidate generation.
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
    let a = attrs("qwen-2.5", "interactive");
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
/// Matching, an exclusion reading a candidate attribute, and the verdict
/// flipping on an edited override.
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
    let a = attrs("qwen-2.5", "batch");

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

/// `onEmpty: FallThrough` advances to the next matching rule.
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
    let a = attrs("qwen-2.5", "interactive");
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

/// A `select` tier that subsets by class skips the other class without a
/// trace entry; the explainer reports it.
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
    let a = attrs("qwen-2.5", "interactive");
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
    let a = attrs("qwen-2.5", "interactive");
    let got = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 0)]);
    assert_eq!(got.verdict, Verdict::RejectedUnmatched);
    assert!(!got.steps[0].matched);
    assert_eq!(got.winner, None);
}

/// The default `unmatched: ServeLocal` serves, reported as its own verdict
/// rather than `chosen`, as in `kinference_policy_decisions_total`.
#[test]
fn unmatched_serve_local_reports_unmatched_served() {
    let p = policy(serde_json::json!({
        "rules": [{
            "name": "batch-only",
            "matches": [{ "qos": ["batch"] }],
            "route": { "select": [{ "type": "Local", "minimize": "PredictedTTFT" }] }
        }]
    }));
    let a = attrs("qwen-2.5", "interactive");
    let got = explain(&p, &a, &[local("ws-qwen", "qwen-2.5", 300.0, 0)]);
    assert_eq!(got.verdict, Verdict::UnmatchedServed);
    assert!(got.winner.is_some());
    assert_eq!(
        got.steps.last().unwrap().name,
        "<unmatched>",
        "the fallback route gets its own step"
    );
}

/// Sources of nondeterminism are declared.
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

/// The built-in route for `unmatched: ServeLocal` (the default) breaks ties at
/// random, so it is a source too.
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

/// Every rule appears, matched or not.
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
    let a = attrs("qwen-2.5", "interactive");
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
