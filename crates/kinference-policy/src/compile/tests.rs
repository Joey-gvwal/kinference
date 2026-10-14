use super::*;

/// The consider `type:` shorthand filters by class.
#[test]
fn a_consider_group_type_shorthand_reaches_the_compiled_filter() {
    let spec: crate::crd::InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [{
            "name": "r",
            "route": {
                "consider": [{"type": "Provider"}],
                "select": [{"type": "Provider", "order": ["vendor"]}]
            }
        }]
    }))
    .unwrap();
    let c = CompiledPolicy::compile("p", &spec);
    let g = &c.rules[0].route.consider[0];
    assert_eq!(
        g.backends.backend_type,
        Some(BackendType::Provider),
        "the shorthand must survive compilation, not be parsed and dropped"
    );
}

/// The longhand compiles identically.
#[test]
fn the_two_consider_spellings_compile_the_same() {
    let mk = |route: serde_json::Value| {
        let spec: crate::crd::InferencePolicySpec = serde_json::from_value(serde_json::json!({
            "rules": [{"name": "r", "route": route}]
        }))
        .unwrap();
        CompiledPolicy::compile("p", &spec).rules[0].route.consider[0]
            .backends
            .backend_type
    };
    let short = mk(serde_json::json!({
        "consider": [{"type": "Local"}],
        "select": [{"type": "Local", "minimize": "PredictedTTFT"}]
    }));
    let long = mk(serde_json::json!({
        "consider": [{"backends": {"type": "Local"}}],
        "select": [{"type": "Local", "minimize": "PredictedTTFT"}]
    }));
    assert_eq!(short, Some(BackendType::Local));
    assert_eq!(short, long);
}
use crate::predicate::RequestAttrs;
use serde_json::json;

fn policy(v: serde_json::Value) -> CompiledPolicy {
    let spec: InferencePolicySpec = serde_json::from_value(v).expect("valid fixture");
    CompiledPolicy::compile("test-policy", &spec)
}

/// Every warning rendered and joined, for substring assertions over the lot.
fn joined(p: &CompiledPolicy) -> String {
    p.warnings
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" | ")
}

#[test]
fn guide_example_compiles_clean() {
    let p = policy(json!({
        "rules": [{
            "name": "cheap-when-simple",
            "matches": [{"model": "bar"}],
            "route": {
                "consider": [{"models": {"names": ["foo", "bar"]}}],
                "exclude": [{
                    "name": "keep-long-prompts-off-a100",
                    "if": {"inputTokens": {"greaterThan": 32000}},
                    "drop": {"backend": {"name": "workers-a100"}}
                }],
                "select": [{"minimize": "PredictedTTFT"}],
                "tieBreak": ["Random"],
                "onEmpty": "Reject"
            }
        }]
    }));
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);
    let r = &p.rules[0];
    assert_eq!(
        r.route.consider[0].models,
        ModelChoice::Names(vec!["foo".into(), "bar".into()])
    );
    assert_eq!(r.route.exclude.len(), 1);
    assert!(r.route.exclude[0].if_.is_some());
    assert!(matches!(
        r.route.select[0].rank,
        Rank::MinimizePredictedTtft
    ));
    assert_eq!(r.route.on_empty, OnEmpty::Reject);
}

#[test]
fn rules_are_first_match_wins_in_list_order() {
    let p = policy(json!({
        "rules": [
            {"name": "first",  "matches": [{"qos": ["batch"]}],
             "route": {"select": [{"minimize": "PredictedTTFT"}]}},
            {"name": "second", "matches": [{"qos": ["batch"]}],
             "route": {"select": [{"minimize": "PredictedTTFT"}]}}
        ]
    }));
    let attrs = RequestAttrs {
        qos: Some("batch".into()),
        ..Default::default()
    };
    let first = p.rules.iter().find(|r| r.matches(&attrs)).unwrap();
    assert_eq!(first.name, "first", "list order decides");
}

#[test]
fn catch_all_rule_before_others_warns_about_unreachable_rules() {
    let p = policy(json!({
        "rules": [
            {"name": "catch-all", "route": {"select": [{"minimize": "PredictedTTFT"}]}},
            {"name": "never-runs", "matches": [{"qos": ["batch"]}],
             "route": {"select": [{"minimize": "PredictedTTFT"}]}}
        ]
    }));
    assert!(
        p.warnings
            .iter()
            .any(|w| w.contains("can never be reached")),
        "{:?}",
        p.warnings
    );
}

/// An empty `matches` entry is a catch-all too, since entries OR.
#[test]
fn an_empty_matches_entry_shadows_just_like_a_missing_matches() {
    for entries in [json!([{}]), json!([{}, {"qos": ["batch"]}])] {
        let p = policy(json!({
            "rules": [
                {"name": "looks-conditional", "matches": entries,
                 "route": {"select": [{"minimize": "PredictedTTFT"}]}},
                {"name": "never-runs", "matches": [{"qos": ["batch"]}],
                 "route": {"select": [{"minimize": "PredictedTTFT"}]}}
            ]
        }));
        assert!(
            p.warnings
                .iter()
                .any(|w| w.contains("can never be reached")),
            "entries {entries}: {:?}",
            p.warnings
        );
        // And it really does match everything.
        assert!(
            p.rules[0].matches(&RequestAttrs::default()),
            "entries {entries}"
        );
    }
}

/// A catch-all with `onEmpty: FallThrough` doesn't shadow, so doesn't warn.
#[test]
fn a_catch_all_that_falls_through_does_not_shadow() {
    let p = policy(json!({
        "rules": [
            {"name": "try-local-first", "route": {
                "consider": [{"type": "Local"}],
                "select": [{"minimize": "PredictedTTFT"}],
                "onEmpty": "FallThrough"
            }},
            {"name": "then-a-provider", "route": {
                "consider": [{"type": "Provider"}],
                "select": [{"order": ["databricks"]}]
            }}
        ]
    }));
    assert!(
        !p.warnings
            .iter()
            .any(|w| w.contains("can never be reached")),
        "a fall-through catch-all reaches the next rule: {:?}",
        p.warnings
    );
}

/// A catch-all in last position shadows nothing.
#[test]
fn a_catch_all_in_last_position_is_not_flagged() {
    let p = policy(json!({
        "rules": [
            {"name": "specific", "matches": [{"qos": ["batch"]}],
             "route": {"select": [{"minimize": "PredictedTTFT"}]}},
            {"name": "catch-all", "route": {"select": [{"minimize": "PredictedTTFT"}]}}
        ]
    }));
    assert!(
        !p.warnings
            .iter()
            .any(|w| w.contains("can never be reached")),
        "{:?}",
        p.warnings
    );
}

/// A rule naming a tag no classifier can emit is inert, and the warning lists
/// what is emittable.
#[test]
fn a_rule_on_an_unemittable_tag_is_flagged_as_inert() {
    let p = policy(json!({
        "rules": [{"name": "pii-rule", "matches": [{"tag": "pii-detected"}],
                   "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    assert_eq!(p.rules[0].requires_classifiers, vec!["pii"]);
    let w = joined(&p);
    assert!(
        w.contains("never matches"),
        "must say the rule is inert: {w}"
    );
    assert!(
        w.contains("pii-detected"),
        "must name the offending tag: {w}"
    );
    assert!(
        w.contains("pii-unknown"),
        "must name what IS emittable: {w}"
    );
}

/// A rule written against the emittable `-unknown` tags doesn't warn.
#[test]
fn a_rule_on_an_emittable_tag_is_not_flagged() {
    let p = policy(json!({
        "rules": [{"name": "fail-closed", "matches": [{"tag": "pii-unknown"}],
                   "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    assert_eq!(p.rules[0].requires_classifiers, vec!["pii"]);
    assert!(
        !p.warnings.iter().any(|w| w.contains("never matches")),
        "pii-unknown is emitted on every request: {:?}",
        p.warnings
    );
}

/// An exclusion that cannot fire gets a warning in those terms.
#[test]
fn an_exclusion_on_an_unemittable_tag_warns_that_it_never_fires() {
    let p = policy(json!({
        "rules": [{"name": "r", "route": {
            "exclude": [{"name": "no-pii-on-shared", "if": {"tag": "pii-detected"},
                         "drop": {"backend": {"name": "shared"}}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    let w = joined(&p);
    assert!(w.contains("never fires"), "{w}");
    assert!(
        w.contains("still selectable"),
        "must say what that leaves behind: {w}"
    );
}

#[test]
fn unreachable_select_tier_warns() {
    let p = policy(json!({
        "rules": [{"name": "r", "route": {"select": [
            {"minimize": "PredictedTTFT"},
            {"backends": {"names": ["workers-a100"]}, "minimize": "PredictedTTFT"}
        ]}}]
    }));
    assert!(
        p.warnings.iter().any(|w| w.contains("unreachable")),
        "{:?}",
        p.warnings
    );
}

#[test]
fn subsetted_tiers_do_not_warn() {
    let p = policy(json!({
        "rules": [{"name": "r", "route": {"select": [
            {"backends": {"names": ["workers-h100"]}, "minimize": "PredictedTTFT"},
            {"backends": {"names": ["workers-a100"]}, "minimize": "PredictedTTFT"}
        ]}}]
    }));
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);
    assert_eq!(p.rules[0].route.select.len(), 2);
}

#[test]
fn empty_drop_warns_that_it_removes_nothing() {
    let p = policy(json!({
        "rules": [{"name": "r", "route": {
            "exclude": [{"name": "noop", "drop": {}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    assert!(
        p.warnings.iter().any(|w| w.contains("removes nothing")),
        "{:?}",
        p.warnings
    );
}

#[test]
fn weighted_partition_is_detected_and_unweighted_is_not() {
    let weighted = policy(json!({
        "rules": [{"name": "r", "route": {
            "consider": [
                {"name": "a", "models": {"names": ["bar"]}, "weight": 70},
                {"name": "b", "models": {"names": ["bar"]}, "weight": 30}],
            "select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    assert!(
        weighted.rules[0].route.weighted(),
        "weights → sample one group"
    );

    let union = policy(json!({
        "rules": [{"name": "r", "route": {
            "consider": [{"models": {"names": ["foo", "bar"]}}],
            "select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    assert!(
        !union.rules[0].route.weighted(),
        "no weights → union the groups (adds a model, doesn't partition)"
    );
}

/// An unknown field like the removed `identityHeaders` doesn't fail the
/// policy.
#[test]
fn an_unknown_spec_field_does_not_sink_the_policy() {
    let p = policy(json!({
        "identityHeaders": {"tenant": ["X-Acme-Account"]},
        "rules": [{"name": "r", "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    assert_eq!(p.rules.len(), 1, "the policy must still compile");
    assert!(p.warnings.is_empty(), "and stay quiet: {:?}", p.warnings);
}

#[test]
fn defaults_when_no_policy_ref() {
    let p = CompiledPolicy::serve_as_asked("implicit");
    assert_eq!(p.unmatched, Unmatched::ServeLocal);
    assert!(p.rules.is_empty());
    let route = CompiledPolicy::default_route();
    assert!(
        route.exclude.is_empty(),
        "no exclusions, not even a queue threshold"
    );
    assert_eq!(route.consider[0].models, ModelChoice::FromRequest);
}

#[test]
fn backend_match_semantics() {
    let empty = BackendMatch::default();
    assert!(
        empty.matches(BackendType::Local, "anything", &BTreeMap::new()),
        "empty = everything"
    );

    let by_name = BackendMatch {
        backend_type: None,
        names: vec!["workers-h100".into()],
        match_labels: BTreeMap::new(),
    };
    assert!(by_name.matches(BackendType::Local, "workers-h100", &BTreeMap::new()));
    assert!(!by_name.matches(BackendType::Local, "workers-a100", &BTreeMap::new()));

    let by_label = BackendMatch {
        backend_type: None,
        names: Vec::new(),
        match_labels: [("accelerator".to_string(), "h100".to_string())]
            .into_iter()
            .collect(),
    };
    let mut labels = BTreeMap::new();
    labels.insert("accelerator".to_string(), "h100".to_string());
    assert!(by_label.matches(BackendType::Local, "whatever", &labels));
    assert!(!by_label.matches(BackendType::Local, "whatever", &BTreeMap::new()));
}

/// A bad term is reported against its exact location, and fails closed: the
/// entry can never match.
#[test]
fn bad_predicate_inside_a_rule_fails_closed_and_is_located() {
    let p = policy(json!({
        "rules": [{"name": "oops", "matches": [{"model": {"greaterThan": 5}}],
                   "route": {"select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    assert_eq!(p.warnings.len(), 1, "{:?}", p.warnings);
    let w = &p.warnings[0];
    assert_eq!(w.code, WarningCode::InvalidTerm);
    assert_eq!(w.path, "spec.rules[0].matches[0].model");
    assert!(w.message.contains("rule `oops`"), "{w}");
    assert!(w.message.contains("never matches"), "{w}");
    assert!(
        !p.rules[0].matches(&RequestAttrs {
            model: vec!["anything".into()],
            ..Default::default()
        }),
        "an invalid term must make the entry unsatisfiable, not drop out and widen it"
    );
}

/// A quoted number in a `drop` fails closed, so the drop removes nothing
/// rather than every local candidate.
#[test]
fn a_bad_drop_term_removes_nothing_instead_of_everything() {
    let p = policy(json!({
        "rules": [{"name": "r", "route": {
            "exclude": [{"name": "shed", "drop": {
                "backend": {"type": "Local"},
                "queuedPrompts": {"greaterThan": "4"}
            }}],
            "select": [{"minimize": "PredictedTTFT"}]}}]
    }));
    let drop = &p.rules[0].route.exclude[0].drop;
    let local = crate::predicate::CandidateAttrs {
        backend_type: Some("Local".into()),
        queued_prompts: Some(100.0),
        ..Default::default()
    };
    assert!(
        !drop.matches(&local),
        "even a hopelessly backed-up local candidate survives an invalid drop"
    );
    let w = joined(&p);
    assert!(w.contains("InvalidTerm"), "{w}");
    assert!(
        w.contains("spec.rules[0].route.exclude[0].drop.queuedPrompts"),
        "{w}"
    );
    assert!(w.contains("never drops anything"), "{w}");
}

/// The canonical local-first-then-provider shape compiles clean.
#[test]
fn local_first_then_provider_compiles_without_warnings() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [{
            "name": "local-first-with-provider-failover",
            "route": {
                "exclude": [{
                    "name": "skip-backed-up",
                    "drop": {"queuedPrompts": {"greaterThan": 4}, "backend": {"type": "Local"}}
                }],
                "select": [
                    {"type": "Local", "minimize": "PredictedTTFT"},
                    {"type": "Provider", "order": ["together-prod"]}
                ]
            }
        }]
    }))
    .expect("valid fixture");
    let p = CompiledPolicy::compile("p", &spec);
    assert!(
        p.warnings.is_empty(),
        "the canonical v2 shape must not warn: {:?}",
        p.warnings
    );
    // And the filter is actually present on both tiers.
    let tiers = &p.rules[0].route.select;
    assert_eq!(tiers[0].backends.backend_type, Some(BackendType::Local));
    assert_eq!(tiers[1].backends.backend_type, Some(BackendType::Provider));
}

/// The warning is still right when a tier genuinely has no subset.
#[test]
fn a_tier_with_no_subset_at_all_still_warns() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [{
            "name": "r",
            "route": {"select": [
                {"minimize": "EstimatedTTFT"},
                {"type": "Provider", "order": ["together"]}
            ]}
        }]
    }))
    .expect("valid fixture");
    let p = CompiledPolicy::compile("p", &spec);
    assert!(
        p.warnings.iter().any(|w| w.contains("unreachable")),
        "a tier spanning everything really does starve the ones after it: {:?}",
        p.warnings
    );
}

/// And the general rule, so the next one is caught too.
#[test]
fn a_guard_on_an_unemittable_tag_warns_about_failing_open() {
    let spec: InferencePolicySpec = serde_json::from_value(serde_json::json!({
        "rules": [{
            "name": "r",
            "route": {
                "exclude": [{
                    "name": "pii-never-leaves",
                    "if": {"tag": "pii-detected"},
                    "drop": {"backend": {"type": "Provider"}}
                }],
                "select": [{"type": "Local", "minimize": "PredictedTTFT"}]
            }
        }]
    }))
    .expect("valid fixture");
    let p = CompiledPolicy::compile("p", &spec);
    let w = joined(&p);
    assert!(w.contains("pii-detected"), "{w}");
    // The candidates it would have dropped are still selectable.
    assert!(w.contains("still selectable"), "{w}");
}

// --- the louder compiler ----------------------------------------------------
//
// One test per check, each with a near miss that must stay quiet.

use crate::warning::WarningCode;

/// The codes present, for compact assertions.
fn codes(p: &CompiledPolicy) -> Vec<WarningCode> {
    p.warnings.iter().map(|w| w.code).collect()
}

fn one_rule(route: serde_json::Value) -> CompiledPolicy {
    policy(json!({"rules": [{"name": "r", "route": route}]}))
}

#[test]
fn an_empty_matches_entry_and_an_empty_if_say_fields_may_have_been_pruned() {
    let p = policy(json!({"rules": [{
        "name": "r",
        "matches": [{}],
        "route": {
            "exclude": [{"name": "e", "if": {}, "drop": {"backend": {"name": "x"}}}],
            "select": [{"minimize": "PredictedTTFT"}]
        }
    }]}));
    let empties: Vec<_> = p
        .warnings
        .iter()
        .filter(|w| w.code == WarningCode::EmptyPredicate)
        .collect();
    assert_eq!(empties.len(), 2, "{:?}", p.warnings);
    assert_eq!(empties[0].path, "spec.rules[0].matches[0]");
    assert_eq!(empties[1].path, "spec.rules[0].route.exclude[0].if");
    assert!(
        empties[1].message.contains("applies to every request"),
        "{}",
        empties[1]
    );
    assert!(
        empties[0].message.contains("silently drops fields"),
        "must name the likely cause: {}",
        empties[0]
    );

    // A rule with no `matches` key is a catch-all, not an empty entry.
    let catch_all = policy(json!({"rules": [{"name": "r",
        "route": {"select": [{"minimize": "PredictedTTFT"}]}}]}));
    assert!(catch_all.warnings.is_empty(), "{:?}", catch_all.warnings);
}

#[test]
fn duplicate_names_are_flagged_at_every_level() {
    let p = policy(json!({"rules": [
        {"name": "dup", "matches": [{"qos": ["batch"]}], "route": {
            "consider": [{"name": "g", "type": "Local"}, {"name": "g", "type": "Local"}],
            "exclude": [
                {"name": "e", "drop": {"backend": {"name": "a"}}},
                {"name": "e", "drop": {"backend": {"name": "b"}}}
            ],
            "select": [{"minimize": "PredictedTTFT"}]}},
        {"name": "dup", "matches": [{"qos": ["interactive"]}],
         "route": {"select": [{"minimize": "PredictedTTFT"}]}}
    ]}));
    let paths: Vec<&str> = p
        .warnings
        .iter()
        .filter(|w| w.code == WarningCode::DuplicateName)
        .map(|w| w.path.as_str())
        .collect();
    assert_eq!(
        paths,
        [
            "spec.rules[0].route.consider[1].name",
            "spec.rules[0].route.exclude[1].name",
            "spec.rules[1].name",
        ],
        "{:?}",
        p.warnings
    );
}

#[test]
fn a_rule_repeating_an_earlier_rules_matches_is_shadowed() {
    let p = policy(json!({"rules": [
        {"name": "first", "matches": [{"qos": ["batch"]}],
         "route": {"select": [{"minimize": "PredictedTTFT"}]}},
        {"name": "second", "matches": [{"qos": ["batch"]}],
         "route": {"select": [{"minimize": "PredictedTTFT"}]}}
    ]}));
    let w = joined(&p);
    assert!(w.contains("ShadowedRule: spec.rules[1].matches"), "{w}");
    assert!(w.contains("same `matches` as rule `first`"), "{w}");

    // ...unless the earlier one falls through, which is exactly how a
    // "try local, then a provider" pair is written.
    let fall = policy(json!({"rules": [
        {"name": "first", "matches": [{"qos": ["batch"]}],
         "route": {"select": [{"minimize": "PredictedTTFT"}], "onEmpty": "FallThrough"}},
        {"name": "second", "matches": [{"qos": ["batch"]}],
         "route": {"select": [{"minimize": "PredictedTTFT"}]}}
    ]}));
    assert!(
        !codes(&fall).contains(&WarningCode::ShadowedRule),
        "{:?}",
        fall.warnings
    );
}

#[test]
fn a_percentage_against_a_fraction_is_caught_with_the_likely_fix() {
    let p = one_rule(json!({
        "exclude": [{"name": "shed-full", "drop": {
            "backend": {"type": "Local"}, "kvCacheUsage": {"greaterThan": 80}}}],
        "select": [{"minimize": "PredictedTTFT"}]
    }));
    let w = joined(&p);
    assert!(
        w.contains("ThresholdOutOfRange: spec.rules[0].route.exclude[0].drop.kvCacheUsage"),
        "{w}"
    );
    assert!(w.contains("never true"), "{w}");
    assert!(w.contains("Did you mean 0.8?"), "{w}");
    assert!(w.contains("never drops anything"), "{w}");
}

#[test]
fn constant_comparisons_are_caught_in_both_directions() {
    // Never true: nothing has fewer than zero queued prompts.
    let never = one_rule(json!({
        "exclude": [{"name": "e", "drop": {
            "backend": {"type": "Local"}, "queuedPrompts": {"lessThan": 0}}}],
        "select": [{"minimize": "PredictedTTFT"}]
    }));
    assert!(
        joined(&never).contains("is never true"),
        "{}",
        joined(&never)
    );

    // Always true: every local candidate has at least zero, so this drops all
    // of them.
    let always = one_rule(json!({
        "exclude": [{"name": "e", "drop": {
            "backend": {"type": "Local"}, "queuedPrompts": {"greaterThanOrEqual": 0}}}],
        "select": [{"minimize": "PredictedTTFT"}]
    }));
    let w = joined(&always);
    assert!(w.contains("is always true"), "{w}");
    assert!(w.contains("drops every candidate"), "{w}");

    // Request scope too.
    let req = policy(json!({"rules": [{"name": "r",
        "matches": [{"inputTokens": {"lessThan": 0}}],
        "route": {"select": [{"minimize": "PredictedTTFT"}]}}]}));
    assert!(
        joined(&req).contains("spec.rules[0].matches[0].inputTokens"),
        "{}",
        joined(&req)
    );

    // In range: silence.
    let fine = one_rule(json!({
        "exclude": [{"name": "e", "drop": {
            "backend": {"type": "Local"},
            "kvCacheUsage": {"greaterThan": 0.8},
            "queuedPrompts": {"greaterThan": 4}}}],
        "select": [{"minimize": "PredictedTTFT"}]
    }));
    assert!(fine.warnings.is_empty(), "{:?}", fine.warnings);
}

#[test]
fn a_lowercase_backend_type_is_an_invalid_enum_value() {
    let p = one_rule(json!({
        "exclude": [{"name": "e", "drop": {"backend": {"type": "local"}}}],
        "select": [{"minimize": "PredictedTTFT"}]
    }));
    let w = joined(&p);
    assert!(
        w.contains("InvalidEnumValue: spec.rules[0].route.exclude[0].drop.backend.type"),
        "{w}"
    );
    assert!(w.contains("Did you mean `Local`?"), "{w}");
}

#[test]
fn a_number_where_a_string_belongs_is_a_coerced_value() {
    let p = policy(json!({"rules": [{"name": "r",
        "matches": [{"tenant": 7}],
        "route": {"select": [{"minimize": "PredictedTTFT"}]}}]}));
    assert_eq!(codes(&p), [WarningCode::CoercedValue], "{:?}", p.warnings);
    assert_eq!(p.warnings[0].path, "spec.rules[0].matches[0].tenant");
}

#[test]
fn partial_weights_name_the_unweighted_groups_and_the_real_split() {
    let p = one_rule(json!({
        "consider": [
            {"name": "big", "models": {"names": ["a"]}, "weight": 70},
            {"name": "rest", "models": {"names": ["b"]}}
        ],
        "select": [{"minimize": "PredictedTTFT"}]
    }));
    let w = joined(&p);
    assert!(
        w.contains("PartialWeights: spec.rules[0].route.consider"),
        "{w}"
    );
    assert!(w.contains("`rest`"), "{w}");
    assert!(w.contains("70:1"), "{w}");

    let all = one_rule(json!({
        "consider": [
            {"models": {"names": ["a"]}, "weight": 70},
            {"models": {"names": ["b"]}, "weight": 30}
        ],
        "select": [{"minimize": "PredictedTTFT"}]
    }));
    assert!(all.warnings.is_empty(), "{:?}", all.warnings);
}

#[test]
fn an_order_tier_over_local_endpoints_needs_a_random_tie_break() {
    let pinned = one_rule(json!({"select": [{"order": ["qwen-ws", "gemma-ws"]}]}));
    let w = joined(&pinned);
    assert!(
        w.contains("OrderWithoutRandom: spec.rules[0].route.select[0].order"),
        "{w}"
    );
    assert!(w.contains("same endpoint wins every time"), "{w}");

    let randomized = one_rule(json!({
        "select": [{"order": ["qwen-ws", "gemma-ws"]}],
        "tieBreak": ["Random"]
    }));
    assert!(randomized.warnings.is_empty(), "{:?}", randomized.warnings);

    // Providers have one row per model and nothing to spread: exempt, whether
    // the tier or the consider groups say so.
    let provider_tier = one_rule(json!({"select": [{"type": "Provider", "order": ["vendor"]}]}));
    assert!(
        provider_tier.warnings.is_empty(),
        "{:?}",
        provider_tier.warnings
    );
    let provider_only = one_rule(json!({
        "consider": [{"type": "Provider"}],
        "select": [{"order": ["vendor"]}]
    }));
    assert!(
        provider_only.warnings.is_empty(),
        "{:?}",
        provider_only.warnings
    );
}

/// `{not: X}` over a tag nothing emits is true for every request, and the
/// warning says so.
#[test]
fn a_negated_unemittable_tag_is_reported_as_always_true() {
    let p = one_rule(json!({
        "exclude": [{"name": "only-unscreened",
                     "if": {"tag": {"not": "pii-detected"}},
                     "drop": {"backend": {"type": "Provider"}}}],
        "select": [{"type": "Local", "minimize": "PredictedTTFT"}]
    }));
    let w = joined(&p);
    assert!(w.contains("true for every request"), "{w}");
    assert!(!w.contains("never fires"), "the opposite of the truth: {w}");
}

/// An array with one live tag still matches on it; only the dead alternatives
/// are inert, and the warning says which are which.
#[test]
fn an_array_with_some_unemittable_tags_still_matches_on_the_rest() {
    let p = policy(json!({"rules": [{"name": "r",
        "matches": [{"tag": ["pii-confirmed", "ssn"]}],
        "route": {"select": [{"minimize": "PredictedTTFT"}]}}]}));
    let w = joined(&p);
    assert!(
        w.contains("UnemittableTag: spec.rules[0].matches[0].tag"),
        "{w}"
    );
    assert!(w.contains("still matches on `pii-confirmed`"), "{w}");
    assert!(!w.contains("never matches"), "{w}");
    // `ssn` has no classifier prefix; the emittable list is still non-empty.
    assert!(w.contains("Emittable today: complexity-"), "{w}");
}

/// A `drop` pinned to the class that does NOT carry its signal can never fire.
#[test]
fn a_drop_pinned_to_the_wrong_class_never_drops_anything() {
    let p = one_rule(json!({
        "exclude": [{"name": "e", "drop": {
            "backend": {"type": "Provider"}, "queuedPrompts": {"greaterThan": 4}}}],
        "select": [{"minimize": "PredictedTTFT"}]
    }));
    let w = joined(&p);
    assert!(
        w.contains("DropTermWrongClass: spec.rules[0].route.exclude[0].drop.queuedPrompts"),
        "{w}"
    );
    assert!(w.contains("admits only Provider ones"), "{w}");
}

#[test]
fn from_request_false_without_names_says_so() {
    let p = one_rule(json!({
        "consider": [{"models": {"fromRequest": false}}],
        "select": [{"minimize": "PredictedTTFT"}]
    }));
    let w = joined(&p);
    assert!(
        w.contains("ConsiderModelChoice: spec.rules[0].route.consider[0].models"),
        "{w}"
    );
    assert!(w.contains("`fromRequest: false` with no `names`"), "{w}");
}

/// Every warning must point somewhere an editor can go. Built from one policy
/// that trips most checks at once, so a new check that forgets its path fails
/// here even if its own test only looks at the message.
#[test]
fn every_warning_carries_a_manifest_path() {
    let p = policy(json!({"rules": [
        {"name": "a", "matches": [{}, {"tag": "ssn", "inputTokens": "big"}], "route": {
            "consider": [{"weight": 3}, {"name": "x"}, {"name": "x"}],
            "exclude": [
                {"name": "e", "if": {}, "drop": {}},
                {"name": "e", "drop": {"backend": {"type": "local", "budgetExhausted": true},
                                       "kvCacheUsage": {"greaterThan": 50}}}
            ],
            "select": [
                {"order": ["w"]},
                {"backends": {"selector": {"matchLabels": {"k": "v"}}},
                 "minimize": "PredictedTTFT"}
            ]}},
        {"name": "a", "matches": [{"qos": ["batch"]}],
         "route": {"select": [{"minimize": "PredictedTTFT"}]}}
    ]}));
    assert!(p.warnings.len() >= 10, "{:?}", p.warnings);
    for w in &p.warnings {
        assert!(w.path.starts_with("spec."), "{w}");
        assert!(!w.message.is_empty(), "{w}");
    }
}
