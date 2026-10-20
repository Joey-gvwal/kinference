use super::*;
use serde_json::json;

fn req(model: &str) -> RequestAttrs {
    RequestAttrs {
        model: vec![model.to_string()],
        ..Default::default()
    }
}

fn compile_ok(raw: serde_json::Value, kind: AttrKind) -> Matcher {
    Matcher::compile(&raw, kind).expect("should compile")
}

// --- the three RHS shapes -------------------------------------------

#[test]
fn scalar_is_exact_match() {
    let m = compile_ok(json!("foo"), AttrKind::Str);
    assert!(m.eval(Some(&AttrValue::Str("foo".into()))));
    assert!(!m.eval(Some(&AttrValue::Str("bar".into()))));
}

#[test]
fn array_is_membership_for_a_scalar_attribute() {
    let m = compile_ok(json!(["batch", "deferrable"]), AttrKind::Str);
    assert!(m.eval(Some(&AttrValue::Str("batch".into()))));
    assert!(m.eval(Some(&AttrValue::Str("deferrable".into()))));
    assert!(!m.eval(Some(&AttrValue::Str("interactive".into()))));
}

#[test]
fn array_is_intersection_for_a_list_attribute() {
    // Membership; intersection for list-valued attributes.
    let m = compile_ok(json!(["ssn", "email"]), AttrKind::StrList);
    assert!(
        m.eval(Some(&AttrValue::List(vec!["email".into(), "phone".into()]))),
        "one overlapping element is enough"
    );
    assert!(!m.eval(Some(&AttrValue::List(vec!["phone".into()]))));
}

#[test]
fn hash_is_an_explicit_operator() {
    let m = compile_ok(json!({"greaterThan": 8}), AttrKind::Int);
    assert_eq!(m, Matcher::GreaterThan(8.0));
    assert!(m.eval(Some(&AttrValue::Int(9))));
    assert!(!m.eval(Some(&AttrValue::Int(8))), "strictly greater");
}

// --- every operator --------------------------------------------------

#[test]
fn ordering_operators() {
    let gt = compile_ok(json!({"greaterThan": 10}), AttrKind::Int);
    let lt = compile_ok(json!({"lessThan": 10}), AttrKind::Int);
    let gte = compile_ok(json!({"greaterThanOrEqual": 10}), AttrKind::Int);
    let lte = compile_ok(json!({"lessThanOrEqual": 10}), AttrKind::Int);
    let ten = AttrValue::Int(10);
    assert!(!gt.eval(Some(&ten)));
    assert!(!lt.eval(Some(&ten)));
    assert!(gte.eval(Some(&ten)));
    assert!(lte.eval(Some(&ten)));
}

#[test]
fn not_negates() {
    let m = compile_ok(json!({"not": "workers-a100"}), AttrKind::Str);
    assert!(m.eval(Some(&AttrValue::Str("workers-h100".into()))));
    assert!(!m.eval(Some(&AttrValue::Str("workers-a100".into()))));
}

#[test]
fn not_over_an_array_negates_membership() {
    let m = compile_ok(json!({"not": ["a", "b"]}), AttrKind::Str);
    assert!(m.eval(Some(&AttrValue::Str("c".into()))));
    assert!(!m.eval(Some(&AttrValue::Str("a".into()))));
}

#[test]
fn contains_all_requires_every_element() {
    let m = compile_ok(json!({"containsAll": ["ssn", "email"]}), AttrKind::StrList);
    assert!(m.eval(Some(&AttrValue::List(vec![
        "email".into(),
        "ssn".into(),
        "phone".into()
    ]))));
    assert!(!m.eval(Some(&AttrValue::List(vec!["ssn".into()]))));
}

#[test]
fn exists_is_the_only_form_that_matches_absence() {
    let present = compile_ok(json!({"exists": true}), AttrKind::Str);
    let absent = compile_ok(json!({"exists": false}), AttrKind::Str);
    assert!(present.eval(Some(&AttrValue::Str("x".into()))));
    assert!(!present.eval(None));
    assert!(absent.eval(None));
    assert!(!absent.eval(Some(&AttrValue::Str("x".into()))));
}

// --- absence ---------------------------------------------------------

#[test]
fn absent_attribute_never_matches() {
    // A predicate over a signal the candidate does not carry does not match.
    for raw in [
        json!("foo"),
        json!(["foo"]),
        json!({"not": "foo"}),
        json!({"greaterThan": 1}),
    ] {
        let kind = if raw.is_object() && raw.get("greaterThan").is_some() {
            AttrKind::Int
        } else {
            AttrKind::Str
        };
        let m = Matcher::compile(&raw, kind).unwrap();
        assert!(!m.eval(None), "absent must not match {raw}");
    }
}

// --- model matching, alias-aware -------------------------------------

/// `matches: {model: ...}` matches aliases, as `consider[].models.names[]`
/// does, even though the alias was rewritten to the upstream.
#[test]
fn a_model_predicate_matches_the_alias_the_client_actually_sent() {
    let attrs = RequestAttrs {
        model: vec![
            "Qwen/Qwen2.5-32B-Instruct-AWQ".into(),
            "qwen-2.5".into(),
            "qwen-simple".into(),
        ],
        ..Default::default()
    };
    for spelling in ["Qwen/Qwen2.5-32B-Instruct-AWQ", "qwen-2.5", "qwen-simple"] {
        let mut w = Vec::new();
        let raw: RequestPredicate = serde_json::from_value(json!({"model": spelling})).unwrap();
        let p = CompiledRequestPredicate::compile(&raw, &mut w);
        assert!(w.is_empty(), "{spelling}: {w:?}");
        assert!(p.matches(&attrs), "`model: {spelling}` must match");
    }
}

/// An unrelated model name is still a non-match, and `not` means
/// absent from the set.
#[test]
fn a_model_predicate_still_rejects_an_unrelated_name() {
    let attrs = RequestAttrs {
        model: vec!["Qwen/Qwen2.5-32B-Instruct-AWQ".into(), "qwen-simple".into()],
        ..Default::default()
    };
    for (raw, want) in [
        (json!({"model": "google/gemma-4-12B-it"}), false),
        (
            json!({"model": ["google/gemma-4-12B-it", "qwen-simple"]}),
            true,
        ),
        (json!({"model": {"not": "google/gemma-4-12B-it"}}), true),
        (json!({"model": {"not": "qwen-simple"}}), false),
    ] {
        let mut w = Vec::new();
        let pred: RequestPredicate = serde_json::from_value(raw.clone()).unwrap();
        let p = CompiledRequestPredicate::compile(&pred, &mut w);
        assert!(w.is_empty(), "{raw}: {w:?}");
        assert_eq!(p.matches(&attrs), want, "{raw}");
    }
}

/// An undetermined model is an empty set, not a wildcard.
#[test]
fn an_empty_model_set_matches_no_named_model() {
    let attrs = RequestAttrs::default();
    assert_eq!(
        attrs.get(&RequestAttr::Model),
        Some(AttrValue::List(vec![]))
    );
    let mut w = Vec::new();
    let raw: RequestPredicate = serde_json::from_value(json!({"model": "anything"})).unwrap();
    let p = CompiledRequestPredicate::compile(&raw, &mut w);
    assert!(!p.matches(&attrs));
}

/// Set semantics on `model`: a scalar is contains (not equality), an array
/// intersects, `containsAll` is all-of, and `not` is absence from the set.
#[test]
fn model_matching_has_set_semantics() {
    let mut attrs = req("qwen");
    attrs.model.push("qwen-alias".into());

    for (raw, want, why) in [
        (
            json!({"model": "qwen-alias"}),
            true,
            "scalar means contains",
        ),
        (json!({"model": "llama"}), false, "a name not in the set"),
        (
            json!({"model": ["llama", "qwen-alias"]}),
            true,
            "array intersects",
        ),
        (
            json!({"model": ["llama", "mistral"]}),
            false,
            "array with no overlap",
        ),
        (
            json!({"model": {"containsAll": ["qwen", "qwen-alias"]}}),
            true,
            "containsAll with both present",
        ),
        (
            json!({"model": {"containsAll": ["qwen", "llama"]}}),
            false,
            "containsAll missing one",
        ),
        (json!({"model": {"not": "llama"}}), true, "not means absent"),
        (
            json!({"model": {"not": "qwen"}}),
            false,
            "not on a present name",
        ),
    ] {
        let mut w = Vec::new();
        let pred: RequestPredicate = serde_json::from_value(raw.clone()).unwrap();
        let p = CompiledRequestPredicate::compile(&pred, &mut w);
        assert!(w.is_empty(), "{raw} should compile cleanly: {w:?}");
        assert_eq!(p.matches(&attrs), want, "{raw}: {why}");
    }
}

// --- AND / OR --------------------------------------------------------

#[test]
fn keys_within_one_predicate_are_anded() {
    let mut w = Vec::new();
    let raw: RequestPredicate =
        serde_json::from_value(json!({"model": "bar", "qos": ["batch"]})).unwrap();
    let p = CompiledRequestPredicate::compile(&raw, &mut w);
    let mut attrs = req("bar");
    assert!(!p.matches(&attrs), "qos missing → no match");
    attrs.qos = Some("batch".into());
    assert!(p.matches(&attrs));
    attrs.qos = Some("interactive".into());
    assert!(!p.matches(&attrs));
}

#[test]
fn empty_request_predicate_matches_everything() {
    // A rule with no matches matches everything.
    let p = CompiledRequestPredicate::default();
    assert!(p.matches(&RequestAttrs::default()));
}

#[test]
fn empty_candidate_predicate_matches_nothing() {
    // An exclusion that says nothing removes nothing.
    let p = CompiledCandidatePredicate::default();
    assert!(!p.matches(&CandidateAttrs::default()));
}

// --- durations -------------------------------------------------------

#[test]
fn duration_units_parse() {
    assert_eq!(parse_duration_ms("200ms").unwrap(), 200.0);
    assert_eq!(parse_duration_ms("5s").unwrap(), 5_000.0);
    assert_eq!(parse_duration_ms("2m").unwrap(), 120_000.0);
    assert_eq!(parse_duration_ms("1h").unwrap(), 3_600_000.0);
    // Compound, and `ms` must not be read as minutes.
    assert_eq!(parse_duration_ms("1m30s").unwrap(), 90_000.0);
    assert_eq!(parse_duration_ms("1s500ms").unwrap(), 1_500.0);
    assert!(parse_duration_ms("5").is_err(), "unit required");
    assert!(parse_duration_ms("abc").is_err());
}

#[test]
fn duration_predicate_compares_against_millis() {
    let m = compile_ok(json!({"greaterThan": "1s"}), AttrKind::Duration);
    assert!(m.eval(Some(&AttrValue::DurationMs(1_500.0))));
    assert!(!m.eval(Some(&AttrValue::DurationMs(900.0))));
}

// --- compile-time validation ----------------------------------------

#[test]
fn operator_type_mismatch_is_a_compile_error() {
    // `model` is a string; ordering does not apply.
    let e = Matcher::compile(&json!({"greaterThan": 5}), AttrKind::Str).unwrap_err();
    assert!(e.contains("numeric"), "{e}");
    let e = Matcher::compile(&json!({"containsAll": ["a"]}), AttrKind::Str).unwrap_err();
    assert!(e.contains("list-valued"), "{e}");
}

#[test]
fn quoted_numeric_threshold_is_rejected_with_guidance() {
    // Numbers, not strings.
    let e = Matcher::compile(&json!({"greaterThan": "0.05"}), AttrKind::Float).unwrap_err();
    assert!(e.contains("numbers, not strings"), "{e}");
}

#[test]
fn bare_number_for_a_duration_is_rejected() {
    let e = Matcher::compile(&json!({"lessThan": 5}), AttrKind::Duration).unwrap_err();
    assert!(e.contains("units"), "{e}");
}

#[test]
fn multi_key_operator_hash_is_rejected() {
    let e = Matcher::compile(&json!({"greaterThan": 1, "lessThan": 5}), AttrKind::Int).unwrap_err();
    assert!(e.contains("exactly one key"), "{e}");
}

#[test]
fn unknown_operator_lists_the_valid_ones() {
    let e = Matcher::compile(&json!({"between": [1, 2]}), AttrKind::Int).unwrap_err();
    assert!(e.contains("unknown operator"), "{e}");
    assert!(
        e.contains("greaterThanOrEqual"),
        "should list valid ops: {e}"
    );
}

/// A bad term is reported and kept as [`Matcher::never`], so the entry
/// matches nothing rather than being widened.
#[test]
fn bad_term_fails_closed_instead_of_widening_the_predicate() {
    let mut issues = Vec::new();
    let raw: RequestPredicate =
        serde_json::from_value(json!({"model": {"greaterThan": 5}, "qos": ["batch"]})).unwrap();
    let p = CompiledRequestPredicate::compile(&raw, &mut issues);
    assert_eq!(issues.len(), 1, "{issues:?}");
    assert_eq!(issues[0].attr, "model");
    assert!(issues[0].fatal);
    assert_eq!(
        p.terms.len(),
        2,
        "the bad term stays, as one that never matches"
    );
    assert!(
        !p.matches(&RequestAttrs {
            model: vec!["m".into()],
            qos: Some("batch".into()),
            ..Default::default()
        }),
        "a batch request must not match an entry whose other term is invalid"
    );
}

/// The same for a `drop`: the never-matching term makes it remove nothing.
#[test]
fn bad_candidate_term_fails_closed() {
    let mut issues = Vec::new();
    let raw: CandidatePredicate = serde_json::from_value(json!({
        "backend": {"type": "Local"},
        "kvCacheUsage": {"greaterThan": "0.5"}
    }))
    .unwrap();
    let p = CompiledCandidatePredicate::compile(&raw, &mut issues);
    assert_eq!(issues.len(), 1, "{issues:?}");
    assert_eq!(issues[0].attr, "kvCacheUsage");
    assert!(!p.matches(&CandidateAttrs {
        backend_type: Some("Local".into()),
        kv_cache_usage: Some(0.99),
        ..Default::default()
    }));
}

/// A value converted rather than rejected is reported, but is not fatal.
#[test]
fn a_coerced_value_is_noted_but_compiles() {
    let mut issues = Vec::new();
    let raw: RequestPredicate = serde_json::from_value(json!({"tenant": 7})).unwrap();
    let p = CompiledRequestPredicate::compile(&raw, &mut issues);
    assert_eq!(issues.len(), 1, "{issues:?}");
    assert!(!issues[0].fatal);
    assert!(issues[0].message.contains("\"7\""), "{issues:?}");
    assert!(p.matches(&RequestAttrs {
        tenant: "7".into(),
        ..Default::default()
    }));
}

// --- candidate scope -------------------------------------------------

#[test]
fn candidate_exclusion_over_queued_prompts() {
    let mut w = Vec::new();
    let raw: CandidatePredicate =
        serde_json::from_value(json!({"queuedPrompts": {"greaterThan": 8}})).unwrap();
    let p = CompiledCandidatePredicate::compile(&raw, &mut w);
    assert!(w.is_empty());
    let mut c = CandidateAttrs {
        queued_prompts: Some(9.0),
        ..Default::default()
    };
    assert!(p.matches(&c), "9 > 8 → dropped");
    c.queued_prompts = Some(2.0);
    assert!(!p.matches(&c));
}

#[test]
fn candidate_nested_backend_and_model_paths_compile() {
    let mut w = Vec::new();
    let raw: CandidatePredicate = serde_json::from_value(json!({
        "backend": {"name": "workers-a100", "readyEndpoints": {"lessThan": 2}},
        "model": {"name": {"not": "bar"}}
    }))
    .unwrap();
    let p = CompiledCandidatePredicate::compile(&raw, &mut w);
    assert!(w.is_empty(), "{w:?}");
    assert_eq!(
        p.terms.len(),
        3,
        "backend.name + backend.readyEndpoints + model.name"
    );
    // All three ANDed: needs the a100 backend, <2 ready, and model != bar.
    let c = CandidateAttrs {
        backend_name: Some("workers-a100".into()),
        backend_ready_endpoints: Some(1),
        model_name: Some("foo".into()),
        ..Default::default()
    };
    assert!(p.matches(&c));
    let c2 = CandidateAttrs {
        model_name: Some("bar".into()),
        ..c.clone()
    };
    assert!(!p.matches(&c2), "model.name == bar fails the `not`");
}

/// `path` and `headers` are not part of the grammar; the route already
/// matched on them. The API server prunes them, so a rule written with them
/// is broader than intended, and only an apply-time check can catch that.
#[test]
fn path_and_header_keys_are_not_part_of_the_grammar() {
    let mut w = Vec::new();
    let raw: RequestPredicate = serde_json::from_value(json!({
        "model": "foo",
        "path": "/v1/chat/completions",
        "headers": {"x-acme-account": "health-eu"},
    }))
    .unwrap();
    let p = CompiledRequestPredicate::compile(&raw, &mut w);

    assert_eq!(
        p.terms.len(),
        1,
        "only `model` is a real term: {:?}",
        p.terms
    );
    assert_eq!(p.terms[0].0, RequestAttr::Model);
    assert!(w.is_empty(), "an absent key is not an error: {w:?}");
    assert!(p.matches(&RequestAttrs {
        model: vec!["foo".into()],
        ..Default::default()
    }));
}

/// `qos` is array-only over a closed enum; every other shape fails closed
/// with a warning. The API server rejects these, but a policy admitted under
/// an older schema still reaches the compiler.
#[test]
fn qos_accepts_only_arrays_over_the_closed_set() {
    let compile = |v: serde_json::Value| {
        let mut w = Vec::new();
        let raw: RequestPredicate = serde_json::from_value(json!({"qos": v})).unwrap();
        let p = CompiledRequestPredicate::compile(&raw, &mut w);
        (p, w)
    };
    let batch = RequestAttrs {
        qos: Some("batch".into()),
        ..Default::default()
    };

    // The conformant form: membership over the enum.
    let (p, w) = compile(json!(["batch", "deferrable"]));
    assert!(w.is_empty(), "valid qos must not warn: {w:?}");
    assert!(p.matches(&batch));
    assert!(!p.matches(&RequestAttrs {
        qos: Some("interactive".into()),
        ..Default::default()
    }));

    // Each rejected shape: warns, and blocks the match rather than widening.
    for bad in [
        json!("batch"),          // bare scalar
        json!({"not": "batch"}), // operator hash
        json!(["bacth"]),        // typo inside the array
        json!([]),               // empty
        json!([7]),              // wrong element type
    ] {
        let (p, w) = compile(bad.clone());
        assert!(!w.is_empty(), "expected a warning for {bad}");
        assert_eq!(p.terms.len(), 1, "the term must be retained for {bad}");
        assert!(
            !p.matches(&batch),
            "{bad} must not match a batch request — failing open here would \
             turn a qos-scoped rule into a catch-all"
        );
    }
}

/// An empty tenant is a value, not an absence, so a policy can name the
/// unattributed case.
#[test]
fn an_empty_tenant_matches_a_predicate_naming_the_empty_string() {
    let mut w = Vec::new();
    let raw: RequestPredicate = serde_json::from_value(json!({"tenant": ""})).unwrap();
    let p = CompiledRequestPredicate::compile(&raw, &mut w);
    assert!(w.is_empty(), "`tenant: \"\"` is a valid predicate: {w:?}");

    assert!(
        p.matches(&RequestAttrs::default()),
        "an unattributed request must match `tenant: \"\"`, so a rule set can \
         route it deliberately instead of falling through"
    );
}

/// An unattributed request is not admitted by a rule written for a real
/// tenant.
#[test]
fn an_empty_tenant_does_not_match_a_named_tenant() {
    let mut w = Vec::new();
    let raw: RequestPredicate = serde_json::from_value(json!({"tenant": "acme"})).unwrap();
    let p = CompiledRequestPredicate::compile(&raw, &mut w);
    assert!(w.is_empty(), "`tenant: acme` is a valid predicate: {w:?}");

    assert!(
        !p.matches(&RequestAttrs::default()),
        "an empty tenant must not satisfy `tenant: acme`"
    );
    assert!(
        p.matches(&RequestAttrs {
            tenant: "acme".into(),
            ..Default::default()
        }),
        "the named tenant itself must still match"
    );
}

/// Every candidate attribute's display name (`CandidateAttr::name()`, as
/// printed in traces and declared in the CRD) is the name serde accepts.
/// Otherwise serde would silently ignore the field and the predicate would
/// vanish.
#[test]
fn every_candidate_attr_name_round_trips_through_serde() {
    use crate::crd::CandidatePredicate;
    use serde_json::json;

    // A dotted display name is the spec path (`backend.readyEndpoints` nests
    // under `backend`).
    let all = [
        CandidateAttr::ContextWindow,
        CandidateAttr::PredictedTtft,
        CandidateAttr::KvCacheUsage,
        CandidateAttr::PrefixCacheHitRatio,
        CandidateAttr::QueuedPrompts,
        CandidateAttr::ModelName,
        CandidateAttr::BackendName,
        CandidateAttr::BackendType,
        CandidateAttr::BackendReadyEndpoints,
        CandidateAttr::BackendObservedTtftP95,
        CandidateAttr::BackendBudgetExhausted,
    ];

    for attr in all {
        // A value of the right kind, so only naming can fail.
        let value = match attr.kind() {
            AttrKind::Duration => json!({ "greaterThan": "500ms" }),
            AttrKind::Bool => json!(true),
            AttrKind::Str | AttrKind::StrList => json!("x"),
            AttrKind::Int | AttrKind::Float => json!({ "greaterThan": 1 }),
        };
        // Rebuild the spec shape from the dotted path, innermost first.
        let mut body = value;
        for seg in attr.name().split('.').rev() {
            body = json!({ seg: body });
        }

        let raw: CandidatePredicate = serde_json::from_value(body.clone())
            .unwrap_or_else(|e| panic!("`{}` did not parse ({e}): {body}", attr.name()));
        let mut warnings = Vec::new();
        let compiled = CompiledCandidatePredicate::compile(&raw, &mut warnings);
        assert!(
            compiled.terms.iter().any(|(a, _)| *a == attr),
            "`{}` is the name traces and the CRD schema use, but serde dropped it \
             — the predicate would silently never fire. warnings: {warnings:?}",
            attr.name()
        );
    }
}
