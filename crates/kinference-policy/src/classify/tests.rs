use super::*;

fn tags(text: &str) -> Vec<String> {
    tags_for_request("test", Some(text))
}

#[test]
fn keyword_rules() {
    assert_eq!(tags("a plain question"), ["complexity-normal", "pii-none"]);
    assert_eq!(tags("add oregano"), ["complexity-low", "pii-none"]);
    assert_eq!(tags("add basil"), ["complexity-high", "pii-none"]);
    assert_eq!(
        tags("I live in Kansas"),
        ["complexity-normal", "pii-confirmed"]
    );
    assert_eq!(
        tags("basil in Kansas"),
        ["complexity-high", "pii-confirmed"]
    );
}

/// `basil` wins over `oregano`.
#[test]
fn basil_outranks_oregano() {
    assert_eq!(tags("oregano and basil"), ["complexity-high", "pii-none"]);
    assert_eq!(tags("basil and oregano"), ["complexity-high", "pii-none"]);
}

#[test]
fn matching_is_case_insensitive() {
    assert_eq!(tags("BASIL"), ["complexity-high", "pii-none"]);
    assert_eq!(tags("kansas"), ["complexity-normal", "pii-confirmed"]);
}

/// Whole-word matching: "Arkansas" doesn't match "kansas".
#[test]
fn a_word_inside_another_word_does_not_match() {
    assert_eq!(
        tags("I live in Arkansas"),
        ["complexity-normal", "pii-none"]
    );
    assert_eq!(
        tags("oreganos are plural"),
        ["complexity-normal", "pii-none"]
    );
    // Punctuation still bounds a word.
    assert_eq!(
        tags("Kansas, actually."),
        ["complexity-normal", "pii-confirmed"]
    );
}

/// No user turn means `-unknown` tags, not a default verdict.
#[test]
fn an_unreadable_request_reports_unknown_not_a_default() {
    let t = tags_for_request("test", None);
    assert_eq!(t, ["complexity-unknown", "pii-unknown"]);
    assert!(!t.iter().any(|x| x == "pii-none"));
}

#[test]
fn the_tag_set_is_never_empty() {
    assert!(!tags_for_request("test", None).is_empty());
    assert!(!tags("anything").is_empty());
}

#[test]
fn last_user_turn_wins_and_handles_both_content_shapes() {
    let msgs = serde_json::json!([
        {"role": "user", "content": "basil"},
        {"role": "assistant", "content": "ok"},
        {"role": "user", "content": "oregano"}
    ]);
    assert_eq!(
        last_user_text(msgs.as_array().unwrap()).as_deref(),
        Some("oregano")
    );

    let multimodal = serde_json::json!([
        {"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": "..."}},
            {"type": "text", "text": "with basil"}
        ]}
    ]);
    assert_eq!(
        last_user_text(multimodal.as_array().unwrap()).as_deref(),
        Some("with basil")
    );

    let no_user = serde_json::json!([{"role": "system", "content": "hi"}]);
    assert_eq!(last_user_text(no_user.as_array().unwrap()), None);
}

/// Only the latest user turn is classified.
#[test]
fn an_earlier_turn_does_not_leak_into_the_verdict() {
    let msgs = serde_json::json!([
        {"role": "user", "content": "I live in Kansas"},
        {"role": "assistant", "content": "noted"},
        {"role": "user", "content": "what is the weather"}
    ]);
    let text = last_user_text(msgs.as_array().unwrap());
    assert_eq!(
        tags_for_request("test", text.as_deref()),
        ["complexity-normal", "pii-none"]
    );
}

/// Every emitted tag must resolve back to the classifier that emitted it,
/// or `requiresClassifiers` would omit a classifier the rule depends on.
#[test]
fn emitted_tags_resolve_to_their_own_classifier() {
    for c in CLASSIFIERS {
        for tag in c.emits {
            assert_eq!(
                classifier_for(tag).map(|f| f.name),
                Some(c.name),
                "{tag} must resolve to {}",
                c.name
            );
        }
        assert!(
            c.emits
                .iter()
                .any(|t| *t == format!("{}{UNKNOWN_SUFFIX}", c.name)),
            "{} must be able to say it does not know",
            c.name
        );
    }
}

/// A prefixed tag no classifier emits still resolves to its owner.
#[test]
fn an_unemittable_prefixed_tag_still_resolves_but_is_not_emittable() {
    assert_eq!(classifier_for("pii-detected").map(|c| c.name), Some("pii"));
    assert!(!is_emittable("pii-detected"));
}

#[test]
fn an_unknown_tag_resolves_to_nothing() {
    assert!(classifier_for("wat-is-this").is_none());
    assert!(!is_emittable("wat-is-this"));
}

#[test]
fn emittable_tags_are_scoped_to_the_named_classifiers() {
    assert_eq!(
        emittable_tags(["pii"]),
        ["pii-confirmed", "pii-none", "pii-unknown"]
    );
    assert_eq!(emittable_tags(["nope"]), Vec::<String>::new());
    assert_eq!(
        emittable_tags(["pii", "complexity"]),
        [
            "complexity-high",
            "complexity-low",
            "complexity-normal",
            "complexity-unknown",
            "pii-confirmed",
            "pii-none",
            "pii-unknown"
        ]
    );
}

/// Every classifier's verdicts are inside its own declared `emits`.
///
/// `emits` (what `compile` checks against) and `detect` (what runs) must
/// agree in both directions. The `-unknown` tag is excluded, since
/// `tags_for_request` emits it.
#[test]
fn every_verdict_is_in_its_own_emits() {
    // Inputs chosen to reach every branch of every classifier, plus text that
    // matches nothing so the default branches are covered too.
    let probes = [
        "basil",
        "oregano",
        "kansas",
        "basil and oregano and kansas",
        "nothing here at all",
        "",
    ];
    for c in CLASSIFIERS {
        let mut seen = std::collections::BTreeSet::new();
        for p in probes {
            let v = (c.detect)(&p.to_lowercase());
            assert!(
                c.emits.contains(&v.tag),
                "classifier `{}` produced `{}`, which is not in its emits {:?}",
                c.name,
                v.tag,
                c.emits
            );
            assert!(
                v.tag.starts_with(c.prefix),
                "classifier `{}` produced `{}`, outside its own prefix `{}`",
                c.name,
                v.tag,
                c.prefix
            );
            seen.insert(v.tag);
        }
        // The other direction: everything declared, except the fold's
        // `-unknown`, is actually reachable.
        let unknown = format!("{}{UNKNOWN_SUFFIX}", c.name);
        for declared in c.emits {
            if *declared == unknown {
                continue;
            }
            assert!(
                seen.contains(declared),
                "classifier `{}` declares `{}` but no probe produced it",
                c.name,
                declared
            );
        }
    }
}
