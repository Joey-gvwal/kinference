use super::*;

/// The wire spelling is the variant name, from both `as_str` and serde.
#[test]
fn every_code_serializes_as_its_as_str() {
    for code in WarningCode::ALL {
        assert_eq!(
            serde_json::to_value(code).unwrap(),
            serde_json::json!(code.as_str()),
            "{code:?}"
        );
    }
}

/// `ALL` is hand-maintained, so check every variant appears exactly once.
#[test]
fn all_lists_each_code_once() {
    let mut seen = std::collections::BTreeSet::new();
    for code in WarningCode::ALL {
        assert!(seen.insert(*code), "{code:?} listed twice");
    }
    // Exhaustive match: adding a variant without listing it here fails to
    // compile, which forces a look at `ALL` too.
    for code in WarningCode::ALL {
        match code {
            WarningCode::InvalidTerm
            | WarningCode::EmptyPredicate
            | WarningCode::ShadowedRule
            | WarningCode::UnemittableTag
            | WarningCode::ConsiderModelChoice
            | WarningCode::PartialWeights
            | WarningCode::DuplicateName
            | WarningCode::EmptyDrop
            | WarningCode::DropTermWrongClass
            | WarningCode::NoSignalSource
            | WarningCode::ThresholdOutOfRange
            | WarningCode::InvalidEnumValue
            | WarningCode::CoercedValue
            | WarningCode::SelectRankAmbiguous
            | WarningCode::UnreachableTier
            | WarningCode::EmptySelect
            | WarningCode::OrderWithoutRandom
            | WarningCode::UnknownBackend
            | WarningCode::SelectorMatchesNothing
            | WarningCode::NoDestinationsOfType
            | WarningCode::UnknownModel => {}
        }
    }
    assert_eq!(seen.len(), 21, "update this count with the match above");
}

#[test]
fn renders_code_path_and_message() {
    let w = PolicyWarning::new(WarningCode::EmptyDrop, "spec.rules[0]", "removes nothing");
    assert_eq!(w.to_string(), "EmptyDrop: spec.rules[0]: removes nothing");
    assert!(w.contains("EmptyDrop"));
    assert!(w.contains("removes nothing"));
}

#[test]
fn did_you_mean_finds_typos_and_ignores_strangers() {
    let names = ["databricks", "gemma-4-12b", "qwen-2-5-32b"];
    assert_eq!(did_you_mean("databriks", names), Some("databricks"));
    assert_eq!(
        did_you_mean("qwen-2-5-23b", names),
        Some("qwen-2-5-32b"),
        "transposition"
    );
    assert_eq!(did_you_mean("together", names), None);
    // Case differences are not edits.
    assert_eq!(did_you_mean("local", ["Local", "Provider"]), Some("Local"));
    // An exact match is not a suggestion.
    assert_eq!(did_you_mean("databricks", names), None);
}
