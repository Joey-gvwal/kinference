use super::*;

/// The wire spelling is the variant name. Pinned because both `as_str` and
/// serde produce it, and a drift between them would make the CLI's JSON and
/// `status.warnings` disagree about what a finding is called.
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

/// `ALL` is hand-maintained, so check it is complete: every variant appears
/// exactly once. A code missing from `ALL` would also be missing from the doc
/// drift check, which is the whole reason `ALL` exists.
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
    // Case is not an edit: `Local` for `local` is the classic slip.
    assert_eq!(did_you_mean("local", ["Local", "Provider"]), Some("Local"));
    // An exact match is not a suggestion.
    assert_eq!(did_you_mean("databricks", names), None);
}

/// Every code is documented. The code is the thing to search for, so one the
/// reference page does not explain is a dead end — for a person reading
/// `status.warnings` and for an agent reading `kinfctl check -o json` alike.
#[test]
fn every_code_is_documented_in_the_policy_reference() {
    let doc = include_str!("../../../docs/content/reference/inferencepolicy.md");
    let missing: Vec<&str> = WarningCode::ALL
        .iter()
        .map(|c| c.as_str())
        .filter(|code| !doc.contains(&format!("| `{code}` |")))
        .collect();
    assert!(
        missing.is_empty(),
        "warning codes with no row in inferencepolicy.md's Warnings tables: {missing:?}"
    );
}

/// The reference says which codes `kinfctl` reports as warnings (`W`) by the
/// table they are in: the name checks, against the attached pools. Everything
/// else is an error (`E`). The table must agree with `is_name_check`.
#[test]
fn each_code_is_in_the_table_for_its_kind() {
    let doc = include_str!("../../../docs/content/reference/inferencepolicy.md");
    let (spec, pools) = doc
        .split_once("#### Against the attached pools")
        .expect("inferencepolicy.md has no name-check table");
    let misplaced: Vec<&str> = WarningCode::ALL
        .iter()
        .filter(|c| {
            let row = format!("| `{}` |", c.as_str());
            pools.contains(&row) != c.is_name_check() || spec.contains(&row) == c.is_name_check()
        })
        .map(|c| c.as_str())
        .collect();
    assert!(
        misplaced.is_empty(),
        "codes in the wrong Warnings table for `is_name_check`: {misplaced:?}"
    );
}
