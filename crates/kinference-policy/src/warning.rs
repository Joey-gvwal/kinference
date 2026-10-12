//! Structured findings about an `InferencePolicy`.
//!
//! Every check the compiler and the name lint make reports one of these. Three
//! audiences read them, and each needs a different part:
//!
//! - **An operator** reads `status.warnings`, a `[]string`, so the CRD schema
//!   stays put. Each entry renders through [`Display`](std::fmt::Display) as
//!   `Code: path: message`.
//! - **An agent** reads the JSON from `kinfctl check -o json` or the policy
//!   API, and acts on `code` and `path` rather than parsing prose.
//! - **The docs** list every [`WarningCode`] with how to fix it. A test fails
//!   when a code is missing from `inferencepolicy.md`, because a code nobody
//!   can look up is just a louder string.
//!
//! `path` points into the manifest as written — `spec.rules[2].route.exclude[0]
//! .drop.kvCacheUsage` — so a reader can find the line to edit. Messages still
//! name the rule, since an index is what a tool wants and a name is what a
//! person remembers.

use std::fmt;

/// What a finding is about. The spelling of each variant IS the wire format —
/// it is what `as_str` returns and what serde emits — so renaming one is a
/// breaking change for anyone matching on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
pub enum WarningCode {
    // --- spec checks: decidable from the policy alone -----------------------
    /// A predicate term that cannot be compiled. Fails closed: the entry it
    /// belongs to can never match.
    InvalidTerm,
    /// A `matches` entry or `if:` with no conditions at all.
    EmptyPredicate,
    /// A rule no request can reach, because an earlier one claims everything
    /// it would match and does not fall through.
    ShadowedRule,
    /// A `tag` value no classifier emits, which pins its term true or false.
    UnemittableTag,
    /// A `consider` group whose model choice is contradictory or missing.
    ConsiderModelChoice,
    /// Some `consider` groups are weighted and some are not.
    PartialWeights,
    /// Two rules, or two exclusions or consider groups in one rule, share a
    /// name.
    DuplicateName,
    /// A `drop` with no conditions, which removes nothing.
    EmptyDrop,
    /// A `drop` term over a signal only one backend class carries, combined
    /// with a `backend.type` that does not pin that class.
    DropTermWrongClass,
    /// A signal kinference has no source for, so it is never present.
    NoSignalSource,
    /// A comparison that is constant over the attribute's whole range.
    ThresholdOutOfRange,
    /// A value outside an attribute's closed set, e.g. `backend.type: local`.
    InvalidEnumValue,
    /// A value of the wrong type that was converted rather than rejected.
    CoercedValue,
    /// A `select` tier that sets both or neither of `minimize` and `order`.
    SelectRankAmbiguous,
    /// A `select` tier no candidate can reach.
    UnreachableTier,
    /// A rule with no `select` tiers.
    EmptySelect,
    /// An `order` tier over local endpoints with no random tie-break.
    OrderWithoutRandom,

    // --- name checks: need the pools the policy is attached to -------------
    /// A backend name that matches no destination of the pools.
    UnknownBackend,
    /// A `consider` label selector that matches no destination.
    SelectorMatchesNothing,
    /// A `type:` naming a class the pools have no destination of.
    NoDestinationsOfType,
    /// A model name no destination declares, by name or alias.
    UnknownModel,
}

impl WarningCode {
    /// Every code, in declaration order. The doc-drift test walks this.
    #[cfg(test)]
    pub const ALL: &'static [WarningCode] = &[
        WarningCode::InvalidTerm,
        WarningCode::EmptyPredicate,
        WarningCode::ShadowedRule,
        WarningCode::UnemittableTag,
        WarningCode::ConsiderModelChoice,
        WarningCode::PartialWeights,
        WarningCode::DuplicateName,
        WarningCode::EmptyDrop,
        WarningCode::DropTermWrongClass,
        WarningCode::NoSignalSource,
        WarningCode::ThresholdOutOfRange,
        WarningCode::InvalidEnumValue,
        WarningCode::CoercedValue,
        WarningCode::SelectRankAmbiguous,
        WarningCode::UnreachableTier,
        WarningCode::EmptySelect,
        WarningCode::OrderWithoutRandom,
        WarningCode::UnknownBackend,
        WarningCode::SelectorMatchesNothing,
        WarningCode::NoDestinationsOfType,
        WarningCode::UnknownModel,
    ];

    /// Whether the finding is about the pools the policy serves rather than
    /// the policy itself: a name check. A policy can serve several pools, and a
    /// name one of them lacks may be right for another, so `kinfctl` reports
    /// these as warnings (`W`) and every other code as an error (`E`).
    pub fn is_name_check(self) -> bool {
        matches!(
            self,
            WarningCode::UnknownBackend
                | WarningCode::SelectorMatchesNothing
                | WarningCode::NoDestinationsOfType
                | WarningCode::UnknownModel
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            WarningCode::InvalidTerm => "InvalidTerm",
            WarningCode::EmptyPredicate => "EmptyPredicate",
            WarningCode::ShadowedRule => "ShadowedRule",
            WarningCode::UnemittableTag => "UnemittableTag",
            WarningCode::ConsiderModelChoice => "ConsiderModelChoice",
            WarningCode::PartialWeights => "PartialWeights",
            WarningCode::DuplicateName => "DuplicateName",
            WarningCode::EmptyDrop => "EmptyDrop",
            WarningCode::DropTermWrongClass => "DropTermWrongClass",
            WarningCode::NoSignalSource => "NoSignalSource",
            WarningCode::ThresholdOutOfRange => "ThresholdOutOfRange",
            WarningCode::InvalidEnumValue => "InvalidEnumValue",
            WarningCode::CoercedValue => "CoercedValue",
            WarningCode::SelectRankAmbiguous => "SelectRankAmbiguous",
            WarningCode::UnreachableTier => "UnreachableTier",
            WarningCode::EmptySelect => "EmptySelect",
            WarningCode::OrderWithoutRandom => "OrderWithoutRandom",
            WarningCode::UnknownBackend => "UnknownBackend",
            WarningCode::SelectorMatchesNothing => "SelectorMatchesNothing",
            WarningCode::NoDestinationsOfType => "NoDestinationsOfType",
            WarningCode::UnknownModel => "UnknownModel",
        }
    }
}

impl fmt::Display for WarningCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One finding. Serializes as `{"code", "path", "message"}`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PolicyWarning {
    pub code: WarningCode,
    /// Where in the manifest, e.g. `spec.rules[0].matches[1].inputTokens`.
    pub path: String,
    pub message: String,
}

impl PolicyWarning {
    pub fn new(code: WarningCode, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
            message: message.into(),
        }
    }

    /// Substring test over the rendered form, so a check written against the
    /// prose — `w.contains("never matches")` — reads the same as it did when
    /// warnings were bare strings, and still sees the code and the path.
    #[cfg(test)]
    pub fn contains(&self, needle: &str) -> bool {
        self.to_string().contains(needle)
    }
}

impl fmt::Display for PolicyWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}: {}", self.code, self.path, self.message)
    }
}

/// The closest of `candidates` to `word`, when one is close enough to be a
/// plausible typo. Used for "did you mean" hints on field and backend names.
///
/// Optimal-string-alignment distance, so a transposition (`tennat`) costs one
/// edit rather than two. The cutoff scales with length: one edit for short
/// words, up to a third of the word for long ones — a `databriks` should find
/// `databricks`, while `gpu` should not find `cpu-pool`.
pub fn did_you_mean<'a>(
    word: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    let max = (word.chars().count() / 3).max(1);
    candidates
        .into_iter()
        .filter(|c| *c != word)
        .map(|c| (osa_distance(&word.to_lowercase(), &c.to_lowercase()), c))
        .filter(|(d, _)| *d <= max)
        .min_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)))
        .map(|(_, c)| c)
}

fn osa_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (n, m) = (a.len(), b.len());
    let mut d = vec![vec![0usize; m + 1]; n + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=n {
        for j in 1..=m {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            d[i][j] = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
            }
        }
    }
    d[n][m]
}

#[cfg(test)]
mod tests;
