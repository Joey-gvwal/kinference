//! The PII classifier.
//!
//! # THIS IS A KEYWORD STUB
//!
//! It looks for one word. It is here so the tag vocabulary, the policy
//! grammar and the fail-closed contract can be exercised end to end before a
//! real detector lands; replacing [`detect`] is the whole job, and nothing
//! outside this file needs to change when it happens.

use super::{Classifier, Verdict};

pub(super) const CLASSIFIER: Classifier = Classifier {
    name: "pii",
    prefix: "pii-",
    // Fail-closed: emits pii-unknown when no user turn can be extracted, so a
    // policy that must not admit unscreened traffic matches pii-unknown as
    // well as pii-confirmed. That case is handled by the fold in
    // `tags_for_request`, not here, so this classifier cannot forget it.
    // Stated for a reader of this table; the operator-facing copy is in the
    // InferencePolicy reference docs, since nothing in the router reads a
    // failure mode.
    emits: &["pii-confirmed", "pii-none", "pii-unknown"],
    detect,
};

/// Whole-word `kansas` means confirmed; anything else means none.
///
/// Never called without text — see [`super::tags_for_request`].
fn detect(lowercased: &str) -> Verdict {
    if super::contains_word(lowercased, "kansas") {
        Verdict {
            tag: "pii-confirmed",
            matched: Some("kansas"),
        }
    } else {
        Verdict {
            tag: "pii-none",
            matched: None,
        }
    }
}
