//! The complexity classifier.
//!
//! # THIS IS A KEYWORD STUB
//!
//! It looks for two words. It is here so the tag vocabulary, the policy
//! grammar and the fail-closed contract can be exercised end to end before a
//! real estimator lands; replacing [`detect`] is the whole job, and nothing
//! outside this file needs to change when it happens.

use super::{Classifier, Verdict};

pub(super) const CLASSIFIER: Classifier = Classifier {
    name: "complexity",
    prefix: "complexity-",
    // Fail-closed: emits complexity-unknown when no user turn can be
    // extracted; otherwise always one of low/normal/high. The unknown case is
    // handled by the fold in `tags_for_request`, not here.
    emits: &[
        "complexity-high",
        "complexity-low",
        "complexity-normal",
        "complexity-unknown",
    ],
    detect,
};

/// `basil` → high, `oregano` → low, neither → normal.
///
/// `basil` wins when both appear: a request carrying a signal for "hard" is
/// hard, whatever else it also says. The precedence lives here rather than in
/// the dispatcher because it is a property of this classifier's rules.
///
/// Never called without text — see [`super::tags_for_request`].
fn detect(lowercased: &str) -> Verdict {
    if super::contains_word(lowercased, "basil") {
        Verdict {
            tag: "complexity-high",
            matched: Some("basil"),
        }
    } else if super::contains_word(lowercased, "oregano") {
        Verdict {
            tag: "complexity-low",
            matched: Some("oregano"),
        }
    } else {
        Verdict {
            tag: "complexity-normal",
            matched: None,
        }
    }
}
