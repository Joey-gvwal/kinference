//! The complexity classifier.
//!
//! A keyword stub that looks for two words, until a real estimator replaces
//! [`detect`].

use super::{Classifier, Verdict};

pub(super) const CLASSIFIER: Classifier = Classifier {
    name: "complexity",
    prefix: "complexity-",
    // `complexity-unknown` is emitted by `tags_for_request` when there is no
    // user turn.
    emits: &[
        "complexity-high",
        "complexity-low",
        "complexity-normal",
        "complexity-unknown",
    ],
    detect,
};

/// `basil` means high, `oregano` low, neither normal. `basil` wins when both
/// appear.
///
/// Never called without text; see [`super::tags_for_request`].
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
