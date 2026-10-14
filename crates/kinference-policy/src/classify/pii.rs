//! The PII classifier.
//!
//! A keyword stub that looks for one word, until a real detector replaces
//! [`detect`].

use super::{Classifier, Verdict};

pub(super) const CLASSIFIER: Classifier = Classifier {
    name: "pii",
    prefix: "pii-",
    // `pii-unknown` is emitted by `tags_for_request` when there is no user
    // turn, so a policy rejecting unscreened traffic should match it too.
    emits: &["pii-confirmed", "pii-none", "pii-unknown"],
    detect,
};

/// Whole-word `kansas` means confirmed; anything else means none.
///
/// Never called without text; see [`super::tags_for_request`].
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
