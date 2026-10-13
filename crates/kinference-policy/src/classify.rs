//! Classifiers, and the tags they put on a request.
//!
//! An `InferencePolicy` matches classifier output through one request attribute:
//! `tag`, an unordered set of opaque strings (`pii-detected`, `ssn`,
//! `complexity-low`), so adding a classifier needs no CRD schema change.
//!
//! The classifiers are keyword stubs: [`tags_for_request`] matches literal
//! words in the most recent user turn, so policies can be exercised against a
//! real tag vocabulary before real classifiers land.
//!
//! A request whose user turn can't be extracted gets every classifier's
//! `-unknown` tag rather than a default verdict, so `{not: pii-confirmed}`
//! doesn't match unscreened traffic.
//!
//! `tag` is untyped in the CRD, so [`CLASSIFIERS`] is what written tags are
//! checked against: each entry declares the prefix it owns and every tag it can
//! emit, and `compile` warns on tags nothing emits.

use std::collections::BTreeSet;

mod complexity;
mod pii;

/// One classifier's answer for one request: exactly one tag, plus the keyword
/// that drove it for the DEBUG line (`None` means the default branch).
pub struct Verdict {
    pub tag: &'static str,
    pub matched: Option<&'static str>,
}

/// One classifier kinference can run, and the tag vocabulary it owns.
///
/// Not `PartialEq`, which would trip the function-pointer-comparison lint.
#[derive(Debug, Clone, Copy)]
pub struct Classifier {
    /// Name as it appears in a rule's `requiresClassifiers`, in the policy API's
    /// rule summaries.
    pub name: &'static str,
    /// Tag prefix this classifier owns. A referenced tag starting with this
    /// belongs to this classifier even when it is not in `emits`.
    pub prefix: &'static str,
    /// Every tag this classifier can currently emit.
    pub emits: &'static [&'static str],
    /// This classifier's rules, in its own module.
    ///
    /// Receives the most recent user turn, lowercased once for all
    /// classifiers. Only called when there is text; [`tags_for_request`]
    /// handles the `-unknown` case. The returned tag must appear in `emits`.
    ///
    /// A `fn` pointer keeps `CLASSIFIERS` a `const`; an async, model-backed
    /// classifier would need this widened.
    detect: fn(&str) -> Verdict,
}

/// Appended to a classifier's name to form the tag it emits when it has no
/// answer.
pub const UNKNOWN_SUFFIX: &str = "-unknown";

pub const CLASSIFIERS: &[Classifier] = &[pii::CLASSIFIER, complexity::CLASSIFIER];

/// The most recent USER turn's text, which is all the classifiers look at.
///
/// `content` may be a string or an array of typed parts (multimodal), whose
/// `text` fields are joined. Returns `None` when there is no user turn; the
/// caller then falls back to a raw `prompt`, and failing that the classifiers
/// report `-unknown`.
pub fn last_user_text(messages: &[serde_json::Value]) -> Option<String> {
    let msg = messages
        .iter()
        .rev()
        .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("user"))?;
    match msg.get("content") {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Array(parts)) => {
            let joined: Vec<&str> = parts
                .iter()
                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                .collect();
            (!joined.is_empty()).then(|| joined.join(" "))
        }
        _ => None,
    }
}

/// Whole-word match, so "kansas" doesn't match "Arkansas". `lowercased` must
/// already be lowercase.
fn contains_word(lowercased: &str, word: &str) -> bool {
    lowercased
        .split(|c: char| !c.is_alphanumeric())
        .any(|w| w == word)
}

/// Tags for one request, sorted.
///
/// `user_text` is the most recent user turn (see [`last_user_text`]). `None`
/// means it could not be determined, and every classifier reports `-unknown`.
///
/// Every decision is logged at DEBUG with the keyword that drove it; `req`
/// correlates the lines with the rest of the pick.
///
/// Keyword rules, all case-insensitive and whole-word:
///
/// | word      | tag                |
/// |-----------|--------------------|
/// | `basil`   | `complexity-high`  |
/// | `oregano` | `complexity-low`   |
/// | neither   | `complexity-normal`|
/// | `kansas`  | `pii-confirmed`    |
/// | absent    | `pii-none`         |
///
/// `basil` wins over `oregano` when both appear.
pub fn tags_for_request(req: &str, user_text: Option<&str>) -> Vec<String> {
    let Some(text) = user_text else {
        // The fail-closed rule, in one place for every classifier.
        let mut tags: Vec<String> = CLASSIFIERS
            .iter()
            .map(|c| format!("{}{}", c.name, UNKNOWN_SUFFIX))
            .collect();
        tags.sort();
        tracing::debug!(
            req,
            ?tags,
            "classify: no user turn to inspect; every classifier reports unknown"
        );
        return tags;
    };
    let lowercased = text.to_lowercase();

    let mut tags: Vec<String> = CLASSIFIERS
        .iter()
        .map(|c| {
            let v = (c.detect)(&lowercased);
            tracing::debug!(
                req,
                classifier = c.name,
                tag = v.tag,
                matched = v.matched.unwrap_or("<default>"),
                "classify: applied tag"
            );
            v.tag.to_string()
        })
        .collect();
    // Callers expect sorted output; `CLASSIFIERS` is not in tag order.
    tags.sort();
    tags
}

/// The classifier responsible for `tag`, by prefix first and emittable-set
/// membership second. `None` means no classifier can produce it.
pub fn classifier_for(tag: &str) -> Option<&'static Classifier> {
    CLASSIFIERS
        .iter()
        .find(|c| tag.starts_with(c.prefix))
        .or_else(|| CLASSIFIERS.iter().find(|c| c.emits.contains(&tag)))
}

/// True when some classifier can actually emit `tag`. A rule matching a tag that
/// fails this can never fire, however well-formed it is.
pub fn is_emittable(tag: &str) -> bool {
    CLASSIFIERS.iter().any(|c| c.emits.contains(&tag))
}

/// Every tag the named classifiers can emit, sorted and deduplicated, for the
/// `UnemittableTag` warning.
pub fn emittable_tags<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let wanted: BTreeSet<&str> = names.into_iter().collect();
    let mut out: BTreeSet<String> = BTreeSet::new();
    for c in CLASSIFIERS.iter().filter(|c| wanted.contains(c.name)) {
        out.extend(c.emits.iter().map(|t| (*t).to_string()));
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests;
