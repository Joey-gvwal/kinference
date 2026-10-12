//! Classifiers, and the tags they put on a request.
//!
//! An `InferencePolicy` matches classifier output through one request attribute:
//! `tag`, an unordered set of opaque strings (`pii-detected`, `ssn`,
//! `complexity-low`). One vehicle rather than a field per signal, so adding a
//! classifier is a new tag rather than a CRD schema change.
//!
//! # THESE CLASSIFIERS ARE KEYWORD STUBS
//!
//! [`tags_for_request`] matches literal words in the most recent USER turn.
//! There is no model, no scoring, and no detection worth the name — it exists
//! so policies can be authored and exercised against a real tag vocabulary,
//! with outcomes an operator can trigger on demand, before real classifiers
//! land. Swapping in a real one changes this function and nothing else.
//!
//! A request whose user turn cannot be extracted at all gets every
//! classifier's `-unknown` tag rather than a default verdict. That is the
//! fail-closed contract the CRD describes: absence is not proof of a negative,
//! so a detector that cannot answer says so explicitly instead of emitting
//! nothing and letting `{not: pii-confirmed}` take the permissive branch.
//!
//! # Why a registry rather than parsing prefixes
//!
//! `tag` is untyped in the CRD — no enum, so no value validation, and a typo
//! matches nothing silently. [`CLASSIFIERS`] is what a written tag gets checked
//! against: each entry declares the prefix it owns and every tag it can emit,
//! which is what the `UnemittableTag` warning lists, and what lets a rule
//! referencing an unemittable tag be reported instead of quietly going inert.
//!
//! A prefix alone would not be enough. `pii-detected` resolves by prefix, but
//! the CRD's own vocabulary includes bare tags like `ssn` and `code` that belong
//! to a classifier without naming it — those resolve by emittable-set membership.

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
/// Not `PartialEq`/`Eq`: nothing compares these, and deriving them over a `fn`
/// pointer field invites rustc's function-pointer-comparison lint for no gain.
#[derive(Debug, Clone, Copy)]
pub struct Classifier {
    /// Name as it appears in a rule's `requiresClassifiers`, in the policy API's
    /// rule summaries.
    pub name: &'static str,
    /// Tag prefix this classifier owns. A referenced tag starting with this
    /// belongs to this classifier even when it is not in `emits` — that is what
    /// makes prefixes load-bearing rather than cosmetic.
    pub prefix: &'static str,
    /// Every tag this classifier is currently capable of emitting.
    ///
    /// The honest set, not the aspirational one. A policy matching anything
    /// outside it is inert, and saying so is the whole point of publishing this.
    pub emits: &'static [&'static str],
    /// This classifier's rules, in its own module.
    ///
    /// Receives the most recent user turn, LOWERCASED once for the whole
    /// table so N classifiers do not each pay for their own copy. A classifier
    /// that needs the verbatim text (casing is signal for a real detector)
    /// is the point at which this widens to carry both.
    ///
    /// Called ONLY when there is text to inspect: the `-unknown` case is
    /// handled once by [`tags_for_request`], so an individual classifier
    /// cannot forget to fail closed. The returned tag MUST appear in `emits` —
    /// guarded by `every_verdict_is_in_its_own_emits`.
    ///
    /// A `fn` pointer rather than a trait keeps `CLASSIFIERS` a `const` and
    /// costs no dispatch machinery. It cannot express an `async fn`, so a
    /// model-backed classifier will need this field widened to an enum holding
    /// its client — a change confined to this file, since the per-classifier
    /// modules only export `detect` and their metadata.
    detect: fn(&str) -> Verdict,
}

/// The tag every classifier emits when it has no answer. Appending this suffix
/// to a classifier's name is the convention a restrictive policy relies on.
pub const UNKNOWN_SUFFIX: &str = "-unknown";

pub const CLASSIFIERS: &[Classifier] = &[pii::CLASSIFIER, complexity::CLASSIFIER];

/// The most recent USER turn's text, which is all the classifiers look at.
///
/// Chat bodies carry `content` either as a plain string or as an array of typed
/// parts (multimodal); both are handled, and the parts' `text` fields are joined
/// so an image-plus-text turn still classifies on its words. Returns `None` when
/// there is no user turn at all — the caller falls back to a raw `prompt`, and
/// failing that the classifiers report `-unknown`.
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

/// Whole-word, case-insensitive match.
///
/// Substring matching would be shorter and wrong: "Kansas" is a substring of
/// "Arkansas", so `text.contains("kansas")` flags every request mentioning a
/// neighbouring state as confirmed PII. A keyword stub is still a classifier,
/// and a false positive here routes real traffic somewhere it should not go.
fn contains_word(lowercased: &str, word: &str) -> bool {
    lowercased
        .split(|c: char| !c.is_alphanumeric())
        .any(|w| w == word)
}

/// Tags for one request, sorted.
///
/// `user_text` is the most recent user turn — see [`last_user_text`]. `None`
/// means it could not be determined, and every classifier reports `-unknown`
/// rather than guessing.
///
/// Every decision is logged at DEBUG with the keyword that drove it, because
/// `tag` is untyped and a policy that silently fails to match gives an operator
/// nothing to look at otherwise. `req` correlates the lines with the rest of the
/// pick, which matters as soon as two requests are in flight.
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
/// `basil` wins over `oregano` when both appear — a request carrying a signal
/// for "hard" is hard, whatever else it also says.
pub fn tags_for_request(req: &str, user_text: Option<&str>) -> Vec<String> {
    let Some(text) = user_text else {
        // THE fail-closed rule, in one place, for every classifier in the
        // table — so adding a classifier cannot omit it.
        let mut tags: Vec<String> = CLASSIFIERS
            .iter()
            .map(|c| format!("{}{}", c.name, UNKNOWN_SUFFIX))
            .collect();
        tags.sort();
        // The fail-closed path, and the one most worth seeing: a policy written
        // to reject unscreened traffic fires here, and nothing else says why.
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
    // Load-bearing, not cosmetic: CLASSIFIERS is declared [pii, complexity], so
    // the raw fold is pii-first while every caller and test expects sorted
    // output. Do not "fix" this by reordering the table.
    tags.sort();
    tags
}

/// The classifier responsible for `tag`, by prefix first and emittable-set
/// membership second. `None` means no classifier can produce it — a typo, or a
/// signal kinference does not implement.
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

/// Every tag the named classifiers can emit, sorted and deduplicated. Feeds
/// the "Emittable today" list in the `UnemittableTag` warning, whose job is to
/// let a reviewer check a written tag against reality.
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
