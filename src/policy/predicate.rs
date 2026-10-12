//! The `InferencePolicy` predicate grammar, compiled to a typed AST.
//!
//! The CRD stores every predicate value as
//! `x-kubernetes-preserve-unknown-fields`, because the *shape* of the YAML
//! value is what picks the comparison:
//!
//! | RHS shape | Meaning                                        | Example                        |
//! |-----------|------------------------------------------------|--------------------------------|
//! | scalar    | exact match; CONTAINS for a set-valued attr    | `tag: ssn`, `model: foo`       |
//! | array     | membership — INTERSECTION for set-valued attrs | `qos: [batch, deferrable]`     |
//! | hash      | explicit operator                              | `queuedPrompts: {greaterThan: 8}` |
//!
//! Operators: `not`, `greaterThan`, `lessThan`, `greaterThanOrEqual`,
//! `lessThanOrEqual`, `containsAll`, `exists`. Keys within one predicate are
//! ANDed.
//!
//! **`qos` is the one exception: array-only.** Its CRD schema enumerates the
//! closed domain rather than preserving unknown fields, and a schema with an
//! enum needs a single YAML type — so `qos: [batch]` is the only accepted form
//! and `qos: batch` is rejected by the API server. See [`compile_qos`].
//!
//! Two things here are deliberate:
//!
//! 1. **Compilation happens once per policy revision, never per request.**
//!    Interpreting untyped JSON on the pick hot path would be both slow and
//!    silent about malformed policy. Compiling up front means a bad predicate
//!    is reported once, in `status.warnings`, with a reason an operator can act
//!    on.
//! 2. **The attribute name determines the expected type**, so operator/type
//!    mismatches (`model: {greaterThan: 5}`) are caught at compile time rather
//!    than mysteriously never matching at runtime.
//!
//! ## Absence
//!
//! *"A predicate over a signal the candidate does not carry does not match."*
//! Applied uniformly to both scopes. The single exception is `{exists: false}`,
//! whose entire job is to match absence.
//!
//! `tag` is the one attribute that is ALWAYS present, even when empty — see
//! [`RequestAttrs::get`]. An empty tag set means no classifier answered, which
//! the CRD is explicit is not the same as a clean result, and collapsing the two
//! into "absent" would make `{not: pii-detected}` fail to match a request nobody
//! screened. So classifiers emit an explicit `-unknown` tag instead of nothing;
//! see [`super::classify`].

#![allow(dead_code)]

use std::time::Duration;

use super::crd::{CandidatePredicate, RequestPredicate};

/// A resolved attribute value, from either scope.
#[derive(Debug, Clone, PartialEq)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    /// Stored as milliseconds so duration comparisons unify with numerics.
    DurationMs(f64),
    List(Vec<String>),
}

impl AttrValue {
    /// Numeric view, for the ordering operators. `None` for non-numeric kinds.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            AttrValue::Int(i) => Some(*i as f64),
            AttrValue::Float(f) => Some(*f),
            AttrValue::DurationMs(ms) => Some(*ms),
            _ => None,
        }
    }
}

/// What type an attribute carries. Drives compile-time validation of the
/// operator against the attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrKind {
    Str,
    Int,
    Float,
    Bool,
    /// RHS is a duration string (`5s`, `200ms`); the attribute is millis.
    Duration,
    StrList,
}

impl AttrKind {
    fn is_numeric(self) -> bool {
        matches!(self, AttrKind::Int | AttrKind::Float | AttrKind::Duration)
    }
}

/// Request-scope attributes. One flat namespace, matched by bare name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestAttr {
    Model,
    InputTokens,
    MaxTokens,
    Tenant,
    Qos,
    /// Classifier output, as an unordered set of opaque tags. Which classifier
    /// a rule actually needs comes from the tag VALUES, not from this variant —
    /// see [`CompiledRequestPredicate::required_classifiers`].
    Tag,
}

impl RequestAttr {
    pub fn kind(&self) -> AttrKind {
        match self {
            RequestAttr::InputTokens | RequestAttr::MaxTokens => AttrKind::Int,
            // Both are SETS. `tag` obviously so; `model` because the upstream a
            // request resolved to and the client-facing aliases for it are all
            // names the same request legitimately answers to — see
            // [`RequestAttrs::model`].
            RequestAttr::Tag | RequestAttr::Model => AttrKind::StrList,
            _ => AttrKind::Str,
        }
    }

    /// Name as it appears in YAML, for warnings and traces.
    pub fn name(&self) -> String {
        match self {
            RequestAttr::Model => "model".into(),
            RequestAttr::InputTokens => "inputTokens".into(),
            RequestAttr::MaxTokens => "maxTokens".into(),
            RequestAttr::Tenant => "tenant".into(),
            RequestAttr::Qos => "qos".into(),
            RequestAttr::Tag => "tag".into(),
        }
    }
}

/// Candidate-scope attributes, available in `drop:`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateAttr {
    /// Effective window from THIS endpoint, not the architectural one.
    ContextWindow,
    PredictedTtft,
    PrefixCacheHitRatio,
    /// `num_requests_waiting` on this endpoint. Self-normalizing: nonzero means
    /// the scheduler could not admit.
    QueuedPrompts,
    /// KV-cache utilization, 0.0..1.0, averaged over the scrape window.
    ///
    /// The saturation signal `queuedPrompts` is not: it rises monotonically
    /// with pressure, where the queue can read LOW at high pressure (a fleet
    /// measured at 93% KV / 100 ms ITL showed `waiting = 1`, while a moment at
    /// 31% KV showed `waiting = 5`). Threshold this for headroom; threshold
    /// the queue for admission.
    KvCacheUsage,
    ModelName,
    BackendName,
    /// `Local` or `Provider`. The one candidate signal every candidate carries,
    /// which is what lets a policy scope the rest.
    BackendType,
    /// Local-only.
    BackendReadyEndpoints,
    /// Provider-only. A trailing-window aggregate, so it gates and never ranks.
    BackendObservedTtftP95,
    /// Provider-only, from a `UsageBudget`. No source exists yet; `compile`
    /// warns rather than letting it silently never fire.
    BackendBudgetExhausted,
}

impl CandidateAttr {
    pub fn kind(self) -> AttrKind {
        match self {
            CandidateAttr::ContextWindow | CandidateAttr::BackendReadyEndpoints => AttrKind::Int,
            // FLOAT, not Int: `queuedPrompts` is a mean over the scrape
            // window, so a worker that queued 5 prompts for two seconds out of
            // thirty reads ~0.3. Rounding that to an integer would throw away
            // the whole point of averaging — every non-zero blip would land
            // back on 0 or 1.
            CandidateAttr::QueuedPrompts => AttrKind::Float,
            CandidateAttr::PredictedTtft | CandidateAttr::BackendObservedTtftP95 => {
                AttrKind::Duration
            }
            CandidateAttr::PrefixCacheHitRatio | CandidateAttr::KvCacheUsage => AttrKind::Float,
            CandidateAttr::BackendBudgetExhausted => AttrKind::Bool,
            CandidateAttr::ModelName | CandidateAttr::BackendName | CandidateAttr::BackendType => {
                AttrKind::Str
            }
        }
    }

    /// Which backend class can answer this signal, or `None` when both can.
    ///
    /// Used to warn on a predicate whose signal no candidate in scope carries —
    /// silent by design (absence does not match), and therefore worth saying out
    /// loud at admission.
    pub fn scoped_to(self) -> Option<super::crd::BackendType> {
        use super::crd::BackendType as B;
        match self {
            CandidateAttr::PredictedTtft
            | CandidateAttr::PrefixCacheHitRatio
            | CandidateAttr::QueuedPrompts
            | CandidateAttr::KvCacheUsage
            | CandidateAttr::BackendReadyEndpoints => Some(B::Local),
            CandidateAttr::BackendObservedTtftP95 | CandidateAttr::BackendBudgetExhausted => {
                Some(B::Provider)
            }
            CandidateAttr::ContextWindow
            | CandidateAttr::ModelName
            | CandidateAttr::BackendName
            | CandidateAttr::BackendType => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            CandidateAttr::ContextWindow => "contextWindow",
            CandidateAttr::PredictedTtft => "predictedTTFT",
            CandidateAttr::KvCacheUsage => "kvCacheUsage",
            CandidateAttr::PrefixCacheHitRatio => "prefixCacheHitRatio",
            CandidateAttr::QueuedPrompts => "queuedPrompts",
            CandidateAttr::ModelName => "model.name",
            CandidateAttr::BackendName => "backend.name",
            CandidateAttr::BackendType => "backend.type",
            CandidateAttr::BackendReadyEndpoints => "backend.readyEndpoints",
            CandidateAttr::BackendObservedTtftP95 => "backend.observedTTFTP95",
            CandidateAttr::BackendBudgetExhausted => "backend.budgetExhausted",
        }
    }
}

/// One compiled comparison.
#[derive(Debug, Clone, PartialEq)]
pub enum Matcher {
    Equals(AttrValue),
    /// Membership for a scalar attribute; INTERSECTION for a list-valued one.
    OneOf(Vec<AttrValue>),
    Not(Box<Matcher>),
    GreaterThan(f64),
    LessThan(f64),
    GreaterOrEqual(f64),
    LessOrEqual(f64),
    ContainsAll(Vec<String>),
    Exists(bool),
}

impl Matcher {
    /// Every string this matcher compares against, including under `not`.
    ///
    /// Used to recover the tag VALUES a rule references, which is what selects
    /// the classifiers it needs. `{exists: _}` names no tag and contributes
    /// nothing — a rule asking only whether any tag is present depends on no
    /// particular classifier.
    pub fn string_operands(&self, out: &mut Vec<String>) {
        match self {
            Matcher::Equals(AttrValue::Str(s)) => out.push(s.clone()),
            Matcher::Equals(AttrValue::List(l)) => out.extend(l.iter().cloned()),
            Matcher::OneOf(vs) => {
                for v in vs {
                    if let AttrValue::Str(s) = v {
                        out.push(s.clone());
                    }
                }
            }
            Matcher::ContainsAll(ts) => out.extend(ts.iter().cloned()),
            Matcher::Not(inner) => inner.string_operands(out),
            Matcher::Equals(_)
            | Matcher::GreaterThan(_)
            | Matcher::LessThan(_)
            | Matcher::GreaterOrEqual(_)
            | Matcher::LessOrEqual(_)
            | Matcher::Exists(_) => {}
        }
    }

    /// A matcher that matches nothing — what an invalid term compiles to.
    ///
    /// An empty `OneOf` has no member to equal, so `eval` is false for every
    /// value, present or absent. That makes the entry holding it unsatisfiable,
    /// which is the safe direction everywhere a predicate appears: a `matches`
    /// entry stops matching, an `if` stops applying, a `drop` drops nothing.
    pub fn never() -> Matcher {
        Matcher::OneOf(Vec::new())
    }

    /// Compile a raw YAML/JSON value against the attribute's expected type.
    ///
    /// `Err` carries an operator-facing reason; callers collect these into
    /// `status.warnings` and replace the term with [`Matcher::never`].
    pub fn compile(raw: &serde_json::Value, kind: AttrKind) -> Result<Matcher, String> {
        Matcher::compile_noting(raw, kind, &mut Vec::new())
    }

    /// [`Matcher::compile`], also reporting values that were ACCEPTED after a
    /// lossy conversion — a number given for a string attribute, a fraction
    /// given for an integer one. Those compile, so they are not errors, but the
    /// author wrote something other than what will be compared.
    pub fn compile_noting(
        raw: &serde_json::Value,
        kind: AttrKind,
        notes: &mut Vec<String>,
    ) -> Result<Matcher, String> {
        use serde_json::Value as V;
        match raw {
            // --- hash: explicit operator -------------------------------------
            V::Object(map) => {
                if map.len() != 1 {
                    return Err(format!(
                        "an operator hash takes exactly one key, got {} ({}). \
                         For AND, use separate attributes; for OR, use two entries",
                        map.len(),
                        map.keys().cloned().collect::<Vec<_>>().join(", ")
                    ));
                }
                let (op, arg) = map.iter().next().expect("len checked");
                match op.as_str() {
                    "not" => Ok(Matcher::Not(Box::new(Matcher::compile_noting(
                        arg, kind, notes,
                    )?))),
                    "exists" => arg
                        .as_bool()
                        .map(Matcher::Exists)
                        .ok_or_else(|| "`exists` takes a boolean".to_string()),
                    "containsAll" => {
                        if kind != AttrKind::StrList {
                            return Err(
                                "`containsAll` applies only to list-valued attributes".to_string()
                            );
                        }
                        strings(arg).map(Matcher::ContainsAll)
                    }
                    "greaterThan" | "lessThan" | "greaterThanOrEqual" | "lessThanOrEqual" => {
                        if !kind.is_numeric() {
                            return Err(format!(
                                "`{op}` applies only to numeric or duration attributes"
                            ));
                        }
                        let n = number(arg, kind)?;
                        Ok(match op.as_str() {
                            "greaterThan" => Matcher::GreaterThan(n),
                            "lessThan" => Matcher::LessThan(n),
                            "greaterThanOrEqual" => Matcher::GreaterOrEqual(n),
                            _ => Matcher::LessOrEqual(n),
                        })
                    }
                    other => Err(format!(
                        "unknown operator `{other}`. Valid: not, exists, containsAll, \
                         greaterThan, lessThan, greaterThanOrEqual, lessThanOrEqual"
                    )),
                }
            }
            // --- array: membership / intersection ----------------------------
            V::Array(items) => {
                let mut vals = Vec::with_capacity(items.len());
                for i in items {
                    vals.push(scalar(i, kind, notes)?);
                }
                Ok(Matcher::OneOf(vals))
            }
            // --- scalar: equality -------------------------------------------
            other => Ok(Matcher::Equals(scalar(other, kind, notes)?)),
        }
    }

    /// Evaluate against a resolved value. `None` means the attribute is absent,
    /// which does not match — except for `exists: false`.
    pub fn eval(&self, actual: Option<&AttrValue>) -> bool {
        // `exists` and `not` are the only forms that get a say about absence.
        match self {
            Matcher::Exists(want) => return actual.is_some() == *want,
            Matcher::Not(inner) => {
                // An absent attribute does not match, negated or not — a
                // candidate that carries no `backend.name` is not "a candidate
                // whose backend.name is not X".
                return match actual {
                    None => false,
                    Some(_) => !inner.eval(actual),
                };
            }
            _ => {}
        }
        let Some(actual) = actual else {
            return false;
        };
        match self {
            Matcher::Equals(want) => equals(actual, want),
            Matcher::OneOf(wants) => match actual {
                // Intersection for a list-valued attribute.
                AttrValue::List(have) => wants.iter().any(|w| match w {
                    AttrValue::Str(s) => have.iter().any(|h| h == s),
                    _ => false,
                }),
                _ => wants.iter().any(|w| equals(actual, w)),
            },
            Matcher::ContainsAll(want) => match actual {
                AttrValue::List(have) => want.iter().all(|w| have.contains(w)),
                _ => false,
            },
            Matcher::GreaterThan(n) => actual.as_f64().is_some_and(|a| a > *n),
            Matcher::LessThan(n) => actual.as_f64().is_some_and(|a| a < *n),
            Matcher::GreaterOrEqual(n) => actual.as_f64().is_some_and(|a| a >= *n),
            Matcher::LessOrEqual(n) => actual.as_f64().is_some_and(|a| a <= *n),
            // Handled above.
            Matcher::Exists(_) | Matcher::Not(_) => unreachable!(),
        }
    }
}

/// Type-aware equality. Numbers compare across Int/Float/Duration so
/// `inputTokens: 100` matches whether the value arrived as int or float.
fn equals(actual: &AttrValue, want: &AttrValue) -> bool {
    match (actual, want) {
        (AttrValue::Str(a), AttrValue::Str(b)) => a == b,
        (AttrValue::Bool(a), AttrValue::Bool(b)) => a == b,
        // Scalar against a set means CONTAINS, per the CRD: `tag: pii-detected`
        // asks whether the tag is present, not whether it is the only one.
        (AttrValue::List(a), AttrValue::Str(b)) => a.contains(b),
        _ => match (actual.as_f64(), want.as_f64()) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        },
    }
}

/// Coerce a scalar JSON value to the attribute's type, noting any conversion
/// that changes what will be compared.
fn scalar(
    raw: &serde_json::Value,
    kind: AttrKind,
    notes: &mut Vec<String>,
) -> Result<AttrValue, String> {
    use serde_json::Value as V;
    match kind {
        AttrKind::Str | AttrKind::StrList => match raw {
            V::String(s) => Ok(AttrValue::Str(s.clone())),
            // A bare number/bool against a string attribute is almost always a
            // YAML quoting slip; accept it rather than fail the whole policy —
            // but say so, because `tenant: 007` is compared as "7".
            V::Number(n) => {
                notes.push(format!(
                    "`{n}` is a number but this attribute is a string, so it is compared as \
                     the string \"{n}\" — quote it to say exactly what you mean"
                ));
                Ok(AttrValue::Str(n.to_string()))
            }
            V::Bool(b) => {
                notes.push(format!(
                    "`{b}` is a boolean but this attribute is a string, so it is compared as \
                     the string \"{b}\" — quote it to say exactly what you mean"
                ));
                Ok(AttrValue::Str(b.to_string()))
            }
            _ => Err("expected a string".to_string()),
        },
        AttrKind::Bool => raw
            .as_bool()
            .map(AttrValue::Bool)
            .ok_or_else(|| "expected a boolean".to_string()),
        AttrKind::Int | AttrKind::Float | AttrKind::Duration => {
            number(raw, kind).map(|n| match kind {
                AttrKind::Duration => AttrValue::DurationMs(n),
                AttrKind::Float => AttrValue::Float(n),
                _ => {
                    if n.fract() != 0.0 {
                        notes.push(format!(
                            "`{n}` is not a whole number, but this attribute is an integer, so \
                             the comparison is against {}",
                            n as i64
                        ));
                    }
                    AttrValue::Int(n as i64)
                }
            })
        }
    }
}

/// Numeric RHS. Durations arrive as strings (`5s`); everything else must be a
/// real JSON number — the guide is explicit that thresholds are numbers, not
/// quoted strings.
fn number(raw: &serde_json::Value, kind: AttrKind) -> Result<f64, String> {
    if kind == AttrKind::Duration {
        return match raw {
            serde_json::Value::String(s) => parse_duration_ms(s),
            // A bare number for a duration is ambiguous about units; the CRD
            // spells durations as strings, so say so rather than guess.
            serde_json::Value::Number(_) => Err(
                "a duration takes a string with units (`5s`, `200ms`), not a bare number"
                    .to_string(),
            ),
            _ => Err("expected a duration string".to_string()),
        };
    }
    match raw {
        serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| "number out of range".to_string()),
        serde_json::Value::String(_) => Err(
            "numeric thresholds are numbers, not strings — write `{greaterThan: 0.05}`".to_string(),
        ),
        _ => Err("expected a number".to_string()),
    }
}

fn strings(raw: &serde_json::Value) -> Result<Vec<String>, String> {
    raw.as_array()
        .ok_or_else(|| "expected an array of strings".to_string())?
        .iter()
        .map(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| "expected an array of strings".to_string())
        })
        .collect()
}

/// Parse the CRD's duration form: one or more `<number><unit>` pairs, units
/// `h`/`m`/`s`/`ms` (pattern `^([0-9]{1,5}(h|m|s|ms)){1,4}$`), e.g. `30s`,
/// `1m30s`, `200ms`.
pub fn parse_duration_ms(s: &str) -> Result<f64, String> {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let mut total = 0f64;
    let mut saw_any = false;
    while i < bytes.len() {
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if start == i {
            return Err(format!("invalid duration `{s}`: expected a number"));
        }
        let n: f64 = s[start..i]
            .parse()
            .map_err(|_| format!("invalid duration `{s}`"))?;
        // "ms" must be tested before "m", or `200ms` parses as 200 minutes.
        let mult = if s[i..].starts_with("ms") {
            i += 2;
            1.0
        } else if s[i..].starts_with('s') {
            i += 1;
            1_000.0
        } else if s[i..].starts_with('m') {
            i += 1;
            60_000.0
        } else if s[i..].starts_with('h') {
            i += 1;
            3_600_000.0
        } else {
            return Err(format!(
                "invalid duration `{s}`: expected a unit (h, m, s, ms) after the number"
            ));
        };
        total += n * mult;
        saw_any = true;
    }
    if !saw_any {
        return Err(format!("invalid duration `{s}`"));
    }
    Ok(total)
}

/// The closed QoS domain, matching `PickRequest.QosClass` and `Qos::as_label`.
/// Enumerated in the CRD schema, so the API server rejects a typo — but checked
/// here too, since kinference may be reading a policy admitted by an older schema.
pub // Deliberately excludes `unknown`, which `Qos::as_label` can emit. That value
// exists only on the terminal-report path, for a report whose pick kinference can no
// longer identify; policy matches at PICK time, where `Qos::from_proto` never
// produces it. A rule written `qos: [unknown]` could therefore never fire.
const QOS_VALUES: &[&str] = &["interactive", "batch", "deferrable"];

/// `qos` is the one attribute whose CRD type is not
/// `x-kubernetes-preserve-unknown-fields`: it is an array over a closed enum.
///
/// Enumerating the domain forces a single YAML type, so membership is the only
/// form — `qos: interactive` is rejected by the API server and must be
/// `qos: [interactive]`. Three values have no use for `greaterThan` or a bare
/// scalar, and the trade buys something no other attribute gets: `[bacth]`
/// fails at apply time rather than becoming a rule that never fires.
fn compile_qos(raw: &serde_json::Value) -> Result<Matcher, String> {
    let serde_json::Value::Array(items) = raw else {
        return Err(format!(
            "must be an ARRAY over {QOS_VALUES:?} — write `qos: [interactive]`, not a bare \
             scalar or an operator hash. The closed enum in the schema requires one YAML type."
        ));
    };
    if items.is_empty() {
        return Err("is an empty array, which can never match".to_string());
    }
    let mut vals = Vec::with_capacity(items.len());
    for it in items {
        let s = it
            .as_str()
            .ok_or_else(|| format!("`{it}` is not a string; expected one of {QOS_VALUES:?}"))?;
        if !QOS_VALUES.contains(&s) {
            return Err(format!("unknown value `{s}`. Valid: {QOS_VALUES:?}"));
        }
        vals.push(AttrValue::Str(s.to_string()));
    }
    Ok(Matcher::OneOf(vals))
}

/// A problem with one term, reported before the caller knows where in the
/// manifest the predicate sits — the compiler adds the path and the rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermIssue {
    /// The attribute as spelled in YAML: `inputTokens`, `backend.type`.
    pub attr: String,
    /// `true` when the term failed to compile and was replaced by
    /// [`Matcher::never`]. `false` when it compiled after a lossy conversion.
    pub fatal: bool,
    pub message: String,
}

/// Compile one term, failing CLOSED on error.
///
/// An invalid term becomes [`Matcher::never`] rather than being left out.
/// Leaving it out was the old behaviour, and it failed in the dangerous
/// direction: terms AND, so dropping one WIDENS the predicate. A `drop` of
/// `{backend: {type: Local}, queuedPrompts: {greaterThan: "4"}}` — a quoted
/// number — compiled to `{backend: {type: Local}}` and removed every local
/// candidate, and a `matches` entry whose only term was bad became a
/// catch-all.
fn compile_term<A: Clone>(
    attr: A,
    name: String,
    kind: AttrKind,
    raw: &serde_json::Value,
    terms: &mut Vec<(A, Matcher)>,
    issues: &mut Vec<TermIssue>,
) {
    let mut notes = Vec::new();
    let matcher = match Matcher::compile_noting(raw, kind, &mut notes) {
        Ok(m) => m,
        Err(message) => {
            issues.push(TermIssue {
                attr: name.clone(),
                fatal: true,
                message,
            });
            Matcher::never()
        }
    };
    issues.extend(notes.into_iter().map(|message| TermIssue {
        attr: name.clone(),
        fatal: false,
        message,
    }));
    terms.push((attr, matcher));
}

/// A compiled predicate: `(attribute, matcher)` pairs, all ANDed.
#[derive(Debug, Clone, Default)]
pub struct CompiledRequestPredicate {
    pub terms: Vec<(RequestAttr, Matcher)>,
}

impl CompiledRequestPredicate {
    /// Compile one `matches[]` entry or one `if:` guard.
    ///
    /// A term that fails to compile is reported in `issues` and replaced by
    /// one that never matches, so the whole predicate can never be satisfied —
    /// see [`compile_term`] for why it must not simply be left out.
    pub fn compile(raw: &RequestPredicate, issues: &mut Vec<TermIssue>) -> Self {
        let mut terms = Vec::new();
        let mut push = |attr: RequestAttr, v: &Option<serde_json::Value>| {
            if let Some(raw) = v {
                compile_term(
                    attr.clone(),
                    attr.name(),
                    attr.kind(),
                    raw,
                    &mut terms,
                    issues,
                );
            }
        };
        push(RequestAttr::Model, &raw.model);
        push(RequestAttr::InputTokens, &raw.input_tokens);
        push(RequestAttr::MaxTokens, &raw.max_tokens);
        push(RequestAttr::Tenant, &raw.tenant);
        push(RequestAttr::Tag, &raw.tag);
        // `qos` after the closure's last use, so it can borrow `terms` directly.
        // Term order is irrelevant — they all AND.
        if let Some(v) = &raw.qos {
            match compile_qos(v) {
                Ok(m) => terms.push((RequestAttr::Qos, m)),
                Err(message) => {
                    issues.push(TermIssue {
                        attr: "qos".to_string(),
                        fatal: true,
                        message,
                    });
                    // Fail closed, like every other term: an unsatisfiable term
                    // must make the entry inert, never drop out and widen it.
                    terms.push((RequestAttr::Qos, Matcher::never()));
                }
            }
        }
        Self { terms }
    }

    /// True when every term matches. An empty predicate matches everything —
    /// a rule with no `matches` is the documented catch-all.
    pub fn matches(&self, attrs: &RequestAttrs) -> bool {
        self.terms
            .iter()
            .all(|(a, m)| m.eval(attrs.get(a).as_ref()))
    }

    /// True when this entry matches every request.
    ///
    /// `matches` over zero terms is `all()` over an empty iterator, which is
    /// TRUE — so `matches: [{}]` is a catch-all exactly like omitting `matches`
    /// altogether. Note the deliberate asymmetry with
    /// [`CompiledCandidatePredicate::matches`], which guards the empty case and
    /// returns FALSE: a request predicate with nothing to say admits
    /// everything, a candidate predicate with nothing to say drops nothing.
    pub fn is_unconditional(&self) -> bool {
        self.terms.is_empty()
    }

    /// Every tag VALUE this predicate names, including under `not`.
    pub fn referenced_tags(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (attr, m) in &self.terms {
            if *attr == RequestAttr::Tag {
                m.string_operands(&mut out);
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// Classifiers this predicate depends on, resolved from the tag values it
    /// names — the tag PREFIX is what selects a classifier, so this cannot be
    /// derived from the attribute alone.
    ///
    /// A tag no classifier claims contributes nothing here and is reported
    /// separately by the compiler; it would otherwise show up as a rule
    /// requiring a classifier that does not exist.
    pub fn required_classifiers(&self) -> Vec<&'static str> {
        let mut out: Vec<&'static str> = self
            .referenced_tags()
            .iter()
            .filter_map(|t| super::classify::classifier_for(t).map(|c| c.name))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Tags this predicate names that no classifier can currently emit. A term
    /// over one of these can never be satisfied, so the rule is inert — which is
    /// a different fault from a malformed predicate and is reported as its own
    /// warning.
    pub fn unemittable_tags(&self) -> Vec<String> {
        self.referenced_tags()
            .into_iter()
            .filter(|t| !super::classify::is_emittable(t))
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompiledCandidatePredicate {
    pub terms: Vec<(CandidateAttr, Matcher)>,
}

impl CompiledCandidatePredicate {
    /// Compile one `drop:`. Fails closed exactly like
    /// [`CompiledRequestPredicate::compile`]: an invalid term makes the drop
    /// match nothing, so the exclusion removes nothing.
    pub fn compile(raw: &CandidatePredicate, issues: &mut Vec<TermIssue>) -> Self {
        let mut terms = Vec::new();
        let mut push = |attr: CandidateAttr, v: &Option<serde_json::Value>| {
            if let Some(raw) = v {
                compile_term(
                    attr,
                    attr.name().to_string(),
                    attr.kind(),
                    raw,
                    &mut terms,
                    issues,
                );
            }
        };
        push(CandidateAttr::ContextWindow, &raw.context_window);
        push(CandidateAttr::PredictedTtft, &raw.predicted_ttft);
        push(CandidateAttr::KvCacheUsage, &raw.kv_cache_usage);
        push(
            CandidateAttr::PrefixCacheHitRatio,
            &raw.prefix_cache_hit_ratio,
        );
        push(CandidateAttr::QueuedPrompts, &raw.queued_prompts);
        if let Some(m) = &raw.model {
            push(CandidateAttr::ModelName, &m.name);
        }
        if let Some(b) = &raw.backend {
            push(CandidateAttr::BackendName, &b.name);
            push(CandidateAttr::BackendType, &b.type_);
            push(CandidateAttr::BackendReadyEndpoints, &b.ready_endpoints);
            push(CandidateAttr::BackendObservedTtftP95, &b.observed_ttft_p95);
            push(CandidateAttr::BackendBudgetExhausted, &b.budget_exhausted);
        }
        Self { terms }
    }

    /// True when every term matches — i.e. this exclusion drops the candidate.
    ///
    /// An EMPTY predicate matches nothing. `drop: {}` would otherwise silently
    /// empty the candidate set, and "cannot fail open" cuts both ways: an
    /// exclusion that says nothing should remove nothing.
    pub fn matches(&self, attrs: &CandidateAttrs) -> bool {
        if self.terms.is_empty() {
            return false;
        }
        self.terms
            .iter()
            .all(|(a, m)| m.eval(attrs.get(*a).as_ref()))
    }
}

/// Resolved request-scope attribute values for one request.
#[derive(Debug, Clone, Default)]
pub struct RequestAttrs {
    /// Every name this request answers to: the upstream model it resolved to,
    /// plus each client-facing alias declared for that upstream.
    ///
    /// A SET, because by the time a predicate runs the client's own spelling is
    /// gone — `auto` and declared aliases are both rewritten to a concrete
    /// upstream before scoring. Matching only the upstream meant
    /// `model: qwen-simple` never fired, silently, while the identical name
    /// worked in `route.consider[].models.names[]` (which resolves through the
    /// alias table). Carrying both spellings makes the two agree.
    ///
    /// Ordering is upstream first, then aliases sorted, so traces read
    /// predictably.
    pub model: Vec<String>,
    pub input_tokens: Option<i64>,
    pub max_tokens: Option<i64>,
    /// Mandatory on the wire, so always present. `""` is "undetermined" and
    /// matches only a rule naming the empty string — never a named tenant, and
    /// never every rule.
    pub tenant: String,
    pub qos: Option<String>,
    /// Classifier output. Always populated — every classifier emits at least its
    /// `-unknown` tag — so an EMPTY set means none of them ran, not that they
    /// ran and found nothing. See [`super::classify::tags_for_request`].
    pub tag: Vec<String>,
}

impl RequestAttrs {
    pub fn get(&self, attr: &RequestAttr) -> Option<AttrValue> {
        match attr {
            // Always `Some`, like `tag`: an empty set means the model could not
            // be determined at all, which must not read as "matches anything".
            RequestAttr::Model => Some(AttrValue::List(self.model.clone())),
            RequestAttr::InputTokens => self.input_tokens.map(AttrValue::Int),
            RequestAttr::MaxTokens => self.max_tokens.map(AttrValue::Int),
            // Always `Some`: an empty tenant is the value "", not an absence.
            RequestAttr::Tenant => Some(AttrValue::Str(self.tenant.clone())),
            RequestAttr::Qos => self.qos.clone().map(AttrValue::Str),
            // ALWAYS `Some`, even for an empty set. `Matcher::Not` treats an
            // absent attribute as non-matching, so returning `None` here would
            // make `{not: pii-detected}` silently fail to match a request that
            // no classifier screened — the opposite of fail-closed.
            RequestAttr::Tag => Some(AttrValue::List(self.tag.clone())),
        }
    }
}

/// Resolved candidate-scope attribute values for one candidate.
#[derive(Debug, Clone, Default)]
pub struct CandidateAttrs {
    pub context_window: Option<i64>,
    pub predicted_ttft_ms: Option<f64>,
    pub prefix_cache_hit_ratio: Option<f64>,
    /// Mean queued prompts over the scrape window — fractional by
    /// construction, hence f64. See `MetricsSample::waiting_mean`.
    pub queued_prompts: Option<f64>,
    /// Mean KV-cache utilization over the same window, 0.0..1.0.
    pub kv_cache_usage: Option<f64>,
    pub model_name: Option<String>,
    pub backend_name: Option<String>,
    pub backend_type: Option<String>,
    pub backend_ready_endpoints: Option<i64>,
    pub backend_observed_ttft_ms: Option<f64>,
    pub backend_budget_exhausted: Option<bool>,
}

impl CandidateAttrs {
    pub fn get(&self, attr: CandidateAttr) -> Option<AttrValue> {
        match attr {
            CandidateAttr::ContextWindow => self.context_window.map(AttrValue::Int),
            CandidateAttr::PredictedTtft => self.predicted_ttft_ms.map(AttrValue::DurationMs),
            CandidateAttr::PrefixCacheHitRatio => self.prefix_cache_hit_ratio.map(AttrValue::Float),
            CandidateAttr::QueuedPrompts => self.queued_prompts.map(AttrValue::Float),
            CandidateAttr::KvCacheUsage => self.kv_cache_usage.map(AttrValue::Float),
            CandidateAttr::ModelName => self.model_name.clone().map(AttrValue::Str),
            CandidateAttr::BackendName => self.backend_name.clone().map(AttrValue::Str),
            CandidateAttr::BackendType => self.backend_type.clone().map(AttrValue::Str),
            CandidateAttr::BackendReadyEndpoints => {
                self.backend_ready_endpoints.map(AttrValue::Int)
            }
            CandidateAttr::BackendObservedTtftP95 => {
                self.backend_observed_ttft_ms.map(AttrValue::DurationMs)
            }
            CandidateAttr::BackendBudgetExhausted => {
                self.backend_budget_exhausted.map(AttrValue::Bool)
            }
        }
    }
}

/// `Duration` convenience for callers that hold one.
pub fn duration_to_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests;
