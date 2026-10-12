//! Policy explanation for the dashboard's interactive explorer.
//!
//! Answers "if a request looked like X, what would policy P do?" — and, unlike
//! the pick path, says WHY at every step.
//!
//! # Why this is not the driver
//!
//! `monitor::policy_driver` owns the real rule walk, and it is entangled with
//! candidate generation (async, tokenizer, radix index), Prometheus recording
//! and `PickAndRecordOutcome`. None of that can run for a hypothetical request.
//!
//! So this module re-sequences the walk from the engine's own public pieces —
//! [`engine::matching_rules`], [`engine::models_to_score`],
//! [`engine::evaluate`], [`engine::falls_through`], [`engine::unmatched_route`]
//! — which is the same set the driver calls. The DECISION therefore cannot
//! diverge from production: only the sequencing could, and `tests.rs` pins that
//! against a direct `evaluate()` call.
//!
//! # What it adds
//!
//! Three things the hot path deliberately does not compute, because they cost
//! work per pick and nobody reads them at 1000 rps:
//!
//! 1. **Per-predicate detail.** [`crate::predicate::CompiledRequestPredicate::matches`]
//!    returns a bare `bool`. Here each term is evaluated separately via
//!    `Matcher::eval`, so a rule that did not match can say which attribute
//!    failed. Additive — nothing in `predicate.rs` changes.
//! 2. **The `consider` stage.** Model/backend filtering happens during
//!    candidate GENERATION in the driver, not inside `evaluate`, so a trace
//!    never shows it. With a caller-supplied candidate set it has to be applied
//!    here or `consider` would appear to do nothing.
//! 3. **Select-tier subset misses.** `evaluate` skips a candidate outside a
//!    tier's `backends` with a bare `continue` and no trace entry, so
//!    "this provider was not in tier 0 because the tier is `type: Local`" is
//!    unexplainable from `Trace` alone. Recomputed here.
//!
//! # Nondeterminism
//!
//! `evaluate` is pure except for two `getrandom` calls — `tieBreak: Random`
//! with a live tie, and weighted `consider` sampling. A policy using either
//! gives a different answer between runs, so [`nondeterminism`] names the
//! reasons and the caller can offer to run the simulation repeatedly instead of
//! presenting one roll as the answer.

use super::compile::{CompiledPolicy, CompiledRoute, Rank};
use super::crd::TieBreak;
use super::engine::{self, Candidate, Outcome, ScoreTarget, Trace};
use super::predicate::{Matcher, RequestAttr, RequestAttrs};

/// One term of a rule's `matches`, evaluated on its own.
#[derive(Debug, Clone)]
pub struct TermOutcome {
    pub attr: String,
    pub matched: bool,
}

/// One `matches[]` entry. Entries OR; terms within an entry AND.
#[derive(Debug, Clone)]
pub struct PredicateOutcome {
    pub matched: bool,
    pub terms: Vec<TermOutcome>,
}

/// What `consider` admitted, for one rule.
#[derive(Debug, Clone)]
pub struct ConsiderStage {
    /// `(model, backend subset description)` per resolved target.
    pub targets: Vec<(String, String)>,
    /// Whether the groups were sampled rather than unioned. When true the
    /// targets shown are ONE roll, not the whole set.
    pub weighted: bool,
    pub admitted: Vec<String>,
    /// `(candidate, why it was not admitted)`.
    pub rejected: Vec<(String, String)>,
}

/// One rule, as far as evaluation got with it.
#[derive(Debug, Clone)]
pub struct RuleStep {
    /// Position in `spec.rules`, which `Trace` does not carry.
    pub index: usize,
    pub name: String,
    pub matched: bool,
    /// Empty `matches` is the documented catch-all, and reports as matched with
    /// no predicates rather than as an absence.
    pub predicates: Vec<PredicateOutcome>,
    /// `None` when the rule did not match, so was never evaluated.
    pub consider: Option<ConsiderStage>,
    pub trace: Option<Trace>,
    /// `route.select[tier].rank`, rendered. `Trace` records only the index.
    pub tier_rank: Option<String>,
    /// Tier subset misses, which `evaluate` drops silently.
    pub tier_subset_drops: Vec<(String, String)>,
    pub winner: Option<String>,
    /// Nothing survived and `onEmpty: FallThrough`, so the next rule was tried.
    pub fell_through: bool,
}

/// The terminal state, using the same vocabulary as
/// `kinference_policy_decisions_total{outcome}` so the explorer and the metric agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Chosen,
    UnmatchedServed,
    UnmatchedEmpty,
    RejectedOnEmpty,
    RejectedUnmatched,
}

impl Verdict {
    pub fn as_label(self) -> &'static str {
        match self {
            Verdict::Chosen => "chosen",
            Verdict::UnmatchedServed => "unmatched_served",
            Verdict::UnmatchedEmpty => "unmatched_empty",
            Verdict::RejectedOnEmpty => "rejected_on_empty",
            Verdict::RejectedUnmatched => "rejected_unmatched",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Explanation {
    pub steps: Vec<RuleStep>,
    pub verdict: Verdict,
    /// `Candidate::label()` of the winner, when there is one.
    pub winner: Option<String>,
    /// Why repeating this simulation may give a different answer. Empty means
    /// the result is reproducible.
    pub nondeterminism: Vec<Nondeterminism>,
}

/// One reason a policy may answer differently between runs. Serializes as
/// `{"code", "rule", "message"}`; `code` is one of `RandomTieBreak`,
/// `WeightedConsider` or `UnmatchedRandomTieBreak`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Nondeterminism {
    pub code: &'static str,
    /// The rule responsible; `None` for the built-in unmatched route.
    pub rule: Option<String>,
    pub message: String,
}

#[cfg(test)]
impl Nondeterminism {
    pub fn contains(&self, needle: &str) -> bool {
        self.message.contains(needle)
    }
}

/// Reasons this policy will not give the same answer twice.
///
/// Both come from `getrandom` inside the engine. Reported per policy rather
/// than per run so the UI can warn BEFORE the user reads one roll as the
/// answer.
pub fn nondeterminism(policy: &CompiledPolicy) -> Vec<Nondeterminism> {
    let mut out = Vec::new();
    for (i, rule) in policy.rules.iter().enumerate() {
        if rule.route.tie_break.contains(&TieBreak::Random) && !rule.route.select.is_empty() {
            out.push(Nondeterminism {
                code: "RandomTieBreak",
                rule: Some(rule.name.clone()),
                message: format!(
                    "rule {i} `{}` uses `tieBreak: Random`, so a tie is broken by a coin flip",
                    rule.name
                ),
            });
        }
        if rule.route.weighted() {
            out.push(Nondeterminism {
                code: "WeightedConsider",
                rule: Some(rule.name.clone()),
                message: format!(
                    "rule {i} `{}` has weighted `consider` groups, so ONE group is sampled per \
                     request",
                    rule.name
                ),
            });
        }
    }
    // The route no rule wrote. `default_route()` breaks ties at random, and
    // `ServeLocal` is the default, so nearly every policy has this source —
    // and missing it meant the explorer called a policy reproducible while
    // two idle, equally fast endpoints traded wins between runs.
    if policy.unmatched == crate::crd::Unmatched::ServeLocal {
        out.push(Nondeterminism {
            code: "UnmatchedRandomTieBreak",
            rule: None,
            message: "requests no rule matches are served by the built-in route \
                      (`unmatched: ServeLocal`), which breaks ties between equally fast local \
                      endpoints by a coin flip"
                .to_string(),
        });
    }
    out
}

fn render_rank(rank: &Rank) -> String {
    match rank {
        Rank::MinimizePredictedTtft => "minimize PredictedTTFT".to_string(),
        Rank::MinimizeEstimatedTtft => "minimize EstimatedTTFT".to_string(),
        Rank::Order(names) => format!("order [{}]", names.join(", ")),
    }
}

fn render_backends(m: &super::compile::BackendMatch) -> String {
    let mut parts = Vec::new();
    if let Some(t) = m.backend_type {
        parts.push(format!("type={}", t.as_str()));
    }
    if !m.names.is_empty() {
        parts.push(format!("names=[{}]", m.names.join(", ")));
    }
    if !m.match_labels.is_empty() {
        let l: Vec<String> = m
            .match_labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        parts.push(format!("labels={{{}}}", l.join(", ")));
    }
    if parts.is_empty() {
        "any".to_string()
    } else {
        parts.join(" ")
    }
}

/// Per-term detail for one rule's `matches`.
fn predicates_for(
    rule: &super::compile::CompiledRule,
    attrs: &RequestAttrs,
) -> Vec<PredicateOutcome> {
    rule.matches
        .iter()
        .map(|p| {
            let terms: Vec<TermOutcome> = p
                .terms
                .iter()
                .map(|(attr, m): &(RequestAttr, Matcher)| TermOutcome {
                    attr: attr.name(),
                    matched: m.eval(attrs.get(attr).as_ref()),
                })
                .collect();
            PredicateOutcome {
                // Recomputed from the terms rather than calling `p.matches`,
                // so the summary can never disagree with the detail beneath it.
                matched: terms.iter().all(|t| t.matched),
                terms,
            }
        })
        .collect()
}

/// Apply `consider` to a caller-supplied candidate set.
fn apply_consider(
    route: &CompiledRoute,
    requested_models: &[String],
    candidates: &[Candidate],
) -> (ConsiderStage, Vec<Candidate>) {
    let targets: Vec<ScoreTarget> = engine::models_to_score(route, requested_models);
    let mut admitted = Vec::new();
    let mut rejected = Vec::new();
    let mut kept = Vec::new();

    for c in candidates {
        let model_ok = targets.iter().any(|t| t.model == c.canonical_model);
        let hit = targets.iter().any(|t| {
            t.model == c.canonical_model
                && t.backends.matches(c.backend_type(), &c.backend, &c.labels)
        });
        if hit {
            admitted.push(c.label());
            kept.push(c.clone());
        } else if model_ok {
            rejected.push((
                c.label(),
                "not in any `consider` backend subset".to_string(),
            ));
        } else {
            rejected.push((
                c.label(),
                format!("model `{}` is not in `consider`", c.canonical_model),
            ));
        }
    }

    (
        ConsiderStage {
            targets: targets
                .iter()
                .map(|t| (t.model.clone(), render_backends(&t.backends)))
                .collect(),
            weighted: route.weighted(),
            admitted,
            rejected,
        },
        kept,
    )
}

/// Tier subset misses, which `evaluate` skips without a trace entry.
///
/// Only the survivors matter: a candidate already eliminated by an exclusion
/// was never offered to a tier, and reporting it here would double-count it.
fn tier_subset_drops(
    route: &CompiledRoute,
    candidates: &[Candidate],
    survivors: &[String],
) -> Vec<(String, String)> {
    // A set, not `Vec::contains`: this runs per candidate per tier per run,
    // and a simulation may repeat 500 times.
    let alive: std::collections::HashSet<&str> = survivors.iter().map(String::as_str).collect();
    let mut out = Vec::new();
    for (i, tier) in route.select.iter().enumerate() {
        for c in candidates {
            let label = c.label();
            if !alive.contains(label.as_str()) {
                continue;
            }
            if !tier
                .backends
                .matches(c.backend_type(), &c.backend, &c.labels)
            {
                out.push((
                    label,
                    format!(
                        "outside select tier {i}'s subset ({})",
                        render_backends(&tier.backends)
                    ),
                ));
            }
        }
    }
    out
}

/// Run one route and fold the result into a step.
fn run_route(
    index: usize,
    name: &str,
    rule_name: Option<&str>,
    route: &CompiledRoute,
    attrs: &RequestAttrs,
    requested_models: &[String],
    candidates: &[Candidate],
) -> (RuleStep, Option<Candidate>, bool) {
    let (consider, kept) = apply_consider(route, requested_models, candidates);
    let out = engine::evaluate(route, rule_name, attrs, kept.clone());

    let (trace, winner) = match out {
        Outcome::Chosen { candidate, trace } => (trace, Some(candidate)),
        Outcome::Empty { trace } => (trace, None),
    };
    let tier_rank = trace
        .tier
        .and_then(|t| route.select.get(t))
        .map(|t| render_rank(&t.rank));
    let drops = tier_subset_drops(route, &kept, &trace.survivors);
    let empty = winner.is_none();

    (
        RuleStep {
            index,
            name: name.to_string(),
            matched: true,
            predicates: Vec::new(),
            consider: Some(consider),
            winner: winner.as_ref().map(|c| c.label()),
            trace: Some(trace),
            tier_rank,
            tier_subset_drops: drops,
            fell_through: empty && engine::falls_through(route),
        },
        winner,
        empty,
    )
}

/// Explain what `policy` would do with `attrs` over `candidates`.
///
/// `consider` is resolved against the whole of `attrs.model` — the upstream
/// plus every declared alias — so each destination can resolve through its own
/// alias table. See [`engine::models_to_score`].
pub fn explain(
    policy: &CompiledPolicy,
    attrs: &RequestAttrs,
    candidates: &[Candidate],
) -> Explanation {
    let requested_models = attrs.model.clone();
    let mut steps: Vec<RuleStep> = Vec::new();

    // Every rule is reported, matched or not — a rule that never fires is the
    // thing an operator most needs to see, and `matching_rules` alone would
    // hide it.
    let matched: Vec<usize> = policy
        .rules
        .iter()
        .enumerate()
        .filter(|(_, r)| r.matches(attrs))
        .map(|(i, _)| i)
        .collect();

    for (i, rule) in policy.rules.iter().enumerate() {
        if !matched.contains(&i) {
            steps.push(RuleStep {
                index: i,
                name: rule.name.clone(),
                matched: false,
                predicates: predicates_for(rule, attrs),
                consider: None,
                trace: None,
                tier_rank: None,
                tier_subset_drops: Vec::new(),
                winner: None,
                fell_through: false,
            });
        }
    }

    // Then the matching ones, in list order, until one yields a candidate.
    for i in &matched {
        let rule = &policy.rules[*i];
        let (mut step, winner, empty) = run_route(
            *i,
            &rule.name,
            Some(&rule.name),
            &rule.route,
            attrs,
            &requested_models,
            candidates,
        );
        step.predicates = predicates_for(rule, attrs);
        let fell = step.fell_through;
        steps.push(step);

        if let Some(w) = winner {
            steps.sort_by_key(|s| s.index);
            return Explanation {
                steps,
                verdict: Verdict::Chosen,
                winner: Some(w.label()),
                nondeterminism: nondeterminism(policy),
            };
        }
        if empty && !fell {
            steps.sort_by_key(|s| s.index);
            return Explanation {
                steps,
                verdict: Verdict::RejectedOnEmpty,
                winner: None,
                nondeterminism: nondeterminism(policy),
            };
        }
    }

    // No rule matched, or every matching rule fell through: `spec.unmatched`.
    steps.sort_by_key(|s| s.index);
    match engine::unmatched_route(policy) {
        Err(_) => Explanation {
            steps,
            verdict: Verdict::RejectedUnmatched,
            winner: None,
            nondeterminism: nondeterminism(policy),
        },
        Ok(route) => {
            let (mut step, winner, _) = run_route(
                usize::MAX,
                "<unmatched>",
                None,
                &route,
                attrs,
                &requested_models,
                candidates,
            );
            step.matched = true;
            steps.push(step);
            let verdict = if winner.is_some() {
                Verdict::UnmatchedServed
            } else {
                Verdict::UnmatchedEmpty
            };
            Explanation {
                steps,
                verdict,
                winner: winner.as_ref().map(|c| c.label()),
                nondeterminism: nondeterminism(policy),
            }
        }
    }
}

#[cfg(test)]
mod tests;
