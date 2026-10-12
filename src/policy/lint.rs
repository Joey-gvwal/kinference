//! Name checks: does a policy name things that exist in the pools it serves?
//!
//! The compiler judges a policy by its spec alone. These checks need the other
//! side of every name a policy uses — the destinations of the pools it is
//! attached to — so they run where those are known: on the server, for the
//! pools whose `policyRef` names the policy, and in `kinfctl check`, for pools
//! among its inputs. Both pass `ResolvedPool`s built by the same
//! [`super::store::resolve`], so the CLI cannot disagree with the cluster about
//! what a name refers to.
//!
//! Only DECLARED names are consulted — object names, labels, worker-set
//! `spec.models` rows, provider model tables — never what an engine reports
//! right now. That keeps the results stable enough for `status.warnings`: they
//! change when someone edits an object, not when a pod restarts.

use std::collections::{BTreeMap, BTreeSet};

use super::crd::{BackendType, InferencePolicySpec};
use super::predicate::{AttrKind, Matcher};
use super::store::{ResolvedDestination, ResolvedPool};
use super::warning::{PolicyWarning, WarningCode, did_you_mean};

/// The result of checking one policy's names against a set of pools.
#[derive(Debug, Default)]
pub struct NameCheck {
    pub warnings: Vec<PolicyWarning>,
    /// Why model names were not checked, when they were not.
    ///
    /// A worker set's `spec.models` rows ADD names — they never hide the other
    /// models its engine reports — so a set with no rows can serve any name at
    /// all, and nothing about a model name can be concluded. Reported rather
    /// than silently skipped, so "no `UnknownModel` warnings" is never mistaken
    /// for "every model name checked out".
    pub models_unchecked: Option<String>,
}

/// Check every backend, label, class and model name in `spec` against the
/// union of `pools`' destinations. No pools, no findings: a policy nothing
/// references has nothing to be checked against.
pub fn reference_warnings(spec: &InferencePolicySpec, pools: &[&ResolvedPool]) -> NameCheck {
    check(spec, pools, &[], true)
}

/// What a manifest check knows that a running kinference does not need to be
/// told.
///
/// In a cluster, a destination that did not resolve does not exist, and
/// nothing can route to it. Offline it has usually just not been passed, so
/// the names a pool lists stand in for the objects behind them.
#[derive(Debug, Default)]
pub struct Offline {
    /// Destinations the pools list by name, with their class. Present for the
    /// backend-name and class checks whether or not their manifests were
    /// passed.
    pub listed: Vec<(String, BackendType)>,
    /// Whether to check model names and label selectors, which need the
    /// destinations' own manifests.
    pub details: bool,
}

/// [`reference_warnings`] for a manifest check. See [`Offline`].
pub fn reference_warnings_offline(
    spec: &InferencePolicySpec,
    pools: &[&ResolvedPool],
    offline: &Offline,
) -> NameCheck {
    check(spec, pools, &offline.listed, offline.details)
}

fn check(
    spec: &InferencePolicySpec,
    pools: &[&ResolvedPool],
    listed: &[(String, BackendType)],
    details: bool,
) -> NameCheck {
    if pools.is_empty() {
        return NameCheck::default();
    }
    let fleet = Destinations::of(pools, listed);
    let mut out = NameCheck {
        warnings: Vec::new(),
        models_unchecked: if details {
            fleet.models_unchecked()
        } else {
            None
        },
    };
    let ck = Checker {
        fleet: &fleet,
        details,
        out: &mut out.warnings,
    };
    ck.policy(spec);
    out
}

/// Everything the checks need to know about the attached pools.
struct Destinations<'a> {
    /// `pool foo/bar` or `pools foo/a, foo/b`, for messages.
    scope: String,
    /// By name. Names are unique across classes within a pool; across pools,
    /// the same name in the same namespace is the same object.
    by_name: BTreeMap<&'a str, &'a ResolvedDestination>,
    /// Listed by a pool but not resolved, with the class the pool gives:
    /// known by name only. Empty except offline.
    listed: BTreeMap<&'a str, BackendType>,
    /// Every model name a destination declares, by row name or alias.
    models: BTreeSet<&'a str>,
    /// Local sets that declare no `spec.models` rows.
    undeclared: Vec<&'a str>,
}

impl<'a> Destinations<'a> {
    fn of(pools: &[&'a ResolvedPool], listed: &'a [(String, BackendType)]) -> Destinations<'a> {
        let names: Vec<String> = pools
            .iter()
            .map(|p| format!("{}/{}", p.namespace, p.name))
            .collect();
        let scope = match names.as_slice() {
            [one] => format!("pool {one}"),
            many => format!("pools {}", many.join(", ")),
        };
        let mut by_name = BTreeMap::new();
        let mut models = BTreeSet::new();
        let mut undeclared = Vec::new();
        for d in pools.iter().flat_map(|p| p.destinations.iter()) {
            if by_name.insert(d.name(), d).is_some() {
                continue;
            }
            match d {
                ResolvedDestination::Local(w) => {
                    if w.aliases.is_empty() {
                        undeclared.push(w.name.as_str());
                    }
                    for row in &w.aliases {
                        models.extend(row.name.as_deref());
                        models.extend(row.aliases.iter().map(String::as_str));
                    }
                }
                ResolvedDestination::Provider(p) => {
                    for row in &p.models {
                        models.extend(row.name.as_deref());
                        models.extend(row.aliases.iter().map(String::as_str));
                    }
                }
            }
        }
        let listed = listed
            .iter()
            .filter(|(name, _)| !by_name.contains_key(name.as_str()))
            .map(|(name, class)| (name.as_str(), *class))
            .collect();
        Destinations {
            scope,
            by_name,
            listed,
            models,
            undeclared,
        }
    }

    /// Every destination name, resolved or only listed.
    fn names(&self) -> BTreeSet<&'a str> {
        self.by_name
            .keys()
            .chain(self.listed.keys())
            .copied()
            .collect()
    }

    fn models_unchecked(&self) -> Option<String> {
        (!self.undeclared.is_empty()).then(|| {
            format!(
                "worker set(s) {} declare no `spec.models`, so they may serve any model name \
                 and none can be ruled out",
                self.undeclared.join(", ")
            )
        })
    }

    fn has(&self, class: BackendType) -> bool {
        self.by_name.values().any(|d| d.backend_type() == class)
            || self.listed.values().any(|c| *c == class)
    }
}

struct Checker<'a, 'f> {
    fleet: &'f Destinations<'a>,
    /// Whether model names and label selectors are checked. See [`Offline`].
    details: bool,
    out: &'f mut Vec<PolicyWarning>,
}

impl Checker<'_, '_> {
    fn policy(mut self, spec: &InferencePolicySpec) {
        for (i, rule) in spec.rules.iter().enumerate() {
            let rp = format!("spec.rules[{i}]");
            let r = rule.name.as_str();
            for (j, m) in rule.matches.iter().enumerate() {
                if let Some(raw) = &m.model {
                    self.models(
                        raw,
                        AttrKind::StrList,
                        &format!("{rp}.matches[{j}].model"),
                        r,
                    );
                }
            }
            for (k, g) in rule.route.consider.iter().enumerate() {
                let gp = format!("{rp}.route.consider[{k}]");
                let (class, class_path) = match (g.type_, g.backends.as_ref().and_then(|b| b.type_))
                {
                    (Some(t), _) => (Some(t), format!("{gp}.type")),
                    (None, Some(t)) => (Some(t), format!("{gp}.backends.type")),
                    (None, None) => (None, String::new()),
                };
                if let Some(t) = class {
                    self.class(t, &class_path, r, "consider group");
                }
                if let Some(b) = &g.backends {
                    for (n, name) in b.names.iter().enumerate() {
                        self.backend(name, &format!("{gp}.backends.names[{n}]"), r);
                    }
                    if let Some(sel) = b.selector.as_ref().filter(|s| !s.match_labels.is_empty()) {
                        self.selector(
                            &sel.match_labels,
                            class,
                            &format!("{gp}.backends.selector"),
                            r,
                            "consider group",
                        );
                    }
                }
                if let Some(ms) = &g.models {
                    for (n, name) in ms.names.iter().enumerate() {
                        self.model(name, &format!("{gp}.models.names[{n}]"), r);
                    }
                }
            }
            for (e, ex) in rule.route.exclude.iter().enumerate() {
                let ep = format!("{rp}.route.exclude[{e}]");
                if let Some(raw) = ex.if_.as_ref().and_then(|g| g.model.as_ref()) {
                    self.models(raw, AttrKind::StrList, &format!("{ep}.if.model"), r);
                }
                if let Some(raw) = ex.drop.backend.as_ref().and_then(|b| b.name.as_ref()) {
                    for name in operands(raw, AttrKind::Str) {
                        self.backend(&name, &format!("{ep}.drop.backend.name"), r);
                    }
                }
                if let Some(raw) = ex.drop.model.as_ref().and_then(|m| m.name.as_ref()) {
                    self.models(raw, AttrKind::Str, &format!("{ep}.drop.model.name"), r);
                }
            }
            for (t, tier) in rule.route.select.iter().enumerate() {
                let tp = format!("{rp}.route.select[{t}]");
                let tier_class = tier.type_.or(tier.backends.as_ref().and_then(|b| b.type_));
                match (tier.type_, tier.backends.as_ref().and_then(|b| b.type_)) {
                    (Some(c), _) => self.class(c, &format!("{tp}.type"), r, "select tier"),
                    (None, Some(c)) => {
                        self.class(c, &format!("{tp}.backends.type"), r, "select tier")
                    }
                    (None, None) => {}
                }
                if let Some(sel) = tier
                    .backends
                    .as_ref()
                    .and_then(|b| b.selector.as_ref())
                    .filter(|s| !s.match_labels.is_empty())
                {
                    self.selector(
                        &sel.match_labels,
                        tier_class,
                        &format!("{tp}.backends.selector"),
                        r,
                        "select tier",
                    );
                }
                if let Some(b) = &tier.backends {
                    for (n, name) in b.names.iter().enumerate() {
                        self.backend(name, &format!("{tp}.backends.names[{n}]"), r);
                    }
                }
                for (n, name) in tier.order.iter().enumerate() {
                    self.backend(name, &format!("{tp}.order[{n}]"), r);
                }
            }
        }
    }

    fn backend(&mut self, name: &str, path: &str, rule: &str) {
        let known = self.fleet.names();
        if known.contains(name) {
            return;
        }
        let known: Vec<&str> = known.into_iter().collect();
        let hint = did_you_mean(name, known.iter().copied())
            .map(|s| format!(" Did you mean `{s}`?"))
            .unwrap_or_default();
        self.out.push(PolicyWarning::new(
            WarningCode::UnknownBackend,
            path,
            format!(
                "rule `{rule}`: `{name}` is not a destination of {}, so nothing matches it.{hint} \
                 Known destinations: {}",
                self.fleet.scope,
                known.join(", ")
            ),
        ));
    }

    fn class(&mut self, class: BackendType, path: &str, rule: &str, what: &str) {
        if self.fleet.has(class) {
            return;
        }
        self.out.push(PolicyWarning::new(
            WarningCode::NoDestinationsOfType,
            path,
            format!(
                "rule `{rule}`: this {what} takes only {} destinations, and {} has none, so it \
                 never has a candidate",
                class.as_str(),
                self.fleet.scope
            ),
        ));
    }

    fn selector(
        &mut self,
        labels: &BTreeMap<String, String>,
        class: Option<BackendType>,
        path: &str,
        rule: &str,
        what: &str,
    ) {
        if !self.details {
            return;
        }
        let hit = self.fleet.by_name.values().any(|d| {
            class.is_none_or(|c| d.backend_type() == c)
                && labels.iter().all(|(k, v)| d.labels().get(k) == Some(v))
        });
        if hit {
            return;
        }
        let want = labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ");
        self.out.push(PolicyWarning::new(
            WarningCode::SelectorMatchesNothing,
            path,
            format!(
                "rule `{rule}`: no {}destination of {} has the labels {want}, so this {what}'s \
                 label selector admits nothing",
                class
                    .map(|c| format!("{} ", c.as_str()))
                    .unwrap_or_default(),
                self.fleet.scope
            ),
        ));
    }

    /// Every name a predicate value compares against, checked as a model.
    fn models(&mut self, raw: &serde_json::Value, kind: AttrKind, path: &str, rule: &str) {
        for name in operands(raw, kind) {
            self.model(&name, path, rule);
        }
    }

    fn model(&mut self, name: &str, path: &str, rule: &str) {
        if name == crate::monitor::AUTO_MODEL {
            self.out.push(PolicyWarning::new(
                WarningCode::UnknownModel,
                path,
                format!(
                    "rule `{rule}`: `auto` is resolved to a concrete model before any rule runs, \
                     so a rule never sees it and this never matches. Name the models `auto` can \
                     resolve to instead"
                ),
            ));
            return;
        }
        if !self.details || !self.fleet.undeclared.is_empty() || self.fleet.models.contains(name) {
            return;
        }
        let hint = did_you_mean(name, self.fleet.models.iter().copied())
            .map(|s| format!(" Did you mean `{s}`?"))
            .unwrap_or_default();
        let declared = if self.fleet.models.is_empty() {
            "none".to_string()
        } else {
            self.fleet
                .models
                .iter()
                .copied()
                .collect::<Vec<_>>()
                .join(", ")
        };
        self.out.push(PolicyWarning::new(
            WarningCode::UnknownModel,
            path,
            format!(
                "rule `{rule}`: no destination of {} declares a model named `{name}`, by name or \
                 alias, so nothing matches it.{hint} Declared: {declared}. If an engine serves \
                 `{name}` without declaring it, add it to that worker set's `spec.models`",
                self.fleet.scope,
            ),
        ));
    }
}

/// The strings a predicate value compares against. An invalid value yields
/// nothing: the compiler has already reported it.
fn operands(raw: &serde_json::Value, kind: AttrKind) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(m) = Matcher::compile(raw, kind) {
        m.string_operands(&mut out);
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests;
