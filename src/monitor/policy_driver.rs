//! The CRD policy driver — running `InferencePolicy` for a request that named
//! an `InferencePool`, and turning the result into candidates.
//!
//! This is the seam between `crate::policy` (which resolves and compiles the
//! CRDs, and knows nothing about a request) and the request path (which knows
//! nothing about CRDs). `policy_select` is where a compiled policy meets a
//! live snapshot of pools, worker sets and providers; `candidates_for`
//! flattens the group it chose into per-endpoint candidates.
//!
//! Ranking happens here WITHOUT holding any pool lock, and that is load-bearing
//! rather than incidental — see `policy_select`'s own docs for why taking two
//! pool locks in ranking order would deadlock.

use super::*;

impl KinferenceService {
    /// Run the CRD policy engine for a request that named an `InferencePool`,
    /// returning the group and endpoint it chose.
    ///
    /// Candidates may span WORKER SETS, and each worker set is a different
    /// kinference pool with its own `pick_lock`. Two requests taking two pool locks
    /// in opposite orders would deadlock, so ranking happens here, lock-free,
    /// and only the winner's group takes a lock (to commit). The consequence is
    /// bounded and worth naming: a policy ranks on a snapshot that can be up to
    /// one concurrent pick stale. Inflight bookkeeping is still exact, because
    /// the commit bumps it under the lock.
    ///
    /// `Ok(selection)` always names a group; `selection.endpoint` is `None`
    /// when the policy had no opinion and the normal picker should choose —
    /// the deliberate response to a pool kinference has never seen, since refusing
    /// traffic over an operator's missing CRD is worse than routing on load.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn policy_select(
        &self,
        pref: &crate::policy::PoolRef,
        store: &Arc<crate::policy::store::PolicyStore>,
        primary: Option<ScoredGroup>,
        // The name the request resolved to. Passed explicitly rather than read
        // off `primary`, which may not exist: a model only a provider serves has
        // no scored group to take it from.
        requested_model: &str,
        spare: Option<(Vec<serde_json::Value>, ChatExtras, Vec<u8>)>,
        attrs: crate::policy::predicate::RequestAttrs,
        req_label: &str,
    ) -> Result<PolicySelection, PickAndRecordOutcome> {
        use crate::policy::engine;

        let snapshot = store.snapshot();
        let Some(resolved) = snapshot.lookup(&pref.namespace, &pref.name) else {
            // Both branches fail open — a request is served either way — but
            // they are different operational facts and must not share a log
            // line. Before the first list completes, EVERY pool looks unknown,
            // so treating the two alike buries a real misconfiguration under a
            // burst of startup noise.
            if snapshot.listed() {
                tracing::warn!(
                    req = %req_label,
                    pool = %pref.label(),
                    "policy: request named an unknown InferencePool; routing on load alone"
                );
                self.prom.record_policy_decision("unknown_pool");
            } else {
                tracing::debug!(
                    req = %req_label,
                    pool = %pref.label(),
                    "policy: CRD watch has not listed yet; routing on load alone"
                );
                self.prom.record_policy_decision("not_listed");
            }
            return Ok(PolicySelection::Picker { group: primary });
        };

        // Opaque revision: only equality is meaningful, so we can tell that we
        // differ but not whether we are behind. Advisory per the proto — record
        // it and proceed on cache rather than pay an API round-trip per pick.
        if !pref.resource_version.is_empty() && pref.resource_version != resolved.resource_version {
            self.prom.record_policy_revision_mismatch();
            tracing::debug!(
                req = %req_label,
                pool = %pref.label(),
                requested = %pref.resource_version,
                have = %resolved.resource_version,
                "policy: pool revision differs from the one the gateway routed on"
            );
        }

        let model = requested_model.to_string();
        // The pool is a hard outer bound. If it lists nothing kinference can route
        // to, no rule can rescue it.
        if resolved.destinations.is_empty() {
            self.prom.record_policy_decision("no_destinations");
            return Err(PickAndRecordOutcome::PolicyRejected {
                reason: format!(
                    "pool {} lists no destination kinference can route to",
                    pref.label()
                ),
                model,
            });
        }

        let policy = resolved.policy.clone();
        let rules = engine::matching_rules(&policy, &attrs);
        let any_rule_matched = !rules.is_empty();
        // Rules first, then the implicit unmatched route — `None` marks the
        // latter so traces read `<unmatched>`.
        //
        // The unmatched route closes the plan whether no rule matched or every
        // matching rule fell through, and either way it gets the same
        // generation pass as a rule: it considers what `unmatched` says — the
        // requested model, on local destinations — and scores sets no earlier
        // rule reached. A separate fall-through tail used to reuse whatever
        // groups earlier rules had scored instead, including groups for OTHER
        // models, so a request could be served a model it never asked for.
        let mut plan: Vec<(Option<String>, crate::policy::compile::CompiledRoute)> = rules
            .iter()
            .map(|r| (Some(r.name.clone()), r.route.clone()))
            .collect();
        match engine::unmatched_route(&policy) {
            Ok(route) => plan.push((None, route)),
            Err(_) if !any_rule_matched => {
                // `unmatched: Reject` — the setting that exists so an
                // exhaustive rule set cannot silently admit an unlisted
                // tenant.
                tracing::info!(
                    req = %req_label,
                    pool = %pref.label(),
                    "policy: no rule matched and unmatched=Reject; refusing"
                );
                self.prom.record_policy_decision("rejected_unmatched");
                return Err(PickAndRecordOutcome::PolicyRejected {
                    reason: "no rule matched and unmatched=Reject".to_string(),
                    model,
                });
            }
            // Rules matched. If every one falls through, the refusal comes
            // after the loop.
            Err(_) => {}
        }

        // Groups scored so far, keyed by (pool, model), so a model reachable on
        // several destinations is tokenized once. Empty when nothing local
        // serves the requested model — a rule can still reach a provider, or
        // name a model another worker set does serve, and both are scored
        // lazily below.
        let mut scored: Vec<ScoredGroup> = primary.into_iter().collect();

        for (rule_name, route) in &plan {
            // `attrs.model`, not `model`: the former is the whole set the
            // request answers to, the latter only the upstream it resolved to.
            // Scoring the resolved name alone is what made a provider serving
            // its own build of the same model unreachable.
            let targets = engine::models_to_score(route, &attrs.model);
            let mut candidates: Vec<engine::Candidate> = Vec::new();

            for target in &targets {
                for dest in &resolved.destinations {
                    if !target
                        .backends
                        .matches(dest.backend_type(), dest.name(), dest.labels())
                    {
                        continue;
                    }
                    // A provider needs no scoring, no tokenization and no index
                    // query — there is nothing to scrape. Its candidates come
                    // straight off the declared model table.
                    let dest = match dest.as_local() {
                        Some(local) => local,
                        None => {
                            let prov = dest.as_provider().expect("not local, so provider");
                            candidates.extend(provider_candidates_for(
                                &self.provider_health,
                                prov,
                                &target.model,
                            ));
                            continue;
                        }
                    };
                    // Aliases: a canonical name can map to several upstream
                    // models, and each is its own candidate — that is how
                    // "interchangeable" becomes a routing choice.
                    for upstream in dest.upstreams_for(&target.model) {
                        let idx = match scored
                            .iter()
                            .position(|g| g.pool.id == dest.pool_id && g.model == upstream)
                        {
                            Some(i) => i,
                            None => {
                                let Some(pool) = self.pools.snapshot().get(dest.pool_id).cloned()
                                else {
                                    continue;
                                };
                                let Some((msgs, extras, body)) = spare.clone() else {
                                    // Only reachable if scoring inputs were not
                                    // cloned, which cannot happen when a policy
                                    // is active.
                                    continue;
                                };
                                // Tokenization belongs to the MODEL, not the
                                // pool — same tokenizer, same chat template. So
                                // if we already tokenized this model for another
                                // worker set, reuse the tokens and let this call
                                // do only the per-pool work (block hashing at
                                // this pool's block_size, and the index query).
                                // Otherwise a policy spanning N worker sets would
                                // pay N render round-trips for one prompt.
                                let cached = scored
                                    .iter()
                                    .find(|g| g.model == upstream && !g.side.tokens.is_empty())
                                    .map(|g| g.side.tokens.clone());
                                let (input, raw) = match cached {
                                    Some(t) => (Input::Tokens(t), None),
                                    None => (Input::Messages(msgs), Some(body)),
                                };
                                let (outcome, side) = self
                                    .score_workers(
                                        &pool,
                                        &upstream,
                                        input,
                                        HashMap::new(),
                                        extras,
                                        raw,
                                        // Telemetry is recorded once, for the
                                        // primary group; extra groups are the
                                        // policy's fan-out, not new traffic.
                                        /*record_call_telemetry=*/
                                        false,
                                        Some(req_label),
                                    )
                                    .await;
                                let s = match outcome {
                                    ScoreOutcome::Ok(r) => r.scores,
                                    ScoreOutcome::Unavailable(r, why) => {
                                        self.prom.record_score_degraded(why.as_label());
                                        r.scores
                                    }
                                    // This worker set doesn't serve the model.
                                    ScoreOutcome::NoWorkers => continue,
                                };
                                if s.is_empty() {
                                    continue;
                                }
                                scored.push(ScoredGroup {
                                    pool,
                                    model: upstream.clone(),
                                    scores: s,
                                    side,
                                });
                                scored.len() - 1
                            }
                        };
                        candidates.extend(self.candidates_for(
                            &scored[idx],
                            idx,
                            dest,
                            &target.model,
                        ));
                    }
                }
            }

            // The cross-group comparison, which the score table structurally
            // cannot show: that table is bound to ONE (pool, model) group,
            // because its per-endpoint columns — prefix overlap, kv_blocks —
            // only mean anything within one pool's radix tree. Without this,
            // a candidate eliminated by an `exclude` rule is named in the trace
            // while one that merely LOST on predictedTTFT vanishes, which is the
            // more common outcome and the one you cannot reconstruct.
            //
            // One line per candidate, sorted best-first and capped: endpoints ×
            // models can be dozens on a real fleet, and this is the per-request
            // debug path. `rank` and `n` make a truncated list obvious rather
            // than looking like the whole candidate set.
            if tracing::enabled!(tracing::Level::DEBUG) {
                // Local candidates sort by their prediction; provider candidates
                // have none, so they sort last and are labelled as such rather
                // than being given a number that would read as a measurement.
                let sort_key = |c: &engine::Candidate| match &c.target {
                    engine::CandidateTarget::Local {
                        predicted_ttft_ms, ..
                    } => *predicted_ttft_ms,
                    engine::CandidateTarget::Provider { .. } => f32::INFINITY,
                };
                let mut ranked: Vec<&engine::Candidate> = candidates.iter().collect();
                ranked.sort_by(|a, b| {
                    sort_key(a)
                        .partial_cmp(&sort_key(b))
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                const SHOWN: usize = 8;
                let total = ranked.len();
                for (i, c) in ranked.iter().take(SHOWN).enumerate() {
                    let (endpoint, predicted) = match &c.target {
                        engine::CandidateTarget::Local {
                            endpoint,
                            predicted_ttft_ms,
                            ..
                        } => (endpoint.clone(), format!("{predicted_ttft_ms:.1}")),
                        engine::CandidateTarget::Provider {
                            backend_ref,
                            observed_ttft_ms,
                            ..
                        } => (
                            backend_ref.label(),
                            observed_ttft_ms
                                .map(|ms| format!("~{ms:.1}"))
                                .unwrap_or_else(|| "unobserved".to_string()),
                        ),
                    };
                    tracing::debug!(
                        req = %req_label,
                        rule = rule_name.as_deref().unwrap_or("<unmatched>"),
                        rank = i + 1,
                        n = total,
                        backend = %c.backend,
                        class = %c.backend_type().as_str(),
                        model = %c.upstream_model,
                        endpoint = %endpoint,
                        predicted_ms = %predicted,
                        "policy: candidate"
                    );
                }
                if total > SHOWN {
                    tracing::debug!(
                        req = %req_label,
                        rule = rule_name.as_deref().unwrap_or("<unmatched>"),
                        elided = total - SHOWN,
                        n = total,
                        "policy: further candidates not listed"
                    );
                }
            }

            match engine::evaluate(route, rule_name.as_deref(), &attrs, candidates) {
                engine::Outcome::Chosen { candidate, trace } => {
                    log_winner(req_label, &pref.label(), &candidate, &trace);
                    self.prom.record_policy_decision(if rule_name.is_some() {
                        "chosen"
                    } else {
                        "unmatched_served"
                    });
                    if let Some(n) = rule_name {
                        self.prom.record_policy_rule_match(n);
                    }
                    return Ok(selection(&mut scored, candidate));
                }
                engine::Outcome::Empty { trace } => {
                    tracing::debug!(
                        req = %req_label,
                        pool = %pref.label(),
                        "policy: no candidate survived — {}",
                        trace.summary()
                    );
                    if rule_name.is_some() && engine::falls_through(route) {
                        // FallThrough: try the next matching rule.
                        continue;
                    }
                    let (label, reason) = match rule_name {
                        Some(n) => (
                            "rejected_on_empty",
                            format!("every candidate was excluded by rule `{n}` (onEmpty: Reject)"),
                        ),
                        None => (
                            "unmatched_empty",
                            if any_rule_matched {
                                "every matching rule fell through and the unmatched route had \
                                 no candidate"
                            } else {
                                "no rule matched and the unmatched route had no candidate"
                            }
                            .to_string(),
                        ),
                    };
                    self.prom.record_policy_decision(label);
                    return Err(PickAndRecordOutcome::PolicyRejected { reason, model });
                }
            }
        }

        // Every matching rule fell through and there is no unmatched route to
        // close the plan: `unmatched: Reject`.
        self.prom.record_policy_decision("rejected_unmatched");
        Err(PickAndRecordOutcome::PolicyRejected {
            reason: "every matching rule fell through and unmatched=Reject".to_string(),
            model,
        })
    }

    /// Build one candidate per endpoint in a scored group.
    ///
    /// `group_idx` rides on `Candidate::score_idx` so the winner can be mapped
    /// straight back to the group whose scores and prefix hashes the critical
    /// section must commit with — those are per-pool, so committing against the
    /// wrong group's would record bogus prefix state.
    fn candidates_for(
        &self,
        group: &ScoredGroup,
        group_idx: usize,
        dest: &crate::policy::resolve::ResolvedWorkerSet,
        canonical_model: &str,
    ) -> Vec<crate::policy::engine::Candidate> {
        let now = Instant::now();
        let ready_endpoints = group.pool.registry.len() as i64;
        let labels = Arc::new(dest.labels.clone());
        // Effective context window per ENDPOINT, not per pool: a rollout that
        // changes --max-model-len leaves endpoints disagreeing, and admission
        // must use the individual endpoint's value so long requests stop
        // landing on reconfigured ones rather than failing there.
        let context_by_endpoint: HashMap<String, u32> = group
            .pool
            .registry
            .iter_snapshot()
            .into_iter()
            .filter_map(|(num, entry)| {
                let info = self.metrics.models(num);
                let window = info
                    .models
                    .iter()
                    .find(|m| m.name == group.model)
                    .and_then(|m| m.max_model_len)
                    .or_else(|| info.models.iter().filter_map(|m| m.max_model_len).min())?;
                Some((entry.score_addr, window))
            })
            .collect();
        // The two SATURATION signals, per endpoint, as windowed means.
        //
        // Not taken from `WorkerScore`: that carries the instantaneous
        // `queue_waiting` the picker needs to predict the TTFT of a dispatch
        // happening right now, and no KV field at all. Admission asks a
        // different question — "is this endpoint under SUSTAINED pressure" —
        // and a threshold on the instantaneous gauge answers it wrongly,
        // because `num_requests_waiting` spikes for seconds and clears.
        //
        // Only endpoints that have been scraped: `latest` would hand back an
        // all-zero sample for the rest, which reads as an idle worker and lets
        // a `drop` on either signal pass a worker nothing is known about.
        let saturation_by_endpoint: HashMap<String, (f64, f64)> = group
            .pool
            .registry
            .iter_snapshot()
            .into_iter()
            .filter_map(|(num, entry)| {
                let m = self.metrics.sample(num)?;
                Some((
                    entry.score_addr,
                    (m.waiting_mean as f64, m.kv_cache_usage_mean as f64),
                ))
            })
            .collect();

        group
            .scores
            .iter()
            .map(|s| {
                let summaries: Vec<crate::inflight_prefixes::InflightSummary> = group
                    .pool
                    .inflight_prefixes
                    .dispatches_for(&s.endpoint)
                    .iter()
                    .map(|d| d.summary())
                    .collect();
                let predicted =
                    crate::picker::predicted_wait_ms(s, &group.pool.predictor, &summaries, now)
                        .total_ms();
                // Recorded for EVERY candidate, which is what makes the
                // distribution the right basis for a `predictedTTFT`
                // threshold: that threshold is applied to every candidate too.
                // Recording only the winner would bias the p95 low, because
                // the winner won by being fast.
                self.prom
                    .record_predicted_ttft(&s.endpoint, &group.model, predicted as f64);
                let hit_ratio = if s.total_blocks > 0 {
                    s.matched_blocks as f64 / s.total_blocks as f64
                } else {
                    0.0
                };
                crate::policy::engine::Candidate {
                    canonical_model: canonical_model.to_string(),
                    upstream_model: group.model.clone(),
                    backend: dest.name.clone(),
                    labels: labels.clone(),
                    target: crate::policy::engine::CandidateTarget::Local {
                        pool_id: group.pool.id,
                        endpoint: s.endpoint.clone(),
                        score_idx: group_idx,
                        predicted_ttft_ms: predicted,
                    },
                    attrs: crate::policy::predicate::CandidateAttrs {
                        context_window: context_by_endpoint.get(&s.endpoint).map(|v| *v as i64),
                        predicted_ttft_ms: Some(predicted as f64),
                        prefix_cache_hit_ratio: Some(hit_ratio),
                        // Absent rather than zero when the endpoint has not
                        // been scraped: a predicate over an absent signal does
                        // not match, whereas a fabricated 0.0 would read as
                        // "idle" and let a `drop` rule pass a worker it knows
                        // nothing about.
                        queued_prompts: saturation_by_endpoint.get(&s.endpoint).map(|(q, _)| *q),
                        kv_cache_usage: saturation_by_endpoint.get(&s.endpoint).map(|(_, k)| *k),
                        model_name: Some(canonical_model.to_string()),
                        backend_name: Some(dest.name.clone()),
                        backend_type: Some(
                            crate::policy::crd::BackendType::Local.as_str().to_string(),
                        ),
                        backend_ready_endpoints: Some(ready_endpoints),
                        // Provider-scoped, so absent here — and absent means
                        // "does not match", never a stand-in value.
                        backend_observed_ttft_ms: None,
                        backend_budget_exhausted: None,
                    },
                }
            })
            .collect()
    }
}
