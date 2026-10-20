//! The request lifecycle behind `Pick()`: one request from body to chosen
//! endpoint, and the observation it leaves behind.
//!
//! `pick_and_record` is long because it is one transaction over a JSON body
//! that is rewritten and then drained, plus a critical section under the
//! pool's `pick_lock`. What it reserves is released by the `Observations`
//! part, or by `abort_request` when the caller gives up.

use std::time::Instant;

use super::*;
use crate::router::observations::PendingObservation;

/// What `Pick()` carried, read by every step of the pick.
struct PickRequest<'a> {
    /// The pushed body, forwarded unchanged unless a step rewrites it.
    body: &'a [u8],
    request_id: u64,
    client_request_id: Option<&'a str>,
    /// For logs: the client's id, else ours.
    label: String,
    qos: Qos,
    pool_ref: Option<&'a kinference_policy::PoolRef>,
    identity: &'a kinference_policy::RequestIdentity,
}

/// Step 1. The body as a JSON tree, with what the later steps read from it.
struct Parsed {
    obj: serde_json::Value,
    /// The body's `model`, non-empty.
    model: String,
    facts: BodyFacts,
}

/// What the policy and the observation read from the body.
struct BodyFacts {
    streaming: bool,
    max_tokens: Option<i64>,
    /// The last user turn, or the completions `prompt`, for the classifiers.
    classifier_text: Option<String>,
}

/// The body as `PullBody` will serve it. `None`: the pushed body goes out
/// unchanged, which the outcome spells as an empty body.
struct WireBody {
    rewritten: Option<Vec<u8>>,
}

impl WireBody {
    /// What `/render` and the engine see.
    fn bytes<'a>(&'a self, pushed: &'a [u8]) -> &'a [u8] {
        self.rewritten.as_deref().unwrap_or(pushed)
    }

    /// Name `model` in the body. `false` when the bytes are not a JSON
    /// object, which a body `parse` accepted cannot be.
    fn name_model(&mut self, pushed: &[u8], model: &str) -> bool {
        match crate::router::pick_body::rewrite_model(self.bytes(pushed), model) {
            Some(bytes) => {
                self.rewritten = Some(bytes);
                true
            }
            None => false,
        }
    }

    /// For the outcome: the rewritten bytes, or empty.
    fn into_bytes(self) -> Vec<u8> {
        self.rewritten.unwrap_or_default()
    }
}

/// Step 2. The upstream model, the pools serving it, the body as the engine
/// will see it, and what scoring consumes.
struct Resolved {
    model: String,
    /// Empty when only a provider in the named pool serves the model.
    serving: Vec<Arc<Pool>>,
    body: WireBody,
    facts: BodyFacts,
    inputs: ScoreInput,
}

impl KinferenceService {
    /// Full pick path behind gRPC `Pick()`:
    /// 1. Parse the OpenAI body for model + messages + chat-template
    ///    inputs. Resolve the `"auto"` pseudo-model to a concrete model,
    ///    then apply the Pick-time body rewrites (`crate::router::pick_body`):
    ///    model="auto" → concrete, and `stream_options.include_usage` for
    ///    streaming requests.
    /// 2. Call `score_workers` (which tokenizes + queries the indexer),
    ///    passing the rewritten body so `/render` sees what the engine will.
    /// 3. Inside `pick_lock`: apply layered overlap, snapshot
    ///    InflightRegistry, run the picker, bump inflight, insert
    ///    inflight-prefix, stash PendingObservation.
    /// 4. Return the chosen endpoint + request id + the resolved model + the
    ///    rewritten `body`, which is empty when the body was left alone (then
    ///    `grpc::pick` reuses the pushed body).
    ///
    /// The caller mints `request_id` (see `mint_response_id`), so the same id
    /// flows through pending-observation lookup and log correlation.
    pub(crate) async fn pick_and_record(
        &self,
        body: &[u8],
        request_id: u64,
        client_request_id: Option<&str>,
        qos: Qos,
        pool_ref: Option<&kinference_policy::PoolRef>,
        identity: &kinference_policy::RequestIdentity,
    ) -> PickAndRecordOutcome {
        let req = PickRequest {
            body,
            request_id,
            client_request_id,
            label: client_request_id
                .map(|s| s.to_string())
                .unwrap_or_else(|| request_id.to_string()),
            qos,
            pool_ref,
            identity,
        };

        let parsed = match Self::parse(&req) {
            Ok(p) => p,
            Err(out) => return out,
        };
        let Resolved {
            mut model,
            serving,
            body: mut wire,
            facts,
            inputs: input,
        } = match self.resolve_model(&req, parsed) {
            Ok(r) => r,
            Err(out) => return out,
        };
        let BodyFacts {
            streaming,
            max_tokens,
            classifier_text,
        } = facts;

        // Scoring consumes the inputs, but a second pool or a policy may need
        // them again; only clone in that case.
        let policy_active = req.pool_ref.is_some() && self.policy.is_some();
        let spare = (policy_active || serving.len() > 1).then(|| input.clone());
        let mut degraded_reason: Option<String> = None;
        let mut groups: Vec<ScoredGroup> = Vec::new();
        // Skipped when nothing local serves the model.
        if let Some(first) = serving.first() {
            let (outcome, score_side) = self
                .scoring
                .score_workers(
                    &*self.tokenizer,
                    first,
                    &model,
                    input,
                    HashMap::new(),
                    Some(&req.label),
                )
                .await;
            match outcome {
                ScoreOutcome::Ok(resp) => groups.push(ScoredGroup {
                    pool: first.clone(),
                    model: model.clone(),
                    scores: resp.scores,
                    side: score_side,
                }),
                ScoreOutcome::Unavailable(resp, why) => {
                    tracing::warn!(
                        req = %req.label,
                        model = %model,
                        pool = %first.name,
                        reason = why.as_label(),
                        "pick: no prefix-cache signal; picking on load metrics only"
                    );
                    self.prom.record_score_degraded(why.as_label());
                    degraded_reason = Some(why.reason().to_string());
                    groups.push(ScoredGroup {
                        pool: first.clone(),
                        model: model.clone(),
                        scores: resp.scores,
                        side: score_side,
                    });
                }
                // Not fatal: another pool or a provider may serve the model.
                ScoreOutcome::NoWorkers => {}
            }
        }

        // 2a. The remaining pools serving this model, reusing the first
        //     group's tokens.
        if serving.len() > 1 {
            let reuse = groups
                .first()
                .map(|g| g.side.tokens.clone())
                .filter(|t| !t.is_empty());
            for pool in &serving[1..] {
                let Some(spare) = spare.as_ref() else {
                    continue;
                };
                let input = match reuse.clone() {
                    Some(t) => ScoreInput::tokens(t),
                    None => spare.clone(),
                };
                let (outcome, side) = self
                    .scoring
                    .score_workers(
                        &*self.tokenizer,
                        pool,
                        &model,
                        input,
                        HashMap::new(),
                        Some(&req.label),
                    )
                    .await;
                let scores = match outcome {
                    ScoreOutcome::Ok(r) => r.scores,
                    ScoreOutcome::Unavailable(r, why) => {
                        self.prom.record_score_degraded(why.as_label());
                        r.scores
                    }
                    ScoreOutcome::NoWorkers => continue,
                };
                if scores.is_empty() {
                    continue;
                }
                groups.push(ScoredGroup {
                    pool: pool.clone(),
                    model: model.clone(),
                    scores,
                    side,
                });
            }
        }

        // 2b. Rank across the pools serving this model, choosing the pool; the
        //     picker chooses the endpoint under that pool's lock. `None` isn't
        //     yet fatal, since a policy may still find something.
        //
        // Predictions are recorded by exactly one ranker per request: the
        // policy's `candidates_for` when a policy is in play, otherwise this.
        // Recording in both would double-count. A request naming an unresolved
        // pool records none, but is counted under
        // `kinference_policy_decisions_total`.
        let policy_will_rank = req.pool_ref.is_some() && self.policy.is_some();
        let winner = best_group(
            groups,
            &req.label,
            (!policy_will_rank).then_some(&*self.prom),
        );
        if winner.is_none() && serving.is_empty() {
            // Normal for a provider-only model.
            tracing::debug!(
                req = %req.label,
                model = %model,
                "pick: no local group to score; the decision rests entirely with the policy"
            );
        }

        // 2c. Policy. Candidates may span pools, each with its own `pick_lock`,
        //     so ranking is lock-free (avoiding lock-order deadlocks) and the
        //     winner commits under its own lock below. The ranking input may be
        //     one concurrent pick stale.
        let mut forced_endpoint: Option<String> = None;
        let selected: Option<ScoredGroup>;
        if let (Some(pref), Some(store)) = (req.pool_ref, self.policy.as_ref()) {
            let attrs = kinference_policy::predicate::RequestAttrs {
                // Upstream first, then its declared aliases.
                model: {
                    let mut names = vec![model.clone()];
                    names.extend(self.declared_aliases(req.pool_ref, &model));
                    names
                },
                // Block-granular, from scoring. `None` when nothing local was
                // scored, so `inputTokens` predicates and context-window fitting
                // don't apply.
                input_tokens: winner.as_ref().and_then(|w| {
                    w.scores
                        .first()
                        .map(|s| (s.total_blocks as i64) * (w.pool.block_size as i64))
                }),
                max_tokens,
                qos: Some(req.qos.as_label().to_string()),
                tenant: req.identity.tenant.clone(),
                // See `kinference_policy::classify`.
                tag: kinference_policy::classify::tags_for_request(
                    &req.label,
                    classifier_text.as_deref(),
                ),
            };
            match self
                .policy_select(pref, store, winner, &model, spare, attrs, &req.label)
                .await
            {
                // An off-cluster destination. Returns here, skipping the
                // pool-scoped critical section: rewrite the body to the vendor's
                // model name and hold an observation for the reported TTFT.
                Ok(PolicySelection::Provider(pick)) => {
                    if !wire.name_model(req.body, &pick.upstream_model) {
                        tracing::error!(
                            req = %req.label,
                            from_model = %model,
                            to_model = %pick.upstream_model,
                            provider = %pick.provider.label(),
                            "policy: chose a provider but the body could not be rewritten; \
                             refusing rather than sending a model the vendor does not know"
                        );
                        return PickAndRecordOutcome::BadRequest {
                            reason: "could not rewrite body model for the chosen provider"
                                .to_string(),
                            model,
                        };
                    }
                    tracing::info!(
                        req = %req.label,
                        provider = %pick.provider.label(),
                        backend = %pick.backend_ref.label(),
                        from_model = %model,
                        to_model = %pick.upstream_model,
                        "policy: routing off-cluster"
                    );
                    self.observations.record_provider_pick(
                        req.request_id,
                        &pick.provider,
                        &pick.upstream_model,
                        req.qos,
                        &req.identity.tenant,
                        streaming,
                    );
                    self.prom.record_routed(
                        &req.identity.tenant,
                        &pick.provider.name,
                        kinference_policy::crd::BackendType::Provider,
                    );
                    return PickAndRecordOutcome::Provider {
                        pick,
                        response_id: req.request_id,
                        body: wire.into_bytes(),
                    };
                }
                Ok(sel) => {
                    // Re-bind to the winning group, whose pool's scores and
                    // prefix hashes the critical section uses.
                    let (group, chosen) = match sel {
                        PolicySelection::Local { group, endpoint } => (Some(group), Some(endpoint)),
                        PolicySelection::Picker { group } => (group, None),
                        PolicySelection::Provider(_) => unreachable!("handled above"),
                    };
                    let Some(group) = group else {
                        // Nothing local was scored and no provider chosen.
                        tracing::info!(
                            req = %req.label,
                            model = %model,
                            "pick: no local group and no provider chosen"
                        );
                        return PickAndRecordOutcome::NoWorkers { model };
                    };
                    // The policy may have chosen a different model than the
                    // body names, so rewrite it.
                    if group.model != model {
                        if !wire.name_model(req.body, &group.model) {
                            // Unreachable for a body we already parsed once.
                            // Refuse rather than forward a body naming a
                            // model the chosen endpoint does not serve.
                            tracing::error!(
                                req = %req.label,
                                from_model = %model,
                                to_model = %group.model,
                                "policy: chose a different model but the body could not be \
                                 rewritten; refusing rather than sending a mismatched model"
                            );
                            return PickAndRecordOutcome::BadRequest {
                                reason: "could not rewrite body model for the chosen candidate"
                                    .to_string(),
                                model: group.model,
                            };
                        }
                        tracing::debug!(
                            req = %req.label,
                            from_model = %model,
                            to_model = %group.model,
                            "policy: rewriting the body's model to the chosen one"
                        );
                    }
                    model = group.model.clone();
                    selected = Some(group);
                    forced_endpoint = chosen;
                }
                Err(outcome) => return outcome,
            }
        } else {
            // No pool named, or policy disabled: the ranking above decided, and
            // providers are unreachable.
            selected = winner;
        }

        // Everything below is bound to whichever pool won.
        let Some(group) = selected else {
            tracing::info!(
                req = %req.label,
                model = %model,
                "pick: no workers serve model"
            );
            return PickAndRecordOutcome::NoWorkers { model };
        };
        let pool = group.pool;
        let scores = group.scores;
        let score_side = group.side;

        // Derived AFTER the policy may have re-bound `pool`.
        let predictor_cfg = &pool.predictor;
        let picker_cfg = &pool.picker;
        let inflight = &*pool.inflight;
        let pick_lock = &*pool.pick_lock;
        let inflight_enabled = picker_cfg.inflight_tracking;

        // 3. Critical section: pick and state mutation.
        let chosen_endpoint: String;
        let pending: PendingObservation;
        {
            // Recover from poisoning: one panic would otherwise break every
            // later pick on this pool. The mutex guards `()`, only ordering the
            // section, and the state it touches is TTL-swept.
            let _pick_guard = pick_lock.lock().unwrap_or_else(|poisoned| {
                tracing::warn!(
                    req = %req.label,
                    workerset = %pool.name,
                    "pick_lock was poisoned by an earlier panic; continuing. The lock \
                     guards ordering only, so this is recoverable — but a panic inside \
                     the pick critical section is a bug worth chasing"
                );
                poisoned.into_inner()
            });

            let mut scores = scores;
            let kv_blocks_per_worker: Vec<u32> = scores.iter().map(|s| s.matched_blocks).collect();
            let expected_overlap_alpha = pool.predictor.expected_overlap_alpha;
            let dispatches_per_worker: Vec<Vec<kinference_fleet::inflight::DispatchSnapshot>> =
                scores
                    .iter()
                    .map(|s| pool.inflight_prefixes.dispatches_for(&s.endpoint))
                    .collect();
            let inflight_workload_per_worker: Vec<
                Vec<kinference_fleet::inflight::InflightSummary>,
            > = dispatches_per_worker
                .iter()
                .map(|d| d.iter().map(|s| s.summary()).collect())
                .collect();
            let per_worker_dispatch_overlap: Vec<Vec<f32>> = scores
                .iter()
                .zip(dispatches_per_worker.iter())
                .map(|(s, dispatches)| {
                    let query_set: std::collections::HashSet<u64> = score_side
                        .hashes_by_block_size
                        .get(&s.block_size)
                        .map(|v| v.iter().copied().collect())
                        .unwrap_or_default();
                    dispatches
                        .iter()
                        .map(|d| d.remaining.intersection(&query_set).count() as f32)
                        .collect()
                })
                .collect();
            let inflight_per_worker: Vec<u32> = if inflight_enabled {
                scores.iter().map(|s| inflight.get(&s.endpoint)).collect()
            } else {
                vec![0; scores.len()]
            };

            let score_now = Instant::now();
            let per_worker_dispatch_progress: Vec<Vec<f32>> = dispatches_per_worker
                .iter()
                .map(|dispatches| {
                    let capacity = predictor_cfg.inflight_concurrent_capacity;
                    dispatches
                        .iter()
                        .enumerate()
                        .map(|(idx, d)| {
                            let position = (idx as u32).saturating_add(1);
                            if position > capacity {
                                return 0.0;
                            }
                            let est = crate::router::picker::prefill_ms_for(
                                predictor_cfg,
                                d.uncached_at_dispatch,
                                d.total_at_dispatch,
                            );
                            if est <= 0.0 {
                                return 1.0;
                            }
                            let elapsed = score_now
                                .checked_duration_since(d.dispatched_at)
                                .map(|d| d.as_millis() as f32)
                                .unwrap_or(0.0);
                            (elapsed / est).clamp(0.0, 1.0)
                        })
                        .collect()
                })
                .collect();

            for (i, s) in scores.iter_mut().enumerate() {
                let scaled = crate::router::picker::scale_per_dispatch_by_progress(
                    &per_worker_dispatch_overlap[i],
                    &per_worker_dispatch_progress[i],
                );
                let layered = crate::router::picker::compute_layered_overlap(
                    &scaled,
                    kv_blocks_per_worker[i] as f32,
                    expected_overlap_alpha,
                );
                s.matched_blocks = layered.round() as u32;
            }
            let inputs = crate::router::picker::PickInputs {
                scores: &scores,
                predictor: predictor_cfg,
                picker: picker_cfg,
                inflight: &inflight_per_worker,
                inflight_workload: &inflight_workload_per_worker,
                kv_blocks: &kv_blocks_per_worker,
                now: score_now,
            };

            // The policy already chose, in step 2c.
            let forced_pick = forced_endpoint.as_deref().and_then(|ep| {
                let found = scores.iter().find(|s| s.endpoint == ep).map(|w| {
                    crate::router::picker::PickResult {
                        worker: w,
                        replication_fired: false,
                    }
                });
                if found.is_none() {
                    // Should be unreachable, since the endpoint came from
                    // this group's scores. Warn, since the decision counter
                    // already recorded `chosen`.
                    tracing::warn!(
                        req = %req.label,
                        model = %model,
                        worker_set = %pool.name,
                        endpoint = %ep,
                        n_scored = scores.len(),
                        "pick: policy chose an endpoint absent from the scored set; \
                         falling back to the picker"
                    );
                    self.prom.record_policy_decision("forced_endpoint_missing");
                }
                found
            });

            let pick_result = match forced_pick {
                Some(r) => r,
                None => match crate::router::picker::pick_endpoint(&inputs) {
                    Some(r) => r,
                    None => {
                        tracing::warn!(
                            req = %req.label,
                            model = %model,
                            n_scored = scores.len(),
                            "pick: picker returned None (defensive); reporting BadRequest"
                        );
                        return PickAndRecordOutcome::BadRequest {
                            reason: "picker returned no eligible worker".to_string(),
                            model: model.clone(),
                        };
                    }
                },
            };
            let chosen = pick_result.worker;
            let replication_fired = pick_result.replication_fired;

            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    "pick: score table for req={}\n{}",
                    req.label,
                    crate::router::picker::format_score_table(
                        &inputs,
                        expected_overlap_alpha,
                        Some(&chosen.endpoint),
                    )
                );
            }
            tracing::info!(
                req = %req.label,
                model = %model,
                chosen_endpoint = %chosen.endpoint,
                chosen_name = %chosen.name,
                scored = scores.len(),
                "pick"
            );

            let chosen_idx = scores
                .iter()
                .position(|s| s.endpoint == chosen.endpoint)
                .expect("chosen worker must be in scores");
            let chosen_inflight = inflight_per_worker[chosen_idx];
            let chosen_kv_blocks = kv_blocks_per_worker[chosen_idx];
            let chosen_workload: &[kinference_fleet::inflight::InflightSummary] =
                &inflight_workload_per_worker[chosen_idx];
            let chosen_q_breakdown = crate::router::picker::compute_queue_cost(
                chosen,
                predictor_cfg,
                chosen_workload,
                score_now,
            );
            let chosen_overlap_per_dispatch: Vec<u32> = per_worker_dispatch_overlap[chosen_idx]
                .iter()
                .map(|f| *f as u32)
                .collect();
            let prediction = crate::router::picker::predicted_wait_ms(
                chosen,
                predictor_cfg,
                chosen_workload,
                score_now,
            );
            // Prefix-cache outcome for the pick we actually made. Skipped when
            // the pick was degraded: a tokenize failure would look like a
            // genuine 0% cache hit, and a prefix-index failure can report
            // matched blocks against a zero-length prompt. Those picks are
            // already counted by `kinference_score_degraded_total{reason}`.
            //
            // `chosen.matched_blocks` is the layered value (it was overwritten
            // above with radix match plus in-flight prefill credit), while
            // `chosen_kv_blocks` is the raw tree match for the same pick.
            if degraded_reason.is_none() {
                self.prom.record_prefix_match(
                    &model,
                    req.qos.as_label(),
                    chosen.total_blocks,
                    chosen.matched_blocks,
                    chosen_kv_blocks,
                );
            }

            let chosen_clone = chosen.clone();
            pending = PendingObservation {
                request_id: req.request_id,
                workerset: pool.name.clone(),
                profile: pool.profile.clone(),
                inflight: crate::router::observations::InflightHandles::of(&pool),
                picked_at: score_now,
                chosen_worker: chosen_clone,
                prediction,
                q_inflight: chosen_inflight,
                kv_blocks: chosen_kv_blocks,
                inflight_workload_blocks: chosen_q_breakdown.inflight_workload_blocks,
                q_ms_constant: chosen_q_breakdown.q_ms_constant,
                q_ms_workload: chosen_q_breakdown.q_ms_workload,
                inflight_overlap_blocks: chosen_overlap_per_dispatch,
                expected_overlap_alpha,
                predictor_cfg: predictor_cfg.clone(),
                picker_cfg: picker_cfg.clone(),
                external_request_id: req.client_request_id.map(|s| s.to_string()),
                replication_fired,
                model: model.clone(),
                streaming,
                qos: req.qos,
                tenant: req.identity.tenant.clone(),
            };
            // For ReportFinalToken; the record above is consumed earlier, at
            // ReportFirstToken.
            self.observations.labels.insert(
                req.request_id,
                RequestMeta {
                    model: model.clone(),
                    qos: req.qos,
                    tenant: req.identity.tenant.clone(),
                    streaming,
                    picked_at: score_now,
                    // A local pick, so there is no provider to attribute
                    // feedback to.
                    provider: None,
                },
            );

            if inflight_enabled {
                inflight.inc(&chosen.endpoint);
            }
            let chosen_hashes_u64 = score_side
                .hashes_by_block_size
                .get(&chosen.block_size)
                .cloned()
                .unwrap_or_default();
            let chosen_hashes_set: std::collections::HashSet<u64> =
                chosen_hashes_u64.into_iter().collect();
            let chosen_uncached = chosen.total_blocks.saturating_sub(chosen.matched_blocks);
            pool.inflight_prefixes.insert(
                req.request_id,
                chosen.endpoint.clone(),
                chosen_hashes_set,
                chosen_uncached,
                chosen.total_blocks,
            );

            chosen_endpoint = chosen.endpoint.clone();
        }

        self.observations.pending.insert(pending);
        // The bare name, as `backend.name` spells it; a CRD worker set's
        // `pool.name` is `namespace/name`.
        self.prom.record_routed(
            &req.identity.tenant,
            &pool.key.name,
            kinference_policy::crd::BackendType::Local,
        );

        match degraded_reason {
            Some(reason) => PickAndRecordOutcome::Degraded {
                endpoint: chosen_endpoint,
                response_id: req.request_id,
                reason,
                body: wire.into_bytes(),
                model,
            },
            None => PickAndRecordOutcome::Ok {
                endpoint: chosen_endpoint,
                response_id: req.request_id,
                body: wire.into_bytes(),
                model,
            },
        }
    }

    /// Step 1. `Err` is a `BadRequest`: not JSON, not an object, or no
    /// `model`.
    fn parse(req: &PickRequest<'_>) -> Result<Parsed, PickAndRecordOutcome> {
        // A JSON tree, rewritten in place later (see `crate::router::pick_body`).
        // Not a typed struct, which would drop unknown fields.
        let obj: serde_json::Value = match serde_json::from_slice(req.body) {
            Ok(v) => v,
            Err(e) => {
                return Err(PickAndRecordOutcome::BadRequest {
                    reason: format!("body is not a valid OpenAI ChatCompletionRequest: {e}"),
                    model: String::new(),
                });
            }
        };
        if !obj.is_object() {
            return Err(PickAndRecordOutcome::BadRequest {
                reason: "body is not a valid OpenAI ChatCompletionRequest: expected a JSON object"
                    .to_string(),
                model: String::new(),
            });
        }

        // The body is the only source for `model`.
        let model = obj.get("model").and_then(|v| v.as_str()).unwrap_or("");
        if model.is_empty() {
            return Err(PickAndRecordOutcome::BadRequest {
                reason: "request body has an empty or absent `model`".to_string(),
                model: String::new(),
            });
        }
        let messages = obj
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let facts = BodyFacts {
            // Whether the first-byte report will be a real TTFT, for
            // `Observations::finalize`. The same coercion as the body rewrite;
            // see [`crate::router::pick_body::stream_flag`].
            streaming: crate::router::pick_body::stream_flag(&obj),
            // For `maxTokens` predicates and context-window fitting.
            max_tokens: obj.get("max_tokens").and_then(serde_json::Value::as_i64),
            classifier_text: kinference_policy::classify::last_user_text(messages).or_else(|| {
                obj.get("prompt")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            }),
        };
        Ok(Parsed {
            model: model.to_string(),
            facts,
            obj,
        })
    }

    /// Step 2. Resolve `auto` or a declared alias to the upstream model and
    /// the pools serving it, write it into the body, and take the chat inputs
    /// out of the tree. `Err` is `NoWorkers`: no model known for `auto`, or
    /// no pool and no provider serves the model.
    fn resolve_model(
        &self,
        req: &PickRequest<'_>,
        parsed: Parsed,
    ) -> Result<Resolved, PickAndRecordOutcome> {
        let Parsed {
            mut obj,
            mut model,
            facts,
        } = parsed;

        // `auto`: choose a concrete model, then route normally. `force_model`
        // drives the body rewrite below. With no models known yet, fall through
        // to NoWorkers.
        let mut force_model: Option<String> = None;
        if model == AUTO_MODEL {
            let messages = obj.get("messages").and_then(|v| v.as_array());
            match self
                .scoring
                .choose_model_for_auto(messages.map(|m| m.as_slice()).unwrap_or(&[]))
            {
                Some(chosen) => {
                    tracing::info!(
                        req = %req.label,
                        chosen_model = %chosen,
                        "auto: resolved pseudo-model to concrete model"
                    );
                    model = chosen.clone();
                    force_model = Some(chosen);
                }
                None => {
                    tracing::info!(
                        req = %req.label,
                        "auto: no candidate models available yet (cold start?)"
                    );
                    return Err(PickAndRecordOutcome::NoWorkers { model });
                }
            }
        }

        // Several pools may serve the model; they are scored and ranked against
        // each other.
        let mut serving = self.scoring.resolve_pools(&model);

        // Alias fallback. The model index only holds names engines report, so
        // translate a declared alias via the named pool's worker sets (requires
        // policy and a named pool).
        //
        // Limitation: takes the first served upstream, so interchangeable
        // upstream models always route to the same one rather than being
        // ranked by the policy.
        if serving.is_empty()
            && let Some(upstream) = self.resolve_alias(req.pool_ref, &model)
        {
            tracing::info!(
                req = %req.label,
                alias = %model,
                upstream_model = %upstream,
                "pick: resolved declared alias to an upstream model"
            );
            // Drives the body rewrite below, as for `auto`.
            force_model = Some(upstream.clone());
            model = upstream;
            serving = self.scoring.resolve_pools(&model);
        }

        // A model no local pool serves may still be served by a provider in
        // the named pool, so check before refusing.
        if serving.is_empty() {
            if !self.provider_serves(req.pool_ref, &model) {
                tracing::info!(
                    req = %req.label,
                    model = %model,
                    "pick: no pool serves model (unknown model or cold-start before /v1/models)"
                );
                return Err(PickAndRecordOutcome::NoWorkers { model });
            }
            tracing::debug!(
                req = %req.label,
                model = %model,
                "pick: no local worker set serves this model, but a provider declares it"
            );
        }

        // The Pick-time body rewrites: the resolved model, and
        // `stream_options.include_usage` for streaming requests. Only a changed
        // body is re-serialized.
        let mutated =
            crate::router::pick_body::apply_pick_body_mutations(&mut obj, force_model.as_deref());
        let body = WireBody {
            rewritten: mutated.then(|| {
                serde_json::to_vec(&obj).expect("re-serializing a parsed JSON body cannot fail")
            }),
        };

        // The chat-template inputs, taken out of the tree now that it is
        // serialized. `/render` gets the outbound bytes so it sees what the
        // engine will.
        let messages = match obj.get_mut("messages").map(serde_json::Value::take) {
            Some(serde_json::Value::Array(v)) => v,
            _ => Vec::new(),
        };
        let tools = match obj.get_mut("tools").map(serde_json::Value::take) {
            Some(serde_json::Value::Array(v)) => v,
            _ => Vec::new(),
        };
        let tool_choice = obj
            .get_mut("tool_choice")
            .map(serde_json::Value::take)
            .filter(|v| !v.is_null());
        let add_generation_prompt = obj
            .get("add_generation_prompt")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let chat_extras = ChatExtras {
            tools,
            tool_choice,
            add_generation_prompt,
        };
        let inputs = ScoreInput {
            input: Input::Messages(messages),
            chat_extras,
            raw_body: Some(body.bytes(req.body).to_vec()),
        };
        Ok(Resolved {
            model,
            serving,
            body,
            facts,
            inputs,
        })
    }

    /// Release everything held for `response_id` because the request will never
    /// complete. Backs the `Abort` RPC.
    ///
    /// Returns `(released, bytes_released)`. `released` is false only for an
    /// unknown id or a duplicate abort; never an error, since `Abort` is
    /// idempotent.
    ///
    /// Releases the inflight dispatch for both abort reasons, since after
    /// `PULL_FAILED` no `ReportFirstToken` is coming either.
    pub(crate) fn abort_request(&self, response_id: u64) -> (bool, u64) {
        let (body_released, bytes) = self.bodies.store.abort(response_id);
        let obs_released = self.observations.abort(response_id);
        self.observations.labels.take(response_id);
        (body_released || obs_released, bytes)
    }
}
