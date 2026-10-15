//! The request lifecycle behind `Pick()`: one request from body to chosen
//! endpoint, and the observation it leaves behind.
//!
//! `pick_and_record` is long because it is one transaction over a JSON body
//! that is rewritten and then drained, plus a critical section under the
//! pool's `pick_lock`. `finalize_observation` and `abort_observation` release
//! what it reserved, and the sweeps are the backstop for callers that never
//! report.

use std::time::Instant;

use super::*;
use crate::observations::PendingObservation;

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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn pick_and_record(
        &self,
        body: &[u8],
        request_id: u64,
        client_request_id: Option<&str>,
        qos: Qos,
        pool_ref: Option<&kinference_policy::PoolRef>,
        identity: &kinference_policy::RequestIdentity,
    ) -> PickAndRecordOutcome {
        let req_label = client_request_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| request_id.to_string());

        // 1. Parse the body into a JSON tree, rewritten in place (see
        //    `crate::router::pick_body`). Not a typed struct, which would drop unknown
        //    fields.
        let mut obj: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return PickAndRecordOutcome::BadRequest {
                    reason: format!("body is not a valid OpenAI ChatCompletionRequest: {e}"),
                    model: String::new(),
                };
            }
        };
        if !obj.is_object() {
            return PickAndRecordOutcome::BadRequest {
                reason: "body is not a valid OpenAI ChatCompletionRequest: expected a JSON object"
                    .to_string(),
                model: String::new(),
            };
        }

        // The body is the only source for `model`.
        let body_model = obj.get("model").and_then(|v| v.as_str()).unwrap_or("");
        if body_model.is_empty() {
            return PickAndRecordOutcome::BadRequest {
                reason: "request body has an empty or absent `model`".to_string(),
                model: String::new(),
            };
        }
        let mut model = body_model.to_string();

        // 1a. "auto" pseudo-model: choose a concrete model, then route
        //     normally. `force_model` drives the body rewrite in step 1c. With
        //     no models known yet, fall through to NoWorkers.
        let mut force_model: Option<String> = None;
        if model == AUTO_MODEL {
            let messages = obj.get("messages").and_then(|v| v.as_array());
            match self.choose_model_for_auto(messages.map(|m| m.as_slice()).unwrap_or(&[])) {
                Some(chosen) => {
                    tracing::info!(
                        req = %req_label,
                        chosen_model = %chosen,
                        "auto: resolved pseudo-model to concrete model"
                    );
                    model = chosen.clone();
                    force_model = Some(chosen);
                }
                None => {
                    tracing::info!(
                        req = %req_label,
                        "auto: no candidate models available yet (cold start?)"
                    );
                    return PickAndRecordOutcome::NoWorkers { model };
                }
            }
        }

        // 1b. Resolve the pools serving this model; several are scored and
        //     ranked against each other.
        let mut serving = self.resolve_pools(&model);

        // 1b-i. Alias fallback. `model_router` only holds names engines
        //     report, so translate a declared alias via the named pool's worker
        //     sets (requires policy and a named pool).
        //
        //     Limitation: takes the first served upstream, so interchangeable
        //     upstream models always route to the same one rather than being
        //     ranked by the policy.
        if serving.is_empty()
            && let Some(upstream) = self.resolve_alias(pool_ref, &model)
        {
            tracing::info!(
                req = %req_label,
                alias = %model,
                upstream_model = %upstream,
                "pick: resolved declared alias to an upstream model"
            );
            // Drives the body rewrite in step 1c, as for `auto`.
            force_model = Some(upstream.clone());
            model = upstream;
            serving = self.resolve_pools(&model);
        }

        // A model no local pool serves may still be served by a provider in
        // the named pool, so check before refusing.
        if serving.is_empty() {
            if !self.provider_serves(pool_ref, &model) {
                tracing::info!(
                    req = %req_label,
                    model = %model,
                    "pick: no pool serves model (unknown model or cold-start before /v1/models)"
                );
                return PickAndRecordOutcome::NoWorkers { model };
            }
            tracing::debug!(
                req = %req_label,
                model = %model,
                "pick: no local worker set serves this model, but a provider declares it"
            );
        }
        // Per-pool config and handles are bound only once the winning pool is
        // known, below.

        // 1c. Apply the Pick-time body rewrites (model="auto" → concrete,
        //     and stream_options.include_usage for streaming requests). When
        //     something changed, the re-serialized bytes are both sent to
        //     /render and returned; otherwise `wire_body` is empty.
        let mutated =
            crate::router::pick_body::apply_pick_body_mutations(&mut obj, force_model.as_deref());
        // Whether the first-byte report will be a real TTFT, for
        // `finalize_observation`. Uses the same coercion as the body rewrite;
        // see [`crate::router::pick_body::stream_flag`].
        let streaming = crate::router::pick_body::stream_flag(&obj);
        // For `maxTokens` predicates and context-window fitting.
        let parsed_max_tokens = obj.get("max_tokens").and_then(serde_json::Value::as_i64);
        let outbound = if mutated {
            serde_json::to_vec(&obj).expect("re-serializing a parsed JSON body cannot fail")
        } else {
            body.to_vec()
        };
        let mut wire_body = if mutated {
            outbound.clone()
        } else {
            Vec::new()
        };
        // The model `wire_body` currently names, in case a policy later picks
        // another.
        let body_model = model.clone();

        // 2. Score. Take the chat-template inputs out of the tree, and pass the
        //    outbound body so /render sees what the engine will.
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
        // Captured before scoring consumes `messages`, falling back to the
        // completions-API `prompt`.
        let classifier_text =
            kinference_policy::classify::last_user_text(&messages).or_else(|| {
                obj.get("prompt")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            });
        // Scoring consumes these, but a second pool or a policy may need them
        // again; only clone in that case.
        let policy_active = pool_ref.is_some() && self.policy.is_some();
        let spare = (policy_active || serving.len() > 1)
            .then(|| (messages.clone(), chat_extras.clone(), outbound.clone()));
        let mut degraded_reason: Option<String> = None;
        let mut groups: Vec<ScoredGroup> = Vec::new();
        // Skipped when nothing local serves the model.
        if let Some(first) = serving.first() {
            let (outcome, score_side) = self
                .score_workers(
                    first,
                    &model,
                    Input::Messages(messages),
                    HashMap::new(),
                    chat_extras,
                    Some(outbound),
                    Some(&req_label),
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
                        req = %req_label,
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
                let Some((msgs, extras, raw)) = spare.clone() else {
                    continue;
                };
                let (input, raw) = match reuse.clone() {
                    Some(t) => (Input::Tokens(t), None),
                    None => (Input::Messages(msgs), Some(raw)),
                };
                let (outcome, side) = self
                    .score_workers(
                        pool,
                        &model,
                        input,
                        HashMap::new(),
                        extras,
                        raw,
                        Some(&req_label),
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
        let policy_will_rank = pool_ref.is_some() && self.policy.is_some();
        let winner = best_group(
            groups,
            &req_label,
            (!policy_will_rank).then_some(&*self.prom),
        );
        if winner.is_none() && serving.is_empty() {
            // Normal for a provider-only model.
            tracing::debug!(
                req = %req_label,
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
        if let (Some(pref), Some(store)) = (pool_ref, self.policy.as_ref()) {
            let attrs = kinference_policy::predicate::RequestAttrs {
                // Upstream first, then its declared aliases.
                model: {
                    let mut names = vec![model.clone()];
                    names.extend(self.declared_aliases(pool_ref, &model));
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
                max_tokens: parsed_max_tokens,
                qos: Some(qos.as_label().to_string()),
                tenant: identity.tenant.clone(),
                // See `kinference_policy::classify`.
                tag: kinference_policy::classify::tags_for_request(
                    &req_label,
                    classifier_text.as_deref(),
                ),
            };
            match self
                .policy_select(pref, store, winner, &model, spare, attrs, &req_label)
                .await
            {
                // An off-cluster destination. Returns here, skipping the
                // pool-scoped critical section: rewrite the body to the vendor's
                // model name and hold an observation for the reported TTFT.
                Ok(PolicySelection::Provider(pick)) => {
                    let base = if wire_body.is_empty() {
                        body
                    } else {
                        &wire_body
                    };
                    let Some(rewritten) =
                        crate::router::pick_body::rewrite_model(base, &pick.upstream_model)
                    else {
                        tracing::error!(
                            req = %req_label,
                            from_model = %body_model,
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
                    };
                    tracing::info!(
                        req = %req_label,
                        provider = %pick.provider.label(),
                        backend = %pick.backend_ref.label(),
                        from_model = %body_model,
                        to_model = %pick.upstream_model,
                        "policy: routing off-cluster"
                    );
                    self.record_provider_pick(request_id, &pick, qos, &identity.tenant, streaming);
                    self.prom.record_routed(
                        &identity.tenant,
                        &pick.provider.name,
                        kinference_policy::crd::BackendType::Provider,
                    );
                    return PickAndRecordOutcome::Provider {
                        pick,
                        response_id: request_id,
                        body: rewritten,
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
                            req = %req_label,
                            model = %model,
                            "pick: no local group and no provider chosen"
                        );
                        return PickAndRecordOutcome::NoWorkers { model };
                    };
                    model = group.model.clone();
                    selected = Some(group);
                    forced_endpoint = chosen;

                    // The policy may have chosen a different model than the
                    // body names, so rewrite it. Not via `obj`, whose `messages`
                    // were taken for scoring.
                    if model != body_model {
                        let base = if wire_body.is_empty() {
                            body
                        } else {
                            &wire_body
                        };
                        match crate::router::pick_body::rewrite_model(base, &model) {
                            Some(rewritten) => {
                                tracing::debug!(
                                    req = %req_label,
                                    from_model = %body_model,
                                    to_model = %model,
                                    "policy: rewriting the body's model to the chosen one"
                                );
                                wire_body = rewritten;
                            }
                            None => {
                                // Unreachable for a body we already parsed once.
                                // Refuse rather than forward a body naming a
                                // model the chosen endpoint does not serve.
                                tracing::error!(
                                    req = %req_label,
                                    from_model = %body_model,
                                    to_model = %model,
                                    "policy: chose a different model but the body could not be \
                                     rewritten; refusing rather than sending a mismatched model"
                                );
                                return PickAndRecordOutcome::BadRequest {
                                    reason: "could not rewrite body model for the chosen candidate"
                                        .to_string(),
                                    model,
                                };
                            }
                        }
                    }
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
                req = %req_label,
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
                    req = %req_label,
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
            let dispatches_per_worker: Vec<Vec<crate::fleet::inflight::DispatchSnapshot>> = scores
                .iter()
                .map(|s| pool.inflight_prefixes.dispatches_for(&s.endpoint))
                .collect();
            let inflight_workload_per_worker: Vec<Vec<crate::fleet::inflight::InflightSummary>> =
                dispatches_per_worker
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
                        req = %req_label,
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
                None => match crate::router::picker::pick_endpoint(
                    &scores,
                    predictor_cfg,
                    picker_cfg,
                    &inflight_per_worker,
                    &inflight_workload_per_worker,
                    &kv_blocks_per_worker,
                    score_now,
                ) {
                    Some(r) => r,
                    None => {
                        tracing::warn!(
                            req = %req_label,
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
                    req_label,
                    crate::router::picker::format_score_table(
                        &scores,
                        predictor_cfg,
                        picker_cfg,
                        expected_overlap_alpha,
                        Some(&chosen.endpoint),
                        &inflight_per_worker,
                        &kv_blocks_per_worker,
                        &inflight_workload_per_worker,
                        score_now,
                    )
                );
            }
            tracing::info!(
                req = %req_label,
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
            let chosen_workload: &[crate::fleet::inflight::InflightSummary] =
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
                    qos.as_label(),
                    chosen.total_blocks,
                    chosen.matched_blocks,
                    chosen_kv_blocks,
                );
            }

            let chosen_clone = chosen.clone();
            pending = PendingObservation {
                request_id,
                workerset: pool.name.clone(),
                profile: pool.profile.clone(),
                inflight: crate::observations::InflightHandles::of(&pool),
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
                external_request_id: client_request_id.map(|s| s.to_string()),
                replication_fired,
                model: model.clone(),
                streaming,
                qos,
                tenant: identity.tenant.clone(),
            };
            // For ReportFinalToken; the record above is consumed earlier, at
            // ReportFirstToken.
            self.request_labels.insert(
                request_id,
                RequestMeta {
                    model: model.clone(),
                    qos,
                    tenant: identity.tenant.clone(),
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
                request_id,
                chosen.endpoint.clone(),
                chosen_hashes_set,
                chosen_uncached,
                chosen.total_blocks,
            );

            chosen_endpoint = chosen.endpoint.clone();
        }

        self.pending_observations.insert(pending);
        // The bare name, as `backend.name` spells it; a CRD worker set's
        // `pool.name` is `namespace/name`.
        self.prom.record_routed(
            &identity.tenant,
            &pool.key.name,
            kinference_policy::crd::BackendType::Local,
        );

        match degraded_reason {
            Some(reason) => PickAndRecordOutcome::Degraded {
                endpoint: chosen_endpoint,
                response_id: request_id,
                reason,
                body: wire_body,
                model,
            },
            None => PickAndRecordOutcome::Ok {
                endpoint: chosen_endpoint,
                response_id: request_id,
                body: wire_body,
                model,
            },
        }
    }

    /// Release the inflight counter and inflight-prefix entry a pick reserved
    /// for `record`. The inflight counter has no sweep of its own, so every
    /// terminal path for a pick (`finalize_observation`, `abort_observation`,
    /// `sweep_expired_observations`) must come through here.
    ///
    /// The pool handles ride on the record, so a pool torn down mid-request
    /// stays alive until its pending observations are released.
    fn release_inflight_state(&self, record: &PendingObservation) {
        // Pick-time config, from the record.
        if record.picker_cfg.inflight_tracking {
            record.inflight.inflight.dec(&record.chosen_worker.endpoint);
        }
        record.inflight.prefixes.forget(record.request_id);
    }

    /// Close an observation: release the pick's inflight state and, for a
    /// streaming request, write the CSV row. Called from `ReportFirstToken()`
    /// with the caller's observed TTFT.
    ///
    /// Returns `false`, changing nothing, when `request_id` is unknown (swept,
    /// never inserted, or already consumed).
    pub(crate) fn finalize_observation(
        &self,
        request_id: u64,
        observed_ttft_ms: u32,
        upstream_status: i32,
    ) -> bool {
        let Some(record) = self.pending_observations.take(request_id) else {
            return false;
        };
        // Always release in-flight bookkeeping, streaming or not.
        self.release_inflight_state(&record);

        let req_label_owned = record
            .external_request_id
            .clone()
            .unwrap_or_else(|| record.request_id.to_string());

        // For non-streaming responses `observed_ttft_ms` is the full response
        // time, not TTFT, so skip the CSV row and residual log.
        if !record.streaming {
            tracing::debug!(
                req = %req_label_owned,
                endpoint = %record.chosen_worker.endpoint,
                status = upstream_status,
                "observation skipped: non-streaming response (reported time is total latency, not TTFT)"
            );
            return true;
        }

        // Streaming: a real TTFT.
        self.prom.record_ttft(
            &record.model,
            record.qos.as_label(),
            &record.tenant,
            observed_ttft_ms,
        );

        let predicted_ms = record.prediction.total_ms();
        let residual_ms = observed_ttft_ms as f32 - predicted_ms;
        tracing::debug!(
            req = %req_label_owned,
            endpoint = %record.chosen_worker.endpoint,
            predicted_ms = format!("{predicted_ms:.1}"),
            observed_ms = observed_ttft_ms,
            residual_ms = format!("{residual_ms:+.1}"),
            status = upstream_status,
            "observation"
        );

        self.observation_sink.append_line(&format_csv_row(
            &record,
            observed_ttft_ms,
            residual_ms,
            upstream_status,
        ));
        true
    }

    /// Drop a `PendingObservation` without writing a CSV row.
    ///
    /// For a request that died before its first token: release the inflight
    /// state, but write no row. Returns `true` if a record was found.
    pub(crate) fn abort_observation(&self, request_id: u64) -> bool {
        let Some(record) = self.pending_observations.take(request_id) else {
            return false;
        };
        self.release_inflight_state(&record);
        let req_label_owned = record
            .external_request_id
            .clone()
            .unwrap_or_else(|| record.request_id.to_string());
        tracing::debug!(
            req = %req_label_owned,
            endpoint = %record.chosen_worker.endpoint,
            "abort_observation: cleared inflight/inflight-prefix state without CSV emit"
        );
        true
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
        let (body_released, bytes) = self.body_store.abort(response_id);
        let obs_released = self.abort_observation(response_id);
        self.request_labels.take(response_id);
        (body_released || obs_released, bytes)
    }

    /// Reclaim buffered bodies past their TTL, which is much shorter than the
    /// observation TTL. Returns `(entries, bytes)` freed.
    pub(crate) fn sweep_expired_bodies(&self) -> (usize, u64) {
        self.body_store.take_expired()
    }

    /// Evict pending observations past the TTL, releasing each one's inflight
    /// state as `abort_observation` would. The backstop for callers that never
    /// report. Returns the number evicted.
    pub(crate) fn sweep_expired_observations(&self) -> usize {
        let evicted = self.pending_observations.take_expired();
        for record in &evicted {
            self.release_inflight_state(record);
        }
        // Request labels whose ReportFinalToken never arrived.
        self.request_labels.take_expired();
        evicted.len()
    }
}
