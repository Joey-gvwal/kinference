//! Scoring — turning a pool's live workers into a ranked `ScoreResponse`.
//!
//! This is the arithmetic half of a pick: it reads the metrics registry and
//! the KV index, projects each worker onto a `picker::WorkerScore`, and hands
//! the result to `crate::picker` to rank. It performs no I/O of its own and
//! makes no routing decision — `lifecycle` decides what to do with the ranking.
//!
//! `degraded_response` is the same path with the prefix-cache trio zeroed, for
//! when tokenize is unavailable. Both build their scores through
//! `super::worker_score`, which is the single place that projection happens;
//! they used to write it out separately, and a field added to one but not the
//! other was silently zero on the outage path alone.

use super::*;

impl KinferenceService {
    /// Build a degraded `ScoreResponse` when tokenize is unavailable.
    /// Live load metadata (queue depth, KV cache usage, served models,
    /// completion rate, etc.) is populated from `MetricsRegistry` exactly
    /// as it would be on the success path — only the prefix-cache trio
    /// (`token_count`, `total_blocks`, `matched_blocks`) is zeroed.
    /// Callers can still route on load; only the cache-aware part of
    /// the decision is unavailable.
    pub(crate) fn degraded_response(&self, workers: &[(u64, WorkerEntry)]) -> ScoreResponse {
        let mut scores = Vec::with_capacity(workers.len());
        for (num, entry) in workers {
            let sample = self.metrics.latest(*num);
            // The cache trio is zeroed because tokenize is unavailable; the
            // load fields are as real here as on the success path.
            scores.push(worker_score(entry, &sample, entry.block_size, 0, 0));
        }
        ScoreResponse { scores }
    }

    /// Core scoring engine — the shared body of work that the gRPC
    /// pick path calls.
    ///
    /// `chat_kwargs` is consumed when `input` is Chat / Messages; it's
    /// ignored for Raw (no template) and Tokens (no /tokenize call).
    ///
    /// `record_call_telemetry` gates the `record_call` row push into the
    /// telemetry rings. The gRPC handler sets this to `true` (gRPC calls
    /// are first-class events in the existing dashboard); the pick path
    /// sets it to `false` because picks have their own dedicated
    /// counters (see `picks.*` in the snapshot).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn score_workers(
        &self,
        pool: &Pool,
        model: &str,
        input: Input,
        chat_kwargs: HashMap<String, String>,
        chat_extras: ChatExtras,
        raw_body: Option<Vec<u8>>,
        record_call_telemetry: bool,
        req_id: Option<&str>,
    ) -> (ScoreOutcome, ScoreSideData) {
        let started = Instant::now();
        let _inflight = self.telemetry.inflight_guard();
        let mut side = ScoreSideData {
            tokens: Vec::new(),
            hashes_by_block_size: HashMap::new(),
        };

        // 1. Resolve the workers serving this model. Filter to
        //    workers that have resolved enough state to be picked:
        //    block_size known + /v1/models scraped + initial event-
        //    stream sync done. Picking against an unready worker
        //    computes block hashes at the wrong block_size and
        //    silently misroutes, triggering the tokenize-divergence
        //    warning. See `WorkerEntry::is_ready` for the per-signal
        //    rationale. When NO ready workers serve the model, we
        //    return `NoWorkers` (transient
        //    during cold-start until at least one worker readies).
        let workers: Vec<_> = pool
            .registry
            .workers_for_model(model)
            .into_iter()
            .filter(|(_, w)| w.is_ready())
            .collect();
        if workers.is_empty() {
            return (ScoreOutcome::NoWorkers, side);
        }

        // Destructure once: only one of the Chat/Messages arms below
        // fires per call, so move-by-arm is fine. Raw/Tokens arms
        // don't consume the extras (no chat template applied).
        let ChatExtras {
            tools,
            tool_choice,
            add_generation_prompt,
        } = chat_extras;

        // 2. Get tokens (pre-supplied, or via /tokenize against the
        //    worker pool). Tokenize NEVER returns a Status — total
        //    failures become `Unavailable` and we degrade instead.
        let token_source: TokenSource = match input {
            Input::Tokens(t) => TokenSource::Direct(t),
            Input::Raw(s) => TokenSource::NeedsTokenize(TokenizeInput::Raw(s)),
            Input::Chat(s) => TokenSource::NeedsTokenize(TokenizeInput::Chat {
                messages: vec![serde_json::json!({ "role": "user", "content": s })],
                kwargs: chat_kwargs,
                tools,
                tool_choice,
                add_generation_prompt,
            }),
            Input::Messages(messages) => {
                // Already parsed by the caller (`classify_input` for
                // gRPC) — just hand
                // it to the chat template path. Avoids a redundant
                // serialize → reparse round-trip on every request.
                TokenSource::NeedsTokenize(TokenizeInput::Chat {
                    messages,
                    kwargs: chat_kwargs,
                    tools,
                    tool_choice,
                    add_generation_prompt,
                })
            }
        };

        let tokens: Vec<u32> = match token_source {
            TokenSource::Direct(t) => t,
            TokenSource::NeedsTokenize(tok_input) => {
                match self
                    .render_with_fallback(
                        pool.tokenize_mode,
                        pool.tokenize_render_url.as_deref(),
                        &workers,
                        model,
                        tok_input,
                        raw_body.as_deref(),
                        req_id,
                    )
                    .await
                {
                    crate::tokenize_client::TokenizeOutcome::Ok(tokens) => tokens,
                    crate::tokenize_client::TokenizeOutcome::Unavailable { reason, attempts } => {
                        tracing::error!(
                            model = model,
                            attempts,
                            reason = %reason,
                            "tokenize unavailable across the fleet; returning degraded ScoreResponse"
                        );
                        let degraded = self.degraded_response(&workers);
                        if record_call_telemetry {
                            let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                            let overlaps: Vec<(u64, f64)> =
                                workers.iter().map(|(num, _)| (*num, 0.0)).collect();
                            self.telemetry.record_call(CallSample {
                                model,
                                tokens: 0,
                                latency_ms,
                                overlaps: &overlaps,
                                top: None,
                            });
                        }
                        return (
                            ScoreOutcome::Unavailable(degraded, Degraded::Tokenize),
                            side,
                        );
                    }
                }
            }
        };
        let token_count = tokens.len() as u32;
        // Surface the tokens so a caller reaching the same MODEL across several
        // pools can re-score via `Input::Tokens` instead of re-tokenizing. The
        // tokenizer and chat template belong to the model; only the hashing and
        // the index query below are per-pool.
        side.tokens = tokens.clone();

        // 3. Group workers by block_size, score each group separately.
        let mut by_block: HashMap<u32, Vec<(u64, &WorkerEntry)>> = HashMap::new();
        for w in &workers {
            by_block
                .entry(w.1.block_size)
                .or_default()
                .push((w.0, &w.1));
        }

        let mut out: HashMap<u64, WorkerScore> = HashMap::new();
        for (block_size, group) in by_block {
            let hashes =
                compute_block_hash_for_seq(&tokens, block_size, BlockHashOptions::default());
            let total_blocks = hashes.len() as u32;

            // Snapshot the hashes as u64 for the side-channel before
            // `find_matches` consumes the LocalBlockHash vec. The pick
            // path uses this to seed `InflightPrefixes` after pick and
            // to compute layered expected-overlap inside `pick_lock`;
            // the gRPC `Score` handler discards it.
            let hashes_u64: Vec<u64> = hashes.iter().map(|h| h.0).collect();

            tracing::debug!(
                req = %req_id.unwrap_or("-"),
                block_size,
                total_blocks,
                first_tokens = ?tokens.iter().take(8).collect::<Vec<_>>(),
                first_hashes = ?hashes_u64.iter().take(4).collect::<Vec<_>>(),
                "score: computed block hashes"
            );

            side.hashes_by_block_size.insert(block_size, hashes_u64);

            let overlap = match pool.indexer.find_matches(hashes).await {
                Ok(o) => o,
                Err(e) => {
                    // Internal indexer failure is rare and the caller can't
                    // surface a gRPC error cleanly, so degrade and let the
                    // picker work on load metrics alone. Reported as its OWN
                    // reason: the tokens were fine, this pool's radix tree
                    // wasn't, and telling the operator to look at the engine
                    // would send them to the wrong place.
                    tracing::error!(
                        pool = %pool.name,
                        block_size,
                        error = %e,
                        "prefix index query failed; degrading this pick to load-only"
                    );
                    let degraded = self.degraded_response(&workers);
                    return (
                        ScoreOutcome::Unavailable(degraded, Degraded::PrefixIndex),
                        side,
                    );
                }
            };

            let mut matched: HashMap<u64, u32> = HashMap::new();
            for (k, v) in overlap.scores {
                *matched.entry(k.worker_id).or_insert(0) += v;
            }

            for (num, entry) in &group {
                let sample = self.metrics.latest(*num);

                // Raw radix-tree match count. `WorkerScore.matched_blocks`
                // is the unlayered kv_match here; the pick path applies
                // layered expected-overlap later (inside `pick_lock`,
                // against a consistent `InflightPrefixes` snapshot).
                // The gRPC `Score` endpoint returns this raw value too
                // — external scorers (e.g. the gw-pc WASM plugin) do
                // their own pick logic without our pending tracker.
                let kv_match = matched.get(num).copied().unwrap_or(0);

                out.insert(
                    *num,
                    worker_score(entry, &sample, block_size, kv_match, total_blocks),
                );
            }
        }

        // 4. Feed telemetry: per-worker overlap fractions + the top-overlap
        //    worker (the highest row of the response — NOT a routing
        //    decision; the routing decision is made by the caller).
        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        let mut overlaps: Vec<(u64, f64)> = Vec::with_capacity(out.len());
        let mut top: Option<TopWorker> = None;
        for (num, ws) in &out {
            let overlap = if ws.total_blocks > 0 {
                ws.matched_blocks as f64 / ws.total_blocks as f64
            } else {
                0.0
            };
            overlaps.push((*num, overlap));
            if top.as_ref().map(|t| overlap > t.overlap).unwrap_or(true) {
                top = Some(TopWorker {
                    worker_id_num: *num,
                    addr: ws.name.clone(),
                    overlap,
                    matched: ws.matched_blocks,
                    total: ws.total_blocks,
                });
            }
        }
        if record_call_telemetry {
            self.telemetry.record_call(CallSample {
                model,
                tokens: token_count,
                latency_ms,
                overlaps: &overlaps,
                top,
            });
        }

        let scores: Vec<WorkerScore> = out.into_values().collect();
        (ScoreOutcome::Ok(ScoreResponse { scores }), side)
    }
}
