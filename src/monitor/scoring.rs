//! Scoring: turning a pool's live workers into `WorkerScore` rows.
//!
//! Reads the metrics registry and the KV index and projects each worker via
//! `super::worker_score`. Makes no routing decision; `lifecycle` does.
//! `degraded_response` is the same with the prefix-cache values zeroed.

use super::*;

impl KinferenceService {
    /// Build a degraded `ScoreResponse` when tokenize is unavailable: load
    /// fields as usual, `total_blocks` and `matched_blocks` zeroed.
    pub(crate) fn degraded_response(&self, workers: &[(u64, WorkerEntry)]) -> ScoreResponse {
        let mut scores = Vec::with_capacity(workers.len());
        for (num, entry) in workers {
            let sample = self.metrics.latest(*num);
            scores.push(worker_score(entry, &sample, entry.block_size, 0, 0));
        }
        ScoreResponse { scores }
    }

    /// Score one pool's workers for one model.
    ///
    /// `chat_kwargs` applies to Chat and Messages input only.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn score_workers(
        &self,
        pool: &Pool,
        model: &str,
        input: Input,
        chat_kwargs: HashMap<String, String>,
        chat_extras: ChatExtras,
        raw_body: Option<Vec<u8>>,
        req_id: Option<&str>,
    ) -> (ScoreOutcome, ScoreSideData) {
        let mut side = ScoreSideData {
            tokens: Vec::new(),
            hashes_by_block_size: HashMap::new(),
        };

        // 1. Ready workers serving this model (see `WorkerEntry::is_ready`).
        //    None means `NoWorkers`, typically during cold start.
        let workers: Vec<_> = pool
            .registry
            .workers_for_model(model)
            .into_iter()
            .filter(|(_, w)| w.is_ready())
            .collect();
        if workers.is_empty() {
            return (ScoreOutcome::NoWorkers, side);
        }

        let ChatExtras {
            tools,
            tool_choice,
            add_generation_prompt,
        } = chat_extras;

        // 2. Get tokens, pre-supplied or by tokenizing. Total failure
        //    degrades rather than erroring.
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
                // Already parsed by the caller.
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
                        return (
                            ScoreOutcome::Unavailable(degraded, Degraded::Tokenize),
                            side,
                        );
                    }
                }
            }
        };
        // So callers scoring the same model in other pools needn't re-tokenize.
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

            // Keep the hashes for the pick path before `find_matches` consumes
            // them.
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
                    // Degrade to load-only, reported as a prefix-index (not
                    // tokenize) failure.
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

                // Raw radix-tree match; the pick path layers in expected
                // overlap later, inside `pick_lock`.
                let kv_match = matched.get(num).copied().unwrap_or(0);

                out.insert(
                    *num,
                    worker_score(entry, &sample, block_size, kv_match, total_blocks),
                );
            }
        }

        let scores: Vec<WorkerScore> = out.into_values().collect();
        (ScoreOutcome::Ok(ScoreResponse { scores }), side)
    }
}
