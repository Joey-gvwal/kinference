//! Tokenization for `score_workers`, as methods on
//! [`crate::router::service::KinferenceService`]. One mode per pool profile:
//! - `Render`: vLLM's `/render`, rotating across workers.
//! - `Local`: in-process tokenization, falling back to `/render`.
//! - `Compare`: `/render` is authoritative; the local result is compared in
//!   the background.
//!
//! `render_with_fallback` dispatches and records latency and failures once
//! for every path.

use std::collections::HashMap;
use std::time::Instant;

use serde::Deserialize;

use crate::router::service::KinferenceService;
use kinference_fleet::profile::TokenizeMode;
use kinference_fleet::workers::WorkerEntry;

pub mod http;
pub mod local;
#[cfg(test)]
pub(crate) mod test_support;

use self::local::LocalTokenizeConfig;

/// `tokenize`: tokenization settings shared by all pools. The mode is set
/// per profile.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TokenizeConfig {
    /// Idle keep-alive connections kept per host. Set it at or above peak
    /// concurrency when all renders go to a single `render_url`. `None` means
    /// 16.
    #[serde(default)]
    pub pool_max_idle_per_host: Option<usize>,
    #[serde(default)]
    pub local: LocalTokenizeConfig,
}

/// Map of `chat_template_kwargs` forwarded into the engine's
/// `/render` body. Values are JSON-encoded: `"false"` becomes a bool,
/// `"1024"` an int, and a non-JSON value like `en` stays a string.
pub type ChatTemplateKwargs = HashMap<String, String>;

/// What to tokenize. Carries everything vLLM's `_preprocess_chat` needs to
/// produce the same token sequence the engine would.
///
/// `Clone` because compare mode re-tokenizes it in a background task.
#[derive(Clone)]
pub enum TokenizeInput {
    /// Raw completion-style prompt → `/v1/completions/render` with
    /// `{ model, prompt }`, with vLLM's default `add_special_tokens=true`.
    Raw(String),
    /// Chat-style messages (full OpenAI array) →
    /// `/v1/chat/completions/render` with `{ model, messages, tools,
    /// tool_choice, add_generation_prompt, chat_template_kwargs }`.
    Chat {
        messages: Vec<serde_json::Value>,
        kwargs: ChatTemplateKwargs,
        /// OpenAI `tools` array (JSON-schema dicts). Empty when absent.
        tools: Vec<serde_json::Value>,
        /// OpenAI `tool_choice` ("auto" | "none" | "required" | {...}).
        /// `None` when unset.
        tool_choice: Option<serde_json::Value>,
        /// OpenAI `add_generation_prompt` (default `true`); `false` for
        /// assistant-prefill continuations.
        add_generation_prompt: bool,
    },
}

/// Result of tokenizing one request. `Unavailable` is not an error: scoring
/// degrades to load-only.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TokenizeOutcome {
    Ok(Vec<u32>),
    Unavailable { reason: String, attempts: usize },
}

/// One request to tokenize, against the model's worker pool.
pub(crate) struct TokenizeRequest<'a> {
    /// A dedicated `/render` URL; `None` rotates across `workers`.
    pub render_url: Option<&'a str>,
    pub workers: &'a [(u64, WorkerEntry)],
    pub model: &'a str,
    pub input: TokenizeInput,
    /// The client's body, forwarded verbatim to `/render` when present;
    /// local tokenization uses `input`.
    pub raw_body: Option<&'a [u8]>,
    /// For log context only.
    pub req_id: Option<&'a str>,
}

impl KinferenceService {
    /// Tokenize against the model's worker pool, in the pool's mode.
    pub(crate) async fn render_with_fallback(
        &self,
        mode: TokenizeMode,
        req: TokenizeRequest<'_>,
    ) -> TokenizeOutcome {
        let started = Instant::now();

        // Latency and unavailability are recorded here, once, for every
        // mode. Without a raw body, `/render` gets a synthesized one.
        let outcome = match mode {
            TokenizeMode::Render => self.http.render(&req).await,
            TokenizeMode::Local => self.tokenize_local_then_http(&req).await,
            TokenizeMode::Compare => self.tokenize_via_http_with_compare(&req).await,
        };

        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        let unavailable = matches!(outcome, TokenizeOutcome::Unavailable { .. });
        self.prom.record_tokenize_duration(latency_ms);
        if unavailable {
            // Successful paths are counted where they succeed.
            self.prom.record_tokenize_path("unavailable");
        }

        outcome
    }

    /// `Local` mode: in-process tokenization, falling back to `/render` on
    /// any local failure (unknown model, missing chat template, render or
    /// encode error).
    async fn tokenize_local_then_http(&self, req: &TokenizeRequest<'_>) -> TokenizeOutcome {
        let &TokenizeRequest {
            model,
            ref input,
            req_id,
            ..
        } = req;
        let local_reg = &self.local_tokenizer;
        let chat_kwargs = match input {
            TokenizeInput::Chat { kwargs, .. } => kwargs.clone(),
            TokenizeInput::Raw(_) => std::collections::HashMap::new(),
        };
        let started = std::time::Instant::now();
        let result = local_reg.tokenize(model, input, &chat_kwargs).await;
        let render_ms = started.elapsed().as_secs_f64() * 1000.0;
        match result {
            Ok(tokens) => {
                self.prom.record_tokenize_path("local");
                tracing::debug!(
                    req = %req_id.unwrap_or("-"),
                    mode = "local",
                    model = %model,
                    render_ms = format_args!("{render_ms:.1}"),
                    token_count = tokens.len(),
                    "render ok"
                );
                TokenizeOutcome::Ok(tokens)
            }
            Err(e) => {
                // Some models fail locally on every request (e.g. no
                // chat_template), so warn once per model.
                if self
                    .local_tokenize_warn_latch
                    .insert(model.to_string(), ())
                    .is_none()
                {
                    tracing::warn!(
                        model,
                        error = %e,
                        "local tokenize failed; falling back to HTTP /render"
                    );
                }
                let outcome = self.http.render(req).await;
                // A double failure is counted by `render_with_fallback`.
                if matches!(outcome, TokenizeOutcome::Ok(_)) {
                    self.prom.record_tokenize_path("http_fallback");
                }
                outcome
            }
        }
    }

    /// `Compare` mode: `/render` is returned; the local result is only
    /// compared against it, for the divergence counters.
    async fn tokenize_via_http_with_compare(&self, req: &TokenizeRequest<'_>) -> TokenizeOutcome {
        let outcome = self.http.render(req).await;
        if let TokenizeOutcome::Ok(http_tokens) = &outcome {
            // In the background, so local template rendering doesn't add to
            // pick latency.
            let local_reg = self.local_tokenizer.clone();
            let prom = self.prom.clone();
            let model_owned = req.model.to_string();
            let input_owned = req.input.clone();
            let http_tokens_owned: Vec<u32> = http_tokens.clone();
            tokio::spawn(async move {
                Self::compare_local_tokenize_inner(
                    &local_reg,
                    &prom,
                    &model_owned,
                    &input_owned,
                    &http_tokens_owned,
                )
                .await;
            });
        }
        outcome
    }

    /// Tokenize in-process and count and log divergence from the `/render`
    /// tokens. Errors are counted, never returned. No `&self`, so it can run
    /// in a spawned task.
    async fn compare_local_tokenize_inner(
        local_reg: &local::LocalTokenizerRegistry,
        prom: &crate::router::metrics::RouterMetrics,
        model: &str,
        input: &TokenizeInput,
        http_tokens: &[u32],
    ) {
        // Only `Chat` carries template kwargs.
        let chat_kwargs = match input {
            TokenizeInput::Chat { kwargs, .. } => kwargs.clone(),
            TokenizeInput::Raw(_) => std::collections::HashMap::new(),
        };
        match local_reg.tokenize(model, input, &chat_kwargs).await {
            Ok(local_tokens) if local_tokens.as_slice() == http_tokens => {
                prom.record_tokenize_compare("match");
            }
            Ok(local_tokens) => {
                prom.record_tokenize_compare("divergence");
                let first_diff = http_tokens
                    .iter()
                    .zip(local_tokens.iter())
                    .position(|(a, b)| a != b)
                    .unwrap_or_else(|| http_tokens.len().min(local_tokens.len()));
                // 8 tokens before to 16 after the divergence, on both sides.
                let win_start = first_diff.saturating_sub(8);
                let http_win: Vec<u32> = http_tokens
                    .iter()
                    .skip(win_start)
                    .take(24)
                    .copied()
                    .collect();
                let local_win: Vec<u32> = local_tokens
                    .iter()
                    .skip(win_start)
                    .take(24)
                    .copied()
                    .collect();
                let http_text = local_reg
                    .decode(model, &http_win)
                    .unwrap_or_else(|| "<decode-unavailable>".to_string());
                let local_text = local_reg
                    .decode(model, &local_win)
                    .unwrap_or_else(|| "<decode-unavailable>".to_string());
                tracing::warn!(
                    model,
                    http_count = http_tokens.len(),
                    local_count = local_tokens.len(),
                    first_diff_index = first_diff,
                    window_start = win_start,
                    http_window = ?http_win,
                    local_window = ?local_win,
                    http_text = %http_text,
                    local_text = %local_text,
                    "local_tokenize divergence — in-process result differs from /render"
                );
            }
            Err(e) => {
                prom.record_tokenize_compare("error");
                tracing::warn!(
                    model,
                    error = %e,
                    "local_tokenize failed; HTTP result returned unchanged"
                );
            }
        }
    }
}
