//! Tokenization for `score_workers`, as methods on
//! [`crate::monitor::KinferenceService`]. One mode per pool profile:
//! - `Render`: vLLM's `/render`, rotating across workers.
//! - `Local`: in-process tokenization, falling back to `/render`.
//! - `Compare`: `/render` is authoritative; the local result is compared in
//!   the background.
//!
//! `render_with_fallback` dispatches and records latency and failures once
//! for every path.

use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::local_tokenize::LocalTokenizeConfig;
use crate::monitor::KinferenceService;
use crate::tokenize::{
    TokenizeInput, TokenizeMode, render_post_raw_at_url, render_structured_at_url,
    render_via_vllm_raw, render_via_vllm_structured,
};
use crate::workers::WorkerEntry;

/// `[tokenize]`: tokenization settings shared by all pools. The mode is set
/// per profile.
#[allow(dead_code)]
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

/// Maximum number of workers tried in sequence, freshest metrics first, so
/// an outage doesn't try every worker on every request.
const TOKENIZE_FALLBACK_MAX_ATTEMPTS: usize = 3;

/// Wall-clock cap on the whole attempt loop. The shared `reqwest::Client`
/// timeout bounds each attempt. When it elapses, scoring degrades (see
/// `degraded_response`).
const TOKENIZE_TOTAL_DEADLINE: Duration = Duration::from_secs(5);

/// Result of tokenizing one request. `Unavailable` is not an error: scoring
/// degrades to load-only.
pub(crate) enum TokenizeOutcome {
    Ok(Vec<u32>),
    Unavailable { reason: String, attempts: usize },
}

impl KinferenceService {
    /// Try to tokenize against the model's worker pool.
    ///
    /// Workers are tried freshest-scrape first (a free liveness proxy), up to
    /// [`TOKENIZE_FALLBACK_MAX_ATTEMPTS`], within [`TOKENIZE_TOTAL_DEADLINE`].
    /// Workers serving one model share its tokenizer, so any of them
    /// returns the same tokens.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn render_with_fallback(
        &self,
        mode: TokenizeMode,
        render_url: Option<&str>,
        workers: &[(u64, WorkerEntry)],
        model: &str,
        input: TokenizeInput,
        raw_body: Option<&[u8]>,
        req_id: Option<&str>,
    ) -> TokenizeOutcome {
        let started = Instant::now();

        // Latency and unavailability are recorded here, once, for every
        // mode. `raw_body` is only used for `/render`; local tokenization
        // uses the structured input. Without a raw body, `/render` gets a
        // synthesized one.
        let outcome = match mode {
            TokenizeMode::Render => {
                self.render_via_http(render_url, workers, model, &input, raw_body, req_id)
                    .await
            }
            TokenizeMode::Local => {
                self.tokenize_local_then_http(render_url, workers, model, &input, raw_body, req_id)
                    .await
            }
            TokenizeMode::Compare => {
                self.tokenize_via_http_with_compare(
                    render_url, workers, model, &input, raw_body, req_id,
                )
                .await
            }
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

    /// `/render` with retries across workers. Forwards `raw_body` verbatim
    /// when present, otherwise a body built from `input`.
    async fn render_via_http(
        &self,
        render_url: Option<&str>,
        workers: &[(u64, WorkerEntry)],
        model: &str,
        input: &TokenizeInput,
        raw_body: Option<&[u8]>,
        req_id: Option<&str>,
    ) -> TokenizeOutcome {
        // A configured `render_url` (typically a Service) replaces the
        // per-worker rotation, keeping warm connections to one host. It fails
        // fast rather than falling back to individual workers.
        if let Some(url) = render_url {
            let started = std::time::Instant::now();
            let result = match raw_body {
                Some(body) => render_post_raw_at_url(&self.tokenize_client, url, body).await,
                None => render_structured_at_url(&self.tokenize_client, url, model, input).await,
            };
            let render_ms = started.elapsed().as_secs_f64() * 1000.0;
            match result {
                Ok(tokens) => {
                    tracing::debug!(
                        req = %req_id.unwrap_or("-"),
                        mode = "render",
                        tokenize_url = %url,
                        attempt = 1,
                        render_ms = format_args!("{render_ms:.1}"),
                        token_count = tokens.len(),
                        "render ok"
                    );
                    return TokenizeOutcome::Ok(tokens);
                }
                Err(e) => {
                    let err = format!("{e:#}");
                    tracing::warn!(
                        tokenize_url = url,
                        render_ms = format_args!("{render_ms:.1}"),
                        error = %err,
                        "render via configured tokenize.url failed; failing fast (no per-worker fallback)"
                    );
                    return TokenizeOutcome::Unavailable {
                        reason: format!("configured tokenize.url={url} failed: {err}"),
                        attempts: 1,
                    };
                }
            }
        }

        // Filter to workers with an http_url; carry worker_id_str + url
        // for log context.
        let mut candidates: Vec<(u64, String, String)> = workers
            .iter()
            .filter_map(|(num, w)| {
                w.http_url
                    .as_deref()
                    .map(|u| (*num, w.worker_id_str.clone(), u.to_string()))
            })
            .collect();
        if candidates.is_empty() {
            return TokenizeOutcome::Unavailable {
                reason: "no worker has http_url configured".to_string(),
                attempts: 0,
            };
        }
        // Freshest metrics scrape first.
        candidates.sort_by_key(|(num, _, _)| self.metrics.age_ms_for(*num));
        candidates.truncate(TOKENIZE_FALLBACK_MAX_ATTEMPTS);
        let attempts = candidates.len();

        // The client timeout caps each attempt; the deadline caps the loop.
        let do_attempts = async {
            let mut last_err: Option<String> = None;
            for (i, (_num, worker_id_str, url)) in candidates.iter().enumerate() {
                // Timed per call, logged at DEBUG.
                let render_start = std::time::Instant::now();
                let result = match raw_body {
                    Some(body) => render_via_vllm_raw(&self.tokenize_client, url, body).await,
                    None => {
                        render_via_vllm_structured(&self.tokenize_client, url, model, input).await
                    }
                };
                let render_ms = render_start.elapsed().as_secs_f64() * 1000.0;
                match result {
                    Ok(tokens) => {
                        tracing::debug!(
                            req = %req_id.unwrap_or("-"),
                            mode = "render",
                            http_url = %url,
                            attempt = i + 1,
                            render_ms = format_args!("{render_ms:.1}"),
                            token_count = tokens.len(),
                            "render ok"
                        );
                        return Ok(tokens);
                    }
                    Err(e) => {
                        let err_string = format!("{e:#}");
                        tracing::warn!(
                            worker_id = %worker_id_str,
                            http_url = %url,
                            attempt = i + 1,
                            of_attempts = attempts,
                            render_ms = format_args!("{render_ms:.1}"),
                            error = %err_string,
                            "render failed; falling back to next worker"
                        );
                        last_err = Some(err_string);
                    }
                }
            }
            Err(last_err.unwrap_or_else(|| "no attempts ran".to_string()))
        };

        match tokio::time::timeout(TOKENIZE_TOTAL_DEADLINE, do_attempts).await {
            Ok(Ok(tokens)) => TokenizeOutcome::Ok(tokens),
            Ok(Err(reason)) => TokenizeOutcome::Unavailable { reason, attempts },
            Err(_) => TokenizeOutcome::Unavailable {
                reason: format!(
                    "total deadline of {}s elapsed",
                    TOKENIZE_TOTAL_DEADLINE.as_secs()
                ),
                attempts,
            },
        }
    }

    /// `Local` mode: in-process tokenization, falling back to `/render` on
    /// any local failure (unknown model, missing chat template, render or
    /// encode error).
    async fn tokenize_local_then_http(
        &self,
        render_url: Option<&str>,
        workers: &[(u64, WorkerEntry)],
        model: &str,
        input: &TokenizeInput,
        raw_body: Option<&[u8]>,
        req_id: Option<&str>,
    ) -> TokenizeOutcome {
        let local_reg = match self.local_tokenizer.as_ref() {
            Some(r) => r,
            None => {
                // The registry failed to build at startup.
                tracing::error!(
                    "tokenize_mode = Local but local_tokenizer registry is None; \
                     falling through to HTTP /render. This is a wiring bug — check main.rs."
                );
                return self
                    .render_via_http(render_url, workers, model, input, raw_body, req_id)
                    .await;
            }
        };

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
                let outcome = self
                    .render_via_http(render_url, workers, model, input, raw_body, req_id)
                    .await;
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
    async fn tokenize_via_http_with_compare(
        &self,
        render_url: Option<&str>,
        workers: &[(u64, WorkerEntry)],
        model: &str,
        input: &TokenizeInput,
        raw_body: Option<&[u8]>,
        req_id: Option<&str>,
    ) -> TokenizeOutcome {
        let outcome = self
            .render_via_http(render_url, workers, model, input, raw_body, req_id)
            .await;
        if let (Some(local_reg), TokenizeOutcome::Ok(http_tokens)) =
            (self.local_tokenizer.as_ref(), &outcome)
        {
            // In the background, so local template rendering doesn't add to
            // pick latency.
            let local_reg = local_reg.clone();
            let prom = self.prom.clone();
            let model_owned = model.to_string();
            let input_owned = input.clone();
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
        local_reg: &crate::local_tokenize::LocalTokenizerRegistry,
        prom: &crate::prom::PromMetrics,
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
