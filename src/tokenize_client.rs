//! HTTP `/render` (and local-tokenizer-with-HTTP-fallback) plumbing
//! that backs `score_workers`. Implemented as additional inherent
//! methods on [`crate::monitor::KinferenceService`] — Rust allows
//! `impl KinferenceService` blocks across multiple files in the same
//! crate.
//!
//! Three modes wrap `vLLM`'s `/render` endpoint:
//! - `Render`  : pure HTTP, rotating across workers.
//! - `Local`   : in-process tokenize first, HTTP fallback on failure.
//! - `Compare` : HTTP-authoritative + background local check for audit.
//!
//! All three return a `TokenizeOutcome`. The outer dispatcher
//! (`render_with_fallback`) bumps the uniform outcome counter + latency
//! once on every return path.

use std::time::{Duration, Instant};

use crate::monitor::KinferenceService;
use crate::telemetry::Telemetry;
use crate::tokenize::{
    TokenizeInput, render_post_raw_at_url, render_structured_at_url, render_via_vllm_raw,
    render_via_vllm_structured,
};
use crate::workers::WorkerEntry;

/// Maximum number of workers we'll try in sequence before giving up on
/// tokenize. Picked from `workers_for_model` sorted by metrics-freshness
/// ascending — see `tokenize_with_fallback`. Capped so a fleet-wide
/// outage doesn't grind through every worker on every score call.
const TOKENIZE_FALLBACK_MAX_ATTEMPTS: usize = 3;

/// Hard wall-clock cap on the whole tokenize attempt loop including
/// retries. Per-attempt timeout (3s on the shared `reqwest::Client`)
/// bounds any single hang; this outer deadline bounds the set. If it
/// elapses, the score handler returns a degraded response with
/// prefix-cache fields zeroed — see `degraded_response`.
const TOKENIZE_TOTAL_DEADLINE: Duration = Duration::from_secs(5);

/// Result of attempting tokenize for one score request.
///
/// `Unavailable` is **not** a gRPC error — the score handler turns it
/// into a successful response with `token_count = 0` and zeroed
/// prefix-cache fields, so callers routing on load + queue depth still
/// get an answer.
pub(crate) enum TokenizeOutcome {
    Ok(Vec<u32>),
    Unavailable { reason: String, attempts: usize },
}

impl KinferenceService {
    /// Try to tokenize against the model's worker pool.
    ///
    /// Ordering: workers are sorted by `MetricsRegistry::age_ms_for`
    /// ascending — the worker whose `/metrics` we last scraped most
    /// recently is the most likely to be reachable, so it goes first.
    /// We don't need a dedicated "is this worker healthy?" track —
    /// the scrape-age is a free liveness proxy that's already maintained.
    ///
    /// Attempt count: capped at [`TOKENIZE_FALLBACK_MAX_ATTEMPTS`] so a
    /// fleet-wide outage doesn't burn through every worker on every
    /// score call.
    ///
    /// Total wall-clock: bounded by [`TOKENIZE_TOTAL_DEADLINE`] via
    /// `tokio::time::timeout`. If the loop is still running at deadline
    /// (e.g. two slow attempts) we return `Unavailable` immediately and
    /// let the caller degrade.
    ///
    /// **Soundness**: all workers serving the same `model_name` share
    /// the same tokenizer artifact (and we pass the same flags), so
    /// any worker returns identical tokens for identical input. The
    /// fallback is transparent to the caller.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn render_with_fallback(
        &self,
        mode: crate::config::TokenizeMode,
        render_url: Option<&str>,
        workers: &[(u64, WorkerEntry)],
        model: &str,
        input: TokenizeInput,
        raw_body: Option<&[u8]>,
        req_id: Option<&str>,
    ) -> TokenizeOutcome {
        // Telemetry: bump tokenize inflight at the very top, drop on return.
        // The Arc-held-by-guard pattern ensures the counter balances on
        // every exit path (Ok, Unavailable, panic).
        let _tk_inflight = self.telemetry.tokenize_inflight_guard();
        let started = Instant::now();

        // Mode dispatcher. Each helper returns a `TokenizeOutcome`
        // without recording the outer outcome counter / latency — we
        // do that uniformly here so all three paths get consistent
        // wall-clock measurement and the same Unavailable accounting.
        //
        // `raw_body` is only consulted on the HTTP leg — local mode
        // always uses the structured `TokenizeInput` since the
        // minijinja registry has no concept of "raw OpenAI body". The caller
        // passes `Some(&self.body_buf)` so its HTTP fallback gets
        // byte-perfect parity; Score passes `None` and the HTTP leg
        // synthesizes a body from the structured input.
        use crate::config::TokenizeMode;
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

        // Record the outcome ONCE, on every return path. `_tk_inflight`
        // drops a tick later (end of function scope) so the inflight
        // gauge stays accurate across the whole tokenize lifetime.
        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        let unavailable = matches!(outcome, TokenizeOutcome::Unavailable { .. });
        self.telemetry
            .record_tokenize_outcome(latency_ms, unavailable);
        self.prom.record_tokenize_duration(latency_ms);
        if unavailable {
            // The other two paths are counted where they succeed; this is the
            // only place a double failure is visible, so the three-way split
            // stays exhaustive.
            self.prom.record_tokenize_path("unavailable");
        }

        outcome
    }

    /// Pure HTTP path — retry/deadline/fallback over the worker pool,
    /// POSTing to vLLM's `/render` endpoint. When `raw_body` is
    /// `Some`, we forward those bytes verbatim
    /// (`render_via_vllm_raw`) for full-fidelity preprocessing
    /// (multimodal, tools, response_format, etc.). Otherwise we
    /// synthesize the body from the structured `TokenizeInput`
    /// (`render_via_vllm_structured`). Bumps `tk_attempts` /
    /// `tk_attempt_errors` per individual `/render` call.
    async fn render_via_http(
        &self,
        render_url: Option<&str>,
        workers: &[(u64, WorkerEntry)],
        model: &str,
        input: &TokenizeInput,
        raw_body: Option<&[u8]>,
        req_id: Option<&str>,
    ) -> TokenizeOutcome {
        // Operator-configured dedicated `/render` URL takes precedence
        // over the per-worker rotation. When set, every tokenize call
        // goes here verbatim — keeps a small set of long-lived
        // keepalive connections to one hostname (typically a K8s
        // Service that kube-proxy fans across endpoints) instead of
        // paying TCP-handshake cost on the first burst to each
        // discovered worker pod. Fails fast on error rather than
        // silently rotating to a specific pod — operators who set
        // this URL are making a contract with us, and resilience
        // belongs behind the Service.
        if let Some(url) = render_url {
            let started = std::time::Instant::now();
            let result = match raw_body {
                Some(body) => render_post_raw_at_url(&self.tokenize_client, url, body).await,
                None => render_structured_at_url(&self.tokenize_client, url, model, input).await,
            };
            let render_ms = started.elapsed().as_secs_f64() * 1000.0;
            match result {
                Ok(tokens) => {
                    self.telemetry.record_tokenize_attempt(true);
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
                    self.telemetry.record_tokenize_attempt(false);
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
        // Sort by metrics-scrape freshness — most recent first.
        candidates.sort_by_key(|(num, _, _)| self.metrics.age_ms_for(*num));
        candidates.truncate(TOKENIZE_FALLBACK_MAX_ATTEMPTS);
        let attempts = candidates.len();

        // Run the retry loop with a hard wall-clock deadline. Per-attempt
        // timeout (3s on the shared client) caps any individual hang; this
        // outer deadline caps the whole set.
        let telemetry = self.telemetry.clone();
        let do_attempts = async {
            let mut last_err: Option<String> = None;
            for (i, (_num, worker_id_str, url)) in candidates.iter().enumerate() {
                // Time each /render call individually so we can
                // distinguish "vLLM tokenize is slow" from "we wait
                // on something else after tokens come back" when
                // comparing against pick timings. Emitted at DEBUG
                // because it's per-request; turn it on with
                // `RUST_LOG=kinference::grpc=debug` when comparing
                // backends.
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
                        telemetry.record_tokenize_attempt(true);
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
                        telemetry.record_tokenize_attempt(false);
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

    /// `Local`-mode path: in-process tokenize first, HTTP `/render`
    /// fallback on any local failure (unknown model, missing chat
    /// template, render error, encode error). Local-success bumps
    /// `tk_local_served`; HTTP-fallback bumps `tk_http_fallback`. If
    /// HTTP also fails, returns Unavailable — same shape as the
    /// pure HTTP path. Operator-opt-in after `Compare` has shown
    /// zero divergence; not the process default.
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
                // Configured for local mode but no registry — should
                // be impossible if main.rs is wired right. Defensive
                // fall-through to HTTP rather than panic.
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
                self.telemetry.record_local_served();
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
                // Models whose tokenizer_config.json ships without a
                // chat_template (some Gemma builds, custom-tokenizer-
                // code models, etc.) make local tokenize fail on
                // every chat request — log the first failure per
                // model and stay quiet thereafter.
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
                // Only count as fallback if HTTP actually saved us;
                // a double-fail just becomes Unavailable (the outer
                // dispatcher counts it via record_tokenize_outcome).
                if matches!(outcome, TokenizeOutcome::Ok(_)) {
                    self.telemetry.record_http_fallback();
                    self.prom.record_tokenize_path("http_fallback");
                }
                outcome
            }
        }
    }

    /// Audit mode: HTTP-authoritative, with local computed
    /// side-by-side for divergence counting. Same as the
    /// pre-productionization scout path. The local result is NEVER
    /// returned — HTTP stays the source of truth for the score
    /// response. Bumps `tk_local_matches` / `tk_local_divergences` /
    /// `tk_local_errors`.
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
            // Spawn the local-tokenize comparison in the background.
            // The HTTP `/render` result is already in hand and is what
            // the picker will see — running the local check inline
            // would add 50-100ms of Jinja+encode work to TTFT on every
            // request for models with complex chat templates (Gemma 4
            // etc.). Telemetry (`record_local_tokenize_match` /
            // `_divergence` / `_error`) and the once-per-endpoint
            // WARN log fire from the spawned task, just slightly
            // after the response — fine for a diagnostic surface.
            let local_reg = local_reg.clone();
            let telemetry = self.telemetry.clone();
            let prom = self.prom.clone();
            let model_owned = model.to_string();
            let input_owned = input.clone();
            let http_tokens_owned: Vec<u32> = http_tokens.clone();
            tokio::spawn(async move {
                Self::compare_local_tokenize_inner(
                    &local_reg,
                    &telemetry,
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

    /// Scout-mode comparison. Tokenize the same input in-process and
    /// log/count divergence vs the HTTP-path tokens. Errors here are
    /// counted (`record_local_tokenize_error`) but never bubble — the
    /// score request was already served correctly via HTTP.
    ///
    /// Implemented as a free function (no `&self`) so it can run in a
    /// background `tokio::spawn` without holding a borrow of the
    /// service across the spawned task. Takes the `Arc<...>` deps
    /// directly.
    async fn compare_local_tokenize_inner(
        local_reg: &crate::local_tokenize::LocalTokenizerRegistry,
        telemetry: &Telemetry,
        prom: &crate::prom::PromMetrics,
        model: &str,
        input: &TokenizeInput,
        http_tokens: &[u32],
    ) {
        // Pull chat_template_kwargs out of the input variant — only the
        // Chat case carries them. For Raw, an empty map is correct
        // (no template applied → kwargs are no-op).
        let chat_kwargs = match input {
            TokenizeInput::Chat { kwargs, .. } => kwargs.clone(),
            TokenizeInput::Raw(_) => std::collections::HashMap::new(),
        };
        match local_reg.tokenize(model, input, &chat_kwargs).await {
            Ok(local_tokens) if local_tokens.as_slice() == http_tokens => {
                telemetry.record_local_tokenize_match();
                prom.record_tokenize_compare("match");
            }
            Ok(local_tokens) => {
                telemetry.record_local_tokenize_divergence();
                prom.record_tokenize_compare("divergence");
                let first_diff = http_tokens
                    .iter()
                    .zip(local_tokens.iter())
                    .position(|(a, b)| a != b)
                    .unwrap_or_else(|| http_tokens.len().min(local_tokens.len()));
                // Window 8 tokens before through 16 tokens after the
                // divergence point on both sides — narrow enough to
                // paste into a vocab lookup, wide enough to see what
                // the template emitted around the split.
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
                telemetry.record_local_tokenize_error();
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
