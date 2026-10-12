//! HTTP-leg helpers that POST to vLLM's `/render` endpoints, shared
//! by the pick path.
//!
//! ## Why `/render` and not `/tokenize`
//!
//! vLLM's `/v1/chat/completions/render` runs the full
//! `OpenAIServingChat._preprocess_chat` pipeline (tools, multimodal
//! placeholder substitution, system-prompt defaulting, reasoning
//! prefixes, `add_generation_prompt`) — exactly what
//! `/v1/chat/completions` would do for inference, just stopping
//! before the GPU forward. The returned token IDs are byte-perfect
//! with what the engine actually tokenizes for inference, so the
//! block hashes we compute over them match the `BlockStored` events
//! the engine emits.
//!
//! Vendored `/tokenize` (which only ran the bare Jinja `apply_chat_template`)
//! is gone — superseded by `/render` entirely.
//!
//! ## Two HTTP shapes
//!
//! - [`render_via_vllm_raw`] forwards the caller's existing OpenAI
//!   ChatCompletionRequest body bytes verbatim to
//!   `/v1/chat/completions/render`. Full fidelity — even fields we
//!   don't know about (`response_format`, vLLM extensions, future
//!   OpenAI additions) reach vLLM unchanged. The pick path is the only caller
//!   today (it has the raw body in `self.body_buf`).
//! - [`render_via_vllm_structured`] synthesizes a body from a
//!   [`TokenizeInput`] enum. gRPC `Score` uses this because it has
//!   only proto fields, not a raw request body.
//!
//! Both helpers call into [`parse_render_response`] to extract the
//! `token_ids` field from vLLM's `GenerateRequest` response object
//! (chat) or array (completions).

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

/// Map of `chat_template_kwargs` forwarded into the engine's
/// `/render` body. Values are JSON-encoded — the structured body
/// builder parses each one with `serde_json::from_str` (so `"false"`
/// → bool, `"1024"` → int, `"en"` → falls back to the literal
/// string `"en"`).
pub type ChatTemplateKwargs = HashMap<String, String>;

/// Structured input to the HTTP `/render` leg. Carries everything
/// vLLM's `_preprocess_chat` needs to produce the same token
/// sequence the engine would.
///
/// `Clone` because `render_with_fallback` may dispatch the same input
/// to multiple workers across the retry loop. The body is small (a
/// prompt or chat array) so per-attempt cloning is cheap relative to
/// the HTTP round-trip.
#[derive(Clone)]
pub enum TokenizeInput {
    /// Raw completion-style prompt → `/v1/completions/render` with
    /// `{ model, prompt }`. `add_special_tokens=true` is implied by
    /// the endpoint (matches vLLM's own `/v1/completions` default).
    /// `chat_template_kwargs` are ignored in this variant.
    Raw(String),
    /// Chat-style messages (full OpenAI array) →
    /// `/v1/chat/completions/render` with `{ model, messages, tools,
    /// tool_choice, add_generation_prompt, chat_template_kwargs }`.
    /// The engine renders its model-specific chat template through
    /// the full `_preprocess_chat` pipeline.
    Chat {
        messages: Vec<serde_json::Value>,
        kwargs: ChatTemplateKwargs,
        /// OpenAI `tools` array (JSON-schema dicts). Empty when absent.
        tools: Vec<serde_json::Value>,
        /// OpenAI `tool_choice` ("auto" | "none" | "required" | {...}).
        /// `None` when unset.
        tool_choice: Option<serde_json::Value>,
        /// OpenAI `add_generation_prompt` (default `true` for
        /// `/v1/chat/completions`). Flip to `false` for assistant-
        /// prefill continuations.
        add_generation_prompt: bool,
    },
}

/// vLLM `/render` returns a `GenerateRequest` object (chat) or an
/// array of `GenerateRequest` (completions). We only consume
/// `token_ids`; the rest (`request_id`, `sampling_params`, `features`,
/// `model`, `stream`, …) is ignored.
#[derive(Deserialize)]
struct RenderResponseObject {
    token_ids: Vec<u32>,
}

/// Build the chat-style `/render` request body from a structured
/// `TokenizeInput::Chat`. Pure function — split out for unit-testing
/// without an HTTP round-trip.
fn build_chat_render_body(
    model: &str,
    messages: &[serde_json::Value],
    kwargs: &ChatTemplateKwargs,
    tools: &[serde_json::Value],
    tool_choice: Option<&serde_json::Value>,
    add_generation_prompt: bool,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "model": model,
        "messages": messages,
        "add_generation_prompt": add_generation_prompt,
    });
    if !tools.is_empty() {
        body["tools"] = serde_json::Value::Array(tools.to_vec());
    }
    if let Some(tc) = tool_choice {
        body["tool_choice"] = tc.clone();
    }
    if !kwargs.is_empty() {
        let mut kw_obj = serde_json::Map::with_capacity(kwargs.len());
        for (k, v) in kwargs.iter() {
            // Try to JSON-parse the value so `"false"` becomes the
            // bool, `"1024"` becomes the int, etc. Fall back to
            // treating it as a literal string when the parse fails
            // (e.g. `lang=en` — bareword, not valid JSON).
            let parsed =
                serde_json::from_str(v).unwrap_or_else(|_| serde_json::Value::String(v.clone()));
            kw_obj.insert(k.clone(), parsed);
        }
        body["chat_template_kwargs"] = serde_json::Value::Object(kw_obj);
    }
    body
}

/// Parse vLLM's `/render` response into a token-id list. Pure
/// function — split out so we can unit-test both response shapes
/// (single object for chat; array for completions) without an HTTP
/// round-trip.
fn parse_render_response(json: &[u8]) -> Result<Vec<u32>> {
    let value: serde_json::Value =
        serde_json::from_slice(json).context("decoding /render response as JSON")?;
    let obj: RenderResponseObject = match value {
        serde_json::Value::Object(_) => {
            serde_json::from_value(value).context("/render response missing `token_ids` field")?
        }
        serde_json::Value::Array(mut arr) => {
            let first = arr
                .drain(..)
                .next()
                .ok_or_else(|| anyhow!("/render returned an empty array"))?;
            serde_json::from_value(first)
                .context("/render response[0] missing `token_ids` field")?
        }
        other => {
            return Err(anyhow!(
                "/render response was neither object nor array: {other}"
            ));
        }
    };
    Ok(obj.token_ids)
}

/// POST `raw_body` verbatim to `{base}/v1/chat/completions/render`.
/// Full-fidelity passthrough for callers that already have a real
/// OpenAI ChatCompletionRequest body.
pub async fn render_via_vllm_raw(
    client: &reqwest::Client,
    base: &str,
    raw_body: &[u8],
) -> Result<Vec<u32>> {
    let url = format!("{}/v1/chat/completions/render", base.trim_end_matches('/'));
    render_post_raw_at_url(client, &url, raw_body).await
}

/// POST `raw_body` to `url` verbatim — no path appending. Use when
/// the caller has a fully-qualified `/render` URL (e.g. operator-
/// configured `tokenize.url`) and we shouldn't try to derive the
/// path. Returns parsed token IDs.
pub async fn render_post_raw_at_url(
    client: &reqwest::Client,
    url: &str,
    raw_body: &[u8],
) -> Result<Vec<u32>> {
    let resp = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(raw_body.to_vec())
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("/render returned {}: {}", status, text.trim()));
    }
    let bytes = resp
        .bytes()
        .await
        .context("reading /render response body")?;
    parse_render_response(&bytes)
}

/// Same idea as [`render_via_vllm_structured`] but for a fully-
/// qualified `url` (no path derivation). The caller must point this
/// at the correct endpoint for the chat-vs-completion variant they
/// expect. For mixed traffic, prefer the path-deriving
/// [`render_via_vllm_structured`] over the per-worker `base`.
pub async fn render_structured_at_url(
    client: &reqwest::Client,
    url: &str,
    model: &str,
    input: &TokenizeInput,
) -> Result<Vec<u32>> {
    let body = match input {
        TokenizeInput::Raw(prompt) => serde_json::json!({
            "model": model,
            "prompt": prompt,
        }),
        TokenizeInput::Chat {
            messages,
            kwargs,
            tools,
            tool_choice,
            add_generation_prompt,
        } => build_chat_render_body(
            model,
            messages,
            kwargs,
            tools,
            tool_choice.as_ref(),
            *add_generation_prompt,
        ),
    };
    let resp = client
        .post(url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("/render returned {}: {}", status, text.trim()));
    }
    let bytes = resp
        .bytes()
        .await
        .context("reading /render response body")?;
    parse_render_response(&bytes)
}

/// Build a structured body from `input` and POST it to vLLM's
/// `/render` endpoint that matches the variant
/// (`/v1/chat/completions/render` for `Chat`, `/v1/completions/render`
/// for `Raw`). Used by callers who don't have a raw request body —
/// today only the gRPC `Score` path.
pub async fn render_via_vllm_structured(
    client: &reqwest::Client,
    base: &str,
    model: &str,
    input: &TokenizeInput,
) -> Result<Vec<u32>> {
    let base = base.trim_end_matches('/');
    let (url, body) = match input {
        TokenizeInput::Raw(prompt) => (
            format!("{base}/v1/completions/render"),
            serde_json::json!({
                "model": model,
                "prompt": prompt,
            }),
        ),
        TokenizeInput::Chat {
            messages,
            kwargs,
            tools,
            tool_choice,
            add_generation_prompt,
        } => (
            format!("{base}/v1/chat/completions/render"),
            build_chat_render_body(
                model,
                messages,
                kwargs,
                tools,
                tool_choice.as_ref(),
                *add_generation_prompt,
            ),
        ),
    };
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("/render returned {}: {}", status, text.trim()));
    }
    let bytes = resp
        .bytes()
        .await
        .context("reading /render response body")?;
    parse_render_response(&bytes)
}

#[cfg(test)]
mod tests;
