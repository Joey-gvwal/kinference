//! HTTP helpers that POST to vLLM's `/render` endpoints.
//!
//! `/v1/chat/completions/render` runs the same preprocessing as
//! `/v1/chat/completions` (tools, multimodal placeholders, system-prompt
//! defaulting, reasoning prefixes, `add_generation_prompt`) and stops before
//! inference, so the token ids match what the engine hashes into its
//! `BlockStored` events.
//!
//! - [`render_via_vllm_raw`] forwards the caller's OpenAI request body
//!   verbatim, so fields kinference doesn't know about still reach vLLM.
//! - [`render_via_vllm_structured`] builds a body from a [`TokenizeInput`],
//!   for callers without a raw body.
//!
//! Both parse the response with [`parse_render_response`].

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

/// Map of `chat_template_kwargs` forwarded into the engine's
/// `/render` body. Values are JSON-encoded: `"false"` becomes a bool,
/// `"1024"` an int, and a non-JSON value like `en` stays a string.
pub type ChatTemplateKwargs = HashMap<String, String>;

/// Structured input to the HTTP `/render` leg. Carries everything
/// vLLM's `_preprocess_chat` needs to produce the same token
/// sequence the engine would.
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

/// vLLM `/render` returns a `GenerateRequest` object (chat) or an
/// array of `GenerateRequest` (completions). We only consume
/// `token_ids`; the rest (`request_id`, `sampling_params`, `features`,
/// `model`, `stream`, …) is ignored.
#[derive(Deserialize)]
struct RenderResponseObject {
    token_ids: Vec<u32>,
}

/// Build the chat-style `/render` request body from a structured
/// `TokenizeInput::Chat`.
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
            // JSON-parse the value, falling back to a literal string.
            let parsed =
                serde_json::from_str(v).unwrap_or_else(|_| serde_json::Value::String(v.clone()));
            kw_obj.insert(k.clone(), parsed);
        }
        body["chat_template_kwargs"] = serde_json::Value::Object(kw_obj);
    }
    body
}

/// Parse vLLM's `/render` response into a token-id list: a single object for
/// chat, an array for completions.
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
pub async fn render_via_vllm_raw(
    client: &reqwest::Client,
    base: &str,
    raw_body: &[u8],
) -> Result<Vec<u32>> {
    let url = format!("{}/v1/chat/completions/render", base.trim_end_matches('/'));
    render_post_raw_at_url(client, &url, raw_body).await
}

/// POST `raw_body` verbatim to a fully-qualified `/render` URL (a worker
/// set's `render_url`), without appending a path.
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

/// [`render_via_vllm_structured`] for a fully-qualified `url`, which must be
/// the right endpoint for the input's chat or completion variant.
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
/// for `Raw`). For callers without a raw request body.
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

/// Primary tokenization path.
///
/// The HTTP path calls vLLM's `/v1/chat/completions/render`, which applies the
/// same preprocessing (tools, multimodal, reasoning prefixes) as inference.
///
/// Recommended rollout: start with `Render`, switch to `Compare` and watch
/// `tokenize_divergence_total` until it stays at zero on real traffic, then
/// switch to `Local`.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TokenizeMode {
    /// Every request goes through HTTP `/render`. `http` is accepted as a
    /// legacy alias.
    #[default]
    #[serde(alias = "http")]
    Render,
    /// In-process tokenization (`tokenizers` + `minijinja`), falling back to
    /// `/render` when a model fails to load. Only use after `Compare` shows no
    /// divergence; otherwise prefix-cache routing is silently wrong.
    Local,
    /// `/render` is authoritative; the local path runs alongside it and
    /// divergences are counted.
    Compare,
}

#[cfg(test)]
mod tests;
