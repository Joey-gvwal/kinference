//! In-process tokenization via the HuggingFace `tokenizers` Rust crate.
//!
//! **Scout-mode only**: the side-by-side comparison harness lives in
//! `src/grpc.rs::tokenize_with_fallback`. It calls into this module to
//! produce local tokens for every score request, compares them against
//! the HTTP `/tokenize` result, and counts matches/divergences. The
//! HTTP result is what's returned to the caller — routing is unchanged.
//!
//! The module is dormant unless `tokenize.local.compare = true` is set
//! in config — `LocalTokenizerRegistry::new()` is only constructed in
//! that case, and the gRPC handler skips the local path otherwise.
//!
//! ## Loading model tokenizers
//!
//! Lookup order:
//!
//! 1. **Mount path**: `{config.mount_dir}/{model_name}/tokenizer.json`
//!    plus `tokenizer_config.json` for the chat template. Closest UX to
//!    "operator manages tokenizer files explicitly"; works in
//!    air-gapped clusters.
//! 2. **HuggingFace Hub fallback**: `Tokenizer::from_pretrained` (the
//!    `tokenizers` crate's built-in Hub fetch, enabled by the `http`
//!    feature). For the chat template, we additionally fetch
//!    `tokenizer_config.json` ourselves via reqwest — the crate
//!    doesn't expose that file. Hub requests honor
//!    `HUGGING_FACE_HUB_TOKEN` (gated Gemma family etc.).
//!
//! Tokenizers are cached in a `DashMap` keyed by model name and live
//! for the process lifetime — sub-MB to ~10MB each, so a typical
//! fleet's cache lands well under 100 MB.
//!
//! ## Chat-template rendering
//!
//! The `tokenizers` crate doesn't apply chat templates; that's an
//! upstream `transformers`-library concern. We use `minijinja` to
//! render the model's chat template with the same context Python
//! `transformers` uses: `messages`, `add_generation_prompt = true`,
//! plus any operator-supplied `chat_template_kwargs` (e.g. Qwen3's
//! `enable_thinking`). The rendered string is then encoded with
//! `add_special_tokens = false` since the template already emits the
//! special tokens itself.
//!
//! Two HF conventions for where the template body lives, in lookup
//! order (mirrored across mount + Hub paths):
//!
//! 1. Inline: `tokenizer_config.json` has a top-level `chat_template`
//!    string field. Llama 3.x, Mistral, Qwen 2.5, …
//! 2. Sibling: a separate `chat_template.jinja` file next to
//!    `tokenizer_config.json`. Gemma 3/4, recent multimodal builds, …
//!
//! Minijinja gets `loop_controls` + `json` (for `tojson`) plus
//! `minijinja-contrib`'s `pycompat` unknown-method callback (for
//! Python-style `.get()` / `.items()` / `.keys()` calls that Gemma
//! 3/4 templates make on message dicts). We additionally register
//! `raise_exception` and `strftime_now` as globals — the former is
//! widely used by templates for shape validation, the latter by
//! Llama 3.1+ instruct templates that emit the current date.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use dashmap::DashMap;
use tokenizers::tokenizer::Tokenizer;

use crate::config::LocalTokenizeConfig;
use crate::tokenize::TokenizeInput;

/// Cached, loaded artifacts for one model. Cheap to `Arc`-share.
struct LoadedTokenizer {
    tokenizer: Tokenizer,
    /// Jinja2 chat template string, if present (either inline in
    /// `tokenizer_config.json` or as a sibling `chat_template.jinja`).
    /// `None` for tokenizer-only models that don't publish a template
    /// (chat-mode requests for such a model will error from
    /// `LocalTokenizerRegistry::tokenize`).
    chat_template: Option<String>,
    /// Special-token strings bound into the Jinja render context. Pulled
    /// from `tokenizer_config.json`: every top-level field whose key
    /// ends in `_token` AND whose value is a string (e.g. `bos_token`
    /// → `"<bos>"`, `eos_token` → `"<eos>"`, plus model-specific extras
    /// like Gemma 4's `audio_token`, `boi_token`, etc.). Templates
    /// commonly emit literals like `{{- bos_token -}}`; without these
    /// bindings the expression renders as empty and the encoded prefix
    /// silently drops the special token. transformers'
    /// `apply_chat_template` does the equivalent automatically via
    /// `tokenizer.special_tokens_map`.
    special_tokens: HashMap<String, String>,
}

/// Two-source lookup result for a model's chat template + the
/// special-token bindings the template's render needs. Returned by
/// the mount and Hub loaders.
struct TokenizerConfigMeta {
    chat_template: Option<String>,
    special_tokens: HashMap<String, String>,
}

/// Process-wide registry of loaded tokenizers. Lazy-loaded per model;
/// the load itself is synchronous (Hub fetch can block briefly).
pub struct LocalTokenizerRegistry {
    loaded: DashMap<String, Arc<LoadedTokenizer>>,
    config: LocalTokenizeConfig,
    client: reqwest::Client,
    /// One-shot latch per model for the "mount miss, falling back to
    /// Hub" log. Without this, concurrent first-loads of the same
    /// model (e.g. N workers all hitting their first
    /// `/v1/models`-driven `warmup` call simultaneously) each emit
    /// the same log line. We can't easily dedupe the loads
    /// themselves without a refactor — DashMap's entry-style
    /// or_insert_with is sync-only — so we silence the noise here
    /// instead. The wasted Hub fetches across racing loads are
    /// negligible (small files, then ALL but the first get discarded
    /// at `or_insert` time).
    hub_fallback_logged: DashMap<String, ()>,
}

impl LocalTokenizerRegistry {
    /// Build an empty registry. The first `tokenize()` for each model
    /// triggers a load; subsequent calls for the same model hit the
    /// cache.
    pub fn new(config: LocalTokenizeConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("building reqwest client for local-tokenize Hub fetch")?;
        Ok(Self {
            loaded: DashMap::new(),
            config,
            client,
            hub_fallback_logged: DashMap::new(),
        })
    }

    /// Force-load `model_name`'s tokenizer into the cache without
    /// running any encode. Called from the metrics poller on first
    /// observation of a new model name via `/v1/models` so production
    /// traffic finds the cache warm — the first user request never
    /// pays the load latency.
    ///
    /// Errors are NOT propagated up to a panic-worthy decision: the
    /// caller logs and moves on. Cold cache for a model just means
    /// the first real request takes the latency hit (or falls back
    /// to HTTP, in `mode = "local"`).
    pub async fn warmup(&self, model_name: &str) -> Result<()> {
        let _ = self.get_or_load(model_name).await?;
        Ok(())
    }

    /// Decode a token-ID slice back to text using `model_name`'s
    /// loaded tokenizer. Diagnostics-only — used by compare-mode
    /// divergence logging to surface the exact characters each
    /// renderer produced around the first mismatching token, so an
    /// operator can read the difference (e.g. a missing `\n\n` or
    /// trailing special token) directly out of the log.
    ///
    /// Returns `None` if the model isn't loaded yet — this is a
    /// log-side helper and must never trigger a Hub fetch from a
    /// warn-level diagnostic path. `skip_special_tokens=false` so
    /// control tokens like `<end_of_turn>` and `<start_of_turn>`
    /// show up explicitly (otherwise we'd lose half the signal).
    pub fn decode(&self, model_name: &str, tokens: &[u32]) -> Option<String> {
        let entry = self.loaded.get(model_name)?;
        entry.tokenizer.decode(tokens, false).ok()
    }

    /// Tokenize `input` against `model_name`'s tokenizer. Returns the
    /// token IDs (same shape as `tokenize_via_vllm` returns) so the
    /// caller can compare element-wise.
    ///
    /// Errors when:
    /// - The tokenizer can't be loaded (mount file missing, Hub
    ///   unreachable, model gated without a token, etc.).
    /// - Chat mode is requested but the model has no `chat_template`.
    /// - The Jinja template fails to render with the given kwargs.
    /// - The tokenizer's `encode` rejects the input (malformed UTF-8,
    ///   ridiculous length, etc.).
    pub async fn tokenize(
        &self,
        model_name: &str,
        input: &TokenizeInput,
        chat_kwargs: &HashMap<String, String>,
    ) -> Result<Vec<u32>> {
        let loaded = self.get_or_load(model_name).await?;
        match input {
            TokenizeInput::Raw(prompt) => {
                let encoding = loaded
                    .tokenizer
                    .encode(prompt.as_str(), /* add_special_tokens */ true)
                    .map_err(|e| anyhow!("tokenizer encode failed: {e}"))?;
                Ok(encoding.get_ids().to_vec())
            }
            TokenizeInput::Chat {
                messages,
                tools,
                tool_choice,
                add_generation_prompt,
                ..
            } => {
                let template = loaded.chat_template.as_deref().ok_or_else(|| {
                    anyhow!(
                        "model {model_name} has no chat_template in tokenizer_config.json; \
                         can't render chat-style input locally"
                    )
                })?;
                let rendered = render_chat_template(
                    template,
                    messages,
                    chat_kwargs,
                    tools,
                    tool_choice.as_ref(),
                    *add_generation_prompt,
                    &loaded.special_tokens,
                )?;
                let encoding = loaded
                    .tokenizer
                    // Template emits the special tokens itself; don't
                    // double-add (this matches what Python transformers
                    // does in apply_chat_template + tokenize).
                    .encode(rendered.as_str(), /* add_special_tokens */ false)
                    .map_err(|e| anyhow!("tokenizer encode failed: {e}"))?;
                Ok(encoding.get_ids().to_vec())
            }
        }
    }

    /// Cache hit → return Arc. Cache miss → synchronous load (which may
    /// hit the network), then insert. `DashMap::entry` handles
    /// concurrent first-loads safely (only one waits-and-installs,
    /// others see the cached value).
    async fn get_or_load(&self, model_name: &str) -> Result<Arc<LoadedTokenizer>> {
        if let Some(entry) = self.loaded.get(model_name) {
            return Ok(Arc::clone(entry.value()));
        }
        let loaded = self
            .load(model_name)
            .await
            .with_context(|| format!("loading local tokenizer for model {model_name}"))?;
        Ok(self
            .loaded
            .entry(model_name.to_string())
            .or_insert(loaded)
            .clone())
    }

    async fn load(&self, model_name: &str) -> Result<Arc<LoadedTokenizer>> {
        // Mount path first.
        if let Some(mount_dir) = self.config.mount_dir.as_deref() {
            let dir = mount_dir.join(model_name);
            let tok_path = dir.join("tokenizer.json");
            if tok_path.exists() {
                tracing::info!(
                    model = model_name,
                    path = %tok_path.display(),
                    "local_tokenize: loading from mount"
                );
                let tokenizer = Tokenizer::from_file(&tok_path)
                    .map_err(|e| anyhow!("Tokenizer::from_file({}): {e}", tok_path.display()))?;
                let meta = read_chat_template_from_config(&dir)
                    .unwrap_or_else(|e| {
                        tracing::debug!(
                            model = model_name,
                            error = %e,
                            "local_tokenize: no chat_template available from mount \
                             (checked tokenizer_config.json::chat_template + sibling chat_template.jinja)"
                        );
                        TokenizerConfigMeta {
                            chat_template: None,
                            special_tokens: HashMap::new(),
                        }
                    });
                return Ok(Arc::new(LoadedTokenizer {
                    tokenizer,
                    chat_template: meta.chat_template,
                    special_tokens: meta.special_tokens,
                }));
            }
        }

        // Fallback: HuggingFace Hub. Offload to spawn_blocking since
        // `from_pretrained` uses blocking I/O internally. The log is
        // latched to one line per model — see `hub_fallback_logged`.
        if self
            .hub_fallback_logged
            .insert(model_name.to_string(), ())
            .is_none()
        {
            tracing::info!(
                model = model_name,
                "local_tokenize: mount miss, falling back to HuggingFace Hub"
            );
        }
        let model_name_owned = model_name.to_string();
        let tokenizer = tokio::task::spawn_blocking(move || {
            Tokenizer::from_pretrained(&model_name_owned, None)
                .map_err(|e| anyhow!("Tokenizer::from_pretrained({model_name_owned}): {e}"))
        })
        .await
        .context("spawn_blocking join")??;

        let meta = fetch_chat_template_from_hub(&self.client, model_name)
            .await
            .unwrap_or_else(|e| {
                tracing::debug!(
                    model = model_name,
                    error = %e,
                    "local_tokenize: no chat_template available via Hub"
                );
                TokenizerConfigMeta {
                    chat_template: None,
                    special_tokens: HashMap::new(),
                }
            });
        Ok(Arc::new(LoadedTokenizer {
            tokenizer,
            chat_template: meta.chat_template,
            special_tokens: meta.special_tokens,
        }))
    }
}

/// Render a Jinja2 chat template with the same context Python's
/// `transformers.apply_chat_template` uses: `messages`,
/// `add_generation_prompt = true`, plus any caller-supplied
/// `chat_template_kwargs` (e.g. `enable_thinking=false` for Qwen3).
///
/// Values in `chat_kwargs` are JSON-parsed (so `"false"` → bool
/// `false`, `"1024"` → int `1024`) and fall back to literal strings
/// for unparseable barewords — same convention as the server-side
/// HTTP `/tokenize` body builder in `src/tokenize.rs`. This means a
/// caller setting `chat_template_kwargs["enable_thinking"] = "false"`
/// gets the same rendered prompt as vLLM does.
fn render_chat_template(
    template: &str,
    messages: &[serde_json::Value],
    chat_kwargs: &HashMap<String, String>,
    tools: &[serde_json::Value],
    tool_choice: Option<&serde_json::Value>,
    add_generation_prompt: bool,
    special_tokens: &HashMap<String, String>,
) -> Result<String> {
    let mut ctx = serde_json::Map::new();
    // Bind every `*_token` literal first so subsequent inserts (which
    // include caller-controlled `chat_kwargs`) can override if the
    // caller explicitly wants different values. Templates commonly
    // reference `bos_token` / `eos_token` / Gemma's `image_token` etc.;
    // without these bindings expressions like `{{- bos_token -}}`
    // render to empty and the encoded prefix silently drops a special
    // token.
    for (k, v) in special_tokens {
        ctx.insert(k.clone(), serde_json::Value::String(v.clone()));
    }
    // Mirror vLLM's `_preprocess_chat`: normalize every message's
    // `content` field from a bare string into the OpenAI multimodal
    // content-parts form `[{"type":"text","text":"..."}]`. Chat
    // templates that branch on `content is string` vs
    // `content is sequence` (e.g. Gemma 4's system block, which
    // appends `+ ' '` in the sequence branch but not the string one)
    // will otherwise diverge from vLLM by exactly the per-branch
    // delta. After normalization both renderers always hit the
    // sequence branch; the string branch becomes dead code from our
    // side, matching the engine.
    let normalized_messages: Vec<serde_json::Value> =
        messages.iter().map(normalize_message_content).collect();
    ctx.insert(
        "messages".to_string(),
        serde_json::Value::Array(normalized_messages),
    );
    ctx.insert(
        "add_generation_prompt".to_string(),
        serde_json::Value::Bool(add_generation_prompt),
    );
    // Render tools as `null` (not an empty array) when absent, so
    // `{% if tools %}` evaluates falsy — matches Python transformers'
    // behavior of passing `None` when no tools were supplied.
    let tools_value = if tools.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::Value::Array(tools.to_vec())
    };
    ctx.insert("tools".to_string(), tools_value);
    ctx.insert(
        "tool_choice".to_string(),
        tool_choice.cloned().unwrap_or(serde_json::Value::Null),
    );
    for (k, v) in chat_kwargs {
        let parsed = serde_json::from_str::<serde_json::Value>(v)
            .unwrap_or_else(|_| serde_json::Value::String(v.clone()));
        ctx.insert(k.clone(), parsed);
    }
    let mut env = minijinja::Environment::new();
    // Gemma 3/4 (and likely other newer model templates) call Python
    // dict methods directly on message objects: `message.get('field')`,
    // `.items()`, `.keys()`, etc. Minijinja doesn't expose these on
    // map values out of the box. The `pycompat` feature ships a
    // callback that maps unknown method names to Python-equivalent
    // operations — wire it on every render so any template that
    // assumes Python semantics just works.
    env.set_unknown_method_callback(minijinja_contrib::pycompat::unknown_method_callback);
    // Chat templates can reference `raise_exception` (used by some
    // models to validate message shape). Provide it as a no-op global
    // so templates don't fail with UndefinedError.
    env.add_function(
        "raise_exception",
        |msg: String| -> Result<(), minijinja::Error> {
            Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                msg,
            ))
        },
    );
    // Llama 3.1/3.2/3.3 instruct templates emit a date line via
    // `strftime_now('%d %b %Y')`. transformers registers this global
    // out of the box; minijinja doesn't, so we provide it. POSIX
    // strftime format strings via chrono::format().
    env.add_function(
        "strftime_now",
        |format: String| -> Result<String, minijinja::Error> {
            Ok(chrono::Utc::now().format(&format).to_string())
        },
    );
    env.add_template("chat", template)
        .map_err(|e| anyhow!("minijinja: failed to parse chat template: {e}"))?;
    let tmpl = env.get_template("chat").unwrap();
    tmpl.render(serde_json::Value::Object(ctx))
        .map_err(|e| anyhow!("minijinja: failed to render chat template: {e}"))
}

/// Normalize one OpenAI-style chat message so its `content` field is
/// the multimodal content-parts list form (`[{"type":"text","text":...}]`),
/// matching what vLLM's `_preprocess_chat` does before invoking the
/// chat template.
///
/// Templates that branch on `content is string` vs `content is sequence`
/// (e.g. Gemma 4's system block, which emits a trailing `' '` only in
/// the sequence branch) will otherwise diverge from vLLM by exactly
/// the per-branch delta — and that one missing/extra token breaks the
/// XXH3 block hashes the picker uses for prefix-cache routing.
///
/// Pure transform: if `content` is already a list, returns the message
/// unchanged. If `content` is missing entirely, returns unchanged.
fn normalize_message_content(message: &serde_json::Value) -> serde_json::Value {
    let serde_json::Value::Object(map) = message else {
        return message.clone();
    };
    let Some(content) = map.get("content") else {
        return message.clone();
    };
    if !content.is_string() {
        return message.clone();
    }
    let text = content.as_str().unwrap_or("").to_string();
    let mut out = map.clone();
    out.insert(
        "content".to_string(),
        serde_json::json!([{"type": "text", "text": text}]),
    );
    serde_json::Value::Object(out)
}

/// Read the chat template + special-token bindings for a mounted
/// model directory.
///
/// HuggingFace has two conventions for where the template lives:
///
/// 1. **Inline (older)**: `tokenizer_config.json` has a top-level
///    `"chat_template"` string field. Llama 3.x, Mistral, Qwen 2.5, etc.
/// 2. **Sibling file (newer)**: `tokenizer_config.json` lacks the
///    field, but a sibling `chat_template.jinja` file holds the
///    template body verbatim. Gemma 3/4, and a growing set of
///    multimodal builds.
///
/// Either way, `tokenizer_config.json` also publishes the special-token
/// strings (`bos_token`, `eos_token`, `pad_token`, plus model-specific
/// extras) — we always extract those, because templates frequently
/// emit literals like `{{- bos_token -}}` and an unbound reference
/// silently renders as empty.
///
/// We check inline first; if absent, fall back to sibling. Returns
/// a `TokenizerConfigMeta` with `chat_template = None` if neither
/// source has a template (still includes the special-token map);
/// returns `Err` only if `tokenizer_config.json` itself is missing
/// or unparseable.
fn read_chat_template_from_config(model_dir: &Path) -> Result<TokenizerConfigMeta> {
    let cfg_path = model_dir.join("tokenizer_config.json");
    if !cfg_path.exists() {
        return Err(anyhow!("{} does not exist", cfg_path.display()));
    }
    let body = std::fs::read_to_string(&cfg_path)
        .with_context(|| format!("reading {}", cfg_path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("parsing {} as JSON", cfg_path.display()))?;
    let special_tokens = extract_special_tokens(&value);
    if let Some(inline) = value.get("chat_template").and_then(|v| v.as_str()) {
        return Ok(TokenizerConfigMeta {
            chat_template: Some(inline.to_string()),
            special_tokens,
        });
    }
    // Sibling-file fallback. Missing sibling is a normal "this model
    // has no chat template" outcome (some tokenizer-only repos) —
    // return None, not Err. Read-failure on a sibling that *does*
    // exist is genuinely surprising and worth bubbling.
    let sibling = model_dir.join("chat_template.jinja");
    if !sibling.exists() {
        return Ok(TokenizerConfigMeta {
            chat_template: None,
            special_tokens,
        });
    }
    let template = std::fs::read_to_string(&sibling)
        .with_context(|| format!("reading {}", sibling.display()))?;
    Ok(TokenizerConfigMeta {
        chat_template: Some(template),
        special_tokens,
    })
}

/// Pull every `*_token` string field from a parsed `tokenizer_config.json`
/// into a flat `name -> literal` map for binding into the Jinja render
/// context. Mirrors transformers' `tokenizer.special_tokens_map` —
/// templates reference these by name (`bos_token`, `eos_token`,
/// `image_token`, etc.), and the bound literal is what gets prepended
/// at the encode step.
///
/// Non-string `*_token` fields (some configs ship a dict like
/// `{"content": "<bos>", "lstrip": false, ...}` instead of a bare
/// string) are best-effort: we pull the `content` field if present,
/// otherwise skip. Anything else without `_token` in the key is ignored.
fn extract_special_tokens(cfg: &serde_json::Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(obj) = cfg.as_object() else {
        return out;
    };
    for (k, v) in obj {
        if !k.ends_with("_token") {
            continue;
        }
        if let Some(s) = v.as_str() {
            out.insert(k.clone(), s.to_string());
            continue;
        }
        // Some configs (Llama 3.2, others) wrap special tokens in
        // a `{"content": "<token>", ...}` dict. Pull `content` out.
        if let Some(content) = v.get("content").and_then(|c| c.as_str()) {
            out.insert(k.clone(), content.to_string());
        }
    }
    out
}

/// Fetch the chat template + special-token bindings for a model from
/// the HuggingFace Hub. Mirrors [`read_chat_template_from_config`]'s
/// two-source template lookup and always extracts special tokens
/// from `tokenizer_config.json` regardless of which template source
/// hits.
///
/// 1. GET `tokenizer_config.json`. Always parse the special-token map.
///    Return its inline `chat_template` field if present.
/// 2. Otherwise GET the sibling `chat_template.jinja` file (Gemma 3/4
///    convention) and return its body alongside the special tokens
///    from step 1.
///
/// Honors `HUGGING_FACE_HUB_TOKEN` on both requests for gated models.
/// A missing-or-404 sibling is a normal "this repo has no chat
/// template" outcome and yields `chat_template: None`.
async fn fetch_chat_template_from_hub(
    client: &reqwest::Client,
    model_name: &str,
) -> Result<TokenizerConfigMeta> {
    // Step 1: tokenizer_config.json (inline template + special tokens).
    let cfg_url = format!("https://huggingface.co/{model_name}/resolve/main/tokenizer_config.json");
    let cfg_resp = hub_get(client, &cfg_url)
        .await
        .with_context(|| format!("GET {cfg_url}"))?;
    if !cfg_resp.status().is_success() {
        anyhow::bail!("HTTP {} fetching tokenizer_config.json", cfg_resp.status());
    }
    let cfg_value: serde_json::Value = cfg_resp
        .json()
        .await
        .context("parsing tokenizer_config.json from Hub")?;
    let special_tokens = extract_special_tokens(&cfg_value);
    if let Some(inline) = cfg_value.get("chat_template").and_then(|v| v.as_str()) {
        return Ok(TokenizerConfigMeta {
            chat_template: Some(inline.to_string()),
            special_tokens,
        });
    }
    // Step 2: sibling chat_template.jinja (newer convention — Gemma 3/4,
    // some Llama 4 builds, recent multimodal models).
    let sibling_url =
        format!("https://huggingface.co/{model_name}/resolve/main/chat_template.jinja");
    let sib_resp = hub_get(client, &sibling_url)
        .await
        .with_context(|| format!("GET {sibling_url}"))?;
    if sib_resp.status() == reqwest::StatusCode::NOT_FOUND {
        // Repo legitimately has no chat template — text-only / tokenizer-only.
        return Ok(TokenizerConfigMeta {
            chat_template: None,
            special_tokens,
        });
    }
    if !sib_resp.status().is_success() {
        anyhow::bail!("HTTP {} fetching chat_template.jinja", sib_resp.status());
    }
    let body = sib_resp
        .text()
        .await
        .context("reading chat_template.jinja body")?;
    Ok(TokenizerConfigMeta {
        chat_template: Some(body),
        special_tokens,
    })
}

/// Issue a Hub GET with optional `HUGGING_FACE_HUB_TOKEN` bearer auth.
/// Shared by both the `tokenizer_config.json` and
/// `chat_template.jinja` fetches.
async fn hub_get(client: &reqwest::Client, url: &str) -> Result<reqwest::Response> {
    let mut req = client.get(url);
    if let Ok(token) = std::env::var("HUGGING_FACE_HUB_TOKEN")
        && !token.is_empty()
    {
        req = req.bearer_auth(token);
    }
    Ok(req.send().await?)
}

#[cfg(test)]
mod tests;
