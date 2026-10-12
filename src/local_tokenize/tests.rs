use super::*;

fn render(
    template: &str,
    messages: &[serde_json::Value],
    tools: &[serde_json::Value],
    add_generation_prompt: bool,
) -> Result<String> {
    render_chat_template(
        template,
        messages,
        &HashMap::new(),
        tools,
        None,
        add_generation_prompt,
        &HashMap::new(),
    )
}

/// Same as `render` but lets the caller supply the special-token
/// map (e.g. `{"bos_token": "<bos>"}`). Templates that emit
/// `{{- bos_token -}}` and friends silently render empty without
/// this binding — equivalent to dropping the leading special
/// token from the encoded prefix.
fn render_with_tokens(
    template: &str,
    messages: &[serde_json::Value],
    special_tokens: &HashMap<String, String>,
) -> Result<String> {
    render_chat_template(
        template,
        messages,
        &HashMap::new(),
        &[],
        None,
        true,
        special_tokens,
    )
}

#[test]
fn render_template_with_strftime_now_returns_today() {
    // Llama 3.1/3.2/3.3 instruct templates emit a date line via
    // this Jinja global. minijinja doesn't ship it; we register
    // it ourselves and back it with chrono::Utc::now().
    let got = render("{{ strftime_now('%Y') }}", &[], &[], true)
        .expect("strftime_now should render without error");
    let expected_year = chrono::Utc::now().format("%Y").to_string();
    assert_eq!(
        got, expected_year,
        "strftime_now('%Y') should match today's UTC year"
    );
}

#[test]
fn render_template_with_tools_includes_tool_block() {
    let tmpl = "{% if tools %}HAS_TOOLS{% else %}NO_TOOLS{% endif %}";
    // Empty tools → `null` in the Jinja context → falsy. Matches
    // transformers' behavior of passing `None`.
    let no_tools = render(tmpl, &[], &[], true).expect("empty tools renders");
    assert_eq!(no_tools, "NO_TOOLS");
    // One tool dict → truthy → tool block emitted.
    let with_tools = render(
        tmpl,
        &[],
        &[serde_json::json!({"type": "function", "function": {"name": "foo"}})],
        true,
    )
    .expect("tools-present renders");
    assert_eq!(with_tools, "HAS_TOOLS");
}

#[test]
fn render_template_with_loop_controls_parses() {
    // `{% break %}` / `{% continue %}` require minijinja's
    // `loop_controls` feature. Mistral / some Hermes templates
    // use these; without the feature, the template fails at
    // parse time.
    let tmpl = "{% for m in messages %}\
                    {% if loop.first %}{% continue %}{% endif %}\
                    X\
                    {% endfor %}";
    let messages: Vec<serde_json::Value> = (0..3).map(|i| serde_json::json!({"i": i})).collect();
    let got = render(tmpl, &messages, &[], true).expect("loop_controls feature should be on");
    assert_eq!(got, "XX", "first iteration `continue`s, two X's printed");
}

#[test]
fn render_template_with_tojson_filter_renders() {
    // The `tojson` filter requires minijinja's `json` feature.
    // Llama 3.2 vision and others use it to dump `tools` into
    // the rendered prompt as JSON.
    let tools = vec![serde_json::json!({"name": "foo"})];
    let got =
        render("{{ tools | tojson }}", &[], &tools, true).expect("tojson filter should be on");
    assert!(
        got.contains("\"name\""),
        "tojson should serialize tools as JSON: got {got}"
    );
    assert!(got.contains("\"foo\""), "got {got}");
}

#[test]
fn render_template_respects_add_generation_prompt_false() {
    let tmpl = "A{% if add_generation_prompt %}B{% endif %}";
    let on = render(tmpl, &[], &[], true).expect("agp=true renders");
    let off = render(tmpl, &[], &[], false).expect("agp=false renders");
    assert_eq!(on, "AB");
    assert_eq!(off, "A");
}

/// Smoke: the Gemma 3/4 chat template lives in a sibling
/// `chat_template.jinja` (not embedded in `tokenizer_config.json`
/// — that's the loader-side fix we still need to land). The
/// template itself uses macros, namespaces, `is mapping`/`is
/// string`/`is sequence` tests, `dictsort`, `.get()`, and
/// `loop.index0` — verify our minijinja env can render it against
/// a synthetic chat-completions body without erroring.
///
/// Fixture pulled from
/// `https://huggingface.co/google/gemma-4-12B-it/resolve/main/chat_template.jinja`
/// (committed verbatim so the test doesn't need network).
#[test]
fn render_gemma4_template_against_minimal_chat() {
    let template = include_str!("../local_tokenize_fixtures/gemma4_chat_template.jinja");
    let messages = vec![serde_json::json!({"role": "user", "content": "Hello, world!"})];
    // Bind the special tokens the way the registry would in
    // production — Gemma 4's template emits `{{- bos_token -}}`
    // at the start, plus references to other model-specific
    // tokens deeper in. Without these bindings the output is
    // missing the leading `<bos>` literal.
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let rendered = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &[],
        None,
        true,
        &tokens,
    )
    .expect("Gemma 4 chat template should render without errors");
    // Sanity-check that the template actually emitted *something*
    // structured — Gemma's template wraps each turn in role-
    // delimiting tags, so the user message text must appear.
    assert!(
        rendered.contains("Hello, world!"),
        "rendered output should contain the user message text; got: {rendered}"
    );
    // And the bound bos_token must show up at the very start —
    // matches the leading `<bos>` (token 2) that vLLM's /render
    // produces.
    assert!(
        rendered.starts_with("<bos>"),
        "rendered output must start with bos_token; got: {}",
        &rendered[..rendered.len().min(80)]
    );
}

/// Regression: Gemma 3/4's chat template emits `{{- bos_token -}}`
/// at the start of the rendered prompt. Without the special-token
/// binding, that reference renders empty and the encoded prefix
/// silently drops the leading `<bos>` (token 2 in Gemma's
/// vocabulary). vLLM's `/render` injects bos_token via
/// transformers' apply_chat_template, so a compare-mode run shows
/// 100% divergence on every Gemma request (extra leading 2 on the
/// HTTP side, nothing on the local side). Cover both halves: empty
/// binding -> no `<bos>`; bound `bos_token` -> `<bos>` at index 0.
#[test]
fn render_binds_special_tokens_into_context() {
    let tmpl = "{{- bos_token -}}HELLO";
    let no_bind = render_with_tokens(tmpl, &[], &HashMap::new())
        .expect("render should succeed without binding");
    assert_eq!(
        no_bind, "HELLO",
        "unbound bos_token must render as empty (bug we're fixing)"
    );

    let mut bound = HashMap::new();
    bound.insert("bos_token".to_string(), "<bos>".to_string());
    let with_bind =
        render_with_tokens(tmpl, &[], &bound).expect("render should succeed with binding");
    assert_eq!(
        with_bind, "<bos>HELLO",
        "bound bos_token must appear at the start"
    );
}

#[test]
fn extract_special_tokens_pulls_string_and_dict_shapes() {
    // Gemma-style: every `*_token` is a bare string at top level.
    let g = serde_json::json!({
        "bos_token": "<bos>",
        "eos_token": "<eos>",
        "image_token": "<|image|>",
        "tokenizer_class": "GemmaTokenizer",  // should be ignored — no `_token` suffix
        "some_other_field": "ignored",
    });
    let m = extract_special_tokens(&g);
    assert_eq!(m.get("bos_token").map(String::as_str), Some("<bos>"));
    assert_eq!(m.get("eos_token").map(String::as_str), Some("<eos>"));
    assert_eq!(m.get("image_token").map(String::as_str), Some("<|image|>"));
    assert!(!m.contains_key("tokenizer_class"));
    assert!(!m.contains_key("some_other_field"));

    // Llama 3.2 / newer-style: each `*_token` is a dict with a
    // `content` field. The map should still extract `content`.
    let l = serde_json::json!({
        "bos_token": {
            "content": "<|begin_of_text|>",
            "lstrip": false,
            "normalized": false,
            "rstrip": false,
            "single_word": false,
        },
        "eos_token": {"content": "<|eot_id|>"},
    });
    let m = extract_special_tokens(&l);
    assert_eq!(
        m.get("bos_token").map(String::as_str),
        Some("<|begin_of_text|>")
    );
    assert_eq!(m.get("eos_token").map(String::as_str), Some("<|eot_id|>"));
}

/// Mount-path fallback: when `tokenizer_config.json` lacks a
/// `chat_template` field (Gemma 3/4 convention), the loader must
/// pick up the template from a sibling `chat_template.jinja`. The
/// Hub-path equivalent is harder to unit-test without network; the
/// happy path of `read_chat_template_from_config` is the bit most
/// likely to regress, so cover that here.
#[test]
fn read_chat_template_falls_back_to_sibling_jinja_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    // tokenizer_config.json present but no `chat_template` field
    // (mirrors the actual Gemma 4 file we curled earlier).
    std::fs::write(
        dir.path().join("tokenizer_config.json"),
        r#"{"bos_token": "<bos>", "eos_token": "<eos>"}"#,
    )
    .expect("write config");
    // Sibling chat_template.jinja
    std::fs::write(
        dir.path().join("chat_template.jinja"),
        "{{ messages[0]['content'] }}",
    )
    .expect("write template");
    let meta =
        read_chat_template_from_config(dir.path()).expect("should succeed; either source is fine");
    assert_eq!(
        meta.chat_template.as_deref(),
        Some("{{ messages[0]['content'] }}"),
        "sibling file body should be returned"
    );
    // Special tokens come from tokenizer_config.json regardless of
    // which template source hits.
    assert_eq!(
        meta.special_tokens.get("bos_token").map(String::as_str),
        Some("<bos>")
    );
    assert_eq!(
        meta.special_tokens.get("eos_token").map(String::as_str),
        Some("<eos>")
    );
}

#[test]
fn read_chat_template_prefers_inline_over_sibling() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Both sources present — inline should win (older models that
    // ALSO have a sibling file, e.g. during a partial migration).
    std::fs::write(
        dir.path().join("tokenizer_config.json"),
        r#"{"chat_template": "INLINE"}"#,
    )
    .expect("write config");
    std::fs::write(dir.path().join("chat_template.jinja"), "SIBLING").expect("write template");
    let meta = read_chat_template_from_config(dir.path()).expect("should succeed");
    assert_eq!(
        meta.chat_template.as_deref(),
        Some("INLINE"),
        "inline tokenizer_config field should win"
    );
}

#[test]
fn read_chat_template_returns_none_when_neither_source_has_template() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("tokenizer_config.json"),
        r#"{"bos_token": "<bos>"}"#,
    )
    .expect("write config");
    // No sibling chat_template.jinja
    let meta =
        read_chat_template_from_config(dir.path()).expect("should succeed even with no template");
    assert!(
        meta.chat_template.is_none(),
        "no template anywhere → chat_template: None, got {:?}",
        meta.chat_template
    );
    // Special tokens still come back even when template is missing.
    assert_eq!(
        meta.special_tokens.get("bos_token").map(String::as_str),
        Some("<bos>")
    );
}

/// Same fixture, exercised against richer chat-completions shapes
/// (system + multi-turn + tools). Catches template branches that
/// the minimal single-user-message smoke wouldn't hit: the tools
/// preamble (with `format_function_declaration` / `format_parameters`
/// macros), the assistant tool-call rendering, and multi-turn
/// message loop.
/// Golden render: byte-exact output for the canonical Gemma 4
/// chat-completions shapes. Captured once by running
/// `render_chat_template` against the committed fixture template
/// with `bos_token = "<bos>"`, `eos_token = "<eos>"` bound. Any
/// future drift in the render path — special-token binding,
/// whitespace handling, Jinja-feature flags, the template fixture
/// itself — will flip these to a mismatch.
///
/// These don't compare against vLLM's `/render` directly (that
/// would need network or a recorded fixture and we're already
/// covered there by compare-mode in production). They lock the
/// in-process render so the BOS regression specifically can't
/// silently come back without a test failing.
#[test]
fn render_gemma4_golden_minimal() {
    let template = include_str!("../local_tokenize_fixtures/gemma4_chat_template.jinja");
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let messages = vec![serde_json::json!({"role": "user", "content": "Hello, world!"})];
    let got = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &[],
        None,
        true,
        &tokens,
    )
    .unwrap();
    let want =
        "<bos><|turn>user\nHello, world!<turn|>\n<|turn>model\n<|channel>thought\n<channel|>";
    assert_eq!(got, want, "minimal chat render drift");
}

#[test]
fn render_gemma4_golden_multi_turn_with_system() {
    let template = include_str!("../local_tokenize_fixtures/gemma4_chat_template.jinja");
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let messages = vec![
        serde_json::json!({"role": "system", "content": "Be concise."}),
        serde_json::json!({"role": "user", "content": "Q1"}),
        serde_json::json!({"role": "assistant", "content": "A1"}),
        serde_json::json!({"role": "user", "content": "Q2"}),
    ];
    let got = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &[],
        None,
        true,
        &tokens,
    )
    .unwrap();
    // Note the trailing `' '` after "Be concise." — matches
    // vLLM's `_preprocess_chat` normalizing string content to the
    // multimodal `[{"type":"text","text":...}]` form, which sends
    // the template down the sequence branch (line 191 in the
    // Gemma 4 template) where each item gets `| trim + ' '`.
    // The previous golden lacked this space and was actually
    // capturing the broken behavior that diverged from vLLM.
    let want = "<bos><|turn>system\nBe concise. <turn|>\n<|turn>user\nQ1<turn|>\n<|turn>model\nA1<turn|>\n<|turn>user\nQ2<turn|>\n<|turn>model\n<|channel>thought\n<channel|>";
    assert_eq!(got, want, "multi-turn-with-system render drift");
}

#[test]
fn normalize_message_content_wraps_string_into_text_part() {
    // String content → wrapped into multi-part list, matching
    // vLLM's `_preprocess_chat` canonicalization. Templates that
    // branch `is string` vs `is sequence` (e.g. Gemma 4's
    // system block at line 191 of its chat_template.jinja, which
    // appends `+ ' '` only in the sequence branch) will diverge
    // from vLLM without this normalization.
    let msg = serde_json::json!({"role": "system", "content": "Be concise."});
    let got = normalize_message_content(&msg);
    let want = serde_json::json!({
        "role": "system",
        "content": [{"type": "text", "text": "Be concise."}],
    });
    assert_eq!(got, want);
}

#[test]
fn normalize_message_content_leaves_already_multipart_unchanged() {
    // If `content` is already a list (multimodal /
    // content-parts payload from an OpenAI-style client), pass
    // through untouched.
    let msg = serde_json::json!({
        "role": "user",
        "content": [{"type": "text", "text": "hi"}, {"type": "image"}],
    });
    let got = normalize_message_content(&msg);
    assert_eq!(got, msg);
}

#[test]
fn normalize_message_content_leaves_missing_content_unchanged() {
    // Some legitimate messages omit `content` entirely (e.g.
    // assistant turns with only `tool_calls`). Don't synthesize
    // an empty text part — leave the absence intact so the
    // template's `{% if message['content'] %}` guards still fire
    // as expected.
    let msg = serde_json::json!({
        "role": "assistant",
        "tool_calls": [{"id": "x", "function": {"name": "f"}}],
    });
    let got = normalize_message_content(&msg);
    assert_eq!(got, msg);
}

#[test]
fn render_gemma4_golden_add_generation_prompt_false() {
    let template = include_str!("../local_tokenize_fixtures/gemma4_chat_template.jinja");
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let messages = vec![serde_json::json!({"role": "user", "content": "Hi"})];
    let got = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &[],
        None,
        false, // ← AGP=false: no model-turn primer appended
        &tokens,
    )
    .unwrap();
    let want = "<bos><|turn>user\nHi<turn|>\n";
    assert_eq!(got, want, "AGP=false render drift");
}

/// Contract: when a template emits its own special tokens (e.g.
/// `{{- bos_token -}}` at the start, role markers throughout), the
/// encode step MUST pass `add_special_tokens = false`. Otherwise
/// the tokenizer's `PostProcessor` may prepend its own BOS and the
/// final token sequence has BOS twice. transformers'
/// `apply_chat_template` defaults to `add_special_tokens=False`
/// for exactly this reason.
///
/// We can't load Gemma's tokenizer from disk in this test (gated,
/// ~MB-sized) so we build a synthetic tokenizer from an inline
/// JSON spec: WordLevel vocab + a `TemplateProcessing`
/// post-processor that prepends `<bos>` (id 99). Encoding the same
/// text with the two flag values must differ by exactly that
/// sentinel id — and our production tokenize path is documented
/// to take the `false` branch.
#[test]
fn encode_with_add_special_tokens_false_avoids_postprocessor_bos() {
    // Minimal tokenizer.json: WordLevel on whitespace, a
    // post-processor that prepends `<bos>`, and `<bos>` declared
    // as an added/special token so the literal in the input also
    // resolves to id 99.
    let spec = r#"{
            "version": "1.0",
            "truncation": null,
            "padding": null,
            "added_tokens": [
                {"id": 99, "content": "<bos>", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true}
            ],
            "normalizer": null,
            "pre_tokenizer": {"type": "Whitespace"},
            "post_processor": {
                "type": "TemplateProcessing",
                "single": [
                    {"SpecialToken": {"id": "<bos>", "type_id": 0}},
                    {"Sequence": {"id": "A", "type_id": 0}}
                ],
                "pair": [
                    {"Sequence": {"id": "A", "type_id": 0}},
                    {"Sequence": {"id": "B", "type_id": 1}}
                ],
                "special_tokens": {
                    "<bos>": {"id": "<bos>", "ids": [99], "tokens": ["<bos>"]}
                }
            },
            "decoder": null,
            "model": {
                "type": "WordLevel",
                "vocab": {"hello": 0, "world": 1, "<bos>": 99},
                "unk_token": "<unk>"
            }
        }"#;
    let tok: Tokenizer = spec.parse().expect("parse synthetic tokenizer spec");

    // Rendered text already includes the literal <bos> (mimics
    // what a chat-template render produces).
    let rendered = "<bos> hello world";

    let with_true = tok.encode(rendered, true).expect("encode true");
    let with_false = tok.encode(rendered, false).expect("encode false");

    let true_ids = with_true.get_ids();
    let false_ids = with_false.get_ids();
    // With our chosen contract (false), the leading id is the
    // literal <bos> from the rendered text. It appears exactly
    // once.
    assert_eq!(
        false_ids.first().copied(),
        Some(99),
        "false-branch must start with the template's own <bos>; got {false_ids:?}"
    );
    assert_eq!(
        false_ids.iter().filter(|&&id| id == 99).count(),
        1,
        "false-branch must contain <bos> exactly once; got {false_ids:?}"
    );
    // The wrong contract (true) would invoke the post-processor,
    // which prepends another <bos>. That's the regression this
    // test pins against.
    assert!(
        true_ids.len() > false_ids.len(),
        "true-branch should be strictly longer (post-processor added something); \
             true={true_ids:?} false={false_ids:?}"
    );
    assert_eq!(
        true_ids.iter().filter(|&&id| id == 99).count(),
        2,
        "true-branch should have <bos> twice (template's + post-processor's); \
             this is the bug we're guarding against. got {true_ids:?}"
    );
}

#[test]
fn render_gemma4_template_against_multi_turn_with_tools() {
    let template = include_str!("../local_tokenize_fixtures/gemma4_chat_template.jinja");
    let messages = vec![
        serde_json::json!({"role": "system", "content": "You are a helpful assistant."}),
        serde_json::json!({"role": "user", "content": "What's the weather in SF?"}),
        serde_json::json!({"role": "assistant", "content": "Let me check."}),
        serde_json::json!({"role": "user", "content": "Thanks."}),
    ];
    let tools = vec![serde_json::json!({
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get current weather for a city.",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {"type": "string", "description": "City name"},
                },
                "required": ["city"],
            },
        },
    })];
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());
    let rendered = render_chat_template(
        template,
        &messages,
        &HashMap::new(),
        &tools,
        None,
        true,
        &tokens,
    )
    .expect("Gemma 4 template should render with system + tools + multi-turn");
    assert!(rendered.starts_with("<bos>"), "must start with bos_token");
    assert!(rendered.contains("You are a helpful assistant"));
    assert!(rendered.contains("What's the weather in SF?"));
    assert!(rendered.contains("Thanks."));
    assert!(
        rendered.contains("get_weather"),
        "tools should be rendered into the prompt; got: {rendered}"
    );
}

/// Diagnostic probe: render two consecutive turns of the same
/// conversation (c0t0 and c0t1 from `prompt-output.json`) and find
/// the first byte at which they differ. If the conversation grows
/// strictly by appending new messages, the rendered strings must
/// share a long common prefix — anything else means the chat
/// template is non-deterministic (or our render path is) and
/// prefix-cache reuse can't work across turns. Prints the
/// divergence point + surrounding context to stderr.
///
/// Marked #[ignore] so it doesn't run on every test invocation;
/// invoke with: `cargo test --release render_diff_across_turns -- --ignored --nocapture`
#[test]
#[ignore = "diagnostic: prints render diff between consecutive turns"]
fn render_diff_across_turns() {
    let template = include_str!("../local_tokenize_fixtures/gemma4_chat_template.jinja");
    let mut tokens = HashMap::new();
    tokens.insert("bos_token".to_string(), "<bos>".to_string());
    tokens.insert("eos_token".to_string(), "<eos>".to_string());

    // The exact c0t0 / c0t1 messages from prompt-output.json.
    let c0t0 = vec![
        serde_json::json!({"role":"system","content":"amplitude node moment subscribe not embedding and config"}),
        serde_json::json!({"role":"user","content":"today test actor output with async broker read"}),
    ];
    let c0t1 = vec![
        serde_json::json!({"role":"system","content":"amplitude node moment subscribe not embedding and config"}),
        serde_json::json!({"role":"user","content":"today test actor output with async broker read"}),
        serde_json::json!({"role":"assistant","content":"only queue other black graph compare could small"}),
        serde_json::json!({"role":"user","content":"time south first open plot day warn go"}),
    ];
    let c0t2 = vec![
        serde_json::json!({"role":"system","content":"amplitude node moment subscribe not embedding and config"}),
        serde_json::json!({"role":"user","content":"today test actor output with async broker read"}),
        serde_json::json!({"role":"assistant","content":"only queue other black graph compare could small"}),
        serde_json::json!({"role":"user","content":"time south first open plot day warn go"}),
        serde_json::json!({"role":"assistant","content":"debug offset scalar two first length pipe warn"}),
        serde_json::json!({"role":"user","content":"east send task option iteration array system kind"}),
    ];

    let r0 =
        render_chat_template(template, &c0t0, &HashMap::new(), &[], None, true, &tokens).unwrap();
    let r1 =
        render_chat_template(template, &c0t1, &HashMap::new(), &[], None, true, &tokens).unwrap();
    let r2 =
        render_chat_template(template, &c0t2, &HashMap::new(), &[], None, true, &tokens).unwrap();

    eprintln!();
    eprintln!("=== c0t0 rendered ({} bytes) ===", r0.len());
    eprintln!("{r0:?}");
    eprintln!();
    eprintln!("=== c0t1 rendered ({} bytes) ===", r1.len());
    eprintln!("{r1:?}");
    eprintln!();
    eprintln!("=== c0t2 rendered ({} bytes) ===", r2.len());
    eprintln!("{r2:?}");
    eprintln!();

    fn diff_at(a: &str, b: &str, label: &str) {
        let ab = a.as_bytes();
        let bb = b.as_bytes();
        let n = ab.len().min(bb.len());
        let mut i = 0;
        while i < n && ab[i] == bb[i] {
            i += 1;
        }
        eprintln!("=== {label}: first divergence at byte {i} / shared_prefix_len={i} ===");
        let lo = i.saturating_sub(40);
        let hi_a = (i + 40).min(ab.len());
        let hi_b = (i + 40).min(bb.len());
        eprintln!("  shared prefix tail (last 40 bytes before divergence):");
        eprintln!(
            "    {:?}",
            std::str::from_utf8(&ab[lo..i]).unwrap_or("<utf8>")
        );
        eprintln!("  A continues with:");
        eprintln!(
            "    {:?}",
            std::str::from_utf8(&ab[i..hi_a]).unwrap_or("<utf8>")
        );
        eprintln!("  B continues with:");
        eprintln!(
            "    {:?}",
            std::str::from_utf8(&bb[i..hi_b]).unwrap_or("<utf8>")
        );
        eprintln!();
    }

    diff_at(&r0, &r1, "c0t0 vs c0t1");
    diff_at(&r1, &r2, "c0t1 vs c0t2");
}
