use super::*;

#[test]
fn parse_render_response_single_object_extracts_token_ids() {
    let body = br#"{"request_id":"x","token_ids":[1,2,3],"sampling_params":{}}"#;
    let got = parse_render_response(body).expect("single-object parse");
    assert_eq!(got, vec![1, 2, 3]);
}

#[test]
fn parse_render_response_list_takes_first() {
    let body = br#"[{"token_ids":[1,2,3]},{"token_ids":[9,9]}]"#;
    let got = parse_render_response(body).expect("list parse");
    assert_eq!(got, vec![1, 2, 3], "first element's token_ids wins");
}

#[test]
fn parse_render_response_missing_token_ids_errors() {
    let body = br#"{"request_id":"x"}"#;
    let err = parse_render_response(body).expect_err("must error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("token_ids"),
        "error should mention token_ids: {msg}"
    );
}

#[test]
fn parse_render_response_empty_array_errors() {
    let err = parse_render_response(b"[]").expect_err("must error");
    let msg = format!("{err:#}");
    assert!(msg.contains("empty"), "error should mention empty: {msg}");
}

#[test]
fn build_chat_render_body_includes_extras_when_present() {
    let messages = vec![serde_json::json!({"role": "user", "content": "hi"})];
    let mut kwargs = HashMap::new();
    kwargs.insert("enable_thinking".to_string(), "false".to_string());
    let tools = vec![serde_json::json!({"type": "function", "function": {"name": "foo"}})];
    let tool_choice = serde_json::json!("required");

    let body = build_chat_render_body(
        "qwen",
        &messages,
        &kwargs,
        &tools,
        Some(&tool_choice),
        false,
    );

    assert_eq!(body["model"], "qwen");
    assert!(body["messages"].is_array());
    assert_eq!(body["add_generation_prompt"], false);
    assert!(body["tools"].is_array(), "tools present → field included");
    assert_eq!(body["tool_choice"], "required");
    // `enable_thinking` was string "false"; should JSON-parse to bool false.
    assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
}

#[test]
fn build_chat_render_body_omits_extras_when_empty() {
    let messages = vec![serde_json::json!({"role": "user", "content": "hi"})];
    let body = build_chat_render_body("qwen", &messages, &HashMap::new(), &[], None, true);

    assert_eq!(body["model"], "qwen");
    assert_eq!(body["add_generation_prompt"], true);
    assert!(
        body.get("tools").is_none(),
        "empty tools → field omitted (don't emit `tools: []`): {body}"
    );
    assert!(
        body.get("tool_choice").is_none(),
        "no tool_choice → field omitted: {body}"
    );
    assert!(
        body.get("chat_template_kwargs").is_none(),
        "no kwargs → field omitted: {body}"
    );
}
