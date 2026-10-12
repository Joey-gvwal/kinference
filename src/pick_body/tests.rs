use super::*;
use serde_json::json;

/// The second of the two model decisions. A policy crossing worker sets
/// picks a different model AFTER the body was already serialized, and
/// forwarding it unchanged sent Qwen's model id to a Gemma endpoint, which
/// 404s — the request succeeds all the way through kinference and fails upstream.
#[test]
fn rewrite_model_replaces_the_model_and_preserves_everything_else() {
    let body = br#"{"model":"Qwen/Qwen2.5-32B-Instruct-AWQ","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":true}"#;
    let out = rewrite_model(body, "google/gemma-4-12B-it").expect("a JSON object rewrites");
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["model"], "google/gemma-4-12B-it");
    // Everything the caller sent must survive — this runs after scoring, so
    // dropping a field here changes what the engine actually executes.
    assert_eq!(v["max_tokens"], 16);
    assert_eq!(v["stream"], true);
    assert_eq!(v["messages"][0]["content"], "hi");
}

/// A body we cannot parse must be left alone, not mangled. The caller turns
/// `None` into a refusal rather than forwarding a mismatched model.
#[test]
fn rewrite_model_refuses_a_non_object_body() {
    assert!(rewrite_model(b"not json", "m").is_none());
    assert!(
        rewrite_model(b"[1,2,3]", "m").is_none(),
        "a JSON array is not a request body"
    );
}

#[test]
fn model_rewrite_sets_model_and_reports_mutated() {
    let mut obj = json!({ "model": "auto", "messages": [] });
    assert!(apply_pick_body_mutations(
        &mut obj,
        Some("google/gemma-4-12B-it")
    ));
    assert_eq!(obj["model"], json!("google/gemma-4-12B-it"));
}

#[test]
fn no_force_model_and_no_stream_is_a_noop() {
    let mut obj = json!({ "model": "m", "messages": [], "temperature": 0.5 });
    let before = obj.clone();
    assert!(!apply_pick_body_mutations(&mut obj, None));
    assert_eq!(obj, before);
}

#[test]
fn stream_true_adds_include_usage() {
    let mut obj = json!({ "model": "m", "stream": true });
    assert!(apply_pick_body_mutations(&mut obj, None));
    assert_eq!(obj["stream_options"]["include_usage"], json!(true));
}

#[test]
fn stream_true_preserves_existing_stream_options() {
    let mut obj = json!({
        "model": "m",
        "stream": true,
        "stream_options": { "continuous_usage_stats": true }
    });
    assert!(apply_pick_body_mutations(&mut obj, None));
    assert_eq!(obj["stream_options"]["include_usage"], json!(true));
    // Pre-existing sub-key must survive.
    assert_eq!(obj["stream_options"]["continuous_usage_stats"], json!(true));
}

#[test]
fn include_usage_already_true_is_a_noop() {
    let mut obj = json!({
        "model": "m",
        "stream": true,
        "stream_options": { "include_usage": true }
    });
    assert!(!apply_pick_body_mutations(&mut obj, None));
}

#[test]
fn malformed_stream_options_is_replaced() {
    // A non-object stream_options is coerced to a proper object.
    let mut obj = json!({ "model": "m", "stream": true, "stream_options": "garbage" });
    assert!(apply_pick_body_mutations(&mut obj, None));
    assert_eq!(obj["stream_options"]["include_usage"], json!(true));
}

#[test]
fn stream_false_adds_nothing() {
    let mut obj = json!({ "model": "m", "stream": false });
    assert!(!apply_pick_body_mutations(&mut obj, None));
    assert!(obj.get("stream_options").is_none());
}

#[test]
fn stream_absent_adds_nothing() {
    let mut obj = json!({ "model": "m" });
    assert!(!apply_pick_body_mutations(&mut obj, None));
    assert!(obj.get("stream_options").is_none());
}

#[test]
fn both_mutations_apply_together() {
    let mut obj = json!({ "model": "auto", "stream": true });
    assert!(apply_pick_body_mutations(&mut obj, Some("qwen")));
    assert_eq!(obj["model"], json!("qwen"));
    assert_eq!(obj["stream_options"]["include_usage"], json!(true));
}

#[test]
fn unknown_fields_survive_parse_mutate_serialize() {
    let original = json!({
        "model": "auto",
        "messages": [{ "role": "user", "content": "hi" }],
        "temperature": 0.7,
        "max_tokens": 100,
        "stream": true,
        "some_vendor_ext": { "nested": [1, 2, 3] }
    });
    let mut obj = original.clone();
    assert!(apply_pick_body_mutations(&mut obj, Some("concrete-model")));
    // Round-trip through bytes the way pick_and_record will.
    let bytes = serde_json::to_vec(&obj).unwrap();
    let roundtripped: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(roundtripped["model"], json!("concrete-model"));
    assert_eq!(roundtripped["temperature"], json!(0.7));
    assert_eq!(roundtripped["max_tokens"], json!(100));
    assert_eq!(roundtripped["messages"], original["messages"]);
    assert_eq!(roundtripped["some_vendor_ext"], original["some_vendor_ext"]);
    assert_eq!(roundtripped["stream_options"]["include_usage"], json!(true));
}

#[test]
fn non_object_body_is_left_unchanged() {
    let mut obj = json!([1, 2, 3]);
    assert!(!apply_pick_body_mutations(&mut obj, Some("m")));
    assert_eq!(obj, json!([1, 2, 3]));
}

/// The engine parses bodies with Pydantic, whose lax bool accepts far more
/// than JSON's `true`. Kinference must agree with it or it measures a different
/// request than the one the engine serves — silently, since nothing errors.
#[test]
fn stream_flag_matches_pydantic_lax_bool() {
    let t = |raw: &str| stream_flag(&serde_json::from_str::<Value>(raw).unwrap());

    // Truthy, exactly Pydantic's set.
    for raw in [
        r#"{"stream":true}"#,
        r#"{"stream":"true"}"#,
        r#"{"stream":"True"}"#,
        r#"{"stream":"TRUE"}"#,
        r#"{"stream":"  true  "}"#,
        r#"{"stream":"t"}"#,
        r#"{"stream":"y"}"#,
        r#"{"stream":"yes"}"#,
        r#"{"stream":"on"}"#,
        r#"{"stream":"1"}"#,
        r#"{"stream":1}"#,
    ] {
        assert!(t(raw), "{raw} should read as streaming");
    }

    // Falsy, absent, and malformed. Note `2` is NOT truthy: Pydantic rejects
    // non-0/1 integers rather than treating them as C-style truthiness, and
    // guessing `true` there would invent a streaming request.
    for raw in [
        r#"{"stream":false}"#,
        r#"{"stream":"false"}"#,
        r#"{"stream":"False"}"#,
        r#"{"stream":"f"}"#,
        r#"{"stream":"n"}"#,
        r#"{"stream":"no"}"#,
        r#"{"stream":"off"}"#,
        r#"{"stream":"0"}"#,
        r#"{"stream":0}"#,
        r#"{"stream":2}"#,
        r#"{"stream":null}"#,
        r#"{"stream":[]}"#,
        r#"{"stream":"maybe"}"#,
        r#"{"messages":[]}"#,
    ] {
        assert!(!t(raw), "{raw} should NOT read as streaming");
    }
}

/// The regression this fixes: a string-valued `stream` used to skip the
/// injection entirely, so the engine streamed while the response carried no
/// usage block and every token counter stayed flat.
#[test]
fn string_valued_stream_still_gets_include_usage() {
    for raw in [
        r#"{"stream":"true"}"#,
        r#"{"stream":"yes"}"#,
        r#"{"stream":1}"#,
    ] {
        let mut obj: Value = serde_json::from_str(raw).unwrap();
        assert!(
            apply_pick_body_mutations(&mut obj, None),
            "{raw} should mutate"
        );
        assert_eq!(
            obj["stream_options"]["include_usage"],
            json!(true),
            "{raw} should have had include_usage forced on"
        );
    }
}
