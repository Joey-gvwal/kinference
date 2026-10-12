//! Pick-time rewrites of the OpenAI request body.
//!
//! `PullBody()` serves a (possibly rewritten) body that the caller forwards
//! upstream in place of the one it sent. Two rewrites live here:
//!
//! 1. **model="auto" → concrete model.** When the client sends the `auto`
//!    pseudo-model, Kinference resolves it to a real model
//!    ([`crate::monitor::choose_model_for_auto`]); the body's `model` field
//!    must be rewritten to match, or the upstream engine 400s on `"auto"`.
//! 2. **Streaming usage.** For `stream=true` requests, vLLM only emits the
//!    final `usage` block when `stream_options.include_usage=true`. That
//!    block is what the end-of-response `ReportFinalToken` call reports, so we
//!    force the flag on.
//!
//! The helper mutates an already-parsed [`serde_json::Value`] in place so the
//! hot path parses the body only once. It reports whether anything changed, so
//! the caller re-serializes only when it did — an unchanged body is re-keyed in
//! the body store by `Arc` rather than copied.

use serde_json::Value;

/// Apply the Pick-time body rewrites to `obj` in place.
///
/// `force_model` is `Some(chosen)` **only** when the client sent
/// `model="auto"` and Kinference resolved it to `chosen`; otherwise `None`.
///
/// Returns `true` iff `obj` was actually modified — the caller uses this to
/// decide whether to re-serialize at all: on `false` the pushed bytes are
/// reused as-is (an internal zero-copy fast path — `PullBody` always serves a
/// complete body either way).
///
/// A non-object body (array, scalar) is returned unchanged (`false`); the
/// caller's own parse/validation surfaces that as a bad request.
/// Rewrite just the `model` field of an already-serialized body.
///
/// Needed because the model is decided TWICE: step 1c resolves `auto` and
/// declared aliases before scoring, and a policy may then cross to a different
/// worker set entirely, re-binding the model ~200 lines later. The first rewrite
/// goes through [`apply_pick_body_mutations`] on the parsed object; by the time
/// the second is known that object no longer holds the body (`messages` was
/// taken out of it to feed scoring), so this re-parses the bytes instead.
///
/// Costs one parse + serialize, and only on the path where a policy actually
/// changed the model. Returns `None` if the bytes are not a JSON object, leaving
/// the caller's body untouched — a body we cannot parse is one we must not
/// silently mangle.
pub(crate) fn rewrite_model(body: &[u8], model: &str) -> Option<Vec<u8>> {
    let mut obj: Value = serde_json::from_slice(body).ok()?;
    obj.as_object_mut()?
        .insert("model".to_string(), Value::String(model.to_string()));
    serde_json::to_vec(&obj).ok()
}

/// Whether the body asks for a streamed response — read the way the ENGINE
/// reads it, not the way JSON declares it.
///
/// vLLM and SGLang parse bodies with Pydantic, whose lax mode coerces
/// `"true"`, `"yes"`, `"on"`, `"t"`, `"y"` and `1` to `true`. `Value::as_bool`
/// accepts only a real JSON boolean, so a caller sending `"stream": "true"`
/// got a streamed response from the engine while Kinference read the request as
/// non-streaming — losing `stream_options.include_usage` (and with it every
/// token count) AND the TTFT sample, with nothing failing anywhere to say so.
///
/// Being stricter than the thing you are measuring does not make the
/// measurement stricter; it makes it wrong. So this matches Pydantic's lax
/// bool rather than the OpenAI schema's declared type. Non-1 numbers stay
/// false, as Pydantic rejects them rather than treating them as truthy.
///
/// Shared by both readers of the flag ([`apply_pick_body_mutations`] here and
/// `RequestMeta.streaming` in `monitor::lifecycle`) so they cannot drift:
/// fixing one alone would restore token counts while still mislabelling the
/// request as non-streaming.
pub(crate) fn stream_flag(obj: &Value) -> bool {
    match obj.get("stream") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => matches!(
            s.trim().to_ascii_lowercase().as_str(),
            "1" | "on" | "t" | "true" | "y" | "yes"
        ),
        Some(Value::Number(n)) => n.as_f64() == Some(1.0),
        _ => false,
    }
}

pub(crate) fn apply_pick_body_mutations(obj: &mut Value, force_model: Option<&str>) -> bool {
    // Read before taking the &mut borrow below.
    let streaming = stream_flag(obj);
    let Some(map) = obj.as_object_mut() else {
        return false;
    };
    let mut mutated = false;

    // (1) Rewrite the model when "auto" was resolved to a concrete model.
    if let Some(model) = force_model {
        map.insert("model".to_string(), Value::String(model.to_string()));
        mutated = true;
    }

    // (2) For streaming requests, ensure stream_options.include_usage=true so
    //     the response carries the final usage block ReportFinalToken reports.
    if streaming {
        // Ensure `stream_options` exists AND is an object. `entry` avoids a
        // second borrow of `map`; coercing a malformed value in place (rather
        // than a separate `map.insert`) keeps a single live borrow.
        let so = map
            .entry("stream_options")
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if !so.is_object() {
            *so = Value::Object(serde_json::Map::new());
        }
        let so = so.as_object_mut().expect("stream_options is an object now");
        if so.get("include_usage").and_then(Value::as_bool) != Some(true) {
            so.insert("include_usage".to_string(), Value::Bool(true));
            mutated = true;
        }
    }

    mutated
}

#[cfg(test)]
mod tests;
