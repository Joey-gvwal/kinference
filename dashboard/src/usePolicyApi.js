// Request/response data access for the policy explorer.
//
// Deliberately NOT shaped like `useSnapshot.js`. That hook polls, and its
// convention is "safe empty default, swallow the error, keep the last good
// value" — correct for a dashboard that must never flicker, wrong for a form
// where the user pressed a button and is owed an answer or a reason.
//
// So these return `{ loading, error, ... }` and surface failures.

import React from "react";

// Every `/api` response is JSON, errors included, in one envelope:
// `{"error": {"code", "message", "field"?}}`. The content-type check stays as a
// guard against talking to an older server, whose unknown `/api/...` paths fell
// through to the SPA and answered `index.html` with a 200.
async function readJson(res) {
  const ct = res.headers.get("content-type") || "";
  if (!ct.includes("application/json")) {
    throw new Error(
      `expected JSON, got ${ct || "no content-type"} (HTTP ${res.status}) — ` +
      `is this an older kinference without the policy API?`
    );
  }
  const body = await res.json();
  if (!res.ok) {
    // `message` is written for a person; `field`, when present, names the part
    // of the request at fault.
    const e = body.error || {};
    const where = e.field ? ` [${e.field}]` : "";
    throw new Error(`${e.message || `HTTP ${res.status}`}${where}`);
  }
  return body;
}

/// Fetched once on mount. Policies and pools change on a watch event, not on a
/// keystroke, so polling this would be waste.
export function usePolicyContext() {
  const [state, setState] = React.useState({ loading: true, error: null, data: null });

  const reload = React.useCallback(() => {
    const ac = new AbortController();
    setState((s) => ({ ...s, loading: true, error: null }));
    (async () => {
      try {
        const res = await fetch("/api/policy/context", {
          cache: "no-store",
          signal: ac.signal,
        });
        const data = await readJson(res);
        setState({ loading: false, error: null, data });
      } catch (e) {
        if (e.name === "AbortError") return;
        setState({ loading: false, error: e.message, data: null });
      }
    })();
    return () => ac.abort();
  }, []);

  React.useEffect(() => reload(), [reload]);
  return { ...state, reload };
}

/// One simulation per press of Play.
///
/// Carries an `AbortController` so a second press supersedes the first rather
/// than racing it — there is no other `fetch` in this SPA that can be
/// re-triggered by a click, so nothing established this pattern before.
export function usePolicySim() {
  const [state, setState] = React.useState({ loading: false, error: null, result: null });
  const inflight = React.useRef(null);

  const run = React.useCallback(async (body) => {
    if (inflight.current) inflight.current.abort();
    const ac = new AbortController();
    inflight.current = ac;
    setState({ loading: true, error: null, result: null });
    try {
      const res = await fetch("/api/policy/simulate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: ac.signal,
      });
      const result = await readJson(res);
      setState({ loading: false, error: null, result });
    } catch (e) {
      if (e.name === "AbortError") return;
      setState({ loading: false, error: e.message, result: null });
    } finally {
      if (inflight.current === ac) inflight.current = null;
    }
  }, []);

  React.useEffect(() => () => inflight.current?.abort(), []);
  return { ...state, run };
}
