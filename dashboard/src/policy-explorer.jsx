// kinference — interactive policy explorer
//
// "If a request looked like X, what would policy P do?" — with the decision
// sequence shown well enough to debug it.
//
// Everything the form offers comes from `/api/policy/context` rather than being
// hardcoded here: the qos values, the classifier tags, the policies, and the
// live candidate rows. So the UI cannot drift from what the engine accepts.

import React from "react";
import { Panel } from "./panels.jsx";
import { Button, TextField, Select, Checkbox, Chip, Alert, StatusDot } from "./BuoyantPrimitives.jsx";
import { usePolicyContext, usePolicySim } from "./usePolicyApi.js";

const mono = { fontFamily: "var(--font-mono)", fontSize: 11 };
const label = {
  fontFamily: "var(--font-sans)", fontSize: 11, fontWeight: 600,
  letterSpacing: "0.6px", textTransform: "uppercase", color: "var(--fg-secondary)",
};

// Verdicts use the same vocabulary as `kinference_policy_decisions_total{outcome}`,
// so what the explorer says and what the metric counts are the same word.
const VERDICT = {
  chosen: { sev: "success", text: "A candidate was chosen." },
  unmatched_served: {
    sev: "info",
    text: "No rule matched. `spec.unmatched` served it anyway — fail-open, not an error.",
  },
  unmatched_empty: {
    sev: "warning",
    text: "No rule matched and the unmatched route had no candidate left.",
  },
  rejected_on_empty: {
    sev: "error",
    text: "A rule matched but every candidate was excluded, and `onEmpty: Reject`.",
  },
  rejected_unmatched: {
    sev: "error",
    text: "No rule matched and `spec.unmatched: Reject`.",
  },
};

// JSON → YAML, for displaying a policy spec.
//
// Hand-written rather than a dependency: the input is not arbitrary YAML but
// one serialized `InferencePolicySpec` — objects, arrays, strings, numbers and
// bools, no anchors, tags, dates or multi-line scalars. A library for that is
// ~40 KB of bundle to print five kinds of node.
// Quoting is an ALLOWLIST, not a denylist. Enumerating what YAML would
// misread is a losing game — 1.1 alone reads `on`, `0x10`, `010`, `1:30`,
// `.inf` and `1_000` as non-strings — so instead: emit bare only what is
// unambiguously a plain string, and double-quote everything else. Over-quoting
// is ugly; under-quoting silently changes a value's type.
//
// Safe plain: starts with a letter, contains only word characters, `-`, `.`,
// `/`, `+` and single interior spaces. That covers every name this spec
// carries (`qwen-2.5`, `google/gemma-4-12B-it`, `PredictedTTFT`, `Reject`).
const PLAIN = /^[A-Za-z][\w./+-]*(?: [\w./+-]+)*$/;
// …minus the words YAML reads as bool or null even though they look like text.
const RESERVED_WORDS = /^(?:y|n|yes|no|true|false|on|off|null)$/i;
const scalar = (v) => {
  if (v === null || v === undefined) return "null";
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  const str = String(v);
  // JSON's string escaping is a subset of YAML's double-quoted style, so this
  // is correct for control characters and quotes as well as the plain case.
  return PLAIN.test(str) && !RESERVED_WORDS.test(str) ? str : JSON.stringify(str);
};

const toYaml = (node, indent = 0) => {
  const pad = "  ".repeat(indent);
  if (Array.isArray(node)) {
    if (node.length === 0) return `${pad}[]`;
    return node.map((v) => {
      if (v !== null && typeof v === "object") {
        // The first line of a nested block rides on the dash, so the item
        // reads as one unit rather than starting a line below it.
        const block = toYaml(v, indent + 1);
        return `${pad}- ${block.slice((indent + 1) * 2)}`;
      }
      return `${pad}- ${scalar(v)}`;
    }).join("\n");
  }
  if (node !== null && typeof node === "object") {
    const keys = Object.keys(node);
    if (keys.length === 0) return `${pad}{}`;
    return keys.map((k) => {
      const v = node[k];
      if (v !== null && typeof v === "object" && (!Array.isArray(v) || v.length > 0)) {
        return `${pad}${k}:\n${toYaml(v, indent + 1)}`;
      }
      if (Array.isArray(v)) return `${pad}${k}: []`;
      return `${pad}${k}: ${scalar(v)}`;
    }).join("\n");
  }
  return `${pad}${scalar(node)}`;
};

const Row = ({ children, gap = 10 }) => (
  <div style={{ display: "flex", gap, alignItems: "flex-start", flexWrap: "wrap" }}>{children}</div>
);

const Field = ({ title, children, width }) => (
  <div style={{ display: "flex", flexDirection: "column", gap: 4, minWidth: width || 140 }}>
    <span style={label}>{title}</span>
    {children}
  </div>
);

/// A step's candidate list, with the reason attached where there is one.
const CandLines = ({ title, items, tone }) => {
  if (!items || items.length === 0) return null;
  return (
    <div style={{ marginTop: 8 }}>
      <span style={label}>{title}</span>
      <div style={{ display: "flex", flexDirection: "column", gap: 3, marginTop: 4 }}>
        {items.map((it, i) => (
          <div key={i} style={{ ...mono, color: tone || "var(--fg-primary)" }}>
            {typeof it === "string" ? it : (
              <>
                {it.candidate}
                <span style={{ color: "var(--fg-secondary)" }}> — {it.why}</span>
              </>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

const StepBlock = ({ step }) => {
  const [open, setOpen] = React.useState(step.matched);
  const t = step.trace;
  return (
    <div style={{
      border: "1px solid var(--border)", borderRadius: "var(--radius)",
      background: step.matched ? "var(--bg-surface)" : "var(--bg-surface-alt)",
      padding: "10px 12px",
    }}>
      <div onClick={() => setOpen((o) => !o)}
        style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
        <StatusDot status={step.matched ? (step.winner ? "ok" : "warning") : "unknown"} />
        <span style={{ ...mono, fontSize: 12, fontWeight: 600 }}>
          {step.index === null
            ? "spec.unmatched"
            : `rule ${step.index} · ${step.name}`}
        </span>
        <span style={{ ...mono, color: "var(--fg-secondary)" }}>
          {step.matched
            ? (step.winner ? "decided" : step.fellThrough ? "empty → fell through" : "empty")
            : "did not match"}
        </span>
        <span style={{ marginLeft: "auto", ...mono, color: "var(--fg-secondary)" }}>
          {open ? "collapse" : "expand"}
        </span>
      </div>

      {open && (
        <div style={{ marginTop: 8, paddingLeft: 16, borderLeft: "2px solid var(--divider)" }}>
          {step.predicates.length > 0 && (
            <div>
              <span style={label}>matches — entries OR, terms AND</span>
              {step.predicates.map((p, i) => (
                <div key={i} style={{ ...mono, marginTop: 3 }}>
                  {/* One glyph per term, and no entry-level glyph: the entry is
                      the AND of its terms, so its result is already on the
                      line. A second mark beside the first term read as two
                      verdicts on that one term. The OR between entries is
                      carried by the `or` prefix instead. */}
                  {i > 0 && (
                    <span style={{ color: "var(--fg-secondary)", marginRight: 8 }}>or</span>
                  )}
                  {p.terms.map((tm, j) => (
                    <span key={j} style={{ marginRight: 8 }}>
                      <span style={{ color: tm.matched ? "var(--bel-success)" : "var(--bel-error)" }}>
                        {tm.matched ? "✓" : "✗"}
                      </span>{" "}{tm.attr}
                    </span>
                  ))}
                  {p.terms.length === 0 && (
                    <span style={{ color: "var(--fg-secondary)" }}>no terms — catch-all</span>
                  )}
                </div>
              ))}
            </div>
          )}

          {step.consider && (
            <div style={{ marginTop: 8 }}>
              <span style={label}>
                consider{step.consider.weighted ? " — WEIGHTED, one group sampled" : ""}
              </span>
              <div style={{ ...mono, marginTop: 3, color: "var(--fg-secondary)" }}>
                {step.consider.targets.map((tg, i) => (
                  <div key={i}>{tg.model} @ {tg.backends}</div>
                ))}
              </div>
              <CandLines title="admitted" items={step.consider.admitted} />
              <CandLines title="not considered" items={step.consider.rejected}
                tone="var(--fg-secondary)" />
            </div>
          )}

          {t && (
            <>
              <CandLines title="eliminated" items={t.eliminated} tone="var(--bel-error)" />
              <CandLines title="survivors" items={t.survivors} />
              {/* `Trace` records only the tier INDEX, so the objective is
                  joined on server-side from route.select[tier].rank. */}
              {step.tierRank && (
                <div style={{ marginTop: 8 }}>
                  <span style={label}>selected</span>
                  <div style={{ ...mono, marginTop: 3 }}>
                    tier {t.tier} · {step.tierRank}
                    {t.winnerObjectiveMs != null && ` · ${t.winnerObjectiveMs.toFixed(1)} ms`}
                    {t.tied > 1 && ` · ${t.tied} tied`}
                  </div>
                </div>
              )}
              {/* `evaluate` skips a tier-subset miss with no trace entry, so
                  these are recomputed rather than read out of the trace. */}
              <CandLines title="outside a tier's subset" items={step.tierSubsetDrops}
                tone="var(--fg-secondary)" />
            </>
          )}
        </div>
      )}
    </div>
  );
};

/// `spec` as applied — the text to diff against the manifest, not a rendering
/// of the compiled form (which normalizes and has no inverse).
const SpecBlock = ({ spec }) => {
  const yaml = React.useMemo(() => toYaml(spec), [spec]);
  return (
    <div>
      <span style={label}>spec</span>
      <pre style={{
        ...mono, margin: "6px 0 0", padding: "10px 12px",
        background: "var(--bg-surface-alt)", border: "1px solid var(--border)",
        borderRadius: "var(--radius)", overflowX: "auto",
        color: "var(--fg-primary)", lineHeight: 1.5,
      }}>{yaml}</pre>
    </div>
  );
};

export const PolicyExplorer = () => {
  const ctx = usePolicyContext();
  const sim = usePolicySim();

  const [policyKey, setPolicyKey] = React.useState("");
  const [poolKey, setPoolKey] = React.useState("");
  const [model, setModel] = React.useState("");
  const [qos, setQos] = React.useState("");
  const [tags, setTags] = React.useState([]);
  const [tenant, setTenant] = React.useState("");
  const [inputTokens, setInputTokens] = React.useState("4096");
  const [maxTokens, setMaxTokens] = React.useState("256");
  const [rows, setRows] = React.useState([]);
  const [repeat, setRepeat] = React.useState(false);

  const policies = ctx.data?.policies || [];
  const pools = ctx.data?.pools || [];
  const policy = policies.find((p) => p.key === policyKey);
  const pool = pools.find((p) => p.key === poolKey);

  // Seed the candidate table from live state whenever the pool changes.
  //
  // The API returns one candidate per (endpoint, client-facing NAME), because
  // that is the unit the engine ranks — `models_to_score` yields the names a
  // rule asked for and each becomes its own candidate. But a worker with three
  // aliases is still ONE worker: `queuedPrompts`, cached blocks and pTTFT are
  // properties of the endpoint, so three rows would mean typing each edit three
  // times to describe one busy worker. Collapse to one row per endpoint and
  // carry the names on it; `play` expands them back out.
  const seedRows = React.useCallback((p) => {
    const byEndpoint = new Map();
    for (const c of p?.candidates || []) {
      const key = `${c.backendName}\u0000${c.upstreamModel}\u0000${c.endpoint}`;
      const row = byEndpoint.get(key);
      if (row) {
        if (!row.names.includes(c.canonicalModel)) row.names.push(c.canonicalModel);
        continue;
      }
      byEndpoint.set(key, {
        ...c,
        include: true,
        // The upstream first, then its aliases — the order `aliases_for`
        // produces, so the primary name reads first.
        names: [c.canonicalModel],
        offAliases: [],
      });
    }
    setRows([...byEndpoint.values()]);
  }, []);
  React.useEffect(() => {
    if (!poolKey && pools.length > 0) setPoolKey(pools[0].key);
  }, [pools, poolKey]);
  React.useEffect(() => { seedRows(pool); }, [pool, seedRows]);
  React.useEffect(() => {
    if (!policyKey && policies.length > 0) setPolicyKey(policies[0].key);
  }, [policies, policyKey]);
  // Seed the model at most once, and never over the user's own input.
  //
  // Two flags because they answer different questions. `modelSeeded` is only
  // set by a SUCCESSFUL seed, so opening on a pool with no candidates does not
  // consume the one shot — switching to a populated pool still prefills.
  // `modelTouched` is set by any edit, which is what stops a later prefill
  // from overwriting a value that was typed while the field had nothing to
  // seed from. Together: the field fills in once if it can, and after that the
  // model is the user's, not a function of the pool.
  //
  // A deliberate clear counts as an edit, so it sticks — an empty model is a
  // case worth reaching, since `RequestAttrs::get` always reports `model` as
  // present and an empty one matches no `model:` term.
  const modelSeeded = React.useRef(false);
  const modelTouched = React.useRef(false);
  const editModel = (v) => {
    modelTouched.current = true;
    setModel(v);
  };
  React.useEffect(() => {
    if (modelSeeded.current || modelTouched.current) return;
    const first = pool?.candidates?.[0]?.canonicalModel;
    if (!first) return;
    modelSeeded.current = true;
    setModel(first);
  }, [pool]);

  const setRow = (i, patch) =>
    setRows((rs) => rs.map((r, j) => (j === i ? { ...r, ...patch } : r)));

  // Without this a provider-only pool (or an unreachable fleet) leaves the
  // table empty and no local rule can be exercised at all.
  const addRow = () => setRows((rs) => rs.concat({
    include: true,
    backendName: `sim-${rs.length + 1}`,
    backendType: "Local",
    canonicalModel: model || "",
    upstreamModel: null,
    endpoint: `simulated-${rs.length + 1}:8000`,
    queuedPrompts: 0,
    contextWindow: 32768,
    names: [model || ""],
    offAliases: [],
  }));
  const dropRow = (i) => setRows((rs) => rs.filter((_, j) => j !== i));
  // Dropping one name answers "is this policy reaching this worker under that
  // alias?" without deleting the worker.
  const toggleName = (i, n) => setRow(i, {
    offAliases: rows[i].offAliases.includes(n)
      ? rows[i].offAliases.filter((x) => x !== n)
      : rows[i].offAliases.concat(n),
  });
  const activeNames = (r) => (r.names || [r.canonicalModel])
    .filter((n) => !(r.offAliases || []).includes(n));

  const play = () => {
    sim.run({
      policy: policyKey,
      pool: poolKey || null,
      // The typed name. The server expands it into the alias SET the engine
      // would see — an alias is rewritten to a concrete upstream before any
      // predicate runs, so `matches: {model: qwen-2.5}` fires for a request
      // naming `qwen-simple`. Expanding here instead would duplicate the alias
      // tables in the browser and let them drift.
      requestedModel: model,
      // `qos` can be genuinely ABSENT, and absence never matches. "" here means
      // unset, not the empty string.
      qos: qos || null,
      tags,
      tenant,
      inputTokens: inputTokens === "" ? null : Number(inputTokens),
      maxTokens: maxTokens === "" ? null : Number(maxTokens),
      runs: repeat ? 100 : 1,
      // One row back out to one candidate per name it is routable under — the
      // shape the engine actually ranks, and what the driver's
      // `upstreams_for` loop produces. The endpoint's own edits are shared by
      // every name, because they describe the worker, not the name.
      candidates: rows.filter((r) => r.include).flatMap((r) =>
        activeNames(r).map((name) => ({
        backendName: r.backendName,
        backendType: r.backendType,
        canonicalModel: name,
        upstreamModel: r.upstreamModel,
        endpoint: r.endpoint,
        queuedPrompts: r.queuedPrompts === "" || r.queuedPrompts == null
          ? null : Number(r.queuedPrompts),
        matchedBlocks: r.matchedBlocks === "" || r.matchedBlocks == null
          ? null : Number(r.matchedBlocks),
        kvCacheUsage: r.kvCacheUsage === "" || r.kvCacheUsage == null
          ? null : Number(r.kvCacheUsage),
        contextWindow: r.contextWindow,
        predictedTtftMs: r.predictedTtftMs === "" || r.predictedTtftMs == null
          ? null : Number(r.predictedTtftMs),
        observedTtftP95Ms: r.observedTtftMs == null ? null : Number(r.observedTtftMs),
        }))
      ),
    });
  };

  if (ctx.loading) {
    return <Panel padded={false} title="Policy explorer"><div style={{ padding: 16, ...mono }}>loading…</div></Panel>;
  }
  if (ctx.error) {
    return (
      <Panel padded={false} title="Policy explorer">
        <div style={{ padding: 16 }}>
          <Alert severity="warning" title="Policy context unavailable">
            <span style={mono}>{ctx.error}</span>
          </Alert>
        </div>
      </Panel>
    );
  }

  const res = sim.result;
  const verdict = res && VERDICT[res.verdict];

  return (
    <>
      <Panel padded={false} title="Policy"
        action={<span style={{ ...mono, color: "var(--fg-secondary)" }}>
          {policies.length} registered
        </span>}>
        <div style={{ padding: 14, display: "flex", flexDirection: "column", gap: 12 }}>
          <Row>
            <Field title="policy" width={260}>
              <Select value={policyKey} onChange={setPolicyKey} size="small" fullWidth
                options={policies.map((p) => ({
                  value: p.key,
                  // Unattached is the state most worth seeing: a policy no pool
                  // references is inert, and that is invisible otherwise.
                  label: `${p.key}${p.attached ? "" : "  (unattached)"}${p.warnings.length ? "  ⚠" : ""}`,
                }))} />
            </Field>
            <Field title="pool (candidate source)" width={240}>
              <Select value={poolKey} onChange={setPoolKey} size="small" fullWidth
                options={pools.map((p) => ({ value: p.key, label: p.key }))} />
            </Field>
          </Row>

          {policy && (
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center" }}>
                <Chip size="small" label={policy.state}
                  color={policy.attached ? "primary" : "default"} />
                <Chip size="small" variant="outlined" label={`unmatched: ${policy.unmatched}`} />
                {policy.rules.map((r) => (
                  <Chip key={r.index} size="small" variant="outlined"
                    label={`${r.index}: ${r.name}  (onEmpty ${r.onEmpty})`} />
                ))}
              </div>
              {policy.warnings.length > 0 && (
                <Alert severity="warning"
                  title={`${policy.warnings.length} warning(s) — the policy still routes`}>
                  <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {/* Structured: the code is what the docs index, the path is
                        where in the manifest to look. */}
                    {policy.warnings.map((w, i) => (
                      <span key={i} style={mono}>
                        <strong>{w.code}</strong> · {w.path}
                        <br />{w.message}
                      </span>
                    ))}
                  </div>
                </Alert>
              )}
              {policy.nondeterminism.length > 0 && (
                <Alert severity="info" title="This policy does not give the same answer twice">
                  <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                    {policy.nondeterminism.map((n, i) => <span key={i} style={mono}>{n.message}</span>)}
                  </div>
                </Alert>
              )}

              {policy.spec && <SpecBlock spec={policy.spec} />}
            </div>
          )}
        </div>
      </Panel>

      <Panel padded={false} title="Request">
        <div style={{ padding: 14, display: "flex", flexDirection: "column", gap: 12 }}>
          <Row>
            <Field title="model" width={240}>
              <TextField value={model} onChange={editModel} size="small" fullWidth
                placeholder="qwen-2.5" />
              {/* A `model:` term is matched against the whole set, so a rule
                  naming a sibling alias fires too. Showing the set is what
                  turns that from a surprise into an explanation. */}
              {res?.resolvedModels?.length > 1 && (
                <div style={{ ...mono, color: "var(--fg-secondary)", marginTop: 4 }}>
                  matched as {res.resolvedModels.join(", ")}
                </div>
              )}
            </Field>
            <Field title="qos" width={150}>
              <Select value={qos} onChange={setQos} size="small" fullWidth
                options={[{ value: "", label: "(unset)" }]
                  .concat((ctx.data.qos || []).map((q) => ({ value: q, label: q })))} />
            </Field>
            <Field title="tenant" width={150}>
              <TextField value={tenant} onChange={setTenant} size="small" fullWidth
                placeholder='"" is a value' />
            </Field>
            <Field title="prompt tokens" width={130}>
              <TextField value={inputTokens} onChange={setInputTokens} size="small" fullWidth
                type="number" helperText="rounded up to a block" />
            </Field>
            <Field title="max tokens" width={120}>
              <TextField value={maxTokens} onChange={setMaxTokens} size="small" fullWidth
                type="number" />
            </Field>
          </Row>
          <div>
            <span style={label}>tags</span>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 6 }}>
              {(ctx.data.tags || []).map((t) => (
                <Chip key={t} label={t} size="small"
                  variant={tags.includes(t) ? "filled" : "outlined"}
                  color={tags.includes(t) ? "primary" : "default"}
                  onClick={() => setTags((ts) =>
                    ts.includes(t) ? ts.filter((x) => x !== t) : ts.concat(t))} />
              ))}
            </div>
          </div>
        </div>
      </Panel>

      <Panel padded={false} title="Candidates"
        action={<>
          <Button variant="text" size="small" startIcon="add" onClick={addRow}>worker</Button>
          <Button variant="text" size="small" onClick={() => seedRows(pool)}>reset to live</Button>
        </>}>
        <div style={{ padding: 14, overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", ...mono }}>
            <thead>
              <tr style={{ textAlign: "left", color: "var(--fg-secondary)" }}>
                {["", "backend", "type", "routable as", "endpoint", "queued 30s", "kv 0-1", "cached blk", "pTTFT ms", ""].map((h) => (
                  <th key={h} style={{ padding: "4px 8px", fontWeight: 600 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i} style={{ borderTop: "1px solid var(--divider)",
                  opacity: r.include ? 1 : 0.45 }}>
                  <td style={{ padding: "2px 8px" }}>
                    <Checkbox checked={r.include}
                      onChange={(v) => setRow(i, { include: v })} />
                  </td>
                  <td style={{ padding: "4px 8px" }}>{r.backendName}</td>
                  <td style={{ padding: "4px 8px" }}>{r.backendType}</td>
                  {/* Every client-facing name this endpoint answers to — the
                      upstream plus its `spec.models` aliases. `consider`
                      filters on THESE, not on the upstream, so a rule naming
                      `gemma-4` matches the chip, not the checkpoint. The first
                      is editable because "what if it served another name?" is
                      worth reaching; the aliases toggle off to ask whether a
                      policy is reaching the worker through that name at all. */}
                  <td style={{ padding: "2px 8px", minWidth: 200 }}>
                    <TextField size="small" value={r.canonicalModel}
                      onChange={(v) => setRow(i, {
                        canonicalModel: v,
                        names: [v, ...(r.names || []).slice(1)],
                      })} />
                    {(r.names || []).length > 1 && (
                      <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginTop: 4 }}>
                        {r.names.slice(1).map((n) => (
                          <Chip key={n} label={n} size="small"
                            variant={(r.offAliases || []).includes(n) ? "outlined" : "filled"}
                            color={(r.offAliases || []).includes(n) ? "default" : "primary"}
                            onClick={() => toggleName(i, n)} />
                        ))}
                      </div>
                    )}
                  </td>
                  <td style={{ padding: "4px 8px", color: "var(--fg-secondary)" }}>
                    {r.endpoint || "—"}
                  </td>
                  <td style={{ padding: "2px 8px", width: 90 }}>
                    {r.backendType === "Local" ? (
                      <TextField size="small" type="number"
                        value={r.queuedPrompts ?? ""}
                        onChange={(v) => setRow(i, { queuedPrompts: v })} />
                    ) : "—"}
                  </td>
                  <td style={{ padding: "2px 8px", width: 90 }}>
                    {r.backendType === "Local" ? (
                      <TextField size="small" type="number"
                        value={r.kvCacheUsage ?? ""}
                        onChange={(v) => setRow(i, { kvCacheUsage: v })} />
                    ) : "—"}
                  </td>
                  <td style={{ padding: "2px 8px", width: 90 }}>
                    {r.backendType === "Local" ? (
                      <TextField size="small" type="number"
                        value={r.matchedBlocks ?? ""}
                        onChange={(v) => setRow(i, { matchedBlocks: v })} />
                    ) : "—"}
                  </td>
                  <td style={{ padding: "2px 8px", width: 110 }}>
                    <TextField size="small" type="number"
                      placeholder="predictor"
                      value={r.predictedTtftMs ?? ""}
                      onChange={(v) => setRow(i, { predictedTtftMs: v })} />
                  </td>
                  <td style={{ padding: "2px 4px" }}>
                    <Button variant="text" size="small" color="error"
                      onClick={() => dropRow(i)}>×</Button>
                  </td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr><td colSpan={10} style={{ padding: 12, color: "var(--fg-secondary)" }}>
                  No live candidates in this pool — add a worker to evaluate anyway.
                </td></tr>
              )}
            </tbody>
          </table>
          <div style={{ marginTop: 10, color: "var(--fg-secondary)", ...mono }}>
            One row per endpoint. Each name it is routable under becomes its own
            candidate at play time, sharing the row's queue and timing — those
            describe the worker, not the name.
            <br />
            `queued 30s` and `kv 0-1` are the windowed MEANS a policy thresholds
            on, not instantaneous readings — so `queued 30s` is fractional, and
            0.3 means "a brief spike", 4 means "sustained".
            <br />
            Leave pTTFT blank to run the real predictor against the prompt size above;
            cached blocks feed its prefill term (blank means a cold prompt).
          </div>
        </div>
      </Panel>

      <Panel padded={false} title="Decision"
        action={
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            {policy?.nondeterminism.length > 0 && (
              <Checkbox checked={repeat} onChange={setRepeat} label="run 100×" />
            )}
            <Button size="small" startIcon="play_arrow" onClick={play}
              disabled={!policyKey || sim.loading}>
              {sim.loading ? "running" : "play"}
            </Button>
          </div>
        }>
        <div style={{ padding: 14, display: "flex", flexDirection: "column", gap: 10 }}>
          {sim.error && (
            <Alert severity="error" title="Simulation failed">
              <span style={mono}>{sim.error}</span>
            </Alert>
          )}
          {!res && !sim.error && (
            <div style={{ ...mono, color: "var(--fg-secondary)" }}>
              Press play to evaluate this request against the selected policy.
            </div>
          )}
          {res && (
            <>
              <Alert severity={verdict?.sev || "info"} title={res.verdict}>
                <span>{verdict?.text}</span>
              </Alert>

              {/* The winner is the answer to the question the user asked, so it
                  gets the largest type on the page rather than the smallest. */}
              {res.winner && (
                <div style={{
                  display: "flex", alignItems: "baseline", gap: 12, flexWrap: "wrap",
                  padding: "12px 16px", borderRadius: "var(--radius)",
                  background: "var(--status-ok-bg)",
                  border: "1px solid var(--bel-success)",
                }}>
                  <span style={{ ...label, color: "var(--status-ok-fg)" }}>winner</span>
                  <span style={{
                    fontFamily: "var(--font-mono)", fontSize: 18, fontWeight: 600,
                    color: "var(--status-ok-fg)", wordBreak: "break-all",
                  }}>{res.winner}</span>
                </div>
              )}

              {res.distribution && (
                <div>
                  <span style={label}>{res.distribution.runs} runs</span>
                  <div style={{ marginTop: 4 }}>
                    {res.distribution.outcomes.map((o, i) => (
                      <div key={i} style={mono}>
                        {o.count}× {o.outcome}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {res.steps.map((s, i) => <StepBlock key={i} step={s} />)}
              </div>

              {/* A candidate with no prediction is still RANKED, at 0.0 ms —
                  `CandidateTarget::Local::predicted_ttft_ms` is a plain f32. So
                  under `minimize: PredictedTTFT` the ordering would look real
                  while being a tie on a fabricated zero. Say so. */}
              {res.effectiveCandidates.some((c) => c.predictionSource === "unavailable") && (
                <Alert severity="warning" title="Some candidates were ranked at 0.0 ms">
                  No predictor was available for them, and the ranking field is not
                  optional — so `minimize: PredictedTTFT` compared fabricated zeroes.
                  Type a pTTFT into the table to rank on a real number.
                </Alert>
              )}

              {/* `Trace` carries no per-candidate attribute snapshot, so
                  without this the UI could not show WHY an exclusion matched. */}
              <div>
                <span style={label}>candidates as evaluated</span>
                <div style={{ marginTop: 4 }}>
                  {res.effectiveCandidates.map((c, i) => (
                    <div key={i} style={{ ...mono, color: "var(--fg-secondary)" }}>
                      {c.label}
                      {/* `label()` carries the UPSTREAM model. Two candidates can
                          share it and differ only in the canonical name — which
                          is the one `consider` filters on. */}
                      {c.canonicalModel !== c.upstreamModel && (
                        <span style={{ color: "var(--fg-primary)" }}> as {c.canonicalModel}</span>
                      )}
                      {" · "}queued={c.queuedPrompts ?? "—"} · kv={
                        c.kvCacheUsage != null ? c.kvCacheUsage.toFixed(2) : "—"
                      } · pTTFT={
                        c.predictedTtftMs != null ? c.predictedTtftMs.toFixed(1) : "—"
                      } · ctx={c.contextWindow ?? "—"}
                      <span style={{ opacity: 0.75 }}>
                        {" · "}{c.predictionSource}{c.predictionNote ? ` (${c.predictionNote})` : ""}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}
        </div>
      </Panel>
    </>
  );
};
