// kinference — dashboard panels
// KpiStrip · ScoreActivityPanel · OverlapPanel · WorkersTable · CallsTape · WorkerDrawer

import React from "react";
import { StatusDot, IconButton } from "./BuoyantPrimitives.jsx";
import {
  LineChart, Sparkline, KVBar, QueueBar, Heatmap,
  StackedDistribution, Ridgeline, ConcentrationMeter,
  fmtInt, fmtCtx, fmtPct, fmtMs, fmtAge, fmtSecPerReq,
} from "./charts.jsx";

// ─── Small helpers ──────────────────────────────────────────────
export const Panel = ({ title, eyebrow, action, children, padded = true, style }) => (
  <div style={{
    background: "var(--bg-surface)", borderRadius: "var(--radius)",
    border: "1px solid var(--border)", boxShadow: "var(--shadow-1)",
    display: "flex", flexDirection: "column", minWidth: 0, ...style,
  }}>
    <div style={{
      display: "flex", alignItems: "center", padding: "14px 20px",
      borderBottom: "1px solid var(--divider)", minHeight: 56,
    }}>
      <div style={{ flex: 1, minWidth: 0 }}>
        {eyebrow && (
          <div style={{ fontFamily: "var(--font-sans)", fontSize: 11, fontWeight: 600,
                        letterSpacing: "1px", textTransform: "uppercase",
                        color: "var(--fg-secondary)", whiteSpace: "nowrap",
                        overflow: "hidden", textOverflow: "ellipsis" }}>{eyebrow}</div>
        )}
        <div style={{ fontFamily: "var(--font-sans)", fontSize: 16, fontWeight: 500,
                      color: "var(--fg-primary)", whiteSpace: "nowrap",
                      overflow: "hidden", textOverflow: "ellipsis" }}>{title}</div>
      </div>
      {action && <div style={{ display: "flex", gap: 8, alignItems: "center" }}>{action}</div>}
    </div>
    <div style={{ padding: padded ? 20 : 0, minWidth: 0, flex: 1 }}>{children}</div>
  </div>
);

// Big metric tile (lives in the KPI strip)
export const Kpi = ({ label, value, unit, hint, color, hist, accent }) => (
  <div style={{
    flex: 1, minWidth: 0,
    background: "var(--bg-surface)", borderRadius: "var(--radius)",
    border: "1px solid var(--border)", padding: "14px 18px",
    display: "flex", flexDirection: "column", gap: 4,
    position: "relative", overflow: "hidden",
  }}>
    {accent && (
      <div style={{
        position: "absolute", left: 0, top: 0, bottom: 0, width: 3,
        background: accent,
      }} />
    )}
    <div style={{ fontFamily: "var(--font-sans)", fontSize: 10, fontWeight: 600,
                  letterSpacing: "0.8px", textTransform: "uppercase",
                  color: "var(--fg-secondary)", lineHeight: 1.25,
                  display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical",
                  overflow: "hidden", minHeight: 24 }}>{label}</div>
    <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
      <div style={{ fontFamily: "var(--font-mono)", fontSize: 28, fontWeight: 500,
                    color: color || "var(--fg-primary)", lineHeight: 1.1 }}>{value}</div>
      {unit && (
        <div style={{ fontFamily: "var(--font-sans)", fontSize: 12,
                      color: "var(--fg-secondary)" }}>{unit}</div>
      )}
    </div>
    <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", gap: 8, minHeight: 28 }}>
      {hint && (
        <div style={{ fontFamily: "var(--font-sans)", fontSize: 11, color: "var(--fg-secondary)",
                      whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
                      minWidth: 0, flex: 1 }}>
          {hint}
        </div>
      )}
      {hist && hist.length > 1 && (
        <div style={{ marginLeft: "auto", flexShrink: 0 }}>
          <Sparkline data={hist} color={color || "var(--brand)"} width={72} height={26} fill={color || "var(--brand)"} />
        </div>
      )}
    </div>
  </div>
);

// ─── KpiStrip ───────────────────────────────────────────────────
export const KpiStrip = ({ snap, window: windowSeconds }) => {
  const slice = (arr) => arr.slice(-windowSeconds);
  const workers = snap.workers;
  const okWorkers = workers.filter(w => w.health === "ok").length;
  const totalCacheBlocks = workers.reduce((a, w) => a + (w.num_gpu_blocks || 0), 0);
  const usedBlocks = workers.reduce((a, w) => a + (w.num_gpu_blocks || 0) * w.kv_cache_usage, 0);
  const fleetKv = usedBlocks / Math.max(1, totalCacheBlocks);

  // Total running + waiting + swapped across all workers (sums + sparkline history)
  const totalRunning = workers.reduce((a, w) => a + w.queue_running, 0);
  const totalWaiting = workers.reduce((a, w) => a + w.queue_waiting, 0);
  const totalSwapped = workers.reduce((a, w) => a + w.queue_swapped, 0);
  const sumHist = (key) => {
    const len = Math.min(...workers.map(w => w.hist[key].length));
    const sliced = workers.map(w => w.hist[key].slice(-Math.min(len, windowSeconds)));
    const out = new Array(sliced[0]?.length || 0).fill(0);
    sliced.forEach(arr => arr.forEach((v, i) => out[i] += v));
    return out;
  };
  const runningHist = sumHist("qr");
  const waitingHist = sumHist("qw");
  const swappedHist = sumHist("qs");

  // Engine-reported (not monitor-predicted) fleet hit rate — the sanity
  // check on whether prefix routing is paying off. Lives on the top row
  // because it's the headline routing-correctness signal; the
  // CacheIndexRow below is about the radix tree's internal health.
  // Sourced from snap.engine.prefixCacheHitRate (volume-weighted
  // Σhits/Σqueries across all workers — see telemetry.rs).
  const engine = snap.engine ?? {};
  const engineHitRate = engine.prefixCacheHitRate ?? 0;
  const engineQueriesTotal = engine.prefixCacheQueriesTotal ?? 0;
  const engineHitShown = engineQueriesTotal > 0;

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 16 }}>
      <Kpi label="running requests"
           value={fmtInt(totalRunning)}
           hint={`across ${workers.length} workers`}
           accent="var(--bel-link-green)"
           hist={runningHist} color="var(--bel-link-green)" />
      <Kpi label="waiting requests"
           value={fmtInt(totalWaiting)}
           hint={totalWaiting > 0 ? "queued, not yet decoding" : "no backpressure"}
           accent={totalWaiting > 60 ? "var(--bel-warning)" : "var(--bel-link-blue)"}
           color={totalWaiting > 60 ? "var(--bel-warning)" : "var(--fg-primary)"}
           hist={waitingHist} />
      <Kpi label="swapped requests"
           value={fmtInt(totalSwapped)}
           hint={totalSwapped > 0 ? "evicted from gpu kv cache" : "none swapped"}
           accent={totalSwapped > 0 ? "var(--bel-error)" : "var(--border)"}
           color={totalSwapped > 0 ? "var(--bel-error)" : "var(--fg-primary)"}
           hist={swappedHist} />
      <Kpi label="workers ok"
           value={`${okWorkers}/${workers.length}`}
           hint={okWorkers === workers.length ? "all reporting" : `${workers.length - okWorkers} unresponsive`}
           color={okWorkers === workers.length ? "var(--fg-primary)" : "var(--bel-warning)"}
           accent={okWorkers === workers.length ? "var(--bel-success)" : "var(--bel-warning)"} />
      <Kpi label="kv cache activation"
           value={(fleetKv * 100).toFixed(0)}
           unit="%"
           hint={`${fmtInt(usedBlocks)} blocks`}
           color={fleetKv > 0.88 ? "var(--bel-warning)" : "var(--fg-primary)"}
           accent={fleetKv > 0.88 ? "var(--bel-warning)" : "var(--bel-link-blue)"} />
      <Kpi label="worker cache hit rate"
           value={engineHitShown ? (engineHitRate * 100).toFixed(1) : "—"}
           unit={engineHitShown ? "%" : ""}
           hint={engineHitShown
             ? `${fmtInt(engineQueriesTotal)} queries · lifetime`
             : "engine has not reported any cache queries yet"}
           color={engineHitShown ? "var(--fg-primary)" : "var(--fg-secondary)"}
           accent={engineHitShown ? "var(--bel-link-green)" : "var(--border)"} />
    </div>
  );
};

// ─── CacheIndexRow — radix-tree (prefix index) health ───────────
// Sourced from the monitor's own index, distinct from the engines' /metrics.
// Block/worker gauges come from a throttled dump_events() (lag up to ~10s);
// the rates are 1 Hz from ingest counters.
export const CacheIndexRow = ({ snap, window: windowSeconds }) => {
  const idx = snap.index ?? {};
  const slice = (arr) => (arr ?? []).slice(-windowSeconds);
  const blocksHist = slice(idx.blocksHist);
  const eventsHist = slice(idx.eventsHist);
  const configured = snap.workers.length;
  const inIndex = idx.workers ?? 0;
  const allPresent = configured > 0 && inIndex >= configured;
  const stored = Math.round(idx.storedPerSec ?? 0);
  const removed = Math.round(idx.removedPerSec ?? 0);
  const cleared = Math.round(idx.clearedPerSec ?? 0);

  // (Engine-reported "Worker cache hit" KPI moved to KpiStrip on the
  // top row — it's the headline routing-correctness signal and
  // belongs alongside the other fleet-level health gauges, not in
  // the radix-tree internals row.)

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16 }}>
      <Kpi label="indexed blocks"
           value={fmtInt(idx.blocks ?? 0)}
           hint="radix tree · refreshed ~10s"
           accent="var(--bel-link-blue)"
           color="var(--bel-link-blue)"
           hist={blocksHist} />
      <Kpi label="index events / s"
           value={fmtInt(idx.eventsPerSec ?? 0)}
           hint={`${stored} stored · ${removed} removed${cleared ? ` · ${cleared} cleared` : ""} /s`}
           accent="var(--bel-link-green)"
           color="var(--bel-link-green)"
           hist={eventsHist} />
      <Kpi label="block churn / s"
           value={`+${fmtInt(idx.blocksStoredPerSec ?? 0)} / −${fmtInt(idx.blocksRemovedPerSec ?? 0)}`}
           hint="blocks added / evicted per second"
           accent="var(--bel-warning)" />
      <Kpi label="workers in index"
           value={`${inIndex}/${configured}`}
           hint={allPresent ? "all workers publishing" : `${configured - inIndex} not publishing events`}
           color={allPresent ? "var(--fg-primary)" : "var(--bel-warning)"}
           accent={allPresent ? "var(--bel-success)" : "var(--bel-warning)"} />
    </div>
  );
};

// ─── ScoreActivityPanel ─────────────────────────────────────────
export const ScoreActivityPanel = ({ snap, window: windowSeconds }) => {
  const ref = React.useRef(null);
  const [w, setW] = React.useState(640);
  React.useLayoutEffect(() => {
    const measure = () => { if (ref.current) setW(ref.current.offsetWidth); };
    measure();
    const ro = typeof ResizeObserver !== "undefined" ? new ResizeObserver(measure) : null;
    if (ro && ref.current) ro.observe(ref.current);
    window.addEventListener("resize", measure);
    return () => { window.removeEventListener("resize", measure); if (ro) ro.disconnect(); };
  }, []);

  const slice = (arr) => (arr ?? []).slice(-windowSeconds);
  const rps = slice(snap.score.rpsHist);
  const inflight = slice(snap.score.inflightHist);
  const p50 = slice(snap.score.p50Hist);
  const p95 = slice(snap.score.p95Hist);
  const p99 = slice(snap.score.p99Hist);
  // Response-code time series. okHist + degradedHist matches rpsHist
  // in steady state; errHist is the additional rejected requests not
  // counted in rpsHist (Status::not_found, invalid_argument, internal).
  const okHist = slice(snap.score.okHist);
  const degradedHist = slice(snap.score.degradedHist);
  const errHist = slice(snap.score.errHist);

  // Pick outcomes, surfaced as a one-line header annotation once this
  // process has served any picks.
  const picks = snap.picks ?? {};
  const pickTotal = picks.total ?? 0;
  const picksActive = pickTotal > 0;

  return (
    <Panel
      eyebrow={picksActive ? `inbound · ${fmtInt(pickTotal)} picks · ${fmtInt(picks.noOpinion ?? 0)} no-opinion · ${fmtInt(picks.notFound ?? 0)} no-workers` : "inbound"}
      title="score() activity"
    >
      <div ref={ref} style={{ display: "flex", flexDirection: "column", gap: 18 }}>
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 4, gap: 12, flexWrap: "wrap" }}>
            <div style={{ fontFamily: "var(--font-sans)", fontSize: 12, color: "var(--fg-secondary)", whiteSpace: "nowrap" }}>
              Throughput &amp; concurrency
            </div>
            <div style={{ display: "flex", gap: 12, flexShrink: 0 }}>
              <LegendDot color="var(--bel-link-green)" label="rps" />
              <LegendDot color="var(--bel-link-blue)" label="in-flight" />
            </div>
          </div>
          <LineChart
            width={Math.max(280, w)} height={150}
            series={[
              { data: rps,      color: "var(--bel-link-green)", fill: "var(--bel-link-green)" },
              { data: inflight.map(v => v * (Math.max(...rps, 1) / Math.max(...inflight, 1)) * 0.5), color: "var(--bel-link-blue)", dashed: true },
            ]}
            yMin={0}
            formatY={fmtInt}
          />
        </div>
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 4, gap: 12, flexWrap: "wrap" }}>
            <div style={{ fontFamily: "var(--font-sans)", fontSize: 12, color: "var(--fg-secondary)", whiteSpace: "nowrap" }}>
              score() latency
            </div>
            <div style={{ display: "flex", gap: 12, flexShrink: 0 }}>
              <LegendDot color="var(--brand)" label="p50" />
              <LegendDot color="var(--bel-warning)" label="p95" />
              <LegendDot color="var(--bel-error)" label="p99" />
            </div>
          </div>
          <LineChart
            width={Math.max(280, w)} height={110}
            series={[
              { data: p99, color: "var(--bel-error)", strokeWidth: 1.4 },
              { data: p95, color: "var(--bel-warning)", strokeWidth: 1.4 },
              { data: p50, color: "var(--brand)", strokeWidth: 1.8, fill: "var(--brand)" },
            ]}
            yMin={0}
            formatY={(v) => v.toFixed(1) + " ms"}
          />
        </div>
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 4, gap: 12, flexWrap: "wrap" }}>
            <div style={{ fontFamily: "var(--font-sans)", fontSize: 12, color: "var(--fg-secondary)", whiteSpace: "nowrap" }}>
              gRPC response codes
            </div>
            <div style={{ display: "flex", gap: 12, flexShrink: 0 }}>
              <LegendDot color="var(--bel-link-green)" label="ok" />
              <LegendDot color="var(--bel-warning)" label="degraded" />
              <LegendDot color="var(--bel-error)" label="error" />
            </div>
          </div>
          <LineChart
            width={Math.max(280, w)} height={90}
            series={[
              // Errors topmost so they're visible even when ok dominates.
              { data: errHist, color: "var(--bel-error)", strokeWidth: 1.4 },
              { data: degradedHist, color: "var(--bel-warning)", strokeWidth: 1.4 },
              { data: okHist, color: "var(--bel-link-green)", strokeWidth: 1.8, fill: "var(--bel-link-green)" },
            ]}
            yMin={0}
            formatY={fmtInt}
          />
        </div>
      </div>
    </Panel>
  );
};

// ─── TokenizeActivityPanel ──────────────────────────────────────
// Outbound counterpart to ScoreActivityPanel: surfaces what the
// monitor's /tokenize fanout is doing per second, with the same
// throughput / concurrency / latency shape plus a per-tick attempt-
// failure rate so fleet-side network or worker problems are visible
// before they degrade score() responses.
export const TokenizeActivityPanel = ({ snap, window: windowSeconds }) => {
  const ref = React.useRef(null);
  const [w, setW] = React.useState(640);
  React.useLayoutEffect(() => {
    const measure = () => { if (ref.current) setW(ref.current.offsetWidth); };
    measure();
    const ro = typeof ResizeObserver !== "undefined" ? new ResizeObserver(measure) : null;
    if (ro && ref.current) ro.observe(ref.current);
    window.addEventListener("resize", measure);
    return () => { window.removeEventListener("resize", measure); if (ro) ro.disconnect(); };
  }, []);

  const tk = snap.tokenize ?? {};
  const slice = (arr) => (arr ?? []).slice(-windowSeconds);
  const rps = slice(tk.rpsHist);
  const inflight = slice(tk.inflightHist);
  const p50 = slice(tk.p50Hist);
  const p95 = slice(tk.p95Hist);
  const p99 = slice(tk.p99Hist);
  const errorRate = slice(tk.errorRateHist);
  const total = tk.total ?? 0;
  const unavailable = tk.unavailableTotal ?? 0;
  // Production-mode (mode = "local") path breakdown. Visible when
  // the monitor has actually routed any requests through the local
  // tokenizer — i.e. operator has flipped tokenize.mode to "local".
  const localServed = tk.localServedTotal ?? 0;
  const httpFallback = tk.httpFallbackTotal ?? 0;
  const productionActive = localServed > 0 || httpFallback > 0;
  // Audit-mode (mode = "compare") side-by-side totals. Visible only
  // when comparison has actually run. Divergence count gets a
  // warning color when non-zero — that's the signal we're watching.
  const localMatches = tk.localMatchesTotal ?? 0;
  const localDivergences = tk.localDivergencesTotal ?? 0;
  const localErrors = tk.localErrorsTotal ?? 0;
  const scoutActive = localMatches > 0 || localDivergences > 0 || localErrors > 0;

  return (
    <Panel
      eyebrow={
        `outbound · ${fmtInt(total)} total · ${fmtInt(unavailable)} degraded` +
        (productionActive
          ? ` · ${fmtInt(localServed)} local-served / ${fmtInt(httpFallback)} fallback`
          : "") +
        (scoutActive
          ? ` · local: ${fmtInt(localMatches)} matched / ${fmtInt(localDivergences)} divergent` +
            (localErrors > 0 ? ` / ${fmtInt(localErrors)} errored` : "")
          : "")
      }
      title="Tokenize activity"
    >
      <div ref={ref} style={{ display: "flex", flexDirection: "column", gap: 18 }}>
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 4, gap: 12, flexWrap: "wrap" }}>
            <div style={{ fontFamily: "var(--font-sans)", fontSize: 12, color: "var(--fg-secondary)", whiteSpace: "nowrap" }}>
              Throughput &amp; concurrency
            </div>
            <div style={{ display: "flex", gap: 12, flexShrink: 0 }}>
              <LegendDot color="var(--bel-link-green)" label="rps" />
              <LegendDot color="var(--bel-link-blue)" label="in-flight" />
            </div>
          </div>
          <LineChart
            width={Math.max(280, w)} height={150}
            series={[
              { data: rps,      color: "var(--bel-link-green)", fill: "var(--bel-link-green)" },
              { data: inflight.map(v => v * (Math.max(...rps, 1) / Math.max(...inflight, 1)) * 0.5), color: "var(--bel-link-blue)", dashed: true },
            ]}
            yMin={0}
            formatY={fmtInt}
          />
        </div>
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 4, gap: 12, flexWrap: "wrap" }}>
            <div style={{ fontFamily: "var(--font-sans)", fontSize: 12, color: "var(--fg-secondary)", whiteSpace: "nowrap" }}>
              tokenize() latency
            </div>
            <div style={{ display: "flex", gap: 12, flexShrink: 0 }}>
              <LegendDot color="var(--brand)" label="p50" />
              <LegendDot color="var(--bel-warning)" label="p95" />
              <LegendDot color="var(--bel-error)" label="p99" />
            </div>
          </div>
          <LineChart
            width={Math.max(280, w)} height={110}
            series={[
              { data: p99, color: "var(--bel-error)", strokeWidth: 1.4 },
              { data: p95, color: "var(--bel-warning)", strokeWidth: 1.4 },
              { data: p50, color: "var(--brand)", strokeWidth: 1.8, fill: "var(--brand)" },
            ]}
            yMin={0}
            formatY={(v) => v.toFixed(1) + " ms"}
          />
        </div>
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 4, gap: 12, flexWrap: "wrap" }}>
            <div style={{ fontFamily: "var(--font-sans)", fontSize: 12, color: "var(--fg-secondary)", whiteSpace: "nowrap" }}>
              Attempt error rate
            </div>
            <div style={{ display: "flex", gap: 12, flexShrink: 0 }}>
              <LegendDot color="var(--bel-warning)" label="errors / attempts" />
            </div>
          </div>
          <LineChart
            width={Math.max(280, w)} height={90}
            series={[
              { data: errorRate, color: "var(--bel-warning)", fill: "var(--bel-warning)", strokeWidth: 1.4 },
            ]}
            yMin={0} yMax={1}
            formatY={fmtPct}
          />
        </div>
      </div>
    </Panel>
  );
};

export const LegendDot = ({ color, label }) => (
  <span style={{ display: "inline-flex", alignItems: "center", gap: 6,
                 fontFamily: "var(--font-sans)", fontSize: 12,
                 color: "var(--fg-secondary)" }}>
    <span style={{ width: 8, height: 8, borderRadius: "50%", background: color }} />
    {label}
  </span>
);

// ─── OverlapPanel (THE hero) ────────────────────────────────────
const OVERLAP_LABELS = ["0", "10", "20", "30", "40", "50", "60", "70", "80", "90"];

export const OverlapPanel = ({ snap, vizStyle, window: windowSeconds, onSelectWorker }) => {
  const ref = React.useRef(null);
  const [w, setW] = React.useState(640);
  React.useLayoutEffect(() => {
    const measure = () => { if (ref.current) setW(ref.current.offsetWidth); };
    measure();
    const ro = typeof ResizeObserver !== "undefined" ? new ResizeObserver(measure) : null;
    if (ro && ref.current) ro.observe(ref.current);
    window.addEventListener("resize", measure);
    return () => { window.removeEventListener("resize", measure); if (ro) ro.disconnect(); };
  }, []);

  // Aggregate per-worker buckets over the selected window. Sort rows by
  // worker id ascending so the heatmap rows line up with the Fleet /
  // Workers table below (which defaults to the same sort) — easy
  // eyeball cross-referencing between the two views.
  const workers = snap.workers
    .slice()
    .sort((a, b) => (a.id ?? "").localeCompare(b.id ?? ""));
  const matrix = workers.map(worker => {
    const slice = worker.hist.buckets.slice(-windowSeconds);
    const agg = new Array(snap.numBuckets).fill(0);
    slice.forEach(row => row.forEach((v, i) => agg[i] += v));
    return agg;
  });
  const rowLabels = workers.map(w => w.id);

  // Row labels (worker IDs — `namespace/pod-name` in K8s mode) often
  // exceed the legacy hardcoded 100px label gutter and get clipped by
  // the parent's `overflow: hidden`. Compute the gutter from the longest
  // label so labels always fit; the heatmap cells get whatever space is
  // left and shrink as necessary. Floor at 100px so empty/short-name
  // fleets don't shift layout.
  //
  // The text uses a 12px monospace font (var(--font-mono)); ~7.5px/char
  // is a safe upper-bound for typical mono advance widths, plus an 8px
  // right gutter the Heatmap renderer subtracts (it draws text at
  // `x = labelWidth - 8`).
  const labelWidth = Math.max(
    100,
    ...rowLabels.map(s => (s ?? "").length * 7.5 + 16)
  );

  // For concentration: how many calls per worker would have argmax overlap.
  // This is a *projection*, not a measurement — the monitor doesn't pick a
  // winner; it just reports per-worker scores.
  const winCounts = workers.map(worker => {
    const slice = worker.hist.wins.slice(-windowSeconds);
    return slice.reduce((a, b) => a + b, 0);
  });

  // hover tooltip
  const [hover, setHover] = React.useState(null);

  return (
    <Panel
      eyebrow="Cache overlap distribution"
      title="Prefix overlap map"
      action={
        <div style={{ display: "flex", gap: 14, alignItems: "center" }}>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--fg-secondary)" }}>
            {fmtInt(matrix.flat().reduce((a, b) => a + b, 0))} calls in window
          </span>
        </div>
      }
    >
      <div ref={ref} style={{
        display: "grid",
        gridTemplateColumns: w < 880 ? "1fr" : "minmax(0, 1fr) 230px",
        gap: 24, minWidth: 0,
      }}>
        <div style={{ minWidth: 0, position: "relative", overflow: "hidden" }}>
          {vizStyle === "heatmap" && (
            <Heatmap
              matrix={matrix} rowLabels={rowLabels}
              colLabels={OVERLAP_LABELS.map((l, i) => i === 0 ? "0–10%" : i === 9 ? "90+" : `${l}`)}
              width={Math.max(280, (w < 880 ? w - 8 : w - 254))} rowHeight={32} labelWidth={labelWidth}
              onCellHover={setHover}
              showValues
            />
          )}
          {vizStyle === "stacked" && (
            <StackedDistribution
              matrix={matrix} rowLabels={rowLabels}
              colLabels={OVERLAP_LABELS}
              width={Math.max(280, (w < 880 ? w - 8 : w - 254))} rowHeight={32} labelWidth={labelWidth}
            />
          )}
          {vizStyle === "ridgeline" && (
            <Ridgeline
              matrix={matrix} rowLabels={rowLabels}
              colLabels={OVERLAP_LABELS}
              width={Math.max(280, (w < 880 ? w - 8 : w - 254))} rowHeight={40} labelWidth={labelWidth} peakHeight={32}
            />
          )}
          {hover && vizStyle === "heatmap" && (
            <div style={{
              position: "absolute", right: 12, top: 0,
              background: "var(--bg-surface)", border: "1px solid var(--border)",
              borderRadius: "var(--radius)", padding: "6px 10px",
              boxShadow: "var(--shadow-4)", fontFamily: "var(--font-mono)", fontSize: 11,
              color: "var(--fg-primary)", pointerEvents: "none",
            }}>
              {rowLabels[hover.row]} · overlap {hover.col * 10}–{(hover.col + 1) * 10}%
              <div style={{ color: "var(--fg-secondary)" }}>
                {fmtInt(hover.value)} calls · {(hover.normalized * 100).toFixed(1)}% of row
              </div>
            </div>
          )}
        </div>
        <div style={{
          display: w < 880 ? "grid" : "flex",
          gridTemplateColumns: w < 880 ? "1fr 1fr" : undefined,
          flexDirection: "column",
          gap: 16,
        }}>
          <div style={{
            border: "1px solid var(--border)", borderRadius: "var(--radius)",
            padding: "14px 16px",
          }}>
            <div style={{ fontFamily: "var(--font-sans)", fontSize: 11, fontWeight: 600,
                          letterSpacing: "1px", textTransform: "uppercase",
                          color: "var(--fg-secondary)", marginBottom: 10 }}>
              Top-overlap concentration
            </div>
            <ConcentrationMeter counts={winCounts} />
            <div style={{ marginTop: 12, fontFamily: "var(--font-sans)", fontSize: 12,
                          color: "var(--fg-secondary)", lineHeight: 1.5 }}>
              If callers routed each request to the worker with the highest overlap, this
              is how concentrated the load would be. The monitor doesn't pick — it just
              reports per-worker scores.
            </div>
          </div>
        </div>
      </div>
    </Panel>
  );
};

// ─── WorkersTable ───────────────────────────────────────────────
export const WorkersTable = ({ snap, onSelect, window: windowSeconds }) => {
  const [sort, setSort] = React.useState({ key: "id", dir: "asc" });
  // Shared scale for the queue bars. Tracks the high-watermark of
  // (running + waiting + swapped) across ALL bars seen since the page
  // loaded, so the scale only ever grows — a depth-12 spike makes the
  // scale climb to 12 (rounded up to the next multiple of 4 to avoid
  // jitter), and subsequent quiet ticks keep that scale instead of
  // re-shrinking the bars and visually misrepresenting how busy the
  // fleet just was. Resets to the floor of 4 on hard refresh, which is
  // an acceptable trade-off per the spec.
  //
  // `useRef` (not `useState`) because we don't need a re-render when the
  // high-water mark advances — the parent's 500ms snapshot poll already
  // re-renders us, and the ref read on the next render picks up the new
  // value. Writing to a ref mid-render is fine when guarded like this
  // (idempotent monotonic update); React's docs explicitly bless the
  // pattern.
  const queueHighWaterRef = React.useRef(4);
  const fleetQueueRaw = snap.workers.reduce(
    (m, w) => Math.max(m, w.queue_waiting + w.queue_running + w.queue_swapped),
    0
  );
  const quantized = Math.max(4, Math.ceil(Math.max(fleetQueueRaw, 1) / 4) * 4);
  if (quantized > queueHighWaterRef.current) {
    queueHighWaterRef.current = quantized;
  }
  const fleetQueueMax = queueHighWaterRef.current;
  // Synthetic sort keys for derived columns. `seconds_per_req` is the
  // displayed reciprocal of `completion_rate_per_sec` — we map zeros to
  // +Infinity so workers without a rate sort as "slowest" rather than
  // "fastest" (a 0 req/s worker isn't faster than a 5 s/req worker; we just
  // don't know its rate yet).
  const sortValue = (w, key) => {
    if (key === "model_id") return w.served_models?.[0]?.name ?? "";
    if (key === "max_model_len") return w.served_models?.[0]?.max_model_len ?? 0;
    if (key === "seconds_per_req") {
      const r = w.completion_rate_per_sec ?? 0;
      return r > 0 ? 1 / r : Number.POSITIVE_INFINITY;
    }
    if (key === "prefix_cache_hit_rate") {
      // Workers with zero queries don't have a meaningful rate yet;
      // map them to -1 so they sort to the bottom (asc) / top (desc)
      // as "unknown" rather than competing with measured zeros.
      const q = w.prefix_cache_queries_total ?? 0;
      return q > 0 ? (w.prefix_cache_hit_rate ?? 0) : -1;
    }
    return w[key];
  };
  const rows = snap.workers.slice().sort((a, b) => {
    const av = sortValue(a, sort.key), bv = sortValue(b, sort.key);
    if (av == null) return 1; if (bv == null) return -1;
    if (typeof av === "string") return sort.dir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
    return sort.dir === "asc" ? av - bv : bv - av;
  });

  const Header = ({ k, label, align = "left", w }) => (
    <th onClick={() => setSort({ key: k, dir: sort.key === k && sort.dir === "desc" ? "asc" : "desc" })}
      style={{
        textAlign: align, padding: "10px 16px", fontSize: 11, fontWeight: 600,
        textTransform: "uppercase", letterSpacing: "0.8px",
        color: "var(--fg-secondary)", borderBottom: "1px solid var(--divider)",
        cursor: "pointer", userSelect: "none", whiteSpace: "nowrap", width: w,
        background: sort.key === k ? "var(--hover-bg)" : "transparent",
      }}>
      <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
        {label}
        {sort.key === k && (
          <span className="material-icons" style={{ fontSize: 14 }}>
            {sort.dir === "asc" ? "arrow_drop_up" : "arrow_drop_down"}
          </span>
        )}
      </span>
    </th>
  );

  const healthDot = (h) => h === "ok"
    ? <StatusDot status="ok" />
    : <StatusDot status="warning" />;

  return (
    <Panel
      eyebrow="Fleet"
      title="Workers"
      padded={false}
      action={
        <>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--fg-secondary)", whiteSpace: "nowrap" }}>
            {snap.workers.length} workers
          </span>
          <IconButton icon="filter_list" iconSize={18} size={32} />
        </>
      }
    >
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "var(--font-sans)" }}>
          <thead>
            <tr>
              <Header k="id" label="Worker" />
              <Header k="model_id" label="Model" />
              <Header k="max_model_len" label="Max ctx" align="right" />
              <Header k="kv_cache_usage" label="KV cache" w="220" />
              <Header k="prefix_cache_hit_rate" label="Cache hit" align="right" />
              <Header k="queue_running" label="Queue (run/wait/swap)" w="220" />
              <Header k="seconds_per_req" label="s/req" align="right" />
              <Header k="metrics_age_ms" label="Scrape age" align="right" />
              <Header k="num_gpu_blocks" label="Blocks" align="right" />
              <th style={{ width: 40 }}></th>
            </tr>
          </thead>
          <tbody>
            {rows.map(w => (
              <WorkerRow key={w.id} w={w} window={windowSeconds}
                queueMax={fleetQueueMax}
                healthDot={healthDot(w.health)} onClick={() => onSelect(w.id)} />
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  );
};

const WorkerRow = ({ w, healthDot, onClick, queueMax, window: windowSeconds }) => {
  const [hover, setHover] = React.useState(false);
  return (
    <tr onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)} onClick={onClick}
      style={{
        background: hover ? "var(--hover-bg)" : "transparent", cursor: "pointer",
        transition: "background .12s var(--ease-standard)",
      }}>
      <td style={{ padding: "12px 16px", borderBottom: "1px solid var(--divider)", whiteSpace: "nowrap" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          {healthDot}
          <div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 13, fontWeight: 500, color: "var(--fg-primary)", whiteSpace: "nowrap" }}>
              {w.id}
            </div>
            <div style={{ fontFamily: "var(--font-sans)", fontSize: 11, color: "var(--fg-secondary)", whiteSpace: "nowrap" }}>
              {w.namespace} · {w.ip}:{w.port}
            </div>
          </div>
        </div>
      </td>
      {(() => {
        const sm = w.served_models ?? [];
        const first = sm[0];
        const extra = sm.length > 1 ? ` +${sm.length - 1}` : "";
        return (
          <>
            <td style={{ padding: "12px 16px", borderBottom: "1px solid var(--divider)",
                         fontFamily: "var(--font-mono)", fontSize: 12,
                         color: first ? "var(--fg-primary)" : "var(--fg-secondary)",
                         whiteSpace: "nowrap", maxWidth: 280,
                         overflow: "hidden", textOverflow: "ellipsis" }}
                title={sm.map(m => `${m.name} · ${m.max_model_len || "?"}`).join("\n")}>
              {first ? first.name : "—"}
              {extra && <span style={{ color: "var(--fg-secondary)", marginLeft: 4 }}>{extra}</span>}
            </td>
            <td style={{ padding: "12px 16px", borderBottom: "1px solid var(--divider)",
                         fontFamily: "var(--font-mono)", fontSize: 12, textAlign: "right",
                         color: first && first.max_model_len ? "var(--fg-primary)" : "var(--fg-secondary)" }}>
              {first && first.max_model_len ? fmtCtx(first.max_model_len) : "—"}
            </td>
          </>
        );
      })()}
      <td style={{ padding: "10px 16px", borderBottom: "1px solid var(--divider)" }}>
        <KVBar value={w.kv_cache_usage} hist={w.hist.kv.slice(-windowSeconds)} width={200} />
      </td>
      {(() => {
        // Engine-reported prefix-cache hit rate. Render `—` when the
        // engine hasn't reported any queries yet (older vLLM, SGLang on
        // the first scrape, or genuinely zero traffic in the window).
        // Hover tooltip includes the cumulative query volume so a
        // single number isn't misleading (50% over 5 vs 50% over 10k).
        const q = w.prefix_cache_queries_total ?? 0;
        const r = w.prefix_cache_hit_rate ?? 0;
        const shown = q > 0;
        return (
          <td
            style={{ padding: "12px 16px", borderBottom: "1px solid var(--divider)",
                     fontFamily: "var(--font-mono)", fontSize: 12, textAlign: "right",
                     color: shown ? "var(--fg-primary)" : "var(--fg-secondary)" }}
            title={shown ? `${(r * 100).toFixed(1)}% (${fmtInt(q)} queries cumulative)` : "engine has not reported any cache queries yet"}
          >
            {shown ? `${(r * 100).toFixed(1)}%` : "—"}
          </td>
        );
      })()}
      <td style={{ padding: "10px 16px", borderBottom: "1px solid var(--divider)" }}>
        <QueueBar
          waiting={w.queue_waiting} running={w.queue_running} swapped={w.queue_swapped}
          width={140} max={queueMax}
        />
      </td>
      <td style={{ padding: "12px 16px", borderBottom: "1px solid var(--divider)",
                   fontFamily: "var(--font-mono)", fontSize: 12, textAlign: "right",
                   color: (w.completion_rate_per_sec ?? 0) > 0 ? "var(--fg-primary)" : "var(--fg-secondary)" }}>
        {fmtSecPerReq(w.completion_rate_per_sec)}
      </td>
      <td style={{ padding: "12px 16px", borderBottom: "1px solid var(--divider)",
                   fontFamily: "var(--font-mono)", fontSize: 12, textAlign: "right",
                   color: w.metrics_age_ms > 4000 ? "var(--bel-warning)" : "var(--fg-primary)" }}>
        {fmtAge(w.metrics_age_ms)}
      </td>
      <td style={{ padding: "12px 16px", borderBottom: "1px solid var(--divider)",
                   fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--fg-secondary)", textAlign: "right" }}>
        {fmtInt(w.num_gpu_blocks)}
      </td>
      <td style={{ padding: "0 12px", borderBottom: "1px solid var(--divider)", textAlign: "right" }}>
        <span className="material-icons" style={{ fontSize: 18, color: "var(--navy-a38)" }}>chevron_right</span>
      </td>
    </tr>
  );
};

// ─── CallsTape — recent score() calls ───────────────────────────
export const CallsTape = ({ snap }) => {
  return (
    <Panel eyebrow="Live" title="Recent score() calls" padded={false}
      action={
        <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--fg-secondary)" }}>
          {fmtInt(snap.score.total)} total
        </span>
      }>
      <div style={{ maxHeight: 360, overflowY: "auto" }}>
        {snap.recentCalls.slice(0, 14).map((c, i) => {
          const ovrColor = c.topOverlap > 0.6 ? "var(--bel-link-green)"
                          : c.topOverlap > 0.3 ? "var(--bel-warning)" : "var(--bel-error)";
          return (
            <div key={c.ts + "-" + i + "-" + c.topWorker}
                 style={{
                   padding: "10px 16px",
                   borderBottom: "1px solid var(--divider)",
                   display: "flex", flexDirection: "column", gap: 4,
                   opacity: 1 - i * 0.02,
                 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, fontFamily: "var(--font-mono)", fontSize: 12 }}>
                <span style={{ color: "var(--fg-secondary)", minWidth: 36 }}>t-{snap.ts - c.ts}s</span>
                <span style={{ width: 6, height: 6, background: "var(--bel-link-green)", borderRadius: "50%", flexShrink: 0 }} />
                <span style={{ color: "var(--fg-primary)", flex: 1, minWidth: 0,
                               overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{c.topWorker}</span>
                <span style={{ color: ovrColor, fontWeight: 500, whiteSpace: "nowrap" }}>
                  {(c.topOverlap * 100).toFixed(0)}%
                </span>
              </div>
              <div style={{ display: "flex", alignItems: "baseline", gap: 8, fontSize: 11,
                             fontFamily: "var(--font-sans)", color: "var(--fg-secondary)" }}>
                <span style={{ fontFamily: "var(--font-mono)", flex: 1, minWidth: 0,
                               overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {c.model}
                </span>
                <span style={{ fontFamily: "var(--font-mono)", whiteSpace: "nowrap" }}>
                  {fmtInt(c.tokens)} tok · {c.matched_blocks}/{c.total_blocks} blk
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </Panel>
  );
};

// ─── WorkerDrawer — per-worker inspector ────────────────────────
export const WorkerDrawer = ({ snap, workerId, onClose, window: windowSeconds }) => {
  const w = snap.workers.find(x => x.id === workerId);
  if (!w) return null;
  const slice = (a) => a.slice(-windowSeconds);

  const overlapHist = (() => {
    const buckets = slice(w.hist.buckets);
    const agg = new Array(snap.numBuckets).fill(0);
    buckets.forEach(row => row.forEach((v, i) => agg[i] += v));
    return agg;
  })();
  const totalCalls = overlapHist.reduce((a, b) => a + b, 0);
  const meanOverlap = totalCalls > 0
    ? overlapHist.reduce((a, b, i) => a + b * (i + 0.5) / snap.numBuckets, 0) / totalCalls
    : 0;
  const cacheHitShare = overlapHist.slice(5).reduce((a, b) => a + b, 0) / Math.max(1, totalCalls);

  return (
    <>
      <div onClick={onClose} style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)",
        zIndex: 99, animation: "fade-in .2s var(--ease-standard)",
      }} />
      <aside style={{
        position: "fixed", top: 0, right: 0, bottom: 0,
        width: 540, maxWidth: "100vw",
        background: "var(--bg-surface)",
        borderLeft: "1px solid var(--border)",
        boxShadow: "var(--shadow-16)", zIndex: 100,
        display: "flex", flexDirection: "column", minWidth: 0,
        animation: "slide-in-right .25s var(--ease-decelerate)",
      }}>
        {/* header */}
        <div style={{ padding: "20px 24px", borderBottom: "1px solid var(--divider)",
                       display: "flex", alignItems: "flex-start", gap: 12 }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <StatusDot status={w.health === "ok" ? "ok" : "warning"} size={10} />
              <div style={{ fontFamily: "var(--font-mono)", fontSize: 18, fontWeight: 500, color: "var(--fg-primary)" }}>
                {w.id}
              </div>
            </div>
            <div style={{ marginTop: 4, fontFamily: "var(--font-sans)", fontSize: 13, color: "var(--fg-secondary)" }}>
              {w.pool} · {w.namespace} · {w.ip}:{w.port}
            </div>
          </div>
          <IconButton icon="close" iconSize={20} onClick={onClose} />
        </div>
        {/* body */}
        <div style={{ flex: 1, overflowY: "auto", padding: 24, display: "flex", flexDirection: "column", gap: 24 }}>
          {/* Static + derived facts */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12 }}>
            <Fact label="block_size" value={w.block_size} />
            <Fact label="num_gpu_blocks" value={fmtInt(w.num_gpu_blocks)} />
            <Fact label="kv_cache_usage" value={(w.kv_cache_usage * 100).toFixed(1) + "%"}
                  color={w.kv_cache_usage > 0.9 ? "var(--bel-warning)" : "var(--fg-primary)"} />
            <Fact label="metrics_age_ms" value={fmtAge(w.metrics_age_ms)}
                  color={w.metrics_age_ms > 4000 ? "var(--bel-warning)" : "var(--fg-primary)"} />
          </div>

          {/* KV cache */}
          <DrawerSection title="KV cache utilization">
            <div style={{ display: "flex", gap: 16, alignItems: "center", marginBottom: 12 }}>
              <KVBar value={w.kv_cache_usage} hist={slice(w.hist.kv)} width={300} height={26} />
              <div style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--fg-secondary)" }}>
                {fmtInt(w.num_gpu_blocks * w.kv_cache_usage)} / {fmtInt(w.num_gpu_blocks)} blocks pinned
              </div>
            </div>
            <LineChart series={[{ data: slice(w.hist.kv), color: "var(--bel-link-green)", fill: "var(--bel-link-green)" }]}
                       width={490} height={110} yMin={0} yMax={1} formatY={fmtPct} />
          </DrawerSection>

          {/* Queues */}
          <DrawerSection title="Queue depth">
            <LineChart series={[
              { data: slice(w.hist.qr), color: "var(--bel-link-blue)", fill: "var(--bel-link-blue)" },
              { data: slice(w.hist.qw), color: "var(--bel-warning)" },
              { data: slice(w.hist.qs), color: "var(--bel-error)", dashed: true },
            ]}
              width={490} height={120} yMin={0} formatY={fmtInt} />
            <div style={{ marginTop: 8, display: "flex", gap: 16, flexWrap: "wrap" }}>
              <LegendDot color="var(--bel-link-blue)" label={`running ${w.queue_running}`} />
              <LegendDot color="var(--bel-warning)" label={`waiting ${w.queue_waiting}`} />
              <LegendDot color="var(--bel-error)" label={`swapped ${w.queue_swapped}`} />
            </div>
          </DrawerSection>

          {/* Engine-reported prefix-cache hit rate (the routing sanity
              check: when this stays flat at 0 while the overlap heatmap
              shows healthy prediction, routing or tokenization is the
              culprit). */}
          <DrawerSection
            title="Engine cache hit rate"
            hint={(w.prefix_cache_queries_total ?? 0) > 0
              ? `${((w.prefix_cache_hit_rate ?? 0) * 100).toFixed(1)}% · ${fmtInt(w.prefix_cache_queries_total)} queries lifetime`
              : "engine has not reported any cache queries yet"}>
            <LineChart series={[
              { data: slice(w.hist.phr ?? []),
                color: "var(--bel-link-green)",
                fill: "var(--bel-link-green)" },
            ]}
              width={490} height={110} yMin={0} yMax={1} formatY={fmtPct} />
          </DrawerSection>

          {/* Overlap distribution received by THIS worker */}
          <DrawerSection
            title="Overlap distribution received"
            hint={`mean ${(meanOverlap * 100).toFixed(0)}% · ${(cacheHitShare * 100).toFixed(0)}% of calls at ≥50% overlap`}>
            <svg width={490} height={140} viewBox="0 0 490 140">
              {(() => {
                const max = Math.max(...overlapHist, 1);
                return overlapHist.map((v, i) => {
                  const x = 6 + i * ((490 - 12) / overlapHist.length);
                  const bw = (490 - 12) / overlapHist.length - 4;
                  const bh = (v / max) * 110;
                  const color = i >= 7 ? "var(--bel-link-green)" :
                                i >= 5 ? "var(--bel-link-blue)" :
                                i >= 2 ? "var(--bel-warning)" : "var(--bel-error)";
                  return (
                    <g key={i}>
                      <rect x={x} y={120 - bh} width={bw} height={bh} fill={color} opacity="0.85" rx="2" />
                      <text x={x + bw / 2} y={134} fontSize="10" textAnchor="middle"
                            fill="var(--fg-secondary)" fontFamily="var(--font-mono)">
                        {i * 10}
                      </text>
                    </g>
                  );
                });
              })()}
            </svg>
          </DrawerSection>

          {/* Served-models breakdown — one row per id reported by /v1/models. */}
          <DrawerSection
            title="Served models"
            hint="From /v1/models — refreshed periodically. Multiple entries appear when LoRA adapters are loaded.">
            {(w.served_models ?? []).length === 0 ? (
              <div style={{ fontFamily: "var(--font-sans)", fontSize: 12,
                            color: "var(--fg-secondary)" }}>
                no models reported (engine hasn't responded to /v1/models, or first refresh pending)
              </div>
            ) : (
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: "6px 0", fontSize: 10, fontWeight: 600,
                                 letterSpacing: "0.8px", textTransform: "uppercase",
                                 color: "var(--fg-secondary)" }}>model id</th>
                    <th style={{ textAlign: "right", padding: "6px 0", fontSize: 10, fontWeight: 600,
                                 letterSpacing: "0.8px", textTransform: "uppercase",
                                 color: "var(--fg-secondary)", width: 120 }}>max ctx (tokens)</th>
                  </tr>
                </thead>
                <tbody>
                  {(w.served_models ?? []).map(m => (
                    <tr key={m.name}>
                      <td style={{ padding: "6px 0", fontFamily: "var(--font-mono)",
                                   fontSize: 12, color: "var(--fg-primary)",
                                   wordBreak: "break-all" }}>{m.name}</td>
                      <td style={{ padding: "6px 0", fontFamily: "var(--font-mono)",
                                   fontSize: 12, textAlign: "right",
                                   color: m.max_model_len ? "var(--fg-primary)" : "var(--fg-secondary)" }}>
                        {m.max_model_len ? fmtCtx(m.max_model_len) : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </DrawerSection>

          {/* Static metadata */}
          <DrawerSection title="Worker configuration">
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <tbody>
                {[
                  ["pod", w.id],
                  ["namespace", w.namespace],
                  ["endpoint", `${w.ip}:${w.port}`],
                  ["model (configured)", w.pool],
                  ["block_size", w.block_size],
                  ["num_gpu_blocks", fmtInt(w.num_gpu_blocks)],
                  ["resolved cache", `${fmtInt(w.num_gpu_blocks * w.block_size)} tokens`],
                  ["seconds per request", (w.completion_rate_per_sec ?? 0) > 0
                    ? `${fmtSecPerReq(w.completion_rate_per_sec)} s/req` : "—"],
                ].map(([k, v]) => (
                  <tr key={k}>
                    <td style={{ padding: "6px 0", fontFamily: "var(--font-mono)", fontSize: 12,
                                 color: "var(--fg-secondary)", width: 160 }}>{k}</td>
                    <td style={{ padding: "6px 0", fontFamily: "var(--font-mono)", fontSize: 12,
                                 color: "var(--fg-primary)" }}>{v}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </DrawerSection>
        </div>
      </aside>
    </>
  );
};

const Fact = ({ label, value, color }) => (
  <div style={{
    padding: "10px 12px", background: "var(--bg-surface-alt)",
    borderRadius: "var(--radius)", border: "1px solid var(--border)",
  }}>
    <div style={{ fontFamily: "var(--font-sans)", fontSize: 10, fontWeight: 600, letterSpacing: "0.8px",
                  textTransform: "uppercase", color: "var(--fg-secondary)" }}>{label}</div>
    <div style={{ marginTop: 4, fontFamily: "var(--font-mono)", fontSize: 14, color: color || "var(--fg-primary)" }}>{value}</div>
  </div>
);

const DrawerSection = ({ title, hint, children }) => (
  <div>
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 12 }}>
      <div style={{ fontFamily: "var(--font-sans)", fontSize: 13, fontWeight: 500, color: "var(--fg-primary)" }}>
        {title}
      </div>
      {hint && (
        <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--fg-secondary)" }}>
          {hint}
        </div>
      )}
    </div>
    {children}
  </div>
);
