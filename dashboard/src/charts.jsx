// kinference — SVG chart primitives
// Lightweight inline charts: LineChart, Sparkline, StackBar, BlockBar, Heatmap.
// Designed to inherit Buoyant color tokens. Brand-aware (uses --brand,
// --bel-link-green, --bel-link-blue, --bel-warning, --bel-error).

import React from "react";

// ─── format helpers ───────────────────────────────────────────
export const fmtInt = (n) => {
  if (n == null) return "—";
  if (Math.abs(n) >= 1000) return (n / 1000).toFixed(n >= 10000 ? 0 : 1) + "k";
  return Math.round(n).toString();
};
// Format an LLM context-window length (in tokens) the way models are
// marketed. LLM context windows are conventionally powers of 2
// (8192, 32768, 131072) — `fmtInt(32768)` rounds 32.768 to "33k",
// which is mathematically right but wrong for what an operator
// expects ("32k"). Use base-1024 when the value is power-of-2-divisible
// so 32768 → "32k", 131072 → "128k". Fall back to base-1000 floor
// for non-power-of-2 values like Claude's 200000 → "200k".
export const fmtCtx = (n) => {
  if (n == null || n === 0) return "—";
  if (n < 1000) return n.toString();
  if (n % 1024 === 0) {
    const k = n / 1024;
    return k >= 1024 ? (k / 1024).toFixed(0) + "M" : k.toFixed(0) + "k";
  }
  const k = Math.floor(n / 1000);
  return k >= 1000 ? Math.floor(k / 1000).toString() + "M" : k.toString() + "k";
};
export const fmtPct = (n) => (n == null) ? "—" : (n * 100).toFixed(0) + "%";
export const fmtMs  = (n) => (n == null) ? "—" : (n < 10 ? n.toFixed(1) : Math.round(n)) + " ms";
export const fmtAge = (ms) => {
  if (ms == null) return "—";
  if (ms < 1500) return `${Math.round(ms)} ms`;
  if (ms < 60000) return `${(ms/1000).toFixed(1)} s`;
  return `${Math.round(ms/60000)} m`;
};
// Display the inverse of a req/s rate as seconds-per-request, with
// resolution that adapts to magnitude — 30 s/req renders "30", 5 s/req
// renders "5.0", 0.3 s/req renders "0.30". `0` or `null` → "—".
export const fmtSecPerReq = (rate) => {
  if (rate == null || rate <= 0) return "—";
  const s = 1 / rate;
  if (s >= 10) return s.toFixed(0);
  if (s >= 1)  return s.toFixed(1);
  return s.toFixed(2);
};

// ─── LineChart ───────────────────────────────────────────────
// Multi-series, no axis labels by default — annotation lives in panel headers.
// series: [{ data: number[], color, fill?, dashed? }]
// data points are evenly spaced; lower-right is most recent.
export const LineChart = ({
  series, width = 600, height = 160,
  // Y-axis tick labels render right-anchored at `x = padding.l - 6` and
  // extend leftward; if `padding.l` is too small for the formatter's
  // output, the leading character clips against the SVG viewBox. 64
  // fits the widest realistic labels — `"9999.9 ms"`, `"12.3k"`, or
  // `"100%"` — with breathing room across font-rendering variability
  // (sans-serif glyph widths aren't fully predictable, and 56 turned
  // out to be a hair too tight for `"100%"` in the worker drawer).
  // Narrower-label charts (fmtPct, fmtInt) sacrifice ~36px of plot-
  // area width, which is a small price for never-clipped labels and
  // consistent vertical alignment across stacked charts.
  padding = { t: 8, r: 8, b: 18, l: 64 },
  yMin = null, yMax = null, gridLines = 4, xTicks = null, formatY = fmtInt,
  showGrid = true,
}) => {
  const w = width, h = height;
  const innerW = w - padding.l - padding.r;
  const innerH = h - padding.t - padding.b;

  // Determine y-bounds
  let lo = yMin, hi = yMax;
  if (lo == null || hi == null) {
    let allVals = [];
    series.forEach(s => allVals = allVals.concat(s.data));
    if (!allVals.length) allVals = [0, 1];
    const min = Math.min(...allVals), max = Math.max(...allVals);
    if (lo == null) lo = Math.min(0, min);
    if (hi == null) hi = max === lo ? lo + 1 : max + (max - lo) * 0.15;
  }
  const yScale = (v) => padding.t + innerH - ((v - lo) / (hi - lo || 1)) * innerH;

  // Pick the longest series as the x reference
  const n = Math.max(...series.map(s => s.data.length), 1);
  const xScale = (i) => padding.l + (i / Math.max(1, n - 1)) * innerW;

  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} style={{ display: "block" }}>
      {showGrid && Array.from({ length: gridLines + 1 }).map((_, i) => {
        const y = padding.t + (i / gridLines) * innerH;
        const v = hi - (i / gridLines) * (hi - lo);
        return (
          <g key={i}>
            <line x1={padding.l} x2={w - padding.r} y1={y} y2={y}
                  stroke="var(--divider)" strokeWidth="1" strokeDasharray={i === gridLines ? "" : "2 3"} />
            <text x={padding.l - 6} y={y + 3} fontSize="10" textAnchor="end"
                  fill="var(--fg-secondary)" fontFamily="var(--font-sans)">
              {formatY(v)}
            </text>
          </g>
        );
      })}
      {xTicks && xTicks.map((t, i) => {
        const x = padding.l + (t.pos) * innerW;
        return (
          <text key={i} x={x} y={h - 4} fontSize="10" textAnchor="middle"
                fill="var(--fg-secondary)" fontFamily="var(--font-sans)">{t.label}</text>
        );
      })}
      {series.map((s, si) => {
        const pts = s.data.map((v, i) => `${xScale(i + (n - s.data.length))},${yScale(v)}`).join(" ");
        const areaPts = s.data.length
          ? `${xScale(n - s.data.length)},${padding.t + innerH} ${pts} ${xScale(n - 1)},${padding.t + innerH}`
          : "";
        return (
          <g key={si}>
            {s.fill && (
              <polygon points={areaPts} fill={s.fill} opacity="0.18" />
            )}
            <polyline points={pts} fill="none" stroke={s.color}
                      strokeWidth={s.strokeWidth || 1.75}
                      strokeDasharray={s.dashed ? "4 3" : ""}
                      strokeLinecap="round" strokeLinejoin="round" />
          </g>
        );
      })}
    </svg>
  );
};

// ─── Sparkline ───────────────────────────────────────────────
export const Sparkline = ({ data, color = "var(--brand)", width = 120, height = 28, fill, strokeWidth = 1.5, baseline = false }) => {
  if (!data || !data.length) return <svg width={width} height={height} />;
  const min = Math.min(...data), max = Math.max(...data);
  const lo = baseline ? Math.min(0, min) : min;
  const hi = max === lo ? lo + 1 : max;
  const xs = (i) => (i / Math.max(1, data.length - 1)) * (width - 2) + 1;
  const ys = (v) => height - 2 - ((v - lo) / (hi - lo || 1)) * (height - 4);
  const pts = data.map((v, i) => `${xs(i)},${ys(v)}`).join(" ");
  const area = `${xs(0)},${height - 1} ${pts} ${xs(data.length - 1)},${height - 1}`;
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} style={{ display: "block" }}>
      {fill && <polygon points={area} fill={fill} opacity="0.20" />}
      <polyline points={pts} fill="none" stroke={color} strokeWidth={strokeWidth}
                strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
};

// ─── KVBar — utilization bar with mini sparkline overlay ─────
export const KVBar = ({ value, hist, height = 22, width = 180 }) => {
  const pct = Math.max(0, Math.min(1, value || 0));
  const color = pct > 0.92 ? "var(--bel-error)" : pct > 0.82 ? "var(--bel-warning)" : "var(--bel-link-green)";
  return (
    <div style={{ position: "relative", width, height, background: "var(--navy-a08)",
                  borderRadius: 3, overflow: "hidden" }}>
      <div style={{
        position: "absolute", left: 0, top: 0, bottom: 0, width: `${pct * 100}%`,
        background: color, opacity: 0.55,
        transition: "width .8s var(--ease-standard)",
      }} />
      {hist && hist.length > 1 && (
        <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`}
             style={{ position: "absolute", inset: 0 }} preserveAspectRatio="none">
          {(() => {
            const lo = 0, hi = 1;
            const xs = (i) => (i / Math.max(1, hist.length - 1)) * width;
            const ys = (v) => height - ((v - lo) / (hi - lo)) * height;
            const pts = hist.map((v, i) => `${xs(i)},${ys(v)}`).join(" ");
            return <polyline points={pts} fill="none" stroke={color} strokeWidth="1.4" strokeLinejoin="round" />;
          })()}
        </svg>
      )}
      <div style={{
        position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "flex-end",
        paddingRight: 8, fontFamily: "var(--font-mono)", fontSize: 11, fontWeight: 500,
        color: "var(--fg-primary)", textShadow: "0 0 4px var(--bg-surface)",
      }}>
        {(pct * 100).toFixed(0)}%
      </div>
    </div>
  );
};

// ─── QueueBar — stacked horizontal bar: waiting / running / swapped ─
export const QueueBar = ({ waiting = 0, running = 0, swapped = 0, max = 80, width = 180, height = 18 }) => {
  const total = waiting + running + swapped;
  const norm = Math.min(1, total / max);
  const seg = (v) => total === 0 ? 0 : (v / total) * norm * width;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{
        width, height, background: "var(--navy-a08)", borderRadius: 3,
        display: "flex", overflow: "hidden",
      }}>
        <div style={{ width: seg(running), background: "var(--bel-link-blue)", transition: "width .5s" }} />
        <div style={{ width: seg(waiting), background: "var(--bel-warning)",   transition: "width .5s" }} />
        <div style={{ width: seg(swapped), background: "var(--bel-error)",     transition: "width .5s" }} />
      </div>
      <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--fg-secondary)", minWidth: 56, whiteSpace: "nowrap" }}>
        <span style={{ color: "var(--bel-link-blue)" }}>{running}</span>
        <span style={{ opacity: 0.5 }}> / </span>
        <span style={{ color: "var(--bel-warning)" }}>{waiting}</span>
        <span style={{ opacity: 0.5 }}> / </span>
        <span style={{ color: "var(--bel-error)" }}>{swapped}</span>
      </div>
    </div>
  );
};

// ─── Heatmap — workers × overlap buckets ──────────────────────
// matrix: number[rows][cols]. Normalized per row by default (shape of distribution).
// Color uses Linkerd gradient — green→blue, mapped from 0→1.
export const Heatmap = ({
  matrix, rowLabels, colLabels,
  normalize = "row", // "row" | "absolute" | "global"
  width = 760, rowHeight = 30, labelWidth = 130,
  cellGap = 2, showValues = false,
  onCellHover = null, highlightRow = null,
}) => {
  const rows = matrix.length;
  const cols = matrix[0]?.length || 1;
  const innerW = width - labelWidth - 36;
  const cellW = (innerW - (cols - 1) * cellGap) / cols;
  const height = rows * rowHeight + 32;

  // Normalize
  const norm = matrix.map((row, ri) => {
    if (normalize === "row") {
      const sum = row.reduce((a, b) => a + b, 0) || 1;
      return row.map(v => v / sum);
    }
    if (normalize === "global") {
      const max = Math.max(...matrix.flat()) || 1;
      return row.map(v => v / max);
    }
    return row;
  });

  // Color scale — emphasises low values with a clear teal→blue ramp
  const colorFor = (v) => {
    if (v <= 0) return "rgba(255,255,255,0.03)";
    const eased = Math.pow(v, 0.45);
    // hue 165 (teal) → 210 (blue) → 200 high
    const hue = 165 + eased * 45;
    const sat = 55 + eased * 35;
    const light = 22 + eased * 36;
    const alpha = 0.45 + 0.55 * eased;
    return `hsla(${hue}, ${sat}%, ${light}%, ${alpha})`;
  };

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} style={{ display: "block" }}>
      {/* column header */}
      {colLabels && colLabels.map((c, i) => (
        <text key={i} x={labelWidth + i * (cellW + cellGap) + cellW / 2} y={14}
              fontSize="10" textAnchor="middle"
              fill="var(--fg-secondary)" fontFamily="var(--font-mono)">{c}</text>
      ))}
      {/* rows */}
      {norm.map((row, ri) => (
        <g key={ri} transform={`translate(0, ${24 + ri * rowHeight})`}>
          {/* row highlight */}
          {highlightRow === ri && (
            <rect x={0} y={-3} width={width} height={rowHeight} fill="var(--hover-bg)" />
          )}
          {/* label */}
          <text x={labelWidth - 8} y={rowHeight / 2 + 4} fontSize="12" textAnchor="end"
                fill="var(--fg-primary)" fontFamily="var(--font-mono)">{rowLabels[ri]}</text>
          {/* cells */}
          {row.map((v, ci) => {
            const x = labelWidth + ci * (cellW + cellGap);
            return (
              <g key={ci}>
                <rect x={x} y={2} width={cellW} height={rowHeight - 4}
                      fill={colorFor(v)} rx="2"
                      onMouseEnter={() => onCellHover?.({ row: ri, col: ci, value: matrix[ri][ci], normalized: v })}
                      onMouseLeave={() => onCellHover?.(null)} />
                {showValues && v > 0.04 && (
                  <text x={x + cellW / 2} y={rowHeight / 2 + 4}
                        fontSize="9" textAnchor="middle" fontFamily="var(--font-mono)"
                        fill={v > 0.5 ? "rgba(0,30,60,0.9)" : "var(--fg-primary)"} opacity="0.85">
                    {Math.round(v * 100)}
                  </text>
                )}
              </g>
            );
          })}
        </g>
      ))}
    </svg>
  );
};

// ─── StackedDistribution — vertical stacked bars per server ──
// Renders one stacked bar per row (server), showing the share of each
// overlap bucket. Alternative to heatmap.
export const StackedDistribution = ({
  matrix, rowLabels, colLabels,
  width = 760, rowHeight = 26, labelWidth = 130,
}) => {
  const innerW = width - labelWidth - 12;
  const height = matrix.length * rowHeight + 28;

  // Bucket colors — gradient from low-overlap (warn red-ish) to high (link-green)
  const bucketColor = (i, n) => {
    const t = i / Math.max(1, n - 1);
    // red → orange → teal → green
    if (t < 0.4) return `rgba(211, 47, 47, ${0.18 + t * 0.6})`;
    if (t < 0.7) return `rgba(255, 152, 0, ${0.25 + t * 0.4})`;
    return `rgba(43, 237, 167, ${0.35 + t * 0.6})`;
  };

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} style={{ display: "block" }}>
      {/* legend */}
      {colLabels && colLabels.map((c, i) => (
        <g key={i}>
          <rect x={labelWidth + (i / colLabels.length) * innerW}
                y={6} width={(innerW / colLabels.length) - 2} height={6}
                fill={bucketColor(i, colLabels.length)} />
          <text x={labelWidth + (i / colLabels.length) * innerW + ((innerW / colLabels.length) - 2) / 2}
                y={22} fontSize="9" textAnchor="middle"
                fill="var(--fg-secondary)" fontFamily="var(--font-mono)">{c}</text>
        </g>
      ))}
      {matrix.map((row, ri) => {
        const sum = row.reduce((a, b) => a + b, 0) || 1;
        let x = labelWidth;
        return (
          <g key={ri} transform={`translate(0, ${30 + ri * rowHeight})`}>
            <text x={labelWidth - 8} y={rowHeight / 2 + 4} fontSize="12" textAnchor="end"
                  fill="var(--fg-primary)" fontFamily="var(--font-mono)">{rowLabels[ri]}</text>
            {row.map((v, ci) => {
              const w = (v / sum) * innerW;
              const r = <rect key={ci} x={x} y={2} width={w} height={rowHeight - 6}
                              fill={bucketColor(ci, row.length)} />;
              x += w;
              return r;
            })}
          </g>
        );
      })}
    </svg>
  );
};

// ─── Ridgeline — overlapping density per worker ──────────────
export const Ridgeline = ({
  matrix, rowLabels, colLabels,
  width = 760, rowHeight = 34, labelWidth = 130, peakHeight = 30,
}) => {
  const innerW = width - labelWidth - 12;
  const height = matrix.length * rowHeight + 30;
  // Stroke color cycles thru a small navy/green/blue palette
  const palette = [
    "var(--bel-link-green)", "var(--bel-link-blue)", "var(--bel-warning)",
    "var(--bel-success-light)", "var(--bel-info-light)", "var(--bel-error-light)",
    "var(--brand)", "var(--bel-navy-300)",
  ];

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} style={{ display: "block" }}>
      {colLabels && colLabels.map((c, i) => (
        <text key={i} x={labelWidth + (i + 0.5) / colLabels.length * innerW}
              y={14} fontSize="9" textAnchor="middle"
              fill="var(--fg-secondary)" fontFamily="var(--font-mono)">{c}</text>
      ))}
      {matrix.map((row, ri) => {
        const max = Math.max(...row, 1);
        const y0 = 24 + ri * rowHeight + peakHeight;
        // smoothed path
        const pts = row.map((v, ci) => {
          const x = labelWidth + (ci + 0.5) / row.length * innerW;
          const y = y0 - (v / max) * peakHeight;
          return [x, y];
        });
        // closing area path
        const area = [
          `M ${labelWidth} ${y0}`,
          ...pts.map(([x, y]) => `L ${x} ${y}`),
          `L ${labelWidth + innerW} ${y0}`,
          "Z",
        ].join(" ");
        const line = "M " + pts.map(([x, y]) => `${x} ${y}`).join(" L ");
        const color = palette[ri % palette.length];
        return (
          <g key={ri}>
            <path d={area} fill={color} opacity="0.16" />
            <path d={line} fill="none" stroke={color} strokeWidth="1.5" strokeLinejoin="round" />
            <text x={labelWidth - 8} y={y0 - 4} fontSize="12" textAnchor="end"
                  fill="var(--fg-primary)" fontFamily="var(--font-mono)">{rowLabels[ri]}</text>
          </g>
        );
      })}
    </svg>
  );
};

// ─── Concentration meter ─────────────────────────────────────
// Single-axis visualization of how concentrated traffic is on one worker.
// Uses normalized Gini coefficient over per-worker win share.
export const concentrationGini = (counts) => {
  const sum = counts.reduce((a, b) => a + b, 0);
  if (sum === 0) return 0;
  const sorted = counts.slice().sort((a, b) => a - b);
  let cum = 0, area = 0;
  const n = sorted.length;
  sorted.forEach((v, i) => {
    cum += v;
    area += cum / sum;
  });
  const fair = (n + 1) / 2;
  return Math.max(0, Math.min(1, (fair - area / 1) / fair * 2));
};

export const ConcentrationMeter = ({ counts, height = 60 }) => {
  const sum = counts.reduce((a, b) => a + b, 1);
  const shares = counts.map(c => c / sum).sort((a, b) => b - a);
  const gini = concentrationGini(counts);
  // narrative label
  let label = "Well balanced", color = "var(--bel-success)";
  if (gini > 0.55) { label = "Highly concentrated", color = "var(--bel-error)"; }
  else if (gini > 0.32) { label = "Skewed", color = "var(--bel-warning)"; }

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
      <div style={{ flex: 1 }}>
        <div style={{ display: "flex", gap: 2, height: height - 18, alignItems: "flex-end" }}>
          {shares.map((s, i) => (
            <div key={i} style={{
              flex: 1,
              height: `${Math.max(2, s * 100)}%`,
              background: i === 0 ? color : "var(--navy-a24)",
              borderRadius: 2,
            }} />
          ))}
        </div>
        <div style={{
          marginTop: 6, fontFamily: "var(--font-sans)", fontSize: 11,
          color: "var(--fg-secondary)", display: "flex", justifyContent: "space-between",
        }}>
          <span>top worker {(shares[0] * 100).toFixed(0)}%</span>
          <span>n = {counts.length}</span>
        </div>
      </div>
      <div style={{ textAlign: "right" }}>
        <div style={{ fontFamily: "var(--font-mono)", fontSize: 22, fontWeight: 500, color }}>
          {gini.toFixed(2)}
        </div>
        <div style={{ fontFamily: "var(--font-sans)", fontSize: 11, color: "var(--fg-secondary)", textTransform: "uppercase", letterSpacing: 1 }}>
          gini · {label}
        </div>
      </div>
    </div>
  );
};
