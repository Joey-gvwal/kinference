#!/usr/bin/env python3
"""
Diagnostic analysis for the picker's per-request latency CSV.

The CSV the picker emits is a 42-column, headerless file where every
row carries:
  - the picker's prediction inputs (overlap, blocks, queue depth,
    kv-cache usage, completion rate, q_running, ...)
  - the prediction components it produced (base_ms, q_ms, prefill_ms,
    cache_ms, q_running_ms, predicted_ms)
  - the observed TTFT it later measured (observed_ms, residual_ms)
  - the full PickerConfig snapshot that drove the prediction
    (queue_ms_per_entry, alpha, weight, attention coefficients,
    cache pressure, ...) so replay is fully self-contained.

The current predictor is FIVE components:
    predicted_ms = base_ms + q_ms + prefill_ms + cache_ms + q_running_ms

`q_running_ms` is the U-shape penalty applied at pick time based on
the worker's scraped `queue_running` count plus inflight tracking —
it captures the "scheduler-tick danger zone" between fully-idle and
fully-saturated where a worker has just enough decode load to
contend for GPU but not enough to absorb new prefills.

The prefill model is QUADRATIC now:
    prefill_ms = a · uncached + c · uncached · total
where `a = attention_uncached_block_ms` (per-uncached-block setup) and
`c = attention_uncached_per_every_block_ms` (attention scales with
prompt length, so cost grows with uncached × total).

Because every config knob is stored per-row, the analysis can REPLAY
alternative tuning choices (different alpha, weight, attention
coefficients, ...) purely from the CSV — no external state needed.
The replay sweeps are the headline use of this script.

Backward-compat: 38- and 39-column files emitted by older versions are
still accepted. Missing trailing columns (`attention_uncached_block_ms`,
`attention_uncached_per_every_block_ms`, `q_running_ms`) are filled
in as 0.0 and the legacy linear `prefill_ms_per_block` is used during
replay for those rows.

Knobs the script can tune from the CSV alone:
  - expected_overlap_alpha           (α-sweep)
  - queue_workload_weight            (w-sweep)
  - base_ms                          (refit)
  - queue_ms_per_entry               (refit)
  - attention_uncached_block_ms      (refit, new quadratic-aware)
  - attention_uncached_per_every_block_ms (refit, new)
  - cache_pressure_penalty_ms        (refit)
  - saturated_cold_ms_per_entry      (cold-sat regime advisory)

Knobs the script can't yet tune via replay (the values are now
captured in cols 40-48, but no automated sweep is implemented; see
the `report_q_running` and `report_replication` sections for
diagnostic-only signals on whether these knobs are correctly shaped):
  - q_running_penalty_peak           (count)        — captured col 41
  - q_running_penalty_peak_ms        (latency)      — captured col 40
  - q_running_penalty_falloff        (count)        — captured col 42
  - inflight_concurrent_capacity                    — captured col 44
  - forward_pass_ms (decode-contention coef)        — captured col 45
  - replication_threshold_pct / warm_target / warm_threshold_fraction
                                                    — captured cols 46-48

Sections produced (severity-graded advisory at the end of each):

  I.   Sanity & shape
       shape, errors, sanity, residual distribution
  II.  Per-input attribution
       per-worker bias, per-component accuracy (5-way),
       residual-feature correlations
  III. Regime detection
       q_waiting/q_inflight/hit-vs-miss/overlap-ratio splits,
       cold-saturation gate, q_running U-shape penalty
  IV.  Replay (the headline feature)
       alpha sweep, weight sweep, joint alpha-weight sweep,
       coefficient refit (quadratic prefill aware)
  V.   Asymmetry detection
       inflight-overlap credit failures,
       workload-overcharge in q_waiting=0 regime
  VI.  Operational
       outliers (with request_id), herding, concurrency

Connection-failure rows (status != 200) are filtered out of all
analyses except `report_errors`, since their observed_ms doesn't
measure TTFT.

The replay APIs (`load_csv`, `replay_alpha`, `replay_weight`,
`replay_coefficients`) are exported for use from Jupyter or other
scripts:

    import analyze_latency as al
    df = al.load_csv("latency.csv")
    df2 = al.replay_alpha(df, 0.6)
    df3 = al.replay_weight(df2, 0.3)
    print(df3["residual_ms"].abs().mean())

Usage:
  ./analyze_latency.py path/to/latency.csv
  ./analyze_latency.py path/to/latency.csv --top-n 10
  ./analyze_latency.py path/to/latency.csv --plot /tmp/plots
  ./analyze_latency.py path/to/latency.csv --alpha-grid 11 --weight-grid 11
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from typing import Optional

import numpy as np
import pandas as pd

# Pandas/numpy emits a harmless RuntimeWarning when computing
# correlations against a column that's constant (e.g. q_swapped == 0
# always). We already detect the resulting NaN and print "n/a" — drop
# the noise from stderr.
warnings.filterwarnings("ignore", category=RuntimeWarning, module="numpy")


# ============================================================================
# Schema
# ============================================================================

# 50-column positional schema (current). The CSV is emitted
# headerless (but we auto-detect and skip a header line if one's
# present, for transition-period files).
#
# Schema-version history:
#   50 cols (current): added q_running U-shape + inflight-discount +
#                       replication knobs to the snapshot, plus
#                       replication_fired boolean.
#   40 cols (prior):   [predictor]+[picker] config split; legacy
#                       prefill_ms_per_block + queue_workload_weight
#                       columns dropped.
#   42 cols (transient): added quadratic prefill + q_running_ms
#                       coefs to the original 39-col format.
#   39 cols: added x_request_id to the original 38-col format.
#   38 cols: original.
COLUMNS = [
    # Identity (cols 0-4)
    "unix_ms",                          # 0
    "name",                             # 1
    "endpoint",                         # 2
    "status",                           # 3
    "upstream_error",                   # 4
    # Prediction components the picker produced (cols 5-10)
    "predicted_ms",                     # 5
    "base_ms",                          # 6
    "q_ms",                             # 7
    "q_rate_ms_per_entry",              # 8
    "prefill_ms",                       # 9
    "cache_ms",                         # 10
    # Outcome (cols 11-12)
    "observed_ms",                      # 11
    "residual_ms",                      # 12
    # Inputs the picker scored on (cols 13-20)
    "overlap_blocks",                   # 13  (layered, post-α, post-cache-credit)
    "total_blocks",                     # 14
    "q_waiting",                        # 15
    "q_inflight",                       # 16  (shadow count at pick time)
    "q_running",                        # 17
    "q_swapped",                        # 18
    "kv_cache_usage",                   # 19
    "compl_per_s",                      # 20
    # Diagnostic inputs / replay-affording (cols 21-27)
    "kv_blocks",                        # 21  (raw radix-tree match)
    "q_workload",                       # 22  (Σ uncached_at_dispatch)
    "request_id",                       # 23  (matches req=N in logs)
    "q_ms_constant",                    # 24  (q at weight=0)
    "q_ms_workload",                    # 25  (q at weight=1)
    "inflight_overlap_blocks",          # 26  ("o0;o1;..." semicolon-list, oldest first)
    "expected_overlap_alpha",           # 27  (α used at this pick)
    # PredictorConfig snapshot (cols 28-33)
    "queue_ms_per_entry",               # 28
    "cache_pressure_penalty_ms",        # 29
    "saturated_cold_ms_per_entry",      # 30
    "cache_pressure_saturated_threshold",  # 31
    "attention_uncached_block_ms",      # 32  (a, linear per-uncached-block)
    "attention_uncached_per_every_block_ms",  # 33  (c, quadratic uncached × total)
    # PickerConfig snapshot (cols 34-37)
    "top_k_min",                        # 34
    "top_k_fraction",                   # 35
    "inflight_tracking",                # 36  ("true"/"false")
    "tie_tolerance_ms",                 # 37
    # Per-request annotations (cols 38-40)
    "x_request_id",                     # 38  (caller-supplied; empty when unset)
    "q_running_ms",                     # 39  (U-shape penalty applied at this pick)
    "decode_contention_ms",             # 40  (flat per-active-decode cost applied at this pick)
    # PredictorConfig snapshot, q_running U-shape + capacity + decode (cols 41-45)
    "q_running_penalty_peak_ms",        # 41
    "q_running_penalty_peak_qr",        # 42
    "q_running_penalty_falloff_qr",     # 43
    "inflight_concurrent_capacity",     # 44
    "forward_pass_ms",                  # 45  (coefficient for decode_contention_ms)
    # PickerConfig snapshot, replication (cols 46-48)
    "replication_threshold_pct",        # 46
    "replication_warm_target",          # 47
    "replication_warm_threshold_fraction",  # 48
    # Per-request annotation (col 49) — true when the picker swapped
    # the normal pick for a cold-idle worker to seed cache.
    "replication_fired",                # 49  ("true"/"false")
    # Provenance (cols 50-51) — which declaration and which tuning produced
    # this row. `profile` is what makes a per-profile refit meaningful when
    # several worker sets share one profile.
    "workerset",                        # 50
    "profile",                          # 51
]
assert len(COLUMNS) == 52, f"COLUMNS has {len(COLUMNS)} entries, expected 52"

# Column counts we know how to load.
CURRENT_52_COLS = 52    # added workerset + profile provenance
PRIOR_50_COLS = 50      # added q_running/inflight-discount/replication + replication_fired
PRIOR_40_COLS = 40      # [predictor]+[picker] split, legacy cols dropped
TRANSIENT_42_COLS = 42  # had queue_workload_weight + prefill_ms_per_block; both dropped on load
LEGACY_38_COLS = 38     # pre-x_request_id
LEGACY_39_COLS = 39     # pre-quadratic-prefill, pre-q_running
ACCEPTED_COL_COUNTS = (
    LEGACY_38_COLS, LEGACY_39_COLS,
    PRIOR_40_COLS, TRANSIENT_42_COLS,
    PRIOR_50_COLS, CURRENT_52_COLS,
)

# Columns to coerce to numeric on load. Everything not listed stays
# as the raw string (name, endpoint, upstream_error,
# inflight_overlap_blocks parsed separately, inflight_tracking
# kept as the literal "true"/"false" string).
NUMERIC_COLS = {
    "unix_ms", "status",
    "predicted_ms", "base_ms", "q_ms", "q_rate_ms_per_entry",
    "prefill_ms", "cache_ms",
    "observed_ms", "residual_ms",
    "overlap_blocks", "total_blocks",
    "q_waiting", "q_inflight", "q_running", "q_swapped",
    "kv_cache_usage", "compl_per_s",
    "kv_blocks", "q_workload", "request_id",
    "q_ms_constant", "q_ms_workload",
    "expected_overlap_alpha",
    "queue_ms_per_entry",
    "cache_pressure_penalty_ms",
    "saturated_cold_ms_per_entry", "cache_pressure_saturated_threshold",
    "attention_uncached_block_ms",
    "attention_uncached_per_every_block_ms",
    "top_k_min", "top_k_fraction", "tie_tolerance_ms",
    "q_running_ms",
    "decode_contention_ms",
    # q_running U-shape + capacity + decode-contention snapshot (cols 41-45).
    "q_running_penalty_peak_ms",
    "q_running_penalty_peak_qr",
    "q_running_penalty_falloff_qr",
    "inflight_concurrent_capacity",
    "forward_pass_ms",
    # Replication snapshot (cols 46-48). col 49 (replication_fired)
    # is bool, kept as a string by `dtype=str` on read.
    "replication_threshold_pct",
    "replication_warm_target",
    "replication_warm_threshold_fraction",
    # Legacy compat — loader pads to constant when absent. Numeric so
    # downstream replay/sweep code doesn't blow up parsing strings.
    "queue_workload_weight",
    "prefill_ms_per_block",
}


# ============================================================================
# Severity-graded advisory scaffolding (kept verbatim from old script)
# ============================================================================

#   OK       — no action needed.
#   LOW      — informational; address if convenient.
#   MEDIUM   — moderate signal; worth addressing.
#   HIGH     — clear actionable signal; should address.
#   CRITICAL — production-impacting; address before next deploy.
#   BLOCKING — fix this before trusting any other analysis output.
SEVERITY_WIDTH = 8


def section(title: str) -> None:
    print()
    print(f"=== {title} ===")


def advise(severity: str, message: str) -> None:
    print(f"  → [{severity:<{SEVERITY_WIDTH}}] {message}")


def fmt(x: float) -> str:
    return f"{x:+.1f}" if abs(x) < 1e9 else "n/a"


# ============================================================================
# Loader
# ============================================================================

def load_csv(path: str) -> pd.DataFrame:
    """Load a 40-column picker latency CSV (or a legacy variant).

    Returns a DataFrame with typed columns, plus derived helpers:
      * `ts`                  pandas Timestamp from unix_ms
      * `uncached_blocks`     total_blocks - overlap_blocks
      * `kv_uncached`         total_blocks - kv_blocks
      * `inflight_overlap_list`  list[int] parsed from col 26
      * `cache_hit`           "hit" if overlap_blocks > 0 else "miss"
      * `regime_q`            "q=0" if q_waiting == 0 else "q>0"
      * `regime_inflight`     "in=0" if q_inflight == 0 else "in>0"
      * `cold_sat`            bool: cold-saturation gate fired this row
      * `inflight_active`     bool: q_inflight > 0
      * `overlap_ratio`       overlap_blocks / max(total_blocks, 1)
      * `is_legacy_csv`       bool: True iff loaded from a <40-col
                              file (pre-quadratic-prefill rows)

    Accepts four schema versions:
      * 40 cols (current — [predictor]+[picker] config split)
      * 42 cols (transient — included legacy queue_workload_weight
                 + prefill_ms_per_block; both dropped on load)
      * 39 cols (pre-quadratic-prefill, pre-q_running)
      * 38 cols (oldest, pre-x_request_id)

    Missing trailing columns are filled with 0.0 / "". For sub-40-
    column files the legacy `prefill_ms_per_block` is read into a
    temporary column so the quadratic-prefill fallback inside
    `replay_alpha` still produces meaningful predictions.
    """
    # First-row sniff: if it looks like a header (literal column names,
    # potentially with a different column count than the data rows
    # below it), skip it. This commonly happens when an emitter was
    # updated mid-run and the first line was carried over from a
    # previous schema version.
    skiprows: list[int] = []
    with open(path, "r") as f:
        first_line = f.readline().rstrip("\n")
    if first_line and not _is_numeric_str(first_line.split(",", 1)[0]):
        skiprows = [0]
    df = pd.read_csv(
        path,
        header=None,
        dtype=str,
        keep_default_na=False,
        skiprows=skiprows,
    )
    n = df.shape[1]
    is_legacy_csv = False
    if n == CURRENT_52_COLS:
        df.columns = COLUMNS
    elif n == PRIOR_50_COLS:
        # 50-col schema: predates the workerset/profile provenance columns.
        # Left empty rather than guessed — a per-profile refit on rows that
        # never recorded one would silently pool unrelated fleets.
        df.columns = COLUMNS[:PRIOR_50_COLS]
        df["workerset"] = ""
        df["profile"] = ""
    elif n == PRIOR_40_COLS:
        # 40-col schema: the q_running U-shape + inflight-discount +
        # replication snapshot columns hadn't been added yet, and
        # replication_fired wasn't captured. Pad with 0s / "false".
        df.columns = COLUMNS[:PRIOR_40_COLS]
        df["q_running_penalty_peak_ms"] = "0"
        df["q_running_penalty_peak_qr"] = "0"
        df["q_running_penalty_falloff_qr"] = "0"
        df["inflight_concurrent_capacity"] = "0"
        df["decode_contention_ms"] = "0"
        df["forward_pass_ms"] = "0"
        df["replication_threshold_pct"] = "0"
        df["replication_warm_target"] = "0"
        df["replication_warm_threshold_fraction"] = "0"
        df["replication_fired"] = "false"
        df["workerset"] = ""
        df["profile"] = ""
        df = df[COLUMNS]
    elif n == TRANSIENT_42_COLS:
        # 42-col transient: drop legacy queue_workload_weight (col 28)
        # and prefill_ms_per_block (col 29), then pad the same trailing
        # 10 cols as PRIOR_40_COLS.
        df = df.drop(df.columns[[28, 29]], axis=1).reset_index(drop=True)
        df.columns = COLUMNS[:PRIOR_40_COLS]
        df["q_running_penalty_peak_ms"] = "0"
        df["q_running_penalty_peak_qr"] = "0"
        df["q_running_penalty_falloff_qr"] = "0"
        df["inflight_concurrent_capacity"] = "0"
        df["decode_contention_ms"] = "0"
        df["forward_pass_ms"] = "0"
        df["replication_threshold_pct"] = "0"
        df["replication_warm_target"] = "0"
        df["replication_warm_threshold_fraction"] = "0"
        df["replication_fired"] = "false"
        df["workerset"] = ""
        df["profile"] = ""
        df = df[COLUMNS]
    elif n == LEGACY_39_COLS:
        is_legacy_csv = True
        # 38 identity/prediction/inputs cols + 9 picker_cfg cols + x_request_id.
        # Quadratic-prefill + q_running cols didn't exist; the 9-col
        # picker_cfg in that schema included queue_workload_weight (col 28)
        # and prefill_ms_per_block (col 29) — drop those, and keep the
        # mapped rest.
        df = df.drop(df.columns[[28, 29]], axis=1).reset_index(drop=True)
        # Now 37 cols. The remaining picker_cfg block (cols 28-32) maps
        # to the new schema's [predictor] snapshot (queue_ms_per_entry,
        # cache_pressure_penalty_ms, saturated_cold_ms_per_entry,
        # cache_pressure_saturated_threshold) + the picker block
        # (top_k_min, top_k_fraction, inflight_tracking, tie_tolerance_ms)
        # + x_request_id (col 36).
        df.columns = [
            *COLUMNS[:28],
            "queue_ms_per_entry",
            "cache_pressure_penalty_ms",
            "saturated_cold_ms_per_entry",
            "cache_pressure_saturated_threshold",
            "top_k_min",
            "top_k_fraction",
            "inflight_tracking",
            "tie_tolerance_ms",
            "x_request_id",
        ]
        df["attention_uncached_block_ms"] = "0"
        df["attention_uncached_per_every_block_ms"] = "0"
        df["q_running_ms"] = "0"
        df["q_running_penalty_peak_ms"] = "0"
        df["q_running_penalty_peak_qr"] = "0"
        df["q_running_penalty_falloff_qr"] = "0"
        df["inflight_concurrent_capacity"] = "0"
        df["decode_contention_ms"] = "0"
        df["forward_pass_ms"] = "0"
        df["replication_threshold_pct"] = "0"
        df["replication_warm_target"] = "0"
        df["replication_warm_threshold_fraction"] = "0"
        df["replication_fired"] = "false"
        # Reorder to canonical.
        df = df[COLUMNS]
    elif n == LEGACY_38_COLS:
        is_legacy_csv = True
        # Same shape as LEGACY_39 minus the trailing x_request_id col.
        df = df.drop(df.columns[[28, 29]], axis=1).reset_index(drop=True)
        df.columns = [
            *COLUMNS[:28],
            "queue_ms_per_entry",
            "cache_pressure_penalty_ms",
            "saturated_cold_ms_per_entry",
            "cache_pressure_saturated_threshold",
            "top_k_min",
            "top_k_fraction",
            "inflight_tracking",
            "tie_tolerance_ms",
        ]
        df["x_request_id"] = ""
        df["attention_uncached_block_ms"] = "0"
        df["attention_uncached_per_every_block_ms"] = "0"
        df["q_running_ms"] = "0"
        df["q_running_penalty_peak_ms"] = "0"
        df["q_running_penalty_peak_qr"] = "0"
        df["q_running_penalty_falloff_qr"] = "0"
        df["inflight_concurrent_capacity"] = "0"
        df["decode_contention_ms"] = "0"
        df["forward_pass_ms"] = "0"
        df["replication_threshold_pct"] = "0"
        df["replication_warm_target"] = "0"
        df["replication_warm_threshold_fraction"] = "0"
        df["replication_fired"] = "false"
        df["workerset"] = ""
        df["profile"] = ""
        df = df[COLUMNS]
    else:
        sys.exit(
            f"expected one of {ACCEPTED_COL_COUNTS} columns, got {n}.\n"
            f"first row sample: "
            f"{df.iloc[0].tolist() if len(df) else '[empty file]'}\n"
            f"the CSV format requires the 52-column positional schema; "
            f"see the COLUMNS docstring for the legacy variants we still accept."
        )
    if is_legacy_csv:
        print(
            f"note: loaded a {n}-column legacy CSV; quadratic-prefill + "
            f"q_running_ms columns filled with 0. Coefficient refits "
            f"interpret prefill cost via the linear `attention_uncached_block_ms` "
            f"term only — the quadratic refit needs a current-schema CSV.",
            file=sys.stderr,
        )
    # The picker no longer carries `queue_workload_weight` (the
    # constant-vs-workload blend was superseded by a position-discount
    # model) or the legacy `prefill_ms_per_block` (the quadratic model
    # replaced it). Both columns are gone from the CSV. The
    # weight-sweep + legacy-prefill code paths still expect them as
    # DataFrame columns, though — pad with frozen defaults so those
    # sections degrade to "no signal" cleanly instead of KeyErroring.
    if "queue_workload_weight" not in df.columns:
        df["queue_workload_weight"] = "1.0"
    if "prefill_ms_per_block" not in df.columns:
        df["prefill_ms_per_block"] = "0.0"

    # Header-row auto-detect: first row's unix_ms isn't a number → skip.
    if len(df) and not _is_numeric_str(df["unix_ms"].iloc[0]):
        df = df.iloc[1:].reset_index(drop=True)
    if df.empty:
        sys.exit("no data rows after header detection")

    # Coerce numeric columns. NaN propagates for cells that fail to
    # parse; we explicitly catch failed `unix_ms` and drop those rows
    # with a warning (most common cause: emitter column drift).
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    bad_mask = df["unix_ms"].isna()
    if bad_mask.any():
        n_bad = int(bad_mask.sum())
        sample = df.loc[bad_mask].iloc[:3].to_dict("records")
        print(
            f"warning: {n_bad} of {len(df)} rows had non-numeric unix_ms "
            f"(sample: {sample}); dropping",
            file=sys.stderr,
        )
        if n_bad == len(df):
            sys.exit(
                "structural error — every row's unix_ms is non-numeric. "
                "The CSV's column count or order may have drifted from "
                "the 38-column spec."
            )
        df = df.loc[~bad_mask].copy()

    # Derived helpers.
    df["ts"] = pd.to_datetime(df["unix_ms"], unit="ms")
    df["uncached_blocks"] = df["total_blocks"] - df["overlap_blocks"]
    df["kv_uncached"] = df["total_blocks"] - df["kv_blocks"]
    df["cache_hit"] = np.where(df["overlap_blocks"] > 0, "hit", "miss")
    df["regime_q"] = np.where(df["q_waiting"] == 0, "q=0", "q>0")
    df["regime_inflight"] = np.where(df["q_inflight"] == 0, "in=0", "in>0")
    df["cold_sat"] = (
        np.abs(df["q_rate_ms_per_entry"] - df["queue_ms_per_entry"]) > 0.5
    )
    df["inflight_active"] = df["q_inflight"] > 0
    df["overlap_ratio"] = np.where(
        df["total_blocks"] > 0,
        df["overlap_blocks"] / df["total_blocks"].replace(0, 1),
        0.0,
    )
    df["inflight_overlap_list"] = (
        df["inflight_overlap_blocks"].fillna("").map(_parse_int_list)
    )
    # Parse the replication_fired string into a real bool so report
    # filters can use it directly. Legacy/40-col files have all-false.
    df["replication_fired_bool"] = df["replication_fired"].str.lower() == "true"
    # Set once at load; consumers (replay, reports) branch on it for
    # the legacy linear-prefill fallback.
    df.attrs["is_legacy_csv"] = is_legacy_csv

    return df


def load(path: str) -> pd.DataFrame:
    """Backwards-compatible alias for `load_csv`."""
    return load_csv(path)


def _is_numeric_str(s: object) -> bool:
    """True iff `s` parses as a number."""
    if s is None:
        return False
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _parse_int_list(s: str) -> list[int]:
    """Parse '1;2;3' → [1, 2, 3]; '' → []. Malformed entries silently dropped."""
    if not s:
        return []
    out: list[int] = []
    for piece in s.split(";"):
        piece = piece.strip()
        if not piece:
            continue
        try:
            out.append(int(piece))
        except ValueError:
            pass
    return out


# ============================================================================
# Replay primitives
# ============================================================================

def _layered_overlap_row(
    inflight_list: list[int],
    kv_blocks: int,
    alpha: float,
) -> float:
    """The layered expected-overlap formula from the spec.

    Each layer is given by `max(0, oᵢ - Mᵢ)` where `Mᵢ` is the running
    max over all newer layers (so a layer only extends the longest
    matched prefix, never debits). Layers are weighted by α^i,
    newest first, with the radix-tree (kv_blocks) as the oldest layer
    at index len(inflight).
    """
    overlap = 0.0
    running_max = -1.0  # blocks are non-negative ints; -1 sentinels "no prior layer"
    for i, o in enumerate(inflight_list):  # newest → older
        layer = max(0.0, float(o) - running_max if running_max >= 0 else float(o))
        overlap += (alpha ** i) * layer
        if o > running_max:
            running_max = float(o)
    k = len(inflight_list)
    radix_layer = max(
        0.0,
        float(kv_blocks) - running_max if running_max >= 0 else float(kv_blocks),
    )
    overlap += (alpha ** k) * radix_layer
    return overlap


def _layered_overlap_series(df: pd.DataFrame, alpha: float) -> pd.Series:
    """Vectorized-where-possible apply of `_layered_overlap_row`. The
    layered max-tracking is per-row sequential, so we still loop in
    Python; lists are short (0-3 entries typically) so this is fine
    at the dataset sizes we see."""
    return pd.Series(
        [
            _layered_overlap_row(lst, int(kv), alpha)
            for lst, kv in zip(df["inflight_overlap_list"], df["kv_blocks"])
        ],
        index=df.index,
        dtype=float,
    )


def _prefill_ms_series(df: pd.DataFrame, uncached: pd.Series) -> pd.Series:
    """Compute prefill_ms via the quadratic formula:
        prefill_ms = a · uncached + c · uncached · total
    where `a = attention_uncached_block_ms` and
    `c = attention_uncached_per_every_block_ms`. Both coefficients are
    per-row (so a sweep that touches one row's config doesn't bleed
    into others).

    Legacy CSV rows (38/39-column variants) have a=c=0 from the
    loader pad; in that case fall back to the linear legacy formula
    `prefill_ms_per_block × uncached` so old data still produces
    meaningful predictions.
    """
    a = df["attention_uncached_block_ms"].astype(float)
    c = df["attention_uncached_per_every_block_ms"].astype(float)
    total = df["total_blocks"].astype(float)
    u = uncached.astype(float)
    legacy = (a == 0.0) & (c == 0.0)
    if legacy.any():
        return pd.Series(
            np.where(
                legacy,
                df["prefill_ms_per_block"].astype(float) * u,
                a * u + c * u * total,
            ),
            index=df.index,
            dtype=float,
        )
    return a * u + c * u * total


def _predicted_ms_series(df: pd.DataFrame) -> pd.Series:
    """The 6-component predictor sum:
        predicted_ms = base + q + prefill + cache + q_running + decode_contention
    `q_running_ms` is 0 on pre-q_running CSVs (loader pad).
    `decode_contention_ms` is 0 on CSVs older than the
    decode-contention term landing (also loader-padded). Either way
    the sum degrades to the older shorter form transparently.
    """
    return (
        df["base_ms"]
        + df["q_ms"]
        + df["prefill_ms"]
        + df["cache_ms"]
        + df["q_running_ms"]
        + df["decode_contention_ms"]
    )


def replay_alpha(df: pd.DataFrame, alpha: float) -> pd.DataFrame:
    """Replay with a hypothetical `expected_overlap_alpha`.

    Recomputes `overlap_blocks` via the layered formula, then
    `prefill_ms` (quadratic), `predicted_ms` (5 components), and
    `residual_ms`. All other components (base_ms, q_ms, cache_ms,
    q_running_ms) stay as the picker emitted them — α only affects
    the prefill term.

    Approximation note: the production picker applies a cache-credit
    refinement that scales each inflight's overlap contribution by
    its prefill-progress at score time. The CSV doesn't carry that
    progress signal per dispatch, so this replay assumes every
    inflight contributed full credit. The α-sweep optima land in the
    right direction but may differ from production by a few percent.

    Returns a fresh DataFrame (the input isn't mutated).
    """
    out = df.copy()
    out["overlap_blocks"] = _layered_overlap_series(out, alpha)
    out["uncached_blocks"] = out["total_blocks"] - out["overlap_blocks"]
    out["prefill_ms"] = _prefill_ms_series(out, out["uncached_blocks"])
    out["predicted_ms"] = _predicted_ms_series(out)
    out["residual_ms"] = out["observed_ms"] - out["predicted_ms"]
    out["expected_overlap_alpha"] = alpha
    return out


def replay_weight(df: pd.DataFrame, weight: float) -> pd.DataFrame:
    """Replay with a hypothetical `queue_workload_weight`.

    `q_ms' = (1 − w) · q_ms_constant + w · q_ms_workload`, then
    `predicted_ms` (5 components incl. q_running_ms) and
    `residual_ms`. α-side fields (overlap_blocks, prefill_ms) are
    unchanged.

    Returns a fresh DataFrame.
    """
    w = float(weight)
    out = df.copy()
    out["q_ms"] = (1.0 - w) * out["q_ms_constant"] + w * out["q_ms_workload"]
    out["predicted_ms"] = _predicted_ms_series(out)
    out["residual_ms"] = out["observed_ms"] - out["predicted_ms"]
    out["queue_workload_weight"] = w
    return out


def _q_running_penalty_series(
    df: pd.DataFrame,
    peak_ms: float,
    peak_qr: int,
    falloff_qr: int,
) -> pd.Series:
    """Compute synthetic q_running_ms per-row for a hypothetical
    U-shape `(peak_ms, peak_qr, falloff_qr)`. Mirrors the Rust
    `q_running_penalty_ms` in `src/picker.rs` byte-for-byte:

      - effective_qr = q_running + q_inflight  (shadow folds in)
      - 0 at effective_qr=0
      - linear ramp from 0 → peak_ms over [0, peak_qr]
      - linear decline from peak_ms → 0 over [peak_qr, falloff_qr]
      - 0 at effective_qr >= falloff_qr
      - Guard: peak_qr clamped to >= 1; falloff clamped to >= peak_qr+1
    """
    effective = (df["q_running"].astype(float) + df["q_inflight"].astype(float))
    if peak_ms <= 0:
        return pd.Series(np.zeros(len(df)), index=df.index, dtype=float)
    pk = max(int(peak_qr), 1)
    fo = max(int(falloff_qr), pk + 1)
    rising = peak_ms * (effective / pk)
    falling = peak_ms * (1.0 - (effective - pk) / (fo - pk))
    out = np.where(
        effective <= 0,
        0.0,
        np.where(
            effective <= pk,
            rising,
            np.where(effective < fo, falling, 0.0),
        ),
    )
    return pd.Series(np.maximum(out, 0.0), index=df.index, dtype=float)


def replay_q_running(
    df: pd.DataFrame,
    peak_ms: float,
    peak_qr: int,
    falloff_qr: int,
) -> pd.DataFrame:
    """Replay with a hypothetical q_running U-shape penalty.

    Recomputes `q_running_ms` per-row via the production tent
    function (see `_q_running_penalty_series`), then `predicted_ms`
    (5-component sum) and `residual_ms`. The other four components
    (base, q_ms, prefill_ms, cache_ms) stay as the picker emitted
    them — only the q_running term changes.

    Returns a fresh DataFrame.
    """
    out = df.copy()
    out["q_running_ms"] = _q_running_penalty_series(
        out, peak_ms, peak_qr, falloff_qr
    )
    out["q_running_penalty_peak_ms"] = float(peak_ms)
    out["q_running_penalty_peak_qr"] = int(peak_qr)
    out["q_running_penalty_falloff_qr"] = int(falloff_qr)
    out["predicted_ms"] = _predicted_ms_series(out)
    out["residual_ms"] = out["observed_ms"] - out["predicted_ms"]
    return out


def replay_coefficients(
    df: pd.DataFrame,
    *,
    base_ms: Optional[float] = None,
    prefill_ms_per_block: Optional[float] = None,
    attention_uncached_block_ms: Optional[float] = None,
    attention_uncached_per_every_block_ms: Optional[float] = None,
    queue_ms_per_entry: Optional[float] = None,
    cache_pressure_penalty_ms: Optional[float] = None,
) -> pd.DataFrame:
    """Replay with one or more coefficient overrides.

    `None` for a parameter means "keep the CSV's per-row value." Only
    the explicitly-passed values override. Returns a fresh DataFrame
    with `predicted_ms` (5 components) and `residual_ms` recomputed.

    Prefill cost: the modern model is quadratic
    (`a · uncached + c · uncached · total`); override via
    `attention_uncached_block_ms` and/or
    `attention_uncached_per_every_block_ms`. `prefill_ms_per_block`
    only has effect on legacy 38/39-column CSVs (where the new
    coefficients are zero); on current CSVs it's a no-op.

    Note: `queue_ms_per_entry` only affects the constant arm of the
    blend (`q_ms_constant`). The workload arm is `Σ uncached × prefill_rate
    + unknown × q_rate`, which uses `q_rate_ms_per_entry` (post
    cold-saturation), so we leave it untouched here. `q_running_ms`
    knobs are now in the CSV (cols 40-42) but `replay_coefficients`
    doesn't surface them — adjustments are diagnostic-only via
    `report_q_running`; keep `q_running_ms` as-emitted in replay.
    """
    out = df.copy()
    if base_ms is not None:
        out["base_ms"] = float(base_ms)
    prefill_touched = False
    if prefill_ms_per_block is not None:
        out["prefill_ms_per_block"] = float(prefill_ms_per_block)
        prefill_touched = True
    if attention_uncached_block_ms is not None:
        out["attention_uncached_block_ms"] = float(attention_uncached_block_ms)
        prefill_touched = True
    if attention_uncached_per_every_block_ms is not None:
        out["attention_uncached_per_every_block_ms"] = float(
            attention_uncached_per_every_block_ms
        )
        prefill_touched = True
    if prefill_touched:
        out["prefill_ms"] = _prefill_ms_series(out, out["uncached_blocks"])
    if queue_ms_per_entry is not None:
        # Only meaningful for the constant arm of the blend.
        out["queue_ms_per_entry"] = float(queue_ms_per_entry)
        new_const = float(queue_ms_per_entry) * out["q_waiting"]
        # Rebuild q_ms preserving the existing weight.
        w = out["queue_workload_weight"]
        out["q_ms_constant"] = new_const
        out["q_ms"] = (1.0 - w) * out["q_ms_constant"] + w * out["q_ms_workload"]
    if cache_pressure_penalty_ms is not None:
        out["cache_pressure_penalty_ms"] = float(cache_pressure_penalty_ms)
        out["cache_ms"] = (
            out["cache_pressure_penalty_ms"] * out["kv_cache_usage"]
        )
    out["predicted_ms"] = _predicted_ms_series(out)
    out["residual_ms"] = out["observed_ms"] - out["predicted_ms"]
    return out


# ============================================================================
# Report sections — I. Sanity & shape
# ============================================================================

def report_shape(df: pd.DataFrame) -> None:
    section("Shape")
    dt = (df["ts"].max() - df["ts"].min()).total_seconds()
    print(f"  rows         : {len(df)}")
    print(f"  duration     : {dt:.1f}s  ({df['ts'].min()} -> {df['ts'].max()})")
    print(f"  workers      : {sorted(df['name'].unique())}")
    err = (df["status"] != 200).sum()
    err_rate = err / len(df) if len(df) else 0
    print(f"  errors       : {err} ({100*err_rate:.1f}%)")

    if err_rate > 0.10:
        advise(
            "CRITICAL",
            f"error rate {100*err_rate:.1f}% — workers refusing connections "
            "under load. Reduce concurrent dispatch (top-K / in-flight "
            "tracking) and/or raise worker capacity (vLLM max_num_seqs, "
            "server connection limits) before tuning weights.",
        )
    elif err_rate > 0.02:
        advise(
            "HIGH",
            f"error rate {100*err_rate:.1f}% — workers occasionally refusing "
            "connections. Anti-dogpiling routing is the cheapest first move.",
        )
    elif len(df) < 50:
        advise(
            "LOW",
            f"only {len(df)} rows — findings below are indicative but with "
            "wide confidence intervals. Collect more data before acting "
            "on coefficient refits or replay-optima.",
        )
    else:
        advise("OK", "row count + error rate both look healthy.")


def report_errors(df: pd.DataFrame, n: int) -> None:
    errs = df[df["status"] != 200]
    if len(errs) == 0:
        return
    section("Errors")
    err_rate = len(errs) / len(df)
    print(f"  count: {len(errs)} ({100*err_rate:.1f}%)")
    print(f"  by worker:")
    print(errs["name"].value_counts().to_string())
    if "upstream_error" in errs.columns:
        print(f"\n  upstream_error frequencies:")
        print(errs["upstream_error"].value_counts().head(5).to_string())
    print(f"\n  top {n} busiest error seconds:")
    errs = errs.copy()
    errs["sec"] = errs["ts"].dt.floor("s")
    by_sec = errs.groupby("sec").size().reset_index(name="errors")
    print(by_sec.sort_values("errors", ascending=False).head(n).to_string(index=False))

    total_secs = df["ts"].dt.floor("s").nunique()
    err_secs = errs["sec"].nunique()
    clustering = 1 - (err_secs / total_secs) if total_secs else 0
    top_share = errs["name"].value_counts().iloc[0] / len(errs)

    if err_rate > 0.10:
        advise(
            "CRITICAL",
            f"{100*err_rate:.1f}% error rate. Workers are refusing "
            "connections under load. Reduce burst pressure (top-K) and/or "
            "raise worker capacity immediately.",
        )
    elif top_share > 0.5:
        advise(
            "HIGH",
            f"{int(100*top_share)}% of errors hit a single worker — "
            "that worker may be sized differently. Investigate per-worker "
            "capacity before changing routing.",
        )
    elif clustering > 0.5:
        advise(
            "HIGH",
            f"errors cluster in time ({err_secs}/{total_secs} seconds saw "
            "errors) — herding-driven overload during bursts.",
        )
    else:
        advise(
            "MEDIUM",
            "errors present but spread across workers and time — "
            "likely background flakiness.",
        )


def report_current_config(ok: pd.DataFrame) -> None:
    """Print the config that produced this CSV, pulled from the snapshot
    columns on the last row. Every row carries the config; the last row
    reflects "what's in effect right now" for the operator.

    The point of this section is orientation: "which set of knobs
    generated this data?" Analysts and future-me should not have to
    grep the raw CSV to answer that.
    """
    section("Current configuration (from last row)")
    last = ok.iloc[-1]
    # Predictor knobs
    print("  [predictor]")
    print(f"    base_ms                              = {last['base_ms']:.1f}")
    print(f"    queue_ms_per_entry                   = {last['queue_ms_per_entry']:.3f}")
    print(f"    attention_uncached_block_ms          = {last['attention_uncached_block_ms']:.3f}")
    print(f"    attention_uncached_per_every_block_ms= {last['attention_uncached_per_every_block_ms']:.5f}")
    print(f"    cache_pressure_penalty_ms            = {last['cache_pressure_penalty_ms']:.1f}")
    print(f"    saturated_cold_ms_per_entry          = {last['saturated_cold_ms_per_entry']:.1f}")
    print(f"    cache_pressure_saturated_threshold   = {last['cache_pressure_saturated_threshold']:.2f}")
    print(f"    q_running_penalty_peak_ms            = {last['q_running_penalty_peak_ms']:.1f}")
    print(f"    q_running_penalty_peak_qr            = {int(last['q_running_penalty_peak_qr'])}")
    print(f"    q_running_penalty_falloff_qr         = {int(last['q_running_penalty_falloff_qr'])}")
    print(f"    inflight_concurrent_capacity         = {int(last['inflight_concurrent_capacity'])}")
    print(f"    forward_pass_ms                      = {last['forward_pass_ms']:.2f}")
    print()
    print("  [picker]")
    print(f"    top_k_min                            = {int(last['top_k_min'])}")
    print(f"    top_k_fraction                       = {last['top_k_fraction']:.3f}")
    print(f"    inflight_tracking                    = {last['inflight_tracking']}")
    print(f"    tie_tolerance_ms                     = {last['tie_tolerance_ms']:.1f}")
    print(f"    replication_threshold_pct            = {last['replication_threshold_pct']:.1f}")
    print(f"    replication_warm_target              = {int(last['replication_warm_target'])}")
    print(f"    replication_warm_threshold_fraction  = {last['replication_warm_threshold_fraction']:.2f}")


def report_sanity(ok: pd.DataFrame) -> None:
    """Check that the recorded residual equals observed - predicted."""
    diff = (ok["residual_ms"] - (ok["observed_ms"] - ok["predicted_ms"])).abs()
    section("Sanity check")
    print(f"  max |residual - (observed - predicted)| = {diff.max():.2f}  (should be ~0)")

    if diff.max() > 1.0:
        advise(
            "BLOCKING",
            f"residual_ms doesn't equal observed - predicted (max diff "
            f"{diff.max():.1f}ms) — the CSV is internally inconsistent. "
            "Trace the emitter before trusting any other section.",
        )
    else:
        advise("OK", "residual accounting is self-consistent.")


def report_residuals(ok: pd.DataFrame) -> None:
    section("Residual summary (success only, ms)")
    s = ok["residual_ms"].describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95, 0.99])
    for k, v in s.items():
        print(f"  {k:8s} {v:>12.1f}")

    mean = ok["residual_ms"].mean()
    median = ok["residual_ms"].median()
    p95 = ok["residual_ms"].quantile(0.95)
    p5 = ok["residual_ms"].quantile(0.05)
    spread = p95 - p5

    if abs(mean) > 1000:
        advise(
            "HIGH",
            f"mean residual {mean:+.0f}ms is large — the model is "
            f"systematically {'under' if mean > 0 else 'over'}-predicting. "
            "Check the replay sections for the right re-tuning move.",
        )
    elif abs(mean) > 250:
        advise(
            "MEDIUM",
            f"mean residual {mean:+.0f}ms — modest systematic bias. "
            "Replay sections may suggest a small α/weight/coefficient nudge.",
        )
    elif spread > 5000:
        advise(
            "MEDIUM",
            f"mean near 0 ({mean:+.0f}ms) but p5–p95 spread is "
            f"{spread:.0f}ms — model is on-average correct but noisy. "
            "Tail-driven issues (herding, per-worker drift) outweigh "
            "coefficient tuning.",
        )
    else:
        advise(
            "OK",
            f"residuals centered (mean {mean:+.0f}, median {median:+.0f}) "
            "with reasonable spread.",
        )


# ============================================================================
# Report sections — II. Per-input attribution
# ============================================================================

def report_per_worker(ok: pd.DataFrame) -> None:
    section("Per-worker bias")
    g = ok.groupby("name").agg(
        n=("residual_ms", "size"),
        mean_residual=("residual_ms", "mean"),
        median_residual=("residual_ms", "median"),
        std_residual=("residual_ms", "std"),
        mean_observed=("observed_ms", "mean"),
    ).round(1).sort_values("mean_residual")
    print(g.to_string())

    if len(g) < 2:
        advise("LOW", "only one worker — no inter-worker bias to assess.")
        return

    spread = g["mean_residual"].max() - g["mean_residual"].min()
    ratio = (
        g["mean_observed"].max() / g["mean_observed"].min()
        if g["mean_observed"].min() > 0 else 0
    )

    if spread > 1500 or ratio > 3.0:
        advise(
            "HIGH",
            f"per-worker mean-residual spread is {spread:.0f}ms "
            f"(observed-latency ratio {ratio:.1f}×). The linear model has "
            "no per-worker term — this gap is unmodelable from the current "
            "features. Highest-leverage fix: per-worker EMA offset added "
            "to predicted_wait_ms, learned online from recent residuals.",
        )
    elif spread > 500:
        advise(
            "MEDIUM",
            f"per-worker spread {spread:.0f}ms — some heterogeneity. "
            "Per-worker EMA offsets would help; consider once other fixes "
            "don't close the residual gap.",
        )
    else:
        advise(
            "OK",
            f"per-worker spread {spread:.0f}ms — workers are reasonably "
            "fungible from the predictor's view.",
        )


def report_per_component(ok: pd.DataFrame) -> None:
    """Attribute the residual to each prediction component.

    The model predicts observed = base + q + prefill + cache + q_running.
    Reports the share each component contributes to the *signed*
    residual via residual↔component correlation: a positive correlation
    means the residual grows with the component, which usually means
    the component's coefficient is too small (model under-charges it).
    The largest |correlation| flags the most-miscalibrated component.

    `q_running_ms` is included but flagged differently — its three
    config knobs (peak_ms, peak_qr, falloff_qr) are captured per-row
    in cols 40-42, but the script has no replay sweep for them. A
    strong correlation here points to `report_q_running` for the
    bucketed shape diagnostic; coefficient changes are still manual.
    """
    section("Per-component attribution")

    print(f"  mean predicted breakdown (ms):")
    print(f"    base_ms              = {ok['base_ms'].mean():>7.1f}")
    print(f"    q_ms                 = {ok['q_ms'].mean():>7.1f}")
    print(f"      q_ms_constant      = {ok['q_ms_constant'].mean():>7.1f}  (q_waiting × q_rate_ms_per_entry)")
    print(f"      q_ms_workload      = {ok['q_ms_workload'].mean():>7.1f}  (in-flight position cost + unknown-q baseline)")
    print(f"    prefill_ms           = {ok['prefill_ms'].mean():>7.1f}")
    print(f"    cache_ms             = {ok['cache_ms'].mean():>7.1f}")
    print(f"    q_running_ms         = {ok['q_running_ms'].mean():>7.1f}")
    print(f"    decode_contention_ms = {ok['decode_contention_ms'].mean():>7.1f}")
    print(f"    predicted_ms         = {ok['predicted_ms'].mean():>7.1f}")
    print(f"    observed_ms          = {ok['observed_ms'].mean():>7.1f}")
    print(f"    mean residual        = {ok['residual_ms'].mean():>7.1f}")
    print()

    # Residual ↔ component correlations: how much does each component
    # contribute to the bias? q_ms is reported split into its constant
    # (q_waiting × rate) and workload (sum of per-inflight position
    # costs) sub-components — they map to different tuning knobs and
    # routinely move in opposite directions, so the aggregate q_ms
    # correlation mis-routes advice.
    print(f"  residual correlation with each component:")
    corrs = {}
    components = (
        "base_ms", "q_ms_constant", "q_ms_workload",
        "prefill_ms", "cache_ms", "q_running_ms", "decode_contention_ms",
    )
    for comp in components:
        if ok[comp].std() == 0:
            print(f"    {comp:20s}  n/a (constant in this dataset)")
            corrs[comp] = float("nan")
        else:
            r = ok[comp].corr(ok["residual_ms"])
            print(f"    {comp:20s}  corr = {r:+.3f}")
            corrs[comp] = r

    # Strongest correlation = the component most miscalibrated.
    real = {k: v for k, v in corrs.items() if not pd.isna(v)}
    if not real:
        advise("LOW", "no component varied enough to attribute residuals.")
        return
    worst_comp = max(real, key=lambda k: abs(real[k]))
    worst_r = real[worst_comp]

    coef_for_component = {
        "base_ms": "base_ms",
        "q_ms_constant": (
            "queue_ms_per_entry (or saturated_cold_ms_per_entry when "
            "the cold-saturation gate fires)"
        ),
        "q_ms_workload": (
            "the in-flight prefill estimate "
            "(attention_uncached_block_ms / "
            "attention_uncached_per_every_block_ms) AND/OR the "
            "`inflight_discount` shape — see `Regime split: by "
            "q_inflight` for the per-inflight over-charge in ms"
        ),
        "prefill_ms": (
            "attention_uncached_block_ms / "
            "attention_uncached_per_every_block_ms "
            "(and/or expected_overlap_alpha)"
        ),
        "cache_ms": "cache_pressure_penalty_ms",
        "q_running_ms": (
            "q_running_penalty_{peak_ms, peak_qr, falloff_qr} — "
            "captured cols 41-43; see report_q_running for the shape "
            "diagnostic (no automated refit yet)"
        ),
        "decode_contention_ms": (
            "forward_pass_ms — see the dedicated forward_pass_ms sweep "
            "for an optimum, not just the correlation sign"
        ),
    }
    coef = coef_for_component[worst_comp]
    direction = "increase" if worst_r > 0 else "decrease"

    if worst_comp == "q_running_ms" and abs(worst_r) > 0.3:
        advise(
            "HIGH",
            f"q_running_ms carries the strongest miscalibration "
            f"(corr={worst_r:+.2f}). See the `q_running` section "
            "below — it shows the bucketed residual-by-qr shape plus "
            "the captured peak_ms / peak_qr / falloff_qr config from "
            "cols 40-42, so you can adjust the U-shape parameters "
            "based on where the residual peak actually lands.",
        )
        return
    if abs(worst_r) > 0.4:
        advise(
            "OBSERVATION",
            f"{worst_comp} has the strongest residual correlation "
            f"(corr={worst_r:+.2f}). Direction suggests `{coef}` may "
            f"want to {direction}, but a single-run correlation is "
            "noisy — confirm across multiple benches before acting. "
            "This often misleads when one feature dominates the "
            "current routing distribution.",
        )
    elif abs(worst_r) > 0.2:
        advise(
            "MILD",
            f"{worst_comp} has a moderate residual correlation "
            f"(corr={worst_r:+.2f}). Probably within run-to-run noise.",
        )
    else:
        advise(
            "OK",
            "no single component is dominantly miscalibrated — residuals "
            "are spread across components or driven by features the model "
            "doesn't see.",
        )


def report_correlations(ok: pd.DataFrame) -> None:
    section("Residual correlation with input features")
    cols = [
        "q_waiting", "q_inflight", "q_running", "q_swapped",
        "uncached_blocks", "overlap_blocks", "kv_blocks",
        "kv_cache_usage", "compl_per_s", "q_workload",
    ]
    corrs: dict[str, float] = {}
    for c in cols:
        try:
            r = ok[c].corr(ok["residual_ms"])
        except Exception:
            r = float("nan")
        if pd.isna(r):
            print(f"  {c:28s}  corr =   n/a  (constant feature?)")
        else:
            print(f"  {c:28s}  corr = {r:+.3f}")
            corrs[c] = r

    # Feature → coefficient mapping for the advice.
    coef_for_feature = {
        "q_waiting": "queue_ms_per_entry",
        "uncached_blocks": (
            "attention_uncached_block_ms / "
            "attention_uncached_per_every_block_ms (or α — see α-sweep)"
        ),
        "overlap_blocks": "expected_overlap_alpha (negative corr = α too high)",
        "kv_cache_usage": "cache_pressure_penalty_ms",
        "compl_per_s": None,  # gates cold-sat, no direct coefficient
        "q_workload": "queue_workload_weight (high corr = increase w)",
        "q_running": (
            "q_running_penalty_{peak_ms, peak_qr, falloff_qr} — "
            "captured cols 40-42; see report_q_running for the "
            "bucketed shape diagnostic"
        ),
    }
    tunable = {c: r for c, r in corrs.items() if c in coef_for_feature}
    strong = [(c, r) for c, r in tunable.items() if abs(r) > 0.3]
    moderate = [(c, r) for c, r in tunable.items() if 0.15 < abs(r) <= 0.3]

    def describe(feature: str, r: float) -> str:
        coef = coef_for_feature[feature]
        if coef is None:
            direction = (
                "high-completion-rate workers being over-predicted"
                if r < 0
                else "low-completion-rate workers being under-predicted"
            )
            return f"{feature} (r={r:+.2f}) → {direction}"
        verb = "increase" if r > 0 else "decrease"
        return f"{feature} (r={r:+.2f}) → {verb} `{coef}`"

    if strong:
        details = "; ".join(describe(c, r) for c, r in strong)
        advise(
            "OBSERVATION",
            f"strong residual correlations on this dataset: {details}. "
            "Each is a directional hint, not a recipe — adopting the "
            "implied coefficient changes naively often makes things "
            "worse (knobs interact). Worth re-running 2-3× to confirm "
            "the correlation is stable before acting.",
        )
    elif moderate:
        details = "; ".join(describe(c, r) for c, r in moderate)
        advise(
            "MILD",
            f"moderate residual correlations: {details}. Likely "
            "within run-to-run noise.",
        )
    else:
        advise(
            "OK",
            "no strong correlations on tunable features — model is using "
            "each feature about right; remaining error is noise or "
            "non-linear structure (per-worker bias, herding).",
        )


# ============================================================================
# Report sections — III. Regime detection
# ============================================================================

def report_regimes(ok: pd.DataFrame) -> None:
    section("Regime split: by q_waiting")
    by_q = {}
    for regime, g in ok.groupby("regime_q"):
        by_q[regime] = g["residual_ms"]
        print(
            f"  {regime:4s}  n={len(g):4d}  "
            f"mean={g['residual_ms'].mean():+8.1f}  "
            f"median={g['residual_ms'].median():+8.1f}  "
            f"std={g['residual_ms'].std():.1f}"
        )
    if "q=0" in by_q and "q>0" in by_q and len(by_q["q>0"]) >= 5:
        gap = by_q["q>0"].median() - by_q["q=0"].median()
        if abs(gap) > 1000:
            advise(
                "HIGH",
                f"queued requests have median residual {gap:+.0f}ms vs "
                "idle ones — `queue_ms_per_entry` is "
                f"{'undertuned' if gap > 0 else 'overtuned'} for the "
                "congested regime. Check the weight-sweep section: a "
                "higher weight may also help if `q_workload` is more "
                "predictive than `q_waiting` count alone.",
            )
        elif abs(gap) > 300:
            advise(
                "MEDIUM",
                f"queued-vs-idle residual gap is {gap:+.0f}ms — small "
                "but consistent bias in the congested regime.",
            )
        else:
            advise(
                "OK",
                f"queued and idle regimes agree within {abs(gap):.0f}ms.",
            )
    else:
        advise(
            "LOW",
            "too few queued (q>0) rows to assess regime split. Re-run "
            "with higher concurrency to exercise the queue term.",
        )

    section("Regime split: by q_inflight")
    by_i = {}
    for regime, g in ok.groupby("regime_inflight"):
        by_i[regime] = g["residual_ms"]
        print(
            f"  {regime:5s}  n={len(g):4d}  "
            f"mean={g['residual_ms'].mean():+8.1f}  "
            f"median={g['residual_ms'].median():+8.1f}"
        )
    if "in=0" in by_i and "in>0" in by_i:
        share_inflight = len(by_i["in>0"]) / (len(by_i["in=0"]) + len(by_i["in>0"]))
        if share_inflight < 0.05:
            advise(
                "LOW",
                f"only {100*share_inflight:.1f}% of rows had q_inflight>0. "
                "α-sweep is uninformative on the in=0 rows (layered "
                "overlap collapses to `kv_blocks` regardless of α); the "
                "sweep will operate on a thin slice. Run a higher-"
                "concurrency benchmark to exercise α.",
            )
        else:
            advise(
                "OK",
                f"{100*share_inflight:.1f}% of rows had q_inflight>0 — "
                "α-sweep has enough rows to be meaningful.",
            )
        # Per-inflight over/under-charge: the residual GAP between
        # cohorts, normalized by the mean q_inflight count in the in>0
        # cohort. Convention: positive = model over-charges per inflight,
        # negative = under-charges. The in-flight cost term in q_ms is
        # ~linear in q_inflight (it sums one position-cost per inflight),
        # so this attributes the gap directly to that term.
        in_gt0 = ok[ok["regime_inflight"] == "in>0"]
        mean_qi = float(in_gt0["q_inflight"].mean()) if len(in_gt0) else 0.0
        if mean_qi >= 0.5 and len(in_gt0) >= 5:
            resid_gap_ms = by_i["in=0"].mean() - by_i["in>0"].mean()
            per_inflight_ms = resid_gap_ms / mean_qi
            direction = "over-charges" if per_inflight_ms > 0 else "under-charges"
            print(
                f"  per-inflight charge gap: {per_inflight_ms:+.0f}ms "
                f"(mean q_inflight in cohort = {mean_qi:.1f})"
            )
            if abs(per_inflight_ms) > 200:
                advise(
                    "HIGH",
                    f"model {direction} each inflight by ~{abs(per_inflight_ms):.0f}ms. "
                    "Tune `attention_uncached_block_ms` / "
                    "`attention_uncached_per_every_block_ms` (the per-"
                    "inflight prefill estimate) OR revisit the discount "
                    "logic in `inflight_discount` — the simplified "
                    "`(1 - progress)` ceiling=1.0 may be too aggressive "
                    "for fresh inflights that vLLM pipelines.",
                )
            elif abs(per_inflight_ms) > 50:
                advise(
                    "MEDIUM",
                    f"model {direction} each inflight by ~{abs(per_inflight_ms):.0f}ms — "
                    "modest miscalibration of the in-flight prefill "
                    "estimate or discount.",
                )
            else:
                advise(
                    "OK",
                    f"per-inflight bias is within ±{abs(per_inflight_ms):.0f}ms — "
                    "in-flight cost is well-calibrated.",
                )

    section("Regime split: by prefix-cache hit/miss")
    by_h = {}
    for regime, g in ok.groupby("cache_hit"):
        by_h[regime] = g["residual_ms"]
        print(
            f"  {regime:4s}  n={len(g):4d}  "
            f"mean={g['residual_ms'].mean():+8.1f}  "
            f"median={g['residual_ms'].median():+8.1f}  "
            f"std={g['residual_ms'].std():.1f}"
        )
    if "hit" in by_h and "miss" in by_h and len(by_h["miss"]) >= 5:
        gap = by_h["miss"].median() - by_h["hit"].median()
        if abs(gap) > 1500:
            advise(
                "HIGH",
                f"cache-miss median residual is {gap:+.0f}ms vs hits — "
                f"`prefill_ms_per_block` is "
                f"{'undertuned' if gap > 0 else 'overtuned'} for the "
                "full-prefill case. The α-sweep section will distinguish "
                "this from an overlap-credit miscalibration.",
            )
        elif abs(gap) > 500:
            advise(
                "MEDIUM",
                f"hit-vs-miss gap is {gap:+.0f}ms — modest "
                "miscalibration of the prefill term.",
            )
        else:
            advise(
                "OK",
                f"hit and miss regimes agree within {abs(gap):.0f}ms.",
            )
    elif "miss" not in by_h:
        advise(
            "LOW",
            "no cache-miss rows in this dataset — `prefill_ms_per_block` "
            "can't be tuned independently of overlap credit here. Run a "
            "benchmark with fresh prompts (no prefix in the radix tree) "
            "to exercise the miss regime.",
        )
    elif "hit" not in by_h:
        advise(
            "LOW",
            "no cache-hit rows in this dataset — no overlap data to "
            "compare against.",
        )

    section("Regime split: by overlap ratio (high vs low cache benefit)")
    high = ok[ok["overlap_ratio"] >= 0.5]
    low = ok[ok["overlap_ratio"] <= 0.1]
    print(
        f"  high overlap (≥0.5)  n={len(high):4d}  "
        f"mean residual = {high['residual_ms'].mean() if len(high) else float('nan'):+.1f}  "
        f"(q_ms dominates predicted_ms)"
    )
    print(
        f"  low overlap  (≤0.1)  n={len(low):4d}  "
        f"mean residual = {low['residual_ms'].mean() if len(low) else float('nan'):+.1f}  "
        f"(prefill_ms dominates predicted_ms)"
    )
    if len(high) >= 5 and len(low) >= 5:
        gap = abs(high["residual_ms"].mean() - low["residual_ms"].mean())
        if gap > 500:
            advise(
                "MEDIUM",
                f"residual differs by {gap:.0f}ms between low-overlap and "
                "high-overlap regimes — the coefficient that dominates "
                "each regime (prefill vs queue) is miscalibrated "
                "asymmetrically. Use the replay sweeps to identify which.",
            )
        else:
            advise(
                "OK",
                f"residual agrees within {gap:.0f}ms across overlap "
                "regimes — coefficients are similarly tuned in both.",
            )
    else:
        advise(
            "LOW",
            "not enough rows in one of the high/low overlap buckets to "
            "draw conclusions.",
        )


def report_cold_saturation(ok: pd.DataFrame) -> None:
    """Rows where the cold-saturation gate fired use a different
    `q_rate_ms_per_entry` than the base `queue_ms_per_entry`. Tuning
    `saturated_cold_ms_per_entry` requires looking at those rows
    separately."""
    section("Cold-saturation gate")
    fired = ok[ok["cold_sat"]]
    n_fired = len(fired)
    print(f"  rows where gate fired: {n_fired} ({100*n_fired/len(ok):.1f}%)")
    if n_fired:
        print(f"  mean residual (fired):    {fired['residual_ms'].mean():+.1f}")
        print(f"  median residual (fired):  {fired['residual_ms'].median():+.1f}")
        nf = ok[~ok["cold_sat"]]
        print(f"  mean residual (not fired): {nf['residual_ms'].mean():+.1f}")
        print(f"  median residual (not fired):{nf['residual_ms'].median():+.1f}")

    if n_fired == 0:
        advise(
            "LOW",
            "the cold-saturation gate never fired in this dataset. "
            "Either the workload didn't approach saturation or the "
            "threshold is set so high it's unreachable. Either way, "
            "`saturated_cold_ms_per_entry` and "
            "`cache_pressure_saturated_threshold` can't be tuned from "
            "this data.",
        )
        return
    elif n_fired / len(ok) < 0.02:
        advise(
            "LOW",
            f"gate fired on {100*n_fired/len(ok):.1f}% of rows — "
            "rare enough that the cold-sat coefficients are largely "
            "irrelevant. Don't tune from these alone.",
        )
        return

    bias = fired["residual_ms"].median()
    nf_bias = ok[~ok["cold_sat"]]["residual_ms"].median()
    delta = bias - nf_bias
    if abs(delta) > 1000:
        verb = (
            "lower `saturated_cold_ms_per_entry`"
            if delta > 0
            else "raise `saturated_cold_ms_per_entry`"
        )
        advise(
            "HIGH",
            f"gate-fired residual median is {delta:+.0f}ms vs non-fired — "
            f"the cold-sat regime is being "
            f"{'under-predicted' if delta > 0 else 'over-predicted'}. "
            f"{verb}, or adjust "
            "`cache_pressure_saturated_threshold` to fire less aggressively.",
        )
    elif abs(delta) > 300:
        advise(
            "MEDIUM",
            f"gate-fired median is {delta:+.0f}ms vs non-fired — "
            "moderate cold-sat coefficient miscalibration.",
        )
    else:
        advise(
            "OK",
            f"gate-fired residuals agree with non-fired within "
            f"{abs(delta):.0f}ms.",
        )


def report_q_running(ok: pd.DataFrame) -> None:
    """Diagnostic for the q_running U-shape penalty.

    The picker adds a U-shape latency penalty driven by
    `queue_running` (vLLM's num_requests_running) plus our inflight
    shadow. It models the "scheduler-tick danger zone" where a worker
    has just enough decode load to contend for GPU but not enough to
    absorb new prefills into a running batch — empirically peaks at
    qr=2 and falls off by qr~8.

    The penalty's three knobs (peak_ms, peak_qr, falloff_qr) ARE in
    the CSV at cols 40-42, but this section is diagnostic only — no
    automated sweep is implemented. What it surfaces:
      * the captured config in effect (peak_ms / peak_qr / falloff_qr)
      * bucketed residuals by q_running, so you can see if the
        residual peak lines up with the configured `peak_qr`
      * residual ↔ penalty correlation:
        - strong positive corr → penalty too small at the firing band
          (or the U is centered wrong)
        - strong negative corr → penalty overcorrecting; reduce
          `peak_ms`
      * a flag for "dataset didn't exercise q_running at all"
    Adjustments are still manual: read the bucketed shape, then
    bump `peak_qr` toward the observed residual peak and / or scale
    `peak_ms` by the correlation magnitude.
    """
    section("q_running U-shape penalty")

    # Surface the captured config (cols 40-42) so operators see what
    # peak/falloff was in effect when the residuals were collected.
    peak_ms = float(ok["q_running_penalty_peak_ms"].mode().iloc[0])
    peak_qr = int(ok["q_running_penalty_peak_qr"].mode().iloc[0])
    falloff = int(ok["q_running_penalty_falloff_qr"].mode().iloc[0])
    print(f"  config: peak_ms={peak_ms:.1f}  peak_qr={peak_qr}  falloff_qr={falloff}")

    n_with_penalty = (ok["q_running_ms"] > 0).sum()
    mean_penalty = ok["q_running_ms"].mean()
    max_penalty = ok["q_running_ms"].max()
    print(f"  rows with q_running_ms > 0: {n_with_penalty} ({100*n_with_penalty/len(ok):.1f}%)")
    print(f"  mean q_running_ms (over all rows): {mean_penalty:.1f}")
    print(f"  max  q_running_ms                : {max_penalty:.1f}")

    if n_with_penalty == 0:
        # Two cases: (a) legacy CSV with q_running_ms padded to 0, or
        # (b) penalty disabled / U-shape's peak_ms set to 0.
        if ok.attrs.get("is_legacy_csv", False):
            advise(
                "LOW",
                "legacy CSV — q_running_ms wasn't captured. Re-run with "
                "the current emitter to get this section.",
            )
        else:
            advise(
                "LOW",
                "q_running_ms is 0 on every row — either the penalty is "
                "disabled (q_running_penalty_peak_ms = 0) or this "
                "workload never produced q_running>0. Either way no "
                "tuning signal here.",
            )
        return

    # Per-q_running bucket: median residual at each q_running count.
    # If the U-shape is correctly placed, residuals should be ~0 at
    # the peak (where the penalty is largest) and at the falloff
    # (where it's 0). Deviations show the U is mis-aligned.
    print()
    print(f"  median residual by q_running bucket (n ≥ 3 only):")
    print(f"  {'q_running':>10s}  {'n':>5s}  {'mean penalty':>13s}  "
          f"{'median resid':>13s}  {'mean resid':>11s}")
    by_qr = ok.groupby("q_running")
    bucket_rows = []
    for qr, g in by_qr:
        if len(g) < 3:
            continue
        bucket_rows.append((
            int(qr),
            len(g),
            g["q_running_ms"].mean(),
            g["residual_ms"].median(),
            g["residual_ms"].mean(),
        ))
    if not bucket_rows:
        advise(
            "LOW",
            "no q_running bucket has ≥3 rows — too sparse to assess the "
            "U-shape. Re-run with higher concurrency to populate the qr>0 "
            "buckets.",
        )
        return
    bucket_rows.sort()
    for qr, n, pen, med, mean in bucket_rows:
        print(f"  {qr:>10d}  {n:>5d}  {pen:>13.1f}  {med:>+13.1f}  {mean:>+11.1f}")

    # Correlation between applied penalty and residual.
    # Strong positive ⇒ we charged a big penalty AND it wasn't warranted
    #                   (under-predicted; observed was larger). Wait —
    #                   bigger penalty makes predicted_ms bigger, which
    #                   makes residual = observed - predicted SMALLER.
    #                   So positive corr means "rows we penalized more
    #                   ALSO saw more residual" — that's a sign the
    #                   penalty isn't catching the latency, the latency
    #                   exists for some other reason. The penalty might
    #                   be misshaped (wrong qr peak) or correlated with
    #                   an unmodeled feature.
    # Strong negative ⇒ rows we penalized more saw smaller residual —
    #                   penalty is doing real work, possibly TOO much.
    if ok["q_running_ms"].std() > 0:
        r = ok["q_running_ms"].corr(ok["residual_ms"])
        print(f"\n  corr(q_running_ms, residual_ms) = {r:+.3f}")
        if r > 0.3:
            advise(
                "MEDIUM",
                f"q_running_ms correlates POSITIVELY with residuals "
                f"(r={r:+.2f}) — rows where the penalty fired hardest also "
                "saw the largest under-prediction. The U-shape may be in "
                "the wrong qr range OR there's an unmodeled feature "
                "correlated with q_running. Inspect the residual-by-qr "
                "bucket above: if the peak is shifted from where the "
                "penalty fires hardest, raise/lower "
                "`q_running_penalty_peak` to match.",
            )
        elif r < -0.3:
            advise(
                "MEDIUM",
                f"q_running_ms correlates NEGATIVELY with residuals "
                f"(r={r:+.2f}) — the penalty is doing real work but may be "
                "overcorrecting. If the residual-by-qr table shows big "
                "negative residuals (over-prediction) at the qr peak, "
                "reduce `q_running_penalty_peak_ms`.",
            )
        else:
            advise(
                "OK",
                f"q_running_ms is well-uncorrelated with residuals "
                f"(r={r:+.2f}) — the U-shape's location and magnitude are "
                "reasonable for this dataset.",
            )
    else:
        advise(
            "LOW",
            "q_running_ms was constant across rows — can't compute the "
            "residual correlation. Either every row had the same qr "
            "or the penalty is disabled.",
        )


def report_replication(ok: pd.DataFrame) -> None:
    """Diagnostic for the cache-replication override.

    When the picker would normally route to a committed-warm worker
    but the fleet's warm count is below `replication_warm_target`, it
    swaps the pick for a cold-idle alternative whose predicted_ms is
    within `replication_threshold_pct` of the best. That seeds the
    cache on a second worker — useful for redundancy, costly if the
    threshold is too generous.

    This section reports: how often the swap fired, and whether
    replication-driven picks have systematically different residuals
    than baseline picks (i.e. did the cold-idle worker actually
    deliver TTFT close to what we predicted, or did we overpay for
    redundancy?). Config knobs (`replication_threshold_pct`,
    `replication_warm_target`, `replication_warm_threshold_fraction`)
    are snapshotted per-row from cols 46-48, so the report can show
    operators what setting was in effect.
    """
    section("Cache-replication override")

    if "replication_fired_bool" not in ok.columns:
        advise(
            "LOW",
            "replication_fired column not in CSV (legacy schema). "
            "Re-emit with current kinference to surface this section.",
        )
        return

    fired_mask = ok["replication_fired_bool"]
    n_fired = int(fired_mask.sum())
    n_total = len(ok)
    frac = n_fired / max(n_total, 1)

    # Print effective config (mode value; assumes uniform within a
    # run — operators rolling out a tuning change would see this
    # shift here too).
    threshold_pct = float(ok["replication_threshold_pct"].mode().iloc[0])
    warm_target = int(ok["replication_warm_target"].mode().iloc[0])
    warm_frac = float(ok["replication_warm_threshold_fraction"].mode().iloc[0])
    print(f"  config: replication_threshold_pct={threshold_pct:.1f}  "
          f"warm_target={warm_target}  warm_threshold_fraction={warm_frac:.2f}")
    print(f"  rows where replication fired: {n_fired} / {n_total} ({100*frac:.1f}%)")

    if threshold_pct <= 0.0 or warm_target <= 1:
        advise(
            "OK",
            f"replication is disabled (threshold_pct={threshold_pct:.1f}, "
            f"warm_target={warm_target}). No swaps possible.",
        )
        return
    if n_fired == 0:
        # Feature is enabled but never triggered: either the fleet is
        # already replicated past the target on every pick, or the
        # threshold is so tight nothing qualifies.
        advise(
            "LOW",
            "replication is enabled but never fired. Either the fleet "
            "is already replicated past `replication_warm_target` on "
            "every pick, or `replication_threshold_pct` is too tight "
            "for any cold worker to qualify. Try raising the threshold "
            "(more tolerant of the latency hit) if you want to exercise "
            "the feature.",
        )
        return

    # Compare residuals between fired vs not-fired rows.
    fired = ok[fired_mask]
    rest = ok[~fired_mask]
    fired_med = fired["residual_ms"].median()
    rest_med = rest["residual_ms"].median() if len(rest) else 0.0
    fired_mean = fired["residual_ms"].mean()
    rest_mean = rest["residual_ms"].mean() if len(rest) else 0.0
    print(f"  median residual (fired):    {fired_med:>+8.1f}")
    print(f"  median residual (not fired):{rest_med:>+8.1f}")
    print(f"  mean residual   (fired):    {fired_mean:>+8.1f}")
    print(f"  mean residual   (not fired):{rest_mean:>+8.1f}")

    delta = fired_med - rest_med
    if n_fired < 5:
        advise(
            "LOW",
            f"only {n_fired} replication swaps in this dataset — too few "
            "to draw conclusions. Either rare workload or threshold is "
            "tight; run a longer bench to confirm.",
        )
    elif delta > 1000:
        advise(
            "HIGH",
            f"replication-fired rows skew {delta:+.0f}ms vs baseline — "
            "swaps land on cold workers that take significantly longer "
            "than predicted. Tighten `replication_threshold_pct` to "
            "accept fewer swaps, or raise `replication_warm_threshold_fraction` "
            "so 'warm enough' is more demanding.",
        )
    elif delta > 300:
        advise(
            "MEDIUM",
            f"replication-fired rows skew {delta:+.0f}ms — modest hit. "
            "If redundancy is more valuable than the small latency "
            "cost, leave it; otherwise tighten the threshold.",
        )
    else:
        advise(
            "OK",
            f"replication swaps land within {abs(delta):.0f}ms of baseline — "
            "the cold workers are absorbing the load at the expected "
            "cost. Feature is paying for itself.",
        )


# ============================================================================
# Report sections — IV. Replay
# ============================================================================

def section_iv_preamble() -> None:
    """Banner explaining how to read the tuning sections that follow.

    The Fit(1)/Fit(2) refits and the α/w/q_running sweeps are
    diagnostic surfaces, not config recipes. They look at one knob
    at a time, holding the others as the picker had them at run
    time. Adopting their recommendations naively — picking the
    'optimum' from one sweep, then re-running the bench and adopting
    the next 'optimum' — does NOT converge to a good config. Each
    bench shifts the data distribution; the next refit chases a
    moving target.

    Use these sections to spot ANOMALIES (coefficients an order of
    magnitude off, wrong-sign correlations, residuals concentrated
    on one worker), not to drive incremental tuning.
    """
    print()
    print("=" * 76)
    print("HOW TO READ THE SECTIONS BELOW")
    print("=" * 76)
    print(
        "  These are DIAGNOSTIC SURFACES, not a tuning recipe. Each sweep / refit"
    )
    print(
        "  looks at one knob in isolation. Adopting recommendations one at a time"
    )
    print(
        "  and re-benching is a known bad workflow — it doesn't converge because"
    )
    print(
        "  every config change shifts the data the next refit sees."
    )
    print()
    print(
        "  Use these to spot ANOMALIES (coefficient an order of magnitude off,"
    )
    print(
        "  wrong-sign correlation, residual centered on one worker). Don't use"
    )
    print(
        "  them as an autopilot. If you change a knob, change ONE, run the bench"
    )
    print(
        "  multiple times to estimate noise, and only iterate when the effect is"
    )
    print(
        "  larger than the inter-run variance."
    )
    print()


def report_alpha_sweep(ok: pd.DataFrame, grid: int) -> None:
    """Sweep `expected_overlap_alpha` and report mean |residual|.

    Only rows with q_inflight > 0 are α-sensitive — others collapse
    to `kv_blocks` regardless of α. We restrict the sweep to those
    rows so the surface isn't flattened by α-irrelevant rows.
    """
    section("α sweep (expected_overlap_alpha)")
    sub = ok[ok["inflight_active"]]
    if len(sub) < 10:
        advise(
            "LOW",
            f"only {len(sub)} rows with q_inflight>0 — α-sweep is "
            "uninformative. Run a higher-concurrency benchmark to "
            "exercise the layered-overlap formula.",
        )
        return

    current_alpha = float(ok["expected_overlap_alpha"].mode().iloc[0])
    alphas = np.linspace(0.0, 1.0, grid)
    rows = []
    for a in alphas:
        replayed = replay_alpha(sub, float(a))
        mae = replayed["residual_ms"].abs().mean()
        p95 = replayed["residual_ms"].abs().quantile(0.95)
        rows.append((float(a), mae, p95))

    print(f"  rows in sweep: {len(sub)} (q_inflight > 0)")
    print(f"  current α    : {current_alpha:.3f}")
    print()
    print(f"  {'α':>6s}  {'mean |resid|':>14s}  {'p95 |resid|':>13s}")
    for a, mae, p95 in rows:
        marker = " ←" if abs(a - current_alpha) < (1.0 / (grid - 1)) / 2 else ""
        print(f"  {a:>6.3f}  {mae:>14.1f}  {p95:>13.1f}{marker}")

    # Find optimum.
    best_idx = int(np.argmin([r[1] for r in rows]))
    best_alpha, best_mae, _ = rows[best_idx]
    current_mae = next(
        (mae for a, mae, _ in rows if abs(a - current_alpha) < 1e-6),
        rows[
            int(round(current_alpha * (grid - 1)))
        ][1],
    )
    reduction = (current_mae - best_mae) / max(current_mae, 1e-9)

    print()
    print(f"  optimum α    : {best_alpha:.3f}  (mean |resid| = {best_mae:.1f})")
    print(f"  current α    : {current_alpha:.3f}  (mean |resid| = {current_mae:.1f})")
    print(f"  improvement  : {100*reduction:.1f}% reduction in mean |resid|")

    if reduction > 0.10:
        advise(
            "OBSERVATION",
            f"on this dataset, α={best_alpha:.2f} would reduce mean "
            f"|resid| by {100*reduction:.0f}% on inflight-active rows. "
            "This is a single-run estimate — re-run the bench 2-3× to "
            "confirm the gap is real before changing α. Note: this "
            "sweep only affects the prefill term; queue / decode-load "
            "knobs are untouched.",
        )
    elif reduction > 0.03:
        advise(
            "MEDIUM",
            f"α={best_alpha:.2f} would reduce mean |resid| by "
            f"{100*reduction:.0f}%. Modest win; worth trying if "
            "operationally cheap.",
        )
    else:
        advise(
            "OK",
            f"current α={current_alpha:.2f} is within "
            f"{100*reduction:.1f}% of the sweep optimum. No meaningful "
            "tuning headroom on α.",
        )


def report_weight_sweep(ok: pd.DataFrame, grid: int) -> None:
    """Sweep `queue_workload_weight` and report mean |residual|.

    No-op on current-schema CSVs: the picker dropped the constant-vs-
    workload blend in favor of the position-discount model, so
    `queue_workload_weight` is frozen at 1.0. The sweep only produces
    a useful surface on legacy CSVs that captured the live blend value.
    """
    section("Weight sweep (queue_workload_weight)")
    if len(ok) < 10:
        advise("LOW", "too few rows for a meaningful weight sweep.")
        return

    if ok["queue_workload_weight"].nunique() == 1 and ok["queue_workload_weight"].iloc[0] == 1.0:
        advise(
            "OK",
            "queue_workload_weight is constant 1.0 (current schema dropped "
            "the constant-vs-workload blend). Weight sweep is uninformative; "
            "skipping.",
        )
        return

    current_w = float(ok["queue_workload_weight"].mode().iloc[0])
    weights = np.linspace(0.0, 1.0, grid)
    rows = []
    for w in weights:
        replayed = replay_weight(ok, float(w))
        mae = replayed["residual_ms"].abs().mean()
        p95 = replayed["residual_ms"].abs().quantile(0.95)
        rows.append((float(w), mae, p95))

    print(f"  rows in sweep: {len(ok)}")
    print(f"  current w    : {current_w:.3f}")
    print()
    print(f"  {'w':>6s}  {'mean |resid|':>14s}  {'p95 |resid|':>13s}")
    for w, mae, p95 in rows:
        marker = " ←" if abs(w - current_w) < (1.0 / (grid - 1)) / 2 else ""
        print(f"  {w:>6.3f}  {mae:>14.1f}  {p95:>13.1f}{marker}")

    best_idx = int(np.argmin([r[1] for r in rows]))
    best_w, best_mae, _ = rows[best_idx]
    current_mae = next(
        (mae for w, mae, _ in rows if abs(w - current_w) < 1e-6),
        rows[int(round(current_w * (grid - 1)))][1],
    )
    reduction = (current_mae - best_mae) / max(current_mae, 1e-9)

    print()
    print(f"  optimum w    : {best_w:.3f}  (mean |resid| = {best_mae:.1f})")
    print(f"  current w    : {current_w:.3f}  (mean |resid| = {current_mae:.1f})")
    print(f"  improvement  : {100*reduction:.1f}% reduction in mean |resid|")

    if reduction > 0.10:
        advise(
            "HIGH",
            f"w={best_w:.2f} would reduce mean |resid| by "
            f"{100*reduction:.0f}%. Switching weight is a cheap, "
            "operationally-safe change.",
        )
    elif reduction > 0.03:
        advise(
            "MEDIUM",
            f"w={best_w:.2f} would reduce mean |resid| by "
            f"{100*reduction:.0f}%. Modest win.",
        )
    else:
        advise(
            "OK",
            f"current w={current_w:.2f} is within "
            f"{100*reduction:.1f}% of the sweep optimum.",
        )


def report_q_running_sweep(ok: pd.DataFrame) -> None:
    """3D grid sweep over the q_running U-shape: peak_ms × peak_qr ×
    falloff_qr. Picks the combination that minimizes mean |residual|
    and reports the candidate config values directly — analogous to
    the α-sweep, but over three knobs instead of one.

    The sweep is only meaningful when the dataset has rows with
    `effective_qr = q_running + q_inflight > 0` — otherwise every
    candidate produces q_running_ms=0 and the surface is flat. Below
    a threshold of 10 such rows the sweep is skipped.

    Grid resolution: 8 × 6 × 5 = 240 combinations (a few hundred
    ms in numpy). Hard-coded values cover the empirically-relevant
    range:
      - peak_ms:    0, 100, 300, 500, 700, 1000, 1500, 2000
      - peak_qr:    1, 2, 3, 4, 5, 6
      - falloff_qr: 3, 5, 7, 10, 15
    """
    section("q_running U-shape sweep")

    effective_qr = ok["q_running"].astype(int) + ok["q_inflight"].astype(int)
    n_active = int((effective_qr > 0).sum())
    if n_active < 10:
        advise(
            "LOW",
            f"only {n_active} rows with q_running+q_inflight > 0 — "
            "every candidate produces near-zero q_running_ms across the "
            "dataset and the sweep surface is flat. Run a higher-"
            "concurrency bench to exercise the U-shape.",
        )
        return

    current_peak_ms = float(ok["q_running_penalty_peak_ms"].mode().iloc[0])
    current_peak_qr = int(ok["q_running_penalty_peak_qr"].mode().iloc[0])
    current_falloff = int(ok["q_running_penalty_falloff_qr"].mode().iloc[0])

    # Baseline MAE under the current (as-emitted) q_running_ms.
    current_mae = ok["residual_ms"].abs().mean()

    peak_ms_grid = [0, 100, 300, 500, 700, 1000, 1500, 2000]
    peak_qr_grid = [1, 2, 3, 4, 5, 6]
    falloff_grid = [3, 5, 7, 10, 15]

    results: list[tuple[float, int, int, float]] = []
    best = (current_peak_ms, current_peak_qr, current_falloff, current_mae)
    for peak_ms in peak_ms_grid:
        for peak_qr in peak_qr_grid:
            for falloff in falloff_grid:
                if falloff <= peak_qr:
                    continue
                replayed = replay_q_running(ok, peak_ms, peak_qr, falloff)
                mae = float(replayed["residual_ms"].abs().mean())
                results.append((float(peak_ms), peak_qr, falloff, mae))
                if mae < best[3]:
                    best = (float(peak_ms), peak_qr, falloff, mae)

    print(f"  rows in sweep: {len(ok)}  (effective_qr>0 on {n_active}, "
          f"{100*n_active/len(ok):.1f}%)")
    print(f"  current : peak_ms={current_peak_ms:>5.0f}  peak_qr={current_peak_qr}  "
          f"falloff_qr={current_falloff}  mean|resid|={current_mae:>7.1f}")
    print(f"  optimum : peak_ms={best[0]:>5.0f}  peak_qr={best[1]}  "
          f"falloff_qr={best[2]}  mean|resid|={best[3]:>7.1f}")
    reduction = (current_mae - best[3]) / max(current_mae, 1e-9)
    print(f"  reduction: {100*reduction:.1f}%")
    print()

    # Show the (peak_ms × peak_qr) slice at the optimum falloff_qr —
    # makes the curvature of the surface visible without dumping all
    # 240 cells.
    best_falloff = best[2]
    by_qr_at_best_fo: dict[tuple[float, int], float] = {
        (pm, pq): mae for (pm, pq, fo, mae) in results if fo == best_falloff
    }
    print(f"  mean |resid| slice at falloff_qr={best_falloff}:")
    header = "  peak_ms\\peak_qr  " + " ".join(f"{q:>7d}" for q in peak_qr_grid)
    print(header)
    for pm in peak_ms_grid:
        row = " ".join(
            f"{by_qr_at_best_fo.get((float(pm), pq), float('nan')):>7.0f}"
            if not np.isnan(by_qr_at_best_fo.get((float(pm), pq), float('nan')))
            else f"{'-':>7s}"
            for pq in peak_qr_grid
        )
        marker = " ←" if abs(pm - best[0]) < 1e-6 else ""
        print(f"  {pm:>14.0f}  {row}{marker}")
    print()

    # Projected values from the sweep optimum. NOT a recipe — these
    # are the argmin of mean |residual| under THIS dataset's
    # routing distribution. Adopting them will shift routing, which
    # shifts the data, which shifts the next sweep's optimum.
    print(f"  Sweep optimum projected onto [predictor] knobs "
          "(DIAGNOSTIC — see banner above):")
    print(f"    {'param':45s}  {'current':>10s}  {'projected':>12s}")
    print(f"    {'q_running_penalty_peak_ms':45s}  "
          f"{current_peak_ms:>10.1f}  {best[0]:>12.1f}")
    print(f"    {'q_running_penalty_peak_qr':45s}  "
          f"{current_peak_qr:>10d}  {best[1]:>12d}")
    print(f"    {'q_running_penalty_falloff_qr':45s}  "
          f"{current_falloff:>10d}  {best[2]:>12d}")
    print()

    if reduction > 0.10:
        advise(
            "OBSERVATION",
            f"on this dataset, the U-shape optimum reduces mean |resid| "
            f"by {100*reduction:.0f}%. Worth investigating WHY (is the "
            "current peak_qr off? wrong magnitude?) but don't blindly "
            "adopt the projected values — re-bench multiple times to "
            "estimate noise first.",
        )
    elif reduction > 0.03:
        advise(
            "MILD",
            f"the U-shape optimum is only {100*reduction:.0f}% better — "
            "within typical single-run noise. Don't iterate on this "
            "unless multiple runs agree.",
        )
    else:
        advise(
            "OK",
            f"current q_running U-shape is within {100*reduction:.1f}% of "
            "the sweep optimum. Either it's well-tuned or the workload "
            "doesn't exercise the contention zone enough to distinguish.",
        )


def report_alpha_weight_joint(ok: pd.DataFrame, alpha_grid: int, weight_grid: int) -> None:
    """Joint (α, w) sweep. α touches overlap_blocks → prefill_ms only;
    w touches q_ms only. They're independent in the formula, so the
    joint optimum should equal `(α*, w*)` from the marginal sweeps.
    The joint sweep is here as a sanity check and to visualize the
    surface shape."""
    section("Joint (α, w) sweep")
    if ok["queue_workload_weight"].nunique() == 1 and ok["queue_workload_weight"].iloc[0] == 1.0:
        advise(
            "OK",
            "queue_workload_weight is fixed at 1.0 in the current picker — "
            "joint sweep collapses to the α-sweep alone. Skipping.",
        )
        return
    sub = ok[ok["inflight_active"]]
    if len(sub) < 10:
        advise(
            "LOW",
            "too few inflight-active rows for the joint sweep "
            "(α-irrelevant rows would dominate).",
        )
        return

    alphas = np.linspace(0.0, 1.0, alpha_grid)
    weights = np.linspace(0.0, 1.0, weight_grid)

    # Compute mean |resid| at every (α, w).
    grid = np.zeros((len(weights), len(alphas)))
    for i, a in enumerate(alphas):
        sub_a = replay_alpha(sub, float(a))
        for j, w in enumerate(weights):
            grid[j, i] = replay_weight(sub_a, float(w))["residual_ms"].abs().mean()

    best_jw, best_ia = np.unravel_index(int(np.argmin(grid)), grid.shape)
    best_a = float(alphas[best_ia])
    best_w = float(weights[best_jw])
    best_mae = float(grid[best_jw, best_ia])

    current_a = float(ok["expected_overlap_alpha"].mode().iloc[0])
    current_w = float(ok["queue_workload_weight"].mode().iloc[0])

    # Heatmap (text). Print on a coarse grid for legibility.
    print(f"  rows in joint sweep: {len(sub)} (q_inflight > 0)")
    print(f"  current (α, w)     : ({current_a:.2f}, {current_w:.2f})")
    print(f"  optimum (α, w)     : ({best_a:.2f}, {best_w:.2f})")
    print(f"  optimum mean|resid|: {best_mae:.1f}")
    print()

    # Pick a stride that fits 11-ish columns regardless of grid size.
    step_a = max(1, len(alphas) // 10)
    step_w = max(1, len(weights) // 10)
    print(f"  mean |resid| heatmap (rows=w↑, cols=α→):")
    header = "  w\\α   " + " ".join(f"{alphas[i]:>5.2f}" for i in range(0, len(alphas), step_a))
    print(header)
    for j in range(0, len(weights), step_w):
        row = " ".join(
            f"{grid[j, i]:>5.0f}" for i in range(0, len(alphas), step_a)
        )
        print(f"  {weights[j]:>5.2f}  {row}")

    # Verdict comes from comparing optimum to current.
    current_mae = grid[
        np.argmin(np.abs(weights - current_w)),
        np.argmin(np.abs(alphas - current_a)),
    ]
    reduction = (current_mae - best_mae) / max(current_mae, 1e-9)
    if reduction > 0.10:
        advise(
            "HIGH",
            f"joint (α, w) = ({best_a:.2f}, {best_w:.2f}) reduces mean "
            f"|resid| by {100*reduction:.0f}% over the current "
            f"({current_a:.2f}, {current_w:.2f}). Worth adopting.",
        )
    elif reduction > 0.03:
        advise(
            "MEDIUM",
            f"joint optimum reduces mean |resid| by {100*reduction:.0f}%. "
            "Modest win.",
        )
    else:
        advise(
            "OK",
            f"joint optimum within {100*reduction:.1f}% of current. "
            "(α, w) are well-tuned together; remaining error is elsewhere.",
        )


def report_forward_pass_sweep(ok: pd.DataFrame) -> None:
    """Sweep `forward_pass_ms` and report mean |residual|.

    `decode_contention_ms = (q_running + q_inflight) × forward_pass_ms`,
    so this is a one-knob linear sweep. We restrict to rows where the
    term is non-zero (effective_qr > 0); the others are insensitive.

    Caveat: this sweep only changes a single term. Bumping
    `forward_pass_ms` from 0 → 30 also raises the absolute predicted_ms
    by 30 × effective_qr per row, which may *increase* mean |residual|
    if the model is already over-predicting overall. A negative
    headroom finding here is informative: it says "this term is
    structurally too small to be tuned UP without first tightening
    other components" — i.e., fix the over-prediction floor first.
    """
    section("forward_pass_ms sweep")

    effective_qr = ok["q_running"] + ok["q_inflight"]
    sub = ok[effective_qr > 0].copy()
    sub_qr = effective_qr[effective_qr > 0]
    if len(sub) < 10:
        advise(
            "LOW",
            f"only {len(sub)} rows with q_running+q_inflight > 0 — "
            "forward_pass_ms sweep is uninformative. Run a higher-"
            "concurrency benchmark to exercise the decode-contention term.",
        )
        return

    current_fp = float(ok["forward_pass_ms"].mode().iloc[0])
    grid = [0.0, 5.0, 10.0, 20.0, 30.0, 50.0, 75.0, 100.0]
    # Base predicted, with the decode_contention term stripped out so
    # we can rebuild it at each candidate without depending on the
    # current config value.
    base_pred = sub["predicted_ms"] - sub["decode_contention_ms"]

    rows = []
    for fp in grid:
        alt_pred = base_pred + sub_qr * fp
        alt_resid = sub["observed_ms"] - alt_pred
        mae = alt_resid.abs().mean()
        p95 = alt_resid.abs().quantile(0.95)
        rows.append((fp, mae, p95))

    print(f"  rows in sweep: {len(sub)}  "
          f"(q_running+q_inflight > 0; mean effective_qr = {sub_qr.mean():.1f})")
    print(f"  current fp   : {current_fp:.1f} ms")
    print()
    print(f"  {'forward_pass_ms':>15s}  {'mean |resid|':>14s}  {'p95 |resid|':>13s}")
    for fp, mae, p95 in rows:
        marker = " ←" if abs(fp - current_fp) < 0.5 else ""
        print(f"  {fp:>15.1f}  {mae:>14.1f}  {p95:>13.1f}{marker}")

    best_idx = int(np.argmin([r[1] for r in rows]))
    best_fp, best_mae, _ = rows[best_idx]
    # Current MAE: if current_fp isn't on the grid, replay it.
    if any(abs(fp - current_fp) < 0.5 for fp, _, _ in rows):
        current_mae = next(mae for fp, mae, _ in rows if abs(fp - current_fp) < 0.5)
    else:
        alt_pred = base_pred + sub_qr * current_fp
        current_mae = (sub["observed_ms"] - alt_pred).abs().mean()
    reduction = (current_mae - best_mae) / max(current_mae, 1e-9)

    print()
    print(f"  optimum fp   : {best_fp:.1f}  (mean |resid| = {best_mae:.1f})")
    print(f"  current fp   : {current_fp:.1f}  (mean |resid| = {current_mae:.1f})")
    print(f"  improvement  : {100*reduction:.1f}% reduction in mean |resid|")

    # Boundary check: optimum at fp=0 means the model is already
    # over-predicting and adding decode-contention only makes it worse
    # on average. That's a "fix something else first" signal.
    at_low_boundary = best_fp == grid[0]
    at_high_boundary = best_fp == grid[-1]
    if at_low_boundary:
        advise(
            "OBSERVATION",
            f"optimum forward_pass_ms = 0.0 — the model is already "
            f"over-predicting on the cohort that exercises this term, "
            f"so any positive fp grows the residual. "
            "Don't raise fp until other components (base_ms intercept, "
            "in-flight discount, prefill quadratic) are tightened. The "
            "term's correct value can only be diagnosed AFTER mean "
            "residual is near zero.",
        )
        return
    if at_high_boundary:
        advise(
            "OBSERVATION",
            f"optimum sits at the top of the grid ({best_fp:.0f} ms) — "
            "the sweep wants to go higher. Extend the grid before "
            "drawing a conclusion; the curve may still be falling.",
        )
        return
    if reduction > 0.10:
        advise(
            "OBSERVATION",
            f"on this dataset, forward_pass_ms = {best_fp:.0f} would "
            f"reduce mean |resid| by {100*reduction:.0f}% on the "
            f"decode-loaded cohort (effective_qr > 0). Single-run "
            f"estimate — re-run the bench 2-3× before adopting.",
        )
    elif reduction > 0.03:
        advise(
            "MEDIUM",
            f"forward_pass_ms = {best_fp:.0f} would reduce mean |resid| "
            f"by {100*reduction:.0f}%. Modest win.",
        )
    else:
        advise(
            "OK",
            f"current forward_pass_ms = {current_fp:.1f} is within "
            f"{100*reduction:.1f}% of the sweep optimum on this dataset.",
        )


def report_coefficient_refit(ok: pd.DataFrame) -> None:
    """Two refits, both run side-by-side:

    1. Per-component multiplier fit:
       `observed ~ b0 + α · q_ms + β · prefill_ms + γ · cache_ms +
                       δ · q_running_ms`.
       Coefficients are dimensionless; a value of 1.0 means "current
       component is correct," values far from 1.0 are tuning targets.

    2. Raw-feature fit (quadratic-prefill-aware):
       `observed ~ b0 + b1 · q_waiting + b2 · uncached + b3 · uncached·total
                       + b4 · kv_cache_usage`.
       The `b2`/`b3` pair maps directly to
       `attention_uncached_block_ms` / `attention_uncached_per_every_block_ms`.

    q_running_ms is in Fit(1) so you can see whether the U-shape's
    overall magnitude is right — but the shape's three knobs aren't in
    the CSV, so Fit(2) doesn't expose them; see report_q_running.
    """
    section("Coefficient refit")

    n = len(ok)
    if n < 20:
        advise("LOW", f"only {n} rows — refit is over-fit on this dataset.")
        return

    y = ok["observed_ms"].astype(float).values
    ones = np.ones(n)

    # (1) Component-multiplier fit. Include q_running_ms only when it
    # has any variation (otherwise it's a constant column → singular).
    has_qr = ok["q_running_ms"].std() > 0
    if has_qr:
        X1 = np.column_stack([
            ones,
            ok["q_ms"].astype(float).values,
            ok["prefill_ms"].astype(float).values,
            ok["cache_ms"].astype(float).values,
            ok["q_running_ms"].astype(float).values,
        ])
        comp_names = ("q_ms", "prefill_ms", "cache_ms", "q_running_ms")
    else:
        X1 = np.column_stack([
            ones,
            ok["q_ms"].astype(float).values,
            ok["prefill_ms"].astype(float).values,
            ok["cache_ms"].astype(float).values,
        ])
        comp_names = ("q_ms", "prefill_ms", "cache_ms")
    beta1, *_ = np.linalg.lstsq(X1, y, rcond=None)
    y_hat1 = X1 @ beta1
    ss_tot = ((y - y.mean()) ** 2).sum()
    r2_1 = 1 - ((y - y_hat1) ** 2).sum() / max(ss_tot, 1e-9)

    # Pull current config values from the CSV (every row carries them).
    cur_base = float(ok["base_ms"].iloc[0])
    cur_qrate = float(ok["queue_ms_per_entry"].iloc[0])
    cur_attn_lin = float(ok["attention_uncached_block_ms"].iloc[0])
    cur_attn_quad = float(ok["attention_uncached_per_every_block_ms"].iloc[0])
    cur_cache_penalty = float(ok["cache_pressure_penalty_ms"].iloc[0])

    # Reliability of each multiplier — a component with near-zero
    # variance can't be distinguished from the intercept, so its
    # multiplier is noise. Threshold: 1 ms mean-absolute. Below that,
    # we don't propose a new value for the underlying config knob.
    component_mean_abs = {
        comp: ok[comp].abs().mean() for comp in comp_names
    }
    def reliable(comp: str) -> bool:
        return component_mean_abs.get(comp, 0) >= 1.0

    # Translate multipliers → config knob values.
    # - β scales prefill_ms = a·u + c·u·t uniformly by scaling both
    #   attention coefs by β. Preserves the curve's shape (the data
    #   can't separate linear vs quadratic when prompt sizes don't
    #   vary much, so uniform scaling is the safe choice).
    # - α scales q_ms; queue_ms_per_entry is the dominant knob.
    # - γ scales cache_ms; only cache_pressure_penalty_ms drives it.
    # - b0 replaces base_ms (the new intercept is the recommended
    #   constant offset, not "current base + b0").
    alpha = float(beta1[1])
    beta = float(beta1[2])
    gamma = float(beta1[3])
    new_base = float(beta1[0])
    new_qrate = cur_qrate * alpha
    new_attn_lin = cur_attn_lin * beta
    new_attn_quad = cur_attn_quad * beta
    new_cache_penalty = cur_cache_penalty * gamma

    if has_qr:
        print(
            f"  Fit (1): observed ~ b0 + α · q_ms + β · prefill_ms + "
            f"γ · cache_ms + δ · q_running_ms"
        )
    else:
        print(f"  Fit (1): observed ~ b0 + α · q_ms + β · prefill_ms + γ · cache_ms")
    print(f"    b0   (intercept) = {beta1[0]:>9.1f}  ms")
    print(f"    α    (q_ms multiplier)        = {beta1[1]:>9.3f}  (1.0 = current is correct)")
    print(f"    β    (prefill_ms multiplier)  = {beta1[2]:>9.3f}")
    print(f"    γ    (cache_ms multiplier)    = {beta1[3]:>9.3f}")
    if has_qr:
        print(f"    δ    (q_running_ms mult)      = {beta1[4]:>9.3f}")
    print(f"    R²                            = {r2_1:>9.3f}")
    print()

    # Translate Fit (1)'s multipliers into hypothetical config values.
    # NOT a tuning recipe — these are projections of the multipliers
    # back onto knob space, useful for spotting "is anything an order
    # of magnitude off?" but NOT for incremental tuning. Skip rows
    # where the underlying component had near-zero variance (the
    # multiplier is noise) and where the underlying feature was
    # constant in the data (refit has no signal).
    print(f"  Fit (1) projected knob values (DIAGNOSTIC ONLY — see banner above)")
    print(f"  Use this to spot anomalies (>2× or <0.5×), not to set values.")
    print(f"    {'param':45s}  {'current':>12s}  {'projected':>14s}")
    print(f"    {'base_ms':45s}  {cur_base:>12.1f}  {new_base:>14.1f}")
    # Data-quality gate for queue_ms_per_entry: if q_waiting was
    # ~constant across the dataset, the model never saw a queue
    # signal and the multiplier is meaningless regardless of α's
    # value. This burned us before — Fit(2) said queue=0 because
    # there was no signal, and adopting it broke routing entirely.
    q_waiting_has_signal = ok["q_waiting"].std() > 0.5
    if reliable("q_ms") and q_waiting_has_signal:
        print(f"    {'queue_ms_per_entry':45s}  {cur_qrate:>12.1f}  {new_qrate:>14.1f}")
    elif not q_waiting_has_signal:
        print(
            f"    {'queue_ms_per_entry':45s}  {cur_qrate:>12.1f}  "
            f"{'(no signal)':>14s}  q_waiting was ~constant; cannot refit "
            "the queue cost"
        )
    else:
        print(
            f"    {'queue_ms_per_entry':45s}  {cur_qrate:>12.1f}  "
            f"{'(no signal)':>14s}  q_ms mean ≈ 0; α is noise"
        )
    if reliable("prefill_ms"):
        # Both attention coefs scale by β. If only one is currently
        # non-zero, scaling preserves that — the other stays 0.
        print(f"    {'attention_uncached_block_ms':45s}  {cur_attn_lin:>12.3f}  {new_attn_lin:>14.3f}")
        print(
            f"    {'attention_uncached_per_every_block_ms':45s}  "
            f"{cur_attn_quad:>12.6f}  {new_attn_quad:>14.6f}"
        )
    else:
        print(
            f"    {'attention_uncached_block_ms':45s}  {cur_attn_lin:>12.3f}  "
            f"{'(skip)':>14s}  prefill_ms mean ≈ 0; β is noise"
        )
    if reliable("cache_ms"):
        print(
            f"    {'cache_pressure_penalty_ms':45s}  "
            f"{cur_cache_penalty:>12.1f}  {new_cache_penalty:>14.1f}"
        )
    else:
        print(
            f"    {'cache_pressure_penalty_ms':45s}  "
            f"{cur_cache_penalty:>12.1f}  {'(skip)':>14s}  cache_ms ≈ 0 across "
            "rows; γ is noise"
        )
    if has_qr and reliable("q_running_ms"):
        delta = float(beta1[4])
        cur_q_running_peak_ms = float(
            ok["q_running_penalty_peak_ms"].mode().iloc[0]
        )
        # δ scales the EMITTED q_running_ms uniformly. peak_ms is
        # the only linear magnitude knob in the U-shape (peak_qr /
        # falloff_qr define the shape, not amplitude), so scaling
        # peak_ms by δ scales every per-row penalty by δ.
        #
        # Edge case: if the current peak_ms is 0, the penalty is
        # disabled and `cur × δ = 0`, which is meaningless. Defer
        # to the q_running sweep section for a starting value.
        if cur_q_running_peak_ms > 0.0:
            new_q_running_peak_ms = cur_q_running_peak_ms * delta
            print(
                f"    {'q_running_penalty_peak_ms':45s}  "
                f"{cur_q_running_peak_ms:>12.1f}  {new_q_running_peak_ms:>14.1f}"
            )
        else:
            print(
                f"    {'q_running_penalty_peak_ms':45s}  "
                f"{cur_q_running_peak_ms:>12.1f}  {'(disabled)':>14s}  "
                "penalty is off; δ scaling is meaningless. See "
                "report_q_running_sweep for a starting value."
            )
    print(
        f"    note: attention coefs scale uniformly by β to preserve "
        "the curve shape — the data usually can't separate the linear "
        "and quadratic terms (multicollinearity)."
    )
    if has_qr and reliable("q_running_ms"):
        print(
            f"    note: q_running_penalty_peak_ms scales by δ; "
            "peak_qr and falloff_qr define the U-shape and are not refit "
            "here — use `report_q_running_sweep` to tune the shape."
        )
    print()

    # (2) Raw-feature fit, quadratic-prefill-aware (direct coefficient
    # candidates). `uncached × total` is the quadratic term; its
    # coefficient maps directly to `attention_uncached_per_every_block_ms`.
    uncached = ok["uncached_blocks"].astype(float).values
    total = ok["total_blocks"].astype(float).values
    X2 = np.column_stack([
        ones,
        ok["q_waiting"].astype(float).values,
        uncached,
        uncached * total,
        ok["kv_cache_usage"].astype(float).values,
    ])
    beta2, *_ = np.linalg.lstsq(X2, y, rcond=None)
    y_hat2 = X2 @ beta2
    r2_2 = 1 - ((y - y_hat2) ** 2).sum() / max(ss_tot, 1e-9)

    # cur_* values were pulled at the top of the function for Fit (1)'s
    # recommendation table; reuse them here.

    print(
        f"  Fit (2): observed ~ b0 + b1 · q_waiting + b2 · uncached + "
        f"b3 · uncached·total + b4 · kv_cache_usage"
    )
    print(f"    {'param':45s}  {'current':>10s}  {'fitted':>10s}")
    print(f"    {'base_ms':45s}  {cur_base:>10.1f}  {beta2[0]:>10.1f}")
    print(f"    {'queue_ms_per_entry':45s}  {cur_qrate:>10.1f}  {beta2[1]:>10.1f}")
    print(f"    {'attention_uncached_block_ms':45s}  {cur_attn_lin:>10.3f}  {beta2[2]:>10.3f}")
    print(f"    {'attention_uncached_per_every_block_ms':45s}  {cur_attn_quad:>10.6f}  {beta2[3]:>10.6f}")
    print(f"    {'cache_pressure_penalty_ms':45s}  {cur_cache_penalty:>10.1f}  {beta2[4]:>10.1f}")
    print(f"    R²                                              = {r2_2:>9.3f}")
    # Flag obviously-nonsensical fitted coefficients (negative when
    # the feature physically can only add latency).
    nonsense = []
    if beta2[1] < -50:
        nonsense.append("queue_ms_per_entry strongly negative")
    if beta2[2] < 0:
        nonsense.append("attention_uncached_block_ms < 0")
    if beta2[3] < 0:
        nonsense.append("attention_uncached_per_every_block_ms < 0")
    if beta2[4] < -50:
        nonsense.append("cache_pressure_penalty_ms strongly negative")
    if nonsense:
        print(
            f"    NOTE: Fit(2) produced physically nonsensical "
            f"coefficient(s): {', '.join(nonsense)}. Don't adopt those — "
            f"either multicollinearity (the raw features are correlated "
            f"with each other) or selection bias. Trust Fit(1) above "
            f"for component-level tuning targets."
        )
    print()

    # Decide which fit to advise from. q_running's δ enters when present.
    mult_pairs = [(comp_names[i], beta1[i + 1]) for i in range(len(comp_names))]
    # Cache often runs near 0 in datasets without cache pressure — ignore
    # multipliers on near-zero components to avoid noise-driven verdicts.
    component_mean_abs = {
        comp: ok[comp].abs().mean() for comp in comp_names
    }
    mult_pairs_meaningful = [
        (comp, mult) for comp, mult in mult_pairs
        if component_mean_abs.get(comp, 0) > 1.0
    ]
    mult_pairs_use = mult_pairs_meaningful or mult_pairs
    mult_off = max(abs(mult - 1.0) for _, mult in mult_pairs_use)
    if mult_off > 0.5:
        worst_comp, worst_mult = max(
            mult_pairs_use, key=lambda kv: abs(kv[1] - 1.0)
        )
        advise(
            "ANOMALY",
            f"`{worst_comp}` multiplier fits to {worst_mult:.2f} on this "
            f"dataset — that's a {abs(worst_mult - 1)*100:.0f}% gap from 1.0. "
            "Worth investigating WHY (workload mismatch? wrong coef? "
            "missing feature?) before adjusting any knob. Single-knob "
            "adjustments based on this number tend not to converge — "
            "see the banner at the top of this section.",
        )
    elif mult_off > 0.2:
        advise(
            "MILD",
            f"largest meaningful component multiplier is "
            f"{mult_off + 1:.2f} away from 1.0 — within the range of "
            "run-to-run noise on a single bench. Don't act unless you "
            "see it consistently across multiple runs.",
        )
    else:
        advise(
            "OK",
            "all component multipliers within ±20% of 1.0; coefficients are "
            "well-calibrated.",
        )


# ============================================================================
# Report sections — V. Asymmetry detection
# ============================================================================

def report_inflight_overlap_failures(ok: pd.DataFrame) -> None:
    """Rows where the inflight overlap list had at least one large
    entry — meaning a still-in-flight dispatch overlapped substantially
    with our prompt's prefix. If we credited that cache-warming (high
    α) but the inflight request hadn't actually finished prefilling
    by the time our request reached the worker, we over-predict the
    overlap → under-predict the prefill_ms → residual skews positive."""
    section("Asymmetry: high inflight-overlap credit")
    has_max = ok["inflight_overlap_list"].map(lambda lst: max(lst) if lst else 0)
    high = ok[has_max > 100]
    rest = ok[has_max <= 100]
    n_high = len(high)

    print(
        f"  rows with max(inflight_overlap) > 100 blocks: "
        f"{n_high} ({100*n_high/len(ok):.1f}%)"
    )
    if n_high:
        print(f"    mean residual (high):    {high['residual_ms'].mean():+.1f}")
        print(f"    median residual (high):  {high['residual_ms'].median():+.1f}")
    if len(rest):
        print(f"    mean residual (others):  {rest['residual_ms'].mean():+.1f}")
        print(f"    median residual (others):{rest['residual_ms'].median():+.1f}")

    if n_high < 5:
        advise(
            "LOW",
            "too few high-inflight-overlap rows to assess. Either "
            "concurrency is low or inflight dispatches don't share "
            "prefixes much.",
        )
        return

    delta = high["residual_ms"].median() - rest["residual_ms"].median()
    if delta > 500:
        advise(
            "HIGH",
            f"high-inflight-overlap rows skew {delta:+.0f}ms (under-"
            "predicted) vs others — we credited cache warming from "
            "inflight dispatches that hadn't materialized. α is too high; "
            "the α-sweep section quantifies the right value.",
        )
    elif delta < -500:
        advise(
            "HIGH",
            f"high-inflight-overlap rows skew {delta:+.0f}ms (over-"
            "predicted) vs others — the model thinks the worker is "
            "slower than it actually is when many inflight dispatches "
            "share our prefix. This is the opposite asymmetry: "
            "inflight crediting is real and α might even be too low; "
            "or the inflight dispatches are warming the cache "
            "non-linearly (your prompt benefits more than the layered "
            "formula predicts).",
        )
    elif abs(delta) > 200:
        advise(
            "MEDIUM",
            f"high-inflight-overlap rows skew {delta:+.0f}ms vs others "
            f"— modest "
            f"{'under-crediting' if delta > 0 else 'over-crediting'} "
            "of inflight cache warming.",
        )
    else:
        advise(
            "OK",
            f"high-inflight-overlap rows track others within "
            f"{abs(delta):.0f}ms — α is reasonably tuned.",
        )


def report_workload_overcharge(ok: pd.DataFrame) -> None:
    """Rows where the workload model is charging queue cost but the
    scorer says queue is empty — and weight is high enough that the
    workload model dominates. vLLM admits concurrently when there's
    headroom, so this q_workload "cost" may not actually materialize.
    Residuals on those rows should skew negative (we over-predicted)."""
    section("Asymmetry: workload over-charge in q_waiting=0 regime")
    if ok["queue_workload_weight"].nunique() == 1 and ok["queue_workload_weight"].iloc[0] == 1.0:
        # Current-schema CSVs: the constant-vs-workload blend is gone;
        # the workload model is always at weight=1. The "weight≥0.9"
        # filter would match every workload-bearing row, which is the
        # whole population — there's no comparison to draw.
        advise(
            "OK",
            "queue_workload_weight is frozen at 1.0 in the current picker — "
            "the asymmetry-detection filter has no comparison cohort. Skipping.",
        )
        return
    suspect = ok[
        (ok["q_waiting"] == 0)
        & (ok["q_workload"] > 0)
        & (ok["queue_workload_weight"] >= 0.9)
    ]
    rest = ok[~ok.index.isin(suspect.index)]
    n_susp = len(suspect)

    print(
        f"  rows with q_waiting=0 AND q_workload>0 AND weight≥0.9: "
        f"{n_susp} ({100*n_susp/len(ok):.1f}%)"
    )
    if n_susp:
        print(f"    mean residual (suspect):    {suspect['residual_ms'].mean():+.1f}")
        print(f"    median residual (suspect):  {suspect['residual_ms'].median():+.1f}")
    if len(rest):
        print(f"    mean residual (others):     {rest['residual_ms'].mean():+.1f}")

    if n_susp < 5:
        advise(
            "LOW",
            "too few rows in the suspect regime to assess. Workload "
            "over-charge is dataset-specific; this section will fire "
            "more on weight=1 runs.",
        )
        return

    delta = suspect["residual_ms"].median() - (
        rest["residual_ms"].median() if len(rest) else 0.0
    )
    if delta < -500:
        advise(
            "HIGH",
            f"suspect rows skew {delta:+.0f}ms vs others — we charged "
            "queue cost based on q_workload of inflight dispatches "
            "that didn't actually queue. Either lower "
            "`queue_workload_weight` or gate the workload model on "
            "admission-headroom (kv_cache_usage above some threshold).",
        )
    elif delta < -200:
        advise(
            "MEDIUM",
            f"suspect rows skew {delta:+.0f}ms vs others — modest "
            "workload over-charge. The weight-sweep section will "
            "indicate whether lowering w would help globally.",
        )
    else:
        advise(
            "OK",
            f"suspect rows track others within {abs(delta):.0f}ms — "
            "workload model isn't over-charging in this dataset.",
        )


# ============================================================================
# Report sections — VI. Operational
# ============================================================================

def report_outliers(ok: pd.DataFrame, n: int) -> None:
    section(f"Outliers (top {n} each direction)")
    cols = [
        "request_id", "name", "endpoint",
        "q_waiting", "q_inflight", "q_running",
        "uncached_blocks", "overlap_blocks",
        "kv_cache_usage", "compl_per_s",
        "q_running_ms",
        "predicted_ms", "observed_ms", "residual_ms",
    ]
    # If the upstream tracing id is populated, show it right after the
    # integer request_id — both can help locate the request in
    # logs / traces.
    if "x_request_id" in ok.columns and (ok["x_request_id"] != "").any():
        cols.insert(1, "x_request_id")
    avail = [c for c in cols if c in ok.columns]

    pos = ok.nlargest(n, "residual_ms")
    neg = ok.nsmallest(n, "residual_ms")
    print(f"  most-positive (model under-predicts):")
    print(pos[avail].to_string(index=False))
    print()
    print(f"  most-negative (model over-predicts):")
    print(neg[avail].to_string(index=False))

    # Tip suffix tailored to which id columns are populated.
    has_str_id = (
        "x_request_id" in ok.columns and (ok["x_request_id"] != "").any()
    )
    if has_str_id:
        grep_tip = (
            "Use `request_id` for `req=N` log greps and `x_request_id` "
            "for cross-system trace lookups."
        )
    else:
        grep_tip = "Grep the logs by `req=<request_id>` to inspect individual cases."

    pos_workers = pos["name"].value_counts()
    if len(pos_workers) and pos_workers.iloc[0] >= max(3, n // 2):
        dominant = pos_workers.index[0]
        advise(
            "HIGH",
            f"{pos_workers.iloc[0]}/{n} biggest positive outliers are "
            f"`{dominant}` — that worker is reliably slower than the "
            f"model expects. Per-worker EMA offset is the right fix. {grep_tip}",
        )
    else:
        advise(
            "LOW",
            f"outliers spread across workers — the tail is more about "
            f"workload structure than a specific worker. {grep_tip}",
        )


def report_herding(ok: pd.DataFrame) -> None:
    section("Herding fingerprint: per-worker observed-latency distribution")
    print(f"  {'worker':18s}  {'n':>4s}  "
          f"{'p50':>7s}  {'p95':>7s}  {'max':>7s}  {'max/p50':>8s}")
    ratios = []
    for w, g in ok.groupby("name"):
        p = g["observed_ms"]
        ratio = p.max() / p.median() if p.median() > 0 else float("nan")
        ratios.append(ratio)
        print(
            f"  {w:18s}  {len(g):>4d}  "
            f"{p.median():>7.0f}  "
            f"{p.quantile(0.95):>7.0f}  "
            f"{p.max():>7.0f}  "
            f"{ratio:>7.1f}x"
        )

    ratios = [r for r in ratios if not pd.isna(r)]
    max_ratio = max(ratios) if ratios else 0
    bad = sum(1 for r in ratios if r > 20)

    if max_ratio > 50:
        advise(
            "CRITICAL",
            f"max/p50 latency ratio of {max_ratio:.0f}× — severe herding. "
            "Concurrent requests are piling onto the same worker between "
            "metrics scrapes. Anti-dogpiling (top-K random / power-of-"
            "two-choices) is the highest-impact change for tail latency.",
        )
    elif max_ratio > 20:
        advise(
            "HIGH",
            f"max/p50 ratio of {max_ratio:.0f}× on {bad}/{len(ratios)} "
            "workers — clear herding signature.",
        )
    elif max_ratio > 8:
        advise(
            "MEDIUM",
            f"max/p50 ratio peaks at {max_ratio:.1f}× — some bursting "
            "but not consistently herding.",
        )
    else:
        advise(
            "OK",
            f"per-worker tail well-behaved (max ratio {max_ratio:.1f}×).",
        )


def report_concurrency(df: pd.DataFrame, n: int) -> None:
    section(f"Concurrency proxy: top {n} buckets (1-second windows)")
    df = df.copy()
    df["sec"] = df["ts"].dt.floor("s")
    fleet = df.groupby("sec").size().reset_index(name="reqs")
    fleet = fleet.sort_values("reqs", ascending=False)
    median_rps = fleet["reqs"].median()
    max_rps = fleet["reqs"].max()
    print(f"  median rps across the run: {median_rps:.0f}")
    print(f"  max rps                  : {max_rps}")
    print(f"  top fleet-wide:")
    print(fleet.head(n).to_string(index=False))

    per_worker = df.groupby(["sec", "name"]).size().reset_index(name="reqs")
    per_worker = per_worker.sort_values("reqs", ascending=False)
    max_per_worker = per_worker["reqs"].max() if len(per_worker) else 0
    print(f"\n  top per-worker:")
    print(per_worker.head(n).to_string(index=False))

    burstiness = max_rps / max(median_rps, 1)
    if max_per_worker >= 5:
        advise(
            "HIGH",
            f"single-worker peaks of {max_per_worker} reqs/s — the "
            "scorer's q_waiting=0 snapshot will lie to the picker "
            "during these bursts. Anti-dogpiling routing helps.",
        )
    elif burstiness > 5:
        advise(
            "MEDIUM",
            f"workload is bursty (peak {max_rps} rps vs median "
            f"{median_rps:.0f}). Bursts amplify any staleness.",
        )
    else:
        advise(
            "OK",
            f"concurrency is modest (max {max_rps} rps, median "
            f"{median_rps:.0f}).",
        )


# ============================================================================
# Tuning recommendation — the "what one thing should I change next?" section.
# ============================================================================
#
# The point of this section is to encode a tuning METHODOLOGY, not just to
# emit numbers. The methodology, learned the hard way while tuning this
# model:
#
# 1. FIX RESIDUALS FIRST. Routing/policy changes on top of a miscalibrated
#    predictor don't converge. Every routing knob's effect gets aliased with
#    the residual's own bias, and multi-run averages just chase the moving
#    target that miscalibration creates.
#
# 2. ONE KNOB AT A TIME. Multi-knob changes make attribution impossible.
#    When you make a change and the number moves, you must know which
#    coefficient moved it.
#
# 3. VERIFY AGAINST NOISE. Single-run p99 has wide variance (often ±5% of
#    the value). Any single-run "improvement" smaller than that band is
#    not real — re-run 2–3× with cache cleared to establish the noise
#    envelope before believing a knob helped.
#
# 4. WORK DOWN THE PRIORITY LADDER, DO NOT NAIVELY ADOPT FIT PROJECTIONS.
#    Fit(1)'s β multiplier and intercept are DIAGNOSTIC hints, not recipes.
#    The correct move is to identify the *single dominant* miscalibration,
#    take a step in its direction (typically 50–100% of the projection),
#    re-run, then re-read the residuals.
#
# The priority ladder (from most-to-least impactful, in this order):
#
#   A. Prefill quadratic — Fit(1) β multiplier on prefill_ms deviates
#      >15% from 1.0. Adjust `attention_uncached_per_every_block_ms`.
#   B. Uniform over-/under-prediction — Fit(1) intercept differs from
#      current `base_ms` by >150 ms. Shift `base_ms`, with the caveat
#      that this changes the replication-threshold scale.
#   C. In-flight discount over-charging — per-inflight over-charge (from
#      the q_inflight regime split) > 200 ms. This is STRUCTURAL:
#      `(1 - progress)` charges nearly-full prefill for a fresh inflight,
#      but vLLM pipelines it more aggressively. Not a coefficient fix;
#      flagged for a discount-model rework.
#   D. Residuals centered — mean residual within ±200 ms of zero.
#      Predictor is calibrated enough to reason about routing on top of.
#      Next tier: `forward_pass_ms`, `replication_threshold_pct`,
#      `top_k_fraction`.
#
# The function picks the FIRST rung of the ladder that applies and stops
# there. The operator then re-runs, comes back, sees the next
# recommendation.

def report_tuning_recommendation(ok: pd.DataFrame) -> None:
    section("Next single-knob change")

    # Snapshot current config from last row (every row carries the
    # config; last row is "what's in effect now").
    last = ok.iloc[-1]
    cur_base = float(last["base_ms"])
    cur_attn_lin = float(last["attention_uncached_block_ms"])
    cur_attn_quad = float(last["attention_uncached_per_every_block_ms"])
    cur_fwp = float(last["forward_pass_ms"])
    cur_repl_pct = float(last["replication_threshold_pct"])

    # CRITICAL: filter to only rows that share the last row's config.
    # A latency.csv accumulated across multiple bench runs will contain
    # rows written under different `[predictor]` / `[picker]` settings.
    # Running Fit(1) or the per-inflight diagnostic against a mixed-
    # config dataset produces meaningless "average across configs"
    # numbers. Only rows that match the current knobs on every dimension
    # the recommender uses can vote on what to change next.
    config_cols = [
        "base_ms",
        "queue_ms_per_entry",
        "attention_uncached_block_ms",
        "attention_uncached_per_every_block_ms",
        "cache_pressure_penalty_ms",
        "saturated_cold_ms_per_entry",
        "cache_pressure_saturated_threshold",
        "q_running_penalty_peak_ms",
        "q_running_penalty_peak_qr",
        "q_running_penalty_falloff_qr",
        "inflight_concurrent_capacity",
        "forward_pass_ms",
        "top_k_min",
        "top_k_fraction",
        "tie_tolerance_ms",
        "replication_threshold_pct",
        "replication_warm_target",
        "replication_warm_threshold_fraction",
    ]
    mask = pd.Series(True, index=ok.index)
    for col in config_cols:
        mask &= (ok[col] == last[col])
    filtered = ok[mask]
    total_rows = len(ok)
    curr_rows = len(filtered)
    if curr_rows < total_rows:
        print(
            f"  (filtering to the {curr_rows}/{total_rows} rows written under "
            "the current config; earlier rows used different knobs and would"
        )
        print("   confound the fit)")
        print()
        ok = filtered  # shadow — remainder of the function uses only current-config rows

    # Drop rows where the model produced an absurd prediction (typically
    # a runaway in-flight-discount stack). No real TTFT is >30s at the
    # workloads this code targets; those rows are model-output pathologies,
    # not observations of reality, and they'd dominate the mean + wreck
    # the Fit(1) LSQ. Log the count so the operator sees the drop.
    absurd_mask = ok["predicted_ms"] > 30000
    if absurd_mask.sum() > 0:
        n_dropped = int(absurd_mask.sum())
        max_pred = ok["predicted_ms"].max()
        print(
            f"  dropping {n_dropped} rows where the model predicted >30s "
            f"(max = {max_pred:.0f} ms) — almost certainly a runaway"
        )
        print(
            "   in-flight cost accumulation, not a real observation. Those "
            "rows shouldn't drive tuning."
        )
        print()
        ok = ok[~absurd_mask]

    # Use MEDIAN, not mean, as the primary bias signal. Even after the
    # absurd-prediction filter, residual distributions are asymmetric —
    # a few tail rows can drag the mean by hundreds of ms without
    # reflecting typical behavior. Median is what the priority ladder
    # keys off; mean is reported alongside as a divergence-detector.
    mean_resid = ok["residual_ms"].mean()
    median_resid = ok["residual_ms"].median()
    bias = median_resid  # <-- ladder decisions use this

    # Fit(1) inline — just the b0 + α·q + β·prefill + γ·cache subset,
    # skipping the q_running column (we don't need δ here).
    n = len(ok)
    if n < 20:
        advise(
            "LOW",
            f"only {n} rows at the current config after outlier filtering; "
            "can't compute a reliable Fit(1). Run a longer bench (128+ "
            "requests) at these settings before recommending a coefficient "
            "change.",
        )
        return
    y = ok["observed_ms"].astype(float).values
    ones = np.ones(n)
    X1 = np.column_stack([
        ones,
        ok["q_ms"].astype(float).values,
        ok["prefill_ms"].astype(float).values,
        ok["cache_ms"].astype(float).values,
    ])
    beta1, *_ = np.linalg.lstsq(X1, y, rcond=None)
    b0, alpha, beta_prefill, _gamma = (float(x) for x in beta1[:4])

    # Per-inflight over-charge (mirrors the diagnostic in report_regimes).
    in0 = ok[ok["regime_inflight"] == "in=0"]
    ingt0 = ok[ok["regime_inflight"] == "in>0"]
    per_inflight_bias = None
    if len(ingt0) >= 5:
        mean_qi = float(ingt0["q_inflight"].mean())
        if mean_qi >= 0.5:
            resid_gap = in0["residual_ms"].mean() - ingt0["residual_ms"].mean()
            per_inflight_bias = resid_gap / mean_qi

    # Print the framing before diving into the ladder. Show both the
    # median (which drives the ladder) and the mean (as a divergence-
    # detector — a big gap flags fat-tailed distributions where the
    # mean alone would mislead).
    print(f"  median residual : {median_resid:+.0f} ms   ← ladder keys off this")
    print(f"  mean residual   : {mean_resid:+.0f} ms")
    if abs(mean_resid - median_resid) > 500:
        print(f"    ↑ mean and median disagree by {abs(mean_resid - median_resid):.0f} ms — the residual distribution is fat-tailed;")
        print(f"      individual bad-luck rows (e.g., inflight-discount runaways) are dragging the mean.")
    print(f"  Fit(1) β on prefill : {beta_prefill:.3f}  (1.0 = calibrated)")
    print(f"  Fit(1) intercept    : {b0:.0f} ms  (current base_ms = {cur_base:.0f})")
    if per_inflight_bias is not None:
        print(f"  per-inflight bias   : {per_inflight_bias:+.0f} ms/inflight")
    print()
    print("  Priority ladder — take the top rung that applies, adjust, re-run 2-3×")
    print("  before revisiting. Do NOT change multiple knobs simultaneously.")
    print()

    # --- Rung A: prefill quadratic is miscalibrated ---
    if abs(beta_prefill - 1.0) > 0.15 and abs(bias) > 300:
        proj_lin = cur_attn_lin * beta_prefill
        proj_quad = cur_attn_quad * beta_prefill
        step = 0.75  # take 75% of the projection, not the whole thing
        step_lin = cur_attn_lin + (proj_lin - cur_attn_lin) * step
        step_quad = cur_attn_quad + (proj_quad - cur_attn_quad) * step
        direction = "down" if beta_prefill < 1.0 else "up"
        print(f"  [A] PREFILL QUADRATIC is off by {(beta_prefill - 1) * 100:+.0f}%.")
        print(f"      This is the biggest lever. Adjust `attention_uncached_*` "
              f"{direction}.")
        print()
        print(f"      Try (75% of the Fit(1) projection — conservative step):")
        print(f"        attention_uncached_block_ms          : "
              f"{cur_attn_lin:.3f} → {step_lin:.3f}")
        print(f"        attention_uncached_per_every_block_ms: "
              f"{cur_attn_quad:.5f} → {step_quad:.5f}")
        print()
        print(f"      Expected: median residual moves from {bias:+.0f} ms "
              f"toward zero by ~{abs(bias) * (1 - step):.0f}–"
              f"{abs(bias):.0f} ms.")
        print(f"      Verify: 2-3× cache-cleared bench replicas at the new coef; "
              f"expect p99 std ~40 ms.")
        return

    # --- Rung B: uniform intercept offset (base_ms) ---
    if abs(b0 - cur_base) > 150 and abs(bias) > 200:
        direction = "raise" if b0 > cur_base else "lower"
        print(f"  [B] BASE_MS is off by {b0 - cur_base:+.0f} ms.")
        print(f"      Fit(1) intercept = {b0:.0f}, current base_ms = {cur_base:.0f}.")
        print(f"      Prefill and workload cost are now calibrated (β ≈ 1); the "
              f"remaining bias is a uniform floor.")
        print()
        print(f"      Try:")
        print(f"        base_ms: {cur_base:.0f} → {b0:.0f}  (or a half-step of "
              f"{(cur_base + b0) / 2:.0f} as a conservative first cut)")
        print()
        print(f"      Effect on routing:")
        print(f"        - Worker-to-worker ranking is UNCHANGED (uniform offset).")
        print(f"        - Replication-threshold percent check IS AFFECTED "
              f"(same absolute gap becomes a larger % of a smaller "
              f"warm_predicted → fewer replication swaps fire).")
        print(f"      Caveat from prior experiments: base_ms drops sometimes "
              f"coincide with wider p99 variance. Confirm with 3-4 replicas.")
        return

    # --- Rung C: in-flight discount over-charging (structural) ---
    if per_inflight_bias is not None and per_inflight_bias > 200:
        print(f"  [C] IN-FLIGHT DISCOUNT over-charges by "
              f"~{per_inflight_bias:.0f} ms per fresh inflight.")
        print(f"      This is STRUCTURAL, not a coefficient tune. The current "
              f"`(1 - progress)` discount charges nearly-full prefill for a")
        print(f"      fresh inflight; observations suggest vLLM pipelines "
              f"prefill more aggressively than the model assumes.")
        print()
        print(f"      Options (both require code changes):")
        print(f"        1. Reintroduce a ceiling < 1.0 in inflight_discount() "
              f"— empirical ceiling around 0.3–0.5 based on the observed gap.")
        print(f"        2. Switch inflight cost from `discount × prefill_ms_for(…)` "
              f"to a flat per-inflight constant (~{per_inflight_bias:.0f} ms)")
        print(f"           — matches the observed physics but is a bigger refactor.")
        print()
        print(f"      DO NOT try to compensate for this via coefficient nudges "
              f"— it will re-introduce prefill miscalibration.")
        return

    # --- Rung D: residuals are centered — turn to routing ---
    if abs(bias) < 200 and abs(beta_prefill - 1.0) < 0.15:
        print(f"  [D] RESIDUALS ARE CENTERED (|median| = {abs(bias):.0f} ms, "
              f"β = {beta_prefill:.2f}).")
        print(f"      Predictor is calibrated enough to reason about routing. "
              f"Move to the routing tier:")
        print()
        if cur_fwp < 5:
            print(f"        - forward_pass_ms: currently {cur_fwp:.1f}. Try 20-30 "
                  f"to price decode contention explicitly.")
        else:
            print(f"        - forward_pass_ms: currently {cur_fwp:.1f}. Sweep in "
                  f"section IV shows the residual-optimal value; adopt that if "
                  f"it's outside {cur_fwp:.0f} ± 5.")
        print(f"        - replication_threshold_pct: currently {cur_repl_pct:.0f}. "
              f"Dial DOWN to reduce spurious cold-worker swaps under bursts, "
              f"UP to warm cache more aggressively.")
        print(f"        - top_k_fraction: currently "
              f"{last['top_k_fraction']:.2f}. Raise from 0 to spread bursts "
              f"across the top-K workers (trades cache hit rate for tail "
              f"latency).")
        print()
        print(f"      Verify routing changes THE SAME WAY: one knob, re-run "
              f"multiple times, confirm the effect is larger than the noise "
              f"envelope.")
        return

    # --- Fallthrough: modest bias, no dominant lever ---
    print(f"  Median residual is modest ({bias:+.0f} ms) and no single lever "
          f"stands out.")
    print(f"    - Fit(1) β = {beta_prefill:.3f} (target 1.0, within ±15%)")
    print(f"    - Intercept offset = {b0 - cur_base:+.0f} ms (target ±150)")
    if per_inflight_bias is not None:
        print(f"    - Per-inflight bias = {per_inflight_bias:+.0f} ms/inflight "
              f"(target ±200)")
    print()
    print(f"  Options:")
    print(f"    - Run 2-3 more cache-cleared replicas to reduce noise on the")
    print(f"      current diagnostics before making a call.")
    print(f"    - Inspect the per-worker bias table (Section II) — a single-")
    print(f"      worker straggler can dominate p99 without moving the mean.")


# ============================================================================
# Optional plotting (lazy matplotlib)
# ============================================================================

def _try_import_matplotlib():
    """Return matplotlib.pyplot or None if unavailable."""
    try:
        import matplotlib

        matplotlib.use("Agg")  # headless
        import matplotlib.pyplot as plt

        return plt
    except ImportError:
        return None


def write_plots(df: pd.DataFrame, ok: pd.DataFrame, out_dir: str,
                alpha_grid: int, weight_grid: int) -> None:
    plt = _try_import_matplotlib()
    if plt is None:
        print(
            "warning: matplotlib not available; --plot is a no-op",
            file=sys.stderr,
        )
        return

    os.makedirs(out_dir, exist_ok=True)

    # Residual histogram.
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(ok["residual_ms"].clip(-10000, 10000), bins=60)
    ax.set_xlabel("residual_ms (observed - predicted, clipped to ±10s)")
    ax.set_ylabel("count")
    ax.set_title("Residual distribution")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "residual_hist.png"), dpi=100)
    plt.close(fig)

    # Residual vs each input.
    inputs = [
        "q_waiting", "q_inflight", "uncached_blocks", "overlap_blocks",
        "kv_cache_usage", "compl_per_s", "q_workload",
    ]
    for col in inputs:
        if col not in ok.columns or ok[col].std() == 0:
            continue
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.scatter(ok[col], ok["residual_ms"], alpha=0.3, s=10)
        ax.axhline(0, color="gray", linewidth=0.7)
        ax.set_xlabel(col)
        ax.set_ylabel("residual_ms")
        ax.set_title(f"residual vs {col}")
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"residual_vs_{col}.png"), dpi=100)
        plt.close(fig)

    # Alpha sweep.
    sub = ok[ok["inflight_active"]]
    if len(sub) >= 10:
        alphas = np.linspace(0.0, 1.0, alpha_grid)
        mae = np.array([
            replay_alpha(sub, float(a))["residual_ms"].abs().mean()
            for a in alphas
        ])
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.plot(alphas, mae, marker="o")
        cur_a = float(ok["expected_overlap_alpha"].mode().iloc[0])
        ax.axvline(cur_a, color="red", linestyle="--", label=f"current α={cur_a:.2f}")
        ax.set_xlabel("α (expected_overlap_alpha)")
        ax.set_ylabel("mean |residual_ms|")
        ax.set_title("α sweep")
        ax.legend()
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "alpha_sweep.png"), dpi=100)
        plt.close(fig)

    # Weight sweep — only meaningful when the legacy
    # queue_workload_weight column actually varied. Current-schema
    # CSVs have it pinned at 1.0 so the sweep is a flat line.
    has_weight_signal = not (
        ok["queue_workload_weight"].nunique() == 1
        and ok["queue_workload_weight"].iloc[0] == 1.0
    )
    if has_weight_signal:
        weights = np.linspace(0.0, 1.0, weight_grid)
        mae = np.array([
            replay_weight(ok, float(w))["residual_ms"].abs().mean()
            for w in weights
        ])
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.plot(weights, mae, marker="o")
        cur_w = float(ok["queue_workload_weight"].mode().iloc[0])
        ax.axvline(cur_w, color="red", linestyle="--", label=f"current w={cur_w:.2f}")
        ax.set_xlabel("queue_workload_weight")
        ax.set_ylabel("mean |residual_ms|")
        ax.set_title("Weight sweep")
        ax.legend()
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "weight_sweep.png"), dpi=100)
        plt.close(fig)

    # Joint (α, w) heatmap — same gate as the weight sweep.
    if has_weight_signal and len(sub) >= 10:
        alphas = np.linspace(0.0, 1.0, alpha_grid)
        weights = np.linspace(0.0, 1.0, weight_grid)
        grid = np.zeros((len(weights), len(alphas)))
        for i, a in enumerate(alphas):
            sub_a = replay_alpha(sub, float(a))
            for j, w in enumerate(weights):
                grid[j, i] = replay_weight(sub_a, float(w))["residual_ms"].abs().mean()
        fig, ax = plt.subplots(figsize=(7, 5))
        im = ax.imshow(grid, origin="lower", aspect="auto",
                       extent=(0, 1, 0, 1), cmap="viridis")
        fig.colorbar(im, ax=ax, label="mean |residual_ms|")
        ax.set_xlabel("α")
        ax.set_ylabel("queue_workload_weight")
        ax.set_title("Joint (α, w) sweep")
        # Mark current and optimum.
        ax.scatter([cur_a], [cur_w], color="red", marker="x", s=80, label="current")
        bj, bi = np.unravel_index(int(np.argmin(grid)), grid.shape)
        ax.scatter([alphas[bi]], [weights[bj]], color="white", marker="*",
                   s=120, edgecolor="black", label="optimum")
        ax.legend()
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "alpha_weight_joint.png"), dpi=100)
        plt.close(fig)

    # Per-endpoint residual boxplot.
    if ok["name"].nunique() > 1:
        groups = [g["residual_ms"].values for _, g in ok.groupby("name")]
        names = sorted(ok["name"].unique())
        fig, ax = plt.subplots(figsize=(max(8, len(names)), 5))
        ax.boxplot(groups, labels=names, showfliers=False)
        ax.set_ylabel("residual_ms")
        ax.set_title("Per-endpoint residual distribution")
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "per_endpoint_residual.png"), dpi=100)
        plt.close(fig)

    print(f"plots written to {out_dir}/", file=sys.stderr)


# ============================================================================
# CLI
# ============================================================================

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Analyze picker latency CSV; replay alternative tuning choices.",
    )
    ap.add_argument("csv", help="path to latency.csv (42-column format; 38/39-col legacy accepted)")
    ap.add_argument(
        "--top-n", type=int, default=5,
        help="how many rows to show in outlier/concurrency/error sections (default 5)",
    )
    ap.add_argument(
        "--alpha-grid", type=int, default=21,
        help="number of α values to sweep in [0, 1] (default 21)",
    )
    ap.add_argument(
        "--weight-grid", type=int, default=21,
        help="number of w values to sweep in [0, 1] (default 21)",
    )
    ap.add_argument(
        "--plot", metavar="DIR", default=None,
        help="write diagnostic PNGs to DIR (requires matplotlib; "
             "skipped with a warning if not installed)",
    )
    args = ap.parse_args()

    df = load_csv(args.csv)

    # I. Sanity & shape
    report_shape(df)
    report_errors(df, args.top_n)

    ok = df[df["status"] == 200].copy()
    if len(ok) == 0:
        print("\nno successful rows — nothing to analyze further")
        return 1

    report_current_config(ok)
    report_sanity(ok)
    report_residuals(ok)

    # II. Per-input attribution
    report_per_worker(ok)
    report_per_component(ok)
    report_correlations(ok)

    # III. Regime detection
    report_regimes(ok)
    report_cold_saturation(ok)
    report_q_running(ok)
    report_replication(ok)

    # IV. Replay
    section_iv_preamble()
    report_alpha_sweep(ok, args.alpha_grid)
    report_weight_sweep(ok, args.weight_grid)
    report_alpha_weight_joint(ok, args.alpha_grid, args.weight_grid)
    report_q_running_sweep(ok)
    report_forward_pass_sweep(ok)
    report_coefficient_refit(ok)

    # V. Asymmetry detection
    report_inflight_overlap_failures(ok)
    report_workload_overcharge(ok)

    # VI. Operational
    report_outliers(ok, args.top_n)
    report_herding(ok)
    report_concurrency(df, args.top_n)

    # VII. The bottom-line recommendation. Reads all the sections above
    # and picks the ONE next knob to tune, backed by residual math.
    report_tuning_recommendation(ok)

    # Optional plots
    if args.plot:
        write_plots(df, ok, args.plot, args.alpha_grid, args.weight_grid)

    print()
    print("=" * 76)
    print("HOW TO USE THIS REPORT")
    print("=" * 76)
    print(
        "  - Sections I-III + V-VI are DIAGNOSTIC: read them as 'is anything"
    )
    print(
        "    obviously broken?' — per-worker bias, herding, residual signs,"
    )
    print(
        "    outliers. ANOMALY-level findings here are usually worth acting on."
    )
    print()
    print(
        "  - Section IV (Replay) is EXPLORATORY: the sweeps and refits show"
    )
    print(
        "    what a single-knob change would do to mean |resid| on THIS"
    )
    print(
        "    dataset's routing distribution. Don't autopilot off these — see"
    )
    print(
        "    the banner above section IV for why."
    )
    print()
    print(
        "  - Section VII ('Next single-knob change') is the ACTIONABLE ONE."
    )
    print(
        "    It picks the single highest-impact parameter to change next,"
    )
    print(
        "    following a priority ladder: prefill quadratic → base_ms →"
    )
    print(
        "    in-flight discount → routing tier. Change ONE knob, re-run 2-3×"
    )
    print(
        "    with cache cleared, then re-run this analyzer."
    )
    print()
    print(
        "  - When in doubt: run the bench multiple times with the same config"
    )
    print(
        "    first, to learn how much the geomean swings just from noise."
    )
    print(
        "    Any single-run improvement smaller than that swing is not real."
    )
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
