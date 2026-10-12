#!/usr/bin/env bash
# bench-suite.sh — archetype × variant benchmark suite for a single vLLM instance.
#
# Runs the full archetype set against a single endpoint:
#
#   chat              — multi-turn 1024 sys + 128 user + 256 asst, 5 turns.
#                       SLO: TTFT p95 < 2500ms, ITL p95 < 150ms.
#   agent             — multi-turn 32K sys + 18K user + 512 asst, 5 turns.
#                       Async; TPOT-anchored SLO. Turn 5 cumulative ≈ 127K,
#                       so it fits within a 128K max_model_len.
#   batch             — single-shot 120K input, 2K output. cache_hit=0.
#                       Batch/offline analysis (doc summarization, contract
#                       review). No no_cache_aware_variant (every request is
#                       a unique 120K prompt; cache reuse impossible). SLO
#                       is E2E latency (prefill dominates wall time here).
#                       Sized to fit inside a 128K-context deployment with
#                       headroom for chat-template overhead.
#   reasoning         — multi-turn 32K sys + 7168 user + 128 asst, 5 turns.
#                       Turn-5 KV depth (~69K) matches a real 32K sys +
#                       1K user + 8K asst reasoning session; output is
#                       truncated to 128 tokens to keep wall time short.
#                       TPOT + generous TTFT SLO.
#
# Idempotent: skips output files that already exist. Ctrl-C, edit, resume.
# Early-abort: bench aborts a run once error rate ≥ 20% (via --max-error-rate).
#
# Usage:
#   ./scripts/bench-suite.sh <base-url> <model> [output-dir] [archetype[,archetype...]]
#
#   base-url    : vLLM endpoint (no path, e.g. http://vllm.example.com:8000)
#   model       : model name as vLLM knows it (e.g. google/gemma-4-12B-it)
#   output-dir  : directory for JSON outputs (default: ./bench-suite-<timestamp>)
#   archetypes  : comma-separated subset — chat,agent,batch,reasoning.
#                 Default: all four.
#
# Example:
#   ./scripts/bench-suite.sh \
#     http://k8s-default-workerro-...elb.amazonaws.com:8000 \
#     google/gemma-4-12B-it \
#     ./bench-gemma-g6e
#
# Wall-time estimate: ~2-4 hours for the full suite (agent and reasoning
# dominate). Individual archetypes take 15-60 minutes depending on shape.

set -euo pipefail

# --- args ---
BASE_URL="${1:-}"
MODEL="${2:-}"
OUT_DIR="${3:-./bench-suite-$(date +%Y%m%d-%H%M%S)}"
ARCHETYPES="${4:-chat,agent,batch,reasoning}"

if [[ -z "$BASE_URL" || -z "$MODEL" ]]; then
  head -40 "$0" | sed -n '/^# Usage/,/^$/p' | sed 's/^# \?//'
  exit 1
fi

# --- setup ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH_BIN="${REPO_DIR}/bench"

echo "==> Building bench binary..."
(cd "$REPO_DIR" && go build -o "$BENCH_BIN" ./cmd/bench)

mkdir -p "$OUT_DIR"
echo "==> Output dir: $OUT_DIR"
echo "==> Endpoint:   $BASE_URL"
echo "==> Model:      $MODEL"
echo "==> Archetypes: $ARCHETYPES"
echo

# Minimum successful samples per operating-point file. Sized so that the
# p95 latency estimator has a 95% CI of ~±12 order statistics — usable
# for engineering-grade regression detection. Both sweep helpers below
# scale mt-conversations / total-requests up to guarantee this floor
# regardless of concurrency.
MIN_SAMPLES=200

# --- helpers ---

# Runs a multiturn concurrency sweep. Each concurrency level = C parallel
# conversation slots draining a pool of mt_convs conversations, each
# running mt_turns sequentially. mt_convs is chosen so total samples
# (mt_convs * mt_turns) ≥ MIN_SAMPLES. Skips existing outputs.
run_multiturn_sweep() {
  local name=$1
  local streams=$2         # space-separated list, e.g. "8 16 32 64"
  local sys_tokens=$3
  local user_tokens=$4
  local asst_tokens=$5
  local mt_turns=$6
  local timeout=$7

  echo "--- multiturn sweep: $name (sys=$sys_tokens, user=$user_tokens, asst=$asst_tokens, turns=$mt_turns, streams=[$streams], min-samples=$MIN_SAMPLES) ---"
  for C in $streams; do
    local out_file="${OUT_DIR}/bench-${name}-c${C}.json"
    if [[ -f "$out_file" ]]; then
      printf "  [skip] C=%-4d  (file exists)\n" "$C"
      continue
    fi
    # Conversations = ceil(MIN_SAMPLES / mt_turns), but not below C so the
    # queue depth actually reaches the concurrency level.
    local min_convs=$(( (MIN_SAMPLES + mt_turns - 1) / mt_turns ))
    local mt_convs=$C
    if (( min_convs > mt_convs )); then mt_convs=$min_convs; fi
    local samples=$(( mt_convs * mt_turns ))
    printf "  [run]  C=%-4d  convs=%-4d  samples=%-4d  " "$C" "$mt_convs" "$samples"
    local start=$SECONDS
    "$BENCH_BIN" \
      --base-url "$BASE_URL" \
      --model "$MODEL" \
      --workload multiturn \
      --mt-conversations "$mt_convs" \
      --mt-turns "$mt_turns" \
      --mt-system-tokens "$sys_tokens" \
      --mt-user-tokens "$user_tokens" \
      --mt-assistant-tokens "$asst_tokens" \
      --concurrency "$C" \
      --warmup-requests 5 \
      --timeout "$timeout" \
      --max-error-rate 0.20 \
      --min-checks-for-rate 20 \
      --output "$out_file" \
      > "${out_file%.json}.log" 2>&1 || true
    local dur=$((SECONDS - start))
    local rps err
    rps=$(jq -r '(.requests_per_sec // 0) * 100 | round / 100' "$out_file" 2>/dev/null || echo "?")
    err=$(jq -r '.failed_requests // 0' "$out_file" 2>/dev/null || echo "?")
    printf "%4ds  rps=%s  err=%s\n" "$dur" "$rps" "$err"
  done
  echo
}

# Runs a synthetic-workload sweep (unique prefix per request; no cache
# reuse). Total requests per operating-point file = max(min_samples,
# C * reqs_per_stream). Callers can pass an explicit min_samples floor
# (7th arg) to override the global MIN_SAMPLES — used for very slow
# archetypes (batch) where 200 samples per file is impractical and a
# looser p95 CI is accepted in exchange.
run_synthetic_sweep() {
  local name=$1
  local streams=$2
  local tail_tokens=$3
  local max_output=$4
  local timeout=$5
  local reqs_per_stream=$6
  local min_samples=${7:-$MIN_SAMPLES}

  echo "--- synthetic sweep: $name (tail=$tail_tokens, out=$max_output, streams=[$streams], min-samples=$min_samples) ---"
  for C in $streams; do
    local out_file="${OUT_DIR}/bench-${name}-c${C}.json"
    if [[ -f "$out_file" ]]; then
      printf "  [skip] C=%-4d  (file exists)\n" "$C"
      continue
    fi
    local total_reqs=$(( C * reqs_per_stream ))
    if (( min_samples > total_reqs )); then total_reqs=$min_samples; fi
    printf "  [run]  C=%-4d  reqs=%-4d  " "$C" "$total_reqs"
    local start=$SECONDS
    # --prefix-count 10000 with --prefix-tokens 1 gives an effectively unique
    # 1-token "prefix" per request drawn from a large pool → cache never warms.
    "$BENCH_BIN" \
      --base-url "$BASE_URL" \
      --model "$MODEL" \
      --workload synthetic \
      --prefix-count 10000 \
      --prefix-tokens 1 \
      --tail-tokens "$tail_tokens" \
      --max-output-tokens "$max_output" \
      --total-requests "$total_reqs" \
      --concurrency "$C" \
      --warmup-requests 2 \
      --timeout "$timeout" \
      --max-error-rate 0.20 \
      --min-checks-for-rate 5 \
      --output "$out_file" \
      > "${out_file%.json}.log" 2>&1 || true
    local dur=$((SECONDS - start))
    local rps err
    rps=$(jq -r '(.requests_per_sec // 0) * 100 | round / 100' "$out_file" 2>/dev/null || echo "?")
    err=$(jq -r '.failed_requests // 0' "$out_file" 2>/dev/null || echo "?")
    printf "%4ds  rps=%s  err=%s\n" "$dur" "$rps" "$err"
  done
  echo
}

want() { [[ ",$ARCHETYPES," == *",$1,"* ]]; }

# --- archetype definitions ---

# chat: 1024 system + 128 user + 256 assistant, 5 turns.
# SLO: TTFT p95 < 2500ms AND ITL p95 < 150ms — the SLO filter picks the
# highest-rps operating point that satisfies both conditions.
if want chat; then
  run_multiturn_sweep "chat" \
    "1 2 4 8 16 32 64 128 256" \
    1024 128 256 5 60s
fi

# agent (async): 32K system + 18K user/turn + 512 assistant/turn, 5 turns.
# Non-streaming; TPOT p99 < 100ms is the target SLO.
# Turn 5 cumulative context ≈ 127K, fitting a 128K max_model_len with
# minimal chat-template headroom. Concurrency stays low.
if want agent; then
  run_multiturn_sweep "agent" \
    "1 2 4 8" \
    32768 18432 512 5 300s
fi

# batch: single-shot 120K input, 2K output. cache_hit=0 by design.
# No no_cache_aware_variant needed — workload is inherently non-cache-aware.
# Batch/offline: prefill dominates E2E, so the SLO is E2E-anchored
# (see populate.py ARCH_SLOS). Very extreme KV pressure per request;
# concurrency very low. Sized at 120K (not 128K) so the shape fits a
# 128K-context-window deployment once chat-template overhead is added.
# Uses a 50-sample floor (vs the global 200) because each request takes
# 30-60s+ — 200 samples at C=1 would take an hour. p95 CI is looser
# (ranks 47-50, effectively "max of 50") but batch's SLO is a coarse
# 120s threshold so the looser CI is acceptable.
if want batch; then
  run_synthetic_sweep "batch" \
    "1 2 4" \
    122880 2048 600s 3 50
fi

# reasoning: 32K system + 7168 user/turn + 128 assistant/turn, 5 turns.
# Cumulative context reaches ~69K by turn 5 — same KV depth as a "real"
# 32K sys + 1K user + 8K asst reasoning session, but with generation
# truncated to 128 tokens so wall time is bounded. TPOT / TTFT under
# this measurement are within a few percent of the full-8K-output case
# (attention memory bandwidth dominates), but RPS is inflated because
# each request finishes ~64× faster than a real 8K-output reasoning
# turn — populate.py notes this and the widget's workload_params still
# report output_tokens=8192 so downstream token-volume math is correct.
if want reasoning; then
  # Bumped user tokens keep KV depth realistic while output stays short.
  run_multiturn_sweep "reasoning" \
    "1 2 4 8" \
    32768 7168 128 5 120s
fi

# --- summary ---

echo "==> Summary — SLO-respecting knees per archetype:"
echo

# Filter operating points by a JSON-path metric ≤ threshold, plus
# error_rate ≤ (1 - percentile). For a p95 SLO we allow up to 5% errors:
# errors are p95-violations, so as long as the error rate stays under
# the SLO's allowed miss rate AND the reported p95 latency (over
# successful requests) is ≤ threshold, the run is p95-passing overall.
# The MAX_ERR_RATE below assumes p95 SLOs across the suite (which is
# the current convention).
# Multi-condition SLOs are evaluated as AND across all conditions.
# Args: <name> [<metric_path>:<threshold_ms>] ...
MAX_ERR_RATE="0.05"
summarize_multi() {
  local name=$1
  shift
  local conditions=("$@")

  local files=("$OUT_DIR/bench-${name}-c"*.json)
  [[ -e "${files[0]}" ]] || { printf "  %-24s: no files\n" "$name"; return; }

  # Track (a) the SLO-respecting knee = max-rps point that satisfies all
  # conditions with errors=0, and (b) the best (minimum) observed value per
  # metric across all files, for informative output when the SLO isn't met.
  # Best-value tracking is split by error state so we can distinguish
  # "SLO fails because latency exceeded threshold" from "SLO fails because
  # every file with good latency also had errors."
  local best_c="" best_rps=0
  local -a best_vals best_streams best_errs      # min across ALL files
  local -a best_clean_vals best_clean_streams   # min across error-free files only
  local i=0
  for cond in "${conditions[@]}"; do
    best_vals[$i]=""
    best_streams[$i]=""
    best_errs[$i]=""
    best_clean_vals[$i]=""
    best_clean_streams[$i]=""
    i=$((i+1))
  done
  local any_error_free=0

  for f in "${files[@]}"; do
    local c err rps total err_rate within_budget
    c=$(basename "$f" .json | sed 's/.*-c//')
    err=$(jq -r '.failed_requests // 0' "$f" 2>/dev/null || echo 0)
    total=$(jq -r '.total_requests // 0' "$f" 2>/dev/null || echo 0)
    rps=$(jq -r '.requests_per_sec // 0' "$f" 2>/dev/null || echo 0)
    if [[ "$total" == "0" ]]; then
      err_rate="1"
    else
      err_rate=$(awk "BEGIN {printf \"%.4f\", $err / $total}")
    fi
    if awk "BEGIN {exit !($err_rate <= $MAX_ERR_RATE)}"; then
      within_budget=1
      any_error_free=1
    else
      within_budget=0
    fi

    # Track best-observed value for each metric across all files (with
    # or without errors) AND across files within the error budget only.
    i=0
    for cond in "${conditions[@]}"; do
      local path="${cond%:*}"
      local val
      val=$(jq -r ".${path} // 0 | . * 10 | round / 10" "$f" 2>/dev/null || echo 0)
      if awk "BEGIN {exit !($val > 0)}"; then
        if [[ -z "${best_vals[$i]}" ]] || awk "BEGIN {exit !($val < ${best_vals[$i]})}"; then
          best_vals[$i]=$val
          best_streams[$i]=$c
          best_errs[$i]=$err
        fi
        if [[ "$within_budget" == "1" ]]; then
          if [[ -z "${best_clean_vals[$i]}" ]] || awk "BEGIN {exit !($val < ${best_clean_vals[$i]})}"; then
            best_clean_vals[$i]=$val
            best_clean_streams[$i]=$c
          fi
        fi
      fi
      i=$((i+1))
    done

    # SLO-respecting knee: error_rate within budget AND all conditions hold.
    [[ "$within_budget" != "1" ]] && continue
    local ok=1
    for cond in "${conditions[@]}"; do
      local path="${cond%:*}"
      local threshold="${cond#*:}"
      local val
      val=$(jq -r ".${path} // 0 | . * 10 | round / 10" "$f" 2>/dev/null || echo 0)
      if ! awk "BEGIN {exit !($val <= $threshold)}"; then
        ok=0
        break
      fi
    done
    [[ "$ok" != "1" ]] && continue

    if awk "BEGIN {exit !($rps > $best_rps)}"; then
      best_rps=$rps
      best_c=$c
    fi
  done

  # Build "best: metric=val@C=N …" string. When an SLO-respecting point
  # exists we show error-free bests; otherwise we show absolute bests
  # and append the error count from the file each best came from — so
  # a user can see whether the SLO failed on latency or on errors.
  local best_str="" show_errors=0
  if [[ -z "$best_c" && "$any_error_free" == "0" ]]; then
    show_errors=1
  fi
  i=0
  for cond in "${conditions[@]}"; do
    local path="${cond%:*}"
    local v s e
    if [[ -n "$best_c" ]]; then
      # SLO-respecting knee found: prefer error-free best
      v="${best_clean_vals[$i]}"
      s="${best_clean_streams[$i]}"
      e=""
    else
      v="${best_vals[$i]}"
      s="${best_streams[$i]}"
      e="${best_errs[$i]}"
    fi
    if [[ -n "$v" ]]; then
      local frag="${path}=${v}@C=${s}"
      if [[ "$show_errors" == "1" && -n "$e" && "$e" != "0" ]]; then
        frag="${frag}[${e}err]"
      fi
      best_str="${best_str} ${frag}"
    else
      best_str="${best_str} ${path}=n/a"
    fi
    i=$((i+1))
  done
  best_str="${best_str# }"

  if [[ -z "$best_c" ]]; then
    if [[ "$any_error_free" == "0" ]]; then
      printf "  %-24s: no SLO-respecting point — every C level exceeded %.0f%% error rate (best observed: %s)\n" "$name" "$(awk "BEGIN {print $MAX_ERR_RATE * 100}")" "$best_str"
    else
      printf "  %-24s: no SLO-respecting point (best observed: %s)\n" "$name" "$best_str"
    fi
  else
    printf "  %-24s: streams=%-4s rps=%.3f (best observed: %s)\n" "$name" "$best_c" "$best_rps" "$best_str"
  fi
}

if want chat; then
  echo "  chat SLO (TTFT p95 < 2500ms, ITL p95 < 150ms):"
  summarize_multi "chat"       "ttft.p95_ms:2500" "itl.p95_ms:150"
fi

if want agent; then
  echo "  agent SLO (TPOT p95 < 100ms):"
  summarize_multi "agent"      "tpot.p95_ms:100"
fi

if want batch; then
  echo "  batch SLO (E2E p95 < 120000ms):"
  summarize_multi "batch"      "e2e.p95_ms:120000"
fi

if want reasoning; then
  echo "  reasoning SLO (TPOT p95 < 50ms, TTFT p95 < 30000ms):"
  summarize_multi "reasoning"  "tpot.p95_ms:50" "ttft.p95_ms:30000"
fi

echo
echo "==> Done. Output: $OUT_DIR"
echo "    Next: extract per-archetype operating_points from the JSON files and"
echo "    populate /Users/william/devel/inference-d-website/static/data/models.json"
