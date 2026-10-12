#!/usr/bin/env bash
# Smoke assertions against a cluster prepared by `just up` + the manifests in
# tests/e2e/manifests (kustomize). The worker is the REAL dummy-vllm — vLLM
# on CPU with dummy weights — so this proves the actual engine contract:
# real /metrics, real /v1/models, real ZmqEventPublisher KV events. Blocks
# are only stored on prefill, so the script sends one real completion to
# trigger events. Every poll is bounded so a failure diagnoses, not hangs.
set -euo pipefail

CTX="${KIND_CONTEXT:-kind-kinference-smoke}"
K="kubectl --context $CTX"

poll() { # poll <seconds> <description> <command...>
  local deadline=$(( $(date +%s) + $1 )); shift
  local what=$1; shift
  until "$@" >/dev/null 2>&1; do
    if (( $(date +%s) > deadline )); then
      echo "✗ timed out waiting for: $what" >&2
      return 1
    fi
    sleep 2
  done
  echo "✓ $what"
}

# Generous: first run pulls the ~600MB CPU image and vLLM fetches the model
# config/tokenizer from HF Hub before serving. `rollout status` (not `wait
# --for=condition=available`) so this tracks the CURRENT generation and never
# returns on a pod that is about to be replaced.
$K rollout status deployment/dummy-vllm --timeout=420s

$K port-forward svc/kinference 19001:9001 >/dev/null 2>&1 &
PF1=$!
$K port-forward svc/dummy-vllm 18000:8000 >/dev/null 2>&1 &
PF2=$!
trap 'kill $PF1 $PF2 2>/dev/null || true' EXIT
poll 30 "kinference port-forward up" curl -sf localhost:19001/readyz
poll 60 "dummy-vllm serving" bash -c 'curl -sf localhost:18000/v1/models | grep -q SmolLM2'

poll 120 "worker discovered (readyz reports a ready endpoint)" \
  bash -c 'curl -s localhost:19001/readyz | grep -q "with ready endpoints: 1"'

# Trigger a prefill so vLLM publishes BlockStored events. Dummy weights →
# garbage text, but the KV cache and event stream are real. The prompt must
# be LONG: vLLM only stores (and thus publishes) FULL blocks, and the CPU
# backend's block size is 128 tokens — a short prompt produces zero events.
# ~60 repetitions ≈ 900 tokens ≈ 7 full blocks.
echo "sending one long completion to trigger KV events..."
PROMPT=$(printf 'The quick brown fox jumps over the lazy dog near the riverbank at dawn. %.0s' {1..60})
curl -sf localhost:18000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d "{\"model\":\"HuggingFaceTB/SmolLM2-135M-Instruct\",\"max_tokens\":8,
       \"messages\":[{\"role\":\"user\",\"content\":\"$PROMPT\"}]}" \
  > /dev/null
echo "✓ completion request served"

# The index gauge refreshes on a throttled ~10s radix-tree dump.
poll 120 "KV events ingested (kinference_index_blocks > 0)" \
  bash -c 'curl -s localhost:19001/metrics | grep -E "^kinference_index_blocks [1-9]"'

ACCEPTED=$($K get inferenceworkersets.kinference.buoyant.io dummy \
  -o jsonpath='{.status.conditions[?(@.type=="Accepted")].status}')
[[ "$ACCEPTED" == "True" ]] || { echo "✗ workerset dummy not Accepted: '$ACCEPTED'" >&2; exit 1; }
echo "✓ workerset Accepted=True"

echo "smoke: all checks passed"
