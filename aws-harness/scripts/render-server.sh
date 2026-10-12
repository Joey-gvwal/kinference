#!/usr/bin/env bash
# render-server.sh — install/upgrade or delete one inference server instance.
#
# Reads a per-server TOML config and runs `helm upgrade --install` (or
# `helm uninstall`) against the chart for the configured engine. One TOML
# = one Helm release = one StatefulSet (with N pods, where N = replicas).
#
# Replicas are managed by the StatefulSet itself: bumping `replicas = N`
# in the TOML and re-running `server-up` is just `helm upgrade --set
# replicas=N` — K8s adds or terminates pods in order. Pods are named
# `<release>-0`, `<release>-1`, …, `<release>-(N-1)`.
#
# Supported engines: vllm, sglang.
#
# Dependencies: yq >= 4 (TOML support), jq, helm, kubectl.
#
# Usage:
#   ACTION=up   bash scripts/render-server.sh servers/qwen-vllm.toml
#   ACTION=down bash scripts/render-server.sh servers/qwen-vllm.toml

set -euo pipefail

CFG="${1:-}"
ACTION="${ACTION:-up}"
NAMESPACE="${NAMESPACE:-default}"

if [ -z "${CFG}" ] || [ ! -f "${CFG}" ]; then
  echo "ERROR: pass a TOML config file as the first argument (got: '${CFG}')" >&2
  exit 1
fi

for tool in yq jq helm kubectl; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "ERROR: ${tool} not found on PATH" >&2
    exit 1
  fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Parse the TOML once to JSON, then pull fields with jq.
CFG_JSON="$(yq -p toml -o json "${CFG}")"

field() {
  echo "${CFG_JSON}" | jq -r "${1} // empty"
}

NAME="$(field .name)"
ENGINE="$(field .engine)"
IMAGE="$(field .image)"
MODEL="$(field .model_name)"
PREFIX="$(field .weights_s3_prefix)"
GPU_COUNT="$(field .gpu_count)"
BLOCK_SIZE="$(field .block_size)"
# node_pool: which GPU node group this deployment must land on. Matches the
# `gpu-pool` label applied by terraform/eks.tf ("l40s", "h200"). Empty →
# no additional nodeSelector (schedules anywhere with role=gpu). Explicit
# opt-in avoids stranding a small pod on a $60/hr H200 node.
NODE_POOL="$(field .node_pool)"

# Optional per-pod resource overrides. Chart defaults (values.yaml) are
# sized for a 1-GPU L40S deployment; larger models on 8-GPU nodes (e.g.
# GLM-5.2 on p5e.48xlarge) need much more host CPU/RAM for tokenization,
# KV metadata, and activation staging. Unset → chart default.
CPU_REQUEST="$(field .cpu_request)"
CPU_LIMIT="$(field .cpu_limit)"
MEMORY_REQUEST="$(field .memory_request)"
MEMORY_LIMIT="$(field .memory_limit)"
# Resolve replicas in priority order: env-var override (deploy-time) wins,
# then the TOML's `replicas` field, then 1. So:
#   make server-up CFG=foo.toml                  → TOML's replicas, or 1
#   make server-up REPLICAS=3 CFG=foo.toml       → 3 (ignores TOML)
# Deploy-time override is a transient deviation — the TOML stays the
# source of truth; the next server-up without REPLICAS=... reverts.
REPLICAS="${REPLICAS:-$(field .replicas)}"
REPLICAS="${REPLICAS:-1}"

if [ -z "${NAME}" ]; then
  echo "ERROR: ${CFG}: 'name' is required" >&2
  exit 1
fi

case "${REPLICAS}" in
  ''|*[!0-9]*)
    echo "ERROR: ${CFG}: 'replicas' must be a positive integer (got: '${REPLICAS}')" >&2
    exit 1
    ;;
esac
if [ "${REPLICAS}" -lt 1 ]; then
  echo "ERROR: ${CFG}: 'replicas' must be >= 1 (got: ${REPLICAS})" >&2
  exit 1
fi

case "${ACTION}" in
  up)
    case "${ENGINE}" in
      vllm)   CHART_DIR="${REPO_ROOT}/charts/vllm-server" ;;
      sglang) CHART_DIR="${REPO_ROOT}/charts/sglang-server" ;;
      *)
        echo "ERROR: ${CFG}: engine=${ENGINE} not supported (expected: vllm, sglang)" >&2
        exit 1
        ;;
    esac
    if [ -z "${MODEL}" ] || [ -z "${PREFIX}" ]; then
      echo "ERROR: ${CFG}: model_name and weights_s3_prefix are required" >&2
      exit 1
    fi

    # Image repo / tag split. Default tag is "latest" if the TOML's image
    # field has no ':' separator.
    IMG_REPO="${IMAGE%:*}"
    IMG_TAG="${IMAGE##*:}"
    if [ "${IMG_REPO}" = "${IMAGE}" ]; then
      IMG_TAG="latest"
    fi

    set_args=(
      --set "nameOverride=${NAME}"
      --set "image.repository=${IMG_REPO}"
      --set "image.tag=${IMG_TAG}"
      --set "model.name=${MODEL}"
      --set "weights.prefix=${PREFIX}"
      --set "gpu.count=${GPU_COUNT:-1}"
      --set "gpu.tensorParallelSize=${GPU_COUNT:-1}"
      --set "gpu.nodePool=${NODE_POOL}"
      --set "replicas=${REPLICAS}"
    )

    # Optional block size. Omit the --set entirely when the TOML doesn't
    # specify one; chart's values.yaml defaults to "" which means the
    # deployment template skips the --block-size / --page-size flag and
    # the engine picks its own optimal size (best for hybrid-arch models).
    # Explicit value here overrides all of the above.
    [ -n "${BLOCK_SIZE}" ]     && set_args+=(--set "model.blockSize=${BLOCK_SIZE}")

    # Optional resource overrides — only add --set when the TOML field is
    # present, otherwise chart defaults from values.yaml apply.
    [ -n "${CPU_REQUEST}" ]    && set_args+=(--set "resources.requests.cpu=${CPU_REQUEST}")
    [ -n "${CPU_LIMIT}" ]      && set_args+=(--set "resources.limits.cpu=${CPU_LIMIT}")
    [ -n "${MEMORY_REQUEST}" ] && set_args+=(--set "resources.requests.memory=${MEMORY_REQUEST}")
    [ -n "${MEMORY_LIMIT}" ]   && set_args+=(--set "resources.limits.memory=${MEMORY_LIMIT}")

    # extra_args[] → extraArgs[N]. Use `--set-json` (Helm ≥ 3.10) with
    # a jq-encoded JSON string literal, so that values containing helm's
    # reserved syntactic characters (`{` `}` `,`) pass through unchanged.
    # `--set` and `--set-string` both tokenize the value (interpreting
    # `{a,b}` as a 2-element array, etc.); `--set-json` skips that and
    # treats the value as raw JSON. Required for compilation overrides
    # like `-O {"cudagraph_capture_sizes":[1,2,4]}`.
    i=0
    while IFS= read -r line; do
      [ -z "${line}" ] && continue
      json_value=$(jq -nc --arg s "${line}" '$s')
      set_args+=(--set-json "extraArgs[${i}]=${json_value}")
      i=$((i + 1))
    done < <(echo "${CFG_JSON}" | jq -r '.extra_args // [] | .[]')

    echo "▶  helm upgrade --install ${NAME} (engine=${ENGINE}, model=${MODEL}, replicas=${REPLICAS})..."
    helm upgrade --install "${NAME}" "${CHART_DIR}" \
      --namespace "${NAMESPACE}" \
      --create-namespace \
      "${set_args[@]}" \
      --wait \
      --timeout 30m

    echo ""
    echo "✓  ${NAME} deployed at replicas=${REPLICAS}."
    echo "   Pods:     kubectl get pods -n ${NAMESPACE} -l mm/server-name=${NAME}"
    echo "   Logs:     kubectl logs -n ${NAMESPACE} -l mm/server-name=${NAME} -f"
    echo "   Forward:  make -f Makefile.server port-forward"
    ;;

  down)
    echo "▶  helm uninstall ${NAME}..."
    helm uninstall "${NAME}" --namespace "${NAMESPACE}" 2>/dev/null \
      || echo "  (helm release ${NAME} not found — skipping)"
    # PV is cluster-scoped + Retain, so helm won't clean it up. The PVC
    # gets removed with the release; the PV needs an explicit delete.
    kubectl delete pv "${NAME}-weights" --ignore-not-found
    echo "✓  ${NAME} removed."
    ;;

  *)
    echo "ERROR: ACTION=${ACTION} not recognised (expected 'up' or 'down')" >&2
    exit 1
    ;;
esac
