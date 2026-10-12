#!/usr/bin/env bash
# init-weights.sh — populate the shared weights bucket from a HuggingFace
# repo. Idempotent on a sentinel object so re-runs are cheap.
#
# Two modes, selected by the MODE env var:
#
#   MODE=cluster (default)
#     Submits a Kubernetes Job that runs `huggingface_hub.snapshot_download`
#     → `aws s3 sync` inside the cluster. Best for big models / slow home
#     connections — the HF→AWS→S3 path uses the AWS backbone, not your
#     home pipe. Requires the cluster to be up (`make up` + `make addons`).
#
#   MODE=local
#     Runs entirely on your laptop: HF → local cache → `aws s3 sync` from
#     the laptop. Original behavior, kept for cases where you can't reach
#     the cluster (e.g. running this before the cluster exists) or for
#     small models where laptop-side is faster.
#
# Both modes are idempotent on the same sentinel object
# (s3://<bucket>/<prefix>/.harness-init-weights containing the HF repo id).
#
# Usage:
#   bash scripts/init-weights.sh <hf-repo> <s3-rel-prefix>
#   MODE=local bash scripts/init-weights.sh <hf-repo> <s3-rel-prefix>
#
# Examples:
#   bash scripts/init-weights.sh Qwen/Qwen3.6-35B-A3B-FP8 weights/qwen-3-6-35b-a3b-fp8
#   MODE=local bash scripts/init-weights.sh Qwen/Qwen2.5-0.5B-Instruct weights/qwen-2-5-0-5b
#
# Env overrides (both modes):
#   WEIGHTS_BUCKET=...    destination bucket (default: 471112539178-us-west-2)
#   AWS_REGION=...        bucket region (default: us-west-2)
#
# Cluster-mode env:
#   SCRATCH_GIB=100       ephemeral PVC size in the Job (default 100)
#   KUBECONFIG=...        kubeconfig path (Makefile.server already exports
#                         ~/.kube/$(CLUSTER_NAME))
#   Optional: a `hf-token` Secret in the default namespace for gated repos:
#     kubectl create secret generic hf-token --from-literal=token=hf_xxx
#
# Local-mode env:
#   HF_TOKEN=...          for gated repos (read from your shell env)
#   LOCAL_CACHE=...       download dir (default: ~/.cache/hf-init-weights/<repo>)
#   PYTHON=...            python interpreter (default: python3)

set -euo pipefail

MODEL="${1:-}"
PREFIX_RAW="${2:-}"
if [ -z "${MODEL}" ] || [ -z "${PREFIX_RAW}" ]; then
  echo "ERROR: usage: bash scripts/init-weights.sh <hf-repo> <s3-rel-prefix>" >&2
  echo "       e.g.   bash scripts/init-weights.sh Qwen/Qwen3.6-35B-A3B-FP8 weights/qwen-3-6-35b-a3b-fp8" >&2
  exit 1
fi

# Normalise the prefix to end in exactly one slash.
PREFIX="${PREFIX_RAW%/}/"

MODE="${MODE:-cluster}"
BUCKET="${WEIGHTS_BUCKET:-471112539178-us-west-2}"
REGION="${AWS_REGION:-us-west-2}"
SENTINEL_KEY="${PREFIX}.harness-init-weights"

case "${MODE}" in
  cluster|local) ;;
  *) echo "ERROR: MODE must be 'cluster' or 'local' (got: ${MODE})" >&2; exit 1 ;;
esac

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }

# ── Shared sentinel check (both modes) ────────────────────────────────────
log "Checking s3://${BUCKET}/${SENTINEL_KEY} ..."
if existing=$(aws s3 cp "s3://${BUCKET}/${SENTINEL_KEY}" - --region "${REGION}" 2>/dev/null) \
   && [ "${existing}" = "${MODEL}" ]; then
  skip "Prefix already populated with ${MODEL} (sentinel matches). Nothing to do."
  exit 0
fi

if [ -n "${existing:-}" ] && [ "${existing}" != "${MODEL}" ]; then
  log "Sentinel says ${existing}; replacing with ${MODEL} (aws s3 sync --delete will clean up old files)."
fi


# ══════════════════════════════════════════════════════════════════════════
# MODE=cluster: submit a Job and tail its logs.
# ══════════════════════════════════════════════════════════════════════════
if [ "${MODE}" = "cluster" ]; then
  for tool in kubectl python3 aws; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
      echo "ERROR: required tool '${tool}' not found on PATH" >&2
      exit 1
    fi
  done

  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  TEMPLATE="${SCRIPT_DIR}/../k8s/jobs/init-weights.yaml.tmpl"
  if [ ! -f "${TEMPLATE}" ]; then
    echo "ERROR: Job template not found at ${TEMPLATE}" >&2
    exit 1
  fi

  # SLUG: DNS-safe Job name suffix derived from the prefix.
  #   weights/qwen-3-6-35b-a3b-fp8/ → qwen-3-6-35b-a3b-fp8
  # Lowercase + replace non-alnum with '-' + collapse + trim trailing dashes.
  SLUG=$(echo "${PREFIX_RAW#weights/}" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's#[^a-z0-9]+#-#g; s#^-+##; s#-+$##')
  if [ -z "${SLUG}" ]; then
    echo "ERROR: could not derive a DNS-safe slug from PREFIX=${PREFIX_RAW}" >&2
    exit 1
  fi
  JOB_NAME="init-weights-${SLUG}"
  # 200 GiB default: snapshot_download stages each shard at full size in
  # a .incomplete file before renaming, so peak transient usage is
  # max_workers × largest_shard + already-finalised files. For a 100 GB
  # checkpoint with 32 GB shards and max_workers=2, peak is ~164 GB.
  # EBS gp3 at $0.08/GB-month × 10 min lifetime ≈ $0.0004/run, so we
  # err generous. Override with SCRATCH_GIB=… if you have a reason.
  SCRATCH_GIB="${SCRATCH_GIB:-200}"

  # Sanity-check the cluster is reachable and the IRSA ServiceAccount exists.
  # If `make infra` hasn't been applied since terraform/init-weights.tf was
  # added, the SA won't be there yet — surface that with a clear message.
  if ! kubectl get sa init-weights -n default >/dev/null 2>&1; then
    echo "ERROR: ServiceAccount 'init-weights' not found in namespace 'default'." >&2
    echo "       Run 'make infra' to apply terraform/init-weights.tf, which" >&2
    echo "       creates the IAM role + IRSA + ServiceAccount the Job needs." >&2
    echo "" >&2
    echo "       Or fall back to laptop-side: MODE=local bash scripts/init-weights.sh ..." >&2
    exit 1
  fi

  # Preflight: the Job pins to role=gpu nodes (system nodes are too small
  # to host a 30+ GB HF download; see init-weights.yaml.tmpl for the
  # rationale). If no GPU node is up the Job would sit Pending forever,
  # so fail fast with a clear next step.
  GPU_NODE_COUNT=$(kubectl get nodes -l role=gpu \
    --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [ "${GPU_NODE_COUNT}" = "0" ]; then
    echo "ERROR: no GPU nodes available — the init-weights Job runs on GPU" >&2
    echo "       nodes (system nodes are too small for large HF downloads)." >&2
    echo "" >&2
    echo "       Scale up at least one GPU node first:" >&2
    echo "         make scale-gpu N=1" >&2
    echo "" >&2
    echo "       Or fall back to laptop-side: MODE=local bash scripts/init-weights.sh ..." >&2
    exit 1
  fi

  # Render the template via Python's os.path.expandvars (no envsubst
  # dependency — works on every macOS / Linux out of the box).
  RENDERED=$(SLUG="${SLUG}" MODEL="${MODEL}" PREFIX="${PREFIX}" \
             BUCKET="${BUCKET}" REGION="${REGION}" SCRATCH_GIB="${SCRATCH_GIB}" \
             python3 -c "import os,sys; print(os.path.expandvars(sys.stdin.read()))" \
             < "${TEMPLATE}")

  # Delete any previous Job with the same name so re-runs against a still-
  # present (e.g. failed, ttl not yet elapsed) Job don't fail with
  # AlreadyExists. Safe — only matches our `mm/task=init-weights` Jobs.
  if kubectl get job "${JOB_NAME}" -n default >/dev/null 2>&1; then
    log "Removing previous Job ${JOB_NAME} so a fresh one can run..."
    kubectl delete job "${JOB_NAME}" -n default --wait=true >/dev/null
  fi

  log "Submitting ${JOB_NAME} (slug=${SLUG}, scratch=${SCRATCH_GIB}Gi)..."
  echo "${RENDERED}" | kubectl apply -f -

  # Out-of-band progress: the init container writes a JSON status object to
  # S3 every 30s while it downloads. Tell the user how to read it now, in
  # case kubectl logs against the new pod hits a TLS handshake timeout
  # (common on freshly-joined system nodes).
  PROGRESS_URI="s3://${BUCKET}/${PREFIX}.harness-init-weights-progress"
  echo ""
  echo "  📊  Live download progress (works even if kubectl logs hangs):"
  echo "        aws s3 cp ${PROGRESS_URI} - --region ${REGION} 2>/dev/null | jq ."
  echo "      Or poll every 30s:"
  echo "        watch -n 30 \"aws s3 cp ${PROGRESS_URI} - --region ${REGION} 2>/dev/null | jq .\""
  echo ""

  # Tail logs from both containers. `-l job-name=...` follows whichever
  # pod the Job spawns. `--all-containers` covers init + main.
  log "Waiting for pod to be Ready (image pulls may take a minute)..."
  if ! kubectl wait --for=condition=Ready pod \
        -l "job-name=${JOB_NAME}" -n default --timeout=10m 2>/dev/null; then
    # Pod may have completed before we got to Ready — that's a success
    # case, not a failure. Fall through to the completion wait below.
    :
  fi

  log "Tailing Job logs (Ctrl-C is safe — Job keeps running; use the progress URI above)..."
  kubectl logs -f -l "job-name=${JOB_NAME}" -n default \
    --all-containers --prefix --max-log-requests=4 --timestamps=false 2>/dev/null || true

  # Final completion check — blocks until success/failure.
  log "Waiting for Job completion (up to 3h)..."
  if kubectl wait --for=condition=complete "job/${JOB_NAME}" -n default --timeout=3h 2>/dev/null; then
    ok "Job ${JOB_NAME} succeeded."
    ok "Uploaded ${MODEL} → s3://${BUCKET}/${PREFIX}"
    echo "   Verify: aws s3 ls s3://${BUCKET}/${PREFIX} --region ${REGION}"
  else
    echo "✗  Job ${JOB_NAME} did not complete cleanly." >&2
    echo "   Inspect: kubectl describe job ${JOB_NAME} -n default" >&2
    echo "            kubectl logs -l job-name=${JOB_NAME} -n default --all-containers --previous" >&2
    exit 1
  fi
  exit 0
fi


# ══════════════════════════════════════════════════════════════════════════
# MODE=local: laptop-side download + upload (original behavior).
# ══════════════════════════════════════════════════════════════════════════
LOCAL_CACHE_ROOT="${LOCAL_CACHE:-$HOME/.cache/hf-init-weights}"
LOCAL="${LOCAL_CACHE_ROOT}/${MODEL}"
PYTHON="${PYTHON:-python3}"

for tool in aws "${PYTHON}"; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "ERROR: required tool '${tool}' not found on PATH" >&2
    exit 1
  fi
done

log "Downloading ${MODEL} → ${LOCAL} (laptop-side; this can take a while on slow connections — consider MODE=cluster)"
mkdir -p "${LOCAL}"

# pip-install huggingface_hub on the fly. Quiet upgrade — no-op when current.
"${PYTHON}" -m pip install --quiet --upgrade 'huggingface_hub>=0.24' >/dev/null

"${PYTHON}" - "${MODEL}" "${LOCAL}" <<'PY'
import os, sys
from huggingface_hub import snapshot_download

model, local_dir = sys.argv[1], sys.argv[2]
snapshot_download(
    repo_id=model,
    local_dir=local_dir,
    token=os.environ.get("HF_TOKEN") or None,
)
PY
ok "Downloaded to ${LOCAL}"

log "aws s3 sync ${LOCAL}/ s3://${BUCKET}/${PREFIX} (--delete keeps prefix tidy)"
aws s3 sync "${LOCAL}/" "s3://${BUCKET}/${PREFIX}" \
  --region "${REGION}" \
  --delete \
  --exclude '.cache/*' \
  --exclude '.huggingface/*'

echo -n "${MODEL}" | aws s3 cp - "s3://${BUCKET}/${SENTINEL_KEY}" --region "${REGION}"

ok "Uploaded ${MODEL} → s3://${BUCKET}/${PREFIX}"
echo "   Verify:  aws s3 ls s3://${BUCKET}/${PREFIX} --region ${REGION}"
echo "   Deploy:  edit a server TOML with weights_s3_prefix = \"${PREFIX}\" and run"
echo "            make -f Makefile.server server-up CFG=servers/<name>.toml"
