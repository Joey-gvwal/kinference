#!/usr/bin/env bash
# init-weights-31b-awq.sh — Download Gemma 4 31B AWQ-4bit weights to S3.
#
# Idempotent: checks S3 directly and exits immediately if weights are present.
#
# Usage:
#   make -f Makefile.31b-awq init-weights
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

S3_URI="s3://471112539178-us-west-2/weights/gemma-4-31b-awq/"
EXPECTED_MODEL="cyankiwi/gemma-4-31B-it-AWQ-4bit"
JOB_NAME="gemma-4-31b-awq-download"
JOB_NAMESPACE="default"
TIMEOUT="${INIT_WEIGHTS_TIMEOUT:-3600s}"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }

# ── 1. Check S3 ───────────────────────────────────────────────────────────────
log "Checking S3 for existing weights: ${S3_URI}"
STORED_MODEL=$(aws s3 cp "${S3_URI}.model-id" - 2>/dev/null | tr -d '[:space:]' || echo "")

if [ -n "${STORED_MODEL}" ] && [ "${STORED_MODEL}" = "${EXPECTED_MODEL}" ]; then
  skip "Weights already in S3 for '${STORED_MODEL}' — nothing to do."
  echo ""
  echo "  Run 'make -f Makefile.31b-awq inference' to serve the model."
  echo ""
  exit 0
fi

if [ -n "${STORED_MODEL}" ] && [ "${STORED_MODEL}" != "${EXPECTED_MODEL}" ]; then
  echo ""
  echo "  ERROR: S3 prefix already contains weights for a different model."
  echo "         Stored:   ${STORED_MODEL}"
  echo "         Expected: ${EXPECTED_MODEL}"
  echo ""
  echo "  Delete existing weights and retry:"
  echo "    aws s3 rm ${S3_URI} --recursive"
  echo ""
  exit 1
fi

log "No weights found at ${S3_URI}. Starting download..."

# ── 2. Delete any leftover Job ────────────────────────────────────────────────
log "Removing any existing download Job..."
kubectl delete job "${JOB_NAME}" -n "${JOB_NAMESPACE}" \
  --ignore-not-found --wait=true

# ── 3. Submit the download Job ────────────────────────────────────────────────
log "Applying download Job..."
kubectl apply -f "${REPO_ROOT}/k8s/model-cache/download-job-31b-awq.yaml"

echo ""
echo "  Downloading cyankiwi/gemma-4-31B-it-AWQ-4bit from HuggingFace (~16 GB)."
echo "  Stage 1: HF download to node-local emptyDir     (~15-20 min)"
echo "  Stage 2: aws s3 sync → ${S3_URI}  (~3-5 min)"
echo ""
echo "  Follow progress:"
echo "    kubectl logs -f job/${JOB_NAME} -c hf-download  -n ${JOB_NAMESPACE}"
echo "    kubectl logs -f job/${JOB_NAME} -c s3-upload    -n ${JOB_NAMESPACE}"
echo ""

# ── 4. Wait for completion ────────────────────────────────────────────────────
log "Waiting for download Job to complete (timeout: ${TIMEOUT})..."
kubectl wait "job/${JOB_NAME}" \
  -n "${JOB_NAMESPACE}" \
  --for=condition=Complete \
  --timeout="${TIMEOUT}"

ok "Weights uploaded to ${S3_URI}"
echo ""
echo "  Next step: make -f Makefile.31b-awq inference"
echo ""
