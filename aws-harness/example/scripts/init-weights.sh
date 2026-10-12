#!/usr/bin/env bash
# init-weights.sh — Download Gemma 4 26B-A4B AWQ weights to S3.
#
# S3 is the permanent weight store. This script is idempotent: it checks S3
# directly (no sentinel files, no Kubernetes Job state) and exits immediately
# if the weights are already present.
#
# Usage:
#   make init-weights                   # called automatically by `make up`
#
# What this does:
#   1. Counts objects in the S3 prefix — if > 5 files exist, weights are
#      already present and the script exits 0 immediately.
#   2. Deletes any leftover download Job (stale completed/failed runs).
#   3. Applies the download Job, which:
#        Stage 1 — huggingface_hub.snapshot_download → node-local emptyDir
#        Stage 2 — aws s3 sync emptyDir → S3 prefix
#   4. Waits for the Job to complete.
#
# After this runs once, future `make init-weights` calls finish in seconds.
# The weights persist in S3 across cluster destroy/recreate cycles.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

S3_URI="s3://471112539178-us-west-2/weights/gemma-4-26b-awq/"
EXPECTED_MODEL="cyankiwi/gemma-4-26B-A4B-it-AWQ-4bit"
JOB_NAME="gemma-4-26b-download"
JOB_NAMESPACE="default"
TIMEOUT="${INIT_WEIGHTS_TIMEOUT:-7200s}"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }

# ── 1. Check S3 — the authoritative source of truth ──────────────────────────
# Read the .model-id metadata file written by the download job.
# This tells us which model's weights are stored at this prefix.
log "Checking S3 for existing weights: ${S3_URI}"
STORED_MODEL=$(aws s3 cp "${S3_URI}.model-id" - 2>/dev/null | tr -d '[:space:]' || echo "")

if [ -n "${STORED_MODEL}" ] && [ "${STORED_MODEL}" = "${EXPECTED_MODEL}" ]; then
  skip "Weights already in S3 for '${STORED_MODEL}' — nothing to do."
  echo ""
  echo "  Run 'make inference' to serve the model."
  echo ""
  exit 0
fi

if [ -n "${STORED_MODEL}" ] && [ "${STORED_MODEL}" != "${EXPECTED_MODEL}" ]; then
  echo ""
  echo "  ERROR: S3 prefix already contains weights for a different model."
  echo "         Stored:   ${STORED_MODEL}"
  echo "         Expected: ${EXPECTED_MODEL}"
  echo ""
  echo "  Either update S3_URI / EXPECTED_MODEL in this script to match the"
  echo "  stored model, or delete the existing weights and retry:"
  echo "    aws s3 rm ${S3_URI} --recursive"
  echo ""
  exit 1
fi

log "No weights found at ${S3_URI}. Starting download..."

# ── 2. Delete any leftover Job ────────────────────────────────────────────────
# Always start fresh — there is no sentinel to protect against re-download;
# the S3 check above is the only gate. kubectl cannot patch a Job's pod spec,
# so deletion is the only safe way to change/recreate the Job.
log "Removing any existing download Job..."
kubectl delete job "${JOB_NAME}" -n "${JOB_NAMESPACE}" \
  --ignore-not-found --wait=true

# ── 3. Submit the download Job ────────────────────────────────────────────────
log "Applying download Job..."
kubectl apply -f "${REPO_ROOT}/k8s/model-cache/download-job.yaml"

echo ""
echo "  Downloading cyankiwi/gemma-4-26B-A4B-it-AWQ-4bit from HuggingFace (~13 GB)."
echo "  Stage 1: HF download to node-local emptyDir     (~20-40 min)"
echo "  Stage 2: aws s3 sync → ${S3_URI}  (~5-10 min)"
echo ""
echo "  Follow progress:"
echo "    kubectl logs -f job/${JOB_NAME} -c hf-download   -n ${JOB_NAMESPACE}"
echo "    kubectl logs -f job/${JOB_NAME} -c s3-upload     -n ${JOB_NAMESPACE}"
echo ""

# ── 4. Wait for completion ────────────────────────────────────────────────────
log "Waiting for download Job to complete (timeout: ${TIMEOUT})..."
kubectl wait "job/${JOB_NAME}" \
  -n "${JOB_NAMESPACE}" \
  --for=condition=Complete \
  --timeout="${TIMEOUT}"

ok "Weights uploaded to ${S3_URI}"
echo ""
echo "  Next steps:"
echo "    make inference         — deploy the model (reads weights from S3)"
echo "    make inference-scaled  — deploy with 2 replicas across both GPU nodes"
echo ""
