#!/usr/bin/env bash
# cache-image.sh — Mirror the llm-d-cuda container image from GHCR to ECR.
#
# Run once per image version from the developer's laptop (not in-cluster).
# Idempotent: exits immediately if the tag already exists in ECR.
#
# Why ECR?
#   Cold pulls of ghcr.io/llm-d/llm-d-cuda from GHCR on a fresh GPU node
#   take 10-20 minutes. ECR in the same AWS region pulls over the internal
#   network (~1-3 min), costs nothing in egress, and has no rate limits.
#
# Requirements:
#   • docker (running locally)
#   • aws CLI with ECR push permissions (ecr:GetAuthorizationToken,
#     ecr:BatchCheckLayerAvailability, ecr:PutImage, etc.)
#
# Usage:
#   make cache-image                          # mirror default v0.6.0
#   SOURCE_IMAGE=ghcr.io/llm-d/llm-d-cuda:v0.7.0 ECR_TAG=v0.7.0 make cache-image
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-west-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-471112539178}"
SOURCE_IMAGE="${SOURCE_IMAGE:-ghcr.io/llm-d/llm-d-cuda:v0.6.0}"
ECR_REPO="${ECR_REPO:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/llm-d-cuda}"
ECR_TAG="${ECR_TAG:-v0.6.0}"
ECR_IMAGE="${ECR_REPO}:${ECR_TAG}"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }

# ── 1. Idempotency check ──────────────────────────────────────────────────────
# aws ecr describe-images exits non-zero when the tag doesn't exist — use that
# as the gate rather than file sentinels or counting files.
log "Checking ECR for existing tag: ${ECR_IMAGE} ..."
if aws ecr describe-images \
     --region "${AWS_REGION}" \
     --repository-name "llm-d-cuda" \
     --image-ids "imageTag=${ECR_TAG}" \
     --output text &>/dev/null; then
  skip "Tag '${ECR_TAG}' already exists in ECR — nothing to do."
  echo ""
  echo "  ECR image: ${ECR_IMAGE}"
  echo "  Run 'make inference' to deploy using this image."
  echo ""
  exit 0
fi

log "Tag not found in ECR. Starting pull → tag → push ..."

# ── 2. Authenticate Docker to ECR ────────────────────────────────────────────
log "Authenticating Docker to ECR (${AWS_REGION}) ..."
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login \
      --username AWS \
      --password-stdin \
      "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ok "Docker authenticated to ECR"

# ── 3. Pull from GHCR ────────────────────────────────────────────────────────
log "Pulling ${SOURCE_IMAGE} ..."
docker pull "${SOURCE_IMAGE}"
ok "Pulled ${SOURCE_IMAGE}"

# ── 4. Tag for ECR ───────────────────────────────────────────────────────────
log "Tagging as ${ECR_IMAGE} ..."
docker tag "${SOURCE_IMAGE}" "${ECR_IMAGE}"

# ── 5. Push to ECR ───────────────────────────────────────────────────────────
log "Pushing ${ECR_IMAGE} ..."
docker push "${ECR_IMAGE}"

ok "Image cached in ECR: ${ECR_IMAGE}"
echo ""
echo "  EKS nodes will now pull this image from ECR on the next deployment."
echo "  Run 'make inference' to redeploy using the ECR image."
echo ""
