#!/usr/bin/env bash
# install-addons-31b-awq.sh — Gemma 4 31B AWQ cluster add-ons
# Called by `make -f Makefile.31b-awq addons`.
#
# Runs the cluster-agnostic common steps, then installs the 31B-AWQ-specific
# GAIE InferencePool and HTTPRoute.
set -euo pipefail

export CLUSTER_NAME="${CLUSTER_NAME:-wm-inf-31b-awq}"
export AWS_REGION="${AWS_REGION:-us-west-2}"
TF_WORKSPACE="${TF_WORKSPACE:-31b-awq}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Common add-ons (NVIDIA, Gateway API, S3 CSI, agentgateway, LBC, llm-d) ───
# shellcheck source=install-addons-common.sh
source "${SCRIPT_DIR}/install-addons-common.sh"

# ── S3 PV/PVC for 31B AWQ weights ─────────────────────────────────────────────
log "Applying S3 PersistentVolume and PersistentVolumeClaim (31B AWQ)..."
kubectl apply -f "${REPO_ROOT}/k8s/model-cache/s3-pv-31b-awq.yaml"
kubectl apply -f "${REPO_ROOT}/k8s/model-cache/s3-pvc-31b-awq.yaml"
ok "S3 PV/PVC applied"

# ── GAIE InferencePool (31B AWQ) ──────────────────────────────────────────────
GAIE_VER="v1.4.0"
GAIE_CRD_BASE="https://raw.githubusercontent.com/kubernetes-sigs/gateway-api-inference-extension/${GAIE_VER}/config/crd/bases"

log "Applying GAIE InferencePool CRD (${GAIE_VER})..."
kubectl apply -f "${GAIE_CRD_BASE}/inference.networking.k8s.io_inferencepools.yaml"
kubectl wait --for=condition=Established \
  crd/inferencepools.inference.networking.k8s.io \
  --timeout=60s
ok "InferencePool CRD established"

GAIE_CURRENT=$(helm list -n default --filter "^gemma-4-31b-awq-pool$" --output json 2>/dev/null | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['chart'] if r else '')" \
  2>/dev/null || echo "")

if [ -n "$GAIE_CURRENT" ]; then
  skip "GAIE InferencePool gemma-4-31b-awq-pool already installed (${GAIE_CURRENT}) — skipping"
else
  log "Installing GAIE InferencePool gemma-4-31b-awq-pool (${GAIE_VER})..."
  helm upgrade --install gemma-4-31b-awq-pool \
    oci://registry.k8s.io/gateway-api-inference-extension/charts/inferencepool \
    --version "${GAIE_VER}" \
    --namespace default \
    -f "${REPO_ROOT}/k8s/addons/gaie-values-31b-awq.yaml" \
    --wait --timeout 5m
  ok "GAIE InferencePool installed"
fi

# ── HTTPRoute (31B AWQ) ───────────────────────────────────────────────────────
log "Applying HTTPRoute (gemma-4-31b-awq-route)..."
kubectl apply -f "${REPO_ROOT}/k8s/expose/httproute-31b-awq.yaml"
ok "HTTPRoute applied"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  All add-ons up to date (Gemma 4 31B AWQ cluster)."
echo ""
echo "  Next steps:"
echo "  1. Load model weights into S3 (once, ~20-30 min):"
echo "       make -f Makefile.31b-awq init-weights"
echo "  2. Deploy model:"
echo "       make -f Makefile.31b-awq inference"
echo "  3. Expose publicly:"
echo "       make -f Makefile.31b-awq public"
echo ""
echo "  Check pod health:  make -f Makefile.31b-awq status"
echo "════════════════════════════════════════════════════════════"
