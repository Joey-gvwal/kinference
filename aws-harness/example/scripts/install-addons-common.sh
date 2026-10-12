#!/usr/bin/env bash
# install-addons-common.sh
# Cluster-agnostic add-ons shared by all model configurations.
# Sourced by install-addons.sh (26B) and install-addons-31b.sh (31B).
#
# Installs:
#   0. kubectl configuration
#   1. NVIDIA device plugin
#   2. Gateway API CRDs
#   3. Mountpoint for Amazon S3 CSI driver
#   4. Helm repos
#   5. agentgateway
#   6. AWS Load Balancer Controller
#   7. llm-d-infra
#   8. agentgateway RBAC patch for InferencePool
#
# Model-specific steps (GAIE InferencePool, HTTPRoute) are left to the caller.
#
# Expected env vars (all have defaults):
#   CLUSTER_NAME    — EKS cluster name (default: wm-inference)
#   AWS_REGION      — AWS region       (default: us-west-2)
#   TF_WORKSPACE    — Terraform workspace to read LBC role ARN from (default: wm-inference)
#   KUBECONFIG      — kubeconfig file path (default: ~/.kube/$CLUSTER_NAME)
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-wm-inference}"
AWS_REGION="${AWS_REGION:-us-west-2}"
TF_WORKSPACE="${TF_WORKSPACE:-wm-inference}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }
warn() { echo "⚠  $*"; }

# helm_install_if_needed RELEASE VERSION CHART [EXTRA_ARGS...]
# Skips if the release is already "deployed" at the target version.
# A "failed" release at the same version is NOT skipped — it gets retried.
helm_install_if_needed() {
  local release="$1"
  local version="$2"
  local chart="$3"
  shift 3

  local info chart_name status
  info=$(helm list -A --filter "^${release}$" --output json 2>/dev/null | \
    python3 -c "
import sys, json
r = json.load(sys.stdin)
print(r[0]['chart'] + '|' + r[0]['status'] if r else '|')
" 2>/dev/null || echo "|")
  chart_name=$(echo "$info" | cut -d'|' -f1)
  status=$(echo "$info"     | cut -d'|' -f2)

  if echo "$chart_name" | grep -q "${version}" && [ "$status" = "deployed" ]; then
    skip "${release} already at ${version} (deployed) — skipping"
    return 0
  fi

  log "Installing/upgrading ${release} to ${version}..."
  helm upgrade --install "${release}" "${chart}" \
    --version "${version}" \
    "$@"
  ok "${release} installed"
}

# ── 0. Configure kubectl ──────────────────────────────────────────────────────
log "Configuring kubectl for cluster '${CLUSTER_NAME}' in ${AWS_REGION}..."
aws eks update-kubeconfig \
  --region "${AWS_REGION}" \
  --name "${CLUSTER_NAME}" \
  --kubeconfig "${KUBECONFIG:-$HOME/.kube/$CLUSTER_NAME}"
ok "kubectl configured"

log "Waiting for system nodes to be Ready..."
kubectl wait node \
  --selector role=system \
  --for condition=Ready \
  --timeout=300s
ok "System nodes ready"

# ── 1. NVIDIA Device Plugin ───────────────────────────────────────────────────
log "Applying NVIDIA device plugin..."
kubectl apply -f "${REPO_ROOT}/k8s/addons/nvidia-device-plugin.yaml"
ok "NVIDIA device plugin applied"

# ── 2. Gateway API CRDs ───────────────────────────────────────────────────────
log "Applying Gateway API CRDs (v1.5.1)..."
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.5.1/standard-install.yaml
ok "Gateway API CRDs applied"

# ── 3. Mountpoint for Amazon S3 CSI Driver ───────────────────────────────────
# Provides ReadOnlyMany PersistentVolumes backed by S3.
# Authentication via GPU node instance profile — no IRSA needed.
helm repo add aws-mountpoint-s3-csi-driver \
  https://awslabs.github.io/mountpoint-s3-csi-driver 2>/dev/null || true
helm repo update aws-mountpoint-s3-csi-driver

S3_CSI_VER=$(helm search repo aws-mountpoint-s3-csi-driver/aws-mountpoint-s3-csi-driver \
  --output json 2>/dev/null | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['version'] if r else '')" \
  2>/dev/null || echo "")
if [ -z "${S3_CSI_VER}" ]; then
  warn "Could not resolve aws-mountpoint-s3-csi-driver chart version — skipping"
else
  helm_install_if_needed aws-mountpoint-s3-csi-driver "${S3_CSI_VER}" \
    aws-mountpoint-s3-csi-driver/aws-mountpoint-s3-csi-driver \
    --namespace kube-system \
    --wait --timeout 5m
fi

# ── 4. Helm repos ─────────────────────────────────────────────────────────────
helm repo list 2>/dev/null | grep -q "^llm-d-infra"        || helm repo add llm-d-infra        https://llm-d-incubation.github.io/llm-d-infra
helm repo list 2>/dev/null | grep -q "^llm-d-modelservice" || helm repo add llm-d-modelservice  https://llm-d-incubation.github.io/llm-d-modelservice/
helm repo list 2>/dev/null | grep -q "^eks"                 || helm repo add eks                 https://aws.github.io/eks-charts
helm repo update llm-d-infra llm-d-modelservice eks

# ── 5. agentgateway ───────────────────────────────────────────────────────────
helm_install_if_needed agentgateway-crds v1.0.1 \
  oci://cr.agentgateway.dev/charts/agentgateway-crds \
  --namespace agentgateway-system \
  --create-namespace \
  --wait --timeout 5m

helm_install_if_needed agentgateway v1.0.1 \
  oci://cr.agentgateway.dev/charts/agentgateway \
  --namespace agentgateway-system \
  --create-namespace \
  --set inferenceExtension.enabled=true \
  --wait --timeout 5m

# ── 6. AWS Load Balancer Controller ──────────────────────────────────────────
# Reads the IRSA role ARN from the correct Terraform workspace.
LBC_ROLE_ARN=$(cd "${REPO_ROOT}/terraform" && \
  env -u TF_WORKSPACE terraform workspace select "${TF_WORKSPACE}" >/dev/null 2>&1 && \
  env -u TF_WORKSPACE terraform output -raw aws_lbc_role_arn 2>/dev/null || echo "")
if [ -z "$LBC_ROLE_ARN" ]; then
  warn "Could not read aws_lbc_role_arn from Terraform workspace '${TF_WORKSPACE}' — run 'make infra' first"
else
  helm_install_if_needed aws-load-balancer-controller "1.17.1" \
    eks/aws-load-balancer-controller \
    --namespace kube-system \
    --set clusterName="${CLUSTER_NAME}" \
    --set serviceAccount.create=true \
    --set replicaCount=1 \
    --set "serviceAccount.annotations.eks\.amazonaws\.com/role-arn=${LBC_ROLE_ARN}" \
    --wait --timeout 5m
fi

# ── 7. llm-d-infra ───────────────────────────────────────────────────────────
LLM_D_CURRENT=$(helm list -n llmd --filter "^llm-d$" --output json 2>/dev/null | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['chart'] if r else '')" \
  2>/dev/null || echo "")

if [ -n "$LLM_D_CURRENT" ]; then
  skip "llm-d already installed (${LLM_D_CURRENT}) — skipping"
else
  log "Installing llm-d-infra..."
  helm upgrade --install llm-d llm-d-infra/llm-d-infra \
    --version v1.4.0 \
    --namespace llmd \
    --create-namespace \
    -f "${REPO_ROOT}/k8s/addons/llmd-values.yaml" \
    --wait --timeout 10m
  ok "llm-d-infra installed"
fi

# ── 8. Patch agentgateway RBAC for InferencePool ─────────────────────────────
# agentgateway's ClusterRole is created before the InferencePool CRD exists,
# so it has no rules for inference.networking.k8s.io. Patch is idempotent.
log "Patching agentgateway ClusterRole to allow InferencePool reads..."
AG_CLUSTERROLE=$(kubectl get clusterrole \
  -l "app.kubernetes.io/instance=agentgateway" \
  --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null \
  | head -1)
if [ -z "$AG_CLUSTERROLE" ]; then
  AG_CLUSTERROLE=$(kubectl get clusterrole --no-headers \
    -o custom-columns=NAME:.metadata.name 2>/dev/null \
    | grep -E '^agentgateway' | head -1)
fi
if [ -z "$AG_CLUSTERROLE" ]; then
  warn "Could not find agentgateway ClusterRole — skipping RBAC patch"
else
  log "Found ClusterRole: ${AG_CLUSTERROLE}"
  kubectl patch clusterrole "${AG_CLUSTERROLE}" --type=json -p='[
    {
      "op": "add",
      "path": "/rules/-",
      "value": {
        "apiGroups": ["inference.networking.k8s.io"],
        "resources": ["inferencepools"],
        "verbs": ["get", "list", "watch"]
      }
    }
  ]'
fi
log "Enabling InferencePool support (AGW_ENABLE_INFER_EXT=true)..."
kubectl set env deployment/agentgateway -n agentgateway-system \
  AGW_ENABLE_INFER_EXT=true 2>/dev/null || true
log "Restarting agentgateway to pick up RBAC + InferExt changes..."
kubectl rollout restart deployment -n agentgateway-system 2>/dev/null || true
kubectl rollout status deployment -n agentgateway-system --timeout=120s 2>/dev/null || true
ok "agentgateway RBAC patched and InferencePool support enabled"
