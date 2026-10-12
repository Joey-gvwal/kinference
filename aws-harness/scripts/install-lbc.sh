#!/usr/bin/env bash
# install-lbc.sh — idempotent install of the AWS Load Balancer Controller.
#
# LBC is the controller that watches Kubernetes Service-of-type-LoadBalancer
# and Ingress resources annotated for AWS, and provisions the corresponding
# NLBs / ALBs (and target groups / listener rules) in AWS.
#
# IRSA for LBC is already provisioned by terraform/eks.tf
# (module.aws_lbc_irsa, bound to kube-system:aws-load-balancer-controller).
# This script installs the controller itself via Helm and attaches that
# IRSA role ARN to the ServiceAccount it creates.
#
# Kept out of `make addons` deliberately — LBC is only needed for the
# public-exposure flow (`make expose-gateway`), so we keep the default
# cluster lighter. Re-run is a no-op when the release is already at the
# resolved chart version.
#
# Required env (the Makefile sets these):
#   CLUSTER_NAME   EKS cluster name
#   AWS_REGION     region
#   KUBECONFIG     kubeconfig file path

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-model-harness}"
AWS_REGION="${AWS_REGION:-us-west-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TERRAFORM_DIR="${REPO_ROOT}/terraform"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }
warn() { echo "⚠  $*" >&2; }

# ── 0. kubectl sanity check ───────────────────────────────────────────────────
ACTUAL_CTX="$(kubectl config current-context 2>/dev/null || true)"
if [ -z "${ACTUAL_CTX}" ]; then
  warn "No kubectl context — run 'make kubeconfig' first."
  exit 1
fi

# ── 1. Resolve the LBC IRSA role ARN ──────────────────────────────────────────
# Preferred: terraform output (definitive source). Falls back to
# constructing the ARN from the IRSA module's naming convention if
# terraform state isn't reachable from this invocation (e.g. CI without
# the state backend). The role name is fixed by terraform/eks.tf as
# `${var.cluster_name}-aws-lbc`.
log "Resolving AWS Load Balancer Controller IRSA role ARN..."

ROLE_ARN=""
if [ -d "${TERRAFORM_DIR}" ]; then
  ROLE_ARN=$(cd "${TERRAFORM_DIR}" \
    && terraform output -raw aws_lbc_role_arn 2>/dev/null \
    || echo "")
fi

if [ -z "${ROLE_ARN}" ]; then
  log "  terraform output unavailable — deriving by convention"
  ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
  if [ -z "${ACCOUNT_ID}" ]; then
    warn "Could not resolve AWS account ID; ensure your AWS credentials are configured."
    exit 1
  fi
  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${CLUSTER_NAME}-aws-lbc"
fi

# Validate the role actually exists (cheap call, $0 cost). Surfaces a
# clear error if terraform hasn't been applied yet.
if ! aws iam get-role --role-name "${ROLE_ARN##*/}" >/dev/null 2>&1; then
  warn "IAM role '${ROLE_ARN}' not found."
  warn "Run 'make infra' to apply terraform (which creates the LBC IRSA role)."
  exit 1
fi
ok "IRSA role: ${ROLE_ARN}"

# ── 2. Helm repo ──────────────────────────────────────────────────────────────
log "Updating eks-charts Helm repo..."
if ! helm repo list 2>/dev/null | grep -q '^eks[[:space:]]'; then
  helm repo add eks https://aws.github.io/eks-charts
fi
helm repo update eks >/dev/null

# ── 3. Version detection + idempotent install ─────────────────────────────────
LBC_VER=$(helm search repo eks/aws-load-balancer-controller \
  --output json 2>/dev/null | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['version'] if r else '')" \
  2>/dev/null || echo "")

if [ -z "${LBC_VER}" ]; then
  warn "Could not resolve aws-load-balancer-controller chart version — aborting."
  exit 1
fi

EXISTING_VER=$(helm list -n kube-system --filter '^aws-load-balancer-controller$' \
  --output json 2>/dev/null | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['chart'] if r else '')" \
  2>/dev/null || echo "")

if echo "${EXISTING_VER}" | grep -q "${LBC_VER}$"; then
  skip "aws-load-balancer-controller already at ${EXISTING_VER}"
else
  log "Installing aws-load-balancer-controller ${LBC_VER}..."
  helm upgrade --install aws-load-balancer-controller \
    eks/aws-load-balancer-controller \
    --version "${LBC_VER}" \
    --namespace kube-system \
    --set "clusterName=${CLUSTER_NAME}" \
    --set "region=${AWS_REGION}" \
    --set "vpcId=$(aws eks describe-cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" --query 'cluster.resourcesVpcConfig.vpcId' --output text 2>/dev/null)" \
    --set "serviceAccount.create=true" \
    --set "serviceAccount.name=aws-load-balancer-controller" \
    --set "serviceAccount.annotations.eks\.amazonaws\.com/role-arn=${ROLE_ARN}" \
    --wait --timeout 5m
  ok "aws-load-balancer-controller installed"
fi

echo ""
ok "LBC ready. Next:"
echo "   make expose-gateway        # provision a public NLB for the gateway Service"
echo "   make gateway-endpoint      # print the public URL(s)"
echo "   make unexpose-gateway      # tear it down"
