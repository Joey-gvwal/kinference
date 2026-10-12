#!/usr/bin/env bash
# install-addons.sh — idempotent Kubernetes addon install.
#
# Installs:
#   1. NVIDIA device plugin DaemonSet
#   2. gp3 StorageClass (default) for ephemeral PVCs
#   3. Mountpoint for Amazon S3 CSI driver
#
# Both are safe to re-run. Authentication for the S3 CSI driver flows from
# the GPU node instance profile (granted s3:GetObject + s3:ListBucket on the
# weights bucket by terraform/eks.tf), so no IRSA is needed for the driver.
#
# Required env (Makefile sets these):
#   CLUSTER_NAME   EKS cluster name
#   AWS_REGION     region
#   KUBECONFIG     kubeconfig file path

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-model-harness}"
AWS_REGION="${AWS_REGION:-us-west-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }
warn() { echo "⚠  $*" >&2; }

# ── 0. kubectl sanity check ───────────────────────────────────────────────────
log "Confirming kubectl is pointed at '${CLUSTER_NAME}'..."
ACTUAL_CTX="$(kubectl config current-context 2>/dev/null || true)"
if [ -z "${ACTUAL_CTX}" ]; then
  warn "No kubectl context — run 'make kubeconfig' first."
  exit 1
fi

log "Waiting for system nodes to be Ready..."
kubectl wait node \
  --selector role=system \
  --for condition=Ready \
  --timeout=300s
ok "System nodes ready"

# ── 1. NVIDIA Device Plugin ───────────────────────────────────────────────────
# kubectl apply is itself idempotent — re-running diffs into a no-op when the
# manifest is unchanged.
log "Applying NVIDIA device plugin..."
kubectl apply -f "${REPO_ROOT}/k8s/addons/nvidia-device-plugin.yaml"
ok "NVIDIA device plugin applied (advertises nvidia.com/gpu once GPU nodes join)"

# ── 2. gp3 StorageClass (default) ─────────────────────────────────────────────
# The EKS managed EBS-CSI addon installs the driver but not a StorageClass.
# Some EKS releases ship `gp2` with `Immediate` binding mode, which breaks
# zone-aware scheduling for AZ-scoped EBS volumes. We ship gp3 with
# WaitForFirstConsumer so the init-weights Job's ephemeral PVC provisions
# in whichever AZ the pod schedules to. Marked default so any future
# StatefulSet using volumeClaimTemplates picks it up too.
#
# Before applying: if another StorageClass is currently marked default
# (e.g. EKS's legacy `gp2`), unset that annotation so we don't end up
# with two defaults (which makes K8s log a warning and pick non-deterministically).
log "Configuring gp3 StorageClass as the cluster default..."
for sc in $(kubectl get sc -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
  if [ "${sc}" != "gp3" ]; then
    log "  Removing is-default-class annotation from ${sc}..."
    kubectl annotate sc "${sc}" \
      storageclass.kubernetes.io/is-default-class- --overwrite >/dev/null
  fi
done
kubectl apply -f "${REPO_ROOT}/k8s/addons/gp3-storage-class.yaml"
ok "gp3 StorageClass applied (WaitForFirstConsumer, ebs.csi.aws.com)"

# ── 3. Cross-engine inference Service ─────────────────────────────────────────
# Headless Service that fronts every vLLM + SGLang pod the harness deploys
# (selector: app.kubernetes.io/component=inference). Pre-provisioned here so
# kinference and per-pod consumers can reach all workers via one DNS name
# regardless of which engines/models happen to be running.
log "Applying cross-engine inference Service..."
kubectl apply -f "${REPO_ROOT}/k8s/addons/inference-service.yaml"
ok "inference Service applied (headless; per-pod DNS at inference.default.svc.cluster.local)"

# ── 4. Mountpoint for Amazon S3 CSI Driver ────────────────────────────────────
log "Installing Mountpoint for Amazon S3 CSI driver..."

if ! helm repo list 2>/dev/null | grep -q '^aws-mountpoint-s3-csi-driver'; then
  helm repo add aws-mountpoint-s3-csi-driver \
    https://awslabs.github.io/mountpoint-s3-csi-driver
fi
helm repo update aws-mountpoint-s3-csi-driver >/dev/null

S3_CSI_VER=$(helm search repo aws-mountpoint-s3-csi-driver/aws-mountpoint-s3-csi-driver \
  --output json 2>/dev/null | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['version'] if r else '')" \
  2>/dev/null || echo "")

if [ -z "${S3_CSI_VER}" ]; then
  warn "Could not resolve aws-mountpoint-s3-csi-driver chart version — skipping"
  exit 1
fi

# Idempotent: helm upgrade --install is a no-op if the release is at the same
# version and chart already.
EXISTING_VER=$(helm list -n kube-system --filter '^aws-mountpoint-s3-csi-driver$' \
  --output json 2>/dev/null | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['chart'] if r else '')" \
  2>/dev/null || echo "")

if echo "${EXISTING_VER}" | grep -q "${S3_CSI_VER}$"; then
  skip "aws-mountpoint-s3-csi-driver already at ${EXISTING_VER}"
else
  log "Installing aws-mountpoint-s3-csi-driver ${S3_CSI_VER}..."
  helm upgrade --install aws-mountpoint-s3-csi-driver \
    aws-mountpoint-s3-csi-driver/aws-mountpoint-s3-csi-driver \
    --version "${S3_CSI_VER}" \
    --namespace kube-system \
    --wait --timeout 5m
  ok "aws-mountpoint-s3-csi-driver installed"
fi

# ── 5. Kubernetes Metrics Server ──────────────────────────────────────────────
# Provides the metrics.k8s.io API that powers `kubectl top pods/nodes` and
# Horizontal Pod Autoscaler. Not pre-installed on EKS.
#
# Two EKS-specific tweaks vs. the chart's defaults:
#   • --kubelet-insecure-tls — EKS kubelets serve their metrics endpoint with
#     self-signed certs. Without this flag Metrics Server refuses to scrape
#     them and the metrics.k8s.io API stays unavailable.
#   • nodeSelector role=system — pin to the system node group so we don't
#     burn GPU-node CPU on observability. (Wouldn't schedule on the tainted
#     GPU nodes anyway, but explicit pinning prevents accidental drift if
#     the GPU taint is ever relaxed.)
log "Installing Kubernetes Metrics Server..."

if ! helm repo list 2>/dev/null | grep -q '^metrics-server[[:space:]]'; then
  helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
fi
helm repo update metrics-server >/dev/null

METRICS_VER=$(helm search repo metrics-server/metrics-server \
  --output json 2>/dev/null | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['version'] if r else '')" \
  2>/dev/null || echo "")

if [ -z "${METRICS_VER}" ]; then
  warn "Could not resolve metrics-server chart version — skipping"
else
  EXISTING_VER=$(helm list -n kube-system --filter '^metrics-server$' \
    --output json 2>/dev/null | \
    python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['chart'] if r else '')" \
    2>/dev/null || echo "")

  if echo "${EXISTING_VER}" | grep -q "${METRICS_VER}$"; then
    skip "metrics-server already at ${EXISTING_VER}"
  else
    log "Installing metrics-server ${METRICS_VER}..."
    helm upgrade --install metrics-server metrics-server/metrics-server \
      --version "${METRICS_VER}" \
      --namespace kube-system \
      --set "args[0]=--kubelet-insecure-tls" \
      --set "nodeSelector.role=system" \
      --wait --timeout 5m
    ok "metrics-server installed (try: kubectl top nodes / kubectl top pods)"
  fi
fi

echo ""
ok "Addons installed. Next:"
echo "   make scale-gpu N=1"
echo "   make -f Makefile.server server-up CFG=servers/qwen-vllm.toml"
