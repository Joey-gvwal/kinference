#!/usr/bin/env bash
# uninstall-monitoring.sh — reverse install-monitoring.sh in dependency
# order so `terraform destroy` (or `make down`) isn't blocked by a
# still-present NLB or stale CRD-backed objects.
#
# Order (each step safe to re-run):
#   1. Tear down the Grafana NLB if any — polls AWS until the LB is
#      actually deleted, matching the expose-gateway down pattern.
#   2. Delete the harness-added objects (PodMonitor + dashboard CMs).
#      These are CR's backed by the operator's CRDs; deleting them
#      before the helm uninstall avoids finalizer hangs.
#   3. helm uninstall kube-prom-stack.
#   4. Delete the monitoring namespace.
#
# CRDs (monitoring.coreos.com/*) are intentionally kept — they're
# upstream Prometheus-operator CRDs that future workloads may want.
# A user wanting truly zero residue can run:
#   kubectl get crd | grep monitoring.coreos.com | awk '{print $1}' | xargs kubectl delete crd

set -euo pipefail

AWS_REGION="${AWS_REGION:-us-west-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

NAMESPACE="monitoring"
RELEASE="kube-prom-stack"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }
warn() { echo "⚠  $*" >&2; }

# Short-circuit: nothing to uninstall if the namespace doesn't exist.
if ! kubectl get ns "${NAMESPACE}" >/dev/null 2>&1; then
  skip "Namespace '${NAMESPACE}' not present — nothing to uninstall."
  exit 0
fi

# ── 1. Grafana NLB ────────────────────────────────────────────────────────────
if kubectl -n "${NAMESPACE}" get svc grafana-public >/dev/null 2>&1; then
  log "Tearing down Grafana NLB first..."
  bash "${SCRIPT_DIR}/expose-grafana.sh" down
fi

# ── 2. Harness-added CRs ──────────────────────────────────────────────────────
log "Deleting PodMonitor + dashboard ConfigMaps..."
kubectl -n "${NAMESPACE}" delete podmonitor inference-pods --ignore-not-found
kubectl -n "${NAMESPACE}" delete configmap \
  -l 'grafana_dashboard=1,app.kubernetes.io/part-of=model-harness' \
  --ignore-not-found

# ── 3. helm uninstall ─────────────────────────────────────────────────────────
if helm list -n "${NAMESPACE}" --filter "^${RELEASE}\$" --output json 2>/dev/null \
    | grep -q "\"${RELEASE}\""; then
  log "Uninstalling helm release ${RELEASE}..."
  helm uninstall "${RELEASE}" -n "${NAMESPACE}" --wait --timeout 5m \
    || warn "helm uninstall returned non-zero — continuing with cleanup."
else
  skip "helm release ${RELEASE} not present"
fi

# ── 4. Namespace ──────────────────────────────────────────────────────────────
log "Deleting namespace ${NAMESPACE}..."
kubectl delete namespace "${NAMESPACE}" --ignore-not-found --timeout=2m \
  || warn "Namespace delete timed out — inspect with 'kubectl get all -n ${NAMESPACE}'."

echo ""
ok "Monitoring stack removed."
echo "   The Prometheus-operator CRDs (PodMonitor, ServiceMonitor, etc.)"
echo "   are intentionally kept. Remove manually if desired:"
echo "     kubectl get crd | awk '/monitoring.coreos.com/ {print \$1}' | xargs kubectl delete crd"
