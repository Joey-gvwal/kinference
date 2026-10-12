#!/usr/bin/env bash
# install-monitoring.sh — idempotent install of kube-prometheus-stack plus
# the harness's PodMonitor + vLLM dashboards.
#
# What gets deployed (all in the `monitoring` namespace):
#   • Prometheus + Prometheus Operator (CRDs: PodMonitor, ServiceMonitor, …)
#   • Grafana with a sidecar that auto-loads ConfigMaps tagged
#     grafana_dashboard=1
#   • kube-state-metrics + node-exporter (K8s and node-level panels for free)
#   • A `PodMonitor` named `inference-pods` selecting every vLLM / SGLang
#     pod by `app.kubernetes.io/component=inference`
#   • A `ConfigMap/grafana-dashboard-vllm-*` per JSON in
#     k8s/monitoring/dashboards/, labeled `grafana_dashboard=1` so the
#     Grafana sidecar picks them up
#
# Reverse with `scripts/uninstall-monitoring.sh`. Expose to the browser
# via `scripts/expose-grafana.sh up` (requires `make install-lbc` first).
#
# Required env (set by the Makefile):
#   CLUSTER_NAME, AWS_REGION, KUBECONFIG

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-model-harness}"
AWS_REGION="${AWS_REGION:-us-west-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MONITORING_DIR="${REPO_ROOT}/k8s/monitoring"
DASHBOARDS_DIR="${MONITORING_DIR}/dashboards"

NAMESPACE="monitoring"
RELEASE="kube-prom-stack"
REPO_URL="https://prometheus-community.github.io/helm-charts"

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

# ── 1. Helm repo ──────────────────────────────────────────────────────────────
log "Updating prometheus-community Helm repo..."
if ! helm repo list 2>/dev/null | grep -q '^prometheus-community[[:space:]]'; then
  helm repo add prometheus-community "${REPO_URL}"
fi
helm repo update prometheus-community >/dev/null

# ── 2. Version detection + idempotent install ─────────────────────────────────
CHART_VER=$(helm search repo prometheus-community/kube-prometheus-stack \
  --output json 2>/dev/null \
  | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['version'] if r else '')" \
  2>/dev/null || echo "")

if [ -z "${CHART_VER}" ]; then
  warn "Could not resolve kube-prometheus-stack chart version — aborting."
  exit 1
fi

EXISTING_VER=$(helm list -n "${NAMESPACE}" --filter "^${RELEASE}\$" \
  --output json 2>/dev/null \
  | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['chart'] if r else '')" \
  2>/dev/null || echo "")

# ALWAYS upgrade. There used to be a skip here when the installed chart version
# already matched the resolved one — but that guard was on the CHART VERSION
# ALONE, so an edit to kube-prom-stack-values.yaml was silently a no-op
# whenever the chart was already current. It printed "already at
# kube-prometheus-stack-<ver>" and changed nothing, which is indistinguishable
# from success. That cost an afternoon: a raised Grafana memory limit appeared
# to be applied and was not.
#
# `helm upgrade --install` is itself idempotent, so there is nothing to
# optimize away — a run with no changes is fast. The only cost is a new
# revision number in `helm history`, which is a fair price for "the values
# file is what is actually deployed."
#
# Chart name from `helm list` shows as `kube-prometheus-stack-89.2.2`.
if echo "${EXISTING_VER}" | grep -q "${CHART_VER}\$"; then
  log "Re-applying values to ${RELEASE} (chart already at ${CHART_VER})..."
else
  log "Installing kube-prometheus-stack ${CHART_VER} into namespace ${NAMESPACE}..."
fi
helm upgrade --install "${RELEASE}" \
  prometheus-community/kube-prometheus-stack \
  --version "${CHART_VER}" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  -f "${MONITORING_DIR}/kube-prom-stack-values.yaml" \
  --wait --timeout 10m
ok "kube-prometheus-stack at ${CHART_VER}, values applied"

# ── 3. PodMonitor ─────────────────────────────────────────────────────────────
# Lives in the monitoring namespace (so Prometheus sees it via its
# namespaceSelector) but selects pods in `default` (where engine
# Deployments land per Makefile.server).
log "Applying inference-pods PodMonitor..."
kubectl apply -f "${MONITORING_DIR}/vllm-podmonitor.yaml"

# ── 4. Dashboard ConfigMaps ───────────────────────────────────────────────────
# One ConfigMap per JSON file. Labeled `grafana_dashboard=1` so the
# Grafana sidecar discovers and live-loads them. Re-running rewrites
# the ConfigMap data; the sidecar picks up the change without a
# Grafana restart.
if [ -d "${DASHBOARDS_DIR}" ] && ls "${DASHBOARDS_DIR}"/*.json >/dev/null 2>&1; then
  log "Loading Grafana dashboards from ${DASHBOARDS_DIR}..."
  for f in "${DASHBOARDS_DIR}"/*.json; do
    bn=$(basename "$f" .json)
    cm_name="grafana-dashboard-vllm-${bn//_/-}"
    # `kubectl create configmap --dry-run=client -o yaml` → label → apply.
    # Two-stage avoids a separate `kubectl label` round-trip and keeps
    # the apply declarative.
    kubectl create configmap "${cm_name}" \
      -n "${NAMESPACE}" \
      --from-file="${bn}.json=${f}" \
      --dry-run=client -o yaml \
      | python3 -c "
import sys, yaml
d = yaml.safe_load(sys.stdin)
d.setdefault('metadata', {}).setdefault('labels', {})['grafana_dashboard'] = '1'
d['metadata']['labels']['app.kubernetes.io/part-of'] = 'model-harness'
print(yaml.safe_dump(d))
" \
      | kubectl apply -f -
    echo "    ${cm_name}  ←  ${bn}.json"
  done
  ok "Dashboards applied (Grafana sidecar will pick them up within ~30s)"
else
  warn "No JSON files in ${DASHBOARDS_DIR} — skipping dashboard load."
fi

echo ""
ok "Monitoring stack ready. Next:"
echo "   make install-lbc           # if not already (prerequisite for expose-grafana)"
echo "   make expose-grafana        # provision a public NLB for Grafana"
echo "   make grafana-endpoint      # print the URL + admin password"
echo "   make uninstall-monitoring  # tear everything down"
