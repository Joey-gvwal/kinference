#!/usr/bin/env bash
# expose-grafana.sh — public NLB for the in-cluster Grafana from the
# kube-prometheus-stack release.
#
# Same up/down/endpoint shape as expose-gateway.sh — see that script for
# the design rationale (parallel Service vs. patching, polling AWS for
# NLB removal, etc.). Notable differences here:
#   • Single port (Grafana serves on 3000 inside the pod, exposed as 80
#     externally so users can browse to the bare NLB hostname).
#   • Selector is hardcoded against kube-prom-stack's standard Grafana
#     pod labels (confirmed via `helm template`), not pulled from a
#     live Service — there's no upstream Service we're shadowing in
#     the way expose-gateway shadows the user's `gateway`.
#   • `endpoint` additionally extracts the random admin password the
#     chart wrote to a Secret and prints it alongside the URL.
#
# Required env (set by the Makefile):
#   CLUSTER_NAME, AWS_REGION, KUBECONFIG

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-model-harness}"
AWS_REGION="${AWS_REGION:-us-west-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATE="${REPO_ROOT}/k8s/monitoring/grafana-public.yaml.tmpl"

NAMESPACE="monitoring"
RELEASE="kube-prom-stack"
PUBLIC_SVC="grafana-public"
GRAFANA_SECRET="${RELEASE}-grafana"   # chart-managed; key: admin-password

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }
warn() { echo "⚠  $*" >&2; }

cmd="${1:-up}"

case "${cmd}" in

  # ── up ──────────────────────────────────────────────────────────────────────
  up)
    # 1. LBC must be installed.
    if ! kubectl -n kube-system get deploy aws-load-balancer-controller \
         >/dev/null 2>&1; then
      warn "AWS Load Balancer Controller is not installed."
      warn "Run 'make install-lbc' first."
      exit 1
    fi

    # 2. Grafana pod must exist — otherwise the NLB Service would target
    #    nothing and we'd just have a Pending Service.
    if ! kubectl -n "${NAMESPACE}" get deploy "${RELEASE}-grafana" \
         >/dev/null 2>&1; then
      warn "Grafana Deployment '${RELEASE}-grafana' not found in '${NAMESPACE}'."
      warn "Run 'make install-monitoring' first."
      exit 1
    fi

    # 3. Owner tag for AWS resource attribution. Falls back to "unknown"
    #    so the NLB still gets tagged, just less informatively.
    OWNER_TAG="$(git config user.email 2>/dev/null || echo unknown)"

    # 4. Render and apply. expandvars handles ${OWNER_TAG} only; the
    #    selector is hardcoded in the template (kube-prom-stack's
    #    pod labels are stable across releases).
    log "Rendering and applying grafana-public Service..."
    OWNER_TAG="${OWNER_TAG}" \
      python3 -c "import os,sys; print(os.path.expandvars(sys.stdin.read()))" \
      < "${TEMPLATE}" \
      | kubectl apply -f -

    echo ""
    ok "NLB provisioning started. Takes ~60-90s for the hostname to appear."
    echo "   make grafana-endpoint     # poll until ready, then print URL + password"
    ;;

  # ── down ────────────────────────────────────────────────────────────────────
  down)
    NLB_HOST=$(kubectl -n "${NAMESPACE}" get svc "${PUBLIC_SVC}" \
      -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null \
      || echo "")

    log "Deleting svc/${PUBLIC_SVC}..."
    kubectl -n "${NAMESPACE}" delete svc "${PUBLIC_SVC}" --ignore-not-found

    if [ -z "${NLB_HOST}" ]; then
      ok "Service removed (no NLB hostname was recorded, nothing to poll)."
      exit 0
    fi

    log "Polling AWS until NLB '${NLB_HOST}' is fully deleted..."
    until [ -z "$(aws elbv2 describe-load-balancers \
            --region "${AWS_REGION}" \
            --query "LoadBalancers[?DNSName=='${NLB_HOST}'].LoadBalancerArn" \
            --output text 2>/dev/null)" ]; do
      printf '.'
      sleep 5
    done
    echo ""
    ok "NLB removed."
    ;;

  # ── endpoint ────────────────────────────────────────────────────────────────
  endpoint)
    if ! kubectl -n "${NAMESPACE}" get svc "${PUBLIC_SVC}" \
         >/dev/null 2>&1; then
      warn "Service '${PUBLIC_SVC}' not found — run 'make expose-grafana' first."
      exit 1
    fi

    log "Waiting for NLB hostname (Ctrl-C is safe)..."
    until kubectl -n "${NAMESPACE}" get svc "${PUBLIC_SVC}" \
          -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null \
          | grep -q .; do
      printf '.'
      sleep 5
    done
    echo ""

    NLB_HOST=$(kubectl -n "${NAMESPACE}" get svc "${PUBLIC_SVC}" \
      -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

    # Chart-generated admin password from the Secret. Falls back to a
    # placeholder if missing — should never happen on a successful
    # install but a clear marker beats an empty string.
    ADMIN_PASSWORD=$(kubectl -n "${NAMESPACE}" get secret "${GRAFANA_SECRET}" \
      -o jsonpath='{.data.admin-password}' 2>/dev/null \
      | base64 -d 2>/dev/null \
      || echo "<not-found>")

    echo ""
    echo "  Grafana URL:"
    echo "    http://${NLB_HOST}/"
    echo ""
    echo "  Login:"
    echo "    user:     admin"
    echo "    password: ${ADMIN_PASSWORD}"
    echo ""
    echo "  Dashboards loaded by the harness:"
    echo "    • vLLM Performance Statistics"
    echo "    • vLLM Query Statistics"
    echo "  (Browse → 'vLLM Monitoring' folder, or the General folder if"
    echo "  Grafana hasn't bucketed them yet.)"
    echo ""
    echo "  Note: AWS NLB DNS takes a minute or two after provisioning"
    echo "  before it resolves consistently from outside AWS. If the URL"
    echo "  doesn't load on the first try, retry in ~60s."
    ;;

  *)
    echo "Usage: $0 {up|down|endpoint}" >&2
    exit 1
    ;;
esac
