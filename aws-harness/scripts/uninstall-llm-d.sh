#!/usr/bin/env bash
# uninstall-llm-d.sh — fully removes everything install-llm-d.sh deployed.
#
# Safe to re-run on a half-installed cluster — every step uses
# --ignore-not-found / kubectl checks before acting.
#
# Inverse of install-llm-d.sh in reverse order:
#
#   1. NLB Service (llmd-gateway-public)   kubectl delete + poll AWS for NLB removal
#   2. Deployments + Svcs + ConfigMaps     both agentgateway-* deletions
#   3. GAIE InferencePool helm release(s)  helm uninstall (matches *-pool releases)
#   4. llm-d / GAIE CRDs                   kubectl delete (helm leaves these by design)
#   5. Leftover webhooks/clusterroles      kubectl delete by label
#   6. llmd namespace                      kubectl delete
#
# Old-shape leftovers (llm-d-infra release, agentgateway/agentgateway-crds
# helm releases, Gateway/HTTPRoute resources, agentgateway-system namespace)
# are also cleaned up — guarded by existence checks — so a user upgrading
# from the prior Gateway-API shape gets a clean transition.
#
# Called by `make uninstall-llm-d` and (guarded by namespace check) by
# scripts/cleanup-lbc-sgs.sh during `make down`.

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-model-harness}"
AWS_REGION="${AWS_REGION:-us-west-2}"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }
warn() { echo "⚠  $*" >&2; }

# Early-out if nothing was ever installed.
if ! kubectl get ns llmd >/dev/null 2>&1 \
   && ! kubectl get ns agentgateway-system >/dev/null 2>&1; then
  skip "llm-d not installed (neither llmd nor agentgateway-system namespace exists)"
  exit 0
fi

# ── 1. NLB Service (poll AWS until LB itself is gone) ────────────────────────
NLB_HOST=$(kubectl -n llmd get svc llmd-gateway-public \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null \
  || echo "")

if kubectl -n llmd get svc llmd-gateway-public >/dev/null 2>&1; then
  log "Deleting svc/llmd-gateway-public (forces NLB deletion)..."
  kubectl -n llmd delete svc llmd-gateway-public --ignore-not-found
else
  skip "llmd-gateway-public Service not present"
fi

if [ -n "${NLB_HOST}" ]; then
  log "Polling AWS until NLB '${NLB_HOST}' is fully deleted..."
  until [ -z "$(aws elbv2 describe-load-balancers \
          --region "${AWS_REGION}" \
          --query "LoadBalancers[?DNSName=='${NLB_HOST}'].LoadBalancerArn" \
          --output text 2>/dev/null)" ]; do
    printf '.'
    sleep 5
  done
  echo ""
  ok "NLB removed"
fi

# ── 2. Raw agentgateway resources (current shape) ────────────────────────────
log "Deleting agentgateway Deployments + Services + ConfigMaps..."
for name in agentgateway-default agentgateway-monitor; do
  kubectl -n llmd delete deployment "${name}" --ignore-not-found 2>/dev/null || true
  kubectl -n llmd delete service    "${name}" --ignore-not-found 2>/dev/null || true
done
kubectl -n llmd delete configmap agentgateway-default-config \
  --ignore-not-found 2>/dev/null || true
kubectl -n llmd delete configmap agentgateway-monitor-config \
  --ignore-not-found 2>/dev/null || true
ok "agentgateway raw resources deleted"

# ── 2b. Legacy Gateway-API shape (only matches if upgrading from old install) ─
log "Removing legacy Gateway-API resources if any..."
kubectl -n default delete httproute -l mm/managed-by=model-harness \
  --ignore-not-found 2>/dev/null || true
kubectl -n llmd delete httproute -l mm/managed-by=model-harness \
  --ignore-not-found 2>/dev/null || true
if helm list -n llmd --filter '^llm-d$' --output json 2>/dev/null \
   | grep -q '"llm-d"'; then
  log "  helm uninstall llm-d (legacy llm-d-infra release)..."
  helm uninstall llm-d -n llmd --wait 2>/dev/null || true
fi
if helm list -n agentgateway-system --filter '^agentgateway$' \
     --output json 2>/dev/null | grep -q '"agentgateway"'; then
  log "  helm uninstall agentgateway (legacy controller)..."
  helm uninstall agentgateway -n agentgateway-system --wait 2>/dev/null || true
fi
if helm list -n agentgateway-system --filter '^agentgateway-crds$' \
     --output json 2>/dev/null | grep -q '"agentgateway-crds"'; then
  log "  helm uninstall agentgateway-crds..."
  helm uninstall agentgateway-crds -n agentgateway-system --wait 2>/dev/null || true
fi

# ── 3. GAIE InferencePool helm release(s) ─────────────────────────────────────
log "Uninstalling GAIE InferencePool helm releases..."
GAIE_RELEASES=$(helm list -n default --output json 2>/dev/null \
  | python3 -c "
import sys, json
r = json.load(sys.stdin)
for h in r:
    if 'inferencepool' in (h.get('chart') or '').lower():
        print(h['name'])
" 2>/dev/null || echo "")

if [ -z "${GAIE_RELEASES}" ]; then
  skip "no GAIE InferencePool helm releases"
else
  while IFS= read -r release; do
    [ -z "${release}" ] && continue
    log "  helm uninstall ${release}"
    helm uninstall "${release}" -n default --wait 2>/dev/null || true
  done <<<"${GAIE_RELEASES}"
  ok "GAIE InferencePool releases uninstalled"
fi

# ── 4. llm-d / GAIE CRDs ──────────────────────────────────────────────────────
# Helm leaves CRDs in place by design. Delete the llm-d-specific ones.
# Skip Gateway API CRDs (upstream, potentially used by other workloads).
log "Deleting llm-d / GAIE-specific CRDs..."
for crd in \
    inferencepools.inference.networking.k8s.io \
    inferenceobjectives.llm.llm-d.ai \
    inferencemodelrewrites.llm.llm-d.ai \
    ; do
  if kubectl get crd "${crd}" >/dev/null 2>&1; then
    if ! timeout 30 kubectl delete crd "${crd}" --ignore-not-found 2>/dev/null; then
      warn "CRD ${crd} stuck (likely a finalizer) — force-removing finalizers"
      kubectl patch crd "${crd}" --type=merge \
        -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
      kubectl delete crd "${crd}" --ignore-not-found 2>/dev/null || true
    fi
  fi
done
ok "llm-d / GAIE CRDs deleted"

# ── 5. Leftover webhooks ──────────────────────────────────────────────────────
log "Cleaning up llm-d ValidatingWebhookConfigurations (if any)..."
kubectl delete validatingwebhookconfigurations \
  -l app.kubernetes.io/part-of=llm-d --ignore-not-found 2>/dev/null || true
kubectl delete mutatingwebhookconfigurations \
  -l app.kubernetes.io/part-of=llm-d --ignore-not-found 2>/dev/null || true

# ── 6. Namespaces ─────────────────────────────────────────────────────────────
log "Deleting namespaces..."
kubectl delete ns llmd agentgateway-system --ignore-not-found 2>/dev/null || true
ok "Namespaces deleted (deletion is async — may take ~30s to finalize)"

echo ""
ok "llm-d uninstalled."
echo ""
echo "  Verify clean state:"
echo "    kubectl get ns | grep -E 'llmd|agentgateway' || echo '  (none)'"
echo "    kubectl get crd | grep -E 'llm-d|inference\\.networking' || echo '  (none)'"
echo ""
echo "  Gateway API CRDs were left in place (other workloads may use them)."
