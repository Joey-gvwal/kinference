#!/usr/bin/env bash
# install-llm-d.sh — install two raw agentgateway Deployments backed by
# the harness's vLLM workers, each pointing at a different EPP:
#
#   agentgateway-default  (port 8081 inside pod, agw-default)
#                            ↓
#               GAIE inference scheduler  (qwen-vllm-pool-epp:9002)
#                            ↓
#                       vLLM workers
#
#   agentgateway-monitor  (port 8082 inside pod, agw-monitor)
#                            ↓
#               kinference ext_proc      (kinference:50052)
#                            ↓
#                       vLLM workers
#
# A single LoadBalancer Service (llmd-gateway-public) fronts both pods
# via named-port targeting — NLB :8081 routes to the default agentgateway,
# :8082 routes to the monitor agentgateway.
#
# Idempotent layered install (re-runs are no-ops once each layer is at
# the target version):
#
#   1. Sanity checks                       kubectl ctx, LBC, target vLLM server exists
#   2. GAIE InferencePool CRD              kubectl apply
#   3. GAIE InferencePool chart            helm OCI, namespace default
#                                          (deploys the EPP scheduler we point the default agentgateway at)
#   4. llmd namespace                      kubectl create --ignore-not-found
#   5. ConfigMap: agentgateway-default     rendered from template, GAIE_EPP_SVC substituted
#   6. ConfigMap: agentgateway-monitor     rendered from template (no substitution)
#   7. Deployment + Svc: agentgateway-default   rendered from agentgateway-deployment.yaml.tmpl
#   8. Deployment + Svc: agentgateway-monitor   rendered from same template, different vars
#   9. Wait for both Deployments Ready
#  10. NLB Service (llmd-gateway-public)   rendered + applied
#
# Required env (Makefile sets these):
#   CLUSTER_NAME   EKS cluster name
#   AWS_REGION     region
#   KUBECONFIG     kubeconfig file path
# Optional:
#   SERVER_NAME    mm/server-name label to route to (default: qwen-3-6-vllm).
#                  Names the GAIE pool's release as ${SERVER_NAME}-pool so
#                  the EPP Service becomes ${SERVER_NAME}-pool-epp.

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-model-harness}"
AWS_REGION="${AWS_REGION:-us-west-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATES_DIR="${REPO_ROOT}/k8s/llm-d"

SERVER_NAME="${SERVER_NAME:-qwen-3-6-vllm}"

# Pinned versions.
GAIE_VER="v1.4.0"

POOL_NAME="${POOL_NAME:-${SERVER_NAME}-pool}"
GAIE_EPP_SVC="${POOL_NAME}-epp"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }
warn() { echo "⚠  $*" >&2; }

# ── 1. Sanity checks ──────────────────────────────────────────────────────────
log "Sanity-checking cluster state..."

ACTUAL_CTX="$(kubectl config current-context 2>/dev/null || true)"
if [ -z "${ACTUAL_CTX}" ]; then
  warn "No kubectl context — run 'make kubeconfig' first."
  exit 1
fi

if ! kubectl -n kube-system get deploy aws-load-balancer-controller \
     >/dev/null 2>&1; then
  warn "AWS Load Balancer Controller is not installed."
  warn "Run 'make install-lbc' first — the NLB exposure depends on it."
  exit 1
fi

if ! kubectl -n default get deploy kinference >/dev/null 2>&1; then
  warn "kinference Deployment not found in default namespace."
  warn "The agentgateway-monitor flavour points at kinference.default.svc:50052."
  warn "Deploy kinference first (cd k8s/kinference && make deploy TAG=…)."
  exit 1
fi

# Two-stage pod check: server-name labels and mm/engine=vllm.
POD_INFO=$(kubectl get pods -n default \
  -l "mm/server-name=${SERVER_NAME}" \
  -o jsonpath='{range .items[*]}{.metadata.name}{" mm/engine="}{.metadata.labels.mm/engine}{"\n"}{end}' \
  2>/dev/null)

if [ -z "${POD_INFO}" ]; then
  warn "No pods found with mm/server-name=${SERVER_NAME}."
  warn "Deploy at least one vLLM server first:"
  warn "  make -f Makefile.server server-up CFG=servers/${SERVER_NAME}.toml"
  warn "Or override SERVER_NAME=… to point at a different deployed server."
  exit 1
fi

if ! echo "${POD_INFO}" | grep -q 'mm/engine=vllm$'; then
  warn "Found ${SERVER_NAME} pods, but none carry mm/engine=vllm:"
  echo "${POD_INFO}" | sed 's/^/      /' >&2
  warn "Redeploy the server to pick up the current chart labels:"
  warn "  make -f Makefile.server server-down CFG=servers/${SERVER_NAME}.toml"
  warn "  make -f Makefile.server server-up   CFG=servers/${SERVER_NAME}.toml"
  exit 1
fi

ok "Found vLLM pods for mm/server-name=${SERVER_NAME}:"
echo "${POD_INFO}" | sed 's/^/      /'

# ── 2. GAIE InferencePool CRD ─────────────────────────────────────────────────
log "Applying GAIE InferencePool CRD (${GAIE_VER})..."
kubectl apply -f \
  "https://raw.githubusercontent.com/kubernetes-sigs/gateway-api-inference-extension/${GAIE_VER}/config/crd/bases/inference.networking.k8s.io_inferencepools.yaml"
kubectl wait --for=condition=Established \
  crd/inferencepools.inference.networking.k8s.io \
  --timeout=60s
ok "InferencePool CRD established"

# ── 3. GAIE InferencePool chart (deploys the default EPP scheduler) ───────────
GAIE_EXISTING=$(helm list -n default --filter "^${POOL_NAME}$" \
  --output json 2>/dev/null | \
  python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['chart'] if r else '')" \
  2>/dev/null || echo "")

if echo "${GAIE_EXISTING}" | grep -q "${GAIE_VER}$"; then
  skip "InferencePool ${POOL_NAME} already at ${GAIE_EXISTING}"
else
  log "Rendering gaie-values.yaml.tmpl (server=${SERVER_NAME})..."
  GAIE_VALUES=$(mktemp)
  SERVER_NAME="${SERVER_NAME}" \
    python3 -c "import os,sys; print(os.path.expandvars(sys.stdin.read()))" \
    < "${TEMPLATES_DIR}/gaie-values.yaml.tmpl" > "${GAIE_VALUES}"

  log "Installing InferencePool ${POOL_NAME} ${GAIE_VER}..."
  helm upgrade --install "${POOL_NAME}" \
    oci://registry.k8s.io/gateway-api-inference-extension/charts/inferencepool \
    --version "${GAIE_VER}" \
    --namespace default \
    -f "${GAIE_VALUES}" \
    --wait --timeout 5m
  rm -f "${GAIE_VALUES}"
  ok "InferencePool ${POOL_NAME} installed (EPP Service: ${GAIE_EPP_SVC})"
fi

# Verify the EPP Service the default agentgateway will dial actually exists.
if ! kubectl -n default get svc "${GAIE_EPP_SVC}" >/dev/null 2>&1; then
  warn "Expected GAIE EPP Service '${GAIE_EPP_SVC}' not found in default namespace."
  warn "List of services there:"
  kubectl -n default get svc >&2
  exit 1
fi

# ── 3b. Patch the EPP args to force plaintext gRPC ────────────────────────────
# WHY THIS PATCH EXISTS:
#   The GAIE chart's `inferenceExtension.flags` map renders each flag as
#   TWO separate args: `--secure-serving` and `false`. The EPP binary's
#   pflag-based CLI parses boolean flags differently from non-boolean:
#     • `--bool=value`  → sets bool to value
#     • `--bool value`  → sets bool to TRUE; `value` becomes a positional arg
#   So the chart's `flags.secure-serving: false` is silently ignored — the
#   EPP starts with secure-serving=true and serves TLS, which agentgateway
#   v1.3.0-alpha.1 has no YAML config knob to dial (we tested every
#   permutation of backendTLS placement locally — none affects the
#   ext_proc connection).
#
# This patch replaces the EPP's container args with the `=`-joined form,
# which pflag correctly parses. After the patch the EPP serves plaintext
# gRPC and agentgateway's default extProc dial Just Works.
#
# Idempotent: re-running yields the same args set, kubectl no-ops.
log "Patching EPP deployment args to use --secure-serving=false (joined form)..."
kubectl -n default patch deploy "${GAIE_EPP_SVC}" --type='json' -p='[
  {"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": [
    "--pool-name='${POOL_NAME}'",
    "--pool-namespace=default",
    "--pool-group=inference.networking.k8s.io",
    "--zap-encoder=json",
    "--config-file=/config/default-plugins.yaml",
    "--secure-serving=false",
    "--v=1",
    "--tracing=false"
  ]}
]' >/dev/null
kubectl -n default rollout status deploy/"${GAIE_EPP_SVC}" --timeout=2m
ok "EPP rolled with plaintext gRPC enabled"

# Verify the patch landed and the new pod logs secure-serving=false:
if ! kubectl -n default logs deploy/"${GAIE_EPP_SVC}" --tail=200 2>/dev/null \
     | grep -q '"secure-serving":false'; then
  warn "EPP startup log doesn't show secure-serving:false — the patch may not have taken effect."
  warn "Check manually: kubectl logs deploy/${GAIE_EPP_SVC} | grep secure-serving"
  # Don't exit — let the user inspect; agentgateway will still fail downstream if it didn't.
fi

# ── 4. llmd namespace ─────────────────────────────────────────────────────────
log "Ensuring llmd namespace exists..."
kubectl create namespace llmd --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# ── 5+6. ConfigMaps (default + monitor) ──────────────────────────────────────
log "Applying agentgateway-default ConfigMap (EPP=${GAIE_EPP_SVC}.default:9002)..."
GAIE_EPP_SVC="${GAIE_EPP_SVC}" \
  python3 -c "import os,sys; print(os.path.expandvars(sys.stdin.read()))" \
  < "${TEMPLATES_DIR}/agentgateway-default-config.yaml.tmpl" \
  | kubectl apply -f -

log "Applying agentgateway-monitor ConfigMap (EPP=kinference.default:50052)..."
kubectl apply -f "${TEMPLATES_DIR}/agentgateway-monitor-config.yaml.tmpl"

ok "Both agentgateway ConfigMaps applied"

# ── 7+8. Deployments + Services (default + monitor) ───────────────────────────
apply_agentgateway() {
  local name="$1" configmap="$2" port="$3" port_name="$4"
  log "Applying ${name} Deployment + Service (port=${port}, name=${port_name})..."
  NAME="${name}" CONFIGMAP="${configmap}" \
    CONTAINER_PORT="${port}" CONTAINER_PORT_NAME="${port_name}" \
    python3 -c "import os,sys; print(os.path.expandvars(sys.stdin.read()))" \
    < "${TEMPLATES_DIR}/agentgateway-deployment.yaml.tmpl" \
    | kubectl apply -f -
}

apply_agentgateway agentgateway-default \
                   agentgateway-default-config \
                   8081 agw-default

apply_agentgateway agentgateway-monitor \
                   agentgateway-monitor-config \
                   8082 agw-monitor

# ── 9. Wait for both Deployments Ready ────────────────────────────────────────
log "Waiting for both agentgateway Deployments to roll out..."
kubectl -n llmd rollout status deployment/agentgateway-default --timeout=3m
kubectl -n llmd rollout status deployment/agentgateway-monitor --timeout=3m
ok "Both agentgateway Deployments are Ready"

# ── 10. NLB Service ───────────────────────────────────────────────────────────
OWNER_TAG="$(git config user.email 2>/dev/null || echo unknown)"

log "Applying llmd-gateway-public Service (NLB, two listener ports)..."
OWNER_TAG="${OWNER_TAG}" \
  python3 -c "import os,sys; print(os.path.expandvars(sys.stdin.read()))" \
  < "${TEMPLATES_DIR}/llmd-gateway-public.yaml.tmpl" \
  | kubectl apply -f -

ok "llmd-gateway-public applied — NLB provisioning starts now."

echo ""
ok "llm-d ready. Next:"
echo "   make llmd-endpoint       # wait for NLB hostname, print URLs for both ports"
echo "   make uninstall-llm-d     # tear it all down"
