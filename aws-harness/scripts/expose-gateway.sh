#!/usr/bin/env bash
# expose-gateway.sh — public NLB for a Gateway API data-plane Service.
#
# Three subcommands:
#
#   up        Provision a Service of type LoadBalancer (NLB) named
#             `<GATEWAY_SVC>-public` that targets the same pods as
#             $GATEWAY_SVC. All ports declared on the source Service
#             are forwarded as TCP (mirrored at apply time; no
#             hardcoded port list).
#   down      Delete the public Service. Polls AWS until the NLB
#             itself is fully removed (so a subsequent `make down` /
#             `terraform destroy` doesn't trip on a still-present NLB).
#   endpoint  Wait for the NLB hostname to be assigned, then print
#             `<host>:<port>` per exposed port + copy-pasteable curl
#             examples (scheme is unknown at L4 — the printout offers
#             http/https/h2c so you can pick).
#
# GATEWAY_SVC is REQUIRED — there is no default. The convention with
# Gateway API is that a controller (kgateway, Envoy Gateway, etc.)
# provisions a data-plane Service named `<gateway-name>-gatewayd` in
# response to each Gateway CR you apply. That's the Service to expose.
#
# Example:
#   GATEWAY_SVC=bllmp-gateway-gatewayd bash scripts/expose-gateway.sh up
#
# Required env (set by the Makefile):
#   CLUSTER_NAME   EKS cluster name
#   AWS_REGION     region
#   KUBECONFIG     kubeconfig file path
#   GATEWAY_SVC    data-plane Service name (required, no default)

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-model-harness}"
AWS_REGION="${AWS_REGION:-us-west-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATE="${REPO_ROOT}/k8s/expose/gateway-public.yaml.tmpl"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }
warn() { echo "⚠  $*" >&2; }

# GATEWAY_SVC — required, no default. Different clusters name their
# data-plane Services differently, and defaulting silently exposed
# the wrong thing (an internal control-plane, port 8443, etc.)
GATEWAY_SVC="${GATEWAY_SVC:-}"
if [ -z "${GATEWAY_SVC}" ]; then
  warn "GATEWAY_SVC is required."
  warn ""
  warn "Set it to the Gateway API data-plane Service name — the one the"
  warn "controller (kgateway / envoy-gateway / etc.) provisioned when"
  warn "you applied your Gateway CR. Convention: <gateway-name>-gatewayd."
  warn ""
  warn "Example:"
  warn "  GATEWAY_SVC=bllmp-gateway-gatewayd bash scripts/expose-gateway.sh up"
  exit 1
fi
# Public NLB Service name derived from the source Service so `unexpose`
# targets the same object `expose` created. Rename convention:
# `<source>-public`.
PUBLIC_SVC="${GATEWAY_SVC}-public"
NAMESPACE="${NAMESPACE:-default}"

cmd="${1:-up}"

case "${cmd}" in

  # ── up ──────────────────────────────────────────────────────────────────────
  up)
    # 1. LBC must be installed. The expose Service won't be reconciled
    #    without it, and we'd just leave a Pending Service forever.
    if ! kubectl -n kube-system get deploy aws-load-balancer-controller \
         >/dev/null 2>&1; then
      warn "AWS Load Balancer Controller is not installed."
      warn "Run 'make install-lbc' first."
      exit 1
    fi

    # 2. The user's gateway Service must exist — otherwise we'd render a
    #    Service with no selector that targets every pod in the namespace.
    if ! kubectl -n "${NAMESPACE}" get svc "${GATEWAY_SVC}" \
         >/dev/null 2>&1; then
      warn "Service '${GATEWAY_SVC}' not found in namespace '${NAMESPACE}'."
      warn "Deploy the gateway first, or set GATEWAY_SVC=<name> if it has"
      warn "a different name."
      exit 1
    fi

    # 3. Fetch the source Service once, then extract selector and ports
    #    from the same JSON blob (avoids two kubectl round trips and any
    #    read-your-write skew between them).
    log "Reading pod selector + ports from svc/${GATEWAY_SVC}..."
    SVC_JSON=$(kubectl -n "${NAMESPACE}" get svc "${GATEWAY_SVC}" -o json 2>/dev/null)
    if [ -z "${SVC_JSON}" ]; then
      warn "Failed to read svc/${GATEWAY_SVC}."
      exit 1
    fi

    # Selector as YAML-indented key/value pairs (4-space indent so it
    # slots cleanly under the template's `selector:` block).
    SELECTOR_YAML=$(echo "${SVC_JSON}" | python3 -c "
import sys, json
sel = (json.load(sys.stdin).get('spec') or {}).get('selector') or {}
if not sel:
    sys.stderr.write('ERROR: svc/${GATEWAY_SVC} has no selector\n')
    sys.exit(1)
for k, v in sel.items():
    print(f'    {k}: {v}')
")

    if [ -z "${SELECTOR_YAML}" ]; then
      warn "Failed to read selector from svc/${GATEWAY_SVC}."
      exit 1
    fi
    echo "${SELECTOR_YAML}" | sed 's/^/   /'

    # Ports mirrored from source svc.spec.ports as a YAML list. Each
    # entry emits name/port/targetPort/protocol on separate lines with
    # 4-space list-item indent + 6-space value indent, matching the
    # template's `  ports:` block. Protocol is forced to TCP: NLB
    # pass-through supports TCP and UDP, and the harness only exposes
    # TCP services (the source may declare UDP but we don't NLB it here).
    # `targetPort` may be a string (named port) or int — passed through
    # verbatim so both work.
    PORTS_YAML=$(echo "${SVC_JSON}" | python3 -c "
import sys, json
ports = (json.load(sys.stdin).get('spec') or {}).get('ports') or []
tcp = [p for p in ports if p.get('protocol', 'TCP') == 'TCP']
if not tcp:
    sys.stderr.write('ERROR: svc/${GATEWAY_SVC} has no TCP ports\n')
    sys.exit(1)
for p in tcp:
    port = p['port']
    target = p.get('targetPort', port)
    name = p.get('name', f'port-{port}')
    print(f'    - name: {name}')
    print(f'      port: {port}')
    print(f'      targetPort: {target}')
    print(f'      protocol: TCP')
")

    if [ -z "${PORTS_YAML}" ]; then
      warn "Failed to read ports from svc/${GATEWAY_SVC}."
      exit 1
    fi
    echo "${PORTS_YAML}" | sed 's/^/   /'

    # 4. Owner tag = git committer email, matching the harness's
    #    terraform default_tags convention. Fallback to "unknown" so
    #    the NLB still gets a tag value (just less informative).
    OWNER_TAG="$(git config user.email 2>/dev/null || echo unknown)"

    # 5. Render and apply. expandvars handles ${SELECTOR_YAML},
    #    ${PORTS_YAML}, ${PUBLIC_SVC}, and ${OWNER_TAG}; no envsubst
    #    dependency on macOS / Linux.
    log "Rendering and applying svc/${PUBLIC_SVC}..."
    SELECTOR_YAML="${SELECTOR_YAML}" \
    PORTS_YAML="${PORTS_YAML}" \
    PUBLIC_SVC="${PUBLIC_SVC}" \
    OWNER_TAG="${OWNER_TAG}" \
      python3 -c "import os,sys; print(os.path.expandvars(sys.stdin.read()))" \
      < "${TEMPLATE}" \
      | kubectl apply -f -

    echo ""
    ok "NLB provisioning started. Takes ~60-90s for the hostname to appear."
    echo "   make gateway-endpoint     # poll until ready, then print URLs"
    ;;

  # ── down ────────────────────────────────────────────────────────────────────
  down)
    # Capture the NLB DNS name BEFORE deleting the Service so we can
    # poll AWS for actual NLB removal. After `kubectl delete svc`, the
    # Service is gone immediately but the underlying NLB takes ~60s to
    # finish deleting — `terraform destroy` will hang if it tries to
    # remove the VPC while the NLB is still around.
    NLB_HOST=$(kubectl -n "${NAMESPACE}" get svc "${PUBLIC_SVC}" \
      -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null \
      || echo "")

    log "Deleting svc/${PUBLIC_SVC}..."
    kubectl -n "${NAMESPACE}" delete svc "${PUBLIC_SVC}" \
      --ignore-not-found

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
      warn "Service '${PUBLIC_SVC}' not found — run 'make expose-gateway' first."
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

    # Read the actual port list from the running gateway-public Service.
    # Mirrors whatever spec.ports has, which was mirrored from the source
    # Service at apply time — so this printout always matches reality.
    PORTS=$(kubectl -n "${NAMESPACE}" get svc "${PUBLIC_SVC}" \
      -o jsonpath='{range .spec.ports[*]}{.port}{"\n"}{end}')

    # NLB is L4 (TCP pass-through) — it doesn't actually know what
    # protocol/scheme the pod serves. We print `<host>:<port>` without
    # a scheme so we don't mislead. Common curl invocations for the
    # scheme-detection logic you'll do yourself:
    #
    #   Plain HTTP/1.1:    curl -i    http://<host>:<port>/
    #   HTTPS:             curl -ik   https://<host>:<port>/
    #   HTTP/2 cleartext:  curl -v --http2-prior-knowledge http://<host>:<port>/
    #   gRPC:              grpcurl -plaintext <host>:<port> list
    #
    # (Port number is only a weak hint; e.g. some services serve
    # plaintext on 8443, some serve gRPC on 8080, etc.)
    echo ""
    echo "  Public endpoints (NLB is L4 pass-through — scheme is up to the pod):"
    for port in ${PORTS}; do
      echo "    ${NLB_HOST}:${port}"
    done
    echo ""
    FIRST_PORT=$(echo "${PORTS}" | head -n1)
    echo "  Test — try in order until one gets past the handshake:"
    echo "    curl -i    http://${NLB_HOST}:${FIRST_PORT}/"
    echo "    curl -ik   https://${NLB_HOST}:${FIRST_PORT}/"
    echo "    curl -v --http2-prior-knowledge http://${NLB_HOST}:${FIRST_PORT}/"
    echo ""
    echo "  Note: AWS NLB DNS takes a minute or two after provisioning"
    echo "  before it resolves consistently from outside AWS. If 'curl'"
    echo "  hangs on the first attempt, retry in ~60s."
    ;;

  *)
    echo "Usage: $0 {up|down|endpoint}" >&2
    exit 1
    ;;
esac
