#!/usr/bin/env bash
# expose.sh — Toggle the inference API between public (AWS ALB) and private.
#
# Usage:
#   expose.sh public  [CIDR]   — create ALB Ingress + HTTPRoute, annotate HTTPS cert
#   expose.sh private          — delete ALB Ingress + HTTPRoute, remove Route 53 record
#   expose.sh endpoint         — upsert Route 53 ALIAS, print stable URL
#
# DNS / TLS:
#   When the Route 53 hosted zone for LINKY_DOMAIN exists and the ACM cert is
#   ISSUED, expose.sh automatically:
#     public   — annotates the Ingress with the cert ARN + HTTPS redirect
#     endpoint — upserts an ALIAS record pointing LINKY_DOMAIN at the ALB
#     private  — deletes the ALIAS record
#
#   Run `make dns` once to provision the zone and cert.  All DNS actions are
#   no-ops if the zone / cert are not present.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-wm-inference}"
AWS_REGION="${AWS_REGION:-us-west-2}"
LINKY_DOMAIN="${LINKY_DOMAIN:-drlinky.buoyant.cloud}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

INGRESS_MANIFEST="${REPO_ROOT}/k8s/expose/public-ingress.yaml"
INGRESS_NAME="inference-public"
INGRESS_NAMESPACE="llmd"
HTTPROUTE_NS="default"

# Cluster-specific settings derived from CLUSTER_NAME.
case "${CLUSTER_NAME}" in
  wm-inf-31b)
    MODEL_NAME="google/gemma-4-31B-it"
    HTTPROUTE_MANIFEST="${REPO_ROOT}/k8s/expose/httproute-31b.yaml"
    HTTPROUTE_NAME="gemma-4-31b-route"
    ;;
  wm-inf-31b-awq|wm-inf-31b-v2)
    # Parallel v2 of the AWQ cluster reuses the same model + HTTPRoute manifest.
    MODEL_NAME="google/gemma-4-31B-it"
    HTTPROUTE_MANIFEST="${REPO_ROOT}/k8s/expose/httproute-31b-awq.yaml"
    HTTPROUTE_NAME="gemma-4-31b-awq-route"
    ;;
  *)
    MODEL_NAME="cyankiwi/gemma-4-26B-A4B-it-AWQ-4bit"
    HTTPROUTE_MANIFEST="${REPO_ROOT}/k8s/expose/httproute.yaml"
    HTTPROUTE_NAME="gemma-4-26b-route"
    ;;
esac

cmd="${1:-}"
allowed_cidr="${2:-0.0.0.0/0}"

# ── DNS helpers ───────────────────────────────────────────────────────────────

# Returns the Route 53 hosted zone ID for LINKY_DOMAIN, or empty string.
dns_zone_id() {
  aws route53 list-hosted-zones-by-name \
    --dns-name "${LINKY_DOMAIN}." \
    --query "HostedZones[?Name=='${LINKY_DOMAIN}.'].Id" \
    --output text 2>/dev/null | sed 's|/hostedzone/||' || echo ""
}

# Returns the ISSUED ACM certificate ARN for LINKY_DOMAIN, or empty string.
dns_cert_arn() {
  aws acm list-certificates \
    --region "${AWS_REGION}" \
    --certificate-statuses ISSUED \
    --query "CertificateSummaryList[?DomainName=='${LINKY_DOMAIN}'].CertificateArn" \
    --output text 2>/dev/null | head -1 || echo ""
}

# Returns the canonical hosted zone ID for the given ALB DNS name.
alb_zone_id() {
  local alb_host="$1"
  aws elbv2 describe-load-balancers \
    --region "${AWS_REGION}" \
    --query "LoadBalancers[?DNSName=='${alb_host}'].CanonicalHostedZoneId" \
    --output text 2>/dev/null || echo ""
}

# Upserts a Route 53 ALIAS A record: LINKY_DOMAIN → ALB.
dns_upsert() {
  local zone_id="$1" alb_host="$2" alb_zone="$3"
  aws route53 change-resource-record-sets \
    --hosted-zone-id "${zone_id}" \
    --change-batch "{
      \"Changes\": [{
        \"Action\": \"UPSERT\",
        \"ResourceRecordSet\": {
          \"Name\": \"${LINKY_DOMAIN}\",
          \"Type\": \"A\",
          \"AliasTarget\": {
            \"HostedZoneId\": \"${alb_zone}\",
            \"DNSName\": \"${alb_host}\",
            \"EvaluateTargetHealth\": true
          }
        }
      }]
    }" > /dev/null 2>&1
}

# Deletes the Route 53 ALIAS record ONLY if it currently points at this
# cluster's ALB.  This prevents `make private` on a retiring cluster from
# blowing away DNS that's already been flipped to a newer cluster.
#
# Call as: dns_delete "$zone_id" "$our_alb_host"
# If $our_alb_host is empty (our ingress already gone / unknown), the delete
# is skipped — safer to leak a DNS record than to take down a live service.
dns_delete() {
  local zone_id="$1" our_alb="$2"
  local record target

  record=$(aws route53 list-resource-record-sets \
    --hosted-zone-id "${zone_id}" \
    --query "ResourceRecordSets[?Name=='${LINKY_DOMAIN}.'] | [0]" \
    --output json 2>/dev/null || echo "null")

  if [ "${record}" = "null" ] || ! echo "${record}" | grep -q "AliasTarget"; then
    return 0
  fi

  if [ -z "${our_alb}" ]; then
    echo "–  Skipping Route 53 ALIAS delete — can't determine this cluster's ALB."
    echo "   If this cluster really owns the record, delete manually in the AWS console."
    return 0
  fi

  # Route 53 returns AliasTarget.DNSName with a trailing dot; ingress hostname
  # does not.  Normalise by stripping the trailing dot from the record side.
  target=$(echo "${record}" | python3 -c "
import sys, json
r = json.load(sys.stdin)
print((r.get('AliasTarget', {}).get('DNSName') or '').rstrip('.'))
" 2>/dev/null || echo "")

  if [ "${target}" != "${our_alb}" ]; then
    echo "–  Route 53 ALIAS (${LINKY_DOMAIN}) points at ${target:-<unknown>}, not this cluster's ALB (${our_alb}) — leaving it alone."
    return 0
  fi

  aws route53 change-resource-record-sets \
    --hosted-zone-id "${zone_id}" \
    --change-batch "{\"Changes\":[{\"Action\":\"DELETE\",\"ResourceRecordSet\":${record}}]}" \
    > /dev/null 2>&1 && echo "✓  Route 53 ALIAS removed (${LINKY_DOMAIN} → ${our_alb})" || true
}

case "$cmd" in

  # ── public ──────────────────────────────────────────────────────────────────
  public)
    echo "▶  Exposing inference API via AWS ALB..."

    if [ "$allowed_cidr" = "0.0.0.0/0" ]; then
      echo ""
      echo "  ⚠  WARNING: The API will be open to the entire internet."
      echo "     To restrict access: make public ALLOWED_CIDR=x.x.x.x/32"
      echo ""
    else
      echo "  Restricting access to: $allowed_cidr"
    fi

    echo "▶  Applying HTTPRoute (InferencePool-backed)..."
    kubectl apply -f "${HTTPROUTE_MANIFEST}"

    kubectl apply -f "$INGRESS_MANIFEST"

    # Patch in HTTPS + cert if the ACM cert is already issued.
    CERT_ARN=$(dns_cert_arn)
    if [ -n "${CERT_ARN}" ]; then
      kubectl annotate ingress "$INGRESS_NAME" -n "$INGRESS_NAMESPACE" \
        "alb.ingress.kubernetes.io/certificate-arn=${CERT_ARN}" \
        "alb.ingress.kubernetes.io/listen-ports=[{\"HTTP\":80},{\"HTTPS\":443}]" \
        "alb.ingress.kubernetes.io/ssl-redirect=443" \
        --overwrite
      echo "✓  HTTPS configured (${LINKY_DOMAIN})"
    else
      echo "  (ACM cert not yet issued — HTTP only; re-run after 'make dns' + NS delegation)"
    fi

    # Patch in the CIDR restriction annotation if specified.
    if [ "$allowed_cidr" != "0.0.0.0/0" ]; then
      kubectl annotate ingress "$INGRESS_NAME" -n "$INGRESS_NAMESPACE" \
        "alb.ingress.kubernetes.io/inbound-cidrs=${allowed_cidr}" \
        --overwrite
    fi

    echo ""
    echo "✓  ALB provisioning started. Takes ~2 minutes."
    echo "   Run 'make endpoint' to get the public URL once it's ready."
    ;;

  # ── private ─────────────────────────────────────────────────────────────────
  private)
    echo "▶  Removing public ALB and HTTPRoute..."

    # Resolve our ALB hostname BEFORE we start deleting things — dns_delete
    # uses it to verify the Route 53 record actually points at this cluster
    # before removing it.  If the ingress is already gone, ALB_HOST stays
    # empty and dns_delete safely bails out.
    ALB_HOST=$(kubectl get ingress "$INGRESS_NAME" -n "$INGRESS_NAMESPACE" \
      -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")

    # Delete Route 53 ALIAS record before the ALB disappears — but only if
    # it still points at us.
    ZONE_ID=$(dns_zone_id)
    if [ -n "${ZONE_ID}" ]; then
      dns_delete "${ZONE_ID}" "${ALB_HOST}"
    fi

    kubectl delete httproute "${HTTPROUTE_NAME}" -n "${HTTPROUTE_NS}" 2>/dev/null || true
    kubectl delete -f "$INGRESS_MANIFEST" --wait=false 2>/dev/null || true
    kubectl delete service inference-public-lb -n llmd 2>/dev/null || true

    if [ -n "$ALB_HOST" ]; then
      echo "▶  Waiting for ALB to be deleted in AWS (this takes ~1 minute)..."
      until [ -z "$(aws elbv2 describe-load-balancers \
          --region "${AWS_REGION}" \
          --query "LoadBalancers[?DNSName=='${ALB_HOST}'].LoadBalancerArn" \
          --output text 2>/dev/null)" ]; do
        printf '.'
        sleep 5
      done
      echo " done"
    fi

    echo "✓  ALB removed. API is internal-only (port-forward to access)."
    ;;

  # ── endpoint ────────────────────────────────────────────────────────────────
  endpoint)
    echo "▶  Waiting for ALB hostname (Ctrl-C to abort)..."

    until kubectl get ingress "$INGRESS_NAME" -n "$INGRESS_NAMESPACE" \
      -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null \
      | grep -q .; do
      printf '.'
      sleep 5
    done
    echo ""

    ALB_HOST=$(kubectl get ingress "$INGRESS_NAME" -n "$INGRESS_NAMESPACE" \
      -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

    # Upsert Route 53 ALIAS if the hosted zone exists.
    ZONE_ID=$(dns_zone_id)
    if [ -n "${ZONE_ID}" ]; then
      ALB_ZONE=$(alb_zone_id "${ALB_HOST}")
      if [ -n "${ALB_ZONE}" ]; then
        dns_upsert "${ZONE_ID}" "${ALB_HOST}" "${ALB_ZONE}"
        echo "✓  Route 53: ${LINKY_DOMAIN} → ${ALB_HOST}"
      fi
    fi

    # Determine the public-facing URL.
    CERT_ARN=$(dns_cert_arn)
    if [ -n "${CERT_ARN}" ] && [ -n "${ZONE_ID}" ]; then
      ENDPOINT="https://${LINKY_DOMAIN}"
    else
      ENDPOINT="http://${ALB_HOST}"
    fi

    echo ""
    echo "  Public endpoint:"
    echo "    ${ENDPOINT}/v1/chat/completions"
    echo ""
    echo "  Test:"
    echo "    curl ${ENDPOINT}/model/v1/chat/completions \\"
    echo "      -H 'Content-Type: application/json' \\"
    echo "      -d '{\"model\":\"${MODEL_NAME}\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello! Reply in one sentence.\"}],\"max_tokens\":50}'"
    echo ""
    echo "  Linky chatbot: ${ENDPOINT}/linky"
    echo ""
    ;;

  *)
    echo "Usage: $0 {public [CIDR]|private|endpoint}"
    exit 1
    ;;
esac
