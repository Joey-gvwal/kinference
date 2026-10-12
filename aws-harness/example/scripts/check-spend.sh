#!/usr/bin/env bash
# check-spend.sh
# Checks whether any tagged AWS resources are in a state that costs money.
# Safe to run at any time — read-only, no changes made.
#
# Exit codes:
#   0 — nothing billable found
#   1 — one or more billable resources detected

set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT="${PROJECT:-inference}"
# CLUSTER_NAME is the EKS cluster name — may differ from PROJECT tag value.
# e.g. PROJECT=inference, CLUSTER_NAME=wm-inference
CLUSTER_NAME="${CLUSTER_NAME:-${PROJECT}}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
RESET='\033[0m'

warn()  { echo -e "${YELLOW}  ⚠  $*${RESET}"; }
ok()    { echo -e "${GREEN}  ✓  $*${RESET}"; }
bad()   { echo -e "${RED}  ✗  $*${RESET}"; }
info()  { echo -e "     $*"; }

FOUND_SPEND=0

echo ""
echo -e "${BOLD}AWS spend check — Project=${PROJECT}, Cluster=${CLUSTER_NAME}, Region=${AWS_REGION}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════${RESET}"
echo ""
echo "Note: the tag index lags by minutes to hours after deletion."
echo "This script checks actual resource state, not the tag cache."
echo ""

# ── Fetch all tagged resource ARNs ────────────────────────────────────────────
echo -e "${BOLD}[1/5] Fetching tagged resources...${RESET}"
TAGGED=$(aws resourcegroupstaggingapi get-resources \
  --tag-filters "Key=Project,Values=${PROJECT}" \
  --region "${AWS_REGION}" \
  --query 'ResourceTagMappingList[*].ResourceARN' \
  --output text 2>/dev/null || echo "")

if [ -z "$TAGGED" ]; then
  ok "Tag index is empty — no tagged resources found (tag index can lag 30+ min after creation)."
  echo ""
else
  info "Tag index returned $(echo "$TAGGED" | wc -w | tr -d ' ') resource ARN(s). Checking actual state..."
  echo ""
fi

# ── Helper: extract IDs by resource type from ARN list ───────────────────────
extract_ids() {
  local type="$1"   # e.g. "instance", "natgateway", "volume"
  # `|| true` prevents grep's exit code 1 (no matches) from killing the script
  # under `set -euo pipefail`.
  echo "$TAGGED" | tr '\t' '\n' | grep ":${type}/" | sed "s|.*/${type}/||" | tr -d ' ' || true
}

# ── [2] EC2 Instances ─────────────────────────────────────────────────────────
echo -e "${BOLD}[2/5] EC2 instances${RESET}"
INSTANCE_IDS=$(extract_ids "instance")

if [ -z "$INSTANCE_IDS" ]; then
  ok "No EC2 instances in tag index."
else
  # Query actual state — terminated instances still appear in tag index for hours
  # but disappear from the describe API ~1 hour after termination (fully gone).
  STATES=$(aws ec2 describe-instances \
    --instance-ids $INSTANCE_IDS \
    --region "${AWS_REGION}" \
    --query 'Reservations[*].Instances[*].[InstanceId,State.Name,InstanceType]' \
    --output text 2>/dev/null || echo "")

  if [ -z "$STATES" ]; then
    ok "Instance(s) no longer exist in EC2 API — fully terminated and purged (not billed)"
  else
    while IFS=$'\t' read -r id state type; do
      [ -z "$id" ] && continue
      case "$state" in
        terminated|shutting-down)
          ok "${id} (${type}) — ${state} (not billed)"
          ;;
        stopped)
          # Stopped instances don't incur compute cost but their EBS volumes do.
          # Flagged as a warning, not a hard failure.
          warn "${id} (${type}) — stopped (compute free, but EBS volumes still billed)"
          ;;
        running|pending|stopping)
          bad "${id} (${type}) — ${state} — BILLED"
          FOUND_SPEND=1
          ;;
        *)
          warn "${id} (${type}) — unknown state: ${state}"
          ;;
      esac
    done <<< "$STATES"
  fi
fi
echo ""

# ── [3] NAT Gateways ──────────────────────────────────────────────────────────
echo -e "${BOLD}[3/5] NAT gateways${RESET}"
NGW_IDS=$(extract_ids "natgateway")

if [ -z "$NGW_IDS" ]; then
  ok "No NAT gateways in tag index."
else
  STATES=$(aws ec2 describe-nat-gateways \
    --nat-gateway-ids $NGW_IDS \
    --region "${AWS_REGION}" \
    --query 'NatGateways[*].[NatGatewayId,State]' \
    --output text 2>/dev/null || echo "")

  while IFS=$'\t' read -r id state; do
    [ -z "$id" ] && continue
    case "$state" in
      deleted|deleting)
        ok "${id} — ${state} (not billed)"
        ;;
      available|pending)
        bad "${id} — ${state} — BILLED (~\$0.045/hr + data transfer)"
        FOUND_SPEND=1
        ;;
      *)
        warn "${id} — unknown state: ${state}"
        ;;
    esac
  done <<< "$STATES"
fi
echo ""

# ── [4] EBS Volumes ───────────────────────────────────────────────────────────
echo -e "${BOLD}[4/5] EBS volumes${RESET}"
VOL_IDS=$(extract_ids "volume")

if [ -z "$VOL_IDS" ]; then
  ok "No EBS volumes in tag index."
else
  STATES=$(aws ec2 describe-volumes \
    --volume-ids $VOL_IDS \
    --region "${AWS_REGION}" \
    --query 'Volumes[*].[VolumeId,State,Size]' \
    --output text 2>/dev/null || echo "")

  while IFS=$'\t' read -r id state size; do
    [ -z "$id" ] && continue
    case "$state" in
      # EBS volumes bill by existence (available or in-use), not by usage.
      available|in-use)
        bad "${id} — ${state}, ${size} GB — BILLED (~\$0.08/GB/month = \$$(echo "scale=2; $size * 0.08 / 730" | bc)/hr)"
        FOUND_SPEND=1
        ;;
      deleting|deleted)
        ok "${id} — ${state} (not billed)"
        ;;
      *)
        warn "${id} — unknown state: ${state}"
        ;;
    esac
  done <<< "$STATES"
fi
echo ""

# ── [5] EKS Clusters ─────────────────────────────────────────────────────────
# Checked directly by name rather than through the tag API, which can lag or
# miss EKS clusters entirely.  CLUSTER_NAME may differ from the PROJECT tag value.
echo -e "${BOLD}[5/5] EKS cluster${RESET}"
CLUSTER_STATE=$(aws eks describe-cluster \
  --name "${CLUSTER_NAME}" \
  --region "${AWS_REGION}" \
  --query 'cluster.status' \
  --output text 2>/dev/null || echo "NOT_FOUND")

case "$CLUSTER_STATE" in
  NOT_FOUND|"")
    ok "Cluster '${CLUSTER_NAME}' not found (not billed)"
    ;;
  DELETING)
    warn "Cluster '${CLUSTER_NAME}' is DELETING (will stop billing shortly)"
    ;;
  ACTIVE|CREATING|UPDATING)
    bad "Cluster '${CLUSTER_NAME}' — ${CLUSTER_STATE} — BILLED (\$0.10/hr)"
    FOUND_SPEND=1
    ;;
  *)
    warn "Cluster '${CLUSTER_NAME}' — unknown state: ${CLUSTER_STATE}"
    ;;
esac
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}════════════════════════════════════════════════════${RESET}"
if [ "$FOUND_SPEND" -eq 0 ]; then
  echo -e "${GREEN}${BOLD}✓ Clean. No billable resources detected.${RESET}"
  echo ""
  echo "  Resources still appearing in the tag index are in a non-billed"
  echo "  state (terminated/deleted) and will drop off within a few hours."
  exit 0
else
  echo -e "${RED}${BOLD}✗ Billable resources detected. See above.${RESET}"
  echo ""
  echo "  If 'make down' has already run, try re-running it:"
  echo "    make down AWS_REGION=${AWS_REGION}"
  echo ""
  echo "  To force-delete specific resources, use the IDs above with:"
  echo "    aws ec2 terminate-instances --instance-ids <id> --region ${AWS_REGION}"
  echo "    aws ec2 delete-nat-gateway  --nat-gateway-id <id> --region ${AWS_REGION}"
  echo "    aws ec2 delete-volume       --volume-id <id>      --region ${AWS_REGION}"
  exit 1
fi
