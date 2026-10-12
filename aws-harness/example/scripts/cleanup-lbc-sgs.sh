#!/usr/bin/env bash
# cleanup-lbc-sgs.sh — Remove stray AWS Load Balancer Controller security groups.
#
# The LBC creates security groups for ALBs that it doesn't always clean up when
# an Ingress is deleted (especially if deleted manually or with --wait=false).
# These orphaned SGs block `terraform destroy` when it tries to delete the VPC.
#
# This script finds all LBC-managed SGs in the cluster's VPC, revokes any
# cross-referencing rules, and deletes them.
#
# Usage:
#   CLUSTER_NAME=wm-inference AWS_REGION=us-west-2 bash scripts/cleanup-lbc-sgs.sh
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-wm-inference}"
AWS_REGION="${AWS_REGION:-us-west-2}"

log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
skip() { echo "–  $*"; }

# ── 1. Find the cluster VPC ───────────────────────────────────────────────────
log "Finding VPC for cluster '${CLUSTER_NAME}'..."
VPC_ID=$(aws ec2 describe-vpcs \
  --region "${AWS_REGION}" \
  --filters "Name=tag:kubernetes.io/cluster/${CLUSTER_NAME},Values=owned,shared" \
  --query 'Vpcs[0].VpcId' \
  --output text 2>/dev/null || echo "")

if [ -z "${VPC_ID}" ] || [ "${VPC_ID}" = "None" ]; then
  # Fallback: find by cluster name tag
  VPC_ID=$(aws ec2 describe-vpcs \
    --region "${AWS_REGION}" \
    --filters "Name=tag:Name,Values=${CLUSTER_NAME}" \
    --query 'Vpcs[0].VpcId' \
    --output text 2>/dev/null || echo "")
fi

if [ -z "${VPC_ID}" ] || [ "${VPC_ID}" = "None" ]; then
  skip "No VPC found for cluster '${CLUSTER_NAME}' — nothing to clean up."
  exit 0
fi

log "VPC: ${VPC_ID}"

# ── 2. Find stray LBC security groups ────────────────────────────────────────
# LBC SG names follow patterns:
#   k8s-traffic-<hash>   — shared backend SG
#   k8s-<elb>-<hash>     — ALB frontend SG
LBC_SGS=$(aws ec2 describe-security-groups \
  --region "${AWS_REGION}" \
  --filters \
    "Name=vpc-id,Values=${VPC_ID}" \
    "Name=tag:elbv2.k8s.aws/cluster,Values=${CLUSTER_NAME}" \
  --query 'SecurityGroups[*].GroupId' \
  --output text 2>/dev/null || echo "")

# Also catch untagged LBC SGs by name pattern
NAME_SGS=$(aws ec2 describe-security-groups \
  --region "${AWS_REGION}" \
  --filters \
    "Name=vpc-id,Values=${VPC_ID}" \
    "Name=group-name,Values=k8s-*" \
  --query 'SecurityGroups[*].GroupId' \
  --output text 2>/dev/null || echo "")

# Combine and deduplicate
ALL_SGS=$(echo "${LBC_SGS} ${NAME_SGS}" | tr ' ' '\n' | sort -u | grep -v '^$' || true)

if [ -z "${ALL_SGS}" ]; then
  skip "No stray LBC security groups found — VPC is clean."
  exit 0
fi

log "Found stray LBC security groups: $(echo ${ALL_SGS} | tr '\n' ' ')"

# ── 3. Revoke cross-referencing rules ────────────────────────────────────────
# SGs referencing each other must have those rules removed before deletion.
for SG in ${ALL_SGS}; do
  RULES=$(aws ec2 describe-security-group-rules \
    --region "${AWS_REGION}" \
    --filters "Name=group-id,Values=${SG}" \
    --query 'SecurityGroupRules[?ReferencedGroupInfo!=null].SecurityGroupRuleId' \
    --output text 2>/dev/null || echo "")

  if [ -n "${RULES}" ]; then
    log "Revoking cross-referencing rules on ${SG}..."
    # Split into ingress and egress rules separately
    for RULE_ID in ${RULES}; do
      IS_EGRESS=$(aws ec2 describe-security-group-rules \
        --region "${AWS_REGION}" \
        --security-group-rule-ids "${RULE_ID}" \
        --query 'SecurityGroupRules[0].IsEgress' \
        --output text 2>/dev/null || echo "false")
      if [ "${IS_EGRESS}" = "true" ]; then
        aws ec2 revoke-security-group-egress \
          --region "${AWS_REGION}" \
          --group-id "${SG}" \
          --security-group-rule-ids "${RULE_ID}" 2>/dev/null || true
      else
        aws ec2 revoke-security-group-ingress \
          --region "${AWS_REGION}" \
          --group-id "${SG}" \
          --security-group-rule-ids "${RULE_ID}" 2>/dev/null || true
      fi
    done
  fi

  # Also revoke any rules in OTHER SGs that reference this SG
  REFERENCING=$(aws ec2 describe-security-groups \
    --region "${AWS_REGION}" \
    --filters "Name=ip-permission.group-id,Values=${SG}" \
    --query 'SecurityGroups[*].GroupId' \
    --output text 2>/dev/null || echo "")

  for REF_SG in ${REFERENCING}; do
    RULE_IDS=$(aws ec2 describe-security-group-rules \
      --region "${AWS_REGION}" \
      --filters \
        "Name=group-id,Values=${REF_SG}" \
        "Name=referenced-group-id,Values=${SG}" \
      --query 'SecurityGroupRules[*].SecurityGroupRuleId' \
      --output text 2>/dev/null || echo "")
    if [ -n "${RULE_IDS}" ]; then
      log "Revoking rules on ${REF_SG} referencing ${SG}..."
      aws ec2 revoke-security-group-ingress \
        --region "${AWS_REGION}" \
        --group-id "${REF_SG}" \
        --security-group-rule-ids ${RULE_IDS} 2>/dev/null || true
    fi
  done
done

# ── 4. Delete the stray SGs ───────────────────────────────────────────────────
for SG in ${ALL_SGS}; do
  log "Deleting security group ${SG}..."
  aws ec2 delete-security-group \
    --region "${AWS_REGION}" \
    --group-id "${SG}" 2>/dev/null && ok "Deleted ${SG}" || echo "  (skipped ${SG} — may already be gone)"
done

ok "LBC security group cleanup complete."
