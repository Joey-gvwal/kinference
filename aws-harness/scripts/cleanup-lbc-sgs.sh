#!/usr/bin/env bash
# cleanup-lbc-sgs.sh — pre-destroy cleanup hook for `make down`.
#
# Two responsibilities:
#   1. Uninstall llm-d (if present) so its NLB + RBAC drop cleanly
#      before terraform tries to delete the VPC. uninstall-llm-d.sh
#      no-ops if llm-d isn't installed, so this is safe unconditionally.
#   2. Remove stray AWS Load Balancer Controller security groups that
#      LBC sometimes orphans (LB-target SGs tagged with the cluster
#      name). Without this, terraform destroy hangs on ENI deletion.
#
# Both steps no-op cleanly on a cluster that never had LBC / llm-d
# installed.

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-model-harness}"
AWS_REGION="${AWS_REGION:-us-west-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { echo "▶  $*"; }
skip() { echo "–  $*"; }

# ── 0. Uninstall llm-d if it's deployed ───────────────────────────────────────
# Guard on namespace presence so this is a no-op when llm-d was never
# installed. The uninstall script itself also self-guards, but the
# guard here avoids printing its "not installed" line during typical
# `make down` runs.
if kubectl get ns llmd >/dev/null 2>&1 \
   || kubectl get ns agentgateway-system >/dev/null 2>&1; then
  log "llm-d detected — running uninstall-llm-d.sh before AWS cleanup..."
  bash "${SCRIPT_DIR}/uninstall-llm-d.sh" || \
    echo "⚠  uninstall-llm-d.sh failed; continuing with SG cleanup anyway" >&2
fi

# ── 0b. Uninstall monitoring stack if deployed ────────────────────────────────
# Same self-guarding pattern as llm-d above. uninstall-monitoring.sh
# polls AWS until its Grafana NLB is fully removed, so the SG cleanup
# pass below has a clean slate.
if kubectl get ns monitoring >/dev/null 2>&1; then
  log "monitoring detected — running uninstall-monitoring.sh before AWS cleanup..."
  bash "${SCRIPT_DIR}/uninstall-monitoring.sh" || \
    echo "⚠  uninstall-monitoring.sh failed; continuing with SG cleanup anyway" >&2
fi

VPC_ID=$(aws ec2 describe-vpcs \
  --region "${AWS_REGION}" \
  --filters "Name=tag:kubernetes.io/cluster/${CLUSTER_NAME},Values=owned,shared" \
  --query 'Vpcs[0].VpcId' \
  --output text 2>/dev/null || echo "")

if [ -z "${VPC_ID}" ] || [ "${VPC_ID}" = "None" ]; then
  VPC_ID=$(aws ec2 describe-vpcs \
    --region "${AWS_REGION}" \
    --filters "Name=tag:Name,Values=${CLUSTER_NAME}-vpc" \
    --query 'Vpcs[0].VpcId' \
    --output text 2>/dev/null || echo "")
fi

if [ -z "${VPC_ID}" ] || [ "${VPC_ID}" = "None" ]; then
  skip "No VPC found for cluster '${CLUSTER_NAME}' — nothing to clean up."
  exit 0
fi

log "Searching VPC ${VPC_ID} for stray LBC security groups..."

LBC_SGS=$(aws ec2 describe-security-groups \
  --region "${AWS_REGION}" \
  --filters \
    "Name=vpc-id,Values=${VPC_ID}" \
    "Name=tag:elbv2.k8s.aws/cluster,Values=${CLUSTER_NAME}" \
  --query 'SecurityGroups[*].GroupId' \
  --output text 2>/dev/null || echo "")

NAME_SGS=$(aws ec2 describe-security-groups \
  --region "${AWS_REGION}" \
  --filters \
    "Name=vpc-id,Values=${VPC_ID}" \
    "Name=group-name,Values=k8s-*" \
  --query 'SecurityGroups[*].GroupId' \
  --output text 2>/dev/null || echo "")

ALL_SGS=$(echo "${LBC_SGS} ${NAME_SGS}" | tr ' ' '\n' | sort -u | grep -v '^$' || true)

if [ -z "${ALL_SGS}" ]; then
  skip "No stray LBC security groups in ${VPC_ID}."
  exit 0
fi

log "Found stray SGs: $(echo ${ALL_SGS} | tr '\n' ' ')"

# Revoke cross-referencing rules so deletes don't fail.
for SG in ${ALL_SGS}; do
  RULES=$(aws ec2 describe-security-group-rules \
    --region "${AWS_REGION}" \
    --filters "Name=group-id,Values=${SG}" \
    --query 'SecurityGroupRules[?ReferencedGroupInfo!=null].SecurityGroupRuleId' \
    --output text 2>/dev/null || echo "")

  for RULE_ID in ${RULES}; do
    IS_EGRESS=$(aws ec2 describe-security-group-rules \
      --region "${AWS_REGION}" \
      --security-group-rule-ids "${RULE_ID}" \
      --query 'SecurityGroupRules[0].IsEgress' \
      --output text 2>/dev/null || echo "false")
    if [ "${IS_EGRESS}" = "true" ]; then
      aws ec2 revoke-security-group-egress \
        --region "${AWS_REGION}" --group-id "${SG}" \
        --security-group-rule-ids "${RULE_ID}" 2>/dev/null || true
    else
      aws ec2 revoke-security-group-ingress \
        --region "${AWS_REGION}" --group-id "${SG}" \
        --security-group-rule-ids "${RULE_ID}" 2>/dev/null || true
    fi
  done

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
      aws ec2 revoke-security-group-ingress \
        --region "${AWS_REGION}" --group-id "${REF_SG}" \
        --security-group-rule-ids ${RULE_IDS} 2>/dev/null || true
    fi
  done
done

for SG in ${ALL_SGS}; do
  aws ec2 delete-security-group --region "${AWS_REGION}" --group-id "${SG}" 2>/dev/null \
    && echo "✓  Deleted ${SG}" \
    || echo "–  Skipped ${SG} (may already be gone)"
done
