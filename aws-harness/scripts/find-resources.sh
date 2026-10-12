#!/usr/bin/env bash
# find-resources.sh — list AWS resources tagged Project=$PROJECT_TAG across
# the regions the harness deploys to. Run after `make down` to verify clean
# teardown.
#
# Uses the Resource Groups Tagging API, which only returns resource types
# that support tagging. Things like IAM inline policies and Route53 records
# won't appear; that's by design.

set -euo pipefail

PROJECT_TAG="${PROJECT_TAG:-inference-testbed}"
TAG_SEARCH_REGIONS="${TAG_SEARCH_REGIONS:-us-west-2 us-east-1}"

# Output mode:
#   table (default) — human-readable, one ARN per row
#   text            — raw ARNs, one per line, suitable for grep/xargs pipelines
OUTPUT_MODE="${OUTPUT_MODE:-table}"

for region in $TAG_SEARCH_REGIONS; do
  if [ "${OUTPUT_MODE}" = "text" ]; then
    aws resourcegroupstaggingapi get-resources \
      --region "${region}" \
      --tag-filters "Key=Project,Values=${PROJECT_TAG}" \
      --query 'ResourceTagMappingList[].ResourceARN' \
      --output text 2>/dev/null | tr '\t' '\n'
  else
    echo ""
    echo "── ${region} ──────────────────────────────────────────────"
    aws resourcegroupstaggingapi get-resources \
      --region "${region}" \
      --tag-filters "Key=Project,Values=${PROJECT_TAG}" \
      --query 'ResourceTagMappingList[].ResourceARN' \
      --output table 2>/dev/null \
      | sed 's/^/  /' \
      || echo "  (no resources or API error)"
  fi
done

echo ""
echo "  Note: Route53 records, CloudFront OACs, and IAM inline policies"
echo "        are not tagged by AWS and therefore don't appear above."
