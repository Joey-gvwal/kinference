#!/usr/bin/env bash
# gen-monitor-config.sh — generate a kinference worker-set config from the servers
# currently deployed in the cluster.
#
# Output is intended for `kubectl port-forward` usage: every worker's
# zmq_endpoint / replay_endpoint / metrics_url uses tcp://127.0.0.1:<port>.
#
# kinference's config is worker-set based: ONE [[workerset]] per distinct model
# deployed in the cluster, each with its own [[profile]]. A set's static
# worker endpoints live in a SEPARATE sibling file referenced via
# [workerset.discovery].workers_file. So this
# writes:
#   <OUT_CONFIG>                            the config (default ../config.toml)
#   <OUT_CONFIG stem>.<set>.workers.toml    one workers file per set (siblings)
#
# Usage:
#   bash scripts/gen-monitor-config.sh [OUT_CONFIG]
#
# Examples:
#   bash scripts/gen-monitor-config.sh              # -> aws-harness/config.toml (+ per-set workers files)
#   bash scripts/gen-monitor-config.sh /tmp/foo.toml
#
# Each generated `workers_file` is a bare filename; kinference resolves it
# relative to the config file's own directory, so the files just need to
# sit side by side (they always do here).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE_SELECTOR="${NAMESPACE_SELECTOR:---all-namespaces}"

# Output paths. Default the config next to the harness root (../config.toml).
# The Python helper writes per-set "<stem>.<set>.workers.toml" siblings
# into OUT_DIR.
OUT_CONFIG="${1:-${SCRIPT_DIR}/../config.toml}"
OUT_DIR="$(cd "$(dirname "${OUT_CONFIG}")" && pwd)"
OUT_BASE="$(basename "${OUT_CONFIG}")"
OUT_STEM="${OUT_BASE%.toml}"

# Pull managed pods + all nodes, then combine into one JSON document so the
# Python helper can join pod → node. Availability zone lives on the node's
# `topology.kubernetes.io/zone` label, not on the pod; node IP and pod IP
# come straight from pod status. This whole metadata set is what a
# service-discovery-driven router would normally read from EndpointSlices +
# node labels — we materialize it into the static config here as a prototype.
# shellcheck disable=SC2086
pods_json=$(kubectl get pods \
  -l mm/managed-by=model-harness,app.kubernetes.io/component=inference \
  --field-selector=status.phase=Running \
  ${NAMESPACE_SELECTOR} \
  -o json)

# Listing nodes is best-effort: if the caller's RBAC can't list nodes, fall
# back to an empty set (the helper degrades availability_zone to "" rather
# than failing).
nodes_json=$(kubectl get nodes -o json 2>/dev/null || echo '{"items":[]}')

# The Python helper writes the config to stdout (captured to
# OUT_CONFIG) and one workers file per set into WORKERS_DIR, named
# "<CONFIG_STEM>.<set>.workers.toml".
WORKERS_DIR="${OUT_DIR}" CONFIG_STEM="${OUT_STEM}" \
jq -n --argjson pods "${pods_json}" --argjson nodes "${nodes_json}" \
  '{pods: $pods, nodes: $nodes}' \
| python3 "${SCRIPT_DIR}/_gen_monitor_config.py" > "${OUT_CONFIG}"

echo "# wrote config to ${OUT_CONFIG} (per-workerset workers files alongside it)" >&2
