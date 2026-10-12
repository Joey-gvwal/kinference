#!/usr/bin/env bash
# port-forward.sh — forward HTTP + KV-event ZMQ ports for each managed pod
# to localhost, so a downstream KV-event subscriber can reach them.
#
# Forwards per pod:
#   localhost:<l_http>     → pod:8000   (OpenAI HTTP + /metrics)
#   localhost:<l_pub>      → pod:5557   (KV-events PUB)
#   localhost:<l_replay>   → pod:5558   (KV-events ROUTER)
#
# Each StatefulSet replica is its own pod (`<server>-0`, `<server>-1`, …),
# so a 3-replica StatefulSet produces three rows in the port-map table.
#
# Port offsets are assigned deterministically: managed pods are sorted by
# (namespace, pod name); pod N (0-indexed across the full sorted list)
# gets offset N*1000 on every port. Same sort and step are used by
# gen-monitor-config.sh, so the generated config endpoints always line up.
#
# Idempotent: re-running does the minimum needed to make reality match the
# desired map. Always prints the final map regardless of whether anything
# was actually changed.
#
# Usage:
#   bash scripts/port-forward.sh                # all managed pods
#   bash scripts/port-forward.sh qwen-vllm      # all pods under that server name
#                                                (matches the mm/server-name label)
#   bash scripts/port-forward.sh qwen-vllm-1    # just one pod by name
#   ACTION=stop bash scripts/port-forward.sh    # stop all
#   ACTION=stop bash scripts/port-forward.sh qwen-vllm   # stop one server's pods
#
# Logs land at /tmp/port-forward-<pod>.log. The background kubectl
# processes survive shell exit (nohup + disown); use ACTION=stop to kill
# them cleanly.
#
# Robustness: kubectl port-forward's legacy SPDY transport drops on certain
# stream events that show up around HTTP 4xx responses (some clients RST
# the local socket on error, vLLM occasionally sends Connection: close on
# validation failures). To keep benchmark traffic flowing, each pod's
# forwarder runs inside a tiny respawn wrapper that re-execs `kubectl
# port-forward` whenever it exits. We also opt into the websocket transport
# (KUBECTL_PORT_FORWARD_WEBSOCKETS=true, supported by kubectl 1.30+ against
# Kubernetes 1.30+), which is dramatically more stable than SPDY and avoids
# most of the drops the respawn loop was designed to catch — the loop stays
# as a belt-and-suspenders defense for the remaining edge cases.

set -euo pipefail

# Prefer the websocket port-forward transport (kubectl ≥ 1.30, EKS ≥ 1.30).
# Older kubectls ignore this env var; safe to set unconditionally.
export KUBECTL_PORT_FORWARD_WEBSOCKETS="${KUBECTL_PORT_FORWARD_WEBSOCKETS:-true}"

NAMESPACE_SELECTOR="${NAMESPACE_SELECTOR:---all-namespaces}"
ACTION="${ACTION:-up}"
TARGET="${1:-}"

case "${ACTION}" in
  up|stop) ;;
  *) echo "ERROR: ACTION must be 'up' or 'stop' (got: ${ACTION})" >&2; exit 1 ;;
esac

# Sorted (namespace, pod-name, server-name) list of managed pods that are
# in Running phase. Each line is `<ns>|<pod>|<server>`.
#
# Filter on `mm/managed-by=model-harness` so we pick up everything the
# harness deployed; pod ownership flows naturally from the StatefulSet's
# pod template labels. `awk 'NF'` instead of `grep -v '^$'` so an empty
# result doesn't fail the pipeline under `set -o pipefail` (grep returns 1
# on no matches, which would exit before the "no managed pods" diagnostic
# below ever runs).
# shellcheck disable=SC2086
pods=$(kubectl get pods \
  -l mm/managed-by=model-harness \
  --field-selector=status.phase=Running \
  ${NAMESPACE_SELECTOR} \
  -o jsonpath='{range .items[*]}{.metadata.namespace}{"|"}{.metadata.name}{"|"}{.metadata.labels.mm/server-name}{"\n"}{end}' \
  | awk 'NF' \
  | sort -t'|' -k1,1 -k2,2)

if [ -z "${pods}" ]; then
  echo "No managed Running pods found." >&2
  echo "Deploy something with: make -f Makefile.server server-up CFG=servers/<name>.toml" >&2
  echo "(Pods may also still be in Pending or ContainerCreating — check kubectl get pods -A -l mm/managed-by=model-harness.)" >&2
  exit 1
fi

# Resolve TARGET, which may be either:
#   • a server name (mm/server-name label) — match every pod of that StatefulSet
#   • a pod name (metadata.name) — match that one pod
# If neither matches, error out with the list of known servers + pods.
if [ -n "${TARGET}" ]; then
  filtered=$(echo "${pods}" | awk -F'|' -v t="${TARGET}" '$2==t || $3==t')
  if [ -z "${filtered}" ]; then
    echo "ERROR: no managed pod or server named '${TARGET}'" >&2
    echo "Known servers (mm/server-name):" >&2
    echo "${pods}" | awk -F'|' '{print "  " $3}' | sort -u >&2
    echo "Known pods (metadata.name):" >&2
    echo "${pods}" | awk -F'|' '{print "  " $2}' >&2
    exit 1
  fi
fi

declare -a rows=()

# pgrep_kubectl <pod> — print the PID of the respawn wrapper managing this
# pod's port-forward (matched via the `mm-pf-<pod>` tag we embed in argv).
#
# We deliberately find the wrapper, not the inner kubectl child, because:
#   • killing the wrapper traps and tears down kubectl cleanly;
#   • killing kubectl directly just causes the wrapper to respawn it.
#
# The trailing space in the regex prevents `mm-pf-qwen` from matching
# `mm-pf-qwen-vllm-0`.
pgrep_kubectl() {
  pgrep -af "mm-pf-$1 " 2>/dev/null \
    | awk '{print $1}' | head -1
}

handle() {
  local ns="$1" pod="$2" server="$3" offset="$4"
  local l_http=$((8000 + offset))
  local l_pub=$((5557 + offset))
  local l_rep=$((5558 + offset))

  local existing
  existing=$(pgrep_kubectl "${pod}" || true)

  if [ "${ACTION}" = "stop" ]; then
    if [ -n "${existing}" ]; then
      kill "${existing}" 2>/dev/null || true
      rows+=("${server}|${pod}|—|—|—|stopped (pid ${existing})")
    else
      rows+=("${server}|${pod}|—|—|—|not running")
    fi
    return
  fi

  # ACTION=up: decide whether the existing forward (if any) is already correct.
  if [ -n "${existing}" ]; then
    local cmd
    cmd=$(ps -p "${existing}" -o command= 2>/dev/null || true)
    if echo "${cmd}" | grep -q "${l_http}:8000" \
       && echo "${cmd}" | grep -q "${l_pub}:5557" \
       && echo "${cmd}" | grep -q "${l_rep}:5558"; then
      rows+=("${server}|${pod}|http|${l_http}|8000|already (pid ${existing})")
      rows+=("${server}|${pod}|kv-pub|${l_pub}|5557|already")
      rows+=("${server}|${pod}|kv-replay|${l_rep}|5558|already")
      return
    fi
    # Wrong ports — restart.
    kill "${existing}" 2>/dev/null || true
    sleep 1
  fi

  # No (matching) kubectl process. Make sure no unrelated process is
  # squatting on the local ports we want to use.
  #
  # `-sTCP:LISTEN` is load-bearing: a bare `lsof -ti :PORT` matches sockets
  # where PORT is EITHER the local or remote endpoint, so an outgoing
  # connection to e.g. NLB:8000 (made by kinference when configured against
  # the worker-router) would look like a conflict against local-port 8000.
  # We only care about bound listeners.
  local conflict=""
  for p in "${l_http}" "${l_pub}" "${l_rep}"; do
    if lsof -ti :"${p}" -sTCP:LISTEN >/dev/null 2>&1; then
      conflict="${conflict}${p} "
    fi
  done
  if [ -n "${conflict}" ]; then
    rows+=("${server}|${pod}|http|${l_http}|8000|CONFLICT (local ports busy: ${conflict% })")
    rows+=("${server}|${pod}|kv-pub|${l_pub}|5557|CONFLICT")
    rows+=("${server}|${pod}|kv-replay|${l_rep}|5558|CONFLICT")
    return
  fi

  # Spawn a respawn wrapper that keeps `kubectl port-forward` alive for
  # this pod. The wrapper:
  #   • tags itself with `mm-pf-<pod>` in argv so pgrep_kubectl finds it;
  #   • runs kubectl with all three port pairs in one invocation;
  #   • traps TERM/INT so `ACTION=stop` cleanly kills the inner kubectl;
  #   • respawns on any kubectl exit with a 2s backoff (covers SPDY drops
  #     around HTTP errors, pod IP changes after a restart, etc.).
  #
  # Port mappings are passed as literal positional args ("18000:8000" etc.)
  # so the wrapper's command line contains them — the "already at the right
  # ports?" check below greps `ps -o command=` for those strings.
  nohup bash -c '
    marker="$1"; ns="$2"; pod="$3"; shift 3
    # Escape the $ so kpid is read at trap-fire time, not when trap is
    # registered (at registration kpid is unset → trap body becomes a
    # no-op "kill 2>/dev/null", and the kubectl child orphans on stop).
    trap "kill \${kpid:-} 2>/dev/null; exit 0" TERM INT
    echo "[$marker] starting respawn loop $(date)" >&2
    while true; do
      kubectl port-forward -n "$ns" "pod/$pod" "$@" &
      kpid=$!
      wait "$kpid"
      rc=$?
      echo "[$marker] kubectl exited rc=$rc at $(date), respawn in 2s" >&2
      sleep 2
    done
  ' bash-wrapper "mm-pf-${pod}" "${ns}" "${pod}" \
    "${l_http}:8000" "${l_pub}:5557" "${l_rep}:5558" \
    >"/tmp/port-forward-${pod}.log" 2>&1 &
  local pid=$!
  disown "${pid}" 2>/dev/null || true

  # Briefly verify the forward actually came up; the kubectl process can
  # exit immediately if the pod isn't Ready yet.
  local ok=0
  for _ in 1 2 3 4 5; do
    sleep 0.5
    if kill -0 "${pid}" 2>/dev/null && lsof -ti :"${l_http}" -sTCP:LISTEN >/dev/null 2>&1; then
      ok=1
      break
    fi
  done
  if [ "${ok}" -eq 1 ]; then
    rows+=("${server}|${pod}|http|${l_http}|8000|started (pid ${pid})")
    rows+=("${server}|${pod}|kv-pub|${l_pub}|5557|started")
    rows+=("${server}|${pod}|kv-replay|${l_rep}|5558|started")
  else
    rows+=("${server}|${pod}|http|${l_http}|8000|FAILED — see /tmp/port-forward-${pod}.log")
    rows+=("${server}|${pod}|kv-pub|${l_pub}|5557|FAILED")
    rows+=("${server}|${pod}|kv-replay|${l_rep}|5558|FAILED")
  fi
}

# Walk every pod in the *full* sorted list to assign offsets, but only
# act on the targeted ones (if filtering was requested).
i=0
while IFS='|' read -r ns pod server; do
  [ -z "${ns}" ] && continue
  offset=$((i * 1000))
  if [ -z "${TARGET}" ] || [ "${pod}" = "${TARGET}" ] || [ "${server}" = "${TARGET}" ]; then
    handle "${ns}" "${pod}" "${server}" "${offset}"
  fi
  i=$((i + 1))
done <<<"${pods}"

# ── Pretty-print the resulting map ────────────────────────────────────────
echo ""
printf '%-18s  %-22s  %-10s  %-7s  %-5s  %s\n' SERVER POD PURPOSE LOCAL POD-PORT STATUS
printf '%-18s  %-22s  %-10s  %-7s  %-5s  %s\n' ------------------ ---------------------- ---------- ------- ----- ------
for row in "${rows[@]}"; do
  IFS='|' read -r srv pd purpose l podp st <<<"${row}"
  printf '%-18s  %-22s  %-10s  %-7s  %-5s  %s\n' "${srv}" "${pd}" "${purpose}" "${l}" "${podp}" "${st}"
done

if [ "${ACTION}" = "up" ]; then
  echo ""
  echo "  Logs:        /tmp/port-forward-<pod>.log"
  echo "  Stop all:    ACTION=stop bash scripts/port-forward.sh"
  echo "  Regenerate config:"
  echo "               bash scripts/gen-monitor-config.sh > config.toml"
fi
