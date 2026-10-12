#!/usr/bin/env python3
"""Read combined pod+node JSON on stdin, emit a config.toml on stdout.
Internal helper for gen-monitor-config.sh.

Input shape (produced by gen-monitor-config.sh):
    {"pods": <kubectl get pods -o json>, "nodes": <kubectl get nodes -o json>}
A bare `kubectl get pods -o json` (i.e. {"items": [...]}) is also accepted
for backwards compatibility / direct testing — node-derived fields like
availability_zone just come out empty in that case.

Port assignment matches scripts/port-forward.sh: managed pods are sorted
by (namespace, pod-name); pod N (0-indexed across the full sorted list)
gets local ports 8000+N*1000 (HTTP), 5557+N*1000 (KV PUB),
5558+N*1000 (KV ROUTER). Worker identity (`worker_id_str`) is the pod
name (e.g. "qwen-vllm-0") — a stable id that survives port-map
reshuffles. The HTTP host:port is still available via `http_url`.

Each worker entry also carries discovery metadata (pod name, namespace,
engine, node name/IP, pod IP, availability zone). In production this set
would come from Kubernetes service discovery (EndpointSlices + node
labels) rather than a static config; materializing it here prototypes the
shape a discovery-driven router would route on.
"""

import json
import os
import socket
import sys


# Host the emitted worker URLs point at. Default 127.0.0.1 fits the
# `scripts/port-forward.sh` workflow (locally-bound forwards). Override
# via the BASE_HOST env var to target an NLB hostname instead — e.g.
#   BASE_HOST=$(make worker-nlb-host) bash gen-monitor-config.sh
# emits worker URLs that point at the in-cluster `worker-router` NLB
# from scripts/expose-workers.sh. The port-offset scheme is identical
# either way: slot N's HTTP port is 8000+N*1000, etc.
HOST = os.environ.get("BASE_HOST", "127.0.0.1")

# Pre-resolved IP form of HOST. Emitted per-worker as `endpoint = "IP:port"`
# alongside the hostname-form URLs. The IP form is what SocketAddr-strict
# consumers (e.g. agentgateway's ext_proc client) need — they reject DNS
# names with "invalid socket address syntax". NLB public IPs are stable
# Elastic IPs assigned at NLB provisioning, so resolving once at gen
# time is safe; the resolution doesn't go stale as long as the NLB lives.
# If HOST is already an IP or "127.0.0.1", gethostbyname is a no-op.
try:
    HOST_IP = socket.gethostbyname(HOST)
except socket.gaierror:
    # Fall back to HOST as-is rather than crash. If HOST was a hostname
    # that couldn't resolve, the `endpoint` field will be useless, but
    # the rest of config.toml (http_url etc.) still works for clients
    # that do their own DNS.
    HOST_IP = HOST

PORT_HTTP = 8000
PORT_KV_PUB = 5557
PORT_KV_REPLAY = 5558
OFFSET_STEP = 1000

# Well-known node labels carrying the availability zone. The first is the
# current standard; the second is the pre-1.21 beta name still seen on
# older clusters.
ZONE_LABELS = (
    "topology.kubernetes.io/zone",
    "failure-domain.beta.kubernetes.io/zone",
)

# Well-known node labels carrying the instance type. First is the current
# standard; second is the pre-1.17 beta name. EKS nodes carry both with the
# same value.
INSTANCE_TYPE_LABELS = (
    "node.kubernetes.io/instance-type",
    "beta.kubernetes.io/instance-type",
)


def _first_label(labels: dict, keys) -> str:
    """Return the value of the first present label key, or ''."""
    for key in keys:
        if labels.get(key):
            return labels[key]
    return ""


def main() -> int:
    data = json.load(sys.stdin)

    # Accept either the combined {pods, nodes} shape or a bare pod list.
    if "pods" in data:
        pod_doc = data.get("pods") or {}
        node_doc = data.get("nodes") or {}
    else:
        pod_doc = data
        node_doc = {}

    items = pod_doc.get("items", [])
    node_items = node_doc.get("items", [])

    if not items:
        sys.stderr.write(
            "# No model-harness pods found in Running phase.\n"
            "# Deploy something with: make -f Makefile.server server-up CFG=servers/<name>.toml\n"
            "# (Pods may also still be Pending or ContainerCreating — check\n"
            "# kubectl get pods -A -l mm/managed-by=model-harness.)\n"
        )
        return 1

    # Build nodeName -> {zone, instance_type} from node labels (for the
    # pod→node join below). Empty when nodes weren't provided or RBAC
    # denied listing — derived fields then degrade to "".
    node_info: dict[str, dict[str, str]] = {}
    for n in node_items:
        name = n.get("metadata", {}).get("name", "")
        labels = n.get("metadata", {}).get("labels", {}) or {}
        if name:
            node_info[name] = {
                "zone": _first_label(labels, ZONE_LABELS),
                "instance_type": _first_label(labels, INSTANCE_TYPE_LABELS),
            }

    # Match port-forward.sh's sort order: (namespace, pod name).
    items.sort(
        key=lambda it: (
            it["metadata"].get("namespace", ""),
            it["metadata"]["name"],
        )
    )

    # Model name + block size live in annotations because Kubernetes label
    # values can't hold "/" (so model names like "Qwen/Qwen2.5-7B-Instruct"
    # would be rejected as labels). The chart stamps them on the pod
    # template so each pod carries the annotation set.
    def ann(it: dict, key: str) -> str:
        return (it["metadata"].get("annotations") or {}).get(key, "")

    # Group pods by model → ONE kinference [[workerset]] per distinct model.
    # Within a set the fleet is homogeneous (same model + block_size); across
    # sets it isn't. Insertion order follows the global (namespace, name)
    # sort, so set order is deterministic. The GLOBAL sorted index (gi)
    # drives port assignment + worker_id_num so port-forward.sh's offset
    # scheme still lines up and ids stay unique across sets.
    def slug(s: str) -> str:
        return "".join(c if c.isalnum() or c in "-_" else "-" for c in s) or "pool"

    pools: dict[str, list[tuple[int, dict]]] = {}
    for gi, it in enumerate(items):
        pools.setdefault(ann(it, "mm/model"), []).append((gi, it))

    # Where the per-pool workers files land + how they're named. The
    # wrapper passes these so the files sit next to the config it writes;
    # kinference resolves a relative workers_file against the config's own dir.
    workers_dir = os.environ.get("WORKERS_DIR", ".")
    config_stem = os.environ.get("CONFIG_STEM", "config")

    out = sys.stdout
    out.write(f"""
# Auto-generated by aws-harness/scripts/gen-monitor-config.sh.
# Endpoints match scripts/port-forward.sh's port-offset scheme.
# Run `bash scripts/port-forward.sh` first to bind the local ports.
#
# ONE [[workerset]] per distinct model ({len(pools)} here). Each set's static
# endpoints live in its own sibling workers file (workers_file), and each gets
# its own [[profile]] so the sets can be tuned independently — refit a profile
# from the latency.csv rows carrying its name.
#
# Sets that genuinely share hardware AND workload can point at one profile
# instead; that is the case profiles exist for.

listen = "0.0.0.0:50051"
admin_listen = "0.0.0.0:9001"
metrics_poll_ms = 250

# Fleet-wide tokenize plumbing (shared HTTP client + on-disk tokenizer
# cache). The tokenization STRATEGY is tuning, so it lives in the profile.
[tokenize]
#pool_max_idle_per_host = 64
""")

    for model, members in pools.items():
        pool_name = slug(model)
        # Per-pool block_size homogeneity: all pods of ONE model must
        # agree. Different models (different pools) may differ.
        bsizes = {ann(it, "mm/block-size") for _, it in members}
        if len(bsizes) != 1 or "" in bsizes or None in bsizes:
            sys.stderr.write(
                f"# ERROR: pods for model {model} disagree on block_size "
                f"(found {sorted(bsizes)}); reconcile servers/*.toml.\n"
            )
            return 1
        (block_size,) = bsizes
        workers_basename = f"{config_stem}.{pool_name}.workers.toml"

        out.write(f"""
[[profile]]
name = "{pool_name}"

  # Latency-model coefficients — start from these defaults and refit from the
  # latency.csv rows tagged with this profile via analysis/analyze-latency.py.
  [profile.predictor]
  base_ms = 400.0
  queue_ms_per_entry = 0.2
  attention_uncached_block_ms = 2.3
  attention_uncached_per_every_block_ms = 0.03
  cache_pressure_penalty_ms = 0.0
  q_running_penalty_peak_ms = 0
  forward_pass_ms = 5
  expected_overlap_alpha = 1.0

  [profile.picker]
  top_k_min = 1
  top_k_fraction = 0.0
  inflight_tracking = true
  replication_threshold_pct = 10
  replication_warm_target = 2
  tie_tolerance_ms = 0

  # `local` keys tokenizers by model, so it works for any set. Switch to
  # `compare` while validating a new model, or `render` to delegate to the
  # engine's own endpoint (with an optional per-set `render_url`).
  [profile.tokenize]
  mode = "local"

[[workerset]]
name = "{pool_name}"
profile = "{pool_name}"
kv_block_size = {block_size}
#render_url = "http://XXXXXXXXXXXX:XXXXX/v1/chat/completions/render"

  [workerset.discovery]
  mode = "static"
  workers_file = "{workers_basename}"
""")

        # Write this pool's static workers to its own sibling file.
        workers_path = os.path.join(workers_dir, workers_basename)
        with open(workers_path, "w") as wf:
            wf.write(
                f"# Static worker endpoints for the `{pool_name}` worker set.\n"
                "# Auto-generated alongside config.toml by gen-monitor-config.sh.\n\n"
            )
            for gi, it in members:
                meta = it.get("metadata", {})
                spec = it.get("spec", {})
                status = it.get("status", {})

                # Port assignment uses the GLOBAL index so it matches
                # port-forward.sh (which forwards every managed pod in one
                # sorted list) and keeps worker_id_num unique across pools.
                offset = gi * OFFSET_STEP
                l_http = PORT_HTTP + offset
                l_pub = PORT_KV_PUB + offset
                l_rep = PORT_KV_REPLAY + offset

                # Discovery metadata (pod object + node join).
                pod_name = meta.get("name", "")
                namespace = meta.get("namespace", "")
                node_name = spec.get("nodeName", "")
                node_ip = status.get("hostIP", "")
                pod_ip = status.get("podIP", "")
                ninfo = node_info.get(node_name, {})
                zone = ninfo.get("zone", "")
                instance_type = ninfo.get("instance_type", "")

                wf.write("[[workers]]\n")
                wf.write(f'worker_id_str   = "{pod_name}"\n')
                wf.write(f"worker_id_num   = {gi + 1}\n")
                wf.write(f'zmq_endpoint    = "tcp://{HOST}:{l_pub}"\n')
                wf.write(f'replay_endpoint = "tcp://{HOST}:{l_rep}"\n')
                wf.write(f'metrics_url     = "http://{HOST}:{l_http}/metrics"\n')
                wf.write(f'http_url        = "http://{HOST}:{l_http}/"\n')
                # Uniform IP:port form for consumers that need a
                # SocketAddr-parseable address (agentgateway's ext_proc
                # client rejects DNS names). HOST_IP is pre-resolved.
                wf.write(f'endpoint        = "{HOST_IP}:{l_http}"\n')
                wf.write(f'model_name      = "{model}"\n')
                wf.write(f'pod_name          = "{pod_name}"\n')
                wf.write(f'namespace         = "{namespace}"\n')
                wf.write(f'node_name         = "{node_name}"\n')
                wf.write(f'node_ip           = "{node_ip}"\n')
                wf.write(f'pod_ip            = "{pod_ip}"\n')
                wf.write(f'availability_zone = "{zone}"\n')
                wf.write(f'instance_type     = "{instance_type}"\n')
                wf.write("\n")

        sys.stderr.write(
            f"# pool {pool_name}: {len(members)} worker(s) -> {workers_path}\n"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
