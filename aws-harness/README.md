# aws-harness

EKS harness for spinning up live inference engines that publish KV-cache
events on ZMQ (Dynamo wire format) and serve OpenAI-compatible HTTP +
Prometheus `/metrics`. Each server is its own Helm release under
`charts/<engine>-server/`, driven by a per-server TOML in `servers/`.
Engine support: **vLLM** and **SGLang**. TrtLLM / KServe slot into the
same directory structure later.

Outputs:
- One pod per server instance, GPU-scheduled, weights mounted read-only
  from S3 via the Mountpoint CSI driver.
- A headless `ClusterIP` Service per server exposing ports 8000
  (HTTP/metrics), 5557 (KV-events PUB), 5558 (KV-events ROUTER).
- A generated `config.toml` describing every running server's
  port-forwarded local endpoints, ready for any KV-event subscriber to
  consume.

## Layout

```
Makefile              cluster lifecycle (up, down, kubeconfig, addons, scale-gpu, status, find-resources, init-weights)
Makefile.server       per-server lifecycle (server-up, server-down, port-forward, port-forward-stop, monitor-config, list-servers)
terraform/            VPC + EKS + system NG + GPU NG (desired=0); IRSA for EBS CSI, LBC
scripts/              install-addons, render-server, port-forward, gen-monitor-config, init-weights, find-resources, cleanup-lbc-sgs
charts/vllm-server/   in-tree Helm chart; one release per running vLLM instance
charts/sglang-server/ in-tree Helm chart for SGLang; same layout as vllm-server
servers/              per-instance TOML configs, committed in-tree
k8s/addons/           NVIDIA device plugin DaemonSet (idempotent kubectl apply)
example/              previous-project reference; not used at runtime
```

## Prereqs

- An AWS account with permission to create VPCs, EKS clusters, IAM roles
  (for IRSA), and `g6e` capacity in the chosen region.
- `aws` CLI configured (profile or env vars) for that account.
- `terraform >= 1.6`, `kubectl`, `helm` (Helm 4 supported), `python3` (3.x;
  no `tomllib` needed — TOML parsing is done via `yq`).
- `yq >= 4` (with TOML support) and `jq` — `scripts/render-server.sh`
  uses them to parse the per-server TOMLs.
- `git config user.email` set — the Makefile passes this through to
  Terraform as the `Owner` tag.

## Tagging

Every AWS resource is tagged:

| Key      | Value                              |
|----------|------------------------------------|
| Project  | `model-harness` (override `PROJECT_TAG`) |
| Owner    | `$(git config user.email)` (override `OWNER`) |

After teardown, `make find-resources` re-checks the Resource Groups Tagging
API across us-west-2 and us-east-1 to confirm nothing was left behind.

## End-to-end walk-through

Run from `aws-harness/`. Each step is reversible by the one below it.

```bash
# 1. Provision the cluster (~12-15 min on first run).
make up

# 2. Configure kubectl. Writes ~/.kube/model-harness; does NOT touch ~/.kube/config.
make kubeconfig

# 3. Install cluster addons: NVIDIA device plugin, gp3 StorageClass,
#    cross-engine inference Service, Mountpoint S3 CSI, Metrics Server
#    (powers `kubectl top pods` / `kubectl top nodes`).
make addons

# 4. Add GPU capacity. ON_DEMAND only; idle cost while N=0 is the EKS control
#    plane + NAT gateway (~$0.10/hr).
make scale-gpu N=1

# 5. Deploy a vLLM server instance. The committed TOMLs (qwen-vllm.toml,
#    gemma-vllm.toml, qwen-sglang.toml) work as-is; edit one in place to
#    change model / weights prefix / replicas / extra_args.
make -f Makefile.server server-up REPLICAS=1 CFG=servers/qwen-vllm.toml

# 6. In a second shell, port-forward each server's ZMQ + HTTP ports to
#    your laptop. Idempotent; safe to re-run.
make -f Makefile.server port-forward

# 7. Generate a config.toml describing every running server's local
#    forwarded ports. Pipe to wherever your consumer expects to find it.
make -f Makefile.server monitor-config > config.toml
```

### Teardown audit

```bash
# Remove server instances.
make -f Makefile.server server-down CFG=servers/qwen-vllm.toml

# Scale GPU to 0 (skip if going straight to destroy).
make scale-gpu N=0

# Destroy everything.
make down

# Verify nothing leaked.
make find-resources
```

## Adding a new server instance

1. If the model's weights aren't already in the shared bucket, populate
   them once with `make init-weights`. See ["Populating the weights bucket"](#populating-the-weights-bucket)
   below for the cluster-vs-laptop modes and gated-repo setup. Quick form:
   ```bash
   make init-weights MODEL=<hf-repo> PREFIX=weights/<your-slug>/
   #   e.g. MODEL=cyankiwi/Qwen3.6-27B-AWQ-INT4 PREFIX=weights/qwen-3-6-27b-awq
   ```
   Idempotent on a sentinel object — re-runs against an already-populated
   prefix exit immediately.
2. Copy an existing TOML: `cp servers/qwen-vllm.toml servers/<your-name>.toml`.
3. Edit `name`, `model_name`, `weights_s3_prefix`, and `extra_args` as needed.
4. `make -f Makefile.server server-up CFG=servers/<your-name>.toml`.
5. If `gpu_count` exceeds the current GPU node count, run
   `make scale-gpu N=<total-gpus-needed>` first.

## Populating the weights bucket

`make init-weights MODEL=<hf-repo> PREFIX=weights/<your-slug>` mirrors a
HuggingFace repo into the harness's S3 bucket. Two modes; both are
idempotent on a sentinel object (re-runs against a populated prefix
exit in <1s).

### Cluster mode (default) — recommended

Runs the download as a Kubernetes Job inside the cluster, so the
HuggingFace → S3 path stays on AWS's backbone instead of crossing your
home connection. Best for big models and slow links — a 36 GB MoE
pulls in ~5-15 min vs ~30-60 min laptop-side.

```bash
make up && make addons    # if you haven't already
make init-weights MODEL=Qwen/Qwen3.6-35B-A3B-FP8 \
                  PREFIX=weights/qwen-3-6-35b-a3b-fp8
```

The Job:
- Schedules on a system node (`role=system`), CPU-only, ~$0.04/hr.
- Mounts an ephemeral gp3 PVC (default 100 GiB; override via
  `SCRATCH_GIB=200` for unusually-large checkpoints).
- Assumes a narrow IAM role via IRSA — only `init-weights` Job pods can
  write to the bucket; long-running inference pods stay read-only.
- Self-cleans 1 hour after success (`ttlSecondsAfterFinished: 3600`).

For **gated HuggingFace repos**, create a `hf-token` Secret in the
default namespace once; the Job auto-mounts it if present:

```bash
kubectl --kubeconfig ~/.kube/model-harness create secret generic hf-token \
  --from-literal=token="$HF_TOKEN"
```

The init-weights ServiceAccount and IRSA role are created by
`terraform/init-weights.tf` and applied with `make up` / `make infra`.

### Local mode — fallback for tiny models or pre-cluster bootstrap

Runs entirely on your laptop. Useful when the cluster isn't up yet
(you can't depend on the cluster to populate the bucket if you need
the bucket populated before bringing the cluster up — though that's
not our usual order) or for small models where the laptop path is
fast enough to not be worth the Job overhead.

```bash
make init-weights MODE=local \
  MODEL=Qwen/Qwen2.5-0.5B-Instruct \
  PREFIX=weights/qwen-2-5-0-5b
```

In local mode, gated repos read `HF_TOKEN` straight from your shell
environment instead of from a Kubernetes Secret.

## Deploying multiple replicas of the same server

To run N identical pods of one model — useful for testing how a
KV-event subscriber distributes work across replicas — set
`replicas = N` in the TOML. The harness deploys one Helm release per
server (one Deployment) and lets Kubernetes manage the N pods.

```toml
# servers/qwen-vllm.toml
name      = "qwen-vllm"
# ... model, weights, gpu_count, etc.
replicas  = 3
```

```bash
make scale-gpu N=3                                   # one GPU per pod
make -f Makefile.server server-up CFG=servers/qwen-vllm.toml
# One `helm upgrade --install`; K8s creates all 3 pods in parallel.

make -f Makefile.server port-forward                 # one row per pod
make -f Makefile.server monitor-config > config.toml
# Three [[workers]] entries; worker_id_str = pod name.
```

Semantics:

- **Pod naming**: pods are named `<release>-<rs-hash>-<random>` (e.g.
  `qwen-vllm-7f9c8b5d6-x9j2k`), following the standard Deployment +
  ReplicaSet shape. **Names rotate on every rollout** — a chart edit
  or `helm upgrade` with changed values creates a new ReplicaSet, new
  pod names, and (consequently) new `worker_id_str` values in the
  generated config.
- **Parallelism**: Deployment creates all `replicas` pods in parallel
  by default. Wall-clock ≈ slowest pod, not N × per-pod. Tail one
  pod's bring-up with `kubectl logs -f -n default <pod-name>`.
- **Bumping `replicas` up or down**: `helm upgrade --set replicas=M`.
  K8s adds new pods (scale-up) or terminates surplus pods (scale-down,
  picked by K8s heuristics). Surviving pods stay running through the
  change.
- **Rolling updates**: default strategy is `maxSurge=0, maxUnavailable=1`,
  i.e. terminate one old pod first and let its GPU slot be reused by
  the new pod. No extra GPU capacity needed for a rollout; trade-off
  is brief per-pod downtime during transitions. Override via
  `rollingUpdate.maxSurge` / `rollingUpdate.maxUnavailable` in values
  if you have spare GPU and want zero-downtime updates.
- **Stuck rollouts**: bounded by `progressDeadlineSeconds` (default
  1800s / 30 min, matching the startupProbe budget). After that
  K8s marks the rollout failed instead of waiting forever.
- **`server-down`**: deletes the Deployment, its Service, its PVC, and
  the PV. All N pods drain.
- **`worker_id_str`**: in the generated `config.toml`, each pod gets
  its own `[[workers]]` entry keyed by pod name. Re-run `monitor-config`
  after any rollout to pick up the new names.

Use this when you want N identical replicas of one config. If you want
N *different* configs (e.g., one replica with prefix caching on and
another without), use N separate TOMLs with distinct `name` values —
the harness happily runs both patterns side-by-side.

## Adding a new engine (TrtLLM, KServe, …)

1. Drop a chart at `charts/<engine>-server/` matching the shape of
   `charts/vllm-server/` or `charts/sglang-server/`. The chart should
   expose the same port triple as the existing charts (8000 HTTP/metrics,
   5557 KV-events PUB, 5558 KV-events ROUTER) so `port-forward.sh` and
   `gen-monitor-config.sh` work unchanged.
2. Extend `scripts/render-server.sh` to recognise the new `engine` value
   and dispatch to the new chart.
3. Update `scripts/gen-monitor-config.sh` only if the new engine needs
   different default endpoints than the 8000/5557/5558 triple.

## Running vLLM and SGLang side by side

Both engines publish KV-cache events in the same Dynamo ZMQ wire format
(3-part multipart frames: `[topic, seq_u64, protobuf_payload]`), so any
downstream subscriber that reads one can read both without code changes.
Both expose `sglang:` / `vllm:` Prometheus metrics on `/metrics`.

```bash
make scale-gpu N=2

# vLLM:
make -f Makefile.server server-up CFG=servers/qwen-vllm.toml

# SGLang on the same Qwen 32B AWQ weights:
make -f Makefile.server server-up CFG=servers/qwen-sglang.toml

# Both servers, distinct local-port allocations, one config:
make -f Makefile.server port-forward          # prints the per-server map
make -f Makefile.server monitor-config > config.toml
```

### Qwen 3.6 27B AWQ (vLLM only)

A more recent lineup using `cyankiwi/Qwen3.6-27B-AWQ-INT4`. **vLLM only on
this model** — SGLang can't load this specific checkpoint (see below).

Quirks worth knowing:

- **Engine version floor**: requires `vllm >= 0.19.0`. Pinned to
  `vllm/vllm-openai:v0.21.0` in `servers/qwen-3-6-vllm.toml`. Verify the
  running version with
  `kubectl logs -l mm/server-name=qwen-3-6-vllm | grep -i 'vllm version'`.
- **No `--quantization` flag**: this checkpoint ships in
  `compressed-tensors` format, which vLLM auto-detects from `config.json`.
  Setting `--quantization=awq_marlin` (as we do for the *older* Qwen 2.5
  AWQ checkpoint) triggers a validation error here.
- **Tighter `--gpu-memory-utilization`**: 0.88, not 0.95. Qwen 3.6's
  larger MLP intermediate dim + thinking-mode activations push memory
  pressure above what works for Qwen 2.5 at the same context length.
  0.95 OOMs at engine init; 0.88 boots cleanly at 16K context.

Workflow:

```bash
# One-time: populate the bucket. ~10-20 min on a typical home connection.
make init-weights MODEL=cyankiwi/Qwen3.6-27B-AWQ-INT4 \
                  PREFIX=weights/qwen-3-6-27b-awq

# Then the usual:
make scale-gpu N=1
make -f Makefile.server server-up CFG=servers/qwen-3-6-vllm.toml
make -f Makefile.server port-forward
make -f Makefile.server monitor-config > config.toml
```

#### Why no SGLang twin for Qwen 3.6

`cyankiwi/Qwen3.6-27B-AWQ-INT4`'s `config.json` declares
`compressed-tensors` quantization with `{num_bits: 4, strategy: group,
group_size: 32, symmetric: false, format: pack-quantized}`. SGLang
(through v0.5.12.post1) has no scheme registered for that exact combo
and refuses to load the model with `NotImplementedError: No
compressed-tensors compatible scheme was found.` vLLM has broader
compressed-tensors coverage and handles it.

There is no flag override — SGLang routes by what `config.json`
declares; forcing a different scheme would either fail to load or read
the tensor layout incorrectly. As of this writing, no community-uploaded
Qwen 3.6 27B AWQ in a standard layout (group_size=128 symmetric or
canonical AWQ tensor names) exists on HuggingFace.

If you want SGLang on Qwen 3 specifically, the workable paths are:

- A different Qwen 3 size with a clean AWQ build (search HF for what's
  available at the time you read this).
- `btbtyler09/Qwen3.6-27B-GPTQ-8bit` via SGLang's GPTQ loader — ~27 GB
  weights, tight on one L40S but workable. Different quantization than
  the vLLM twin so A/B isn't perfectly matched.
- Wait for upstream SGLang to add the missing scheme to its
  compressed-tensors registry.

### Faster Qwen 3.6 via MoE: `Qwen/Qwen3.6-35B-A3B-FP8`

The dense 27B AWQ checkpoint above is conveniently small (19 GB weights)
but slow per token because every decode step streams all 27B of weights.
Qwen's official MoE build is the alternative when throughput matters
more than fitting in a small memory budget.

| | Dense 27B AWQ-INT4 (above) | MoE 35B-A3B FP8 (`servers/qwen-3-6-35b-vllm.toml`) |
|---|---|---|
| Weights on disk | 19 GB | **36 GB** |
| Per-token bandwidth (decode) | ~14 GB (whole model) | **~3-4 GB** (3B activated of 35B) |
| Theoretical decode ceiling on L40S | ~62 tok/s | **~250 tok/s** |
| Realistic decode speedup vs the dense 27B | baseline | **~2.5-3.5×** |
| Max practical context on single L40S | 16K | 8K (tight; 4K safest) |
| Architecture | dense attention | hybrid (Gated DeltaNet + Gated Attention; KV events only from the attention layers) |
| Quality | baseline | ~equal across most benchmarks |

Workflow:

```bash
# One-time: populate the bucket. ~36 GB download; budget 30-60 min.
make init-weights MODEL=Qwen/Qwen3.6-35B-A3B-FP8 \
                  PREFIX=weights/qwen-3-6-35b-a3b-fp8

make scale-gpu N=1
make -f Makefile.server server-up CFG=servers/qwen-3-6-35b-vllm.toml
```

Things to know:

- **Memory budget is sharp.** 36 GB weights leave only ~6-10 GB for
  activations + KV cache on a 48 GB L40S. The TOML caps context at 8K
  and utilization at 0.90. If it OOMs at boot, drop context to 4K
  before touching utilization.
- **FP8 on L40S is real but not Hopper-fast.** Ada (sm_89) has FP8
  tensor cores, so vLLM's FP8 matmul kernel runs natively. Per-cycle
  throughput is lower than H100 but the bandwidth savings (3-4 GB
  per token vs 14 GB) dominate, so you get a real speedup anyway.
- **Hybrid attention affects the KV-events stream.** Some layers are
  Gated DeltaNet (linear attention, no traditional KV cache → no
  events), others are Gated Attention (full attention → standard
  events). Per-token event volume is lower than a pure-attention
  model of the same depth. Useful realism for a KV-event subscriber.
- **No `--quantization` flag.** Qwen-official FP8 builds declare the
  quantization in `config.json`; vLLM auto-detects.
- **For longer context, move to `g6e.12xlarge`.** 4× L40S = 192 GB
  total. Edit the TOML's `gpu_count = 4` and the chart will add
  `--tensor-parallel-size=4`. ~$14/hr vs ~$1.86/hr; ~3-4× more decode
  throughput on top of the MoE savings; 64K+ context fits comfortably.

If you keep both the dense 27B and the MoE 35B-A3B TOMLs deployed
simultaneously, you have a built-in A/B for "same Qwen 3.6 generation,
different architecture and quantization, same chat API." Useful for
exercising KV-event subscribers under varied per-token event volumes.

## Known limitations

- **No cluster autoscaler.** `make scale-gpu` is manual. Suitable for an
  evaluation harness; add Karpenter if traffic patterns demand it.
- **No spot capacity.** GPU spot quota is hard to come by; on-demand only.
- **kubectl port-forward connectivity.** The default exposure model assumes
  the KV-event subscriber and HTTP client run on your laptop. For
  in-cluster or remote-VM scenarios, add an NLB via `make expose-server
  NAME=<name>` (not yet implemented).
- **KV-events flag schemas differ between engines and may drift between
  releases.** Both charts pass `--kv-events-config '{...}'` JSON. vLLM
  auto-implies `publisher=zmq` from `enable_kv_cache_events=true`; SGLang
  requires the explicit `"publisher":"zmq"` discriminator (its
  `EventPublisherFactory` falls through to a no-op publisher otherwise,
  surfaced as `TypeError: EventPublisher.__init__() got an unexpected
  keyword argument 'endpoint'` at scheduler init). If a future release
  rejects the chart's JSON, drop the offending field via `extra_args` or
  patch the deployment template — both charts comment the expected schema
  inline.
- **SGLang `--page-size` is unset by default upstream**, which the runtime
  often resolves to 1 (per-token events, 16× the event volume). Both
  charts pass `--page-size={{ .Values.model.blockSize }}` (and vLLM's
  `--block-size` equivalent) so all engines emit at the same granularity
  and a downstream consumer's global block-size setting matches.

## Tips

- **Quieter terraform output.** `make up` / `make down` filter the
  per-resource diff from `terraform plan` and only show the summary
  (`Plan: N to add, ...`); the full plan is still tee'd to
  `/tmp/tf-{plan,destroy}-$(CLUSTER_NAME).log`. Set `VERBOSE=1` on the
  make invocation to bypass the filter.
- **Live tail across all replicas.** `tail -f /tmp/helm-up-*.log` after a
  `server-up` to watch every replica's helm output without interleaving
  in the foreground.
- **Find a specific server's KV-events log line.** `kubectl logs -l
  mm/server-name=qwen-vllm-2 -f` works because every chart stamps that
  label on its pods.

## Generated `config.toml` format

`make -f Makefile.server monitor-config` emits one `[[workers]]` entry
per running server pod. Endpoints use `tcp://127.0.0.1:<port>` /
`http://127.0.0.1:<port>` so the same file works when `port-forward`
is active on the same machine:

```toml
listen = "0.0.0.0:50051"
kv_block_size = 16
metrics_poll_secs = 1

[[workers]]
worker_id_str   = "qwen-vllm"
worker_id_num   = 1
zmq_endpoint    = "tcp://127.0.0.1:5557"      # KV-events PUB
replay_endpoint = "tcp://127.0.0.1:5558"      # KV-events ROUTER
metrics_url     = "http://127.0.0.1:8000/metrics"
http_url        = "http://127.0.0.1:8000/"
model_name      = "Qwen/Qwen2.5-32B-Instruct-AWQ"
```

With multiple servers, each gets a +1000 offset on every port (see
`port-forward` output for the exact allocation).
