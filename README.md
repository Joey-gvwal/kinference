# kinference

**Buoyant LLM Picker.** A standalone Rust service that subscribes to KV-cache
event streams from one or more LLM inference engines (currently vLLM and
SGLang), maintains a per-worker prefix index of which token blocks are cached
on which GPU, and serves routing decisions over two interfaces:

- gRPC `Picker.Pick` + `Picker.Observe` — the canonical RPC pair: send the
  OpenAI chat-completion body, get back a chosen endpoint + opaque
  `response_id`; report observed TTFT later via `Observe(response_id, ms)`.

The heavy lifting (ZMQ wire decode, RadixTree-backed index, scoring) is
done by the [`dynamo-kv-router`](https://github.com/ai-dynamo/dynamo) crate
from NVIDIA's Dynamo project. This service wraps it as a library and adds
config-driven worker discovery, the latency-model picker, and live-traffic
observability.

## Prerequisites

- Rust 1.95+ (`rustup` recommended)
- `protoc` — Protocol Buffers compiler (`brew install protobuf`)
- `libzmq` + `pkg-config` (`brew install zeromq pkg-config`)

For the smoke test:
- Python 3 with `msgspec` + `pyzmq` (`pip install msgspec pyzmq`)
- `grpcurl` (`brew install grpcurl`)

## Build

```sh
cargo build --release
```

First build pulls and compiles `dynamo-kv-router` from a pinned commit — it
takes ~1 minute. Subsequent builds are seconds.

## Configure

The `config.toml` embedded in
[`k8s/kinference/configmap.yaml`](k8s/kinference/configmap.yaml) is a complete, working
config to copy from: two profiles and two worker sets, plus `[policy]` and
`[kubernetes]`. It is the deployed one, so it does not drift.

Schema — two kinds of declaration. `[[workerset]]` says what the workers are
and where they come from; `[[profile]]` says how to route to them, and is
shared by name across as many sets as have the same tuning:

```toml
listen = "0.0.0.0:50051"     # gRPC server bind address
metrics_poll_ms = 1000       # /metrics scrape cadence (default 1000ms; 250 is fine)

[metrics]                    # what kinference PUBLISHES on /metrics — not the scrape
tenant_labels = "none"       # side above. "none" (default) | "all" | ["acme", ...]

[[profile]]
name = "qwen7b-h100"         # name it for what determines the tuning: model
                             # class + hardware + TP degree, not just the model
  [profile.predictor]        # latency-model coefficients; refit with
  base_ms = 400.0            # analysis/analyze-latency.py from the CSV rows
  expected_overlap_alpha = 1.0   # tagged with this profile name
  [profile.picker]
  top_k_min = 1
  [profile.tokenize]
  mode = "local"             # "render" | "compare" | "local"

[[workerset]]
name    = "qwen7b"
profile = "qwen7b-h100"      # required; must name a [[profile]]
kv_block_size = 16           # mirrors the engine's launch flag, so it belongs
                             # to the SET, not the profile
  [workerset.discovery]
  mode = "static"
  workers_file = "pools/qwen7b-workers.toml"
```

The workers file it points at is a flat list:

```toml
[[workers]]
worker_id_str   = "vllm-A"                        # exposed over gRPC
worker_id_num   = 1                                # numeric id, unique FLEET-WIDE
zmq_endpoint    = "tcp://127.0.0.1:5557"          # engine PUB live socket
replay_endpoint = "tcp://127.0.0.1:5558"          # engine ROUTER replay socket; optional
metrics_url     = "http://127.0.0.1:8000/metrics" # engine HTTP /metrics; optional
http_url        = "http://127.0.0.1:8000"         # engine HTTP base, for /render and /v1/models
model_name      = "Qwen/Qwen2.5-7B"               # used to filter label-match metrics
```

There is no `engine` key. vLLM vs SGLang is detected from the engine's own
`/v1/models` response (`data[*].owned_by`) and used only for display labels, so
a mixed fleet needs no declaration. A leftover `engine = "..."` line here is
silently discarded rather than flagged — the workers file tolerates unknown
keys, unlike the main config, which rejects them.

Several worker sets may serve the same model. kinference scores every one of them
and routes to whichever set's best endpoint has the lowest predicted wait.

Block size: `[[workerset]].kv_block_size` is the tokens-per-block in the events
the engines PUBLISH — standard vLLM 16, Gemma 4 256, SGLang `--page-size 1` 1.
It is not always the engine's `--block-size`; Gemma 4 reports 64 there and
publishes 256-token blocks, so read it off a real event.
kinference never adopts an observed value — a worker whose engine publishes a
different block size is EVICTED, with a loud `BLOCK SIZE MISMATCH` warning
naming the remediation. That is why the value lives on the worker set rather
than in a shared profile: one wrong number in a profile would drain every set
using it.

## Run

```sh
RUST_LOG=kinference=debug,dynamo_kv_router=info \
  ./target/release/kinference --config config.toml
```

You should see:

```
INFO kinference: Starting kinference version="0.1.0" listen=0.0.0.0:50051 worker_sets=1 profiles=1 mock_mode=false
INFO kinference: Worker set configured workerset="qwen7b" profile="qwen7b-h100" block_size=16 discovery=Static
INFO kinference::pool_manager: worker set created workerset=qwen7b profile=qwen7b-h100 block_size=16 resurrected=false
INFO kinference::discovery::static_config: StaticDiscovery: all configured workers Added; waiting on shutdown workers=1
INFO kinference: Added worker pool="qwen7b" worker_id="vllm-A" block_size=16 zmq_endpoint="tcp://127.0.0.1:5557" http_url="http://127.0.0.1:8000"
INFO kinference: READY: gRPC accepting connections listen=0.0.0.0:50051 worker_sets=1 mock_mode=false
```

The engine type is absent from these lines because it is not known yet — it is
read from the worker's first `/v1/models` response.

When events arrive, each Stored block is logged on its own line:

```
DEBUG block stored worker=1 seq=42 event="stored" block_idx=0 \
      block_hash=12345 tokens_hash=67890 parent_hash=None mm=false
```

## Smoke test

In one terminal, run kinference as above. In another, publish a few chained blocks
via the fake vLLM publisher and watch the `block stored` log lines on the
kinference side:

```sh
python3 tests/fake_vllm.py --endpoint "tcp://*:5557" --hashes 100 200 300
```

The radix-tree state can be inspected via the dashboard
(`admin_listen` in the config — see below); for end-to-end picks, hit the
gRPC `Picker.Pick` RPC defined in [`proto/kinference.proto`](proto/kinference.proto).

## Connecting to a real vLLM

vLLM must be started with KV-event publishing enabled. The config knob is
on the engine side, e.g.:

```sh
vllm serve <model> --kv-events-config '{
  "enable_kv_cache_events": true,
  "publisher": "zmq",
  "endpoint": "tcp://*:5557",
  "replay_endpoint": "tcp://*:5558"
}'
```

(Field names vary by vLLM version; check `--help` on your build.)

Then forward all three ports (ZMQ live, ZMQ replay, HTTP for `/metrics`)
to the host running kinference:

```sh
# SSH tunnel:
ssh -L 5557:localhost:5557 -L 5558:localhost:5558 -L 8000:localhost:8000 user@vllm-host

# Or kubectl:
kubectl port-forward pod/<vllm-pod> 5557:5557 5558:5558 8000:8000
```

If there's an inference gateway in front of vLLM, it may not pass `/metrics`
(or `/tokenize`) through. In that case, port-forward directly to the pod's
vLLM container port to bypass the gateway.

Copy the config out of [`k8s/kinference/configmap.yaml`](k8s/kinference/configmap.yaml),
point it at your model, then run with it.

If `replay_endpoint` is omitted or unreachable, kinference still works
live-only but will warn `Failed to find parent block; skipping store
operation` for every event chained off a block it never witnessed.

## gRPC API

Defined in [`proto/kinference.proto`](proto/kinference.proto). One service, two RPCs:

- `Pick(PickRequest) → PickResponse`. Body is the full OpenAI chat-completion
  request (bytes). Response is a chosen `endpoint` (host:port) plus an opaque
  `response_id` the caller stashes for later observation.
- `Observe(ObserveRequest) → ObserveResponse`. Caller reports the actual TTFT
  it measured against the picked endpoint; kinference records `(predicted_ms,
  observed_ms)` to a CSV side-channel for offline tuning of the latency model.

The server tokenizes the request body (via in-process tokenizers, or an
engine `/tokenize` HTTP fallback), runs the latency-model picker against the
configured `[predictor]` coefficients, and updates an in-flight prefix
tracker so subsequent picks credit the worker mid-prefill. All routing
policy (replication, top-K, tie-extension, cache-pressure thresholds) is
server-side — callers only see the chosen endpoint.

## Layout

Organised by subsystem. A subsystem owns its own directory, and — where it has
them — its own config section and its own Prometheus metrics, so that working on
one means editing one place. A subsystem with more than one file gets a
`mod.rs`; a single-file one keeps `<name>.rs` with a sibling `<name>/tests.rs`.

```
proto/kinference.proto            Picker, TokenReview and RateLimiter gRPC schema
src/proto.rs                 the generated types (include_proto! mount point)

  request path
src/grpc.rs                  Picker RPC handlers; lifts wire types to domain
src/monitor/                 the service and the request lifecycle
  mod.rs                       KinferenceService, the model→pool router, shared types
  scoring.rs                   workers → ranked ScoreResponse (no I/O)
  lifecycle.rs                 pick_and_record + the observation lifecycle
  policy_driver.rs             runs a compiled InferencePolicy for a request
  provider_feedback.rs         what callers report about off-cluster providers
src/picker.rs                wait-time predictor + top-K picker (pure, no I/O)
src/pick_body.rs             request-body rewrites
src/body_store.rs            PushBody → Pick → PullBody buffer

  routing state
src/pool.rs, pool_manager.rs  pool lifecycle; PoolHooks is the seam to main
src/workers.rs               worker registry (id ↔ entry, http_url, block_size)
src/inflight_prefixes.rs     mid-prefill block tracker
src/inflight_registry.rs     per-endpoint inflight pick counter
src/discovery/               EndpointSlice watch + static config → workers

  ingest and measurement
src/ingest.rs                ZMQ replay + live KV-event recv per worker
src/metrics.rs               per-worker /metrics scraper + label-filtered cache
src/observations.rs          predicted-vs-observed lifecycle
src/observation_sink.rs      CSV sink for the above
src/telemetry.rs             ring buffers + the dashboard snapshot

  CRDs
src/policy/                  everything driven by the five CRDs
  crd.rs                       resource types (k8s/crds/*.yaml is the truth)
  store.rs                     reflectors + the resolved snapshot
  worker_sets.rs               InferenceWorkerSet → PoolSpec, and its watch
  compile.rs, predicate.rs     policy → compiled AST, and the spec-only warnings
  warning.rs                   PolicyWarning: code + manifest path + message
  lint.rs                      name checks against the pools a policy serves
  check.rs                     `kinfctl check`'s engine: manifests in, report out
  engine.rs                    candidate generation, exclusion, selection
  classify/                    per-request tags (pii.rs, complexity.rs — STUBS)
  provider_health.rs           off-cluster destination health
  status.rs                    the controller side (CRD status writing)
  binding.rs                   one BackendBinding per InferencePool, so a
                               gateway can resolve a route naming a pool
src/leader.rs                Lease election, gating status writes

  services and support
src/rate_limit/              RateLimiter gRPC service (STUB: limits nothing)
src/token_review/            Kubernetes bearer-token validation
src/tokenize.rs, local_tokenize.rs, tokenize_client.rs
                             HTTP + in-process tokenization
src/dashboard.rs             live web dashboard + /metrics + /readyz + /api
  policy_api.rs                the read-only policy API (context, simulate)
src/prom.rs                  the Prometheus registry (exposition side)
src/config.rs                root TOML schema; sections live with their owners
src/eviction.rs              typed eviction channel
src/mock.rs                  mock-mode Picker for demos and smoke tests
src/version.rs               the version string, stamped by build.rs
src/lib.rs                   the crate; every module, two public entry points
src/server.rs                the server: arguments, runtime wiring, shutdown
src/main.rs                  `kinference`: a shim over server::run
src/cli.rs                   `kinfctl`'s commands and output
src/bin/kinfctl.rs           `kinfctl`: a shim over cli::main

src/bin/compute_hash.rs      helper: derive tokens_hash from a token sequence
tests/fake_vllm.py           local fake publisher for smoke tests
k8s/kinference/configmap.yaml     a complete, working config to copy from
```

## Live dashboard

An optional in-process web dashboard renders real-time telemetry (pick() RPS +
latency, per-worker queue/KV-cache state, a radix-tree "cache index" row
(indexed blocks, event/churn rates, workers publishing), the prefix overlap
map, recent picks, and a routing-balance gauge) directly from kinference's
memory. It's a bundled React SPA served alongside gRPC on a separate port.

Note: the cache-index "indexed blocks" / "workers in index" gauges refresh on a
~10s throttle (they require an O(tree) `dump_events()` call); the event/churn
rates and every other panel update at 1 Hz.

Enable it with a `admin_listen` line in the config:

```toml
listen = "0.0.0.0:50051"
admin_listen = "0.0.0.0:9100"   # omit to disable; no HTTP listener + no rollup when unset
```

Then open `http://localhost:9100`. The page polls `GET /api/snapshot` once per
second; the time-window picker (60s/5m/15m/1h) slices client-side from a
1-hour ring buffer, so switching windows is instant and needs no refetch.

The dashboard frontend lives in [`dashboard/`](dashboard/) (vite + React). The
built output is embedded into the `kinference` binary at compile time via
`rust-embed`, so a release build is a single self-contained binary. **The built
`dashboard/dist/` is committed** so `cargo build` needs no Node toolchain. After
editing anything under `dashboard/src/`, rebuild it:

```sh
cd dashboard && npm install && npm run build   # regenerates dashboard/dist/
cargo build --release                          # re-embeds it
```

Notes:
- The recent-calls tape is *sampled* (not every call) at high RPS — same as the
  design intent; don't read it as an exhaustive log.
- `topWorker` in each call row is the `host:port` of the worker with the highest
  overlap in that response — a projection of where a cache-respecting router
  *would* send the request, not a routing decision the monitor makes.
- Plaintext HTTP, no auth — intended for localhost / tunnel / trusted-network
  use, same posture as the gRPC port.

The **Policy** tab (`?tab=policy`) is an interactive explorer: pick an applied
`InferencePolicy`, describe a request, edit the candidates, and step through the
decision. It runs on two read-only JSON endpoints, `GET /api/policy/context` and
`POST /api/policy/simulate`, which programs and assistants can call directly —
see [docs/content/reference/policy-api.md](docs/content/reference/policy-api.md).

## Checking policies with kinfctl

`kinfctl` checks `InferencePolicy` manifests before they are applied: fields the
API server would silently drop, values that do not parse, rules nothing can
reach, thresholds outside an attribute's range, and — given the pool and its
destinations — backend and model names that match nothing.

```sh
just install-cli
kinfctl check --pool default/inference-all k8s/examples/demo-configs/*.yaml
```

Prebuilt `kinfctl` binaries for macOS (Apple Silicon) and Linux are attached to
each release at [BuoyantIO/kinference-releases](https://github.com/BuoyantIO/kinference-releases/releases);
its `install.sh` fetches the newest one:

```sh
curl -fsSL https://raw.githubusercontent.com/BuoyantIO/kinference-releases/main/install.sh | sh
```

Every finding names a manifest path and a stable code; `-o json` is for programs,
and the exit status (0 clean, 1 findings, 2 unusable input) is for scripts. See
[docs/content/reference/kinfctl.md](docs/content/reference/kinfctl.md).

## Logging cheat-sheet

| What you want | `RUST_LOG` |
|---|---|
| Default operational logs | `kinference=info,dynamo_kv_router=info` |
| Per-block stored/removed lines | `kinference=debug,dynamo_kv_router=info` |
| Per-block + radix-tree mutations | `kinference=debug,dynamo_kv_router::indexer=trace` |
| Quiet the orphan-block warnings | append `,dynamo_kv_router::indexer::radix_tree=error` |

## Kubernetes mode

Instead of declaring worker sets in TOML, let the cluster declare them. With
`[kubernetes] enabled = true`, kinference watches `InferenceWorkerSet` CRs and
materializes a pool per set — creating one is how you add a fleet, deleting it
is how you remove one, and neither needs a config change or a restart.

```yaml
apiVersion: kinference.buoyant.io/v1alpha1
kind: InferenceWorkerSet
metadata: {name: qwen7b, namespace: infra}
spec:
  profile: qwen7b-h100       # names a [[profile]] in kinference's TOML
  kvBlockSize: 16            # tokens per block in the engines' PUBLISHED events
  serviceRef: {name: qwen7b-vllm, port: openai}
  kvEvents: {port: kv-pub}   # omit if these engines publish no KV events
```

The split is deliberate: **topology in the cluster, tuning in the router.**
Which Service, which ports, which block size are properties of the deployment
and belong in a cluster object. Predictor coefficients come out of
`analysis/analyze-latency.py` and are calibrated against hardware and workload,
so they stay in `[[profile]]` blocks rather than becoming numbers an operator
hand-edits into a CR.

`profile` and `kvBlockSize` are required by the schema. A set that lacks them —
or names a profile kinference does not define — is rejected individually, reported
as `Accepted=False` on the object with the reason, and counted under
`kinference_workerset_rejected_total`. The rest of the fleet is unaffected.

Set `[kubernetes].selector` the moment two kinference deployments share a cluster.
Without it each one materializes every worker set in scope, including sets whose
pods it cannot reach.

RBAC: `inferenceworkersets` get/list/watch belongs in the BASE ClusterRole, not
the policy-gated one — discovery is mandatory in Kubernetes mode and CRD routing
policy is optional. See [rbac.yaml](k8s/kinference/rbac.yaml).

A local rig for exercising all of this against a real apiserver — creating and
deleting sets under traffic, killing the apiserver, churning until something
leaks — is in [k8s/local-test](k8s/local-test/README.md).

## SGLang workers

SGLang is supported alongside vLLM in the same kinference instance, with nothing to
declare: the engine is read from each worker's `/v1/models` response. Engine
differences — metric names, the KV-event wire format — are handled internally,
and the gRPC API is identical.

To enable on the SGLang side, start the server with both `--enable-metrics`
and a `--kv-events-config` JSON that includes a replay endpoint:

```sh
python -m sglang.launch_server \
  --model-path <model> \
  --enable-metrics \
  --kv-events-config '{
    "publisher": "zmq",
    "endpoint": "tcp://*:5577",
    "replay_endpoint": "tcp://*:5578",
    "topic": "kv-events"
  }'
```

Forward all three ports to the host running kinference:

```sh
kubectl port-forward pod/<sglang-pod> 5577:5577 5578:5578 30000:30000
```

Quirks specific to SGLang:
- Only `tp_rank=0` publishes KV events (upstream issue
  [sglang#16030](https://github.com/sgl-project/sglang/issues/16030)).
  Run with `--tp 1` or accept that only one TP shard's cache is observed.
- SGLang has no `swapped` counter and no `cache_config_info` gauge, so
  `queue_swapped` and `num_gpu_blocks` will always be `0` in the gRPC
  response for SGLang workers.
- SGLang's HTTP server defaults to port 30000 (vLLM uses 8000). Set the
  worker's `http_url` accordingly.

See [`k8s/kinference/configmap.yaml`](k8s/kinference/configmap.yaml) for a full worked
schema.

## Scope

Supported: vLLM and SGLang ingest (live + replay), per-engine `/metrics`
polling with queue depth and KV cache usage, gRPC `Picker.Pick/Observe` with
in-process tokenization (or `/tokenize` HTTP fallback) + latency-model
scoring, cache-replication routing (steer cold picks to seed warm cache),
optional in-process live dashboard (`admin_listen`).

Not yet: TRT-LLM bridge, runtime worker register/unregister, mid-stream
gap-triggered replay, persistence, per-DP-rank metrics breakdown, LoRA
adapter awareness, tokenize cache, fallback across multiple `http_url`s
when the first is unreachable.
