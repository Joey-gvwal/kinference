# Architecture

kinference picks the endpoint that serves an LLM request fastest. It reads
each worker's KV-cache events and engine metrics, scores the workers for a
request, and hands the gateway the winner. Kubernetes custom resources
layer routing policy on top.

This page says how the code is grouped, which way imports go, and where new
code goes. Change it in the same pull request as any move.

## Crates

- `kinference`, this crate. The server: wiring, config assembly, the
  admin listener and the metrics exposition.
- `kinference-policy`, in `crates/kinference-policy`. Ids, CRD types, pure
  policy logic and the policy snapshot holder. No client and no runtime.
- `kinference-fleet`, in `crates/kinference-fleet`. Which workers exist
  and what state they are in. Imports `kinference-policy` only; no `kube`,
  no `anyhow`.
- `kinference-router`, in `crates/kinference-router`. The request path.
  Imports `kinference-fleet` and `kinference-policy`; no `kube`, no
  `axum`, no `anyhow`.
- `kinference-controller`, in `crates/kinference-controller`. The
  Kubernetes control plane. Imports `kinference-fleet` and
  `kinference-policy`, never the router; the one crate that depends on
  `kube`; no `anyhow`.
- `kinfctl`, in `crates/kinfctl`. The CLI. Imports `kinference-policy`
  only.

What remains of the restructure (#32) is the server module itself, #51.

## Groups

A module belongs to one group. A group is a directory under `src`, or a
crate under `crates` once it has moved. Its `mod.rs` or `lib.rs` lists the
modules and holds nothing else beyond re-exports.

- Fleet, `crates/kinference-fleet`. Which workers exist and what state they are in.
  Pools, the worker registry, the discovery factory and its config,
  the coordinator that adds and evicts workers, the scrape of engine
  metrics, the ZMQ ingest of KV-cache events, the in-flight state, the
  collector behind the fleet gauges, the fleet's own metric families, its
  error type, and `start`, the one call the server builds and starts it
  with.
- Router, `crates/kinference-router`. The request path. The service, scoring, the
  picker, the body store, the tokenizer, observations, the gRPC glue, the
  request-path metric families, and its error type.
- Controller, `crates/kinference-controller`. The Kubernetes control
  plane. The API server trait and its client, the CRD watch and its
  snapshot, the status and binding writers, the worker-set reconcile,
  leader election, EndpointSlice discovery, the control plane's metric
  families, its error type, and `start`, the one call the server makes.
- Operator surface, the top of `src`. Config, wiring, the admin
  listener, the Prometheus exposition, and the version string.

## Import direction

Imports point down, never up or sideways:

- `kinference-policy` imports nothing of ours.
- The fleet imports `kinference-policy`.
- The router imports the fleet and `kinference-policy`.
- The controller imports the fleet and `kinference-policy`. Never the
  router.
- The operator surface imports all of the above.

## Module map

Rows name a module by its group; in code the fleet's are under
`kinference_fleet::`, the router's under `kinference_router::` and the
controller's under `kinference_controller::`.

| Module | Owns |
| --- | --- |
| `fleet::coordinator` | the worker lifecycle: the loop that takes discovery and eviction events, add, evict and drop, and the pool hooks that start and stop a pool's discovery |
| `fleet::discovery` | the discovery config types, `WorkerChange`, and the factory the server supplies for Kubernetes discovery |
| `fleet::error` | `Error`, the fleet's error type |
| `fleet::eviction` | the channel that asks the coordinator to drop a worker |
| `fleet::gauges` | the Prometheus collector for the fleet gauges: worker health and saturation computed at gather, the index size from a tree dump at most once every 10s |
| `fleet::inflight` | per-endpoint picks not yet observed, and prefixes dispatched but not yet in the KV index |
| `fleet::ingest` | the ZMQ KV-event listener per worker, replay, and block-size checks |
| `fleet::metrics` | `FleetMetrics`, the fleet's Prometheus families, and the helpers every group registers its families with |
| `fleet::models` | `ModelIndex`: which pools serve each model, and the stream of new claims |
| `fleet::pool` | `Pool`, the runtime instance of one worker set; `WorkerContext`, the handles one worker's tasks share; `PoolRegistry`; `PoolManager`: create, update, retire, reap |
| `fleet::profile` | the profile tuning a pool carries: predictor coefficients, picker policy and the tokenize mode |
| `fleet::scrape` | the scrape of worker `/metrics` and `/v1/models`, and the registry of samples |
| `fleet::start` | `Fleet`: builds the shared state, the coordinator and the pool manager, materializes the config-declared worker sets and runs the coordinator; `FleetHandles`, what the router and the control plane read |
| `fleet::sync` | lock helpers that recover from poisoning |
| `fleet::workers` | `WorkerRegistry`: per-worker entries, served models, readiness |
| `router::body_store` | `BodyStore`: request bodies held between `PushBody` and `PullBody`, with byte budgets and a TTL; `Bodies`, the service's part: the store, the `PushBody` drain with its idle budget, and the buffer metrics |
| `router::error` | `Error`, the router's error type |
| `router::grpc` | the `Picker` gRPC service and `server`, the tonic service over it; maps proto types to service calls |
| `router::metrics` | `RouterMetrics`, the request-path families, the tenant label policy and its `metrics` config |
| `router::observations` | `Observations`, the service's part: per-pick state kept until the caller reports, request labels, the CSV row and the CSV sink, and the provider feedback the caller reports |
| `router::pick_body` | rewrites of the request body that `PullBody` serves |
| `router::picker` | the wait-time predictor and the top-K picker; pure functions |
| `router::proto` | the generated tonic types for `kinference.v1` |
| `router::service` | `KinferenceService` and its parts: `Scoring` (the pools, the model index and the scrape reads), `Bodies`, `Observations`, the pick path and the policy driver; `start`, which builds the service from the server's settings and handles and runs its sweeps |
| `router::tokenizer` | the `Tokenizer` trait, the request and outcome types, the `tokenize` config, and `ByMode`, which picks a backend by the pool's mode and records the tokenize metrics |
| `router::tokenizer::compare` | `Compare`: `/render` is authoritative; the local result is compared in the background for the divergence counters |
| `router::tokenizer::http` | `HttpRender`: vLLM's `/render`, a dedicated URL or the pool's workers in turn |
| `router::tokenizer::local` | `LocalTokenizer`: in-process tokenizers, chat templates, the warm-up that follows the model index, and the fallback to `/render` |
| `controller::api` | `ApiServer`, the one door to the API server: watch, list, get, create, replace, apply, delete, patch status; `KubeApi` over a `kube::Client`, and in tests `Recorded`, the same over objects the test supplies |
| `controller::error` | `Error`, the control plane's error type |
| `controller::discovery` | `KubeDiscovery`: the EndpointSlice watch that turns a Service's Ready endpoints into workers; `factory`, which binds the in-cluster client into the fleet's discovery factory |
| `controller::leader` | Lease-based leader election |
| `controller::metrics` | `ControllerMetrics`: worker-set rejections, the leader gauge, policy warning counts |
| `controller::policy` | the CRD watch, which publishes into `kinference_policy::store`; the status writer, the binding writer, the worker-set reconcile; each over `controller::api` |
| `controller::start` | `start`: connects, then runs the worker-set watch or the reap sweep, the CRD watch, the elector and the writers; `Controller`, whose `shutdown` waits for the Lease to be vacated |
| `controller::sync` | lock helpers that recover from poisoning |
| `admin` | `/readyz`, `/metrics`, and the read-only policy API under `/api` |
| `config` | `Config`: load, validation, and the one read of the environment at startup |
| `metrics` | the one Prometheus registry, with `build_info`, and the text exposition over it |
| `server` | `run`: config, wiring and shutdown |
| `version` | the build version string |

`src/bin/compute_hash.rs` is a developer tool, not a module of the crate.

## Where new code goes

- A new signal for scoring. Collect it in the fleet, in `scrape` or
  `ingest`. Store it in the registry. Read it in the router's scoring.
  The picker stays pure: it takes inputs that are already fetched.
- A new RPC or proto field. `crates/kinference-router/proto/kinference.proto`, then
  `router::grpc` maps it to a service call. No logic in `grpc`.
- A new metric. Add it to the metrics struct of the group where the
  event happens, through the helpers in `fleet::metrics`. Record it there.
  The server only lends the registry and renders it.
- A new config setting. The config struct of the subsystem that reads
  it, next to that code. Validate it in `config`. Give it a type, not a
  raw string. Read the environment only in `config`, at startup.
- A new CRD field or policy rule. Types and pure logic in
  `kinference-policy`. The controller writes the status. The router reads
  the snapshot. `kinfctl` gets it for free.
- A new engine kind. Metric names in `scrape`, event parsing in
  `ingest`.
- A new admin endpoint. `admin`. It reads snapshots. It never writes
  router state.
- Anything that talks to the API server. The controller, through
  `controller::api::ApiServer`; never a `kube::Api` of its own.

## Rules for a move

- Behaviour stays the same. Tests, clippy and fmt stay green after every
  commit.
- One structural change per commit. Move, test, commit. Rename, test,
  commit.
- No constructor takes more than six arguments. Group the handles in a
  struct built once in `server`.
- A trait only where two implementations exist.
- `kube`, `reqwest`, `tonic`, `axum` and `zmq` stay at the edges. Decision
  logic takes data and returns data.
- Nothing reads an env var after startup.
