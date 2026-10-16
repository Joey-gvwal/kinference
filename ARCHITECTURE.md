# Architecture

kinference picks the endpoint that serves an LLM request fastest. It reads
each worker's KV-cache events and engine metrics, scores the workers for a
request, and hands the gateway the winner. Kubernetes custom resources
layer routing policy on top.

This page says how the code is grouped, which way imports go, and where new
code goes. Change it in the same pull request as any move.

## Crates

- `kinference`, this crate. The server. The router and the controller
  live here as directories today.
- `kinference-policy`, in `crates/kinference-policy`. Ids, CRD types and
  pure policy logic. No client and no runtime.
- `kinference-fleet`, in `crates/kinference-fleet`. Which workers exist
  and what state they are in. Imports `kinference-policy` only; no `kube`,
  no `anyhow`.
- `kinfctl`, in `crates/kinfctl`. The CLI. Imports `kinference-policy`
  only.

Target: the router and the controller become crates too. The server crate
keeps wiring, config assembly, the admin listener and the metrics
exposition. The plan is #32.

## Groups

A module belongs to one group. A group is a directory under `src`, or a
crate under `crates` once it has moved. Its `mod.rs` or `lib.rs` lists the
modules and holds nothing else beyond re-exports.

- Fleet, `crates/kinference-fleet`. Which workers exist and what state they are in.
  Pools, the worker registry, the discovery trait and its config,
  the coordinator that adds and evicts workers, the scrape of engine
  metrics, the ZMQ ingest of KV-cache events, the in-flight state, the
  collector behind the fleet gauges, the fleet's own metric families, and
  its error type.
- Router, `src/router`. The request path. The service, scoring, the
  picker, the body store, the tokenizer, observations, the gRPC glue, and
  the request-path metric families.
- Controller, `src/controller`. The Kubernetes control plane. The CRD
  watch and its snapshot, the status and binding writers, the worker-set
  reconcile, leader election, EndpointSlice discovery, and the control
  plane's metric families.
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

Today the tree breaks this rule in two places:

- The router imports the controller's policy store to read the snapshot.
  #49 moves the snapshot holder into `kinference-policy`.
- Two pairs inside the router import each other: `service` and
  `observations` (#45), and `service` and `tokenize_client` (#44).

## Module map

Rows name a module by its group; in code the fleet's are under
`kinference_fleet::`.

| Module | Owns |
| --- | --- |
| `fleet::coordinator` | the worker lifecycle: the loop that takes discovery and eviction events, add, evict and drop, and the pool hooks that start and stop a pool's discovery |
| `fleet::discovery` | the `Discovery` trait, its config types, and the factory the server supplies for Kubernetes discovery |
| `fleet::error` | `Error`, the fleet's error type |
| `fleet::eviction` | the channel that asks the coordinator to drop a worker |
| `fleet::gauges` | the Prometheus collector for the fleet gauges: worker health and saturation computed at gather, the index size from a tree dump at most once every 10s |
| `fleet::inflight` | per-endpoint picks not yet observed, and prefixes dispatched but not yet in the KV index |
| `fleet::ingest` | the ZMQ KV-event listener per worker, replay, and block-size checks |
| `fleet::metrics` | `FleetMetrics`, the fleet's Prometheus families, and the helpers every group registers its families with |
| `fleet::models` | `ModelIndex`: which pools serve each model, and the stream of new claims |
| `fleet::pool` | `Pool`, the runtime instance of one worker set; `WorkerContext`, the handles one worker's tasks share; `PoolRegistry`; `PoolManager`: create, update, retire, reap |
| `fleet::profile` | the `[[profile]]` tuning a pool carries: predictor coefficients, picker policy and the tokenize mode |
| `fleet::scrape` | the scrape of worker `/metrics` and `/v1/models`, and the registry of samples |
| `fleet::sync` | lock helpers that recover from poisoning |
| `fleet::workers` | `WorkerRegistry`: per-worker entries, served models, readiness |
| `router::body_store` | request bodies held between `PushBody` and `PullBody`, with byte budgets and a TTL |
| `router::grpc` | the `Picker` gRPC service; maps proto types to service calls |
| `router::local_tokenize` | the in-process tokenizer registry: tokenizers, chat templates, and the warm-up that follows the model index |
| `router::metrics` | `RouterMetrics`, the request-path families, the tenant label policy and its `[metrics]` config |
| `router::observations` | per-pick state kept until the caller reports, request labels, the CSV row, and the CSV sink |
| `router::pick_body` | rewrites of the request body that `PullBody` serves |
| `router::picker` | the wait-time predictor and the top-K picker; pure functions |
| `router::proto` | the generated tonic types for `kinference.v1` |
| `router::service` | `KinferenceService`: scoring, pick, observation lifecycle, policy driver, provider feedback |
| `router::tokenize` | HTTP `/render` helpers |
| `router::tokenize_client` | `/render` with local fallback and compare mode, as methods on the service |
| `controller::discovery` | `KubeDiscovery`: the EndpointSlice watch that turns a Service's Ready endpoints into workers |
| `controller::leader` | Lease-based leader election |
| `controller::metrics` | `ControllerMetrics`: worker-set rejections, the leader gauge, policy warning counts |
| `controller::policy` | the CRD watch and snapshot, the status writer, the binding writer, the worker-set reconcile |
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
- A new RPC or proto field. `proto/kinference.proto`, then
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
- Anything that talks to the API server. The controller, through the
  client it already has.

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
