# Architecture

kinference picks the endpoint that serves an LLM request fastest. It reads
each worker's KV-cache events and engine metrics, scores the workers for a
request, and hands the gateway the winner. Kubernetes custom resources
layer routing policy on top.

This page says how the code is grouped, which way imports go, and where new
code goes. Change it in the same pull request as any move.

## Crates

- `kinference`, this crate. The server. Every group below lives here as a
  directory today.
- `kinference-policy`, in `crates/kinference-policy`. Ids, CRD types and
  pure policy logic. No client and no runtime.
- `kinfctl`, in `crates/kinfctl`. The CLI. Imports `kinference-policy`
  only.

Target: the fleet, the router and the controller become crates. The server
crate keeps wiring, config assembly, the admin listener and the metrics
exposition. The plan is BuoyantIO/llm-monitor#50.

## Groups

A module belongs to one group. A group is a directory under `src`. Its
`mod.rs` lists the modules and holds nothing else.

- Fleet, `src/fleet`. Which workers exist and what state they are in.
  Pools, the worker registry, discovery, eviction, the scrape of engine
  metrics, the ZMQ ingest of KV-cache events, the in-flight state, and the
  rollup of the fleet gauges.
- Router, `src/router`. The request path. The service, scoring, the
  picker, the body store, the tokenizer, observations, the gRPC glue, and
  the mock.
- Controller, `src/controller`. The Kubernetes control plane. The CRD
  watch and its snapshot, the status and binding writers, the worker-set
  reconcile, and leader election.
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

Today the tree breaks this rule in four places:

- Two imports point up from the fleet into the router. `pool` takes the
  picker and tokenize config types from the router
  (BuoyantIO/llm-monitor#67).
- The router imports the controller's policy store to read the snapshot.
  BuoyantIO/llm-monitor#43 moves the snapshot holder into
  `kinference-policy`.
- Every group records into the one `metrics` struct at the top.
  BuoyantIO/llm-monitor#56 gives each group its own families.
- Two pairs inside the router import each other: `service` and
  `observations` (BuoyantIO/llm-monitor#60), and `service` and
  `tokenize_client` (BuoyantIO/llm-monitor#59).

## Module map

| Module | Owns |
| --- | --- |
| `fleet::discovery` | the `Discovery` trait, static workers, and the Kubernetes EndpointSlice watch |
| `fleet::eviction` | the channel that asks the coordinator to drop a worker |
| `fleet::inflight` | per-endpoint picks not yet observed, and prefixes dispatched but not yet in the KV index |
| `fleet::ingest` | the ZMQ KV-event listener per worker, replay, and block-size checks |
| `fleet::models` | `ModelIndex`: which pools serve each model, and the stream of new claims |
| `fleet::pool` | `Pool`, the runtime instance of one worker set; `PoolRegistry`; `PoolManager`: create, update, retire, reap |
| `fleet::rollup` | the task that sets the fleet gauges once a second |
| `fleet::scrape` | the scrape of worker `/metrics` and `/v1/models`, and the registry of samples |
| `fleet::workers` | `WorkerRegistry`: per-worker entries, served models, readiness |
| `router::body_store` | request bodies held between `PushBody` and `PullBody`, with byte budgets and a TTL |
| `router::grpc` | the `Picker` gRPC service; maps proto types to service calls |
| `router::local_tokenize` | the in-process tokenizer registry: tokenizers, chat templates, and the warm-up that follows the model index |
| `router::mock` | the deterministic `Picker` used in mock mode |
| `router::observations` | per-pick state kept until the caller reports, request labels, the CSV row, and the CSV sink |
| `router::pick_body` | rewrites of the request body that `PullBody` serves |
| `router::picker` | the wait-time predictor and the top-K picker; pure functions |
| `router::proto` | the generated tonic types for `kinference.v1` |
| `router::service` | `KinferenceService`: scoring, pick, observation lifecycle, policy driver, provider feedback |
| `router::tokenize` | HTTP `/render` helpers and `TokenizeMode` |
| `router::tokenize_client` | `/render` with local fallback and compare mode, as methods on the service |
| `controller::leader` | Lease-based leader election |
| `controller::policy` | the CRD watch and snapshot, the status writer, the binding writer, the worker-set reconcile |
| `admin` | `/readyz`, `/metrics`, and the read-only policy API under `/api` |
| `config` | `Config`: load, validation, and the one read of the environment at startup |
| `metrics` | our own Prometheus families and their registry |
| `server` | `run`: config, wiring, shutdown, and the fleet coordinator |
| `version` | the build version string |

`src/bin/compute_hash.rs` is a developer tool, not a module of the crate.

## Where new code goes

- A new signal for scoring. Collect it in the fleet, in `scrape` or
  `ingest`. Store it in the registry. Read it in the router's scoring.
  The picker stays pure: it takes inputs that are already fetched.
- A new RPC or proto field. `proto/kinference.proto`, then
  `router::grpc` maps it to a service call. No logic in `grpc`.
- A new metric. Register it in `metrics`. Record it where the event
  happens.
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
