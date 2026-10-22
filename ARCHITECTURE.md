# Architecture

kinference picks the endpoint that serves an LLM request fastest. A gateway
streams it the request body and asks for a pick. kinference tokenizes the
prompt, matches its prefix against the blocks each worker has cached, reads
each worker's load from the last metrics scrape, predicts a wait time per
worker, and hands back the winner. If the request named an `InferencePool`,
the pool's `InferencePolicy` narrows and ranks the candidates first, and the
winner may be an off-cluster provider. The caller reports first and final
token later, which closes the pick's in-flight state and leaves a row for
tuning the predictor. A control plane watches the custom resources, turns
worker sets into pools, writes their status, and elects one replica to do
the writing.

This page is the map: where code lives, which way imports go, what must
stay true, and where new code goes. Change it in the same pull request as
any move. For the dynamics, one request step by step, read
[docs/request-flow.md](docs/request-flow.md).

## Vocabulary

- Worker: one engine process, vLLM or SGLang. It exposes `/metrics` and
  `/v1/models` over HTTP and publishes KV-cache events over ZMQ.
- Worker set: a group of workers with one block size and one profile,
  declared as an `InferenceWorkerSet` or in the config. Both resolve to a
  `PoolSpec`.
- Pool: the runtime state of exactly one worker set. Its radix tree, its
  worker registry, its in-flight bookkeeping and the lock that serializes
  commits. Pools are never merged; one pick may score several.
- Profile: the tuning a pool carries. Predictor coefficients, picker policy
  and the tokenize mode. Named in the config, referenced by worker sets.
- Block: a fixed run of tokens, the worker set's `kvBlockSize`. Engines
  publish cache events per block, so it is the unit of prefix matching.
- Index: the radix tree of blocks each worker has cached, built from the
  KV events. One per pool.
- Pick: one routing decision. The body in, an endpoint and a request id out.
- In-flight: picks dispatched but not yet observed, and prefixes dispatched
  but not yet in the index. Both credit a worker before the engine reports.
- Observation: what a pick leaves behind until the caller reports. Finishing
  it releases the in-flight state and writes the predicted-versus-observed
  CSV row.
- `InferencePool`: the resource naming the destinations a route may reach
  and the policy that chooses among them. Not a pool.
- Provider: an off-cluster destination, an `InferenceProvider`. The caller
  dials it; kinference learns its health only from what the caller reports.
- Snapshot: the resolved view of pools, worker sets, providers and compiled
  policies. The controller publishes it, the router and the admin API read
  it.
- `BackendBinding`: the object the controller writes per `InferencePool`, so
  the gateway can resolve a backend reference it has no schema for.
- Leader: the one replica that writes status and bindings. Every replica
  routes.

## Entry points

- `server::run`, in `src/server.rs`. The binary. It loads the config, makes
  the one metrics registry, builds the fleet, starts the router, starts the
  fleet, starts the control plane, spawns the admin listener, serves gRPC
  until a signal, then cancels and waits for the control plane.
- `Fleet::new` and `Fleet::start`, `KinferenceService::start` and
  `controller::start`. The three subsystems, each built once from the
  server's settings and a struct of shared handles.
- `router::grpc`, the `Picker` handlers. `lifecycle::pick_and_record` is
  the pick itself, five typed steps.
- `crates/kinfctl/src/main.rs`, the CLI. It reads manifests and runs the
  policy crate's checks with no cluster.
- `crates/kinference-router/proto/kinference.proto`, the wire contract.
  Its header documents the per-request flow and the error model.

## Code map

One crate per concern. Each crate's `lib.rs` lists its modules and what
they own; this map stays at the crate level.

### `kinference-policy`, in `crates/kinference-policy`

The ids, the CRD types, and everything about routing policy that can be
computed without a cluster: the predicate grammar, compiling an
`InferencePolicy` once per revision, resolving the resources into the
snapshot, candidate generation and selection, the explain trace, the lint
and the manifest checks behind `kinfctl check`. It also holds the snapshot
store the controller fills and the router reads.

**Invariant.** No client, no runtime, no I/O. `resolve` is a pure function
over the CRD contents, so the router's watcher and `kinfctl` reach the same
answer from the same inputs.

### `kinference-fleet`, in `crates/kinference-fleet`

Which workers exist and what state they are in. Pools and the pool
manager, the worker registry, the model index, the coordinator that takes
discovery and eviction events, the scrape of engine metrics, the ZMQ ingest
of KV events, the in-flight trackers and the fleet's gauges.

**Invariants.** The fleet never holds a Kubernetes client: discovery is a
task the server supplies through a factory. Fleet-wide state is keyed by
worker id, and the first pool to register a worker owns it. Pools are
independent, with no shared radix trees.

### `kinference-router`, in `crates/kinference-router`

The request path. The gRPC glue, the body store that holds bodies between
push and pull, the tokenizer, scoring, the policy driver, the picker, the
pick lifecycle and the observations a pick leaves behind.

**Invariants.** The picker is pure: it takes inputs already fetched and
returns a ranking. `grpc` maps proto types to service calls and holds no
logic. Only `commit` writes state, under the pool's pick lock. Policy
ranking holds no pool lock.

### `kinference-controller`, in `crates/kinference-controller`

The Kubernetes control plane. The CRD watch that feeds the snapshot, the
worker-set reconcile that turns `InferenceWorkerSet`s into pools, the status
and binding writers, leader election over a Lease, and the EndpointSlice
discovery the fleet runs.

**Invariants.** The one crate that depends on `kube`. Everything that talks
to the API server goes through `api::ApiServer`, which has a live
implementation and a recorded one for tests. Only the leader writes.

### `kinference`, the server crate at the top of `src`

The operator surface: the config and its validation, the wiring in
`server`, the admin listener with `/readyz`, `/metrics` and the read-only
policy API, the Prometheus exposition and the version string.

**Invariant.** The only crate that reads the environment, and only at
startup. The only crate that uses `anyhow`.

### `kinfctl`, in `crates/kinfctl`

The CLI. It imports `kinference-policy` and nothing else of ours.

### Import direction

Imports point down, never up or sideways.

- `kinference-policy` imports nothing of ours.
- The fleet imports `kinference-policy`.
- The router imports the fleet and `kinference-policy`.
- The controller imports the fleet and `kinference-policy`, never the
  router.
- The server crate imports all of the above.

`kube`, `reqwest`, `tonic`, `axum` and `zmq` stay at the edges. Decision
logic takes data and returns data.

## Cross-cutting concerns

### Configuration

One `Config`, loaded and validated at startup in the server crate's
`config`. Each subsystem's settings are a struct next to the code that
reads them; `config` assembles and checks them. Nothing reads an
environment variable after startup.

### Errors

Each library crate has its own `Error` enum, written by hand. `anyhow` is
used only by the binary, where it adds context and exits.

### Metrics

The server creates the one Prometheus registry and lends it to each crate.
Each crate has a metrics struct that registers its own families through the
helpers in `fleet::metrics`, and records them where the event happens. Names
start with `kinference_`. The admin listener renders the registry; it owns
no family of its own beyond `build_info`.

### Locking

Standard mutexes, taken through the poison-recovering helpers in
`fleet::sync` and `controller::sync`. Every guarded map changes by single
inserts and removes or by whole-value swaps, so a panic elsewhere cannot
leave it torn, and stale-but-whole data beats cascading the panic. The pool's
pick lock is the one critical section on the request path.

### Shutdown and leadership

One `CancellationToken`, created in `server` and cloned into every task.
SIGTERM or SIGINT stops the gRPC server, then the token is cancelled, then
the control plane is given a bounded moment to vacate its Lease so the peer
takes over at once. Leadership gates only the status and binding writers;
routing never waits on it.

### Observability

Structured logs through `tracing`, filtered by `RUST_LOG`. Metrics as above.
The observations CSV, one row per pick with the predicted and observed
times, is the input for refitting the predictor.

### Testing

Unit tests sit next to the module in a sibling `tests.rs`. The controller
tests run against `api::Recorded`, which answers from objects the test
supplies, so no test needs a cluster. The router's tokenizer and service
have `test_support` modules for the same reason. `tests/fake_vllm.py`
publishes KV events for local runs, and `tests/e2e/smoke.sh` runs the real
engine with dummy weights on a kind cluster.

## Where new code goes

- A new signal for scoring. Collect it in the fleet, in `scrape` or
  `ingest`. Store it in the registry. Read it in the router's scoring. The
  picker stays pure.
- A new RPC or proto field. The proto, then `router::grpc` maps it to a
  service call. No logic in `grpc`.
- A new metric. The metrics struct of the crate where the event happens,
  through the helpers in `fleet::metrics`. Record it there.
- A new config setting. The config struct of the subsystem that reads it,
  next to that code. Validate it in `config`. Give it a type, not a raw
  string.
- A new CRD field or policy rule. Types and pure logic in
  `kinference-policy`. The controller writes the status. The router reads
  the snapshot. `kinfctl` gets it for free.
- A new engine kind. Metric names in `scrape`, event parsing in `ingest`.
- A new admin endpoint. `admin`. It reads snapshots. It never writes router
  state.
- Anything that talks to the API server. The controller, through
  `api::ApiServer`; never a `kube::Api` of its own.

## Rules for a move

- Behaviour stays the same. Tests, clippy and fmt stay green after every
  commit.
- One structural change per commit. Move, test, commit. Rename, test,
  commit.
- No constructor takes more than six arguments. Group the handles in a
  struct built once in `server`.
- A trait only where two implementations exist.
- A module's `//!` header says what it owns and the one rule a caller must
  respect. The crate's `lib.rs` lists its modules in a line each.
