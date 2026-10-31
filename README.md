# Kinference

Kinference routes LLM inference requests. A gateway sends Kinference each
request over gRPC. Kinference reads the workers' KV-cache events and engine
metrics, picks the model and the worker that will serve the request
fastest, and returns its choice to the gateway. Operators declare routing
policy on top of that with Kubernetes custom resources. The KV-cache index
and the event decoding come from NVIDIA's
[dynamo-kv-router](https://github.com/ai-dynamo/dynamo) crate; Kinference
adds discovery, the latency model and the control plane around it.

## How it fits

- The gateway calls the `Picker` gRPC service.
  [kinference.proto](crates/kinference-router/proto/kinference.proto)
  defines the contract, including the steps of each request and the error
  model.
- Workers are vLLM pods. An `InferenceWorkerSet` declares a set of
  workers, and an `InferencePool` groups worker sets.
- An `InferenceProvider` declares a provider outside the cluster.
- An `InferencePolicy` says which pool, model and backend a request may use.

The CRD schemas are in
[charts/kinference-crds](charts/kinference-crds/templates).

## Quick start

```sh
helm install kinference-crds oci://cr.kinference.ai/kinference-ai/charts/kinference-crds --devel
helm install kinference oci://cr.kinference.ai/kinference-ai/charts/kinference --devel \
  --set extension.enabled=true
```

Both commands pass `--devel` because every published chart is still a
pre-release, and Helm skips pre-releases by default. In the second chart,
`extension.enabled` creates the gateway `Extension` that puts Kinference in
the request path, so install the gateway first.
[charts/kinference/values.yaml](charts/kinference/values.yaml) documents
every setting beyond the defaults.

## Documentation

If you are new to the code, read these in order.

- [Architecture](ARCHITECTURE.md): the map of the code. The vocabulary,
  the entry points, what each crate owns and the rules that hold across
  crates.
- [Request flow](docs/request-flow.md): how the server boots, one pick
  from start to end, and the loops that keep the fleet and the policy
  current.
- [Local development](docs/local-development.md): the development loop on
  kind. How to build, install and run a CPU worker, what to look at, and
  how to iterate.
- [Contributing](CONTRIBUTING.md): style, commits and how to run the
  tests.

## kinfctl

`kinfctl check` finds mistakes in `InferencePolicy` manifests before you
apply them. The release workflow attaches the `kinfctl` binaries to each
release at
[Kinference-AI/kinference-releases](https://github.com/Kinference-AI/kinference-releases/releases).
To install the latest one:

```sh
curl -fsSL https://raw.githubusercontent.com/Kinference-AI/kinference-releases/main/install.sh | sh
```

To build it from source, run `just install-cli`.

## Scope

Kinference supports:

- vLLM workers, which Kinference finds through Kubernetes.
- KV-event ingest, with optional replay.
- Scraping of engine metrics.
- Tokenizing in process, or through vLLM's `/render` endpoint.
- Picking by the latency model.
- Providers outside the cluster.
- Routing by policy.
- Leader election for the control plane.

Kinference does not yet support several replicas that share picker state.

## License

Apache 2.0. See [LICENSE](LICENSE).
