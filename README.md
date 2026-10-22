# kinference

kinference routes LLM inference requests. A gateway hands it each request
over gRPC; it picks the model and the worker that will serve it fastest,
from the workers' KV-cache events and engine metrics, and hands back the
winner. Kubernetes custom resources layer routing policy on top. The KV-cache
index and event decoding come from NVIDIA's
[dynamo-kv-router](https://github.com/ai-dynamo/dynamo) crate; kinference
wraps it with discovery, the latency model and the control plane.

## How it fits

- The gateway calls the `Picker` gRPC service. The contract, including the
  per-request flow and error model, is in
  [kinference.proto](crates/kinference-router/proto/kinference.proto).
- Workers are vLLM or SGLang pods, declared with an `InferenceWorkerSet` and
  grouped into an `InferencePool`.
- Off-cluster providers are declared with an `InferenceProvider`.
- An `InferencePolicy` says which pool, model and backend a request may use.

The schemas are in [charts/kinference-crds](charts/kinference-crds/templates).

## Quick start

```sh
helm install kinference-crds oci://ghcr.io/kinference-ai/charts/kinference-crds
helm install kinference oci://ghcr.io/kinference-ai/charts/kinference \
  --set extension.enabled=true
```

The second chart's `extension.enabled` creates the gateway `Extension` that
puts kinference in the request path; it needs the gateway installed first.
Everything past the defaults is documented in
[charts/kinference/values.yaml](charts/kinference/values.yaml).

## Documentation

Read in this order if you are new to the code.

- [Architecture](ARCHITECTURE.md): the map. Vocabulary, entry points, what
  each crate owns and the rules that hold across them.
- [Request flow](docs/request-flow.md): boot, one pick end to end, and
  the loops that keep the fleet and the policy current.
- [Contributing](CONTRIBUTING.md): style, commits, running the tests.

## kinfctl

`kinfctl check` finds mistakes in `InferencePolicy` manifests before you
apply them. Binaries are attached to each release at
[Kinference-AI/kinference-releases](https://github.com/Kinference-AI/kinference-releases/releases):

```sh
curl -fsSL https://raw.githubusercontent.com/Kinference-AI/kinference-releases/main/install.sh | sh
```

Or from source: `just install-cli`.

## Scope

Supported: vLLM and SGLang workers found through Kubernetes, KV-event ingest
with optional replay, engine metrics scrape, in-process or `/render`
tokenizing, latency-model picking, off-cluster providers, policy-driven
routing, leader election for the control plane.

Not yet: more than one replica sharing picker state.

## License

Apache 2.0. See [LICENSE](LICENSE).
