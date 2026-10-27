# Local development

How to run kinference on a kind cluster on your own machine, point it at
a real vLLM worker, and watch kinference discover the worker, ingest the
KV-cache events that the worker publishes, and write status. CI runs the
same loop, through the recipes in the [justfile](../justfile).

## What you need

`docker`, `kind`, `kubectl`, `helm` and `just`. Docker builds the image,
so you need the Rust toolchain only for `just lint` and `just test-unit`;
on the host those two also need `libzmq3-dev`, `pkg-config` and `g++`.

The first `just image` compiles the `dynamo-kv-router` git dependency and
takes a while; BuildKit cache mounts keep later builds incremental. The
worker below pulls a CPU vLLM image of about 600 MB and requests 500m CPU
and 1 GiB of memory.

## Bring it up

```sh
just up
```

The recipe builds `kinference:latest`, creates the kind cluster
`kinference-smoke`, loads the image into the cluster, installs
`charts/kinference-crds` and `charts/kinference` from source into the
`default` namespace, and waits until the Deployment is Available. kind
makes `kind-kinference-smoke` the current kubectl context.

The recipe installs the chart with `extension.enabled` off, so you need
no gateway. Without a gateway, nothing sends requests through kinference;
the section "See it work" lists what you can see anyway, and the section
"A full request path" says how to put a gateway in front.

## Add a worker

```sh
just test-smoke
```

The recipe applies the manifests in
[tests/e2e/manifests](../tests/e2e/manifests) and runs
[tests/e2e/smoke.sh](../tests/e2e/smoke.sh). The manifests are:

- `dummy-vllm.yaml`: vLLM on CPU with `--load-format dummy`, serving
  `HuggingFaceTB/SmolLM2-135M-Instruct` with random weights. The responses
  are garbage, but `/metrics`, `/v1/models` and the KV events on port 5557
  are real. The CPU backend uses 128-token blocks.
- `workerset.yaml`: the `dummy` InferenceWorkerSet. It points at the
  `dummy-vllm` Service, sets `kvBlockSize: 128` and declares the alias
  `dummy`.
- `pool.yaml`: the `smoke` InferencePool, with that worker set as its one
  destination.

The script then waits for the worker, port-forwards kinference and the
worker, checks that `/readyz` reports a ready endpoint, sends one long
completion so that vLLM stores full blocks and publishes events, waits
until `kinference_index_blocks` rises above zero, and checks that the
worker set is `Accepted`. Every wait is bounded; when a step fails, the
script says which step timed out.

You can also apply the manifests yourself and do the rest by hand:

```sh
kubectl apply -f tests/e2e/manifests
kubectl rollout status deployment/dummy-vllm --timeout=420s
```

## See it work

Port-forward both admin ports:

```sh
kubectl port-forward svc/kinference 9001:admin &
kubectl port-forward svc/dummy-vllm 8000:http &
```

`/readyz` reports the fleet:

```sh
curl -s localhost:9001/readyz
```

```
ready
worker sets: 1, with ready endpoints: 1
```

The log shows discovery, the first scrape and, at `trace`, every KV
event:

```sh
kubectl logs deployment/kinference -f
```

Send a prompt that is longer than one block, then watch the index fill.
A short prompt publishes nothing, because vLLM stores only full blocks,
and a prompt that the engine has already seen publishes nothing either,
because its blocks are cached already. The smoke test has sent one
prompt, so use different text:

```sh
PROMPT=$(printf 'Every good boy deserves fudge and the cat sat on the mat by the fire tonight. %.0s' {1..60})
curl -s localhost:8000/v1/chat/completions -H 'content-type: application/json' \
  -d "{\"model\":\"HuggingFaceTB/SmolLM2-135M-Instruct\",\"max_tokens\":8,\"messages\":[{\"role\":\"user\",\"content\":\"$PROMPT\"}]}" > /dev/null
curl -s localhost:9001/metrics | grep -E '^kinference_(workers|index_blocks|index_events_total)[ {]'
```

kinference refreshes `kinference_index_blocks` from a throttled dump of
the radix tree, so allow about ten seconds.

The controller's view is on the custom resources and on the read-only
policy API:

```sh
kubectl get inferenceworkersets,inferencepools
kubectl get inferenceworkersets dummy -o yaml
curl -s localhost:9001/api/policy/context | jq
```

`context` lists every pool with its destinations and its live candidates,
which are the inputs that an `InferencePolicy` is written against.
`kinfctl check` reads the same resources from the cluster to check a
policy draft; `kinfctl check --help` shows the invocation.

## Iterate

A code change means a new image, which you load into the cluster, and a
restart. The chart pulls `IfNotPresent` on a fixed tag, so loading the
same tag again replaces the image:

```sh
just image
kind load docker-image kinference:latest --name kinference-smoke
kubectl rollout restart deployment/kinference
```

A config change goes through the chart. The config is the `config` block
of [charts/kinference/values.yaml](../charts/kinference/values.yaml),
which the chart renders to JSON; a checksum annotation on the pod restarts
the pod when the rendered config changes:

```sh
helm upgrade kinference charts/kinference --reuse-values \
  --set config.metrics_poll_ms=500
```

The same command sets the chart's other knobs: `logLevel` sets `RUST_LOG`
(`kinference=trace,dynamo_kv_router=trace` for KV-event debugging),
`enableReplay` makes kinference fetch the event history on attach, and
`observationsCsv` names the file that receives the rows of predicted
versus observed latency. The CSV is off by default; enable it with a path:

```sh
helm upgrade kinference charts/kinference --reuse-values \
  --set observationsCsv=/var/log/kinference/latency.csv
```

The rows live on an emptyDir that goes away with the pod, so copy them out
before a restart:

```sh
kubectl cp "$(kubectl get pod -l app.kubernetes.io/name=kinference -o name | cut -d/ -f2):/var/log/kinference/latency.csv" latency.csv
```

The unit tests and the lints run on the host, outside the cluster:

```sh
just test-unit
just lint
```

## When it does not

- `dummy-vllm` stays Pending: the node has less than the 500m CPU and
  1 GiB that the worker requests. kind's own pods reserve most of a
  two-core machine.
- `/readyz` says `with ready endpoints: 0`: the worker is not Ready, or
  the Service's port names are not the names that the worker set
  declares. `kubectl get endpointslices -l
  kubernetes.io/service-name=dummy-vllm` shows what discovery sees.
- `/readyz` returns 503: policy is on and the CRD watch has not listed
  yet, usually because the CRDs chart is not installed.
- `kinference_index_blocks` stays 0: the prompt was shorter than a block;
  or `kvBlockSize` disagrees with the block size that the engine
  publishes, which kinference logs as a block size mismatch and the
  engine logs as `Block not published. Block size must be N tokens`; or
  someone set `KINFERENCE_DISABLE_INGEST`.

## A full request path

To send real requests, you put kinference behind the gateway as the
`Picker` extension. The gateway repo's
`documents/kinference-manual-verification.md` walks through it: that
repo's `just up` brings up a kind cluster with the gateway; you load this
repo's image into that cluster, install the chart with
`extension.enabled=true`, apply the same worker set and pool as above,
name the pool in an `HTTPRoute`, and `curl` the gateway. The request
lands on the worker through a pick.

## Tear down

```sh
just down
```
