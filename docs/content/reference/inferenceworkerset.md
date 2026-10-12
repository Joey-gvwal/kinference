---
title: "InferenceWorkerSet"
linkTitle: "InferenceWorkerSet"
weight: 20
description: >
  A set of local model-server pods, named by the Service that fronts them.
---

`kinference.buoyant.io/v1alpha1` · namespaced · short name `iws`

A set of local model-server pods (vLLM, SGLang), named by the Service that
fronts them. Creating one materializes a routable destination; deleting one
retires it. No router restart, no configuration change.

```yaml
apiVersion: kinference.buoyant.io/v1alpha1
kind: InferenceWorkerSet
metadata:
  name: qwen-3-6-35b-a3b
  namespace: default
spec:
  profile: default
  kvBlockSize: 16
  serviceRef:
    name: qwen-3-6-35b-a3b-vllm
    port: http
  kvEvents:
    port: kv-pub
  models:
    - name: Qwen/Qwen3.6-35B-A3B
      aliases: [qwen3.6]
```

## The Service is read for EndpointSlices only

Routing always targets an individual endpoint, so `clusterIP`,
`sessionAffinity` and `trafficDistribution` are ignored. Prefer a headless
Service (`clusterIP: None`) so the object's semantics match what is done with
it — the same use a StatefulSet makes of its headless Service.

EndpointSlice conditions are authoritative for membership, and the tri-state
matters:

| Endpoint state | Serves in-flight | New candidates |
| --- | --- | --- |
| `ready` | yes | yes |
| `serving`, `terminating` | yes | no |
| neither | no | no |

That distinction is why this is a Service reference rather than a pod selector.
Inference requests run for tens of seconds, so draining correctly is a routing
concern, and the endpoints controller already tracks it.

## spec

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `profile` | string (1–253) | yes | — |
| `kvBlockSize` | integer (1–4096) | yes | — |
| `serviceRef` | object | yes | — |
| `serviceRef.name` | string (≤253) | yes | — |
| `serviceRef.port` | string or integer | yes | — |
| `kvEvents.port` | string or integer | yes, if `kvEvents` is set | — |
| `kvEvents.replayPort` | string or integer | no | port named `kv-replay`, else replay disabled |
| `metricsPort` | string or integer | no | port named `metrics`, else `serviceRef.port` |
| `modelDiscovery.mode` | `Endpoint` \| `Static` | no | `Endpoint` |
| `modelDiscovery.interval` | duration | no | `30s` |
| `models` | list (≤64) | no | — |
| `models[].name` | string (≤512) | yes | — |
| `models[].aliases` | list (≤32, each ≤512) | no | — |

There is no `modelDiscovery.path`: the router requests `/v1/models` and nothing
else. There is no top-level `replayPort` either — see [kvEvents](#kvevents).

### profile

Which named routing profile in the router's own configuration supplies this
set's latency-model coefficients, picker policy and tokenization mode.

Required and deliberately not defaulted: routing tuning is calibrated against a
model class *and* its hardware, so no value is right for an arbitrary fleet.
Naming a profile that is not defined is an `Accepted=False` /
`UnknownProfile` condition rather than a silent fallback — falling back would
route real traffic on coefficients nobody chose.

### kvBlockSize

Tokens per block in the KV-cache events these engines **publish**. 16 for
standard vLLM (Llama/Qwen/Mistral), 256 for Gemma 4, 1 for SGLang started with
`--page-size 1`.

Not necessarily the engine's `--block-size`. Gemma 4 reports `block_size=64` in
`cache_config_info` and publishes 256-token blocks. Read it off a real cache
event, not the launch flag.

A wrong value fails in two stages, and which one you see depends on traffic:

1. **Silent.** Every mismatched block is dropped before insertion. Events
   arrive, the radix tree stays empty, `predictedTTFT` loses most of its
   fidelity, and picks degrade to load-only with no error. The `WARN` naming the
   real block size is capped at three per endpoint.
2. **Draining.** After three *consecutive* mismatched events the endpoint is
   evicted; a single matching event resets the counter.

So a quiet fleet can sit in state 1 indefinitely — events fire on newly stored
blocks, and cached prefixes store nothing — while a busy one drains the whole
set, since every endpoint in it shares one configuration. The router never
adopts the observed value.

The tell is in the router's logs:

```
Block not published. Block size must be 16 tokens to be published.
Block size is: M
```

Set `kvBlockSize` to `M`.

It lives on this object rather than in the profile precisely because it mirrors
a flag on this Deployment; a shared profile carrying it would drain every set
that disagreed.

### kvEvents

The socket on which the model servers publish KV cache events (`BlockStored` /
`BlockRemoved` and equivalents). `port` names a port on the **same** Service as
`serviceRef` — these are the same pods, so a second Service would be a second
name for one thing.

Absence is not a benign default. Without cache events there is no prefix model:
`prefixCacheHitRatio` is 0 for every candidate and `predictedTTFT` loses most of
its fidelity, which is the signal the whole select stage rests on. Omit it only
for a worker set whose servers genuinely do not publish events, and expect
`status.kvEvents.configured: false` to say so.

**`replayPort` lives in this block, not beside `metricsPort`.** It is the other
half of *this* transport rather than a separate one: it replays the stream `port`
publishes, and it resolves from the port named `kv-replay`. Because it is nested,
"a replay socket on a set that publishes nothing" is unrepresentable — and a set
that declares no `kvEvents` resolves neither socket.

Unset, replay is disabled and only live events are ingested, so an endpoint's
existing cache contents stay invisible until they are re-stored.

Present-but-portless is refused rather than read either silent way. `kvEvents: {}`
is an `Accepted=False` / `KvEventsMissingPort` condition, because the block *is*
the declaration that these engines publish and it does not say where. Guessing a
default socket that never speaks, or quietly downgrading the set to "publishes
nothing", would both serve traffic while lying about it. Omit the whole block
instead.

### Ports: prefer names

`serviceRef.port`, `metricsPort`, `kvEvents.port` and `kvEvents.replayPort` all
accept a name or a number. Prefer the name.

A number here is a **Service** port, but endpoints carry **target** ports. The
router matches a number against the slice's ports and, failing that, dials it
verbatim with a warning. A worker typically exposes the OpenAI API, Prometheus
metrics and the KV-events socket, so names are also what keeps
which-is-which legible. `appProtocol` is read from the Service port (`http`,
`kubernetes.io/h2c`) and is not restated here.

### models

Optional. Only needed to expose a discovered model under a different name, or
to declare models when discovery is unavailable. Omit it and every discovered
model is routable under the exact string `/v1/models` reports.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | string | yes | What this set reports on `/v1/models`, and what goes in the outbound body |
| `aliases` | list (≤32) | no | Client-facing names that also resolve to `name` |

```yaml
models:
  - name: Qwen/Qwen2.5-32B-Instruct-AWQ    # what the engine reports
    aliases: [qwen-2.5, qwen-simple]       # and two shorter ways to ask for it
```

Aliases are **additive, never a whitelist**. `name` stays routable on its own,
and declaring a row for one model does not hide the engine's other models — which
is the same reason the whole `models` block can be omitted.

The same alias on two rows declares those checkpoints **interchangeable**: one
request name fans out into two candidates and the policy ranks between them.

This is the same shape as [`InferenceProvider.models`]({{< relref "inferenceprovider.md" >}}),
deliberately — one idea, one spelling across both kinds. The difference is that a
provider's list is exhaustive, because it has no discovery to widen it, while this
one only *adds* to what `/v1/models` already reports.

Alias resolution runs on the policy path only: it needs the policy feature
enabled *and* an `InferencePool` named on the request. With policy off, aliases
are inert and clients must use the name the engine reports.

## status

| Field | Meaning |
| --- | --- |
| `models[].upstreamName` | What the engines actually report — the identity of the entry, and the only name here that was observed rather than declared |
| `models[].aliases` | Client-facing names `spec.models` maps onto this upstream, sorted. The only place the router reports which declarations actually resolved |
| `models[].readyEndpoints` | Ready endpoints serving this model |
| `models[].contextWindow` | Effective window (vLLM `max_model_len`) across ready endpoints, as `{min, max}` — a range, because a rollout that changes `--max-model-len` leaves endpoints disagreeing |
| `endpoints.ready` | Candidate endpoints |
| `endpoints.terminating` | Draining: still serving in-flight requests, no longer candidates |
| `kvEvents.configured` | False when `spec.kvEvents` is unset |
| `kvEvents.subscribedEndpoints` | Endpoints the router is currently receiving events from |
| `referencedBy` | Pools listing this set in `spec.destinations`, sorted |
| `conditions` | `Accepted`, `Ready`, `Referenced` |

Status is written for **every** worker set the router lists, not only those a
pool names. Reaching them through pool destinations meant an unreferenced set was
never patched at all — no `status` key on the object — so a set serving traffic
was indistinguishable from one the router had never seen.

The three conditions answer three different questions, and they move
independently:

| Condition | Question |
| --- | --- |
| `Accepted` | Is the object valid and materialized as a routable pool? |
| `Ready` | Does it have ready endpoints? |
| `Referenced` | Does any `InferencePool` route to it — i.e. does pool-scoped policy apply? |

`referencedBy` being empty is **not** an error, and `Referenced=False` does not
mean unreachable: a `Pick` naming no `InferencePool` routes across every set the
router serves, so an unreferenced set still takes traffic. What it means is that
no pool-scoped policy applies. `Accepted` therefore stays `True` — reporting a
set that is carrying traffic as unaccepted would be worse than saying nothing.

(This is deliberately *not* the shape an unreferenced `InferencePolicy` takes,
where `Accepted=False` is the truth, because a policy no pool names does nothing
at all.)

One `status.models` entry per model the engines report, so a fleet also serving
LoRA adapters, or launched with several `--served-model-name` values, reports one
entry each and `readyEndpoints` shows a partial rollout.

`kvEvents.subscribedEndpoints` short of `endpoints.ready` means `predictedTTFT`
is running degraded for those candidates, which is otherwise invisible.

## kubectl columns

```
NAME               READY   STATE                            AGE
qwen-3-6-35b-a3b   3       Accepted                         7d
gemma-4-12b        0       Accepted,NotReady                7d
orphan             0       Accepted,NotReady,Unreferenced   19s
no-such-profile    0       UnknownProfile                   2m
```

`READY` is `.status.endpoints.ready`; `STATE` is `.status.state`, a
comma-separated list of tokens:

| Token | Position | Means |
| --- | --- | --- |
| `Accepted` | primary | Valid and materialized as a routable pool |
| a rejection reason | primary | Refused; the rejection *is* the verdict, so it stands alone (see below) |
| `NotReady` | qualifier | No ready endpoints |
| `Unreferenced` | qualifier | No pool names it, so no pool-scoped policy applies |

The rejection reasons, which are the complete set of ways a set produces no pool
at all:

| Reason | `spec` is |
| --- | --- |
| `MissingProfile` | missing `profile` |
| `UnknownProfile` | naming a profile the router does not define |
| `MissingKvBlockSize` / `InvalidKvBlockSize` | missing `kvBlockSize`, or giving one that is zero |
| `MissingServiceRef` | missing `serviceRef.name`, so there is nothing to watch |
| `KvEventsMissingPort` | declaring `kvEvents` with no `port` |
| `ModelMissingName` | carrying a `models` row with no `name` |

The last two exist because the router parses **permissively** — every spec field
is optional in the parser, whatever the schema says — so an object that bypassed
admission reaches it intact and is refused here by name rather than failing to
parse. That matters more than it sounds: `kube::runtime::watcher` deserializes
list pages *wholesale*, so a parse error on one object takes discovery down for
every set in the namespace. Parse it, then refuse it.

A blank `STATE` means no status was written **at all** — a different problem from
any value it can hold, and most likely the set is in a namespace the router does
not watch.

There are no `POOLS` or `MODELS` columns. Both used `[*]` JSONPaths, and a printer
column with `[*]` renders only the **first** array element — so a set in two pools
reported one pool, silently. `status.referencedBy` and `status.models[]` are
complete in `get -o yaml`, and `Unreferenced` covers the at-a-glance case a
`POOLS` column existed for.

## Diagnosing

| Symptom | Cause |
| --- | --- |
| `Accepted=False`, `UnknownProfile` | `spec.profile` names no profile in the router's config |
| `Accepted=False`, `KvEventsMissingPort` | `spec.kvEvents` is present with no `port`. Omit the block, or name the port |
| `Accepted=False`, `ModelMissingName` | A `spec.models` row has no `name`. Most likely a schema change pruned it — see [Installing]({{< relref "_index.md" >}}) |
| `Accepted=False`, `MissingProfile` / `MissingServiceRef` | The field is absent. Only reachable by an object that bypassed admission |
| `STATE` blank in `kubectl get iws` | No status written at all — the set is in no namespace the router watches |
| `STATE` has `Unreferenced` | No pool names it. Not a fault; the `Referenced` condition carries the patch command |
| `Ready=False`, `NoReadyEndpoints` | The Service has no ready endpoints |
| `status.endpoints.ready: 0` | `serviceRef.name` matches no Service, or it has none ready. Check `kubectl get endpointslices -l kubernetes.io/service-name=<name>` |
| Events arrive, `index.blocks` stays 0 | `kvBlockSize` mismatch, stage 1. Grep the logs for the published size |
| Set drains after a few `BlockStored` | `kvBlockSize` mismatch, stage 2 |
| Endpoints added but unresponsive | Pod IPs unreachable from the router pod. Check NetworkPolicies |
| A written alias never resolves | Check it against `status.models[].aliases`, and confirm the request named a pool |
