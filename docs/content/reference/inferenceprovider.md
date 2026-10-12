---
title: "InferenceProvider"
linkTitle: "InferenceProvider"
weight: 30
description: >
  The inference-specific half of an off-cluster destination: wire format and
  model aliasing. URL and credentials live elsewhere, on purpose.
---

`kinference.buoyant.io/v1alpha1` · namespaced · short names `infprov`, `infprovs`

An off-cluster destination, split from its transport. This object carries wire
format and model aliasing; the referenced
[GatewayBackend]({{< relref "gatewaybackend.md" >}}) carries base URL, credentials and TLS,
because nothing about them is inference-specific.

```yaml
apiVersion: kinference.buoyant.io/v1alpha1
kind: InferenceProvider
metadata:
  name: databricks
  namespace: default
spec:
  backendRef:
    name: databricks
  api: OpenAI
  models:
    - name: Qwen/Qwen2.5-32B-Instruct-Turbo
      aliases: [qwen-2.5, qwen-simple]
      contextWindow: 32768
```

## spec

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `backendRef.group` | string (≤253) | no | `gateway.linkerd.io` |
| `backendRef.kind` | string (≤253) | no | `GatewayBackend` |
| `backendRef.name` | string (≤253) | yes | — |
| `backendRef.namespace` | string (≤253) | no | this namespace |
| `api` | `OpenAI` | yes | — |
| `models` | list (1–64) | yes | — |
| `models[].name` | string (≤512) | yes | — |
| `models[].aliases` | list (≤32, each ≤512) | no | — |
| `models[].contextWindow` | integer ≥1 | no | — |

### backendRef

A `gateway.linkerd.io/GatewayBackend`. Cross-namespace requires a
`ReferenceGrant` in the target namespace naming this group and kind as `from`:

```yaml
apiVersion: gateway.networking.k8s.io/v1beta1
kind: ReferenceGrant
metadata: {name: providers-from-infra, namespace: vendors}
spec:
  from: [{group: kinference.buoyant.io, kind: InferenceProvider, namespace: infra}]
  to: [{group: gateway.linkerd.io, kind: GatewayBackend, name: shared-backend}]
```

A grant from `InferencePool` does not cover it: that is what a pool's
cross-namespace destination needs, a different reference.

The router validates the grant without ever reading the GatewayBackend — a grant
lives in the target namespace and names `from` and `to` by group and kind. That
is why the check belongs here and cannot be done by the gateway, which does not
know this object exists.

### api

Wire format the upstream speaks. `OpenAI` is the only permitted value.

This is not cosmetic, and it is the reason the field exists at all: Anthropic,
Bedrock and Vertex genuinely do not speak this format, so translating to them is
a real dataplane component that nothing here implements. Rather than accept a
declaration that would be silently ignored, the enum admits only what works. A
provider with any other value is rejected rather than resolved.

The narrowness is deliberate and not permanent. Most vendors either speak the
OpenAI format natively or publish a compatible endpoint — Databricks and Azure
do, Vertex has one, and Bedrock can be fronted by a proxy — so pointing
`baseURL` at that surface is the supported route. Widen the enum when something
actually translates, not before.

### models

Required, unlike an [InferenceWorkerSet]({{< relref "inferenceworkerset.md" >}})'s, and declared
rather than discovered. There is no `/v1/models` to poll: reaching one would need
the base URL and the credential that deliberately live out of the router's reach
on the GatewayBackend.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | string | yes | What this **vendor** calls it — an endpoint name, an ARN, a repo path |
| `aliases` | list (≤32) | no | Client-facing names that also resolve to `name` |
| `contextWindow` | integer ≥1 | no | Declared token limit |

A request matches a row by **`name` or any of its aliases**, and either way the
body is rewritten to `name` — the vendor's own spelling is what goes on the wire,
and it is routable under itself, so a row with no aliases is still reachable:

```yaml
models:
  - name: Qwen/Qwen2.5-32B-Instruct-Turbo    # reachable as-is
    aliases: [qwen-2.5, qwen-simple]         # and under either of these
    contextWindow: 32768
  - name: meta/llama-3.3-70b                 # no aliases; still reachable
```

There is no separate `upstreamName`, and that is the point: the row's identity and
the string it puts on the wire are one field, so they cannot drift apart.

Aliases are what make **failover** possible. Put the canonical name your local
worker sets serve into `aliases`, and one request name reaches both a local fleet
and the vendor. Nothing validates that they agree, which makes
`InferencePool.status.models[].servedBy` the place a mismatch shows — a name a
provider alone serves lists exactly one entry where you expected two.

`contextWindow` travels with the row, so every alias of a model inherits it.

Both directions work. Several aliases on one row are several spellings of one
model; the same alias on two rows makes them **interchangeable**, and the policy
ranks between them — the same mechanic a worker set uses for two checkpoints.

Unlike a worker set, `models[]` is **exhaustive**: there is no discovery to widen
it, so a model listed in neither `name` nor an alias does not exist here.

`contextWindow` is declared, not discovered: vendors rarely advertise one, and
the built-in admission check needs it. Set it below a local set's effective
window and long requests can never fail over here.

## status

| Field | Meaning |
| --- | --- |
| `state` | At-a-glance state, comma-separated — see [kubectl columns](#kubectl-columns) |
| `conditions` | `Accepted`, `ResolvedRefs`, `Ready` |

That is the whole of it. **The observed TTFT p95 is not published.** The router
computes it — it is the `EstimatedTTFT` ranking input for a provider and what a
`backend.observedTTFTP95` predicate compares against — but only as a number on
the routing path, never formatted into status.

A policy author writing a threshold against it reads it from **metrics** instead
of from the object:

```promql
histogram_quantile(0.95,
  sum by (le, provider) (rate(kinference_provider_ttft_ms_bucket[5m])))
```

`kinference_provider_ttft_ms{provider,model}` is fed from the same successful
`ReportFirstToken` samples the router's own p95 is computed from, so the two
agree by construction. A time series suits the value better than status did: the
field was absent until the first report — blank on every provider yet to serve —
and once populated it was a single duration string with no history, where the
metric gives any quantile over any window, per model.

Failed reports are excluded. A 401 or a 5xx carries a time-to-*error*, and
folding that into a latency distribution would make a failing vendor look fast;
those surface on the provider's `Suppressed` / `Backend*` state instead.

The predicate semantics are unchanged, and worth restating because they are the
part that still matters: an **absent** value does not match, so an exclusion
written against `observedTTFTP95` does not fire on a provider that has never been
dispatched to. That is the intended direction — a new provider is not excluded by
a signal it has not had the chance to earn.

## Failure is reported, not observed

The router hands the caller a reference to an object it can never read. So an
unresolvable `backendRef`, a missing Secret key, or a denied `ReferenceGrant` in
the gateway's own resolution path is **structurally invisible** to it: the
provider would keep winning picks forever while every resource the router can
read reads green.

The wire protocol closes that loop. The caller reports what only it can see:

| Report | Meaning |
| --- | --- |
| `Abort(BACKEND_UNRESOLVED)` | `backendRef` names nothing the caller can resolve |
| `Abort(BACKEND_UNAUTHORIZED)` | Resolved, but the credential was refused |
| `ReportFirstToken` with `upstream_status >= 500` | Reached, and failing |

Three consecutive failures suppress that provider's candidates for a cooling-off
period; one success resets the count. Without that suppression a broken
`backendRef` is a permanent black hole, because there is no dispatch retry.

Suppression is per-replica and short-lived, so it is not reflected in
`status`. A provider that never seems to be picked, with no `ResolvedRefs`
complaint, is the shape to look for:

```bash
kubectl -n default get infprov databricks -o jsonpath='{.status.conditions}' | jq
```

## kubectl columns

`BACKEND` and `API` are spec, so without a status column the row was identical
whether this provider was serving, suppressed after repeated dispatch failures, or
naming a `GatewayBackend` that does not exist — on the one object whose whole
purpose is surfacing health the router cannot observe for itself.

```
NAME         BACKEND      API      STATE                                     AGE
databricks   databricks   OpenAI   Accepted                                  7d
vertex       vertex       OpenAI   Accepted,BackendUnauthorized,Suppressed   2h
```

`STATE` is `.status.state`, comma-separated:

| Token | Position | Means |
| --- | --- | --- |
| `Accepted` | primary | Well-formed and resolved into a pool destination |
| `UnsupportedApiFormat` | primary | `api` is not `OpenAI` |
| `Incomplete` | primary | No `backendRef.name`, no `api`, or no `models` row carrying a `name` |
| `CrossNamespaceNotGranted` | primary | `backendRef` crosses namespaces with no `ReferenceGrant` |
| `AmbiguousName` | primary | A worker set in the same pool has this name |
| `BackendUnresolved` / `BackendUnauthorized` | qualifier | From the caller's `Abort` reports — the only channel that can see these |
| `Suppressed` | qualifier | Repeated dispatch failures have stopped it producing candidates |

`Suppressed` is the token that matters: the object is well-formed, every resource
the router can read looks healthy, and it is producing **nothing**. It replaced
separate `Refs` and `Serving` columns, and a `Models` column whose `[*]` JSONPath
showed one of N.

There is no `TTFTp95` column, and no field behind one — see [status](#status).

`Accepted` is written on the healthy path too, not only on rejection. Because
status is a merge patch and `conditions` is replaced wholesale, omitting it there
did not leave a stale `False` behind — it did something quieter: a provider you
fixed lost the condition entirely instead of flipping to `True`, and one that was
never broken never had it at all.

## Diagnosing

| Symptom | Cause |
| --- | --- |
| Rejected on create | `api` is not `OpenAI`; the schema enum admits nothing else |
| `STATE: UnsupportedApiFormat` | Same cause, on an object that bypassed admission. The router parses it and then refuses it, rather than failing to parse — a parse error would take down discovery for every provider in the namespace |
| `ResolvedRefs=False` | `backendRef` dangles, or a cross-namespace ref has no `ReferenceGrant` |
| Never picked, conditions clean | Suppressed after repeated dispatch failures, or losing on rank |
| Never picked, and this is its first traffic | Under a `PredictedTTFT` tier a provider carries no value and is dropped from that tier. Use `EstimatedTTFT`, or give providers their own tier |
| Requests fail with a model error upstream | `name` is wrong. The router substitutes it into the body verbatim |
| Long requests never fail over here | `contextWindow` is below the local set's effective window |
