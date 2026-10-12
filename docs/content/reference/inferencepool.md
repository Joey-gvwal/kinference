---
title: "InferencePool"
linkTitle: "InferencePool"
weight: 50
description: >
  The HTTPRoute backendRef: which destinations traffic may reach, and which
  policy chooses among them.
---

`kinference.buoyant.io/v1alpha1` · namespaced · short names `infpool`, `infpools`

What an `HTTPRoute` points its `backendRef` at. It bounds what a policy author
can reach, and a policy author cannot widen it — which is why it is
platform-owned and separate from the policy.

```yaml
apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePool
metadata:
  name: inference-all
  namespace: default
spec:
  destinations:
    - {kind: InferenceWorkerSet, name: gemma-4-12b}
    - {kind: InferenceWorkerSet, name: qwen-2-5-32b}
    - {kind: InferenceProvider,  name: databricks}
  policyRef:
    name: policy-3
```

## spec

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `destinations` | list (≤64) | no | every `InferenceWorkerSet` in this namespace |
| `policyRef` | object | no | serve as asked, on Local destinations |

### destinations

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `group` | string | no | `kinference.buoyant.io` |
| `kind` | `InferenceWorkerSet` \| `InferenceProvider` | yes | — |
| `name` | string | one of `name` / `selector` | — |
| `namespace` | string | no | this namespace; another needs a `ReferenceGrant` there, with `from` kind `InferencePool` |
| `selector.matchLabels` | map (≤64) | one of `name` / `selector` | — |

Exactly one of `name` or `selector`, enforced by CEL.

Omitting `destinations` entirely means every `InferenceWorkerSet` in this
namespace — **Local only**, so reaching a paid provider always requires naming
it. This is one of the three defaults that keep an off-cluster provider from
being reached by accident.

`kind` is required and has no default. A pool listing both an
`InferenceWorkerSet` and an `InferenceProvider` under the same name is rejected
as ambiguous rather than resolved to one of them, because a policy's
`backends.names` selects by bare string and would otherwise silently mean both.

### policyRef

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `group` | string | no | `kinference.buoyant.io` |
| `kind` | string | no | `InferencePolicy` |
| `name` | string | yes | — |

Exactly one [InferencePolicy]({{< relref "inferencepolicy.md" >}}) — not a set of objects
targeting this pool, so there is nothing to merge and no precedence rule to
learn.

`group` and `kind` are **checked**, not decorative. A ref naming anything other
than an `kinference.buoyant.io` `InferencePolicy` dangles with reason
`PolicyRefWrongKind` rather than resolving to whatever object has that name; they
carry defaults, so an omitted one is right by construction. That reason is
separate from `PolicyNotFound` because the fixes are in different places — the
policy may well exist, and what is wrong is the ref.

Omit it for the implicit default: serve every request as asked, on this pool's
Local destinations, ranking by predicted TTFT. That default is Local-scoped at
candidate generation, so a provider candidate is never even built for it.

Switching policies is a patch on the pool, not an edit to a policy:

```bash
kubectl -n default patch infpool inference-all --type=merge \
  -p '{"spec":{"policyRef":{"name":"policy-4"}}}'
```

```bash
kubectl -n default patch infpool inference-all --type=json \
  -p '[{"op":"remove","path":"/spec/policyRef"}]'
```

## status

| Field | Meaning |
| --- | --- |
| `destinationCount` | Resolved destinations |
| `models[]` | What traffic on this route can reach, aggregated across destinations |
| `models[].name` | Canonical, client-facing name |
| `models[].servedBy` | Destination names serving it, across both kinds |
| `models[].contextWindow` | `{min, max}` across destinations |
| `unresolvedDestinations[]` | `{kind, name, reason}` per destination that did not resolve |
| `parents[]` | Gateway API status, one entry per `parentRef` |
| `conditions` | The pool's own `Accepted`, `ResolvedRefs` |
| `parents[].conditions` | The same two, per parent — the handshake a gateway waits on |

The two condition lists are deliberate, and the top-level one is not redundant.
A pool with no `HTTPRoute` attached has **no parents**, so writing these only
per-parent left a pool with zero destinations and a dangling `policyRef` looking
exactly like a healthy one — its admission state existed nowhere. Whether the
router accepted this pool is a fact about the pool; the per-parent copy is the
per-route handshake. Both are computed from one judgement, so they cannot
disagree.

`status.models` answers the question an operator actually has — what can traffic
on this route reach — which no per-backend status can. Check it first when
debugging.

It is also where a canonical-name mismatch becomes visible. A worker set reports
what it was *observed* to serve; a provider reports what it *declares*, since
there is nothing to scrape. Nothing validates that the two agree, so a model
listed with a single entry in `servedBy` when you expected failover means the
names diverged.

`ResolvedRefs` must go false when a `destinations` entry does not resolve **or**
when `policyRef` dangles — two indirection hops, and three reasons:

| Reason | Meaning |
| --- | --- |
| `UnresolvedDestinations` | At least one `destinations` entry did not resolve |
| `PolicyNotFound` | `policyRef.name` names no `InferencePolicy` in this namespace |
| `PolicyRefWrongKind` | `policyRef.group`/`kind` name something else |

All three fail **open**: the pool still serves. Refusing every request over a
typo'd policy name is worse than serving it unfiltered, so the policy falls back
to serve-as-asked and the condition is how you find out.

### The readiness handshake

A gateway MUST NOT program a route until the referenced pool reports
`Accepted=True` on the matching `status.parents[]` entry. That closes the window
in which the gateway knows about a pool the router has not yet seen — during
which every request on the route would be picked with no knowledge of its
destinations.

## kubectl columns

```
NAME            POLICY     DESTINATIONS   STATE                                   AGE
inference-all   policy-3              4   Accepted                                7d
qwen-pool                             0   NoDestinations,UnresolvedDestinations   18d
```

`STATE` is `.status.state`, comma-separated:

| Token | Position | Means |
| --- | --- | --- |
| `Accepted` / `NoDestinations` | primary | Whether anything resolved to route to |
| `UnresolvedDestinations` | qualifier | At least one `destinations` entry did not resolve |
| `PolicyNotFound` | qualifier | `policyRef.name` names no policy here |
| `PolicyRefWrongKind` | qualifier | `policyRef.group`/`kind` name something else |

The dangle reasons are qualifiers because the pool still **routes** through
either: an unresolved destination leaves the others usable, and a bad `policyRef`
fails open to serve-as-asked.

`DESTINATIONS` counts only what **resolved**, so a broken destination makes the
number smaller rather than flagging anything: one-of-one and one-of-three both
print `1`. The `UnresolvedDestinations` token is what says so — it replaced a
separate `REFS` column.

## unresolvedDestinations reasons

Every way a listed destination fails to become routable. The pool still serves
through whatever else resolved — one broken destination never takes the others
with it.

| Reason | Means |
| --- | --- |
| `NotFound` | Nothing of that kind and name exists, or a selector matched nothing |
| `UnsupportedKind` | `kind` is absent or names neither `InferenceWorkerSet` nor `InferenceProvider`. There is no default: guessing between a pod fleet and a paid vendor is not a guess worth making |
| `AmbiguousName` | A worker set and a provider in this pool share a name. A policy's `backends.names` selects by bare string, so it would mean both |
| `CrossNamespaceNotGranted` | No `ReferenceGrant` in the target namespace — or the router is scoped to one namespace and cannot see the grant, which fails closed |
| `Incomplete` | The object exists but does not say enough to route to: a provider missing `backendRef.name`, `api` or a usable `models` row |
| `UnsupportedApiFormat` | A provider's `api` is not `OpenAI`. Nothing translates other wire formats |
| `NoMatchingPool` | The worker set exists but the router built no pool for it — usually the moment between creation and the next reconcile |
| `UnknownProfile` | The worker set names a routing profile the router does not define. The fix is in the **router's** config, not the CR — which is why it is not `NoMatchingPool` |
| `InvalidKvBlockSize` | The worker set's `kvBlockSize` is missing or zero |

The last three are properties of the worker set rather than of this pool, so they
also appear on the set's own `STATE` — see
[InferenceWorkerSet]({{< relref "inferenceworkerset.md" >}}).

## Diagnosing

| Symptom | Cause |
| --- | --- |
| `destinationCount` lower than the list | Check `unresolvedDestinations[].reason` — the complete set is below |
| `ResolvedRefs=False`, `PolicyNotFound` | `policyRef.name` names no policy in this namespace |
| `ResolvedRefs=False`, `PolicyRefWrongKind` | `policyRef.group`/`kind` name something other than an `kinference.buoyant.io` `InferencePolicy` |
| `STATE` blank | No status written at all — the pool is in no namespace the router watches |
| A model shows one `servedBy` entry, not two | Canonical names diverge between a worker set and a provider |
| Providers listed but never picked | The attached policy never admits them. See [InferencePolicy]({{< relref "inferencepolicy.md#select" >}}) |
