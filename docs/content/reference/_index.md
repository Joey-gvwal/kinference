---
title: "API reference"
linkTitle: "Reference"
weight: 10
description: >
  The five custom resources that configure inference routing: what each one owns,
  how they compose, and which defaults are load-bearing.
---

Routing is configured by five custom resources across two API groups. All are
namespaced, all are `v1alpha1`, and all carry a `status` subresource.

| Kind | Group | Short names | Answers |
| --- | --- | --- | --- |
| [InferenceWorkerSet]({{< relref "inferenceworkerset.md" >}}) | `kinference.buoyant.io` | `iws` | Where the local model servers are, and how to read their telemetry |
| [InferenceProvider]({{< relref "inferenceprovider.md" >}}) | `kinference.buoyant.io` | `infprov`, `infprovs` | Which off-cluster models exist, and what the vendor calls them |
| [GatewayBackend]({{< relref "gatewaybackend.md" >}}) | `gateway.linkerd.io` | `gwbe` | Where an off-cluster backend is, and how to authenticate to it |
| [InferencePool]({{< relref "inferencepool.md" >}}) | `kinference.buoyant.io` | `infpool`, `infpools` | Which destinations traffic on a route may reach, and which policy chooses |
| [InferencePolicy]({{< relref "inferencepolicy.md" >}}) | `kinference.buoyant.io` | `infpol`, `infpols` | How to choose among them |

Not API objects, but the other half of the operator surface:
[Configuration]({{< relref "configuration.md" >}}) — the TOML file the router
reads at startup;
[Metrics]({{< relref "metrics.md" >}}) — what the router exposes on `/metrics`,
including the several signals that appear *only* there and in no CR status;
[kinfctl]({{< relref "kinfctl.md" >}}) — the command-line checker for
`InferencePolicy` manifests, run before applying them; and the
[Policy API]({{< relref "policy-api.md" >}}) — the read-only JSON endpoints
behind the dashboard's policy explorer, for programs that want the same answers.

## How they compose

```
HTTPRoute.backendRef
        |
        v
   InferencePool ---- policyRef ----> InferencePolicy
        |                                  (how to choose)
        | destinations[]
        |
        +--> InferenceWorkerSet --> Service --> EndpointSlices --> pods
        |
        +--> InferenceProvider --- backendRef ---> GatewayBackend --> Secret
                                                   (URL, auth)
```

Each hop narrows the previous one. An `InferencePool` bounds what is reachable;
an `InferencePolicy` chooses within that bound and cannot widen it. That split is
the reason the two objects exist separately: the pool is platform-owned, the
policy is the only object a tenant needs write access to.

## Ownership

| Object | Typically owned by | Why |
| --- | --- | --- |
| `InferenceWorkerSet` | Platform | Mirrors flags on a Deployment it must stay in sync with |
| `InferenceProvider` | Platform | Declares which vendor models are sanctioned |
| `GatewayBackend` | Gateway operator | Holds the credential reference |
| `InferencePool` | Platform | Bounds the blast radius of every policy attached to it |
| `InferencePolicy` | Tenant | Chooses within a bound it cannot change |

## The credential split

The router reads `InferenceProvider` and never `GatewayBackend`. It hands the
caller a reference to the backend and lets the caller resolve it, so base URL,
credentials and TLS never enter the router's address space. Its RBAC grants no
`secrets`, nothing in the core (`""`) API group but read access to `services`,
and no `gatewaybackends` — and because Kubernetes RBAC is allowlist-only, those
omissions *are* the guarantee:

```bash
for r in secrets gatewaybackends.gateway.linkerd.io serviceaccounts/token; do
  kubectl auth can-i get "$r" --as=system:serviceaccount:default:kinference
done   # every line must print: no
```

The split runs both ways. The gateway understands `GatewayBackend` and not
`InferenceProvider`; the router understands `InferenceProvider` and not
`GatewayBackend`. One consequence is worth stating plainly: a dangling
`backendRef` or a wrong Secret key is invisible to the router, which is why the
caller reports it back over the wire protocol rather than the router discovering
it. See [InferenceProvider]({{< relref "inferenceprovider.md#failure-is-reported-not-observed" >}}).

## Defaults that are load-bearing

Three defaults conspire so that **an off-cluster provider is never reached by
accident**. Reaching one always takes a deliberate act.

| Omitted | Means |
| --- | --- |
| `InferencePool.spec.destinations` | Every `InferenceWorkerSet` in this namespace — Local only, never a provider |
| `InferencePool.spec.policyRef` | Serve every request as asked, on this pool's Local destinations |
| `InferencePolicy.spec.unmatched` | `ServeLocal` — traffic matching no rule is served locally, not refused |

The corollary: a rule set meant to be exhaustive must set `unmatched: Reject`,
or an unlisted tenant is served rather than refused.

## Conditions

Every kind reports standard `metav1.Condition` entries. `lastTransitionTime` is
preserved when nothing transitions, so its age is meaningful.

Each kind also publishes `status.state` — those same conditions collapsed into
comma-separated tokens for the `STATE` printer column: the primary verdict, then
any qualifier that is true (`Accepted,NotReady,Unreferenced`). It is a joined
string rather than a list because a printer column with a `[*]` JSONPath renders
only the **first** array element, so a list would drop every token after the first
with no trace. It is never empty when status is written, which is what keeps a
blank `STATE` meaning "no status at all" — most often the object is in a namespace
the router does not watch.

| Type | On | `False` means |
| --- | --- | --- |
| `Accepted` | all kinds | The object was rejected, or (on a policy) nothing routes through it |
| `ResolvedRefs` | `InferencePool`, `InferenceProvider` | A reference dangles — a destination, a `policyRef`, or a `backendRef` |
| `Ready` | `InferenceWorkerSet`, `InferenceProvider` | Nothing is currently servable |
| `Referenced` | `InferenceWorkerSet` | No pool names it, so no pool-scoped policy applies — **not** unreachable |

Two kinds answer "is anything pointing at me", and they answer it differently
on purpose. An unreferenced `InferencePolicy` reports `Accepted=False`: a policy
no pool names does nothing at all. An unreferenced `InferenceWorkerSet` stays
`Accepted=True` and reports `Referenced=False`, because a `Pick` naming no pool
routes across every set the router serves — so the set is still taking traffic,
and calling it unaccepted would be a lie.

Status is written for every object the router lists, referenced or not. Reaching
them only through pools is what once left an unattached policy with a blank rule
count and an unreferenced worker set with no `status` key at all.

A gateway MUST NOT program a route until the referenced pool reports
`Accepted=True` on the matching `status.parents[]` entry. That handshake closes
the window in which the gateway knows about a pool the router has not yet seen.
