---
title: "InferencePolicy"
linkTitle: "InferencePolicy"
weight: 60
description: >
  How to choose, given the destination set an InferencePool allows. The only
  object a tenant needs write access to.
---

`kinference.buoyant.io/v1alpha1` · namespaced · short names `infpol`, `infpols`

How to choose among the destinations an [InferencePool]({{< relref "inferencepool.md" >}})
allows. It is the only object a tenant needs write access to, because it cannot
widen what the pool made reachable.

```yaml
apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: policy-4, namespace: default}
spec:
  rules:
  - name: local-first-with-provider-no-pii
    route:
      exclude:
      - name: drop-backed-up-endpoints
        drop:
          backend:       {type: Local}
          queuedPrompts: {greaterThan: 4}
      - name: drop-endpoints-with-no-kv-headroom
        drop:
          backend:       {type: Local}
          kvCacheUsage:  {greaterThan: 0.9}
      - name: pii-never-leaves-cluster
        if:   {tag: [pii-confirmed, pii-unknown]}
        drop: {backend: {type: Provider}}
      select:
      - {type: Local,    minimize: PredictedTTFT}
      - {type: Provider, order: [databricks]}
```

## How a request is routed

Rules are **first-match-wins by list order**. Exactly one rule handles a
request; there is no merging and no fall-through between rules unless
`onEmpty: FallThrough` says so.

1. **Match.** The first rule whose `matches` accepts the request wins. No rule
   matching means `spec.unmatched`.
2. **Consider.** Generate candidates — one per (model, destination) pair. This
   is where models the request did not ask for can enter.
3. **Exclude.** Remove candidates. Order-independent; cannot fail open.
4. **Select.** Walk the tiers in order. The first tier with a surviving
   candidate wins outright, and its ranking decides.
5. **Tie-break.** Resolve candidates that ranked equal.
6. **Empty.** No survivor anywhere means `onEmpty`.

Because rules do not compose, two independent constraints written as two rules
means a request matching both silently loses one. That is what `exclude[].if`
exists for — see [exclude](#exclude).

## spec

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `unmatched` | `ServeLocal` \| `Reject` | no | `ServeLocal` |
| `rules` | list (≤32) | no | — |

### unmatched

What happens to a request no rule matched. `ServeLocal` serves it as asked on
the pool's Local destinations, ranking by predicted TTFT — Local-scoped at
candidate generation, so a provider candidate is never built for it.

`ServeLocal` is fail-open and right for the common case, but a rule set meant to
be exhaustive must say `Reject` — otherwise an unlisted tenant is served rather
than refused.

### rules

| Field | Type | Required |
| --- | --- | --- |
| `name` | string (≤253) | yes |
| `matches` | list (≤16) | no |
| `route` | object | yes |

A rule with no `matches` matches everything and must be last. If later rules
follow it they can never be reached, and compilation warns.

## Predicates

Every predicate value takes the same shorthand, in `matches`, in
`exclude[].if`, and in `exclude[].drop`.

| Form | Means | Example |
| --- | --- | --- |
| scalar | Equality; **contains**, for a set-valued signal | `model: qwen-2.5` |
| array | Membership; **intersects**, for a set-valued signal | `tag: [ssn, credit-card]` |
| hash | An explicit operator | `inputTokens: {greaterThan: 8000}` |

An operator hash takes **exactly one key**. For AND, use separate signals; for
OR, use two entries.

| Operator | Applies to | Notes |
| --- | --- | --- |
| `not` | any | Negates. Absent still does not match |
| `exists` | any | Takes a boolean. The only form that matches on absence |
| `containsAll` | set-valued only | All of them |
| `greaterThan` | numeric, duration | |
| `lessThan` | numeric, duration | |
| `greaterThanOrEqual` | numeric, duration | |
| `lessThanOrEqual` | numeric, duration | |

Durations are strings with units — `5s`, `200ms`, `1m30s`. A bare number is
ambiguous about units, so it is an `InvalidTerm` warning and the term fails
closed (see [Warnings](#warnings)). Numeric thresholds are the reverse: real
numbers, not quoted strings. The API server cannot catch either mistake —
predicate values are free-form in the schema — which is what
[`kinfctl check`]({{< relref "kinfctl.md" >}}) is for.

**Absence never matches.** A predicate over a signal the subject does not carry
is false, negated or not: a candidate carrying no `backend.name` is not "a
candidate whose `backend.name` is not X". Only `exists: false` matches absence.
This is what makes a Local-only signal harmless in a rule that also sees
providers — and also what makes it silent, which is why compilation warns about
it.

### Request-scope signals

Valid in `matches` and in `exclude[].if`. Fields within one entry are ANDed;
entries in `matches` are ORed.

| Signal | Type | Notes |
| --- | --- | --- |
| `model` | set of strings | The upstream the request resolved to **and** every alias for it, so any name the request legitimately answers to matches |
| `inputTokens` | integer | |
| `maxTokens` | integer | |
| `tenant` | string | `""` is a real value meaning "unknown". It matches a rule naming the empty string, never a rule written for a named tenant, and never every rule. Note METRICS treat it differently — see below |
| `qos` | array of `interactive` \| `batch` \| `deferrable` | Always an array — a three-value domain has no use for the other forms, and the enum means the API server rejects a typo. Traffic with no recognised QoS header resolves to `interactive` |
| `tag` | set of strings | Classifier output. See below |

### tenant in policy vs tenant in metrics

The two are deliberately not the same. Here, `""` is a distinct matchable value
naming traffic the caller could not attribute, and a rule can name it. On
`/metrics` it collapses into the shared `tenant="default"` bucket, along with
any tenant not on the metrics allowlist — so a request that matched a
`tenant: ""` rule is not separable in the metrics unless you look at the policy
counters instead. See [Metrics]({{< relref "metrics.md" >}}).

### tag

Classifier output as a set of opaque string tags — `pii-confirmed`, `ssn`,
`complexity-low`, `code`. Deliberately untyped while this part of the product
evolves: no enum, so no value validation, and **a typo matches nothing
silently**.

The tag **prefix** selects which classifier must run, so prefixes are
load-bearing rather than cosmetic: a rule referencing `pii-*` puts the PII
classifier in the request's latency path. Which classifiers exist, what each can
emit and how each fails is fixed for a given router build — see
[Classifiers](#classifiers) below. A tag no classifier can emit is reported in
`status.warnings`, which is where a typo surfaces.

Absence is not proof of a negative. A classifier that timed out or was skipped
emits no tag, which reads identically to "ran and found nothing". A fail-closed
detector must emit an explicit unknown tag, and a restrictive policy must match
on it:

```yaml
if: {tag: [pii-confirmed, pii-unknown]}    # not just pii-confirmed
```

Matching only `pii-confirmed` means a classifier failure silently takes the
permissive branch.

### Classifiers

Fixed for a given router build — not configurable, and not reported per-policy.
This table is the reference; checking a written tag against it is the review step.

| Classifier | Prefix | Can emit | On failure |
| --- | --- | --- | --- |
| `pii` | `pii-` | `pii-confirmed`, `pii-none`, `pii-unknown` | Fail-closed: emits `pii-unknown` when no user turn can be extracted from the request |
| `complexity` | `complexity-` | `complexity-high`, `complexity-low`, `complexity-normal`, `complexity-unknown` | Fail-closed: emits `complexity-unknown` when no user turn can be extracted; otherwise always one of low/normal/high |

**These are placeholders.** Both are keyword stubs that do no real work, so the
latency either adds to the request path is currently 0ms. Treat the *shape* as
stable — prefix selects the classifier, `-unknown` is always emittable — and the
verdicts as not yet meaningful.

The `-unknown` tag is what makes fail-closed expressible: every classifier can
emit it, so a restrictive rule always has something to match when the classifier
could not answer.

A tag outside these sets matches nothing, silently. The compiler catches it and
says so in `status.warnings`:

```
rule `x` exclusion `y`: `if` matches on tag(s) pii-detected that no classifier
can emit yet, so the exclusion never fires and the candidates it would drop are
still selectable
```

That direction matters: an exclusion that never fires leaves the candidates it
was written to remove in the set, which is the dangerous way for a typo to fail.

## route

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `consider` | list (1–16) | no | the requested model on every destination |
| `exclude` | list (≤16) | no | — |
| `select` | list (1–8) | yes | — |
| `tieBreak` | list (≤4) | no | first in rank order wins |
| `onEmpty` | `Reject` \| `FallThrough` | no | `Reject` |

### consider

Candidate generation. Defaults to the requested model on every destination the
pool allows, so most rules omit it. Declare it only to **add** a model the
request did not ask for, or to **partition with weights** — neither is
expressible as an exclusion.

An *empty* list is rejected rather than treated as the default.

| Field | Type | Notes |
| --- | --- | --- |
| `name` | string (≤253) | Label, for traces |
| `type` | `Local` \| `Provider` | Shorthand for `backends.type`. Do not set both |
| `weight` | integer 1–1000000 | Stochastic sampling before anything else, not a ranking input. Relative, not percentages |
| `models.fromRequest` | boolean | Exactly one of `fromRequest` or `names` |
| `models.names` | list (≤32, each ≤512) | Substitute these models |
| `backends.type` | `Local` \| `Provider` | |
| `backends.names` | list (≤32, each ≤253) | Destination names from the pool |
| `backends.selector.matchLabels` | map (≤64) | |

A `backends` block with no fields means every destination.

`type` is the same shorthand a [select](#select) tier takes. Constraining a group
to one backend class is the commonest thing written in either list, so the two
spell it the same way:

```yaml
consider: [{type: Provider}]              # shorthand
consider: [{backends: {type: Provider}}]  # longhand — identical
```

Reach for the longhand when you need another `backends` field alongside it;
setting both `type` and `backends` on one group is rejected.

```yaml
# Route a low-complexity request to a smaller model than it asked for.
consider:
- models: {names: [gemma-4]}
```

```yaml
# Let both compete, and let the ranking decide.
consider:
- models: {names: [gemma-4, qwen-2.5]}
```

### exclude

Order-independent set intersection. It cannot fail open: an exclusion only ever
removes candidates.

Two exclusions express OR. One exclusion with two `drop` fields expresses AND.
There is no `keep:` — an inverse form would eliminate candidates the author never
mentioned.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | string (≤253) | yes | Appears in traces and warnings |
| `if` | object | no | Request-scope guard, a single clause, ANDed |
| `drop` | object | yes | Candidate-scope predicate |

`if` exists because rules are first-match-wins and so do **not** compose
exclusions. Two independent constraints written as two rules means a request
matching both silently loses one; written as two exclusions on one rule, both
apply. For OR, use two entries with the same `drop`.

#### Candidate-scope signals

Valid in `drop`.

| Signal | Type | Carried by |
| --- | --- | --- |
| `contextWindow` | integer | both |
| `model.name` | string | both |
| `backend.name` | string | both |
| `backend.type` | `Local` \| `Provider` | both |
| `predictedTTFT` | duration | Local only |
| `prefixCacheHitRatio` | float | Local only |
| `queuedPrompts` | float | Local only. **Mean over the last 30s**, so fractional. Absent until the worker's first scrape |
| `kvCacheUsage` | float, 0.0–1.0 | Local only. Mean over the same 30s window. Absent until the worker's first scrape |
| `backend.readyEndpoints` | integer | Local only |
| `backend.observedTTFTP95` | duration | Provider only. Not published in the provider's status; read the figure from `kinference_provider_ttft_ms` to calibrate a threshold |
| `backend.budgetExhausted` | boolean | Provider only — **no source exists yet** |

`backend.type` is the one signal every candidate carries, which is what lets a
policy scope the rest. A `drop` using a class-scoped signal without constraining
`backend.type` silently never drops a candidate of the other class, and
compilation warns.

`queuedPrompts` is the endpoint's `num_requests_waiting`, **averaged over a
30-second window** — not the instantaneous depth. It is averaged because the
raw gauge is bursty: it spikes for a few seconds under continuous batching and
clears, so a threshold on the instantaneous value drops a worker over a
transient that has already passed, and under `onEmpty: Reject` can refuse a
request because of one. The mean makes a threshold mean *sustained* pressure.

Being a mean, it is fractional and the threshold needs choosing accordingly. A
worker that queued 5 prompts for two seconds out of thirty reads about `0.3`;
one genuinely backed up across the window reads near `5`. So `greaterThan: 0`
is much stricter than it looks — it fires on any queueing at all in the window
— and a fractional bound like `greaterThan: 0.5` is usually what an interactive
rule wants.

`kvCacheUsage` is the endpoint's KV-cache utilization over the same window, as a
0.0–1.0 ratio. It is the **headroom** signal, and it is not interchangeable with
the queue: the queue is not monotonic in pressure. Measured on a real fleet, a
moment at 93% KV showed one queued prompt while a moment at 31% KV showed five —
so a queue threshold ranks those two backwards. Threshold the queue for
admission backlog and `kvCacheUsage` for "this GPU has no room left"; the two
catch different failures.

Both are published per worker as `kinference_worker_queued_prompts_mean30s{worker,
model}` and `kinference_worker_kv_cache_usage_mean30s{worker,model}` — the exact
values these predicates read, so a threshold can be calibrated against what the
fleet actually does rather than guessed.

`backend.budgetExhausted` has no source until usage budgets exist, so an
exclusion using it never fires. It is accepted and warned about rather than
rejected, because dropping the field would silently compile an exclusion the
author believes is in force.

### select

Ordered tiers. The first tier with a surviving candidate **wins outright** — the
later tiers are never consulted for that request.

| Field | Type | Notes |
| --- | --- | --- |
| `type` | `Local` \| `Provider` | Shorthand for `backends.type`. Do not set both |
| `backends` | object | Same shape as in `consider` |
| `minimize` | `PredictedTTFT` \| `EstimatedTTFT` | Exactly one of `minimize` or `order` |
| `order` | list (≤16) | Destination names, in preference order |

| Rank | Reads |
| --- | --- |
| `PredictedTTFT` | A calibrated per-request prediction. **Local only** |
| `EstimatedTTFT` | `PredictedTTFT` for Local, `observedTTFTP95` for Provider. Ranks across classes, at the cost of comparing a prediction against a trailing aggregate |
| `order` | Destinations in the order listed. Names not listed sort last |

An objective must be defined for every candidate in its tier. `PredictedTTFT` is
Local-only, so a tier spanning both classes cannot use it — CEL rejects that at
admission, and `type: Local` on the tier is the ergonomic fix.

Under `minimize`, a candidate with no value for the tier's objective is
**dropped from that tier** with a trace entry, never defaulted. (An `order` tier
drops nothing — every candidate has a position.) A defaulted zero would be the best possible
score under `minimize`, so a provider carrying no prediction would win every
comparison outright. The one exception: under `EstimatedTTFT` a provider that has
never been observed sorts last but stays eligible, because dropping it would
deadlock — it needs traffic to earn an observation.

The canonical local-first shape:

```yaml
select:
- {type: Local,    minimize: PredictedTTFT}
- {type: Provider, order: [databricks]}
```

Strict tiering: providers are reached only once every local candidate has been
excluded. Pair it with an exclusion that empties the local tier under
load — otherwise the second tier is unreachable, and compilation warns.

A tier with no `backends` subset always keeps whatever survived, so every tier
after it is unreachable. This is the most common way a `select` list means less
than it appears to.

### tieBreak

`PreferLocal` and `Random`, in any order. Both are consulted in a fixed
order regardless of how they are written: `PreferLocal` narrows the tie
deterministically, then `Random` breaks whatever is left. The reverse would make
`PreferLocal` dead whenever both were set.

Omitted on an explicit rule means the first candidate in rank order wins
deterministically. The implicit default route (no `policyRef`, or `unmatched:
ServeLocal`) uses `Random`.

### onEmpty

What happens when no tier had a survivor. `Reject` refuses the request;
`FallThrough` continues to the next matching rule. After the last one,
`spec.unmatched` applies exactly as if no rule had matched: `ServeLocal` serves
the requested model on local destinations, and `Reject` refuses.

## status

| Field | Meaning |
| --- | --- |
| `state` | At-a-glance state, comma-separated. One of `InForce` / `NotReferenced`, then `HasWarnings` |
| `referencedBy` | Pools whose `policyRef` names this policy. Empty means nothing routes through it |
| `warnings` | Findings, each `Code: path: message`. Never fatal. At most 32 — see [Warnings](#warnings) |
| `conditions` | `Accepted` |

### state and the STATE column

```
NAME         STATE                       AGE
policy-1     InForce                     7d
policy-2     NotReferenced               7d
demo-three   NotReferenced,HasWarnings   1m
```

`STATE` and `AGE` are the only columns. `spec.unmatched` had one and does not need
it: the schema default is materialized into the stored object, so `get -o yaml`
spells it out whether or not the author wrote it, and the column was identical on
every policy.

Two orthogonal facts in one column. `InForce` / `NotReferenced` is whether any
pool's `policyRef` names this policy; `HasWarnings` is whether compilation had
anything to say. A policy can be `InForce,HasWarnings` — warnings never make a
policy unusable.

It is a **joined string**, not a list, for a mechanical reason: a printer column
with a `[*]` JSONPath renders only the *first* array element, so a list would
print `NotReferenced` and drop `HasWarnings` without a trace. It is not in the
`Accepted` condition's `reason` either — commas are legal there, but warnings do
not affect acceptance, so `HasWarnings` is not a reason for anything and putting
it there would make the condition lie.

There is always at least one token, which is what keeps a **blank** `STATE`
meaning "no status was written at all" — most likely the policy is in a namespace
the router does not watch.

There is no `ruleCount` and no `rules[]`. Both were restatements of the manifest:
compilation is a straight map over `spec.rules` with no filter and no reorder, so
`ruleCount` was always `len(spec.rules)` and `rules[].name` always echoed
`spec.rules[].name` in order. The count also read like a health signal while
saying only "this parsed", which every policy in the namespace does — a detached
policy showed the same number as a live one.

Status is written for **every** policy in scope, not only the ones a pool
references. An unreferenced policy still compiles and still reports its
warnings — that is the state to inspect before attaching it — and
`Accepted` is what says whether anything routes through it.

| `Accepted` | Reason | Means |
| --- | --- | --- |
| `True` | `Accepted` | In force on at least one pool |
| `False` | `NotReferenced` | Compiled, but no `InferencePool` names it in `spec.policyRef` |

The condition carries the same referenced/not answer as `state`'s first token,
with a message naming the pools (or the patch command). `state` is the column;
the condition is the detail.

### Warnings

Warnings never make a policy unusable. Each describes a part of the policy that
does less than it says — a term that can never match, a rule nothing reaches, a
name that refers to nothing — while the rest still applies. Read them.

Every warning has a stable **code**, which is what to search for here, and a
**path** into the manifest as written. Status renders each one as
`Code: path: message`:

```bash
kubectl -n default get infpol policy-4 -o jsonpath='{.status.warnings}' | jq
```

```
UnknownBackend: spec.rules[0].route.select[1].order[0]: rule `burst-to-cloud`: `databriks` is not a destination of pool default/inference-all, so nothing matches it. Did you mean `databricks`? ...
```

Better still, see them before applying. [`kinfctl check`]({{< relref "kinfctl.md" >}})
reports every warning below, plus fields the API server would silently drop. It
reports those from the spec alone as errors (`E`), since they are wrong whatever
pool serves the policy, and those against the attached pools as warnings (`W`),
since a name one pool lacks may be right for another. Either is something to fix.
In status, all of them are warnings.

**An invalid term fails closed.** A predicate term that cannot be compiled —
`queuedPrompts: {greaterThan: "4"}`, a quoted number — makes the entry it belongs
to unsatisfiable: a `matches` entry never matches, an `if` never applies, a
`drop` removes nothing. It is never left out. Leaving it out would *widen* the
predicate, since terms AND: that `drop`, minus its bad term, is
`{backend: {type: Local}}`, which removes every local candidate.

**Status holds at most 32.** The CRD caps `status.warnings` at 32 entries of
2048 characters, and the API server rejects a status that exceeds either — all
of it, `state` and conditions included. So a longer list keeps the first 31 and
ends with `…and N more warning(s)`. The full list is in the
[policy API]({{< relref "policy-api.md" >}})'s `GET /api/policy/context`, and
from `kinfctl check`. Warning counts by code are exported as
`kinference_policy_warnings{policy,code}`, and each new warning is logged once
at WARN.

#### From the spec alone

These need nothing but the policy, so `kinfctl check policy.yaml` finds them all,
and reports them as errors (`E`).

| Code | Fires when | Fix |
| --- | --- | --- |
| `InvalidTerm` | A predicate value does not compile: wrong type, unknown operator, a bare number for a duration, a quoted threshold. The entry fails closed | Fix the value; the message says what was expected |
| `EmptyPredicate` | A `matches` entry or an `if` has no conditions, so it matches every request. Usually a misspelled field the API server dropped | Correct the field name, or remove the entry |
| `ShadowedRule` | A catch-all rule that does not fall through is not last, or a rule repeats an earlier rule's `matches` verbatim. Either way, later rules are unreachable | Move the catch-all last, give the earlier rule `onEmpty: FallThrough`, or change the `matches` |
| `UnemittableTag` | A `tag` value no classifier emits. `tag: X` never matches; `{not: X}` always does; in a list, only the dead values are inert. See [Classifiers](#classifiers) | Use an emittable tag; the message lists them |
| `ConsiderModelChoice` | A `consider` group's `models` sets both or neither of `fromRequest` and `names`, or `fromRequest: false` with no `names` | Set exactly one |
| `PartialWeights` | Some `consider` groups set `weight` and others do not. Any weight makes the groups a weighted split, with unset weights counting as 1 — so `[{weight: 70}, {}]` is 70:1 | Weight every group, or none to union them |
| `DuplicateName` | Two rules share a name, or two exclusions or consider groups in one rule do. Traces and `kinference_policy_rule_matches_total{rule}` cannot tell them apart | Rename one |
| `EmptyDrop` | A `drop` names no condition, so the exclusion removes nothing. Usually a misspelled field the API server dropped | Correct the field name |
| `DropTermWrongClass` | A `drop` uses a signal only one class carries (`queuedPrompts` is Local-only) without pinning `backend.type` to that class — or while pinning it to the other one | Add `backend: {type: Local}` (or `Provider`) |
| `NoSignalSource` | A `drop` uses `backend.budgetExhausted`, which nothing sets yet | Remove it |
| `ThresholdOutOfRange` | A comparison is constant over the attribute's range — `kvCacheUsage: {greaterThan: 80}` is never true, because `kvCacheUsage` is a 0–1 fraction | Use the attribute's units: `0.8`, not `80` |
| `InvalidEnumValue` | A `backend.type` value other than `Local` or `Provider`. The comparison is case-sensitive, so `local` never matches | Spell it `Local` or `Provider` |
| `CoercedValue` | A value of the wrong type was converted rather than rejected: a number for a string attribute, a fraction for an integer one | Quote the string, or use a whole number |
| `SelectRankAmbiguous` | A `select` tier sets both or neither of `minimize` and `order` | Set exactly one |
| `UnreachableTier` | A `select` tier with no `backends` subset is not last, so it always keeps a candidate and the tiers after it never run | Give it a subset, or move it last |
| `EmptySelect` | A rule has no `select` tiers; it ranks by `minimize: PredictedTTFT` | Add a tier |
| `OrderWithoutRandom` | A tier that can hold local candidates ranks by `order` with no `tieBreak: [Random]`. Every endpoint of the first backend ties, and the first always wins — one pod takes the tier's traffic | Add `tieBreak: [Random]`, or `type: Provider` if the tier only holds providers |

#### Against the attached pools

These check the names a policy uses against the destinations of the pools that
reference it. The router runs them for every attached policy; `kinfctl` runs them
when the pool is among its inputs, and reports them as warnings (`W`). For
`kinfctl`, the pool alone covers backend names and classes; models and labels also
need the worker sets' and providers' manifests. An unattached policy has nothing
to check against, and gets none.

| Code | Fires when | Fix |
| --- | --- | --- |
| `UnknownBackend` | A name in `consider.backends.names`, `select.backends.names`, `select.order` or `drop.backend.name` is not a destination of the pools. The message suggests the closest real name | Use a destination's name |
| `SelectorMatchesNothing` | A `consider` group's or `select` tier's `matchLabels` matches no destination | Fix the labels, or the destinations' |
| `NoDestinationsOfType` | A `consider` group or `select` tier takes only `Provider` (or `Local`) destinations, and the pools have none | Add the destination to the pool, or drop the group or tier |
| `UnknownModel` | A model name in `matches.model`, `consider.models.names` or `drop.model.name` is declared by no destination, by name or alias. Checked only when every local set declares its models in `spec.models`, since a set without rows can serve any name. Naming `auto` always warns: it is resolved before rules run | Use a declared name or alias, or declare the model on its worker set |

## Diagnosing

| Symptom | Cause |
| --- | --- |
| `STATE` blank | No status written at all — the policy is in no namespace the router watches |
| `STATE: NotReferenced` | No pool names it. Patch the pool's `policyRef` |
| A rule never fires | It follows a catch-all, or matches a tag no classifier emits. Check `warnings` |
| Providers never picked | A `PredictedTTFT` tier with no `backends` subset keeps every local candidate, so the provider tier is unreachable |
| A provider is picked on its first request | Under `EstimatedTTFT` an unobserved provider stays eligible and sorts last — with no local candidate surviving, last is first |
| PII reached a provider | The `if` clause matched only `pii-confirmed`, and the classifier emitted `pii-unknown` |
| An exclusion did nothing | It used a class-scoped signal without constraining `backend.type`, or a threshold outside the signal's range. Check `warnings` |
| A rule matches more than it should | A misspelled field was dropped by the API server on apply, leaving the entry with fewer conditions. `EmptyPredicate` catches the case where nothing is left; `kinfctl check` catches every one |
| `status.warnings` ends with `…and N more` | More than 32 findings. Read the full list from `GET /api/policy/context`, or `kinfctl check` |
| Requests are refused with no obvious cause | `unmatched: Reject`, or `onEmpty: Reject` after every candidate was excluded |
