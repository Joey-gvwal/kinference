---
title: "Policy API"
linkTitle: "Policy API"
weight: 65
description: >
  The read-only JSON endpoints behind the dashboard's policy explorer: what an
  applied policy would do with a given request, for programs and assistants as
  well as people.
---

Two endpoints on the router's admin listener — `admin_listen` in
[Configuration]({{< relref "configuration.md" >}}), port 9001 in the shipped
configuration. The dashboard's policy explorer (`?tab=policy`) is built on them.

```bash
kubectl -n default port-forward svc/kinference 9001:9001
curl -s localhost:9001/api/policy/context | jq '.policies[].key'
```

| Endpoint | Answers |
| --- | --- |
| [`GET /api/policy/context`](#get-apipolicycontext) | Every policy with its findings, every pool with its destinations and live candidates |
| [`POST /api/policy/simulate`](#post-apipolicysimulate) | What one policy would decide for one hypothetical request, step by step |

They evaluate **applied** policies. To check a draft before applying it, use
[`kinfctl check`]({{< relref "kinfctl.md" >}}).

## Guarantees

**Read-only.** Both endpoints evaluate an immutable snapshot and change nothing:
no metric, no counter, no cache. That is structural — the handlers hold no handle
to anything they could change — and a test renders `/metrics` before and after a
burst of calls and requires the two to match. Calling them against a production
router is safe.

**Bounded.** Simulation runs off the runtime that serves `Pick`, at most four at
a time; a fifth gets `429 Busy` with `Retry-After`, rather than queueing.

| Limit | Value |
| --- | --- |
| Request body | 256 KiB |
| `candidates` | 256 |
| `model` | 32 names |
| `tags` | 64 |
| `runs` | 1–500 |
| `inputTokens`, `maxTokens` | 0–10,000,000 |
| Concurrent simulations | 4 |

**JSON, always.** Every response under `/api` is JSON, errors included. An
unknown path is a JSON 404 that lists the real endpoints; a wrong method is a JSON
405 with an `Allow` header. Request fields this API does not have are rejected by
name rather than ignored, so a typo cannot silently simulate the default.

**Per replica.** Each router replica answers from its own state. Policies and
pools converge within a watch event; live queue depths and endpoints are that
replica's view.

**Unauthenticated.** The listener is plaintext with no authentication. Everything
here is read-only, but it does describe your routing and your fleet: keep it inside
the cluster and reach it with `kubectl port-forward`.

## Errors

```json
{"error": {"code": "InvalidRequest", "message": "must be 1..=500", "field": "runs"}}
```

`code` is stable and meant to be matched on; `message` is for a person; `field`,
when present, names the part of the request at fault, in the request's own
spelling (`candidates[2].backendType`).

| Code | Status | Means |
| --- | --- | --- |
| `InvalidRequest` | 400 | The body is not JSON, has the wrong shape, carries an unknown field, or a value out of range |
| `NotFound` | 404 | No such endpoint, or no such `policy` or `pool` (see `field`) |
| `MethodNotAllowed` | 405 | The endpoint exists; `Allow` lists the methods it takes |
| `PayloadTooLarge` | 413 | The body exceeds 256 KiB |
| `UnsupportedMediaType` | 415 | The body was not sent as `Content-Type: application/json` |
| `Busy` | 429 | Four simulations are already running. Retry after `Retry-After` seconds |
| `PolicyDisabled` | 501 | `[policy] enabled = false`: nothing is watched, so there is nothing to evaluate |
| `NotListed` | 503 | The CRD watch has not finished its first list, so an empty answer would mean "unknown", not "none". Retry after `Retry-After` seconds |

## GET /api/policy/context

Everything needed to write a simulation, in one call.

| Field | Type | Meaning |
| --- | --- | --- |
| `policies[]` | object[] | Every `InferencePolicy` in scope, attached or not, sorted |
| `pools[]` | object[] | Every `InferencePool`, sorted |
| `qos` | string[] | The values `qos` accepts |
| `tags` | string[] | Every tag a classifier can emit |

### policies[]

| Field | Type | Meaning |
| --- | --- | --- |
| `key` | string | `namespace/name` — what `simulate` takes as `policy` |
| `referencedBy` | string[] | Pools whose `policyRef` names it, sorted |
| `inForce` | boolean | Some pool references it |
| `hasWarnings` | boolean | `warnings` is non-empty |
| `state` | string | The `STATE` column, e.g. `InForce,HasWarnings` |
| `warnings[]` | `{code, path, message}` | Every finding, compile and name checks — never truncated, unlike `status.warnings`. Codes are listed under [InferencePolicy › Warnings]({{< relref "inferencepolicy.md#warnings" >}}) |
| `unmatched` | string | `ServeLocal` or `Reject` |
| `rules[]` | `{index, name, matchEntries, requiresClassifiers, onEmpty}` | `matchEntries` counts the rule's `matches` entries; `0` is a catch-all |
| `nondeterminism[]` | `{code, rule, message}` | Why the policy may answer differently between runs: `RandomTieBreak`, `WeightedConsider`, or `UnmatchedRandomTieBreak` (the built-in route, `rule: null`) |
| `spec` | object | The spec as applied |

### pools[]

| Field | Type | Meaning |
| --- | --- | --- |
| `key` | string | `namespace/name` — what `simulate` takes as `pool` |
| `policyRef` | object or null | `{name, resolved, problem?}`; `null` when the pool names no policy |
| `destinations[]` | object[] | `{name, type, labels, blockSize, declaredModels}`. `blockSize` is the local set's KV block size in tokens, the unit of `matchedBlocks`; `null` for a provider |
| `candidates[]` | object[] | Live candidate seeds, sorted — one per endpoint and served model name. `queuedPrompts` and `kvCacheUsage` are null for a worker not yet scraped. Each can be posted back to `simulate` as one of its `candidates`, unchanged or edited |

## POST /api/policy/simulate

Runs one applied policy against one hypothetical request and a candidate set,
and returns every step of the decision.

### Request

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `policy` | string | yes | `namespace/name` |
| `pool` | string | no | `namespace/name`. Supplies the predictor and the alias tables. Without it, local candidates have no prediction |
| `requestedModel` | string | no | The model name a client would send. Expanded through the pool's alias tables, as a real request is |
| `model` | string[] | no | The exact `model` attribute set, for asserting on one. Set this or `requestedModel`, not both |
| `qos` | string | no | One of `qos` from `context`. Absent is not the same as any value: absence never matches |
| `tags` | string[] | no | Classifier tags the request carries |
| `tenant` | string | no | `""` is a value — what a request with no tenant header carries |
| `inputTokens` | integer | no | Prompt length in tokens. At pick time this is rounded up to whole KV blocks |
| `maxTokens` | integer | no | |
| `candidates` | object[] | no | See below |
| `runs` | integer | no | 1–500. Above 1, also returns the distribution of outcomes — for a policy `nondeterminism` flags |

### candidates[]

| Field | Type | Units | Meaning |
| --- | --- | --- | --- |
| `backendName` | string | | The destination's name, as a policy writes it |
| `backendType` | string | | `Local` or `Provider`, exactly |
| `canonicalModel` | string | | The client-facing name this candidate answers to |
| `upstreamModel` | string | | The wire name; defaults to `canonicalModel` |
| `endpoint` | string | | Local only |
| `pool` | string | | Echo of a seed's pool; must match the request's `pool` |
| `queuedPrompts` | number | prompts, 30s mean | Local only |
| `kvCacheUsage` | number | 0–1 | Local only |
| `contextWindow` | integer | tokens | |
| `predictedTtftMs` | number | ms | Local only. Replaces the predictor's answer |
| `observedTtftP95Ms` | number | ms | Provider only |
| `matchedBlocks` | integer | KV blocks of the destination's `blockSize` | Cached prefix; absent is a cold prompt |
| `prefixCacheHitRatio` | number | 0–1 | Local only. Derived from `matchedBlocks` and `inputTokens` when absent, as at pick time |
| `readyEndpoints` | integer | endpoints | Local only. Derived from the number of local endpoints given for the backend when absent |

### Response

| Field | Type | Meaning |
| --- | --- | --- |
| `verdict` | string | `chosen`, `unmatched_served`, `unmatched_empty`, `rejected_on_empty` or `rejected_unmatched` |
| `winner` | string or null | The chosen candidate's label, `backend/upstream@endpoint` |
| `steps[]` | object[] | One per rule in order, matched or not, then the built-in route if reached: which predicates matched, what `consider` admitted, what was excluded and why, the tier that decided and how many tied |
| `effectiveCandidates[]` | object[] | Each candidate as evaluated, with every signal a predicate can read and `predictionSource` |
| `resolvedModels` | string[] | What `requestedModel` expanded to |
| `warnings[]` | `{code, path, message}` | The policy's findings |
| `nondeterminism[]` | `{code, rule, message}` | As in `context` |
| `distribution` | object or null | `{runs, outcomes: [{outcome, count}]}` when `runs` > 1 |

`predictionSource` says where a local candidate's `predictedTtftMs` came from, and
`predictionNote` says more:

| Value | Means |
| --- | --- |
| `predictor` | The real predictor, with the candidate's own worker set's model |
| `borrowed` | The real predictor, with another set's model — the candidate names no set in the pool |
| `override` | The request supplied `predictedTtftMs` |
| `unavailable` | No prediction could be made, and the candidate was **ranked as 0.0 ms** — the fastest possible. Supply `pool`, or `predictedTtftMs` |
| `notApplicable` | A provider; providers have no per-request prediction |

Predictions are optimistic relative to a real pick under load: a hypothetical
request has no running batch, decode contention or KV pressure to account for.

### Example

```bash
curl -s localhost:9001/api/policy/simulate -H 'Content-Type: application/json' -d @- <<'EOF' | jq '{verdict, winner}'
{
  "policy": "default/policy-3",
  "pool": "default/inference-all",
  "requestedModel": "qwen-2.5",
  "inputTokens": 2048,
  "candidates": [
    {"backendName": "qwen-2-5-32b", "backendType": "Local",
     "canonicalModel": "qwen-2.5", "upstreamModel": "Qwen/Qwen2.5-32B-Instruct-AWQ",
     "endpoint": "10.0.1.7:8000", "queuedPrompts": 9},
    {"backendName": "databricks", "backendType": "Provider",
     "canonicalModel": "qwen-2.5", "upstreamModel": "Qwen/Qwen2.5-32B-Instruct-Turbo"}
  ]
}
EOF
```

To start from the live fleet instead, take `pools[].candidates` from `context`,
edit what the scenario needs, and post them as `candidates`.

## Diagnosing

| Symptom | Cause |
| --- | --- |
| `NotFound` on a path that looks right | A trailing slash, or a typo. The message lists the real endpoints |
| `InvalidRequest` naming a field you sent | The field does not exist in this API. Check the spelling against the tables above |
| A local candidate wins on `0.0 ms` | `predictionSource: unavailable` — no `pool` was given, or the pool has no live local set. Set `pool`, or `predictedTtftMs` |
| A rule does not match a model the client would send | `model` was given as the raw set. Use `requestedModel`, which expands aliases the way a real request is resolved |
| The winner changes between calls | `nondeterminism` says why. Use `runs` to see the distribution |
| `Busy` | Four simulations were already running on this replica |
| `NotListed` right after a restart | The watch has not listed yet. Retry after `Retry-After` |
