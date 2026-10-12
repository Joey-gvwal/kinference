---
title: "Metrics"
linkTitle: "Metrics"
weight: 70
description: >
  Every Prometheus metric the router exposes, and what each one is for.
---

Served on the dashboard listener, alongside `/readyz`:

```bash
kubectl -n default port-forward svc/kinference 9001:9001
curl -s localhost:9001/metrics
```

The address is `admin_listen` in the router's TOML (`0.0.0.0:9001` by
default). Everything is prefixed `kinference_`.

## A labelled metric is absent until its first observation

The Prometheus client emits nothing for a `*Vec` family — not even `# HELP` —
until some label combination has been observed. On a router that has served no
traffic, `/metrics` shows only the unlabelled metrics below, and
**every per-model, per-outcome and per-reason family is missing entirely**.

That is normal and not a fault. It also means you cannot alert on the *absence*
of a series to detect a broken router; alert on `kinference_build_info`,
`kinference_leader` or `/readyz` instead.

`kinference_build_info` is the exception to the rule above: it is set at construction
rather than on first observation, so it is present on a router that has served
nothing. That makes it the one series whose absence genuinely means "this
process is not answering".

## Build identity

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_build_info` | gauge | `version` |

Always `1` — the value carries no meaning, the labels do. That is the
conventional shape, and it buys two things: `group_left` joins, so any other
series can be attributed to the build that produced it, and an alert on more
than one version being live at once, which is how you notice a rollout that
stalled half-done.

```promql
count(count by (version) (kinference_build_info)) > 1
```

`version` is `KINFERENCE_VERSION` when the build set it — `k8s/kinference/Makefile` sets it
from `TAG`, so a container advertises the same identity as its image tag — and
`CARGO_PKG_VERSION` otherwise. It is the same string `--version` prints and
`/api/snapshot` reports, so a scrape and a human cannot disagree about which
build is running.

## The `tenant` label is opt-in, and bucketed by an allowlist

The per-request families carry a `tenant` label, but **off by default**: unlike
`model` and `qos`, which are small server-controlled sets, `tenant` is whatever
the caller put in `PickRequest.tenant`. Left ungated it is unbounded, so it is
gated by `[metrics] tenant_labels`:

| `tenant_labels` | Effect |
| --- | --- |
| omitted, `"none"`, or `[]` | **Default.** Every request reports `tenant="default"`. |
| `"all"` | Every tenant id seen becomes its own label value. Unbounded — see below. |
| `["acme", "globex"]` | Those tenants keep their identity; every other request reports `tenant="default"`. |

A mistyped value is a startup error, not a silent fallback — the one failure
worth designing out is a typo that turns the feature off while you believe it is
on.

### `default` is a reserved value

Requests that carry no tenant land there, as do requests whose tenant is not
allowlisted. A caller whose tenant is *literally* `default` is therefore
indistinguishable from those — harmless under `"all"`, where there is no
fallback, but under an allowlist an allowlisted `default` shares one series with
everything unlisted.

Note this differs from the **policy** semantics on purpose. In an
`InferencePolicy` rule, `tenant: ""` is a distinct matchable value naming
traffic the caller could not attribute. In metrics that collapses into
`default`. The two are not the same string.

### What `"all"` costs

About **91 series per (model, qos, tenant)**: 1 each for the two counters; 19
each for the two length histograms and for `kinference_request_duration_ms` (16
buckets, plus the `+Inf` bucket, `_sum` and `_count`); 16 for `kinference_ttft_ms` on
the same basis; and 16 for the ITL pair (15 explicit `le` values, one of which
is already `+Inf`, plus its `_count`). At 5 models and 3 QoS classes that is
about 1,365 series per tenant. A caller that sends a unique id per request will
grow the registry without limit; kinference logs a `WARN` at startup when `"all"` is
set, saying so. Prefer an explicit allowlist wherever the tenant set is known.

Per the section above, a tenant's series simply does not exist until that tenant
sends something — so a fresh router under `"all"` looks identical to one with
the label off.

### `kinference_provider_ttft_ms` is not labelled by tenant

It is labelled `{provider, model}` and has no `qos` either, so **per-tenant TTFT
covers on-cluster traffic only**. A dashboard that breaks `kinference_ttft_ms` down
by tenant is silently excluding everything routed off-cluster to an
`InferenceProvider`.

`kinference_request_duration_ms` is the exception — it spans both paths, so use it
when you need a per-tenant latency view that does not have a hole in it.

## Router service health

The router's own SLO: whether it is answering `Pick()` successfully, and how
fast. Start here.

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_picks_total` | counter | `outcome` |
| `kinference_pick_duration_ms` | histogram | `outcome` |
| `kinference_workers` | gauge | `state` |
| `kinference_workers_by_model` | gauge | `model`, `state` |
| `kinference_worker_evictions_total` | counter | `reason` |
| `kinference_orphaned_reports_total` | counter | — |
| `kinference_dashboard_slots_used` | gauge | — |
| `kinference_dashboard_slots_capacity` | gauge | — |
| `kinference_dashboard_slots_denied_total` | counter | — |

`outcome` is the result handed back to the caller:

| `outcome` | Means |
| --- | --- |
| `ok` | A destination was chosen (including a `DEGRADED` pick, which still routes) |
| `bad_request` | The body did not parse, or carried nothing to route on |
| `no_workers` | Nothing serves the requested model |

**Not the same as `kinference_policy_decisions_total`.** That one is about resolving
CRD policy, and the two can disagree: a pick that resolved no policy at all
still returns `ok`. This is the top-line "is the router serving requests"
counter; that one explains *how* a destination was chosen.

`kinference_pick_duration_ms` is kinference's own decision latency — scoring plus any
tokenize round-trip, ending when the response is built. It is **not** the
model's latency; that is `kinference_ttft_ms` and `kinference_request_duration_ms`. It is
labelled by outcome because a rejected pick returns in microseconds and would
otherwise flatter the successful distribution.

Note a failure that never reaches a routing decision does not appear here at
all. A `Pick` naming an unknown or expired body fails with a gRPC `NOT_FOUND`
before any outcome exists — per the error model, transport failures are status
codes and routing outcomes are response fields.

### `kinference_workers_by_model{model,state}`

The same reachability, split by served model. A worker serving several models is
counted under **each**, which is why this is a separate family rather than a
label on `kinference_workers`: the per-model counts do not sum to the fleet total and
were never meant to. Derived from the registry's own `by_model` index, so it
agrees with what routing sees rather than re-deriving membership.

Use it when a model goes unroutable while the fleet total looks healthy — the
case `kinference_workers` structurally cannot show.

### `kinference_worker_evictions_total{reason}`

Workers dropped from the fleet, by reason.

`block_size_mismatch` is the one to know. It means the engine published KV
events at a page size other than the set's `kvBlockSize`, and it fails in two
stages: mismatched blocks are discarded before insertion, so events arrive while
the prefix index stays **empty** and picks silently degrade to load-only; then
after a threshold of consecutive mismatches the worker is evicted outright.

Any non-zero value wants investigating, and the fix is in the CR rather than the
worker: read the real page size off an event (the engine logs it — *"Block size
is: N"*) and set `kvBlockSize` to that. Restarting the worker reproduces it.

Note `--block-size` is **not** necessarily the value to use. Gemma 3/4 report
`block_size="64"` in `cache_config_info` while publishing 256-token blocks.

### `kinference_orphaned_reports_total`

Final-token reports that arrived with no matching in-flight request, usually
because `observation_ttl_secs` expired first. Their tokens are counted nowhere,
so a rising value means the token families **understate** real traffic — and a
long non-streaming request is the ordinary cause. Raise the TTL above the
worst-case full response time.

### `kinference_workers{state}`

`ok` or `unresponsive`, the latter meaning the worker's `/metrics` scrape has
been stale for over 5 seconds. Recomputed once per second.

An unresponsive worker is **still routable** — it keeps being scored on its last
known sample, because refusing to route to a worker whose telemetry is late is
usually worse than routing to it. So this gauge climbing does not show up as
errors anywhere else; it is the only signal that the fleet's view is going
stale. It also reads `unresponsive` for a worker configured without a
`metrics_url`, which never had a sample to begin with.

### Dashboard slots

`dashboard_max_workers` preallocates a pool of display slots for the live
dashboard. `used` against `capacity` is its headroom.

**These bound the dashboard, not the router, and not this metric set.**
`kinference_workers` is counted from the pool registries, so it covers every routable
worker however large the fleet. A worker beyond the dashboard's capacity routes
normally and is counted normally — it is simply not drawn.

`kinference_dashboard_slots_denied_total` is therefore **expected to be non-zero** on
any fleet larger than `dashboard_max_workers`, and is not an error. It answers
one question only: why the dashboard is showing a subset. Raise
`dashboard_max_workers` to show more, at roughly 480 KB per slot — the pool
holds an hour of per-worker history at 1 Hz.

## Tokenization

On the `Pick` critical path, and the subsystem everything prefix-related depends
on.

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_tokenize_total` | counter | `path` |
| `kinference_tokenize_duration_ms` | histogram | — |
| `kinference_tokenize_compare_total` | counter | `result` |

`path` is exhaustive — exactly one per attempt, so the three sum to the total:

| `path` | Means |
| --- | --- |
| `local` | The in-process tokenizer served it |
| `http_fallback` | Local failed; the engine's `/render` saved it |
| `unavailable` | Both failed. The pick is `DEGRADED` and routes on load alone |

`http_fallback` climbing means the local tokenizer is failing for a model —
often a missing or unreadable tokenizer under `[tokenize.local] mount_dir`.
It still serves correctly, just over the network and on the critical path, so
it shows up as latency rather than errors. `unavailable` climbing should
correlate with `kinference_score_degraded_total{reason="tokenize"}`.

### `result="divergence"` is the one to alert on

Compare mode runs the local tokenizer and the engine's `/render` side by side. A
divergence means they disagree about the token sequence — a chat-template,
special-token or vocab-version mismatch — and therefore that **every prefix
block hash kinference computes is wrong**. Cache-affinity routing silently stops
working: nothing indexed can match a real prompt, and the `kinference_prefix_*`
metrics keep reporting numbers that look entirely plausible.

The catch: this counter is only ever non-zero when `[profile.tokenize] mode` is
`compare`. In a `render`- or `local`-mode deployment it stays at zero whether or
not the tokenizers agree, so a persistent zero is not evidence of agreement.
Running one profile in compare mode is the cheap way to keep the check live.

## Request outcomes

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_prompt_tokens_total` | counter | `model`, `qos`, `tenant` |
| `kinference_completion_tokens_total` | counter | `model`, `qos`, `tenant` |
| `kinference_cached_prompt_tokens_total` | counter | `model`, `qos`, `tenant` |
| `kinference_cached_tokens_absent_total` | counter | — |
| `kinference_cached_tokens_reported_total` | counter | — |
| `kinference_prompt_length_tokens` | histogram | `model`, `qos`, `tenant` |
| `kinference_completion_length_tokens` | histogram | `model`, `qos`, `tenant` |

The counters are running totals; the histograms are the *shape* of the same
quantity, so `histogram_quantile` gives p50/p90/p99 per model. Both length
histograms share one bucket layout — exponential, factor 2, from 8 to ~262k
tokens — specifically so prompt and completion are directly comparable.

Token counts come from the caller's `ReportFinalToken`. A request that never
reports one is counted nowhere.

`model` is the model each request was **sent** with, on every family labelled by
it: the worker's model name for a local pick, after any alias resolves, and the
matching row's `name` for a provider pick. It is not the alias the client asked
for, and not the name in the report, which is the vendor's own spelling. So one
alias served both on the cluster and off it is two `model` values — accurately,
since two different models served it. `kinference_routed_requests_total` says
where each request went.

### Billable prompt tokens

`kinference_cached_prompt_tokens_total` is what the **engine** reports it served from
its own prefix cache (`usage.prompt_tokens_details.cached_tokens`), relayed on
`ReportFinalToken`. Measured, not predicted — distinct from
`kinference_prefix_matched_blocks_total`, which is what the router *expected* from its
index. Comparing the two is the only check on whether the index reflects reality;
billing should use this one.

The prompt tokens that actually cost prefill compute:

```promql
sum by (tenant) (rate(kinference_prompt_tokens_total[1h]))
  - sum by (tenant) (rate(kinference_cached_prompt_tokens_total[1h]))
```

**Check `kinference_cached_tokens_absent_total` before trusting that.** It counts
reports that carried no `cached_tokens` at all — an older engine, or a caller not
relaying the field. Those requests contribute nothing to the cached counter, so
the subtraction above **overcharges** by exactly their cache hits. A missing
value is deliberately not recorded as zero, precisely so this is visible rather
than silently in the operator's favour.

It is unlabelled on purpose: it answers whether the basis is complete, not whose.
If it is non-zero, per-tenant billing is not yet safe to derive from these
counters.

**Read it against `kinference_cached_tokens_reported_total`, never on its own.** The
two are deliberately a pair — one counts reports carrying `cached_tokens`, the
other reports omitting it — and only their ratio says *how* incomplete the basis
is. An unbounded count answers "is anything missing"; the share answers "how
much", which is the number you can act on:

```promql
rate(kinference_cached_tokens_absent_total[5m])
/
(
  rate(kinference_cached_tokens_absent_total[5m])
  + rate(kinference_cached_tokens_reported_total[5m])
)
```

The companion exists because there was no commensurable denominator. Dividing
the absent count by anything labelled — reports, requests, tenants — compares a
single unlabelled series against a sum over many, and produces a "share" above
100%. Both halves of this pair are unlabelled so the ratio is always a ratio.

## Latency

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_request_duration_ms` | histogram | `model`, `qos`, `tenant` |
| `kinference_ttft_ms` | histogram | `model`, `qos`, `tenant` |
| `kinference_provider_ttft_ms` | histogram | `provider`, `model` |
| `kinference_itl_ms_bucket` | counter | `model`, `qos`, `le`, `tenant` |
| `kinference_itl_ms_count` | counter | `model`, `qos`, `tenant` |

`kinference_request_duration_ms` is the **router's** view of end-to-end time: the
interval from the pick to the caller's `ReportFinalToken`. Read it as "how long
did this request occupy a worker", not as user-observed latency — it includes
the caller's own handling and that report's network hop, and it does not include
whatever the caller did before `Pick`.

Two things it does not cover, both of which bias it optimistic:

- **Requests that never report.** A caller that drops the terminal report leaves
  its labels to age out of the TTL, so the request is counted nowhere. Slow and
  failed requests are exactly the ones most likely to go missing.
- **Requests kinference no longer recognises.** If the `response_id` is unknown —
  already reported, or TTL-swept — there is no pick timestamp to measure from
  and the observation is skipped rather than guessed.

Its bucket edges match `kinference_ttft_ms` through 80s, so where they overlap
`duration - ttft` reads as decode time, and it carries three longer buckets
(160s, 300s, 600s) because a full generation routinely outruns any TTFT.

Unlike every other latency family, this one covers **both local and off-cluster
picks** — `ReportFinalToken` is a single terminal RPC and a provider pick stashes
its labels in the same place a local one does. So it is the only latency metric
you can break down by tenant without silently excluding provider traffic. (For a
provider pick the clock starts when the pick was recorded rather than at scoring
time, a difference of microseconds.)

`kinference_ttft_ms` covers **local** picks only, and **streaming requests only** —
for a non-streaming response the reported first-byte time is the whole response,
not a time-to-first-token, so folding it in would inflate the distribution.

`kinference_provider_ttft_ms` is the off-cluster counterpart, and it exists because a
provider pick reaches no other histogram: it holds no `PendingObservation`, so it
never passes through the code path that feeds `kinference_ttft_ms`. Both share bucket
edges (10ms → 80s) so the two can be read side by side when deciding whether to
spill off-cluster.

It is **also streaming-only**, for the same reason and by the same rule. That
took a fix: a provider pick has no `PendingObservation` to carry the streaming
flag, so the router used to admit every reported first-token time — meaning a
non-streaming provider request contributed its *total latency* to a TTFT
distribution. The flag now rides on the provider's request labels instead.

That mattered beyond the histogram. The same sample feeds the trailing p95 below,
which `EstimatedTTFT` ranks providers by and `backend.observedTTFTP95` predicates
compare against, so an inflated sample cost a provider picks it should have won.
A non-streaming provider request now still clears the provider's failure streak —
it demonstrably worked — but contributes no latency sample. A provider that only
ever serves non-streaming traffic therefore stays eligible under `EstimatedTTFT`
and sorts last, the same as any provider awaiting its first observation.

It is also the **only** operator-visible reading of provider latency. The router's
own trailing p95 — the `EstimatedTTFT` ranking input, and what a
`backend.observedTTFTP95` predicate compares against — is computed on the routing
path and never published. To see the figure a policy is being written against:

```promql
histogram_quantile(0.95,
  sum by (le, provider) (rate(kinference_provider_ttft_ms_bucket[5m])))
```

Successful reports only. A 401 or a 5xx carries a time-to-*error*, and mixing that
in would make a failing vendor look fast; those surface on the provider's
`Suppressed` / `Backend*` state instead.

### Inter-token latency is hand-aggregated

`kinference_itl_ms_bucket` is a **counter with an `le` label**, not a native
histogram. Callers send per-request bucket *counts* in `ReportFinalToken`, and the
client library cannot ingest pre-bucketed data, so the buckets are accumulated by
hand. Consequences:

- `histogram_quantile` works on it as usual.
- There is no `_sum` series — the wire carries counts, not raw gap values, so no
  mean is derivable.
- `kinference_itl_ms_count` is the companion total, equal to the `le="+Inf"` bucket.

Fixed layout, 15 upper-inclusive edges in ms: 5, 10, 15, 20, 30, 40, 50, 75, 100,
150, 200, 300, 500, 1000, +Inf. A report whose declared layout does not match is
dropped rather than misfiled, as is one whose bucket count is the wrong length.

An **empty** report is neither — it is the prescribed value for a response with
fewer than two chunks, so every non-streaming request sends one. It records
nothing and is not an error. So this family is effectively streaming-only too,
though by arithmetic rather than by a gate: a single chunk has no gaps to
measure.

## Prefix cache

Prefix-cache affinity is the reason the router exists, so these are the metrics
that say whether it is working.

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_prefix_requested_blocks_total` | counter | `model`, `qos` |
| `kinference_prefix_matched_blocks_total` | counter | `model`, `qos` |
| `kinference_prefix_radix_matched_blocks_total` | counter | `model`, `qos` |
| `kinference_prefix_match_ratio` | histogram | `model`, `qos` |
| `kinference_prefix_overmatch_total` | counter | `model`, `qos` |

The hit rate, at whatever window you want:

```promql
rate(kinference_prefix_matched_blocks_total[5m]) / rate(kinference_prefix_requested_blocks_total[5m])
```

### Two overlaps, and the difference between them is the point

There are **two** numerators over one denominator, because the router computes
two different overlap numbers and they answer different questions.

- `kinference_prefix_radix_matched_blocks_total` — what the radix tree alone said was
  already cached on the chosen worker. Divided by `requested`, this is *"is my
  index finding real cache hits?"*
- `kinference_prefix_matched_blocks_total` — the **layered** overlap: the radix match
  plus credit for blocks that in-flight requests are still prefilling, weighted
  by the profile's `expected_overlap_alpha`. This is the number the picker
  actually scored on, so divided by `requested` it is *"how much overlap did I
  bet on?"*

Subtract them and you have how much credit in-flight prefill is receiving —
which is what `expected_overlap_alpha` controls, and is otherwise tuned blind.

`kinference_prefix_match_ratio` is the layered ratio per pick. Read it alongside the
counters, not instead of them: the counters give the volume-weighted rate, the
histogram gives the **shape**. A fleet averaging 0.45 might be uniformly
mediocre or bimodal 0/0.9, and those want opposite responses. Buckets are linear
across `[0, 1]` because the interesting structure is at both ends.

### These count picks, not requests

They are recorded at pick time, so they include non-streaming picks and picks
whose caller never reports back. That makes them the most complete view of
routing behaviour available — the observation CSV is streaming-only and
`kinference_request_duration_ms` is report-gated — but it also means **they will not
add up against `kinference_prompt_tokens_total`**, which counts terminal reports.

### Degraded picks are excluded

A pick that lost its prefix signal is counted by
`kinference_score_degraded_total{reason}` and appears in none of the metrics above.
That exclusion is load-bearing rather than tidy: a `tokenize` failure yields
zero matched blocks against a zero-length prompt, which as a ratio would read
`0.0` — indistinguishable from a genuine cache miss. Without the exclusion a
tokenize outage would look like a prefix-routing collapse. (A `prefix_index`
failure is worse still: it can report matched blocks against a zero-length
prompt.)

So if the hit rate looks healthy, check `kinference_score_degraded_total` before
believing it — the two are complementary, and a fleet can have a fine hit rate
across the few picks that still had a signal.

### `kinference_prefix_overmatch_total`

Matched blocks can legitimately exceed the prompt's block count. Overlap scores
are summed across data-parallel ranks, so a worker running DP > 1 that publishes
one prefix from two ranks reports roughly double. The ratio observation is
clamped to `1.0` so the histogram stays meaningful; this counter records how
often that happened, because a silent clamp would hide the misconfiguration. The
block counters keep the raw values, so the counter-derived rate still shows the
anomaly.

Non-zero on a fleet you believe is DP = 1 means something is publishing twice.

## Prefix index health

The radix tree's own state, as distinct from how well it is matching.

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_index_blocks` | gauge | — |
| `kinference_index_workers` | gauge | — |
| `kinference_index_events_total` | counter | `kind` |
| `kinference_index_blocks_stored_total` | counter | — |
| `kinference_index_blocks_removed_total` | counter | — |

**`kinference_index_workers` against your configured worker count is the one to alert
on.** A worker missing from the index can never win a cache-affinity pick, so it
silently receives no prefix credit and quietly becomes a load-only destination.
Nothing else surfaces that.

Both gauges are fleet-wide (summed across pools) and refresh on a **throttled
tree dump, roughly every 10 seconds** — not per scrape, because the dump is
O(tree). Between dumps they hold their previous value, so treat them as a
coarse size signal rather than something to alert on at fine granularity.

`kind` is `stored`, `removed` or `cleared`. Note these count events the router
**ingested**, not events the tree successfully **applied**: the underlying
indexer drops an event whose parent block it never saw (the "Failed to find
parent block" warning), and that drop is not visible here. The indexer's own
apply/error counters exist upstream but are constructed unregistered and cannot
be reached without a dynamo runtime, so there is no status dimension available
today.

## Routing decisions

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_policy_decisions_total` | counter | `outcome` |
| `kinference_policy_rule_matches_total` | counter | `rule` |
| `kinference_score_degraded_total` | counter | `reason` |
| `kinference_policy_revision_mismatch_total` | counter | — |
| `kinference_routed_requests_total` | counter | `tenant`, `destination`, `type` |
| `kinference_predicted_ttft_ms` | histogram | `worker`, `model` |

`outcome` is the single most useful series in the set:

| `outcome` | Means |
| --- | --- |
| `chosen` | A rule matched and picked a destination |
| `unmatched_served` | No rule matched, or every matching rule fell through, and the `unmatched: ServeLocal` route picked |
| `rejected_on_empty` | A rule excluded every candidate and its `onEmpty` is `Reject` (the default) |
| `rejected_unmatched` | No rule matched, or every matching rule fell through, and `unmatched: Reject` |
| `unmatched_empty` | The unmatched route had no surviving candidate |
| `no_destinations` | The pool lists nothing routable |
| `unknown_pool` | The Pick named a pool the router has never seen — **fails open**, routing on load alone |
| `not_listed` | Same, but before the first CRD list completed. Expected briefly at startup |
| `forced_endpoint_missing` | The policy chose an endpoint absent from the scored set, and the picker overruled it. Should be unreachable; pair with `chosen` to detect |

`unmatched_served` climbing is the one to watch when a rule is matching less
traffic than you think — easy to do when a policy has a single rule.

`unknown_pool` and `not_listed` both mean the pool's scoping was silently ignored
for those requests. The first is a misconfiguration; the second is why `/readyz`
gates on the CRD watch having listed.

`kinference_score_degraded_total{reason}` counts picks made without a full signal:
`tokenize` (tokenization unavailable across the fleet) or `prefix_index` (the
index query failed). Sustained non-zero means `predictedTTFT` is running on
reduced information.

These picks are **excluded** from the prefix-cache metrics above, so read the
two together: a healthy-looking hit rate over a small number of picks that still
had a signal is not a healthy fleet.

`kinference_policy_revision_mismatch_total` counts Picks whose `inference_pool`
`resourceVersion` differed from the router's copy. Advisory — the router proceeds
on its cache rather than paying an API round-trip per pick — but a persistently
climbing rate means the gateway and the router disagree about the pool.

Note the router **writes** `InferencePool` status, and any write bumps
`metadata.resourceVersion` — so a caller holding the revision it programmed on
goes stale through no fault of either side. An isolated burst after a status
write is expected; a sustained climb is a caller that never refreshes.

### `kinference_routed_requests_total{tenant,destination,type}`

Where each tenant's requests went. One count per pick that chose a destination:
`destination` is the worker set or provider, by the name a policy uses for it,
and `type` is `Local` or `Provider` — on the cluster or off it. `tenant` follows
`[metrics] tenant_labels` like every other tenant family.

It is the family to read for that split. The token families' `model` is the
model each request was sent with — the worker's model name for a local pick,
the provider row's `name` for a provider pick — so one alias served both ways
shows up as two models, and a model name alone does not say where it ran.

Counted at the pick, so it says where requests were **sent**, whether or not they
then succeeded. A caller that retries starts over with a fresh pick, and the
retry counts again. A pick that routed nothing — no workers, a policy rejection —
is not counted here; see `outcome` above.

```promql
# Share of each tenant's requests sent off-cluster
sum by (tenant) (rate(kinference_routed_requests_total{type="Provider"}[5m]))
  / sum by (tenant) (rate(kinference_routed_requests_total[5m]))
```

### `kinference_predicted_ttft_ms{worker,model}`

The distribution of predicted time-to-first-token across candidates — the values
an [InferencePolicy](../crds/inferencepolicy) `drop: {predictedTTFT: ...}`
clause compares against, so that threshold can be read off real data rather than
guessed.

**Every candidate that was scored, not just the winner.** The distinction is the
point: a threshold applies to every candidate too, and the winner's distribution
is biased low precisely because it won. Recording only winners would flatter the
p95 into uselessness.

Its buckets are deliberately `kinference_ttft_ms`'s, so the predicted and the
observed distributions are directly comparable. That comparison is how a stale
predictor fit surfaces — "predicted p95 of 2s, delivered 5s" means the
coefficients need refitting, not that the fleet is slow:

```promql
histogram_quantile(0.95, sum by (le) (rate(kinference_predicted_ttft_ms_bucket[5m])))
```

Recorded by exactly **one** ranker per request — the policy engine when a policy
is attached, the plain picker otherwise. Both predict per endpoint; recording in
both would double-count the winning group and pull the p95 toward the fastest
one. A Pick naming a pool the watch cannot resolve takes neither branch and
contributes no sample, which is already visible as
`kinference_policy_decisions_total{outcome="unknown_pool"}`.

## Worker saturation

The two signals an [InferencePolicy](../crds/inferencepolicy) exclusion
thresholds on, published per worker so a threshold is calibrated against what
the fleet actually does rather than guessed.

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_worker_queued_prompts_mean30s` | gauge | `worker`, `model` |
| `kinference_worker_kv_cache_usage_mean30s` | gauge | `worker`, `model` |

These are the **exact** values `queuedPrompts` and `kvCacheUsage` read, not
approximations — a value seen here is a value a policy will act on.

### Why they are means, not instantaneous readings

Both average over a 30-second window, sharing the same window as
`completion_rate_per_sec` and the prefix-cache hit rate.

`num_requests_waiting` is **bursty** under continuous batching. The scheduler
admits requests until it hits `max_num_seqs` or runs out of KV blocks, so the
queue sits at exactly zero through the whole degradation range and then spikes
for a few seconds and clears. A threshold on the raw gauge drops a worker over a
transient that has already passed, and under `onEmpty: Reject` can refuse a
request because of one.

A mean rather than a max, deliberately: a max would pin the value at the peak
for the whole window, so one blip would *extend* an exclusion for 30 seconds
rather than damp it.

### `queuedPrompts` is fractional, so thresholds need rethinking

A worker that queued 5 prompts for two seconds out of thirty reads about `0.3`;
one genuinely backed up across the window reads near `5`.

It follows that **`greaterThan: 0` is far stricter than it looks** — it fires on
any queueing at all in the window, and sheds load for up to 30 seconds
afterwards. That is a reasonable shape for batch traffic, which has nowhere
urgent to be. For an interactive rule, a fractional bound such as
`greaterThan: 0.5` is usually what you want.

### The two are not interchangeable

`kvCacheUsage` is the better **headroom** signal, because it rises monotonically
with pressure and the queue does not. Measured on a real fleet: a moment at 93%
KV carried one queued prompt, while a moment at 31% KV carried five. A queue
threshold ranks those two backwards.

Threshold the queue for admission backlog, and `kvCacheUsage` for "this GPU has
no room left". They catch different failures.

### Reading them

Published once per `(worker, model)` so they join against the per-model
families, but the value is a property of the **engine** — a worker serving two
models reports the same number twice. Collapse the duplicate when graphing:

```promql
max by (worker) (kinference_worker_kv_cache_usage_mean30s)
```

A worker that leaves the fleet leaves no reading behind; the series stops, which
Prometheus renders as a gap. A stale `0.95` would read as a permanently
saturated GPU and a stale `0` as permanent headroom, and both are worse than
absence — especially for someone reading the graph to pick a threshold.

## Configuration health

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_workerset_rejected_total` | counter | `reason` |
| `kinference_policy_warnings` | gauge | `policy`, `code` |

One increment per `InferenceWorkerSet` refused, by the same reason string that
appears in its `STATE` column: `MissingProfile`, `UnknownProfile`,
`MissingKvBlockSize`, `InvalidKvBlockSize`, `MissingServiceRef`,
`KvEventsMissingPort`, `ModelMissingName`. See
[InferenceWorkerSet]({{< relref "inferenceworkerset.md" >}}) for what each means.

`kinference_policy_warnings` is how many findings each `InferencePolicy`
carries, by code: its `status.warnings`, as a number an alert can watch.
`policy` is `namespace/name`, and `code` is one of the
[warning codes]({{< relref "inferencepolicy.md#warnings" >}}). It counts the
full list, even past the 32 entries `status.warnings` can hold, and a policy
with no findings has no series.

Only the replica writing status publishes it: the leader, or every replica when
leader election is disabled. Take the max across replicas, not the sum:

```promql
max by (policy, code) (kinference_policy_warnings)
```

## Body buffer

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_body_buffer_bytes` | gauge | — |
| `kinference_body_buffer_entries` | gauge | — |
| `kinference_body_pushed_total` | counter | — |
| `kinference_body_pushed_bytes_total` | counter | — |
| `kinference_body_reaped_total` | counter | — |
| `kinference_body_rejected_total` | counter | `reason` |
| `kinference_body_aborted_total` | counter | `reason` |

Deliberately **not** labelled by `model`/`qos` like everything above: no model is
known at push time, and these describe server memory rather than per-model
traffic.

`kinference_body_buffer_bytes` is the one to alarm on — watch it against the
`body_max_total_kb` budget (configured in KB; this metric reports bytes). `kinference_body_reaped_total` counts bodies reclaimed
by TTL rather than by a caller finishing with them; a rising rate means callers
are abandoning requests after `Pick`.

`kinference_body_rejected_total{reason}` is `declared_too_large`, `request_too_large`
or `server_full`. `kinference_body_aborted_total{reason}` mirrors the proto's
`AbortRequest.Reason`: `not_dispatched`, `pull_failed`, `backend_unresolved`,
`backend_unauthorized`, `unspecified`.

The two `backend_*` reasons are worth a dashboard of their own. They are the only
channel through which the router learns that a `GatewayBackend` is broken — it
never reads that object and holds no credential — so a climbing
`backend_unauthorized` is a wrong Secret key that nothing else in the system can
see.

## Leadership

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_leader` | gauge | — |
| `kinference_leader_transitions_total` | counter | — |
| `kinference_leader_election_errors_total` | counter | — |

`kinference_leader` is 1 on the replica holding the lease and 0 on followers. With
leader election disabled it is 1 everywhere, since every replica writes.

The gauge is **seeded at construction**, not only on the first transition, which
is what makes two different failures distinguishable:

| Condition | Means |
| --- | --- |
| `sum(kinference_leader) == 0` | Pods are up and reporting, but nobody holds the lease |
| the series is **absent entirely** | No pod is reporting at all |

Both are alert conditions, because status writing happens only on the leader:
either one means **no CR status is being written**. That failure is otherwise
invisible — every object simply stops updating, with nothing to say why, and the
`STATE` columns keep showing whatever was last written.

A climbing `kinference_leader_transitions_total` means the lease is flapping; it counts
only actual changes, not ticks.

## Auth and rate limiting

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_token_review_total` | counter | `outcome` |
| `kinference_rate_limit_total` | counter | `outcome` |

`token_review` outcomes are `allowed`, `invalid`, `timeout` and `transport` — the
last two separate a rejected token from an unreachable API server, which are very
different incidents. `rate_limit` outcomes are `allowed`, `commit_ok` and
`commit_not_found`, following the `Reserve`/`Commit` pair.

## Engine metrics are not here

The router **scrapes** vLLM and SGLang for queue depth, KV-cache utilisation and
prefix-cache hit rate, and uses them for scoring — but does not re-export them.
Scrape the engines directly for `vllm:num_requests_running`,
`vllm:kv_cache_usage_perc` and the rest. What the router saw most recently is
visible at `/api/snapshot` on the same listener.

Note the engine's own prefix-cache hit rate is a **different quantity** from the
`kinference_prefix_*` metrics above. Those are what the router *predicted* from its
radix tree at pick time; the engine reports what it actually achieved. Comparing
them is the real test of whether the index reflects reality, and it currently
requires scraping both sides. (If a gateway in front of the engine blocks its
`/metrics`, the router's `/api/snapshot` is the only place that figure appears.)
