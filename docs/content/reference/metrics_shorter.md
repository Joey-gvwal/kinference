---
title: Metrics
description: KInference metrics and telemetry
---

KInference exports a variety of metrics in
[Prometheus](https://prometheus.io/)-compatible format. By default, these
metrics are available via HTTP on port 9001 under the `/metrics` path.

## Build identity

{{< reference-table >}}

| Metric                  | Type  | Labels    |
| ----------------------- | ----- | --------- |
| `kinference_build_info` | gauge | `version` |

{{< /reference-table >}}

Always `1`; the information is in the `version` label. Unlike every other
labelled metric, this one is present before any traffic has been served, so its
absence means the process is not answering.

`version` is `KINFERENCE_VERSION` when the build sets it (the container build sets it
from the image tag) and the `Cargo.toml` version otherwise. It matches the
`--version` output and `/api/snapshot`.

Alert on more than one version being live at once to catch a stalled rollout:

```promql
count(count by (version) (kinference_build_info)) > 1
```

## A note on per-tenant metrics

Metrics are optionally available on a per-tenant basis, i.e. with a `tenant`
label. However, this can result in a large number of metrics: approximately 91
series per (model, qos, tenant) tuple.  At 5 models and 3 QoS classes that is
about 1,365 series per tenant.

Per-tenant metrics are off by default, but can be configured (including via an
explicit allowlist) via the `[metrics] tenant_labels` configuration block:

{{< reference-table >}}

| `tenant_labels` | Effect |
| --- | --- |
| omitted, `"none"`, or `[]` | **Default.** Every request reports `tenant="default"`. |
| `"all"` | Every tenant id seen becomes its own label value. Unbounded--use caution. |
| `["acme", "globex"]` | Allowlist of tenants to track; all other tenants are bucketed under "default" |

{{< /reference-table >}}

As with tenants outside of the allowlist, requests without tenant information
will also be bucketed under the tenant value of `default`.

## Router service health metrics

Whether KInference is answering `Pick()` successfully, and how fast.

{{< reference-table >}}

| Metric                                    | Type      | Labels           |
| ----------------------------------------- | --------- | ---------------- |
| `kinference_picks_total`                  | counter   | `outcome`        |
| `kinference_pick_duration_ms`             | histogram | `outcome`        |
| `kinference_workers`                      | gauge     | `state`          |
| `kinference_workers_by_model`             | gauge     | `model`, `state` |
| `kinference_worker_evictions_total`       | counter   | `reason`         |
| `kinference_orphaned_reports_total`       | counter   | —                |
| `kinference_dashboard_slots_used`         | gauge     | —                |
| `kinference_dashboard_slots_capacity`     | gauge     | —                |
| `kinference_dashboard_slots_denied_total` | counter   | —                |

{{< /reference-table >}}

Where `outcome` is one of:

{{< reference-table >}}

| `outcome` | Means |
| --- | --- |
| `ok` | A destination was chosen (including a degraded pick, which still routes) |
| `bad_request` | The body did not parse, or carried nothing to route on |
| `no_workers` | Nothing serves the requested model |

{{< /reference-table >}}

Distinct from `kinference_policy_decisions_total`, which reports how a destination
was chosen rather than what was returned to the caller; a pick that resolved no
policy still reports `ok`.

`kinference_pick_duration_ms` is KInference's own decision latency, not the model's.
A request that fails before a routing decision--an unknown or expired body,
which returns a gRPC `NOT_FOUND`--is not counted.

`kinference_workers` reports `state` as either `ok` or `unresponsive`, the latter
meaning the worker's metrics scrape has been stale for over 5 seconds. An
unresponsive worker is still routable, scored on its last known sample.

`kinference_workers_by_model` is the same reachability split by served model. A
worker serving several models is counted under each, which is why it is a
separate family rather than a label on `kinference_workers` — the two do not sum to
the same number and were never meant to.

`kinference_worker_evictions_total` counts workers dropped from the fleet, by
`reason`. `block_size_mismatch` means the engine published KV events at a page
size other than the set's `kvBlockSize`; those events are discarded before
insertion, so the prefix index stays empty while picks silently degrade to
load-only. Any non-zero value wants investigating — the fix is to read the size
off a real event and correct the CR, not to restart the worker.

`kinference_orphaned_reports_total` counts final-token reports that arrived with no
matching in-flight request, usually because the observation TTL expired first.
Their tokens are not counted anywhere, so a rising value means the token
families understate real traffic.

The dashboard slot metrics track the display pool sized by
`dashboard_max_workers`. They bound the live dashboard only: `kinference_workers`
counts every routable worker regardless. A worker beyond the pool routes and is
counted normally, it is just not drawn, so
`kinference_dashboard_slots_denied_total` is expected to be non-zero on any fleet
larger than the pool and is not an error.

## Token metrics

{{< reference-table >}}

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_prompt_tokens_total` | counter | `model`, `qos`, `tenant` |
| `kinference_completion_tokens_total` | counter | `model`, `qos`, `tenant` |
| `kinference_cached_prompt_tokens_total` | counter | `model`, `qos`, `tenant` |
| `kinference_cached_tokens_absent_total` | counter | — |
| `kinference_cached_tokens_reported_total` | counter | — |
| `kinference_prompt_length_tokens` | histogram | `model`, `qos`, `tenant` |
| `kinference_completion_length_tokens` | histogram | `model`, `qos`, `tenant` |

{{< /reference-table >}}

`model` is the model each request was sent with: the worker's model for a local
pick, after any alias resolves, and the matching row's `name` for a provider pick.

`kinference_cached_prompt_tokens_total` is the engine's own count of prompt tokens
served from its prefix cache. Subtract it from `kinference_prompt_tokens_total` for
the tokens that actually cost prefill compute.

`kinference_cached_tokens_absent_total` counts reports that omitted the field, whose
cache hits are therefore missing from that subtraction, so billing derived from
it overcharges.

Read it against `kinference_cached_tokens_reported_total`, never on its own. The two
are deliberately a pair: one counts reports carrying `cached_tokens`, the other
reports omitting it, and only their ratio says how complete the basis is. An
unbounded count answers "is anything missing"; the share answers "how much", and
that is the actionable one:

```promql
rate(kinference_cached_tokens_absent_total[5m])
/
(
  rate(kinference_cached_tokens_absent_total[5m])
  + rate(kinference_cached_tokens_reported_total[5m])
)
```

Both are unlabelled on purpose. They answer whether the basis is complete, not
whose it is — and dividing a tenant-split numerator by an unlabelled denominator
is how you get a share above 100%.

## Latency metrics

{{< reference-table >}}

| Metric | Type | Labels |
| --- | --- | --- |
| `kinference_request_duration_ms` | histogram | `model`, `qos`, `tenant` |
| `kinference_ttft_ms` | histogram | `model`, `qos`, `tenant` |
| `kinference_provider_ttft_ms` | histogram | `provider`, `model` |
| `kinference_itl_ms_bucket` | counter | `model`, `qos`, `le`, `tenant` |
| `kinference_itl_ms_count` | counter | `model`, `qos`, `tenant` |

{{< /reference-table >}}

Note that `kinference_ttft_ms` and `kinference_provider_ttft_ms` only populate in the
presence of streaming requests. A non-streaming response delivers its first byte
at completion, so the figure reported for it is total latency rather than
time-to-first-token, and both histograms exclude it. `kinference_ttft_ms` covers
on-cluster picks and `kinference_provider_ttft_ms` off-cluster ones.

The inter-token metrics are effectively streaming-only as well, by arithmetic
rather than by a gate: a single-chunk response has no gaps to measure, and the
empty report it sends records nothing.

Use `kinference_request_duration_ms` for end-to-end timing, which covers both and
does not distinguish streaming.

## Prefix cache metrics

Whether cache-affinity routing is finding reusable prefixes.

{{< reference-table >}}

| Metric                                         | Type      | Labels         |
| ---------------------------------------------- | --------- | -------------- |
| `kinference_prefix_requested_blocks_total`     | counter   | `model`, `qos` |
| `kinference_prefix_matched_blocks_total`       | counter   | `model`, `qos` |
| `kinference_prefix_radix_matched_blocks_total` | counter   | `model`, `qos` |
| `kinference_prefix_match_ratio`                | histogram | `model`, `qos` |
| `kinference_prefix_overmatch_total`            | counter   | `model`, `qos` |

{{< /reference-table >}}

The hit rate, over whatever window you want:

```promql
rate(kinference_prefix_matched_blocks_total[5m]) / rate(kinference_prefix_requested_blocks_total[5m])
```

Two numerators share one denominator: `matched` is the layered overlap the
picker scored on, `radix_matched` is the raw radix-tree match for the same
picks, and the difference between them is credit given to in-flight prefill.

Degraded picks are excluded, so read these alongside
`kinference_score_degraded_total`--a healthy ratio over few picks is not a healthy
fleet. `kinference_prefix_overmatch_total` counts picks where matched blocks exceeded
the prompt, which happens on data-parallel workers publishing one prefix from
several ranks; the ratio is clamped to 1.0 in that case.

## Prefix index health metrics

The radix tree's own state, as distinct from how well it is matching.

{{< reference-table >}}

| Metric                                  | Type    | Labels |
| --------------------------------------- | ------- | ------ |
| `kinference_index_blocks`               | gauge   | —      |
| `kinference_index_workers`              | gauge   | —      |
| `kinference_index_events_total`         | counter | `kind` |
| `kinference_index_blocks_stored_total`  | counter | —      |
| `kinference_index_blocks_removed_total` | counter | —      |

{{< /reference-table >}}

`kinference_index_workers` against the configured worker count is the one to alert
on: a worker missing from the index can never win a cache-affinity pick.

Both gauges refresh on a throttled tree dump roughly every 10 seconds, not per
scrape. `kind` is one of `stored`, `removed` or `cleared`, and counts events
ingested rather than events the tree successfully applied.

## Tokenization metrics

Tokenization is on the `Pick` critical path and everything prefix-related
depends on it.

{{< reference-table >}}

| Metric                              | Type      | Labels   |
| ----------------------------------- | --------- | -------- |
| `kinference_tokenize_total`         | counter   | `path`   |
| `kinference_tokenize_duration_ms`   | histogram | —        |
| `kinference_tokenize_compare_total` | counter   | `result` |

{{< /reference-table >}}

Where `path` is one of, exactly one per attempt:

{{< reference-table >}}

| `path` | Means |
| --- | --- |
| `local` | The in-process tokenizer served it |
| `http_fallback` | Local failed; the engine's `/render` saved it |
| `unavailable` | Both failed. The pick is degraded and routes on load alone |

{{< /reference-table >}}

`kinference_tokenize_compare_total` reports `result` as `match`, `divergence` or
`error`. A divergence means KInference's block hashes disagree with the
engine's, so cache-affinity routing silently stops working while the prefix
metrics continue to report plausible numbers.

It is only ever non-zero when `[profile.tokenize] mode` is `compare`, so a zero
is not by itself evidence of agreement.

## Routing decision metrics

{{< reference-table >}}

| Metric                                      | Type      | Labels                          |
| ------------------------------------------- | --------- | ------------------------------- |
| `kinference_policy_decisions_total`         | counter   | `outcome`                       |
| `kinference_policy_rule_matches_total`      | counter   | `rule`                          |
| `kinference_score_degraded_total`           | counter   | `reason`                        |
| `kinference_policy_revision_mismatch_total` | counter   | —                               |
| `kinference_routed_requests_total`          | counter   | `tenant`, `destination`, `type` |
| `kinference_predicted_ttft_ms`              | histogram | `worker`, `model`               |

{{< /reference-table >}}

Where `outcome` is one of:

{{< reference-table >}}

| `outcome` | Means |
| --- | --- |
| `chosen` | A rule matched and picked a destination |
| `unmatched_served` | No rule matched, or every matching rule fell through, and the `unmatched: ServeLocal` route picked |
| `rejected_on_empty` | A rule excluded every candidate and its `onEmpty` is `Reject` (the default) |
| `rejected_unmatched` | No rule matched, or every matching rule fell through, and `unmatched: Reject` |
| `unmatched_empty` | The unmatched route had no surviving candidate |
| `no_destinations` | The pool lists nothing routable |
| `unknown_pool` | The caller named a pool the router has never seen |
| `not_listed` | Same, but before the first CRD list completed. (Expected briefly at startup) |
| `forced_endpoint_missing` | The policy chose an endpoint absent from the scored set, and the picker overruled it. |

{{< /reference-table >}}

`kinference_routed_requests_total` counts picks that chose a destination: the
worker set or provider (`destination`, as a policy names it) and whether it is
`Local` or a `Provider` (`type`), per tenant. It is the way to see whether a
tenant's traffic stayed on the cluster: the token metrics' `model` is the model
each request was sent with (a provider pick reports its row's `name`), which does
not say where it ran. Counted at the pick, so a caller's retry counts again.

`kinference_predicted_ttft_ms` is the distribution of predicted time-to-first-token
across candidates, which is what an [InferencePolicy](../crds/inferencepolicy)
`drop: {predictedTTFT: ...}` clause compares against — so a threshold can be
read off real data rather than guessed.

Every candidate that was scored, not just the winner. The distinction matters:
a threshold applies to every candidate too, and the winner's distribution is
biased low precisely because it won.

Its buckets are deliberately `kinference_ttft_ms`'s, so the predicted and the
observed distributions can be compared directly. "Predicted p95 of 2s, delivered
5s" is how a stale predictor fit shows up:

```promql
histogram_quantile(0.95, sum by (le) (rate(kinference_predicted_ttft_ms_bucket[5m])))
```

Recorded by exactly one ranker per request — the policy engine when a policy is
attached, the plain picker otherwise — so it is populated either way, and the
winning group is not counted twice.

## Worker saturation metrics

The two signals an [InferencePolicy](../crds/inferencepolicy) exclusion
thresholds on, published per worker so a threshold can be calibrated against
what the fleet actually does.

{{< reference-table >}}

| Metric                                     | Type  | Labels            |
| ------------------------------------------ | ----- | ----------------- |
| `kinference_worker_queued_prompts_mean30s` | gauge | `worker`, `model` |
| `kinference_worker_kv_cache_usage_mean30s` | gauge | `worker`, `model` |

{{< /reference-table >}}

These are the exact values `queuedPrompts` and `kvCacheUsage` read, not
approximations of them, so a value seen here is a value a policy can act on.

Both are **means over a 30-second window**, not instantaneous readings.
`num_requests_waiting` is bursty under continuous batching — it spikes for a few
seconds and clears — so a threshold on the raw gauge sheds load over a transient
that has already passed, and under `onEmpty: Reject` can refuse a request
because of one. The window makes a threshold mean *sustained* pressure.

Being a mean, `kinference_worker_queued_prompts_mean30s` is fractional. A worker that
queued 5 prompts for two seconds out of thirty reads about `0.3`; one genuinely
backed up across the window reads near `5`. It follows that `greaterThan: 0` is
far stricter than it looks — it fires on any queueing at all in the window.

The two are not interchangeable, and KV is the better headroom signal: it rises
monotonically with pressure and the queue does not. On a real fleet, a moment at
93% KV carried one queued prompt while a moment at 31% carried five, so a queue
threshold ranks those two backwards. Threshold the queue for admission backlog
and `kvCacheUsage` for "this GPU has no room left".

Both are published once per `(worker, model)` so they join against the
per-model families, but the value is a property of the engine — a worker serving
two models reports the same number twice. Collapse the duplicate when graphing:

```promql
max by (worker) (kinference_worker_kv_cache_usage_mean30s)
```

A worker that leaves the fleet leaves no reading behind; the series simply
stops, which Prometheus renders as a gap. A stale `0.95` would read as a
permanently saturated GPU and a stale `0` as permanent headroom, and both are
worse than absence.

## Configuration health metrics

{{< reference-table >}}

| Metric                                | Type    | Labels           |
| ------------------------------------- | ------- | ---------------- |
| `kinference_workerset_rejected_total` | counter | `reason`         |
| `kinference_policy_warnings`          | gauge   | `policy`, `code` |

{{< /reference-table >}}

Number of [InferenceWorkerSet](../crds/inferenceworkerset)s rejected and the reason.

Reasons include: `MissingProfile`, `UnknownProfile`, `MissingKvBlockSize`,
`InvalidKvBlockSize`, `MissingServiceRef`, `KvEventsMissingPort`,
`ModelMissingName`.

`kinference_policy_warnings` is how many findings each
[InferencePolicy](../crds/inferencepolicy) carries, by
[warning code](../crds/inferencepolicy#warnings): its `status.warnings` in full,
as a number an alert can watch. `policy` is `namespace/name`.

Only the replica writing status publishes it: the leader, or every replica when
leader election is disabled. Take the max across replicas, not the sum:

```promql
max by (policy, code) (kinference_policy_warnings)
```

## Request buffering metrics

Internal metrics capturing the buffering of request bodies by KInference. These
may be useful for understanding KInference memory usage.

{{< reference-table >}}

| Metric                               | Type    | Labels   |
| ------------------------------------ | ------- | -------- |
| `kinference_body_buffer_bytes`       | gauge   | —        |
| `kinference_body_buffer_entries`     | gauge   | —        |
| `kinference_body_pushed_total`       | counter | —        |
| `kinference_body_pushed_bytes_total` | counter | —        |
| `kinference_body_reaped_total`       | counter | —        |
| `kinference_body_rejected_total`     | counter | `reason` |
| `kinference_body_aborted_total`      | counter | `reason` |

{{< /reference-table >}}

## Leadership election metrics

{{< reference-table >}}

| Metric                                    | Type    | Labels |
| ----------------------------------------- | ------- | ------ |
| `kinference_leader`                       | gauge   | —      |
| `kinference_leader_transitions_total`     | counter | —      |
| `kinference_leader_election_errors_total` | counter | —      |

{{< /reference-table >}}

Internal metrics corresponding to Kubernetes leader election. For diagnostic
purposes.

## Auth and rate limiting metrics

{{< reference-table >}}

| Metric                          | Type    | Labels    |
| ------------------------------- | ------- | --------- |
| `kinference_token_review_total` | counter | `outcome` |
| `kinference_rate_limit_total`   | counter | `outcome` |

{{< /reference-table >}}


Possible `token_review` outcomes are `allowed`, `invalid`, `timeout` and
`transport`.

Possible `rate_limit` outcomes are `allowed`, `commit_ok` and
`commit_not_found`.

