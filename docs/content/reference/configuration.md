---
title: Configuration
description: KInference configuration file reference
---

KInference core configuration is read from a single [TOML](https://toml.io/en/)
file, specified on the comandline with `--config`.

An unrecognised **top-level** key is a startup error naming the key and listing
the accepted ones. Keys inside a `[section]`, and in the separate workers file,
are tolerated and ignored.

## Top level configuration

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `listen` | string | required | gRPC bind address |
| `admin_listen` | string | unset | HTTP bind address for the dashboard, `/metrics` and `/readyz`. If unset, all three endpoints will be disabled. |
| `mock_mode` | bool | `false` | Answer deterministically without ingest, scraping or tokenization |

{{< /reference-table >}}

## Miscellaneous configuration

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `dashboard_max_workers` | integer | `16` | How many workers the live dashboard can display. |
| `metrics_poll_ms` | integer | `1000` | How frequently each worker's `/metrics` is scraped |
| `models_refresh_secs` | integer | `30` | How frequently each worker's `/v1/models` is polled |
| `replay_timeout_ms` | integer | `60000` | Budget for one KV-event replay against a worker's replay socket |
| `observation_ttl_secs` | integer | `600` | How long per-request bookkeeping is held awaiting caller feedback |
| `body_max_total_kb` | integer | `262144` | Server-wide request-body buffer cap, in KB (256 MiB) |
| `body_max_request_kb` | integer | `8192` | Per-request body cap, in KB (8 MiB) |
| `body_ttl_secs` | integer | `60` | Backstop release for a buffered body |
| `body_idle_timeout_secs` | integer | `30` | Maximum silence between `PushBody` chunks |

{{< /reference-table >}}

## Inference profile configuration

Section: `[profile]`

Every worker set is associated with an _inference profile_ that determines how
expected latency is calculated, how prompts are tokenized, and other details.
Profiles may be shared between worker sets.

Profile names must be non-empty and unique.

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `name` | string | required | Name of the profile |

{{< /reference-table >}}

### Per-profile tokenization configuration

Section: `[profile]`

In order to make use of KV-cache-aware load balancing, KInference must template
and tokenize new prompts _in the exact same way that the workers do_. The
easy way to do this is with a round trip to the worker's `/render` endpoint.

However, it can be significantly faster to avoid the round trip and handle this
tokenization in-process. KInference supports this for specific models.

{{< reference-table >}}

| Key | Type | Default | Effect |
| `tokenize.mode` | `render` \| `local` \| `compare` | `render` | How prompts are tokenized. `render` calls the engine's `/render`; `local` uses the in-process tokenizer; `compare` uses local and checks it against `/render` |

{{< /reference-table >}}

`http` is accepted as an alias for `render`.

### Per-profile latency prediction configuration

Section: `[profile.predictor]`

KInference computes the _expected latency_ of a worker by combining a set of
factors with coefficients. This section configures the coefficients.

{{< reference-table >}}

| Key | Type | Default |
| --- | --- | --- |
| `base_ms` | float | `400.0` |
| `forward_pass_ms` | float | `0.0` |
| `queue_ms_per_entry` | float | `1000.0` |
| `attention_uncached_block_ms` | float | `2.3` |
| `attention_uncached_per_every_block_ms` | float | `0.009` |
| `cache_pressure_penalty_ms` | float | `0.0` |
| `cache_pressure_saturated_threshold` | float | `0.9` |
| `saturated_cold_ms_per_entry` | float | `10000.0` |
| `q_running_penalty_peak_ms` | float | `700.0` |
| `q_running_penalty_peak_qr` | integer | `2` |
| `q_running_penalty_falloff_qr` | integer | `5` |
| `inflight_concurrent_capacity` | integer | `5` |
| `expected_overlap_alpha` | float | `1.0` |

{{< /reference-table >}}

### Per-profile Endpoint picking configuration

Section: `[profile.picker]`

Given a set of expected latencies, how should KInference choose between them?
This section configures its picking approach.

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `top_k_min` | integer | `2` | Minimum candidate set size |
| `top_k_fraction` | float | `0.2` | Candidate set as a fraction of eligible workers |
| `tie_tolerance_ms` | float | `50.0` | Predictions within this are treated as tied |
| `inflight_tracking` | bool | `true` | Credit prefixes that in-flight requests are still prefilling |
| `replication_threshold_pct` | float | `0.0` | Prompt-share threshold above which a prefix is replicated. `0` disables |
| `replication_warm_target` | integer | `2` | Workers a replicated prefix should be warm on |
| `replication_warm_threshold_fraction` | float | `0.5` | Overlap fraction counting as warm |

{{< /reference-table >}}

## Worker set configuration

Section: `[workerset]`

An internally homogeneous set of workers: same model, same engine, same KV
block size. Names must be unique and non-empty. At least one is required unless
`[kubernetes] enabled = true`, which derives them from `InferenceWorkerSet` CRs
instead.

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `name` | string | required | Set identity, and the `workerset` label on observations |
| `profile` | string | required | Must name a defined `[[profile]]` |
| `kv_block_size` | integer | required | Tokens per block in the events these engines publish. Must be > 0 |
| `render_url` | string | unset | Override for the engine `/render` endpoint |

{{< /reference-table >}}

`kv_block_size` must match what the engine actually publishes, which is not
always its `--block-size` flag. A worker publishing a different value is
evicted.

### Worker set discovery configuration

Section: `[workerset.discovery]`

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `mode` | `static` \| `kubernetes` | `static` | Where workers come from |
| `workers_file` | path | — | Required for `static`. See [Workers file](#workers-file) |
| `service_name` | string | — | Required for `kubernetes`. The Service whose EndpointSlices are watched |
| `namespace` | string | inherited | Namespace to watch |

{{< /reference-table >}}

#### Worker set discovery defaults configuration

Section: `[workerset.discovery.defaults]`

Ports used when discovery finds no named port. Named ports themselves are not
configurable here — they come only from an `InferenceWorkerSet` CR, so a
`[[workerset]]` in this file resolves ports by these defaults alone.

{{< reference-table >}}

| Key | Type | Default |
| --- | --- | --- |
| `http_port` | integer | `8000` |
| `zmq_port` | integer | `5557` |
| `replay_port` | integer | unset |

{{< /reference-table >}}

## Static workers file configuration

The file named by `workers_file`, containing a flat `[[workers]]` list. Both
`worker_id_num` and `worker_id_str` must be unique across every worker set.

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `worker_id_str` | string | required | Exposed over gRPC and in logs |
| `worker_id_num` | integer | required | Keys the fleet-wide telemetry and metrics registries |
| `model_name` | string | required | Filters label-matched engine metrics |
| `zmq_endpoint` | string | required | The engine's KV-event PUB socket. Optional only when `mock_mode` is true |
| `replay_endpoint` | string | unset | The engine's replay socket. Unset means live-only ingest |
| `metrics_url` | string | unset | The engine's `/metrics`. Unset leaves load fields at zero |
| `http_url` | string | unset | The engine's HTTP base, for `/render` and `/v1/models` |
| `endpoint` | string | derived | Address returned to callers. Derived from `http_url` when unset |
| `pod_name`, `namespace`, `node_name`, `node_ip`, `pod_ip`, `availability_zone`, `instance_type` | string | unset | Topology labels, for the dashboard and observations |

{{< /reference-table >}}

The engine type is not configured here. It is detected from the engine's own
`/v1/models` response and used only for display.

## Top-level tokenization configuration

Section: `[tokenize]`

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `pool_max_idle_per_host` | integer | unset | Idle HTTP connections kept per engine |
| `local.mount_dir` | path | unset | Looked up as `{mount_dir}/{model}/tokenizer.json`. Unset fetches from the Hub |
| `local.warmup` | bool | `true` | Load each model's tokenizer at worker registration |

{{< /reference-table >}}

## Metrics configuration

Section: `[metrics]`

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `tenant_labels` | `"none"` \| `"all"` \| list of strings | `"none"` | Which tenants get their own `tenant` label. See [Metrics]({{< relref "metrics.md" >}}) |

{{< /reference-table >}}

A value other than `"none"`, `"all"` or a list is a startup error.

## Policy configuraiton

Section: `[policy]`

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Watch the `kinference.buoyant.io` CRDs and honor `PickRequest.inference_pool` |
| `namespace` | string | all | Namespace to watch |

{{< /reference-table >}}

With this off, a request naming an `InferencePool` is routed on load alone.

## Kubernetes configuration

Section: `[kubernetes]`

Derive worker sets from `InferenceWorkerSet` CRs instead of `[[workerset]]`
blocks. Requires at least one `[[profile]]`, since every CR names one.

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Watch `InferenceWorkerSet` CRs |
| `namespace` | string | all | Namespace to watch |
| `selector` | string | unset | Label selector limiting which CRs are honored |

{{< /reference-table >}}

`[kubernetes.discovery_defaults]` takes the same keys as
[`[workerset.discovery.defaults]`](#workersetdiscoverydefaults) and applies to
every derived set.

## Authz Kubernetes token review configuration

Section: `[token_review]`

Validate Kubernetes bearer tokens for callers that cannot reach the apiserver
themselves. Needs `create` on `tokenreviews` in `authentication.k8s.io`.

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Serve `TokenReview` on the gRPC listener |
| `default_deadline_ms` | integer | `2000` | Budget for one apiserver round-trip |
| `positive_ttl_secs` | integer | `60` | How long an authenticated result is cached |
| `negative_ttl_secs` | integer | `10` | How long a denied result is cached |
| `max_cache_entries` | integer | `4096` | Cache size cap |

{{< /reference-table >}}

Enabling this without available Kubernetes credentials is a startup error.

## Rate limiting configuration

Section: `[rate_limit]`

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Serve `RateLimiter` on the gRPC listener |

{{< /reference-table >}}

The only implementation admits every reservation and tracks no budget. Enabling
it logs a warning saying so.

## Kubernetes leader election configuration

Section: `[leader_election]`

Lease-based election, gating CRD status writes so replicas do not fight over
them.

{{< reference-table >}}

| Key | Type | Default | Effect |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Contend for the Lease |
| `lease_name` | string | `"kinference"` | Lease object name. Must be non-empty |
| `namespace` | string | inherited | Lease namespace |
| `lease_duration_secs` | integer | `15` | Lease validity |
| `renew_grace_secs` | integer | `10` | Renew once within this of expiry. Must be less than `lease_duration_secs` |
| `retry_secs` | integer | `2` | Retry interval after a failed Lease operation |

{{< /reference-table >}}

All three intervals must be greater than zero.
