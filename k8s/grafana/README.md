# Grafana

A Grafana dashboard over KInference's own `/metrics`, plus the scrape config
that makes it non-empty.

```
kinference-dashboard.json              the dashboard — source of truth, edit this
grafana-dashboard-configmap.yaml  generated wrapper, picked up by the sidecar
kinference-podmonitor.yaml             tells Prometheus to scrape kinference
```

## Apply

```bash
make -C k8s/kinference monitoring
```

Deliberately not part of `make deploy`: the PodMonitor needs the Prometheus
Operator CRDs, which a Docker Desktop cluster does not have. Install the stack
first — on the AWS harness that is `make -C aws-harness install-monitoring`,
which brings up kube-prometheus-stack with a Grafana sidecar watching for
ConfigMaps labelled `grafana_dashboard: "1"`.

The sidecar loads the dashboard within about 30 seconds. Reach it with
`make -C aws-harness expose-grafana` and `make -C aws-harness grafana-endpoint`.

## Edit

`kinference-dashboard.json` is the source of truth. The ConfigMap embeds a copy, so
after changing the JSON:

```bash
make -C k8s/kinference dashboard-configmap
```

Commit both files together. Editing the embedded copy directly makes the two
drift, and the cluster then shows something nobody can reproduce from the repo.

## What it covers

Eight rows, all 48 registered `kinference_*` families. The first is expanded and is
the only one an operator should need day to day:

| Row | Answers |
| --- | --- |
| Service health | Is the control plane picking, how fast, and is it degraded? |
| Traffic & tenants | Token volume and request shape, per tenant where allowed |
| Latency | End-to-end, TTFT, ITL, provider TTFT |
| Routing & policy | How the policy CRD resolved picks; which rules fired |
| Prefix cache | How much of each prompt was already cached |
| Prefix index health | Is the radix tree actually being fed? |
| Tokenization | Which tokenizer served each request, and does it agree? |
| Admission control & body store | Rate limiting, token review, body buffer |

## What it deliberately does not cover

**Per-worker anything.** `/metrics` carries no per-worker label — the
dimensions are `{model,qos,tenant}`, `{outcome}`, `{reason}`, `{state}`,
`{kind}`, `{path}`, `{result}` and `{provider,model}`. The per-worker table and
the prefix overlap map from the built-in SPA on `:9001` have no PromQL
equivalent and are not reproduced here. That SPA remains the drill-down.

**Engine-side KV pressure and queue depth.** `vllm:*` / `sglang:*` series are
scraped from the workers and re-exported by nothing; the KV-cache and queue
thresholds from the SPA cannot be expressed against any `kinference_*` family. Those
panels live on the vLLM dashboards, reachable from the dropdown at the top of
this dashboard — it keeps the time range, so a `no_workers` spike here is one
click from the KV-pressure view there.

## Reading it without being misled

Each panel carries a `description` with its own caveats. The ones that bite
hardest:

- **An empty panel usually means "has not happened yet", not zero.** Every
  labelled family is absent until its first observation. Only
  `kinference_build_info` and `kinference_leader` exist from startup — those are the
  "is it up" signals, and nothing should alert on series absence.
- **`kinference_rate_limit_total` and `kinference_token_review_total` do not exist at
  all** unless their subsystems are enabled, and both default off. Empty means
  disabled.
- **Zero tokenizer divergence proves nothing.** Those counters only move in
  compare mode, so a zero almost always means compare mode is off — not that
  the tokenizers agree.
- **Neither TTFT family sees a non-streaming request**, and `kinference_ttft_ms`
  also excludes provider picks. `kinference_request_duration_ms` is the only
  latency family with no population gap. The TTFT coverage panel puts a number
  on what you are missing.
- **`tenant="default"` is a reserved mixing bucket** — empty tenants,
  non-allowlisted tenants, callers literally sending `default`, and orphaned
  reports whose `response_id` was TTL-swept. Not billing-grade.

Full metric reference: `docs/content/reference/metrics.md`.
