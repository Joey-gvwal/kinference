# LLM Benchmark Harness

A Go CLI for driving load against an OpenAI-compatible inference endpoint and
measuring latency/throughput. Designed for evaluating KV-cache-aware routers but
the harness itself is agnostic to what sits behind the URL.

## What it measures

For each request the harness opens a streaming `POST /v1/chat/completions` with
`stream: true` + `stream_options.include_usage: true`, and captures:

- **TTFT** — wall time from request send to the first content chunk
- **ITL** — per-token inter-arrival time during decode
- **E2E** — total request time
- **Throughput** — completed requests/sec and output tokens/sec
- **Token counts** — from the final `usage` chunk (no client-side tokenizer)

Aggregated as p50/p90/p95/p99 + min/max and emitted as a JSON report.

## Workloads

| Name        | What it does                                                                 |
|-------------|------------------------------------------------------------------------------|
| `synthetic` | N shared prefixes of K tokens, with random tails. Optional Zipf skew over prefix selection. Best for *proving* a router exploits prefix sharing. |
| `multiturn` | Like `synthetic` but with explicit conversation state: M concurrent
conversations × N turns each.
| `mooncake`  | Replay a Moonshot AI Mooncake conversation trace (JSONL). Each `hash_id` is rendered as a deterministic 512-token block, so requests sharing leading `hash_ids` share leading text. Honors `timestamp` for pacing (`--mooncake-speedup` to compress). |
| `sharegpt`  | Real multi-turn chat from a ShareGPT JSON dump. Emits one request per user turn carrying the growing message history — exercises in-session prefix reuse. |

## Build

```
cd benchmark
go build -o bench ./cmd/bench
```

## Quick smoke test

A stdlib-only Python stub is provided for local validation:

```
# terminal 1
python3 scripts/openai_stub.py --port 8000 --ttft-ms 50 --itl-ms 10 --output-tokens 32

# terminal 2
./bench --base-url http://localhost:8000 --model stub \
        --workload synthetic --concurrency 4 --total-requests 16 \
        --prefix-count 2 --prefix-tokens 256 --tail-tokens 32 \
        --output report.json
```

You should see TTFT p50 ≈ 50ms and ITL p50 ≈ 10ms.

## Usage

```
bench \
  --base-url http://router:8080 \
  --model meta-llama/Llama-3.1-8B-Instruct \
  --workload synthetic|mooncake|sharegpt \
  --concurrency 32 \
  [--output report.json]
```

Run `./bench --help` for the full flag list.

### Synthetic shared-prefix

```
./bench --base-url $URL --model $MODEL \
        --workload synthetic \
        --prefix-count 4 --prefix-tokens 2048 --tail-tokens 128 \
        --total-requests 1000 --concurrency 32 \
        --prefix-zipf 1.2          # optional: skew toward prefix 0
```

The pool of `prefix-count` prefixes is generated deterministically from `--seed`.

### Mooncake trace replay

Download a slice of the [Mooncake conversation trace](https://github.com/kvcache-ai/Mooncake)
and point the harness at it:

```
./bench --base-url $URL --model $MODEL \
        --workload mooncake \
        --mooncake-trace conversation_trace.jsonl \
        --mooncake-speedup 10 \
        --concurrency 64
```

Each request's `max_tokens` is set to the trace's `output_length`.

### ShareGPT

```
./bench --base-url $URL --model $MODEL \
        --workload sharegpt \
        --sharegpt-path ShareGPT_V3_unfiltered_cleaned_split.json \
        --max-conversations 200 --concurrency 16
```

### Multi-turn (synthetic, controlled)

Like `synthetic` but with explicit conversation state: M concurrent
conversations × N turns each. Every turn of a conversation extends the
prior turn's prompt verbatim with a new (assistant, user) pair, and all
conversations share a common system prompt. Each request's prefix label is
`c<j>t<k>` (conversation j, turn k). Total requests = M × N. Send order
randomly interleaves conversations while preserving each conversation's
turn order.

```
./bench --base-url $URL --model $MODEL \
        --workload multiturn \
        --mt-conversations 50 --mt-turns 10 \
        --mt-system-tokens 1024 \
        --mt-user-tokens 200 --mt-assistant-tokens 600 \
        --concurrency 32
```

Use this when you want a controlled knob on prefix-sharing structure — e.g.
to probe how a prefix-aware router behaves as session depth grows, or to
isolate the effect of within-session prefix reuse from cross-session
sharing (only the system prompt is shared across conversations; each
conversation's turns are unique). Complements the more random `synthetic`
(no session state) and the empirically-grounded but less tunable
`mooncake` replay.

## Reasoning models (Qwen3, DeepSeek-R1, etc.)

None of the corpora benefit from thinking — synthetic and Mooncake replay use
gibberish prompts, and ShareGPT predates reasoning models. Thinking just adds
output-side latency the router can't influence, swamping the prefill/routing
signal we're trying to measure.

For Qwen3 (and any model whose chat template honors `enable_thinking`), pass:

```
./bench ... --no-think
```

This forwards `chat_template_kwargs: {"enable_thinking": false}` on every
request; vLLM passes it through to the chat template, suppressing the
`<think>` block. With thinking off, all generated tokens go to
`delta.content` and TTFT/ITL measure what we want.

DeepSeek-R1 cannot disable thinking and is a poor fit for routing benchmarks;
use a different model. For other reasoning models, check what their chat
template's "disable thinking" knob is called and either rename the flag or
edit the prompts to include the model-specific suppression token.

## Comparing a router against a baseline

Two options, in order of preference:

### Pairwise mode (recommended)

Set `--base-url-b` to activate. The harness materializes one canonical request
sequence and drives both endpoints through it with their own independent
worker pools. Pairing is by canonical index — A's i-th request and B's i-th
request always have the same prompt — so per-request deltas are directly
comparable without the noise of two separate runs.

```
./bench \
  --base-url   http://prefix-aware-router:8080 \
  --base-url-b http://round-robin:8080 \
  --label-a    router \
  --label-b    baseline \
  --model      meta-llama/Llama-3.1-8B-Instruct \
  --workload   synthetic --prefix-count 4 --concurrency 32 --total-requests 1000 \
  --output     pairwise.json
```

The text report shows:
- Two per-endpoint summary blocks (TTFT/ITL/E2E percentiles, throughput, errors).
- Aggregate pairwise TTFT delta stats: median/p90/p95/p99 of `B − A` (positive = A faster), and the % of pairs where A was faster.
- For small runs (`total < smallRunThreshold`), a per-request send-order table with both TTFTs and their delta.

Each endpoint runs at its own natural rate: a slow endpoint doesn't bottleneck
the fast one. For Mooncake replay, both sides share a single `runStart` so
`DelayUntil` (trace timing) lands at the same absolute moment on both.

Optional flags: `--api-key-b` (defaults to `--api-key`), `--label-a` /
`--label-b` (default "A"/"B"). Other flags (`--model`, `--no-think`,
`--seed`, `--timeout`, workload flags) apply identically to both endpoints.

### Two separate runs (legacy)

If you want a pure load-test comparison with no shared sequencing:

```
./bench --base-url http://prefix-aware-router:8080 --seed 42 ... --output prefix.json
./bench --base-url http://round-robin:8080         --seed 42 ... --output rr.json
diff <(jq .ttft prefix.json) <(jq .ttft rr.json)
```

Use the **same seed and workload flags** for both runs. Statistically much
weaker than pairwise — comparing two aggregate distributions, not per-request
behaviors.

## Troubleshooting

If a run reports failures, the text summary ends with a `Top errors (up to 5)`
section grouping the actual error messages by frequency. The JSON report has
the same data under `error_buckets`. Common patterns:

| Error                                      | Likely cause                                     |
|--------------------------------------------|--------------------------------------------------|
| `dial tcp ...: connection refused`         | `--base-url` host/port wrong or service not up   |
| `status 404: ...`                          | Endpoint path differs (some routers mount under a prefix); harness always POSTs to `{base-url}/v1/chat/completions` |
| `status 400: model ... not found`          | `--model` doesn't match what the server serves; check `curl $URL/v1/models` |
| `status 400: max context length`           | Prompts exceed `--max-model-len`. Reduce `--prefix-tokens`/`--tail-tokens` or filter Mooncake records |
| `status 401: unauthorized`                 | Need `--api-key` / `OPENAI_API_KEY`              |
| `no content chunks received`               | Server returned 200 but never sent SSE chunks — usually a buffering proxy in the path |
| `x509: certificate signed by unknown ...`  | TLS to a self-signed endpoint                    |

Quickest manual sanity-check (bypasses the harness entirely):

```
curl -N -X POST $BASE_URL/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"YOUR_MODEL","stream":true,
       "stream_options":{"include_usage":true},
       "messages":[{"role":"user","content":"hi"}],"max_tokens":16}'
```

The `-N` disables curl's output buffering so you see chunks arrive
progressively. If they arrive in one burst, something on the path is buffering
and TTFT/ITL won't be trustworthy.

## Tests

```
go test ./...
```

Covers: SSE protocol parsing (TTFT/ITL/usage), synthetic-prefix determinism,
Mooncake `hash_id` → block reuse, ShareGPT growing-history emission, and
percentile math.

## Layout

```
benchmark/
├── cmd/bench/                 # CLI entrypoint
├── internal/client/           # streaming OpenAI-compatible HTTP client
├── internal/runner/           # worker pool
├── internal/metrics/          # records, percentiles, JSON/text reports
├── internal/workload/         # Workload interface + 3 generators
├── scripts/openai_stub.py     # local stdlib-only stub for smoke tests
└── testdata/                  # fixtures for unit tests
```
