package metrics

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
)

type Record struct {
	Start            time.Time
	TTFT             time.Duration
	ITLs             []time.Duration
	E2E              time.Duration
	PromptTokens     int
	CompletionTokens int
	Err              string
}

type Collector struct {
	mu      sync.Mutex
	records []Record
}

func NewCollector() *Collector {
	return &Collector{}
}

func (c *Collector) Add(start time.Time, r client.RequestResult) {
	rec := Record{
		Start:            start,
		TTFT:             r.TTFT,
		ITLs:             r.ITL,
		E2E:              r.E2E,
		PromptTokens:     r.PromptTokens,
		CompletionTokens: r.CompletionTokens,
	}
	if r.Err != nil {
		rec.Err = r.Err.Error()
	}
	c.mu.Lock()
	c.records = append(c.records, rec)
	c.mu.Unlock()
}

func (c *Collector) Records() []Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Record, len(c.records))
	copy(out, c.records)
	return out
}

type Percentiles struct {
	P50 float64 `json:"p50_ms"`
	P90 float64 `json:"p90_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Min float64 `json:"min_ms"`
	Max float64 `json:"max_ms"`
}

type ErrorBucket struct {
	Message string `json:"message"`
	Count   int    `json:"count"`
}

type Summary struct {
	Workload          string        `json:"workload"`
	Endpoint          string        `json:"endpoint"`
	Model             string        `json:"model"`
	Concurrency       int           `json:"concurrency"`
	TotalRequests     int           `json:"total_requests"`
	SuccessRequests   int           `json:"success_requests"`
	FailedRequests    int           `json:"failed_requests"`
	WallSeconds       float64       `json:"wall_seconds"`
	RequestsPerSec    float64       `json:"requests_per_sec"`
	OutputTokensPerS  float64       `json:"output_tokens_per_sec"`
	TotalPromptTokens int           `json:"total_prompt_tokens"`
	TotalOutputTokens int           `json:"total_output_tokens"`
	TTFT              Percentiles   `json:"ttft"`
	ITL               Percentiles   `json:"itl"`
	// TPOT is time-per-output-token averaged over each REQUEST's decode phase,
	// then percentiled across requests. Different from ITL, which percentiles
	// individual token-gaps across all requests. TPOT is the right metric for
	// async workloads where the client waits for a full response — a single
	// slow gap doesn't hurt if the request finishes on time overall.
	TPOT              Percentiles   `json:"tpot"`
	E2E               Percentiles   `json:"e2e"`
	ErrorBuckets      []ErrorBucket `json:"error_buckets,omitempty"`
	// TTFTSamplesMS is populated only for small runs (TotalRequests < 50):
	// the raw per-request TTFT values from successful requests, sorted
	// ascending, in integer milliseconds. Useful for inspecting tail
	// behavior when there aren't enough samples for percentiles to be
	// meaningful. Omitted for larger runs.
	TTFTSamplesMS []int `json:"ttft_samples_ms,omitempty"`
}

// smallRunThreshold is the maximum total request count for which raw TTFT
// samples are included in the summary. Below this, percentiles are too coarse
// to be useful, so we dump the sorted list instead.
const smallRunThreshold = 1025

func Summarize(records []Record, wall time.Duration) Summary {
	var ttfts, itls, tpots, e2es []time.Duration
	var success, failed, prompt, output int
	errCounts := map[string]int{}

	for _, r := range records {
		if r.Err != "" {
			failed++
			errCounts[canonErr(r.Err)]++
			continue
		}
		success++
		ttfts = append(ttfts, r.TTFT)
		e2es = append(e2es, r.E2E)
		itls = append(itls, r.ITLs...)
		// Per-request TPOT: average time-per-output-token for this request's
		// decode phase. Requires ≥ 2 output tokens (need at least one gap).
		// If ITLs is empty (single-token output, or nothing streamed), skip —
		// this request contributes no TPOT sample.
		if len(r.ITLs) > 0 {
			var sum time.Duration
			for _, g := range r.ITLs {
				sum += g
			}
			tpots = append(tpots, sum/time.Duration(len(r.ITLs)))
		}
		prompt += r.PromptTokens
		output += r.CompletionTokens
	}

	wallSec := wall.Seconds()
	var rps, tps float64
	if wallSec > 0 {
		rps = float64(success) / wallSec
		tps = float64(output) / wallSec
	}

	s := Summary{
		TotalRequests:     len(records),
		SuccessRequests:   success,
		FailedRequests:    failed,
		WallSeconds:       wallSec,
		RequestsPerSec:    rps,
		OutputTokensPerS:  tps,
		TotalPromptTokens: prompt,
		TotalOutputTokens: output,
		TTFT:              percentiles(ttfts),
		ITL:               percentiles(itls),
		TPOT:              percentiles(tpots),
		E2E:               percentiles(e2es),
		ErrorBuckets:      bucketErrors(errCounts),
	}
	if len(records) < smallRunThreshold && len(ttfts) > 0 {
		s.TTFTSamplesMS = sortedMS(ttfts)
	}
	return s
}

func sortedMS(ds []time.Duration) []int {
	out := make([]int, len(ds))
	for i, d := range ds {
		out[i] = int(math.Round(float64(d) / float64(time.Millisecond)))
	}
	sort.Ints(out)
	return out
}

// canonErr collapses transient details (timestamps, ephemeral ports, random
// IDs) so that "the same failure 100 times" buckets together. Truncates very
// long error bodies (some servers echo back the request).
func canonErr(msg string) string {
	const maxLen = 200
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "…"
	}
	return msg
}

func bucketErrors(counts map[string]int) []ErrorBucket {
	if len(counts) == 0 {
		return nil
	}
	out := make([]ErrorBucket, 0, len(counts))
	for m, c := range counts {
		out = append(out, ErrorBucket{Message: m, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Message < out[j].Message
	})
	const maxBuckets = 5
	if len(out) > maxBuckets {
		out = out[:maxBuckets]
	}
	return out
}

func percentiles(ds []time.Duration) Percentiles {
	if len(ds) == 0 {
		return Percentiles{}
	}
	sorted := make([]time.Duration, len(ds))
	copy(sorted, ds)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return Percentiles{
		P50: ms(quantile(sorted, 0.50)),
		P90: ms(quantile(sorted, 0.90)),
		P95: ms(quantile(sorted, 0.95)),
		P99: ms(quantile(sorted, 0.99)),
		Min: ms(sorted[0]),
		Max: ms(sorted[len(sorted)-1]),
	}
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo] + time.Duration(float64(sorted[hi]-sorted[lo])*frac)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
