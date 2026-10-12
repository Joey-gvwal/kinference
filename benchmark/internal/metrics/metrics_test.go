package metrics

import (
	"math"
	"testing"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
)

func TestPercentilesBasic(t *testing.T) {
	// 1..100 ms; standard quantiles.
	var ds []time.Duration
	for i := 1; i <= 100; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	p := percentiles(ds)

	// Linear interpolation: p50 of 1..100 = 50.5.
	check := func(name string, got, want float64) {
		if math.Abs(got-want) > 0.05 {
			t.Errorf("%s = %.3f, want ~%.3f", name, got, want)
		}
	}
	check("P50", p.P50, 50.5)
	check("P90", p.P90, 90.1)
	check("P95", p.P95, 95.05)
	check("P99", p.P99, 99.01)
	check("Min", p.Min, 1.0)
	check("Max", p.Max, 100.0)
}

func TestPercentilesEmpty(t *testing.T) {
	p := percentiles(nil)
	if p != (Percentiles{}) {
		t.Errorf("empty input should give zero Percentiles, got %+v", p)
	}
}

func TestSummarizeCountsErrors(t *testing.T) {
	c := NewCollector()
	c.Add(time.Now(), client.RequestResult{
		TTFT: 10 * time.Millisecond, E2E: 100 * time.Millisecond, CompletionTokens: 20,
	})
	c.Add(time.Now(), client.RequestResult{Err: errFake("nope")})
	c.Add(time.Now(), client.RequestResult{
		TTFT: 30 * time.Millisecond, E2E: 300 * time.Millisecond, CompletionTokens: 30,
	})

	s := Summarize(c.Records(), 2*time.Second)
	if s.SuccessRequests != 2 || s.FailedRequests != 1 {
		t.Errorf("success=%d failed=%d, want 2 and 1", s.SuccessRequests, s.FailedRequests)
	}
	if s.TotalOutputTokens != 50 {
		t.Errorf("output tokens = %d, want 50", s.TotalOutputTokens)
	}
	wantRPS := 1.0 // 2 successes / 2s
	if math.Abs(s.RequestsPerSec-wantRPS) > 0.01 {
		t.Errorf("rps = %.2f, want %.2f", s.RequestsPerSec, wantRPS)
	}
}

type errFake string

func (e errFake) Error() string { return string(e) }

func TestSummarizeBucketsErrorsByMessage(t *testing.T) {
	c := NewCollector()
	for i := 0; i < 7; i++ {
		c.Add(time.Now(), client.RequestResult{Err: errFake("dial tcp: connection refused")})
	}
	for i := 0; i < 3; i++ {
		c.Add(time.Now(), client.RequestResult{Err: errFake("status 400: model not found")})
	}
	c.Add(time.Now(), client.RequestResult{Err: errFake("status 401: unauthorized")})

	s := Summarize(c.Records(), time.Second)
	if s.SuccessRequests != 0 || s.FailedRequests != 11 {
		t.Fatalf("counts: ok=%d failed=%d", s.SuccessRequests, s.FailedRequests)
	}
	if len(s.ErrorBuckets) != 3 {
		t.Fatalf("got %d buckets, want 3: %+v", len(s.ErrorBuckets), s.ErrorBuckets)
	}
	// Buckets must be sorted by descending count.
	if s.ErrorBuckets[0].Count != 7 || s.ErrorBuckets[1].Count != 3 || s.ErrorBuckets[2].Count != 1 {
		t.Errorf("bucket order wrong: %+v", s.ErrorBuckets)
	}
	if s.ErrorBuckets[0].Message != "dial tcp: connection refused" {
		t.Errorf("top bucket = %q, want connection refused", s.ErrorBuckets[0].Message)
	}
}

func TestSummarizeIncludesTTFTSamplesForSmallRuns(t *testing.T) {
	c := NewCollector()
	// Three successful requests with TTFTs 30ms, 10ms, 20ms.
	ttfts := []time.Duration{30 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}
	for _, d := range ttfts {
		c.Add(time.Now(), client.RequestResult{TTFT: d, E2E: 100 * time.Millisecond, CompletionTokens: 1})
	}
	s := Summarize(c.Records(), time.Second)
	want := []int{10, 20, 30}
	if len(s.TTFTSamplesMS) != len(want) {
		t.Fatalf("got %d samples, want %d: %v", len(s.TTFTSamplesMS), len(want), s.TTFTSamplesMS)
	}
	for i := range want {
		if s.TTFTSamplesMS[i] != want[i] {
			t.Errorf("sample %d = %d, want %d", i, s.TTFTSamplesMS[i], want[i])
		}
	}
}

func TestSummarizeOmitsTTFTSamplesForLargeRuns(t *testing.T) {
	c := NewCollector()
	for i := 0; i < smallRunThreshold; i++ {
		c.Add(time.Now(), client.RequestResult{TTFT: time.Duration(i+1) * time.Millisecond, E2E: 100 * time.Millisecond, CompletionTokens: 1})
	}
	s := Summarize(c.Records(), time.Second)
	if s.TTFTSamplesMS != nil {
		t.Errorf("TTFTSamplesMS should be nil at the threshold (%d), got %d samples",
			smallRunThreshold, len(s.TTFTSamplesMS))
	}
}

func TestSummarizeTTFTSamplesExcludesFailures(t *testing.T) {
	c := NewCollector()
	c.Add(time.Now(), client.RequestResult{TTFT: 5 * time.Millisecond, E2E: 50 * time.Millisecond, CompletionTokens: 1})
	c.Add(time.Now(), client.RequestResult{Err: errFake("connection refused")})
	c.Add(time.Now(), client.RequestResult{TTFT: 15 * time.Millisecond, E2E: 50 * time.Millisecond, CompletionTokens: 1})

	s := Summarize(c.Records(), time.Second)
	want := []int{5, 15}
	if len(s.TTFTSamplesMS) != len(want) {
		t.Fatalf("got %v, want %v", s.TTFTSamplesMS, want)
	}
	for i := range want {
		if s.TTFTSamplesMS[i] != want[i] {
			t.Errorf("sample %d = %d, want %d", i, s.TTFTSamplesMS[i], want[i])
		}
	}
}

func TestSummarizeNoErrorBucketsWhenAllSucceed(t *testing.T) {
	c := NewCollector()
	c.Add(time.Now(), client.RequestResult{TTFT: time.Millisecond, E2E: 10 * time.Millisecond, CompletionTokens: 1})
	s := Summarize(c.Records(), time.Second)
	if s.ErrorBuckets != nil {
		t.Errorf("ErrorBuckets should be nil when no errors, got %+v", s.ErrorBuckets)
	}
}
