package runner

import (
	"context"
	jsonpkg "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/metrics"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/workload"
)

// stubServer streams a small fixed SSE response after the configured TTFT
// delay, then a usage chunk and [DONE]. Each handler call increments hits.
func stubServer(t *testing.T, ttft time.Duration, hits *int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(hits, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		time.Sleep(ttft)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		flusher.Flush()
		fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n")
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
}

// staticWorkload yields N identical requests.
type staticWorkload struct {
	n   int
	i   int
	req *workload.Request
}

func (s *staticWorkload) Name() string { return "static" }
func (s *staticWorkload) Total() int   { return s.n }
func (s *staticWorkload) Next(_ context.Context) (*workload.Request, bool) {
	if s.i >= s.n {
		return nil, false
	}
	s.i++
	return s.req, true
}

func TestRunPairDrivesBothEndpointsIndependently(t *testing.T) {
	// Stub TTFTs spread far enough apart that the gap exceeds the
	// ±tiedToleranceMS noise band, so this counts as a real B-slower result.
	var hitsA, hitsB int64
	srvA := stubServer(t, 20*time.Millisecond, &hitsA)
	defer srvA.Close()
	srvB := stubServer(t, 250*time.Millisecond, &hitsB)
	defer srvB.Close()

	clA := client.New(client.Config{BaseURL: srvA.URL, Model: "stub"})
	clB := client.New(client.Config{BaseURL: srvB.URL, Model: "stub"})

	const n = 10
	wl := &staticWorkload{n: n, req: &workload.Request{
		Messages:  []workload.Message{{Role: "user", Content: "hello"}},
		MaxTokens: 16,
	}}

	pc := metrics.NewPairCollector(n)
	res := RunPair(context.Background(), PairConfig{Concurrency: 2}, clA, clB, wl, pc)

	if res.Total != n {
		t.Errorf("Total = %d, want %d", res.Total, n)
	}
	if int(atomic.LoadInt64(&hitsA)) != n || int(atomic.LoadInt64(&hitsB)) != n {
		t.Errorf("each endpoint should see exactly %d requests; got A=%d B=%d",
			n, atomic.LoadInt64(&hitsA), atomic.LoadInt64(&hitsB))
	}

	// The slower side (B) bounds wall time; faster side (A) should finish first.
	if res.WallA >= res.WallB {
		t.Errorf("expected A faster than B; wallA=%v wallB=%v", res.WallA, res.WallB)
	}

	report := metrics.SummarizePairwise(pc, res.WallA, res.WallB, "A", "B", 0)
	if report.Improvement.N != n {
		t.Fatalf("Improvement.N = %d, want %d", report.Improvement.N, n)
	}
	// A's stub has 20ms TTFT (fast) and B's has 250ms (slow); delta = B - A ≈ +230ms.
	// Sign convention: positive = B slower, since lower latency is better.
	if report.Improvement.TTFT.P50 < 200 || report.Improvement.TTFT.P50 > 280 {
		t.Errorf("p50 delta = %v ms, want around +230 (B slower than A)", report.Improvement.TTFT.P50)
	}
	if report.Improvement.PctBFasterTTFT != 0 {
		t.Errorf("PctBFasterTTFT = %v, want 0 (B is consistently slower)", report.Improvement.PctBFasterTTFT)
	}
	if report.Improvement.PctBSlowerTTFT != 100 {
		t.Errorf("PctBSlowerTTFT = %v, want 100", report.Improvement.PctBSlowerTTFT)
	}
}

func TestRunPairSendsMatchingRequestIDsToBothEndpoints(t *testing.T) {
	// Record the X-Request-ID seen at each endpoint, indexed by request body
	// (which is identical across A/B for a canonical index).
	var muA, muB sync.Mutex
	idsA, idsB := map[string]string{}, map[string]string{}

	hijackStub := func(store map[string]string, mu *sync.Mutex) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			store[string(body)] = r.Header.Get("X-Request-ID")
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			f := w.(http.Flusher)
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
			f.Flush()
			fmt.Fprint(w, "data: [DONE]\n\n")
			f.Flush()
		}))
	}
	srvA := hijackStub(idsA, &muA)
	defer srvA.Close()
	srvB := hijackStub(idsB, &muB)
	defer srvB.Close()

	clA := client.New(client.Config{BaseURL: srvA.URL, Model: "stub"})
	clB := client.New(client.Config{BaseURL: srvB.URL, Model: "stub"})

	const n = 5
	// Each request needs unique content so the body→id map keys are distinct.
	wl := &indexedWorkload{n: n}
	pc := metrics.NewPairCollector(n)
	RunPair(context.Background(), PairConfig{
		Concurrency:     2,
		RequestIDPrefix: "bench-42",
	}, clA, clB, wl, pc)

	if len(idsA) != n || len(idsB) != n {
		t.Fatalf("expected %d unique bodies on each side; got A=%d B=%d", n, len(idsA), len(idsB))
	}
	// Both stubs must have observed the same id for the same body.
	for body, idA := range idsA {
		idB, ok := idsB[body]
		if !ok {
			t.Errorf("body present in A but not B: %q", body)
			continue
		}
		if idA != idB {
			t.Errorf("body %q: idA=%q idB=%q; should match", body, idA, idB)
		}
		if !strings.HasPrefix(idA, "bench-42-") {
			t.Errorf("id %q does not have expected prefix bench-42-", idA)
		}
	}
}

// TestRunPairAppendsPrefixLabelToRequestID checks that when a workload sets
// req.Prefix (e.g. multiturn's "c<j>t<k>"), the runner appends it to the
// generated X-Request-ID so server logs preserve the conversation/turn
// identity in the request ID itself.
func TestRunPairAppendsPrefixLabelToRequestID(t *testing.T) {
	var mu sync.Mutex
	idsByContent := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		idsByContent[string(body)] = r.Header.Get("X-Request-ID")
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		f.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	defer srv.Close()
	cl := client.New(client.Config{BaseURL: srv.URL, Model: "stub"})

	// Custom workload that sets Prefix for each request.
	wl := &labeledWorkload{
		items: []labeledItem{
			{content: "msg0", prefix: "c0t0"},
			{content: "msg1", prefix: "c1t0"},
			{content: "msg2", prefix: "c0t1"},
		},
	}
	pc := metrics.NewPairCollector(wl.Total())
	RunPair(context.Background(), PairConfig{
		Concurrency:     2,
		RequestIDPrefix: "bench-99",
	}, cl, cl, wl, pc)

	mu.Lock()
	defer mu.Unlock()
	// Every observed X-Request-ID must match the pattern bench-99-<idx>-<prefix>.
	expected := map[string]bool{
		"bench-99-0-c0t0": false,
		"bench-99-1-c1t0": false,
		"bench-99-2-c0t1": false,
	}
	for content, id := range idsByContent {
		// Skip the test stub's own potential probe writes (none here).
		_ = content
		if _, ok := expected[id]; !ok {
			t.Errorf("unexpected X-Request-ID %q", id)
			continue
		}
		expected[id] = true
	}
	for id, seen := range expected {
		if !seen {
			t.Errorf("expected X-Request-ID %q was not observed", id)
		}
	}
}

// labeledWorkload yields a fixed list of (content, prefix) pairs.
type labeledWorkload struct {
	mu     sync.Mutex
	cursor int
	items  []labeledItem
}
type labeledItem struct{ content, prefix string }

func (w *labeledWorkload) Name() string { return "labeled" }
func (w *labeledWorkload) Total() int   { return len(w.items) }
func (w *labeledWorkload) Next(_ context.Context) (*workload.Request, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cursor >= len(w.items) {
		return nil, false
	}
	it := w.items[w.cursor]
	w.cursor++
	return &workload.Request{
		Messages:  []workload.Message{{Role: "user", Content: it.content}},
		MaxTokens: 4,
		Prefix:    it.prefix,
	}, true
}

// indexedWorkload emits N requests with unique content per index so tests
// that need to identify which request is which (without relying on send
// order) can use the body as a key.
type indexedWorkload struct {
	n, i int
}

func (w *indexedWorkload) Name() string { return "indexed" }
func (w *indexedWorkload) Total() int   { return w.n }
func (w *indexedWorkload) Next(_ context.Context) (*workload.Request, bool) {
	if w.i >= w.n {
		return nil, false
	}
	idx := w.i
	w.i++
	return &workload.Request{
		Messages:  []workload.Message{{Role: "user", Content: fmt.Sprintf("req-%d", idx)}},
		MaxTokens: 4,
	}, true
}

// trackingStub is an httptest server that records, for each request, the
// time at which it received the request and finished sending the response,
// keyed by the user-message content (which the test sets to "c<j>t<k>").
type trackingStub struct {
	mu       sync.Mutex
	arrivals map[string]time.Time
	finishes map[string]time.Time
}

func newTrackingStub(t *testing.T, ttft time.Duration) (*httptest.Server, *trackingStub) {
	ts := &trackingStub{
		arrivals: map[string]time.Time{},
		finishes: map[string]time.Time{},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// Pull the user message content out of the JSON body. The test
		// constructs each request with exactly one user message whose
		// content is the c<j>t<k> tag.
		var parsed struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		_ = jsonUnmarshal(body, &parsed)
		key := ""
		if len(parsed.Messages) > 0 {
			key = parsed.Messages[len(parsed.Messages)-1].Content
		}
		ts.mu.Lock()
		ts.arrivals[key] = time.Now()
		ts.mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		time.Sleep(ttft)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		f.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		f.Flush()

		ts.mu.Lock()
		ts.finishes[key] = time.Now()
		ts.mu.Unlock()
	}))
	return srv, ts
}

// dependentWorkload emits 2 conversations × 3 turns each (interleaved), with
// turn k+1 declaring a PredecessorIndex of turn k of the same conversation.
type dependentWorkload struct {
	mu     sync.Mutex
	cursor int
	items  []dependentItem
}
type dependentItem struct {
	conv, turn int
	canonIdx   int
	predIdx    *int
}

func newDependentWorkload() *dependentWorkload {
	type ref struct{ c, t int }
	// Schedule interleaves the two convs so a naive concurrent runner
	// would race turn k+1 against turn k.
	sched := []ref{{0, 0}, {1, 0}, {0, 1}, {1, 1}, {0, 2}, {1, 2}}
	items := make([]dependentItem, 0, len(sched))
	lastIdxFor := map[ref]int{}
	for i, r := range sched {
		it := dependentItem{conv: r.c, turn: r.t, canonIdx: i}
		if r.t > 0 {
			prev := lastIdxFor[ref{r.c, r.t - 1}]
			it.predIdx = &prev
		}
		lastIdxFor[r] = i
		items = append(items, it)
	}
	return &dependentWorkload{items: items}
}

func (d *dependentWorkload) Name() string { return "dep" }
func (d *dependentWorkload) Total() int   { return len(d.items) }
func (d *dependentWorkload) Next(_ context.Context) (*workload.Request, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cursor >= len(d.items) {
		return nil, false
	}
	it := d.items[d.cursor]
	d.cursor++
	canon := it.canonIdx
	return &workload.Request{
		Messages: []workload.Message{
			{Role: "user", Content: fmt.Sprintf("c%dt%d", it.conv, it.turn)},
		},
		MaxTokens:        4,
		CanonicalIndex:   &canon,
		PredecessorIndex: it.predIdx,
	}, true
}

func TestRunPairRespectsPredecessorOrdering(t *testing.T) {
	// Stub takes ~50ms to respond. With concurrency=6 (== total request
	// count), a runner without dep gating would send all 6 requests
	// near-simultaneously. With gating, turn 1 of conv j must arrive at the
	// server only AFTER turn 0 of conv j has *finished*.
	srvA, tsA := newTrackingStub(t, 50*time.Millisecond)
	defer srvA.Close()
	srvB, tsB := newTrackingStub(t, 50*time.Millisecond)
	defer srvB.Close()

	clA := client.New(client.Config{BaseURL: srvA.URL, Model: "stub"})
	clB := client.New(client.Config{BaseURL: srvB.URL, Model: "stub"})

	wl := newDependentWorkload()
	pc := metrics.NewPairCollector(wl.Total())
	RunPair(context.Background(), PairConfig{Concurrency: 6}, clA, clB, wl, pc)

	check := func(side string, ts *trackingStub) {
		t.Helper()
		ts.mu.Lock()
		defer ts.mu.Unlock()
		for _, conv := range []int{0, 1} {
			for k := 0; k+1 < 3; k++ {
				prev := fmt.Sprintf("c%dt%d", conv, k)
				next := fmt.Sprintf("c%dt%d", conv, k+1)
				prevFinish, ok1 := ts.finishes[prev]
				nextArrive, ok2 := ts.arrivals[next]
				if !ok1 || !ok2 {
					t.Errorf("%s: missing telemetry for %s or %s (arrivals=%v finishes=%v)",
						side, prev, next, ts.arrivals, ts.finishes)
					continue
				}
				if !nextArrive.After(prevFinish) {
					t.Errorf("%s: %s arrived at server at %v but %s only finished at %v — "+
						"per-conv ordering violated (gating did not block)",
						side, next, nextArrive, prev, prevFinish)
				}
			}
		}
	}
	check("A", tsA)
	check("B", tsB)
}

// jsonUnmarshal is a thin wrapper so we don't import "encoding/json" at the
// top of the file (it's already there transitively but referencing locally
// keeps the new code visible).
func jsonUnmarshal(b []byte, v any) error {
	return jsonpkg.Unmarshal(b, v)
}

func TestRunPairAssignsIndicesContiguously(t *testing.T) {
	// Both stubs return fast; we only verify that every canonical index
	// gets a result on both sides.
	var hits int64
	srvA := stubServer(t, time.Millisecond, &hits)
	defer srvA.Close()
	srvB := stubServer(t, time.Millisecond, &hits)
	defer srvB.Close()

	clA := client.New(client.Config{BaseURL: srvA.URL, Model: "stub"})
	clB := client.New(client.Config{BaseURL: srvB.URL, Model: "stub"})

	const n = 50
	wl := &staticWorkload{n: n, req: &workload.Request{
		Messages:  []workload.Message{{Role: "user", Content: "x"}},
		MaxTokens: 4,
	}}
	pc := metrics.NewPairCollector(n)
	RunPair(context.Background(), PairConfig{Concurrency: 8}, clA, clB, wl, pc)

	report := metrics.SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	if report.EndpointA.TotalRequests != n || report.EndpointB.TotalRequests != n {
		t.Errorf("expected %d records on each side; got A=%d B=%d",
			n, report.EndpointA.TotalRequests, report.EndpointB.TotalRequests)
	}
	if report.EndpointA.SuccessRequests != n || report.EndpointB.SuccessRequests != n {
		t.Errorf("expected all successes; got A=%d/%d B=%d/%d",
			report.EndpointA.SuccessRequests, report.EndpointA.TotalRequests,
			report.EndpointB.SuccessRequests, report.EndpointB.TotalRequests)
	}
}
