package metrics

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
)

// PairCollector holds per-request results from both sides of a pairwise run,
// keyed by canonical request index. A[i] is the result for canonical index i
// on endpoint A; B[i] likewise. Prefixes[i] is the workload's prefix-pool
// label for that request (synthetic's "p<N>"); RequestIDs[i] is the
// per-request ID sent as X-Request-ID to both endpoints; Prompts[i] is an
// optional truncated copy of the request's user message text (set when
// --dump-prompt is enabled). All three side-slabs are written once before
// workers start and read by SummarizePairwise. Per-index writes don't race;
// the mutex guards only initial sizing.
type PairCollector struct {
	mu         sync.Mutex
	A          []Record
	B          []Record
	Prefixes   []string
	RequestIDs []string
	Prompts    []string
}

func NewPairCollector(n int) *PairCollector {
	return &PairCollector{
		A:          make([]Record, n),
		B:          make([]Record, n),
		Prefixes:   make([]string, n),
		RequestIDs: make([]string, n),
		Prompts:    make([]string, n),
	}
}

// SetPrefix stores the workload's prefix label for a canonical index. Safe
// to call only from a single goroutine before workers start (RunPair does
// this in its materialization loop).
func (pc *PairCollector) SetPrefix(idx int, prefix string) {
	if idx >= 0 && idx < len(pc.Prefixes) {
		pc.Prefixes[idx] = prefix
	}
}

// SetRequestID stores the X-Request-ID associated with a canonical index.
// Same single-goroutine-before-workers constraint as SetPrefix.
func (pc *PairCollector) SetRequestID(idx int, id string) {
	if idx >= 0 && idx < len(pc.RequestIDs) {
		pc.RequestIDs[idx] = id
	}
}

// SetPrompt stores a (typically truncated) snapshot of the request's user
// message text. Same single-goroutine-before-workers constraint as SetPrefix.
func (pc *PairCollector) SetPrompt(idx int, prompt string) {
	if idx >= 0 && idx < len(pc.Prompts) {
		pc.Prompts[idx] = prompt
	}
}

func (pc *PairCollector) AddResult(side rune, idx int, start time.Time, r client.RequestResult) {
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
	pc.mu.Lock()
	defer pc.mu.Unlock()
	switch side {
	case 'A', 'a':
		pc.A[idx] = rec
	case 'B', 'b':
		pc.B[idx] = rec
	}
}

// PairRow is one entry in the per-request table emitted for small runs.
// DeltaMS is B_TTFT_ms - A_TTFT_ms; since lower latency is better,
// **negative** means B was faster (an improvement). When either side
// failed, the corresponding error string is set and DeltaMS is 0. Prefix is
// the workload's prefix-pool label for this request (synthetic uses "p<N>"
// where N is the prefix index); empty for workloads that don't set one.
// RequestID is the X-Request-ID sent for this request (e.g. "bench-42-7");
// empty when the runner wasn't configured with a prefix. Prompt is the
// leading bytes of the user message (set when --dump-prompt is on; newlines
// collapsed to spaces).
type PairRow struct {
	Index     int    `json:"index"`
	RequestID string `json:"request_id,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
	ATTFTMS   int    `json:"a_ttft_ms"`
	BTTFTMS   int    `json:"b_ttft_ms"`
	DeltaMS   int    `json:"delta_ms"`
	AErr      string `json:"a_err,omitempty"`
	BErr      string `json:"b_err,omitempty"`
	Prompt    string `json:"prompt,omitempty"`
}

// HistBin is one bucket of the improvement-distribution histogram. LoMS and
// HiMS are the inclusive lower / exclusive upper edges of the bucket in
// integer milliseconds; bins at the ends use math.MinInt/MaxInt to denote
// open intervals. Label is a human-friendly description.
type HistBin struct {
	Label string `json:"label"`
	LoMS  int    `json:"lo_ms"`
	HiMS  int    `json:"hi_ms"`
	Count int    `json:"count"`
}

// SpeedupBin is one bucket of the speedup-ratio histogram. Lo/Hi are
// inclusive-lower/exclusive-upper ratio edges (with math.Inf used for the
// open top bucket).
type SpeedupBin struct {
	Label string  `json:"label"`
	Lo    float64 `json:"lo"`
	Hi    float64 `json:"hi"`
	Count int     `json:"count"`
}

// ImprovementStats summarizes the per-request delta distribution (in the
// B-is-experiment / A-is-baseline framing) across pairs where both sides
// succeeded. Sign convention follows latency: delta = B - A, where
// **negative means B was faster** (an improvement over baseline) and
// positive means B was slower (a regression). Histogram and percentile
// fields below all use this convention.
//
// Speedup fields use the ratio A_TTFT / B_TTFT (>1 = B faster, an
// improvement, matching the standard "speedup factor" convention from CS
// benchmarking literature). Tied band: 0.95x to 1.05x. Tied pairs are
// included in GeomeanSpeedup but excluded from the win/loss geomeans.
// GeomeanSlowdownWhenBSlower is reported as a slowdown *multiplier* (>1)
// so it reads symmetrically with the win geomean.
type ImprovementStats struct {
	N int `json:"n"`
	// TTFT holds percentiles of the SIGNED per-pair delta (B - A) in ms.
	// Because deltas are signed, upper percentiles (p90/p95/p99) describe
	// the regression tail only; for a balanced view use WhenBFasterTTFT
	// and WhenBSlowerTTFT below.
	TTFT Percentiles `json:"ttft_ms"`
	// NWins / NLosses are the count of pairs where |delta| > TiedToleranceMS
	// and the sign indicated B faster / slower respectively.
	NWins   int `json:"n_wins"`
	NLosses int `json:"n_losses"`
	// WhenBFasterTTFT is the percentile distribution of win magnitudes —
	// for the NWins pairs where B was faster, the absolute size of B's
	// improvement (positive ms). Read p50 as "the median win was Xms".
	WhenBFasterTTFT Percentiles `json:"when_b_faster_ttft_ms"`
	// WhenBSlowerTTFT is the percentile distribution of loss magnitudes —
	// for the NLosses pairs where B was slower, the absolute size of the
	// regression (positive ms). Read p50 as "the median loss was Xms".
	WhenBSlowerTTFT Percentiles `json:"when_b_slower_ttft_ms"`
	// TotalSavedSecondsTTFT is the sum of win magnitudes (B faster) in
	// seconds — i.e. the wall-clock time B shaved off across all winning
	// pairs, gross.
	TotalSavedSecondsTTFT float64 `json:"total_saved_seconds_ttft"`
	// TotalLostSecondsTTFT is the sum of loss magnitudes (B slower) in
	// seconds — the wall-clock time B added across all losing pairs, gross.
	TotalLostSecondsTTFT float64 `json:"total_lost_seconds_ttft"`
	// NetSavedSecondsTTFT = TotalSavedSecondsTTFT - TotalLostSecondsTTFT.
	// Positive = B is net-faster across the run; negative = net-slower.
	NetSavedSecondsTTFT float64 `json:"net_saved_seconds_ttft"`
	// AvgSavedPerRequestMS amortizes TotalSavedSecondsTTFT over all N
	// paired requests (not just wins). Useful for cross-run comparison.
	AvgSavedPerRequestMS float64 `json:"avg_saved_per_request_ms"`
	// AvgLostPerRequestMS amortizes TotalLostSecondsTTFT over all N pairs.
	AvgLostPerRequestMS float64 `json:"avg_lost_per_request_ms"`
	// AvgNetPerRequestMS = AvgSavedPerRequestMS - AvgLostPerRequestMS.
	AvgNetPerRequestMS         float64      `json:"avg_net_per_request_ms"`
	PctBFasterTTFT             float64      `json:"pct_b_faster_ttft"`
	PctTied                    float64      `json:"pct_tied"`
	PctBSlowerTTFT             float64      `json:"pct_b_slower_ttft"`
	Histogram                  []HistBin    `json:"histogram"`
	TiedToleranceMS            int          `json:"tied_tolerance_ms"`
	SpeedupHistogram           []SpeedupBin `json:"speedup_histogram,omitempty"`
	GeomeanSpeedup             float64      `json:"geomean_speedup,omitempty"`
	GeomeanSpeedupWhenBFaster  float64      `json:"geomean_speedup_when_b_faster,omitempty"`
	GeomeanSlowdownWhenBSlower float64      `json:"geomean_slowdown_when_b_slower,omitempty"`
}

type PairwiseReport struct {
	Workload    string           `json:"workload"`
	Model       string           `json:"model"`
	Concurrency int              `json:"concurrency"`
	LabelA      string           `json:"label_a"`
	LabelB      string           `json:"label_b"`
	EndpointA   Summary          `json:"endpoint_a"`
	EndpointB   Summary          `json:"endpoint_b"`
	Improvement ImprovementStats `json:"improvement"`
	// PairTable is populated only when total pairs < smallRunThreshold; one
	// row per canonical index, in send order.
	PairTable []PairRow `json:"pair_table,omitempty"`
}

// histogramBackboneMS is the log-spaced backbone of bucket boundaries for the
// delta distribution. Plus/minus the tied tolerance is inserted as additional
// inner edges at runtime so the tied bin matches the configured threshold.
var histogramBackboneMS = []int{-10000, -3000, -1000, -300, -100, 100, 300, 1000, 3000, 10000}

// DefaultTiedToleranceMS is the noise band used when callers don't override
// it. Chosen to be larger than typical measurement jitter (network +
// scheduling, ~10-30ms) but small enough not to mask modest cache-hit wins on
// short prompts. Override via --tied-ms.
const DefaultTiedToleranceMS = 25

// SummarizePairwise builds a full report from a finished pair collector. wallA
// and wallB are each side's runner wall time; we keep them separate so the
// per-endpoint Summary correctly reflects that side's throughput.
//
// Sign convention throughout: delta = B_TTFT - A_TTFT. Negative means B was
// faster (an improvement over the A baseline); positive means B was slower
// (a regression). tiedTolMS classifies any |delta| ≤ tiedTolMS as "tied"
// (noise) in the win/tie/loss counter and as the explicit tied bin in the
// histogram. Pass 0 to use DefaultTiedToleranceMS.
func SummarizePairwise(pc *PairCollector, wallA, wallB time.Duration, labelA, labelB string, tiedTolMS int) PairwiseReport {
	if tiedTolMS <= 0 {
		tiedTolMS = DefaultTiedToleranceMS
	}
	pc.mu.Lock()
	a := append([]Record(nil), pc.A...)
	b := append([]Record(nil), pc.B...)
	pc.mu.Unlock()

	sumA := Summarize(a, wallA)
	sumB := Summarize(b, wallB)

	// Paired deltas across indices where both sides succeeded.
	// delta = B - A; negative means B was faster (an improvement).
	var deltas []time.Duration
	var winMagnitudes []time.Duration  // |delta| for B-faster pairs
	var lossMagnitudes []time.Duration // |delta| for B-slower pairs
	var speedups []float64             // A_TTFT / B_TTFT
	bFaster, tied, bSlower := 0, 0, 0
	tiedTol := time.Duration(tiedTolMS) * time.Millisecond
	for i := range a {
		if a[i].Err != "" || b[i].Err != "" {
			continue
		}
		delta := b[i].TTFT - a[i].TTFT
		deltas = append(deltas, delta)
		switch {
		case delta < -tiedTol:
			bFaster++
			winMagnitudes = append(winMagnitudes, -delta)
		case delta > tiedTol:
			bSlower++
			lossMagnitudes = append(lossMagnitudes, delta)
		default:
			tied++
		}
		if b[i].TTFT > 0 && a[i].TTFT > 0 {
			speedups = append(speedups, float64(a[i].TTFT)/float64(b[i].TTFT))
		}
	}
	var totalSaved, totalLost time.Duration
	for _, d := range winMagnitudes {
		totalSaved += d
	}
	for _, d := range lossMagnitudes {
		totalLost += d
	}
	imp := ImprovementStats{
		N:                     len(deltas),
		TTFT:                  percentiles(deltas),
		NWins:                 bFaster,
		NLosses:               bSlower,
		WhenBFasterTTFT:       percentiles(winMagnitudes),
		WhenBSlowerTTFT:       percentiles(lossMagnitudes),
		TotalSavedSecondsTTFT: totalSaved.Seconds(),
		TotalLostSecondsTTFT:  totalLost.Seconds(),
		NetSavedSecondsTTFT:   (totalSaved - totalLost).Seconds(),
		Histogram:             buildImprovementHistogram(deltas, tiedTolMS),
		TiedToleranceMS:       tiedTolMS,
	}
	if len(deltas) > 0 {
		n := float64(len(deltas))
		imp.AvgSavedPerRequestMS = totalSaved.Seconds() * 1000.0 / n
		imp.AvgLostPerRequestMS = totalLost.Seconds() * 1000.0 / n
		imp.AvgNetPerRequestMS = imp.AvgSavedPerRequestMS - imp.AvgLostPerRequestMS
	}
	if len(deltas) > 0 {
		imp.PctBFasterTTFT = float64(bFaster) * 100.0 / float64(len(deltas))
		imp.PctTied = float64(tied) * 100.0 / float64(len(deltas))
		imp.PctBSlowerTTFT = float64(bSlower) * 100.0 / float64(len(deltas))
	}
	if len(speedups) > 0 {
		imp.SpeedupHistogram = buildSpeedupHistogram(speedups)
		imp.GeomeanSpeedup = geomean(speedups)
		var wins, losses []float64
		for _, s := range speedups {
			switch {
			case s > 1.05:
				wins = append(wins, s)
			case s < 0.95:
				losses = append(losses, 1.0/s) // invert: express slowdown as multiplier > 1
			}
		}
		imp.GeomeanSpeedupWhenBFaster = geomean(wins)
		imp.GeomeanSlowdownWhenBSlower = geomean(losses)
	}

	report := PairwiseReport{
		LabelA:      labelA,
		LabelB:      labelB,
		EndpointA:   sumA,
		EndpointB:   sumB,
		Improvement: imp,
	}

	if len(a) < smallRunThreshold {
		report.PairTable = make([]PairRow, len(a))
		for i := range a {
			row := PairRow{Index: i}
			if i < len(pc.Prefixes) {
				row.Prefix = pc.Prefixes[i]
			}
			if i < len(pc.RequestIDs) {
				row.RequestID = pc.RequestIDs[i]
			}
			if i < len(pc.Prompts) {
				row.Prompt = pc.Prompts[i]
			}
			if a[i].Err == "" {
				row.ATTFTMS = msInt(a[i].TTFT)
			} else {
				row.AErr = a[i].Err
			}
			if b[i].Err == "" {
				row.BTTFTMS = msInt(b[i].TTFT)
			} else {
				row.BErr = b[i].Err
			}
			if a[i].Err == "" && b[i].Err == "" {
				row.DeltaMS = row.BTTFTMS - row.ATTFTMS
			}
			report.PairTable[i] = row
		}
	}
	return report
}

// buildImprovementHistogram returns a slice of HistBins with log-spaced edges
// plus ±tiedTolMS inserted as inner boundaries. Each bin is half-open
// [Lo, Hi); the first bin is (-∞, edges[0]) and the last is [edges[-1], +∞).
// Display order: most-improvement (most negative delta) at top, most-
// regression (most positive delta) at bottom — so a quick glance shows the
// "good" side of the distribution first.
func buildImprovementHistogram(deltas []time.Duration, tiedTolMS int) []HistBin {
	const (
		negInf = -1 << 62
		posInf = 1 << 62
	)

	edges := histogramEdges(tiedTolMS)

	bins := make([]HistBin, 0, len(edges)+1)
	// Below the smallest edge: largest improvement.
	bins = append(bins, HistBin{
		Label: fmt.Sprintf("<%d", edges[0]),
		LoMS:  negInf, HiMS: edges[0],
	})
	for i := 0; i < len(edges)-1; i++ {
		lo, hi := edges[i], edges[i+1]
		var label string
		switch {
		case tiedTolMS > 0 && lo == -tiedTolMS && hi == tiedTolMS:
			label = fmt.Sprintf("tied (±%d)", tiedTolMS)
		case lo >= 0:
			label = fmt.Sprintf("+%d to +%d", lo, hi)
		case hi <= 0:
			label = fmt.Sprintf("%d to %d", lo, hi)
		default:
			// Straddles zero but isn't the tied bin (happens when tiedTolMS
			// equals an existing backbone edge so no extra inner edges were
			// added). Label by raw range.
			label = fmt.Sprintf("%d to +%d", lo, hi)
		}
		bins = append(bins, HistBin{Label: label, LoMS: lo, HiMS: hi})
	}
	// Above the largest edge: largest regression.
	bins = append(bins, HistBin{
		Label: fmt.Sprintf(">+%d", edges[len(edges)-1]),
		LoMS:  edges[len(edges)-1], HiMS: posInf,
	})

	for _, d := range deltas {
		ms := msInt(d)
		for i := range bins {
			if ms >= bins[i].LoMS && ms < bins[i].HiMS {
				bins[i].Count++
				break
			}
		}
	}
	return bins
}

// histogramEdges builds the sorted edge list by taking the fixed log-spaced
// backbone and inserting ±tiedTolMS when those values aren't already on the
// backbone. Defaults to the backbone untouched when tiedTolMS is 0.
func histogramEdges(tiedTolMS int) []int {
	edges := append([]int{}, histogramBackboneMS...)
	if tiedTolMS > 0 && tiedTolMS < edges[len(edges)-1] && !containsInt(edges, tiedTolMS) {
		edges = append(edges, tiedTolMS, -tiedTolMS)
		sort.Ints(edges)
	}
	return edges
}

func containsInt(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func msInt(d time.Duration) int {
	return int(float64(d)/float64(time.Millisecond) + 0.5)
}

// buildSpeedupHistogram returns SpeedupBins in display order, top to bottom,
// largest speedup first. Edges are symmetric on a log scale around 1.0 (the
// no-change line); the tied band is 0.95x – 1.05x.
func buildSpeedupHistogram(speedups []float64) []SpeedupBin {
	type edge struct {
		lo, hi float64
		label  string
	}
	edges := []edge{
		{10, math.Inf(1), ">10x"},
		{3, 10, "3x – 10x"},
		{1.5, 3, "1.5x – 3x"},
		{1.05, 1.5, "1.05x – 1.5x"},
		{0.95, 1.05, "tied (0.95x – 1.05x)"},
		{0.67, 0.95, "0.67x – 0.95x"},
		{0.33, 0.67, "0.33x – 0.67x"},
		{0.1, 0.33, "0.1x – 0.33x"},
		{0, 0.1, "<0.1x"},
	}
	bins := make([]SpeedupBin, len(edges))
	for i, e := range edges {
		bins[i] = SpeedupBin{Label: e.label, Lo: e.lo, Hi: e.hi}
	}
	for _, s := range speedups {
		for i := range bins {
			if s >= bins[i].Lo && (math.IsInf(bins[i].Hi, 1) || s < bins[i].Hi) {
				bins[i].Count++
				break
			}
		}
	}
	return bins
}

// geomean returns the geometric mean of xs (computed in log space for
// numerical stability). Returns 0 for an empty slice; non-positive values
// are skipped (they have no meaningful log).
func geomean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sumLog float64
	var n int
	for _, x := range xs {
		if x <= 0 {
			continue
		}
		sumLog += math.Log(x)
		n++
	}
	if n == 0 {
		return 0
	}
	return math.Exp(sumLog / float64(n))
}

// isTiedSpeedupBin reports whether a SpeedupBin straddles 1.0 — the
// no-change line. Used by the renderer to mark the tied row.
func isTiedSpeedupBin(b SpeedupBin) bool {
	return b.Lo < 1.0 && b.Hi > 1.0
}
