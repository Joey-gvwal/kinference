package metrics

import (
	"testing"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
)

// fillPair populates pc with results from two parallel slices. ATTFTs[i] and
// BTTFTs[i] go into A[i] and B[i] respectively. Errors can be injected via
// the parallel error slices (empty string = success).
func fillPair(pc *PairCollector, ATTFTs, BTTFTs []time.Duration, AErrs, BErrs []string) {
	for i := range ATTFTs {
		var aRes, bRes client.RequestResult
		if AErrs[i] != "" {
			aRes.Err = stringErr(AErrs[i])
		} else {
			aRes.TTFT = ATTFTs[i]
			aRes.E2E = ATTFTs[i] + 50*time.Millisecond
			aRes.CompletionTokens = 16
		}
		if BErrs[i] != "" {
			bRes.Err = stringErr(BErrs[i])
		} else {
			bRes.TTFT = BTTFTs[i]
			bRes.E2E = BTTFTs[i] + 50*time.Millisecond
			bRes.CompletionTokens = 16
		}
		pc.AddResult('A', i, time.Now(), aRes)
		pc.AddResult('B', i, time.Now(), bRes)
	}
}

type stringErr string

func (e stringErr) Error() string { return string(e) }

func TestSummarizePairwiseBasicImprovement(t *testing.T) {
	// A is 200ms slower than B on every request → B faster by 200ms every
	// time. Sign convention: delta = B - A = -200ms (negative = B faster,
	// since smaller-is-better for latency). Magnitude is well outside the
	// ±tiedToleranceMS band so this counts as a genuine improvement.
	const n = 5
	a := make([]time.Duration, n)
	b := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		a[i] = time.Duration(250+i) * time.Millisecond
		b[i] = time.Duration(50+i) * time.Millisecond
	}
	errs := make([]string, n)

	pc := NewPairCollector(n)
	fillPair(pc, a, b, errs, errs)

	r := SummarizePairwise(pc, time.Second, time.Second, "router", "baseline", 0)
	if r.Improvement.N != n {
		t.Fatalf("Improvement.N = %d, want %d", r.Improvement.N, n)
	}
	if got := r.Improvement.TTFT.P50; got > -199 || got < -201 {
		t.Errorf("p50 delta = %v ms, want ~-200", got)
	}
	if r.Improvement.PctBFasterTTFT != 100 {
		t.Errorf("PctBFasterTTFT = %v, want 100", r.Improvement.PctBFasterTTFT)
	}
	if r.EndpointA.SuccessRequests != n || r.EndpointB.SuccessRequests != n {
		t.Errorf("per-side success counts: A=%d B=%d, want %d each",
			r.EndpointA.SuccessRequests, r.EndpointB.SuccessRequests, n)
	}
}

func TestSummarizePairwiseSkipsPartialFailures(t *testing.T) {
	pc := NewPairCollector(3)
	fillPair(pc,
		[]time.Duration{50 * time.Millisecond, 60 * time.Millisecond, 70 * time.Millisecond},
		[]time.Duration{80 * time.Millisecond, 0, 0},
		[]string{"", "boom-A", ""},
		[]string{"", "", "boom-B"},
	)

	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	if r.Improvement.N != 1 {
		t.Errorf("Improvement.N = %d, want 1", r.Improvement.N)
	}
	if r.EndpointA.FailedRequests != 1 {
		t.Errorf("EndpointA.FailedRequests = %d, want 1", r.EndpointA.FailedRequests)
	}
	if r.EndpointB.FailedRequests != 1 {
		t.Errorf("EndpointB.FailedRequests = %d, want 1", r.EndpointB.FailedRequests)
	}
}

func TestSummarizePairwisePairTableForSmallRuns(t *testing.T) {
	// Deltas chosen well outside the ±tiedToleranceMS band so they count as
	// real regressions / improvements rather than noise.
	pc := NewPairCollector(2)
	fillPair(pc,
		[]time.Duration{50 * time.Millisecond, 500 * time.Millisecond},
		[]time.Duration{250 * time.Millisecond, 50 * time.Millisecond},
		[]string{"", ""},
		[]string{"", ""},
	)

	r := SummarizePairwise(pc, time.Second, time.Second, "router", "baseline", 0)
	if len(r.PairTable) != 2 {
		t.Fatalf("PairTable len = %d, want 2", len(r.PairTable))
	}
	// delta = B - A; row 0 has A=50, B=250 → delta=+200 (B slower);
	// row 1 has A=500, B=50 → delta=-450 (B faster).
	if r.PairTable[0].DeltaMS != 200 {
		t.Errorf("row 0 delta = %d, want +200", r.PairTable[0].DeltaMS)
	}
	if r.PairTable[1].DeltaMS != -450 {
		t.Errorf("row 1 delta = %d, want -450", r.PairTable[1].DeltaMS)
	}
	// 1 of 2 pairs has negative delta (B faster) → 50%; the other is B slower.
	if r.Improvement.PctBFasterTTFT != 50 {
		t.Errorf("PctBFasterTTFT = %v, want 50", r.Improvement.PctBFasterTTFT)
	}
	if r.Improvement.PctBSlowerTTFT != 50 {
		t.Errorf("PctBSlowerTTFT = %v, want 50", r.Improvement.PctBSlowerTTFT)
	}
}

func TestSummarizePairwisePropagatesPrefixLabels(t *testing.T) {
	pc := NewPairCollector(3)
	pc.SetPrefix(0, "p0")
	pc.SetPrefix(1, "p2")
	pc.SetPrefix(2, "p1")
	fillPair(pc,
		[]time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond},
		[]time.Duration{15 * time.Millisecond, 22 * time.Millisecond, 35 * time.Millisecond},
		[]string{"", "", ""},
		[]string{"", "", ""},
	)
	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	if len(r.PairTable) != 3 {
		t.Fatalf("PairTable len=%d, want 3", len(r.PairTable))
	}
	want := []string{"p0", "p2", "p1"}
	for i, w := range want {
		if r.PairTable[i].Prefix != w {
			t.Errorf("row %d Prefix = %q, want %q", i, r.PairTable[i].Prefix, w)
		}
	}
}

func TestSummarizePairwisePropagatesRequestIDs(t *testing.T) {
	pc := NewPairCollector(3)
	pc.SetRequestID(0, "bench-42-0")
	pc.SetRequestID(1, "bench-42-1")
	pc.SetRequestID(2, "bench-42-2")
	fillPair(pc,
		[]time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond},
		[]time.Duration{15 * time.Millisecond, 22 * time.Millisecond, 35 * time.Millisecond},
		[]string{"", "", ""},
		[]string{"", "", ""},
	)
	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	for i, want := range []string{"bench-42-0", "bench-42-1", "bench-42-2"} {
		if r.PairTable[i].RequestID != want {
			t.Errorf("row %d RequestID = %q, want %q", i, r.PairTable[i].RequestID, want)
		}
	}
}

func TestSummarizePairwiseTableShowsErrors(t *testing.T) {
	pc := NewPairCollector(2)
	fillPair(pc,
		[]time.Duration{50 * time.Millisecond, 0},
		[]time.Duration{70 * time.Millisecond, 90 * time.Millisecond},
		[]string{"", "timeout"},
		[]string{"", ""},
	)
	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	if len(r.PairTable) != 2 {
		t.Fatalf("PairTable len = %d, want 2", len(r.PairTable))
	}
	if r.PairTable[1].AErr == "" {
		t.Errorf("PairTable[1].AErr should be set, got %q", r.PairTable[1].AErr)
	}
	if r.PairTable[1].DeltaMS != 0 {
		t.Errorf("PairTable[1].DeltaMS should be 0 when one side errors, got %d", r.PairTable[1].DeltaMS)
	}
}

func TestSummarizePairwiseSeparateWinLossPercentiles(t *testing.T) {
	// Construct 4 wins (B faster by 200, 500, 1000, 2000 ms) and
	// 2 losses (B slower by 300, 600 ms). All magnitudes well outside the
	// default ±25ms tied band.
	pc := NewPairCollector(6)
	fillPair(pc,
		[]time.Duration{
			500 * time.Millisecond,  // win 200 ms (B=300)
			1000 * time.Millisecond, // win 500 ms (B=500)
			2000 * time.Millisecond, // win 1000 ms (B=1000)
			5000 * time.Millisecond, // win 2000 ms (B=3000)
			100 * time.Millisecond,  // loss 300 ms (B=400)
			200 * time.Millisecond,  // loss 600 ms (B=800)
		},
		[]time.Duration{
			300 * time.Millisecond,
			500 * time.Millisecond,
			1000 * time.Millisecond,
			3000 * time.Millisecond,
			400 * time.Millisecond,
			800 * time.Millisecond,
		},
		[]string{"", "", "", "", "", ""},
		[]string{"", "", "", "", "", ""},
	)
	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)

	if r.Improvement.NWins != 4 || r.Improvement.NLosses != 2 {
		t.Fatalf("NWins=%d NLosses=%d, want 4 and 2", r.Improvement.NWins, r.Improvement.NLosses)
	}
	// Win magnitudes are 200, 500, 1000, 2000. p50 ≈ 750 (interpolation
	// between 500 and 1000); max should be 2000.
	if got := r.Improvement.WhenBFasterTTFT.Max; got < 1990 || got > 2010 {
		t.Errorf("WhenBFasterTTFT.Max = %v, want ~2000", got)
	}
	if got := r.Improvement.WhenBFasterTTFT.P50; got < 600 || got > 900 {
		t.Errorf("WhenBFasterTTFT.P50 = %v, want ~750", got)
	}
	// Loss magnitudes are 300 and 600. p50 ≈ 450; max = 600.
	if got := r.Improvement.WhenBSlowerTTFT.Max; got < 590 || got > 610 {
		t.Errorf("WhenBSlowerTTFT.Max = %v, want ~600", got)
	}
	if got := r.Improvement.WhenBSlowerTTFT.P50; got < 400 || got > 500 {
		t.Errorf("WhenBSlowerTTFT.P50 = %v, want ~450", got)
	}
	// Magnitudes should always be non-negative.
	if r.Improvement.WhenBFasterTTFT.Min < 0 || r.Improvement.WhenBSlowerTTFT.Min < 0 {
		t.Errorf("magnitudes must be non-negative: wins.Min=%v losses.Min=%v",
			r.Improvement.WhenBFasterTTFT.Min, r.Improvement.WhenBSlowerTTFT.Min)
	}
}

func TestSummarizePairwiseAggregateSavedLost(t *testing.T) {
	// 4 wins totaling 200+500+1000+2000 = 3700 ms = 3.7 s saved.
	// 2 losses totaling 300+600 = 900 ms = 0.9 s lost.
	// Net = 3700 - 900 = 2800 ms saved = 2.8 s.
	// Avg saved per req (n=6): 3700/6 ≈ 616.7 ms.
	// Avg lost per req: 900/6 = 150 ms.
	// Avg net per req: 466.7 ms.
	pc := NewPairCollector(6)
	fillPair(pc,
		[]time.Duration{
			500 * time.Millisecond,
			1000 * time.Millisecond,
			2000 * time.Millisecond,
			5000 * time.Millisecond,
			100 * time.Millisecond,
			200 * time.Millisecond,
		},
		[]time.Duration{
			300 * time.Millisecond,
			500 * time.Millisecond,
			1000 * time.Millisecond,
			3000 * time.Millisecond,
			400 * time.Millisecond,
			800 * time.Millisecond,
		},
		[]string{"", "", "", "", "", ""},
		[]string{"", "", "", "", "", ""},
	)
	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	imp := r.Improvement

	checkF := func(name string, got, want float64, tol float64) {
		t.Helper()
		if got < want-tol || got > want+tol {
			t.Errorf("%s = %v, want ~%v (±%v)", name, got, want, tol)
		}
	}
	checkF("TotalSavedSecondsTTFT", imp.TotalSavedSecondsTTFT, 3.7, 0.01)
	checkF("TotalLostSecondsTTFT", imp.TotalLostSecondsTTFT, 0.9, 0.01)
	checkF("NetSavedSecondsTTFT", imp.NetSavedSecondsTTFT, 2.8, 0.01)
	checkF("AvgSavedPerRequestMS", imp.AvgSavedPerRequestMS, 616.67, 0.5)
	checkF("AvgLostPerRequestMS", imp.AvgLostPerRequestMS, 150.0, 0.5)
	checkF("AvgNetPerRequestMS", imp.AvgNetPerRequestMS, 466.67, 0.5)
}

func TestSummarizePairwiseNetNegativeWhenWorse(t *testing.T) {
	// B is mostly slower → net should be negative.
	pc := NewPairCollector(3)
	fillPair(pc,
		[]time.Duration{100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond},
		[]time.Duration{1000 * time.Millisecond, 1000 * time.Millisecond, 1000 * time.Millisecond},
		[]string{"", "", ""},
		[]string{"", "", ""},
	)
	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	if r.Improvement.NetSavedSecondsTTFT >= 0 {
		t.Errorf("NetSavedSecondsTTFT = %v, want negative", r.Improvement.NetSavedSecondsTTFT)
	}
	if r.Improvement.AvgNetPerRequestMS >= 0 {
		t.Errorf("AvgNetPerRequestMS = %v, want negative", r.Improvement.AvgNetPerRequestMS)
	}
}

func TestSummarizePairwiseTiedToleranceShiftsClassification(t *testing.T) {
	// One pair with a +50ms delta. At tied=25 it's B-slower; at tied=100 it's tied.
	pc := NewPairCollector(1)
	fillPair(pc,
		[]time.Duration{100 * time.Millisecond},
		[]time.Duration{150 * time.Millisecond},
		[]string{""},
		[]string{""},
	)
	rTight := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 25)
	rLoose := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 100)

	if rTight.Improvement.PctBSlowerTTFT != 100 {
		t.Errorf("tight tied (25): PctBSlowerTTFT = %v, want 100", rTight.Improvement.PctBSlowerTTFT)
	}
	if rLoose.Improvement.PctTied != 100 {
		t.Errorf("loose tied (100): PctTied = %v, want 100", rLoose.Improvement.PctTied)
	}
	if rTight.Improvement.TiedToleranceMS != 25 || rLoose.Improvement.TiedToleranceMS != 100 {
		t.Errorf("TiedToleranceMS not echoed back: tight=%d loose=%d",
			rTight.Improvement.TiedToleranceMS, rLoose.Improvement.TiedToleranceMS)
	}

	// Histogram bin labels should include "(±25)" in the tight case and
	// "(±100)" in the loose case.
	findTied := func(bins []HistBin) string {
		for _, b := range bins {
			if b.LoMS < 0 && b.HiMS > 0 && b.LoMS != -1<<62 && b.HiMS != 1<<62 {
				return b.Label
			}
		}
		return ""
	}
	if got := findTied(rTight.Improvement.Histogram); got != "tied (±25)" {
		t.Errorf("tight tied bin label = %q, want %q", got, "tied (±25)")
	}
	if got := findTied(rLoose.Improvement.Histogram); got != "tied (±100)" {
		t.Errorf("loose tied bin label = %q, want %q", got, "tied (±100)")
	}
}

func TestSummarizePairwiseDefaultTiedTolerance(t *testing.T) {
	// Passing 0 should fall back to DefaultTiedToleranceMS.
	pc := NewPairCollector(1)
	fillPair(pc,
		[]time.Duration{100 * time.Millisecond},
		[]time.Duration{105 * time.Millisecond},
		[]string{""},
		[]string{""},
	)
	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	if r.Improvement.TiedToleranceMS != DefaultTiedToleranceMS {
		t.Errorf("TiedToleranceMS = %d, want default %d",
			r.Improvement.TiedToleranceMS, DefaultTiedToleranceMS)
	}
}

func TestSummarizePairwiseSpeedupAndGeomeans(t *testing.T) {
	// Construct pairs whose speedups (A/B) land in known buckets.
	// Pair 0: A=4000, B=1000 → speedup 4x  (in 3x–10x bin)
	// Pair 1: A=2000, B=1000 → speedup 2x  (in 1.5x–3x bin)
	// Pair 2: A=1000, B=1000 → speedup 1x  (tied)
	// Pair 3: A=500,  B=1000 → speedup 0.5x (in 0.33x–0.67x bin), expressed as 2x slowdown
	pc := NewPairCollector(4)
	fillPair(pc,
		[]time.Duration{4000 * time.Millisecond, 2000 * time.Millisecond, 1000 * time.Millisecond, 500 * time.Millisecond},
		[]time.Duration{1000 * time.Millisecond, 1000 * time.Millisecond, 1000 * time.Millisecond, 1000 * time.Millisecond},
		[]string{"", "", "", ""},
		[]string{"", "", "", ""},
	)
	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	if len(r.Improvement.SpeedupHistogram) == 0 {
		t.Fatal("expected speedup histogram, got none")
	}

	// Find counts in named bins.
	count := func(label string) int {
		for _, b := range r.Improvement.SpeedupHistogram {
			if b.Label == label {
				return b.Count
			}
		}
		t.Fatalf("bin %q not found", label)
		return 0
	}
	if count("3x – 10x") != 1 {
		t.Errorf("3x-10x count = %d, want 1", count("3x – 10x"))
	}
	if count("1.5x – 3x") != 1 {
		t.Errorf("1.5x-3x count = %d, want 1", count("1.5x – 3x"))
	}
	if count("tied (0.95x – 1.05x)") != 1 {
		t.Errorf("tied count = %d, want 1", count("tied (0.95x – 1.05x)"))
	}
	if count("0.33x – 0.67x") != 1 {
		t.Errorf("0.33x-0.67x count = %d, want 1", count("0.33x – 0.67x"))
	}

	// Geomean overall: geomean(4, 2, 1, 0.5) = (4*2*1*0.5)^(1/4) = 4^(1/4) ≈ 1.414
	if got := r.Improvement.GeomeanSpeedup; got < 1.40 || got > 1.43 {
		t.Errorf("GeomeanSpeedup = %v, want ~1.414", got)
	}
	// Geomean when B faster: geomean(4, 2) = sqrt(8) ≈ 2.828
	if got := r.Improvement.GeomeanSpeedupWhenBFaster; got < 2.80 || got > 2.85 {
		t.Errorf("GeomeanSpeedupWhenBFaster = %v, want ~2.828", got)
	}
	// Geomean slowdown when B slower: speedup 0.5 → inverted = 2.0 → geomean = 2.0
	if got := r.Improvement.GeomeanSlowdownWhenBSlower; got < 1.95 || got > 2.05 {
		t.Errorf("GeomeanSlowdownWhenBSlower = %v, want ~2.0", got)
	}
}

func TestSummarizePairwiseHistogramBuckets(t *testing.T) {
	// Construct deltas that should land in specific buckets.
	// delta = B - A (negative => B faster).
	pc := NewPairCollector(6)
	fillPair(pc,
		// A times                       // B times    => delta (B-A)
		[]time.Duration{
			100 * time.Millisecond,   // 400-100  = +300 ms (B slower, +100..+300)
			400 * time.Millisecond,   // 100-400  = -300 ms (B faster, -300..-100)
			500 * time.Millisecond,   // 100-500  = -400 ms (-1000..-300)
			15000 * time.Millisecond, // 3000-15000 = -12000 ms (<-10000)
			55 * time.Millisecond,    // 50-55    =   -5 ms (tied)
			50 * time.Millisecond,    // 15050-50 = +15000 ms (>+10000)
		},
		[]time.Duration{
			400 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
			3000 * time.Millisecond,
			50 * time.Millisecond,
			15050 * time.Millisecond,
		},
		[]string{"", "", "", "", "", ""},
		[]string{"", "", "", "", "", ""},
	)
	r := SummarizePairwise(pc, time.Second, time.Second, "A", "B", 0)
	if len(r.Improvement.Histogram) == 0 {
		t.Fatal("expected histogram bins, got none")
	}
	// Sum of all bin counts must equal N.
	var total int
	for _, b := range r.Improvement.Histogram {
		total += b.Count
	}
	if total != r.Improvement.N {
		t.Errorf("histogram total = %d, want N = %d", total, r.Improvement.N)
	}
	// At least one bin in each direction should be non-empty given the inputs.
	pos, neg, tied := 0, 0, 0
	for _, b := range r.Improvement.Histogram {
		if b.Count == 0 {
			continue
		}
		switch {
		case b.LoMS < 0 && b.HiMS > 0:
			tied += b.Count
		case b.LoMS >= 0:
			pos += b.Count
		case b.HiMS <= 0:
			neg += b.Count
		}
	}
	if pos == 0 || neg == 0 || tied == 0 {
		t.Errorf("expected pos/neg/tied all non-zero, got pos=%d neg=%d tied=%d", pos, neg, tied)
	}
}
