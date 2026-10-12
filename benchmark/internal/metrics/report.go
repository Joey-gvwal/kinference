package metrics

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

func WriteJSON(path string, summary Summary) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(summary)
}

func WriteText(w io.Writer, s Summary) {
	fmt.Fprintf(w, "\n=== Benchmark Summary ===\n")
	writeSummaryBody(w, s, true)
}

// writeSummaryBody emits the per-endpoint summary body (everything after the
// "===" header line). When sortedSamples is true and TTFTSamplesMS is present,
// it prints the sorted list — used for single-endpoint runs. Pairwise mode
// suppresses the sorted list and prints its own send-order table instead.
func writeSummaryBody(w io.Writer, s Summary, sortedSamples bool) {
	fmt.Fprintf(w, "Workload:        %s\n", s.Workload)
	fmt.Fprintf(w, "Endpoint:        %s\n", s.Endpoint)
	fmt.Fprintf(w, "Model:           %s\n", s.Model)
	fmt.Fprintf(w, "Concurrency:     %d\n", s.Concurrency)
	fmt.Fprintf(w, "Requests:        %d total, %d ok, %d failed\n", s.TotalRequests, s.SuccessRequests, s.FailedRequests)
	fmt.Fprintf(w, "Wall time:       %s\n", formatWallTime(s.WallSeconds))
	fmt.Fprintf(w, "Throughput:      %.2f req/s, %.2f output tok/s\n", s.RequestsPerSec, s.OutputTokensPerS)
	fmt.Fprintf(w, "Prompt tokens:   %d total\n", s.TotalPromptTokens)
	fmt.Fprintf(w, "Output tokens:   %d total\n", s.TotalOutputTokens)
	fmt.Fprintf(w, "\nLatency (ms)         p50      p90      p95      p99      min      max\n")
	fmt.Fprintf(w, "  TTFT          %9.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n", s.TTFT.P50, s.TTFT.P90, s.TTFT.P95, s.TTFT.P99, s.TTFT.Min, s.TTFT.Max)
	fmt.Fprintf(w, "  ITL           %9.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n", s.ITL.P50, s.ITL.P90, s.ITL.P95, s.ITL.P99, s.ITL.Min, s.ITL.Max)
	fmt.Fprintf(w, "  TPOT          %9.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n", s.TPOT.P50, s.TPOT.P90, s.TPOT.P95, s.TPOT.P99, s.TPOT.Min, s.TPOT.Max)
	fmt.Fprintf(w, "  E2E           %9.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n", s.E2E.P50, s.E2E.P90, s.E2E.P95, s.E2E.P99, s.E2E.Min, s.E2E.Max)

	if sortedSamples && len(s.TTFTSamplesMS) > 0 {
		fmt.Fprintf(w, "\nTTFT samples (ms, sorted, n=%d):\n", len(s.TTFTSamplesMS))
		const perLine = 10
		for i, v := range s.TTFTSamplesMS {
			if i%perLine == 0 {
				fmt.Fprint(w, " ")
			}
			fmt.Fprintf(w, " %5d", v)
			if i%perLine == perLine-1 || i == len(s.TTFTSamplesMS)-1 {
				fmt.Fprintln(w)
			}
		}
	}

	if len(s.ErrorBuckets) > 0 {
		fmt.Fprintf(w, "\nTop errors (up to 5):\n")
		for _, b := range s.ErrorBuckets {
			fmt.Fprintf(w, "  %4d × %s\n", b.Count, b.Message)
		}
	}
	fmt.Fprintf(w, "\n")
}

func WritePairwiseJSON(path string, r PairwiseReport) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func WritePairwiseText(w io.Writer, r PairwiseReport) {
	fmt.Fprintf(w, "\n=== Benchmark Summary: %s ===\n", r.LabelA)
	writeSummaryBody(w, r.EndpointA, false)

	fmt.Fprintf(w, "=== Benchmark Summary: %s ===\n", r.LabelB)
	writeSummaryBody(w, r.EndpointB, false)

	if len(r.PairTable) > 0 {
		fmt.Fprintf(w, "=== Per-request TTFT (send order, n=%d; A=%s, B=%s) ===\n",
			len(r.PairTable), r.LabelA, r.LabelB)

		// Leading column: full request ID string when present, else the
		// canonical integer index. Width adapts to the longest value.
		idHeader := "#"
		idW := 3
		anyRequestID := false
		for _, row := range r.PairTable {
			if row.RequestID != "" {
				anyRequestID = true
				if len(row.RequestID) > idW {
					idW = len(row.RequestID)
				}
			}
		}
		if anyRequestID {
			idHeader = "request_id"
			if len(idHeader) > idW {
				idW = len(idHeader)
			}
		}
		idCell := func(row PairRow) string {
			if row.RequestID != "" {
				return row.RequestID
			}
			return fmt.Sprintf("%d", row.Index)
		}

		// Prefix column: only when at least one row has a non-empty prefix
		// label. For synthetic this is "p<N>" identifying which prefix from
		// the shared pool the request used.
		prefixW := 0
		for _, row := range r.PairTable {
			if len(row.Prefix) > prefixW {
				prefixW = len(row.Prefix)
			}
		}
		if prefixW > 0 && prefixW < 6 {
			prefixW = 6
		}

		// Prompt column (final): when set, appended without padding because
		// the prompt snippet can be long. Set by --dump-prompt.
		anyPrompt := false
		for _, row := range r.PairTable {
			if row.Prompt != "" {
				anyPrompt = true
				break
			}
		}
		promptCell := func(row PairRow) string {
			if !anyPrompt {
				return ""
			}
			return "  " + row.Prompt
		}
		promptHeader := ""
		if anyPrompt {
			promptHeader = "  prompt"
		}

		if prefixW > 0 {
			fmt.Fprintf(w, "  %-*s  %-*s      A       B    B-A%s\n",
				idW, idHeader, prefixW, "prefix", promptHeader)
			for _, row := range r.PairTable {
				switch {
				case row.AErr != "" && row.BErr != "":
					fmt.Fprintf(w, "  %-*s  %-*s   ERROR   ERROR        -%s\n",
						idW, idCell(row), prefixW, row.Prefix, promptCell(row))
				case row.AErr != "":
					fmt.Fprintf(w, "  %-*s  %-*s   ERROR  %6d        -%s\n",
						idW, idCell(row), prefixW, row.Prefix, row.BTTFTMS, promptCell(row))
				case row.BErr != "":
					fmt.Fprintf(w, "  %-*s  %-*s  %6d   ERROR        -%s\n",
						idW, idCell(row), prefixW, row.Prefix, row.ATTFTMS, promptCell(row))
				default:
					fmt.Fprintf(w, "  %-*s  %-*s  %6d  %6d   %+6d%s\n",
						idW, idCell(row), prefixW, row.Prefix,
						row.ATTFTMS, row.BTTFTMS, row.DeltaMS, promptCell(row))
				}
			}
		} else {
			fmt.Fprintf(w, "  %-*s      A       B    B-A%s\n", idW, idHeader, promptHeader)
			for _, row := range r.PairTable {
				switch {
				case row.AErr != "" && row.BErr != "":
					fmt.Fprintf(w, "  %-*s   ERROR   ERROR        -%s\n",
						idW, idCell(row), promptCell(row))
				case row.AErr != "":
					fmt.Fprintf(w, "  %-*s   ERROR  %6d        -%s\n",
						idW, idCell(row), row.BTTFTMS, promptCell(row))
				case row.BErr != "":
					fmt.Fprintf(w, "  %-*s  %6d   ERROR        -%s\n",
						idW, idCell(row), row.ATTFTMS, promptCell(row))
				default:
					fmt.Fprintf(w, "  %-*s  %6d  %6d   %+6d%s\n",
						idW, idCell(row), row.ATTFTMS, row.BTTFTMS, row.DeltaMS, promptCell(row))
				}
			}
		}
		fmt.Fprintf(w, "\n")
	}

	// Final side-by-side TTFT recap so the most important numbers are the
	// last thing printed.
	labelW := len(r.LabelA)
	if len(r.LabelB) > labelW {
		labelW = len(r.LabelB)
	}
	fmt.Fprintf(w, "=== TTFT recap (ms) ===\n")
	fmt.Fprintf(w, "  %-*s      p50      p90      p95      p99      min      max\n", labelW, "")
	fmt.Fprintf(w, "  %-*s %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n", labelW, r.LabelA,
		r.EndpointA.TTFT.P50, r.EndpointA.TTFT.P90, r.EndpointA.TTFT.P95, r.EndpointA.TTFT.P99,
		r.EndpointA.TTFT.Min, r.EndpointA.TTFT.Max)
	fmt.Fprintf(w, "  %-*s %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n", labelW, r.LabelB,
		r.EndpointB.TTFT.P50, r.EndpointB.TTFT.P90, r.EndpointB.TTFT.P95, r.EndpointB.TTFT.P99,
		r.EndpointB.TTFT.Min, r.EndpointB.TTFT.Max)
	fmt.Fprintf(w, "\n")

	// Improvement analysis: the punchline goes last.
	fmt.Fprintf(w, "=== %s vs %s (delta = %s − %s ms; negative ⇒ %s faster) ===\n",
		r.LabelB, r.LabelA, r.LabelB, r.LabelA, r.LabelB)
	if r.Improvement.N == 0 {
		fmt.Fprintf(w, "  no paired successes\n\n")
		return
	}
	imp := r.Improvement
	fmt.Fprintf(w, "  N paired:        %d   (%s faster: %d / %.1f%%   tied: %.1f%%   %s slower: %d / %.1f%%)\n\n",
		imp.N,
		r.LabelB, imp.NWins, imp.PctBFasterTTFT,
		imp.PctTied,
		r.LabelB, imp.NLosses, imp.PctBSlowerTTFT)

	fmt.Fprintf(w, "  Aggregate TTFT impact (across all %d paired requests):\n", imp.N)
	fmt.Fprintf(w, "    Total saved by %s:   %8.2f s   (avg %+8.1f ms/req)\n",
		r.LabelB, imp.TotalSavedSecondsTTFT, imp.AvgSavedPerRequestMS)
	fmt.Fprintf(w, "    Total lost  by %s:   %8.2f s   (avg %+8.1f ms/req)\n",
		r.LabelB, imp.TotalLostSecondsTTFT, imp.AvgLostPerRequestMS)
	fmt.Fprintf(w, "    Net  saved  by %s:   %+8.2f s   (avg %+8.1f ms/req)\n\n",
		r.LabelB, imp.NetSavedSecondsTTFT, imp.AvgNetPerRequestMS)

	writeMagnitudeBlock(w, fmt.Sprintf("Wins (%s faster by >%dms, n=%d, %.1f%%)",
		r.LabelB, imp.TiedToleranceMS, imp.NWins, imp.PctBFasterTTFT),
		imp.WhenBFasterTTFT, imp.NWins)
	writeMagnitudeBlock(w, fmt.Sprintf("Losses (%s slower by >%dms, n=%d, %.1f%%)",
		r.LabelB, imp.TiedToleranceMS, imp.NLosses, imp.PctBSlowerTTFT),
		imp.WhenBSlowerTTFT, imp.NLosses)

	writeImprovementHistogram(w, imp, r.LabelA, r.LabelB)
	writeSpeedupHistogram(w, imp, r.LabelA, r.LabelB)
}

// writeMagnitudeBlock prints a header line followed by p50/p90/p95/p99/max
// of a one-sided magnitude distribution (positive ms). Used for the Wins
// and Losses sections where the sign is already conveyed by the section title.
func writeMagnitudeBlock(w io.Writer, header string, p Percentiles, n int) {
	fmt.Fprintf(w, "  %s:\n", header)
	if n == 0 {
		fmt.Fprintf(w, "    (none)\n\n")
		return
	}
	fmt.Fprintf(w, "    p50  %8.1f ms\n", p.P50)
	fmt.Fprintf(w, "    p90  %8.1f ms\n", p.P90)
	fmt.Fprintf(w, "    p95  %8.1f ms\n", p.P95)
	fmt.Fprintf(w, "    p99  %8.1f ms\n", p.P99)
	fmt.Fprintf(w, "    max  %8.1f ms\n\n", p.Max)
}

// writeImprovementHistogram renders the per-request improvement distribution
// as an ASCII histogram. Each bin gets a bar whose length is proportional to
// its count, plus the raw count and percent. The "tied" row is marked with a
// dashed separator on either side so the zero-line is visually obvious.
func writeImprovementHistogram(w io.Writer, s ImprovementStats, labelA, labelB string) {
	if len(s.Histogram) == 0 || s.N == 0 {
		return
	}
	maxCount := 0
	maxLabelW := 0
	for _, b := range s.Histogram {
		if b.Count > maxCount {
			maxCount = b.Count
		}
		if len(b.Label) > maxLabelW {
			maxLabelW = len(b.Label)
		}
	}
	if maxCount == 0 {
		return
	}
	const barWidth = 40
	const block = "█"

	fmt.Fprintf(w, "  Delta distribution (%s − %s ms, n=%d; negative ⇒ %s faster):\n",
		labelB, labelA, s.N, labelB)
	for _, b := range s.Histogram {
		// Render bar length as a count of full blocks; round to nearest.
		filled := (b.Count*barWidth + maxCount/2) / maxCount
		if b.Count > 0 && filled == 0 {
			filled = 1 // ensure at least one block for any non-zero count
		}
		bar := ""
		for i := 0; i < filled; i++ {
			bar += block
		}
		pct := float64(b.Count) * 100.0 / float64(s.N)
		marker := ""
		if isTiedBin(b) {
			marker = " ←tied"
		}
		fmt.Fprintf(w, "    %-*s  %-*s %5d  %5.1f%%%s\n",
			maxLabelW, b.Label, barWidth, bar, b.Count, pct, marker)
	}
	fmt.Fprintln(w)
}

func isTiedBin(b HistBin) bool {
	// The tied bin straddles zero with symmetric edges.
	return b.LoMS < 0 && b.HiMS > 0
}

// writeSpeedupHistogram renders the ratio histogram (A_TTFT / B_TTFT) plus
// three geomean rows: overall, when-B-faster (>1.05x), and when-B-slower
// (<0.95x; expressed as a slowdown multiplier so it reads symmetrically).
func writeSpeedupHistogram(w io.Writer, s ImprovementStats, labelA, labelB string) {
	if len(s.SpeedupHistogram) == 0 {
		return
	}
	total := 0
	maxCount := 0
	maxLabelW := 0
	for _, b := range s.SpeedupHistogram {
		total += b.Count
		if b.Count > maxCount {
			maxCount = b.Count
		}
		if len(b.Label) > maxLabelW {
			maxLabelW = len(b.Label)
		}
	}
	if total == 0 || maxCount == 0 {
		return
	}
	const barWidth = 40
	const block = "█"

	fmt.Fprintf(w, "  %s speedup over %s  (ratio = %s_TTFT / %s_TTFT; >1 = %s faster, n=%d):\n",
		labelB, labelA, labelA, labelB, labelB, total)
	for _, b := range s.SpeedupHistogram {
		filled := (b.Count*barWidth + maxCount/2) / maxCount
		if b.Count > 0 && filled == 0 {
			filled = 1
		}
		bar := ""
		for i := 0; i < filled; i++ {
			bar += block
		}
		pct := float64(b.Count) * 100.0 / float64(total)
		marker := ""
		if isTiedSpeedupBin(b) {
			marker = " ←tied"
		}
		fmt.Fprintf(w, "    %-*s  %-*s %5d  %5.1f%%%s\n",
			maxLabelW, b.Label, barWidth, bar, b.Count, pct, marker)
	}

	if s.GeomeanSpeedup > 0 {
		fmt.Fprintf(w, "    geomean speedup overall:        %5.2fx\n", s.GeomeanSpeedup)
	}
	if s.GeomeanSpeedupWhenBFaster > 0 {
		fmt.Fprintf(w, "    geomean speedup when %s faster: %5.2fx\n",
			labelB, s.GeomeanSpeedupWhenBFaster)
	}
	if s.GeomeanSlowdownWhenBSlower > 0 {
		fmt.Fprintf(w, "    geomean slowdown when %s slower:%5.2fx\n",
			labelB, s.GeomeanSlowdownWhenBSlower)
	}
	fmt.Fprintln(w)
}

// formatWallTime renders the run's wall-clock seconds as a human-readable
// duration: sub-minute runs keep two-decimal seconds for precision; longer
// runs use Go's compact "1h2m3s" form, truncated to whole seconds.
func formatWallTime(seconds float64) string {
	d := time.Duration(seconds * float64(time.Second))
	if d < time.Minute {
		return fmt.Sprintf("%.2fs", seconds)
	}
	return d.Truncate(time.Second).String()
}
