// diagprefix: dump synthetic prefixes for two adjacent seeds and report
// whether any pair collides (byte-for-byte equal, or shares a long common
// prefix). One-off diagnostic; not part of the harness build.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/workload"
)

func main() {
	s1 := flag.Uint64("seed1", 1781722743, "first seed")
	s2 := flag.Uint64("seed2", 1781722742, "second seed")
	n := flag.Int("prefix-count", 16, "")
	tok := flag.Int("prefix-tokens", 8192, "")
	show := flag.Int("show-chars", 80, "leading chars to print per prefix")
	flag.Parse()

	a := genPrefixes(*s1, *n, *tok)
	b := genPrefixes(*s2, *n, *tok)

	collisions := 0
	for i := 0; i < *n; i++ {
		pa, pb := a[i], b[i]
		ha := sha256.Sum256([]byte(pa))
		hb := sha256.Sum256([]byte(pb))
		eq := pa == pb
		commonRun := commonLeadingBytes(pa, pb)
		fmt.Printf("p%-3d  seed1 sha=%s len=%d … %q\n", i, hex.EncodeToString(ha[:8]), len(pa), trunc(pa, *show))
		fmt.Printf("      seed2 sha=%s len=%d … %q\n", hex.EncodeToString(hb[:8]), len(pb), trunc(pb, *show))
		fmt.Printf("      equal=%v  common_leading_bytes=%d\n\n", eq, commonRun)
		if eq {
			collisions++
		}
	}
	fmt.Printf("byte-for-byte collisions across seeds: %d / %d\n", collisions, *n)
}

// genPrefixes returns the synthetic prefix pool for a given seed by exercising
// the public Workload interface: drive Next() until we've observed each
// prefix label "p0".."p<N-1>" at least once. With TailTokens=0 the request
// content equals the prefix verbatim.
func genPrefixes(seed uint64, n, tokens int) []string {
	s, err := workload.NewSynthetic(workload.SyntheticConfig{
		PrefixCount: n, PrefixTokens: tokens, TailTokens: 0,
		TotalRequests: 100_000, MaxOutputToks: 1, Seed: seed,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	out := make([]string, n)
	have := 0
	for have < n {
		req, ok := s.Next(context.Background())
		if !ok {
			break
		}
		var idx int
		_, _ = fmt.Sscanf(req.Prefix, "p%d", &idx)
		if idx < 0 || idx >= n {
			continue
		}
		if out[idx] == "" {
			out[idx] = req.Messages[0].Content
			have++
		}
	}
	return out
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func commonLeadingBytes(a, b string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
