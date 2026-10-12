package workload

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestSyntheticDeterministicPrefixes(t *testing.T) {
	cfg := SyntheticConfig{
		PrefixCount:   3,
		PrefixTokens:  20,
		TailTokens:    5,
		TotalRequests: 50,
		Seed:          7,
	}

	gather := func() map[string]int {
		s, err := NewSynthetic(cfg)
		if err != nil {
			t.Fatalf("NewSynthetic: %v", err)
		}
		// Collect the first 20 words (~ prefix region) of each request and count.
		seen := map[string]int{}
		for {
			req, ok := s.Next(context.Background())
			if !ok {
				break
			}
			words := strings.Fields(req.Messages[0].Content)
			if len(words) < cfg.PrefixTokens {
				t.Fatalf("request too short: %d words", len(words))
			}
			head := strings.Join(words[:cfg.PrefixTokens], " ")
			seen[head]++
		}
		return seen
	}

	a := gather()
	b := gather()

	// Same seed → same prefix set, same counts.
	if len(a) != cfg.PrefixCount {
		t.Errorf("got %d distinct prefixes, want %d", len(a), cfg.PrefixCount)
	}
	for k, v := range a {
		if b[k] != v {
			t.Errorf("non-deterministic: prefix %q count %d vs %d", k[:20], v, b[k])
		}
	}
}

func TestSyntheticPrefixLabelMatchesPrefixIndex(t *testing.T) {
	cfg := SyntheticConfig{
		PrefixCount: 4, PrefixTokens: 6, TailTokens: 2, TotalRequests: 50, Seed: 99,
	}
	s, err := NewSynthetic(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Walk every request, recording (Prefix → first words of prompt). Requests
	// sharing a Prefix label must share the same prompt prefix bytes, and the
	// label must be "p<N>" for N in [0, PrefixCount).
	bytesForLabel := map[string]string{}
	allowed := map[string]bool{}
	for i := 0; i < cfg.PrefixCount; i++ {
		allowed[fmt.Sprintf("p%d", i)] = true
	}

	for {
		req, ok := s.Next(context.Background())
		if !ok {
			break
		}
		if !allowed[req.Prefix] {
			t.Fatalf("unexpected Prefix %q (PrefixCount=%d)", req.Prefix, cfg.PrefixCount)
		}
		words := strings.Fields(req.Messages[0].Content)
		if len(words) < cfg.PrefixTokens {
			t.Fatalf("too few words: %d", len(words))
		}
		head := strings.Join(words[:cfg.PrefixTokens], " ")
		if prev, seen := bytesForLabel[req.Prefix]; seen {
			if prev != head {
				t.Errorf("Prefix %s mapped to two different prompt prefixes:\n  %q\n  %q",
					req.Prefix, prev, head)
			}
		} else {
			bytesForLabel[req.Prefix] = head
		}
	}
	if len(bytesForLabel) == 0 {
		t.Fatal("no requests produced")
	}
}

func TestSyntheticExhausts(t *testing.T) {
	s, err := NewSynthetic(SyntheticConfig{
		PrefixCount: 2, PrefixTokens: 5, TailTokens: 2, TotalRequests: 4, Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		_, ok := s.Next(context.Background())
		if !ok {
			break
		}
		count++
		if count > 100 {
			t.Fatal("did not exhaust")
		}
	}
	if count != 4 {
		t.Errorf("got %d, want 4", count)
	}
}
