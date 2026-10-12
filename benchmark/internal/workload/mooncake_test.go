package workload

import (
	"context"
	"strings"
	"testing"
)

func TestMooncakePrefixSharing(t *testing.T) {
	m, err := NewMooncake(MooncakeConfig{
		Path:        "../../testdata/mooncake_sample.jsonl",
		BlockTokens: 512,
		Speedup:     1.0,
		Seed:        1,
	})
	if err != nil {
		t.Fatalf("NewMooncake: %v", err)
	}

	var reqs []*Request
	for {
		r, ok := m.Next(context.Background())
		if !ok {
			break
		}
		reqs = append(reqs, r)
	}
	if len(reqs) != 5 {
		t.Fatalf("got %d requests, want 5", len(reqs))
	}

	// Requests 0 and 1 share hash_id 101 → first BlockTokens words must match.
	head := func(s string, n int) string {
		w := strings.Fields(s)
		if len(w) < n {
			n = len(w)
		}
		return strings.Join(w[:n], " ")
	}
	h0 := head(reqs[0].Messages[0].Content, 512)
	h1 := head(reqs[1].Messages[0].Content, 512)
	if h0 != h1 {
		t.Error("requests 0 and 1 share hash_id 101 but their first 512-word blocks differ")
	}

	// Request 2 also starts with hash_id 101 → same head.
	h2 := head(reqs[2].Messages[0].Content, 512)
	if h0 != h2 {
		t.Error("requests 0 and 2 share hash_id 101 but differ")
	}

	// Requests 3 and 4 share hash_ids [201, 202] → first 1024 words must match.
	h3 := head(reqs[3].Messages[0].Content, 1024)
	h4 := head(reqs[4].Messages[0].Content, 1024)
	if h3 != h4 {
		t.Error("requests 3 and 4 share [201,202] but their first 1024-word blocks differ")
	}

	// Requests 0 and 3 share NO hash_ids → first block must differ.
	h0b := head(reqs[0].Messages[0].Content, 512)
	h3b := head(reqs[3].Messages[0].Content, 512)
	if h0b == h3b {
		t.Error("requests 0 and 3 share no hash_ids but their first 512 words match")
	}

	// Timing: 5th record at 320ms, with Speedup=1 → DelayUntil = 320ms.
	if reqs[4].DelayUntil.Milliseconds() != 320 {
		t.Errorf("delay = %v, want 320ms", reqs[4].DelayUntil)
	}

	// MaxTokens should match output_length.
	if reqs[0].MaxTokens != 32 {
		t.Errorf("max_tokens = %d, want 32", reqs[0].MaxTokens)
	}
}
