package workload

import (
	"context"
	"testing"
)

func TestShareGPTGrowingHistory(t *testing.T) {
	s, err := NewShareGPT(ShareGPTConfig{
		Path: "../../testdata/sharegpt_sample.json",
		Seed: 1,
	})
	if err != nil {
		t.Fatalf("NewShareGPT: %v", err)
	}

	var reqs []*Request
	for {
		r, ok := s.Next(context.Background())
		if !ok {
			break
		}
		reqs = append(reqs, r)
	}

	// 3 user turns total across both conversations.
	if len(reqs) != 3 {
		t.Fatalf("got %d requests, want 3", len(reqs))
	}

	// Within each conversation, history grows monotonically; the first user
	// turn of a conversation has just 1 message.
	singleTurn := 0
	growingTurn := 0
	for _, r := range reqs {
		switch len(r.Messages) {
		case 1:
			singleTurn++
		case 3:
			growingTurn++
		}
	}
	if singleTurn != 2 || growingTurn != 1 {
		t.Errorf("got %d single-turn and %d 3-message turns; want 2 and 1", singleTurn, growingTurn)
	}

	// Roles must be mapped correctly.
	for _, r := range reqs {
		for _, m := range r.Messages {
			if m.Role != "user" && m.Role != "assistant" && m.Role != "system" {
				t.Errorf("bad role %q", m.Role)
			}
		}
		// The last message of any prompt should be the user's turn.
		last := r.Messages[len(r.Messages)-1]
		if last.Role != "user" {
			t.Errorf("last message role = %q, want user", last.Role)
		}
	}
}
