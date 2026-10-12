package workload

import (
	"context"
	"fmt"
	"testing"
)

func drainMultiturn(t *testing.T, cfg MultiturnConfig) []*Request {
	t.Helper()
	m, err := NewMultiturn(cfg)
	if err != nil {
		t.Fatalf("NewMultiturn: %v", err)
	}
	var reqs []*Request
	for {
		r, ok := m.Next(context.Background())
		if !ok {
			break
		}
		reqs = append(reqs, r)
	}
	return reqs
}

func TestMultiturnTotalAndLabels(t *testing.T) {
	cfg := MultiturnConfig{
		Conversations:   4,
		Turns:           3,
		SystemTokens:    20,
		UserTokens:      8,
		AssistantTokens: 12,
		Seed:            42,
	}
	reqs := drainMultiturn(t, cfg)
	if got, want := len(reqs), cfg.Conversations*cfg.Turns; got != want {
		t.Fatalf("got %d reqs, want %d", got, want)
	}
	// Every label must be of the form "c<j>t<k>" with j in [0, M), k in [0, N).
	for _, r := range reqs {
		var j, k int
		if _, err := fmt.Sscanf(r.Prefix, "c%dt%d", &j, &k); err != nil {
			t.Fatalf("bad prefix label %q: %v", r.Prefix, err)
		}
		if j < 0 || j >= cfg.Conversations || k < 0 || k >= cfg.Turns {
			t.Errorf("label %q out of range (M=%d, N=%d)", r.Prefix, cfg.Conversations, cfg.Turns)
		}
	}
}

func TestMultiturnDeterministicAcrossInstances(t *testing.T) {
	cfg := MultiturnConfig{
		Conversations: 3, Turns: 2, SystemTokens: 10, UserTokens: 5, AssistantTokens: 7, Seed: 99,
	}
	a := drainMultiturn(t, cfg)
	b := drainMultiturn(t, cfg)
	if len(a) != len(b) {
		t.Fatalf("len mismatch: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Prefix != b[i].Prefix {
			t.Errorf("req %d: prefix differs (%q vs %q)", i, a[i].Prefix, b[i].Prefix)
		}
		if len(a[i].Messages) != len(b[i].Messages) {
			t.Errorf("req %d: message count differs (%d vs %d)", i, len(a[i].Messages), len(b[i].Messages))
			continue
		}
		for mi := range a[i].Messages {
			if a[i].Messages[mi] != b[i].Messages[mi] {
				t.Errorf("req %d msg %d differs:\n  %q\n  %q",
					i, mi, a[i].Messages[mi], b[i].Messages[mi])
				break
			}
		}
	}
}

func TestMultiturnDifferentSeedsDiffer(t *testing.T) {
	base := MultiturnConfig{
		Conversations: 3, Turns: 2, SystemTokens: 10, UserTokens: 5, AssistantTokens: 7, Seed: 99,
	}
	a := drainMultiturn(t, base)
	base.Seed = 100
	b := drainMultiturn(t, base)

	// The system prompt (first message of any request) should differ.
	if a[0].Messages[0].Content == b[0].Messages[0].Content {
		t.Error("system prompt identical across seeds; expected differ")
	}
}

func TestMultiturnSchedulePreservesPerConvOrder(t *testing.T) {
	cfg := MultiturnConfig{
		Conversations: 5, Turns: 4, SystemTokens: 10, UserTokens: 5, AssistantTokens: 7, Seed: 1,
	}
	reqs := drainMultiturn(t, cfg)
	lastTurn := make(map[int]int) // conv -> last turn seen
	for _, conv := range []int{0, 1, 2, 3, 4} {
		lastTurn[conv] = -1
	}
	for i, r := range reqs {
		var j, k int
		fmt.Sscanf(r.Prefix, "c%dt%d", &j, &k)
		if k != lastTurn[j]+1 {
			t.Fatalf("req #%d conv=%d turn=%d, but prior turn for this conv was %d (expected %d next)",
				i, j, k, lastTurn[j], lastTurn[j]+1)
		}
		lastTurn[j] = k
	}
	// And every conversation must have completed all its turns.
	for j, k := range lastTurn {
		if k != cfg.Turns-1 {
			t.Errorf("conv %d ended at turn %d, expected %d", j, k, cfg.Turns-1)
		}
	}
}

func TestMultiturnPromptExtendsAcrossTurns(t *testing.T) {
	// For a given conversation, turn k+1's messages should be turn k's messages
	// plus 2 more entries: an assistant turn (closing turn k) and a new user
	// turn (turn k+1).
	cfg := MultiturnConfig{
		Conversations: 2, Turns: 4, SystemTokens: 12, UserTokens: 6, AssistantTokens: 9, Seed: 5,
	}
	reqs := drainMultiturn(t, cfg)
	// Group requests by conversation, sorted by turn.
	type entry struct {
		turn int
		req  *Request
	}
	byConv := map[int][]entry{}
	for _, r := range reqs {
		var j, k int
		fmt.Sscanf(r.Prefix, "c%dt%d", &j, &k)
		byConv[j] = append(byConv[j], entry{turn: k, req: r})
	}
	// Schedule already preserves per-conv turn order, so byConv[j] is in turn order.
	for j, list := range byConv {
		for k := 1; k < len(list); k++ {
			prev := list[k-1].req
			curr := list[k].req
			// curr.Messages must have exactly len(prev.Messages)+2 entries.
			if got, want := len(curr.Messages), len(prev.Messages)+2; got != want {
				t.Errorf("conv %d turn %d: msg count %d, want %d", j, k, got, want)
				continue
			}
			// Leading len(prev.Messages) entries of curr must equal prev.Messages verbatim.
			for mi := range prev.Messages {
				if curr.Messages[mi] != prev.Messages[mi] {
					t.Errorf("conv %d turn %d: msg[%d] differs from prior turn:\n  prev: %+v\n  curr: %+v",
						j, k, mi, prev.Messages[mi], curr.Messages[mi])
					break
				}
			}
			// The two new entries should be (assistant, user).
			n := len(prev.Messages)
			if curr.Messages[n].Role != "assistant" {
				t.Errorf("conv %d turn %d: expected assistant at position %d, got %q",
					j, k, n, curr.Messages[n].Role)
			}
			if curr.Messages[n+1].Role != "user" {
				t.Errorf("conv %d turn %d: expected user at position %d, got %q",
					j, k, n+1, curr.Messages[n+1].Role)
			}
		}
	}
}

func TestMultiturnNoSystemPromptWhenZeroTokens(t *testing.T) {
	cfg := MultiturnConfig{
		Conversations: 1, Turns: 1, SystemTokens: 0, UserTokens: 5, AssistantTokens: 7, Seed: 1,
	}
	reqs := drainMultiturn(t, cfg)
	if len(reqs) != 1 {
		t.Fatalf("expected 1 req, got %d", len(reqs))
	}
	if reqs[0].Messages[0].Role != "user" {
		t.Errorf("with system_tokens=0, first message should be user; got %q",
			reqs[0].Messages[0].Role)
	}
}

func TestMultiturnSessionIDStableAcrossTurns(t *testing.T) {
	cfg := MultiturnConfig{
		Conversations:   4,
		Turns:           5,
		SystemTokens:    10,
		UserTokens:      5,
		AssistantTokens: 7,
		Seed:            42,
	}
	reqs := drainMultiturn(t, cfg)
	if len(reqs) != cfg.Conversations*cfg.Turns {
		t.Fatalf("got %d reqs, want %d", len(reqs), cfg.Conversations*cfg.Turns)
	}
	// Per conv, all turns must share the same SessionID; across convs, the IDs must differ.
	sessionByConv := map[int]string{}
	for _, r := range reqs {
		var j, k int
		fmt.Sscanf(r.Prefix, "c%dt%d", &j, &k)
		if r.SessionID == "" {
			t.Errorf("conv %d turn %d: SessionID empty", j, k)
			continue
		}
		if prev, ok := sessionByConv[j]; ok {
			if prev != r.SessionID {
				t.Errorf("conv %d: SessionID changed across turns: %q vs %q (turn %d)",
					j, prev, r.SessionID, k)
			}
		} else {
			sessionByConv[j] = r.SessionID
		}
	}
	if len(sessionByConv) != cfg.Conversations {
		t.Errorf("expected %d distinct conversations, observed %d", cfg.Conversations, len(sessionByConv))
	}
	// Distinct IDs across conversations.
	seen := map[string]int{}
	for j, sid := range sessionByConv {
		if prevConv, ok := seen[sid]; ok {
			t.Errorf("convs %d and %d share SessionID %q", prevConv, j, sid)
		}
		seen[sid] = j
	}
}

func TestMultiturnSetsPredecessorAndCanonicalIndex(t *testing.T) {
	cfg := MultiturnConfig{
		Conversations:   3,
		Turns:           4,
		SystemTokens:    10,
		UserTokens:      5,
		AssistantTokens: 7,
		Seed:            7,
	}
	reqs := drainMultiturn(t, cfg)
	if len(reqs) != cfg.Conversations*cfg.Turns {
		t.Fatalf("got %d reqs, want %d", len(reqs), cfg.Conversations*cfg.Turns)
	}
	// Build a (conv, turn) -> canonical-index map from the emitted requests.
	type ref struct{ c, t int }
	canonOf := map[ref]int{}
	for i, r := range reqs {
		var j, k int
		fmt.Sscanf(r.Prefix, "c%dt%d", &j, &k)
		if r.CanonicalIndex == nil {
			t.Errorf("req %d (%s) has nil CanonicalIndex", i, r.Prefix)
			continue
		}
		if *r.CanonicalIndex != i {
			t.Errorf("req %d (%s) CanonicalIndex = %d, want %d", i, r.Prefix, *r.CanonicalIndex, i)
		}
		canonOf[ref{j, k}] = i
	}
	// Verify each non-zero turn declares a predecessor on the prior turn of
	// the same conversation, with the correct canonical index.
	for i, r := range reqs {
		var j, k int
		fmt.Sscanf(r.Prefix, "c%dt%d", &j, &k)
		if k == 0 {
			if r.PredecessorIndex != nil {
				t.Errorf("turn 0 req (%s @ %d) should have nil predecessor, got %d",
					r.Prefix, i, *r.PredecessorIndex)
			}
			continue
		}
		if r.PredecessorIndex == nil {
			t.Errorf("turn k>0 req (%s @ %d) should have a predecessor, got nil", r.Prefix, i)
			continue
		}
		wantPred := canonOf[ref{j, k - 1}]
		if *r.PredecessorIndex != wantPred {
			t.Errorf("req %s @ %d: PredecessorIndex = %d, want %d (canonical idx of c%dt%d)",
				r.Prefix, i, *r.PredecessorIndex, wantPred, j, k-1)
		}
		// Predecessor index must be strictly less than this request's index
		// (otherwise the runner could deadlock).
		if *r.PredecessorIndex >= i {
			t.Errorf("req %s @ %d: PredecessorIndex %d not strictly less than self",
				r.Prefix, i, *r.PredecessorIndex)
		}
	}
}

func TestMultiturnTurnSizeSpreadVaries(t *testing.T) {
	// With spread > 0, turn sizes should vary (not all be exactly UserTokens
	// words). We verify by counting whitespace-separated tokens.
	cfg := MultiturnConfig{
		Conversations: 1, Turns: 10, SystemTokens: 0,
		UserTokens: 100, AssistantTokens: 100,
		TurnSizeSpread: 0.5, Seed: 7,
	}
	reqs := drainMultiturn(t, cfg)
	wordCounts := make(map[int]int)
	for _, r := range reqs {
		last := r.Messages[len(r.Messages)-1].Content
		n := 0
		for _, c := range last {
			if c == ' ' {
				n++
			}
		}
		n++ // last word doesn't have a trailing space
		wordCounts[n]++
	}
	if len(wordCounts) < 2 {
		t.Errorf("expected varied turn sizes with spread=0.5; got only %d distinct counts: %v",
			len(wordCounts), wordCounts)
	}
}
