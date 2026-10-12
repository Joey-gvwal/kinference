package workload

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
)

// tenantNames is the fixed roster used to build per-conversation tenant
// identifiers for the l5d-client-id header (see Request.TenantID). 32
// short, distinct first names — conversations beyond the 32nd wrap
// around via (conv_idx % len(tenantNames)) and re-use the same names.
var tenantNames = [...]string{
	"alice", "bob", "carol", "dave", "eve", "frank", "grace", "henry",
	"iris", "jack", "kate", "liam", "mia", "noah", "olivia", "peter",
	"quinn", "ruby", "sam", "tara", "uma", "victor", "wendy", "xander",
	"yara", "zoe", "ada", "ben", "cora", "drew", "ella", "felix",
}

// MultiturnConfig parameterizes a controlled multi-turn workload: M
// concurrent conversations × N turns each, where every turn of a conversation
// extends the prior turn's prompt verbatim with a new (assistant, user) pair.
// All conversations share a common system prompt.
type MultiturnConfig struct {
	Conversations   int
	Turns           int
	SystemTokens    int     // 0 = no system message; >0 = single shared system prompt of this length
	UserTokens      int
	AssistantTokens int
	TurnSizeSpread  float64 // 0 = uniform; e.g. 0.3 = ±30% jitter per-turn
	Seed            uint64
	// ForceColdTurns prepends a per-request 32-word random nonce to the
	// system prompt so vLLM's block-level automatic prefix cache never
	// hits — simulates worst-case non-cache-aware routing where every
	// turn lands on a fresh GPU with no shared prefix in KV.
	ForceColdTurns bool
	// TenantStart shifts the per-conversation tenant-name assignment
	// within the fixed 32-name roster: conversation j maps to
	// tenantNames[(j + TenantStart) % 32]. Lets multiple concurrent
	// bench runs against the same endpoint avoid landing on the same
	// tenant set. Zero (the default) starts at "alice".
	TenantStart int
}

type Multiturn struct {
	cfg          MultiturnConfig
	systemPrompt string
	// userText[j][k] / asstText[j][k] are the strings for conversation j,
	// turn index k (0-based). Shared by reference across requests.
	userText [][]string
	asstText [][]string
	// schedule[i] = (conv, turn) for the i-th request in send order. Random
	// interleave across conversations, with per-conversation turn order
	// preserved.
	schedule []turnRef
	// predIdx[i] = canonical index of the turn that must complete on the
	// server before schedule[i] can be sent (the prior turn of the same
	// conversation); -1 if schedule[i] is turn 0 of its conversation.
	predIdx []int

	mu     sync.Mutex
	cursor int
}

type turnRef struct {
	conv int
	turn int
}

func NewMultiturn(cfg MultiturnConfig) (*Multiturn, error) {
	if cfg.Conversations <= 0 || cfg.Turns <= 0 {
		return nil, fmt.Errorf("multiturn: conversations and turns must be > 0 (got %d, %d)",
			cfg.Conversations, cfg.Turns)
	}
	if cfg.UserTokens <= 0 || cfg.AssistantTokens <= 0 {
		return nil, fmt.Errorf("multiturn: user-tokens and assistant-tokens must be > 0")
	}
	if cfg.TurnSizeSpread < 0 || cfg.TurnSizeSpread >= 1.0 {
		return nil, fmt.Errorf("multiturn: turn-size-spread must be in [0, 1) (got %v)", cfg.TurnSizeSpread)
	}

	// System prompt is one deterministic string shared across all requests.
	var systemPrompt string
	if cfg.SystemTokens > 0 {
		sysR := rand.New(rand.NewPCG(cfg.Seed, 0xA11C0))
		systemPrompt = generateWords(sysR, cfg.SystemTokens)
	}

	// Generate per-conv per-turn content deterministically. Each turn gets
	// its own PCG state derived from (seed, conv, turn) so changing
	// conversation/turn counts doesn't shuffle earlier content.
	userText := make([][]string, cfg.Conversations)
	asstText := make([][]string, cfg.Conversations)
	for j := 0; j < cfg.Conversations; j++ {
		userText[j] = make([]string, cfg.Turns)
		asstText[j] = make([]string, cfg.Turns)
		for k := 0; k < cfg.Turns; k++ {
			turnKey := uint64(j)<<32 | uint64(k)
			userR := rand.New(rand.NewPCG(cfg.Seed^0x55AA55AA, turnKey))
			asstR := rand.New(rand.NewPCG(cfg.Seed^0xCC33CC33, turnKey))

			uTok := cfg.UserTokens
			aTok := cfg.AssistantTokens
			if cfg.TurnSizeSpread > 0 {
				spreadR := rand.New(rand.NewPCG(cfg.Seed^0x33335555, turnKey))
				uTok = jitterCount(uTok, cfg.TurnSizeSpread, spreadR)
				aTok = jitterCount(aTok, cfg.TurnSizeSpread, spreadR)
			}
			userText[j][k] = generateWords(userR, uTok)
			asstText[j][k] = generateWords(asstR, aTok)
		}
	}

	schedule := buildMultiturnSchedule(cfg.Conversations, cfg.Turns, cfg.Seed)

	// Compute per-schedule-index predecessor: for (conv j, turn k>0), the
	// predecessor is the canonical index where (conv j, turn k-1) was emitted.
	// For turn 0 of any conversation, no predecessor (-1).
	predIdx := make([]int, len(schedule))
	lastEmittedFor := make(map[turnRef]int, cfg.Conversations*cfg.Turns)
	for i, ref := range schedule {
		if ref.turn == 0 {
			predIdx[i] = -1
		} else {
			predIdx[i] = lastEmittedFor[turnRef{conv: ref.conv, turn: ref.turn - 1}]
		}
		lastEmittedFor[ref] = i
	}

	return &Multiturn{
		cfg:          cfg,
		systemPrompt: systemPrompt,
		userText:     userText,
		asstText:     asstText,
		schedule:     schedule,
		predIdx:      predIdx,
	}, nil
}

func (m *Multiturn) Name() string { return "multiturn" }

func (m *Multiturn) Total() int { return len(m.schedule) }

func (m *Multiturn) Next(_ context.Context) (*Request, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cursor >= len(m.schedule) {
		return nil, false
	}
	ref := m.schedule[m.cursor]
	m.cursor++

	j, k := ref.conv, ref.turn

	canonIdx := m.cursor - 1

	// Build the messages list:
	//   [system?, user_0, asst_0, user_1, asst_1, ..., user_k]
	// We send all completed prior turns as user/assistant pairs, then the
	// current turn's user message (no assistant — that's what we're asking
	// the model to generate).
	msgs := make([]Message, 0, 1+2*k+1)
	sysContent := m.systemPrompt
	if m.cfg.ForceColdTurns {
		// Prepend a per-request 32-word random nonce so vLLM's block-level
		// automatic prefix cache diverges at block 0 → no downstream blocks
		// match → forced cold prefill for every turn of every conversation.
		nonceR := rand.New(rand.NewPCG(m.cfg.Seed^0xC01DBEEF, uint64(canonIdx)))
		nonce := generateWords(nonceR, 32)
		if sysContent != "" {
			sysContent = nonce + " " + sysContent
		} else {
			sysContent = nonce
		}
	}
	if sysContent != "" {
		msgs = append(msgs, Message{Role: "system", Content: sysContent})
	}
	for i := 0; i < k; i++ {
		msgs = append(msgs, Message{Role: "user", Content: m.userText[j][i]})
		msgs = append(msgs, Message{Role: "assistant", Content: m.asstText[j][i]})
	}
	msgs = append(msgs, Message{Role: "user", Content: m.userText[j][k]})

	req := &Request{
		Messages:       msgs,
		MaxTokens:      m.cfg.AssistantTokens,
		Prefix:         "c" + strconv.Itoa(j) + "t" + strconv.Itoa(k),
		CanonicalIndex: &canonIdx,
		// SessionID is stable across all N turns of a conversation, so the
		// router / server can correlate them. Format mirrors the runner's
		// X-Request-ID convention.
		SessionID: "bench-" + strconv.FormatUint(m.cfg.Seed, 10) + "-c" + strconv.Itoa(j),
		// TenantID is a per-conversation identifier used only when the
		// client's SendClientHeaders flag is on — sent as l5d-client-id
		// so downstream routing/policy can be exercised per-conversation.
		// Conversation index j (plus cfg.TenantStart) picks from a fixed
		// 32-name roster; wraps modulo 32 for larger conversation counts.
		TenantID: "tenant-" + tenantNames[(j+m.cfg.TenantStart)%len(tenantNames)],
	}
	// Per-conversation ordering: the runner must wait for the prior turn of
	// this conversation to complete server-side before sending this one,
	// otherwise turn k+1 races against turn k's cache write and finds nothing
	// extra to hit on.
	if pi := m.predIdx[canonIdx]; pi >= 0 {
		pIdx := pi
		req.PredecessorIndex = &pIdx
	}
	return req, true
}

// jitterCount applies ±spread variation to base, using r for randomness.
// Returns at least 1 token.
func jitterCount(base int, spread float64, r *rand.Rand) int {
	delta := r.Float64()*2 - 1 // [-1, +1)
	v := int(float64(base) * (1.0 + delta*spread))
	if v < 1 {
		v = 1
	}
	return v
}

// buildMultiturnSchedule returns an M*N-long slice of (conv, turn) pairs
// where:
//   - every (j, k) for j in [0, M), k in [0, N) appears exactly once;
//   - for each j, the k values appear in ascending order;
//   - across conversations, the interleaving is uniformly random
//     (subject to the per-conv ordering constraint).
//
// Algorithm: maintain a list of conversations still pending, plus a per-conv
// cursor. At each step, pick a uniformly random pending conv, advance its
// cursor, and emit (conv, cursor-1). Drop a conv from pending when its
// cursor reaches N.
func buildMultiturnSchedule(M, N int, seed uint64) []turnRef {
	r := rand.New(rand.NewPCG(seed, 0x5CEDC1AA))
	cursors := make([]int, M)
	pending := make([]int, M)
	for j := range pending {
		pending[j] = j
	}
	out := make([]turnRef, 0, M*N)
	for len(pending) > 0 {
		idx := r.IntN(len(pending))
		j := pending[idx]
		out = append(out, turnRef{conv: j, turn: cursors[j]})
		cursors[j]++
		if cursors[j] >= N {
			// Swap-remove from pending.
			pending[idx] = pending[len(pending)-1]
			pending = pending[:len(pending)-1]
		}
	}
	return out
}
