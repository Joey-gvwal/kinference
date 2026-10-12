package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/workload"
)

// warmupVocab is a small inline word list used only to vary the content of
// warmup requests. We deliberately don't reuse the workload package's
// `commonWords` (unexported) since the warmup is independent of any
// measured workload.
var warmupVocab = []string{
	"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
	"india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
	"quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey",
	"xray", "yankee", "zulu", "north", "south", "east", "west",
}

// runWarmup sends n serial warmup requests through cl. Each request is a
// 4-message chat (system / user / assistant / user) with a counter and
// timestamp embedded in the user content so no two warmup requests share a
// cacheable prefix. Returns the first error encountered, or nil on success.
// label is used purely for error attribution.
func runWarmup(ctx context.Context, cl *client.Client, n int, label string) error {
	if n <= 0 {
		return nil
	}
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0xACDC))
	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		token := warmupVocab[rng.IntN(len(warmupVocab))]
		userMsg := fmt.Sprintf("Warmup #%d at %d. Token: %s.", i, time.Now().UnixNano(), token)
		req := &workload.Request{
			Messages: []workload.Message{
				{Role: "system", Content: "You are a helpful assistant being warmed up. Respond briefly."},
				{Role: "user", Content: userMsg},
				{Role: "assistant", Content: "Acknowledged. Ready for the next message."},
				{Role: "user", Content: "Briefly continue."},
			},
			MaxTokens: 32,
			RequestID: fmt.Sprintf("warmup-%d", i),
		}
		res := cl.Do(ctx, req)
		if res.Err != nil {
			return fmt.Errorf("%s warmup #%d: %w", label, i, res.Err)
		}
	}
	return nil
}
