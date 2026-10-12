package runner

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/metrics"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/workload"
)

// PairProgress is called periodically (rate-limited by the caller) with the
// completed-and-error counts for each side. WallElapsed is time since the
// shared run start.
type PairProgress func(doneA, errsA, doneB, errsB, total int, wallElapsed time.Duration)

type PairConfig struct {
	Concurrency int
	Progress    PairProgress
	// RequestIDPrefix, when non-empty, is used to assign each canonical
	// request an `X-Request-ID` header of the form "<prefix>-<i>" where i
	// is the canonical index. Both endpoints send the same value for a
	// given i, so the IDs cross-reference between server logs.
	RequestIDPrefix string
	// DumpPromptBytes, when > 0, captures the first N bytes of each
	// request's user message into the pair collector so the per-request
	// table can render the prompt for diagnostic spot-checks. Newlines are
	// collapsed to spaces. Disabled when 0.
	DumpPromptBytes int
	// MaxErrors, when > 0, aborts the run as soon as EITHER side has this
	// many errors. Tripping on one side cancels both.
	MaxErrors int
	// MaxErrorRate, when > 0, aborts once either side's error rate
	// (errs/done on that side) reaches this threshold, after at least
	// MinChecksForRate completions on the side being checked.
	MaxErrorRate float64
	// MinChecksForRate is the minimum completed-request count (per side)
	// before MaxErrorRate is evaluated. Defaults to 20 when MaxErrorRate > 0.
	MinChecksForRate int
}

type PairResult struct {
	Wall        time.Duration // max of WallA, WallB (when both sides finished)
	WallA       time.Duration
	WallB       time.Duration
	Total       int
	Aborted     bool
	AbortReason string
}

// RunPair drains the workload once into a canonical request slice, then drives
// two independent worker pools — one per endpoint — through that slice. Each
// pool runs at its own rate; pairing is by canonical index.
func RunPair(ctx context.Context, cfg PairConfig, ca, cb *client.Client, wl workload.Workload, pc *metrics.PairCollector) PairResult {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxErrorRate > 0 && cfg.MinChecksForRate <= 0 {
		cfg.MinChecksForRate = 20
	}

	// Cancelable subcontext so an error-threshold trip on either side
	// stops all workers on both sides.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var abortReasonMu sync.Mutex
	var abortReason string
	trip := func(reason string) {
		abortReasonMu.Lock()
		if abortReason == "" {
			abortReason = reason
		}
		abortReasonMu.Unlock()
		cancel()
	}

	// Materialize the canonical sequence once.
	var reqs []*workload.Request
	for {
		req, ok := wl.Next(ctx)
		if !ok {
			break
		}
		reqs = append(reqs, req)
	}
	total := len(reqs)

	// Propagate workload-supplied per-request prefix labels to the collector
	// and (optionally) assign each canonical index a request ID and capture a
	// prompt snippet. Done before workers start, so no synchronization needed.
	for i, req := range reqs {
		pc.SetPrefix(i, req.Prefix)
		if cfg.RequestIDPrefix != "" {
			req.RequestID = cfg.RequestIDPrefix + "-" + strconv.Itoa(i)
			if req.Prefix != "" {
				req.RequestID += "-" + req.Prefix
			}
			pc.SetRequestID(i, req.RequestID)
		}
		if cfg.DumpPromptBytes > 0 && len(req.Messages) > 0 {
			// Take the last user message — for ShareGPT this is the current
			// turn; for synthetic/Mooncake it's the only message.
			content := req.Messages[len(req.Messages)-1].Content
			if len(content) > cfg.DumpPromptBytes {
				content = content[:cfg.DumpPromptBytes]
			}
			// Collapse whitespace so the table row stays on one line.
			content = strings.Map(func(r rune) rune {
				if r == '\n' || r == '\r' || r == '\t' {
					return ' '
				}
				return r
			}, content)
			pc.SetPrompt(i, content)
		}
	}

	runStart := time.Now()
	var doneA, errsA, doneB, errsB int64

	maybeProgress := func() {
		if cfg.Progress == nil {
			return
		}
		cfg.Progress(
			int(atomic.LoadInt64(&doneA)), int(atomic.LoadInt64(&errsA)),
			int(atomic.LoadInt64(&doneB)), int(atomic.LoadInt64(&errsB)),
			total, time.Since(runStart),
		)
	}

	// Per-side completion channels indexed by canonical request index.
	// Closed (by the worker that finishes the request) when the response has
	// been recorded. Used to honor Request.PredecessorIndex — workers block
	// on the predecessor's channel before sending their request, ensuring
	// server-side cache state matches the request's prompt prefix.
	makeCompletionChans := func() []chan struct{} {
		out := make([]chan struct{}, total)
		for i := range out {
			out[i] = make(chan struct{})
		}
		return out
	}

	runSide := func(side rune, cl *client.Client, donePtr, errsPtr *int64, wallOut *time.Duration) {
		var cursor int64 = -1
		var wg sync.WaitGroup
		sideStart := time.Now()
		doneCh := makeCompletionChans()
		for w := 0; w < cfg.Concurrency; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					idx := atomic.AddInt64(&cursor, 1)
					if idx >= int64(total) {
						return
					}
					req := reqs[idx]
					if req.DelayUntil > 0 {
						target := runStart.Add(req.DelayUntil)
						if d := time.Until(target); d > 0 {
							select {
							case <-time.After(d):
							case <-ctx.Done():
								return
							}
						}
					}
					if req.PredecessorIndex != nil {
						pi := *req.PredecessorIndex
						if pi >= 0 && pi < total {
							select {
							case <-doneCh[pi]:
								// predecessor finished, proceed
							case <-ctx.Done():
								return
							}
						}
					}
					reqStart := time.Now()
					res := cl.Do(ctx, req)
					pc.AddResult(side, int(idx), reqStart, res)
					close(doneCh[idx]) // signal dependents regardless of success/failure

					d := atomic.AddInt64(donePtr, 1)
					e := int64(0)
					if res.Err != nil {
						e = atomic.AddInt64(errsPtr, 1)
					} else {
						e = atomic.LoadInt64(errsPtr)
					}
					maybeProgress()

					// Error-threshold check for this side. First tripper on
					// either side wins the abortReason; subsequent trips are
					// no-ops.
					if cfg.MaxErrors > 0 && e >= int64(cfg.MaxErrors) {
						trip(fmt.Sprintf("side %c: max-errors reached (%d)", side, cfg.MaxErrors))
					} else if cfg.MaxErrorRate > 0 && d >= int64(cfg.MinChecksForRate) &&
						float64(e)/float64(d) >= cfg.MaxErrorRate {
						trip(fmt.Sprintf("side %c: max-error-rate reached (%d/%d = %.1f%% ≥ %.1f%%)",
							side, e, d, 100*float64(e)/float64(d), 100*cfg.MaxErrorRate))
					}

					if ctx.Err() != nil {
						return
					}
				}
			}()
		}
		wg.Wait()
		*wallOut = time.Since(sideStart)
	}

	var wallA, wallB time.Duration
	var outer sync.WaitGroup
	outer.Add(2)
	go func() {
		defer outer.Done()
		runSide('A', ca, &doneA, &errsA, &wallA)
	}()
	go func() {
		defer outer.Done()
		runSide('B', cb, &doneB, &errsB, &wallB)
	}()
	outer.Wait()

	return PairResult{
		Wall:        time.Since(runStart),
		WallA:       wallA,
		WallB:       wallB,
		Total:       total,
		Aborted:     abortReason != "",
		AbortReason: abortReason,
	}
}
