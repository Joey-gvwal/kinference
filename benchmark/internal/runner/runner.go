package runner

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/metrics"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/workload"
)

type Config struct {
	Concurrency int
	Progress    func(done, errs int) // optional, called after each completed request
	// RequestIDPrefix, when non-empty, is used to assign each dispatched
	// request an `X-Request-ID` header of the form "<prefix>-<n>", where n
	// counts up from 0 in send order. Empty disables the header.
	RequestIDPrefix string
	// MaxErrors, when > 0, aborts the run as soon as this many requests have
	// errored. Useful for high-concurrency runs that fall off a KV cliff —
	// stop wasting time instead of waiting for `total-requests` failures.
	MaxErrors int
	// MaxErrorRate, when > 0, aborts the run once the observed error rate
	// (errs/done) reaches this threshold. Requires at least MinChecksForRate
	// completions before the check kicks in, to avoid tripping on the first
	// unlucky request. 0.5 = "abort at 50% failure rate".
	MaxErrorRate float64
	// MinChecksForRate is the minimum completed-request count before
	// MaxErrorRate is evaluated. Defaults to 20 when MaxErrorRate > 0.
	MinChecksForRate int
}

type Result struct {
	Wall        time.Duration
	Aborted     bool
	AbortReason string
}

func Run(ctx context.Context, cfg Config, c *client.Client, wl workload.Workload, mc *metrics.Collector) Result {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxErrorRate > 0 && cfg.MinChecksForRate <= 0 {
		cfg.MinChecksForRate = 20
	}

	// Cancelable subcontext so an error-threshold trip stops all workers.
	// The outer ctx.Done() still propagates (Ctrl-C, timeout, etc).
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

	start := time.Now()
	var wg sync.WaitGroup
	var done, errs int64
	var idCounter int64

	// Per-canonical-index completion channels for workloads that declare
	// per-request dependencies (currently: multiturn). Lazy-allocated via
	// sync.Map; only touched when a request carries CanonicalIndex /
	// PredecessorIndex. See workload.Request comments for rationale.
	var doneCh sync.Map // int -> chan struct{}
	getChan := func(idx int) chan struct{} {
		if v, ok := doneCh.Load(idx); ok {
			return v.(chan struct{})
		}
		ch := make(chan struct{})
		actual, _ := doneCh.LoadOrStore(idx, ch)
		return actual.(chan struct{})
	}

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				req, ok := wl.Next(ctx)
				if !ok {
					return
				}
				if req.DelayUntil > 0 {
					target := start.Add(req.DelayUntil)
					if d := time.Until(target); d > 0 {
						select {
						case <-time.After(d):
						case <-ctx.Done():
							return
						}
					}
				}
				if req.PredecessorIndex != nil {
					select {
					case <-getChan(*req.PredecessorIndex):
						// predecessor finished, proceed
					case <-ctx.Done():
						return
					}
				}

				if cfg.RequestIDPrefix != "" {
					n := atomic.AddInt64(&idCounter, 1) - 1
					req.RequestID = cfg.RequestIDPrefix + "-" + strconv.FormatInt(n, 10)
					if req.Prefix != "" {
						req.RequestID += "-" + req.Prefix
					}
				}

				reqStart := time.Now()
				res := c.Do(ctx, req)
				mc.Add(reqStart, res)
				if req.CanonicalIndex != nil {
					close(getChan(*req.CanonicalIndex))
				}

				d := atomic.AddInt64(&done, 1)
				e := int64(0)
				if res.Err != nil {
					e = atomic.AddInt64(&errs, 1)
				} else {
					e = atomic.LoadInt64(&errs)
				}
				if cfg.Progress != nil {
					cfg.Progress(int(d), int(e))
				}

				// Error-threshold check. First tripper wins the abortReason;
				// subsequent trips are no-ops.
				if cfg.MaxErrors > 0 && e >= int64(cfg.MaxErrors) {
					trip(fmt.Sprintf("max-errors reached (%d)", cfg.MaxErrors))
				} else if cfg.MaxErrorRate > 0 && d >= int64(cfg.MinChecksForRate) &&
					float64(e)/float64(d) >= cfg.MaxErrorRate {
					trip(fmt.Sprintf("max-error-rate reached (%d/%d = %.1f%% ≥ %.1f%%)",
						e, d, 100*float64(e)/float64(d), 100*cfg.MaxErrorRate))
				}

				if ctx.Err() != nil {
					return
				}
			}
		}()
	}

	wg.Wait()
	return Result{
		Wall:        time.Since(start),
		Aborted:     abortReason != "",
		AbortReason: abortReason,
	}
}
