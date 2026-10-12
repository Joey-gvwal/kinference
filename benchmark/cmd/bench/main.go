package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/metrics"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/runner"
	"github.com/buoyantio/claude-model-monitor/benchmark/internal/workload"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)

	baseURL := fs.String("base-url", "http://localhost:8000", "OpenAI-compatible endpoint base URL")
	apiKey := fs.String("api-key", os.Getenv("OPENAI_API_KEY"), "bearer token (defaults to $OPENAI_API_KEY)")
	baseURLB := fs.String("base-url-b", "", "second endpoint URL; setting this activates pairwise mode")
	apiKeyB := fs.String("api-key-b", "", "bearer token for endpoint B (defaults to --api-key)")
	labelA := fs.String("label-a", "A", "label for endpoint A in pairwise reports")
	labelB := fs.String("label-b", "B", "label for endpoint B in pairwise reports")
	model := fs.String("model", "", "model name to send in the request")
	wlName := fs.String("workload", "synthetic", "synthetic|mooncake|sharegpt|multiturn")
	concurrency := fs.Int("concurrency", 16, "number of concurrent workers")
	totalRequests := fs.Int("total-requests", 200, "total requests (synthetic only)")
	output := fs.String("output", "", "write JSON report to this path (optional)")
	timeout := fs.Duration("timeout", 5*time.Minute, "per-request timeout")
	// Default seed is the wall-clock at flag-parse time, so back-to-back runs
	// pick different prefix pools / tails / shuffle orders. Pass --seed N
	// explicitly to reproduce a specific run; the chosen seed is logged at
	// startup either way.
	seed := fs.Uint64("seed", uint64(time.Now().UnixNano()), "random seed (default: current time in ns; pass explicitly for reproducibility)")
	noThink := fs.Bool("no-think", false, "for Qwen3-style models: send chat_template_kwargs={enable_thinking:false} on every request, suppressing reasoning output. Recommended for routing benchmarks.")
	ignoreEOS := fs.Bool("ignore-eos", true, "vLLM extension: send ignore_eos:true on every request so generation runs to --max-output-tokens regardless of EOS. Reduces output-length variance on random-word prompts (which otherwise EOS early and skew RPS/tps numbers). Set to false to let the model stop naturally.")
	sendClientHeaders := fs.Int("send-client-headers", -1, "for multiturn: attach an `l5d-client-id: tenant-<name>` header to each request so Linkerd routes per-conversation. Argument is the starting offset within a fixed 32-name roster (alice, bob, carol, …): `--send-client-headers 0` gives conv 0 = alice, conv 1 = bob, …; `--send-client-headers 1` gives conv 0 = bob, conv 1 = carol, …. Every turn of a given conversation gets the same tenant. Negative (default) disables the header entirely.")
	oreganoRate := fs.Int("oregano-rate", 0, "append the canary word \" oregano\" to the last user message with probability 1/N. E.g. --oregano-rate 10 hits roughly 10% of requests, --oregano-rate 1 hits every request. 0 (the default) disables it.")
	kansasRate := fs.Int("kansas-rate", 0, "same as --oregano-rate but appends the word \" kansas\". Independent coin flip; when both fire the request gets both words. 0 (the default) disables it.")
	dumpPrompt := fs.Int("dump-prompt", 0, "pairwise mode: append the first N bytes of each request's prompt as the final column of the per-request TTFT table (0 disables; e.g. 200)")
	tiedMS := fs.Int("tied-ms", metrics.DefaultTiedToleranceMS, "pairwise mode: |delta| ≤ this is considered a tied (noise) result; affects the win/tie/loss counter and the histogram's tied bin")
	dumpPrompts := fs.String("dump-prompts", "", "if set, write the full materialized request sequence as JSONL to this path before running (one record per request with messages, prefix label, predecessor index, etc.). Useful for verifying multi-turn prompt growth.")
	warmupReqs := fs.Int("warmup-requests", 0, "send N warmup requests per endpoint before the measured run begins; each is a 4-message chat (system/user/assistant/user) with a unique counter+timestamp in the content so no two warmups share a cacheable prefix. Default: 0 (no warmup).")
	maxErrors := fs.Int("max-errors", 0, "abort the run once this many requests have errored (0 = disabled). In pairwise mode, the check applies per-side — either side tripping cancels both.")
	maxErrorRate := fs.Float64("max-error-rate", 0, "abort the run once the observed error rate reaches this fraction (0 = disabled). Only evaluated after --min-checks-for-rate completions to avoid tripping on early flukes. E.g. 0.5 = abort at 50% failure rate.")
	minChecksForRate := fs.Int("min-checks-for-rate", 20, "minimum completed requests before --max-error-rate is evaluated. Guards against tripping on the first few requests.")

	// synthetic
	prefixCount := fs.Int("prefix-count", 4, "synthetic: number of shared prefixes")
	prefixTokens := fs.Int("prefix-tokens", 1024, "synthetic: tokens per prefix")
	tailTokens := fs.Int("tail-tokens", 64, "synthetic: random tail tokens per request")
	zipfS := fs.Float64("prefix-zipf", 0, "synthetic: Zipf skew over prefixes (>1 enables; 0 = uniform)")
	maxOut := fs.Int("max-output-tokens", 128, "synthetic/sharegpt: max output tokens")

	// mooncake
	mooncakePath := fs.String("mooncake-trace", "", "mooncake: path to conversation_trace.jsonl")
	mooncakeBlock := fs.Int("mooncake-block-tokens", 512, "mooncake: tokens per hash_id block")
	mooncakeSpeedup := fs.Float64("mooncake-speedup", 1.0, "mooncake: replay speedup factor")
	mooncakeLimit := fs.Int("mooncake-limit", 0, "mooncake: max records to load (0 = all)")

	// sharegpt
	shareGPTPath := fs.String("sharegpt-path", "", "sharegpt: path to ShareGPT JSON")
	maxConvos := fs.Int("max-conversations", 0, "sharegpt: max conversations (0 = all)")
	maxTurnsPerConvo := fs.Int("max-turns-per-convo", 0, "sharegpt: max turns per conversation (0 = all)")

	// multiturn
	mtConvos := fs.Int("mt-conversations", 10, "multiturn: number of independent conversations")
	mtTurns := fs.Int("mt-turns", 5, "multiturn: turns per conversation")
	mtSystemToks := fs.Int("mt-system-tokens", 256, "multiturn: tokens in the shared system prompt (0 = no system message)")
	mtUserToks := fs.Int("mt-user-tokens", 100, "multiturn: tokens per user turn")
	mtAsstToks := fs.Int("mt-assistant-tokens", 200, "multiturn: tokens per simulated assistant turn (also used as max_tokens for the request)")
	mtSpread := fs.Float64("mt-turn-size-spread", 0, "multiturn: ±fractional jitter on per-turn sizes (0 = uniform; 0.3 = ±30%)")
	mtForceCold := fs.Bool("mt-force-cold-turns", false, "multiturn: prepend a per-request 32-word random nonce to the system prompt so vLLM's block-level automatic prefix cache never hits. Simulates worst-case non-cache-aware routing where every turn lands on a fresh GPU.")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	if *model == "" {
		return fmt.Errorf("--model is required")
	}

	// Log the full effective argument set so any captured log can reproduce
	// the run. Defaults (including the wall-clock seed) are included; the
	// api-key fields are redacted.
	fmt.Fprintf(os.Stderr, "Args: %s\n", formatArgs(fs))

	buildArgs := workloadArgs{
		TotalRequests:     *totalRequests,
		Seed:              *seed,
		MaxOutputTokens:   *maxOut,
		PrefixCount:       *prefixCount,
		PrefixTokens:      *prefixTokens,
		TailTokens:        *tailTokens,
		ZipfS:             *zipfS,
		MooncakePath:      *mooncakePath,
		MooncakeBlock:     *mooncakeBlock,
		MooncakeSpeedup:   *mooncakeSpeedup,
		MooncakeLimit:     *mooncakeLimit,
		ShareGPTPath:      *shareGPTPath,
		MaxConvos:         *maxConvos,
		MaxTurnsPerConvo:  *maxTurnsPerConvo,
		MTConvos:          *mtConvos,
		MTTurns:           *mtTurns,
		MTSystemTokens:    *mtSystemToks,
		MTUserTokens:      *mtUserToks,
		MTAssistantTokens: *mtAsstToks,
		MTTurnSizeSpread:  *mtSpread,
		MTForceColdTurns:  *mtForceCold,
		// A negative --send-client-headers value disables the header;
		// treat that as offset 0 for the workload's tenant computation
		// (harmless — the workload still sets TenantID but the client
		// won't emit the header).
		MTTenantStart: max(0, *sendClientHeaders),
	}
	wl, err := buildWorkload(*wlName, buildArgs)
	if err != nil {
		return err
	}

	if *dumpPrompts != "" {
		ctxDump := context.Background()
		var reqs []*workload.Request
		for {
			r, ok := wl.Next(ctxDump)
			if !ok {
				break
			}
			reqs = append(reqs, r)
		}
		if err := writePromptDump(*dumpPrompts, reqs); err != nil {
			return fmt.Errorf("write prompt dump: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Wrote %d requests to %s\n", len(reqs), *dumpPrompts)
		// The workload is exhausted; rebuild it for the actual run (cheap for
		// deterministic generators, re-reads the input file for trace replays).
		wl, err = buildWorkload(*wlName, buildArgs)
		if err != nil {
			return err
		}
	}

	var ctk map[string]any
	if *noThink {
		ctk = map[string]any{"enable_thinking": false}
	}

	clA := client.New(client.Config{
		BaseURL:            *baseURL,
		APIKey:             *apiKey,
		Model:              *model,
		Timeout:            *timeout,
		ChatTemplateKwargs: ctk,
		IgnoreEOS:          *ignoreEOS,
		SendClientHeaders:  *sendClientHeaders >= 0,
		OreganoRate:        *oreganoRate,
		KansasRate:         *kansasRate,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Detect TTY on stderr: when interactive, pin progress to one line with
	// \r; when piped to a file or CI log, fall back to one line per update
	// so the captured output is readable.
	isTTY := false
	if fi, err := os.Stderr.Stat(); err == nil {
		isTTY = (fi.Mode() & os.ModeCharDevice) != 0
	}

	if *baseURLB != "" {
		bKey := *apiKeyB
		if bKey == "" {
			bKey = *apiKey
		}
		clB := client.New(client.Config{
			BaseURL:            *baseURLB,
			APIKey:             bKey,
			Model:              *model,
			Timeout:            *timeout,
			ChatTemplateKwargs: ctk,
			IgnoreEOS:          *ignoreEOS,
			SendClientHeaders:  *sendClientHeaders >= 0,
		})
		if err := warmupAll(ctx, *warmupReqs, *labelA, *labelB, clA, clB); err != nil {
			return err
		}
		return runPairwise(ctx, wl, clA, clB, *concurrency, *seed, *baseURL, *baseURLB,
			*labelA, *labelB, *model, *output, *dumpPrompt, *tiedMS, isTTY,
			*maxErrors, *maxErrorRate, *minChecksForRate)
	}

	if err := warmupAll(ctx, *warmupReqs, *labelA, "", clA, nil); err != nil {
		return err
	}
	return runSingle(ctx, wl, clA, *concurrency, *seed, *baseURL, *model, *output, isTTY,
		*maxErrors, *maxErrorRate, *minChecksForRate)
}

// warmupAll runs warmup requests on each endpoint. In pairwise mode both
// endpoints warm up concurrently (each side serial within itself); in single
// mode only clA is warmed up. Logs progress and total elapsed to stderr.
// Returns the first error from either side.
func warmupAll(ctx context.Context, n int, labelA, labelB string, clA, clB *client.Client) error {
	if n <= 0 {
		return nil
	}
	endpoints := 1
	if clB != nil {
		endpoints = 2
	}
	fmt.Fprintf(os.Stderr, "Warmup: %d requests per endpoint × %d endpoint(s)...\n", n, endpoints)
	start := time.Now()
	if clB == nil {
		if err := runWarmup(ctx, clA, n, labelA); err != nil {
			return err
		}
	} else {
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := runWarmup(ctx, clA, n, labelA); err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			if err := runWarmup(ctx, clB, n, labelB); err != nil {
				errs <- err
			}
		}()
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				return err
			}
		}
	}
	fmt.Fprintf(os.Stderr, "Warmup complete in %s\n", time.Since(start).Truncate(time.Millisecond))
	return nil
}

func runSingle(ctx context.Context, wl workload.Workload, cl *client.Client,
	concurrency int, seed uint64, baseURL, model, output string, isTTY bool,
	maxErrors int, maxErrorRate float64, minChecksForRate int) error {
	mc := metrics.NewCollector()
	fmt.Fprintf(os.Stderr, "Running workload=%s concurrency=%d seed=%d against %s (model=%s)\n",
		wl.Name(), concurrency, seed, baseURL, model)

	total := wl.Total()
	runStart := time.Now()
	lastReport := runStart
	progress := func(done, errs int) {
		if time.Since(lastReport) < 2*time.Second {
			return
		}
		line := formatProgress(time.Now(), runStart, done, total, errs)
		if isTTY {
			fmt.Fprintf(os.Stderr, "\r%s        ", line)
		} else {
			fmt.Fprintln(os.Stderr, line)
		}
		lastReport = time.Now()
	}

	res := runner.Run(ctx, runner.Config{
		Concurrency:      concurrency,
		Progress:         progress,
		RequestIDPrefix:  fmt.Sprintf("bench-%d", seed),
		MaxErrors:        maxErrors,
		MaxErrorRate:     maxErrorRate,
		MinChecksForRate: minChecksForRate,
	}, cl, wl, mc)
	if res.Aborted {
		fmt.Fprintf(os.Stderr, "Run aborted early: %s\n", res.AbortReason)
	}
	if isTTY {
		fmt.Fprintln(os.Stderr)
	}

	summary := metrics.Summarize(mc.Records(), res.Wall)
	summary.Workload = wl.Name()
	summary.Endpoint = baseURL
	summary.Model = model
	summary.Concurrency = concurrency

	metrics.WriteText(os.Stdout, summary)

	if output != "" {
		if err := metrics.WriteJSON(output, summary); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Wrote %s\n", output)
	}

	if summary.FailedRequests > 0 {
		return fmt.Errorf("%d/%d requests failed", summary.FailedRequests, summary.TotalRequests)
	}
	return nil
}

func runPairwise(ctx context.Context, wl workload.Workload, clA, clB *client.Client,
	concurrency int, seed uint64, urlA, urlB, labelA, labelB, model, output string,
	dumpPromptBytes, tiedTolMS int, isTTY bool,
	maxErrors int, maxErrorRate float64, minChecksForRate int) error {
	fmt.Fprintf(os.Stderr, "Running workload=%s concurrency=%d seed=%d pairwise:\n",
		wl.Name(), concurrency, seed)
	fmt.Fprintf(os.Stderr, "  %s = %s\n", labelA, urlA)
	fmt.Fprintf(os.Stderr, "  %s = %s\n", labelB, urlB)
	fmt.Fprintf(os.Stderr, "  model = %s\n", model)

	pc := metrics.NewPairCollector(wl.Total())

	lastReport := time.Now()
	progress := func(doneA, errsA, doneB, errsB, total int, elapsed time.Duration) {
		if time.Since(lastReport) < 2*time.Second {
			return
		}
		line := formatPairProgress(elapsed, doneA, errsA, doneB, errsB, total, labelA, labelB)
		if isTTY {
			fmt.Fprintf(os.Stderr, "\r%s        ", line)
		} else {
			fmt.Fprintln(os.Stderr, line)
		}
		lastReport = time.Now()
	}

	res := runner.RunPair(ctx, runner.PairConfig{
		Concurrency:      concurrency,
		Progress:         progress,
		RequestIDPrefix:  fmt.Sprintf("bench-%d", seed),
		DumpPromptBytes:  dumpPromptBytes,
		MaxErrors:        maxErrors,
		MaxErrorRate:     maxErrorRate,
		MinChecksForRate: minChecksForRate,
	}, clA, clB, wl, pc)
	if isTTY {
		fmt.Fprintln(os.Stderr)
	}
	if res.Aborted {
		fmt.Fprintf(os.Stderr, "Run aborted early: %s\n", res.AbortReason)
	}

	report := metrics.SummarizePairwise(pc, res.WallA, res.WallB, labelA, labelB, tiedTolMS)
	// Populate per-endpoint metadata.
	report.Workload = wl.Name()
	report.Model = model
	report.Concurrency = concurrency
	report.EndpointA.Workload = wl.Name()
	report.EndpointA.Endpoint = urlA
	report.EndpointA.Model = model
	report.EndpointA.Concurrency = concurrency
	report.EndpointB.Workload = wl.Name()
	report.EndpointB.Endpoint = urlB
	report.EndpointB.Model = model
	report.EndpointB.Concurrency = concurrency

	metrics.WritePairwiseText(os.Stdout, report)

	if output != "" {
		if err := metrics.WritePairwiseJSON(output, report); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Wrote %s\n", output)
	}

	if report.EndpointA.FailedRequests+report.EndpointB.FailedRequests > 0 {
		return fmt.Errorf("%s: %d/%d failed, %s: %d/%d failed",
			labelA, report.EndpointA.FailedRequests, report.EndpointA.TotalRequests,
			labelB, report.EndpointB.FailedRequests, report.EndpointB.TotalRequests)
	}
	return nil
}

// formatPairProgress renders a single-line progress update for pairwise runs,
// showing both endpoints independently plus an ETA based on the slower side.
func formatPairProgress(elapsed time.Duration, doneA, errsA, doneB, errsB, total int,
	labelA, labelB string) string {
	el := elapsed.Truncate(time.Second)
	var etaStr string
	if total > 0 {
		slowest := doneA
		if doneB < slowest {
			slowest = doneB
		}
		if slowest > 0 && slowest < total {
			eta := time.Duration(float64(elapsed) * float64(total-slowest) / float64(slowest))
			etaStr = fmt.Sprintf("  eta %s", eta.Truncate(time.Second))
		}
	}
	if total > 0 {
		return fmt.Sprintf("[%s] %s: %d/%d errs=%d | %s: %d/%d errs=%d%s",
			el, labelA, doneA, total, errsA, labelB, doneB, total, errsB, etaStr)
	}
	return fmt.Sprintf("[%s] %s: %d errs=%d | %s: %d errs=%d",
		el, labelA, doneA, errsA, labelB, doneB, errsB)
}

// formatProgress renders a single progress line. The leading bracket shows
// elapsed run time (truncated to whole seconds, e.g. "12s", "3m4s", "1h2m3s").
// When the workload's total is known and at least one request has finished,
// the line also includes percent-done and a linear-extrapolation ETA.
func formatProgress(now, runStart time.Time, done, total, errs int) string {
	elapsed := now.Sub(runStart).Truncate(time.Second)
	if total <= 0 || done <= 0 {
		return fmt.Sprintf("[%s] %d done (%d errors)", elapsed, done, errs)
	}
	pct := float64(done) * 100 / float64(total)
	if pct > 100 {
		pct = 100
	}
	etaStr := ""
	if done < total {
		eta := time.Duration(float64(now.Sub(runStart)) * float64(total-done) / float64(done))
		etaStr = fmt.Sprintf(" eta %s", eta.Truncate(time.Second))
	}
	return fmt.Sprintf("[%s] %d/%d (%.0f%%) done (%d errors)%s",
		elapsed, done, total, pct, errs, etaStr)
}

// formatArgs reconstructs a copy-pasteable "bench --k=v ..." command from
// the parsed flag set. Emits every flag's effective value (defaults
// included) so a run's log alone is enough to reproduce it. Redacts
// api-key fields and omits empty strings for optional path flags.
func formatArgs(fs *flag.FlagSet) string {
	var flags []*flag.Flag
	fs.VisitAll(func(f *flag.Flag) { flags = append(flags, f) })
	sort.Slice(flags, func(i, j int) bool { return flags[i].Name < flags[j].Name })
	parts := []string{"bench"}
	for _, f := range flags {
		val := f.Value.String()
		switch f.Name {
		case "api-key", "api-key-b":
			if val != "" {
				val = "<redacted>"
			}
		}
		// Skip empty optional strings — they're not meaningful to include.
		if val == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("--%s=%s", f.Name, shellQuoteIfNeeded(val)))
	}
	return strings.Join(parts, " ")
}

// shellQuoteIfNeeded wraps s in single quotes if it contains any character
// a shell might interpret specially. Keeps the reproducer safe to paste.
func shellQuoteIfNeeded(s string) string {
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '"' || r == '\'' || r == '$' ||
			r == '`' || r == '\\' || r == '&' || r == '|' || r == ';' ||
			r == '<' || r == '>' || r == '(' || r == ')' || r == '*' ||
			r == '?' || r == '[' || r == ']' || r == '{' || r == '}' ||
			r == '#' || r == '!' {
			return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
		}
	}
	return s
}

type workloadArgs struct {
	TotalRequests     int
	Seed              uint64
	MaxOutputTokens   int
	PrefixCount       int
	PrefixTokens      int
	TailTokens        int
	ZipfS             float64
	MooncakePath      string
	MooncakeBlock     int
	MooncakeSpeedup   float64
	MooncakeLimit     int
	ShareGPTPath      string
	MaxConvos         int
	MaxTurnsPerConvo  int
	MTConvos          int
	MTTurns           int
	MTSystemTokens    int
	MTUserTokens      int
	MTAssistantTokens int
	MTTurnSizeSpread  float64
	MTForceColdTurns  bool
	MTTenantStart     int
}

func buildWorkload(name string, a workloadArgs) (workload.Workload, error) {
	switch name {
	case "synthetic":
		return workload.NewSynthetic(workload.SyntheticConfig{
			PrefixCount:   a.PrefixCount,
			PrefixTokens:  a.PrefixTokens,
			TailTokens:    a.TailTokens,
			TotalRequests: a.TotalRequests,
			MaxOutputToks: a.MaxOutputTokens,
			ZipfS:         a.ZipfS,
			Seed:          a.Seed,
		})
	case "mooncake":
		if a.MooncakePath == "" {
			return nil, fmt.Errorf("--mooncake-trace is required for workload=mooncake")
		}
		return workload.NewMooncake(workload.MooncakeConfig{
			Path:        a.MooncakePath,
			BlockTokens: a.MooncakeBlock,
			Speedup:     a.MooncakeSpeedup,
			Limit:       a.MooncakeLimit,
			Seed:        a.Seed,
		})
	case "sharegpt":
		if a.ShareGPTPath == "" {
			return nil, fmt.Errorf("--sharegpt-path is required for workload=sharegpt")
		}
		return workload.NewShareGPT(workload.ShareGPTConfig{
			Path:             a.ShareGPTPath,
			MaxConversations: a.MaxConvos,
			MaxTurnsPerConvo: a.MaxTurnsPerConvo,
			MaxOutputToks:    a.MaxOutputTokens,
			Seed:             a.Seed,
		})
	case "multiturn":
		return workload.NewMultiturn(workload.MultiturnConfig{
			Conversations:   a.MTConvos,
			Turns:           a.MTTurns,
			SystemTokens:    a.MTSystemTokens,
			UserTokens:      a.MTUserTokens,
			AssistantTokens: a.MTAssistantTokens,
			TurnSizeSpread:  a.MTTurnSizeSpread,
			Seed:            a.Seed,
			ForceColdTurns:  a.MTForceColdTurns,
			TenantStart:     a.MTTenantStart,
		})
	default:
		return nil, fmt.Errorf("unknown workload %q", name)
	}
}

// promptDumpRecord is the per-request JSON shape written by --dump-prompts.
// Mirrors workload.Request but with stable, snake_case field names so the
// file is easy to inspect with jq.
type promptDumpRecord struct {
	CanonicalIndex   *int         `json:"canonical_index,omitempty"`
	Prefix           string       `json:"prefix,omitempty"`
	PredecessorIndex *int         `json:"predecessor_index,omitempty"`
	MaxTokens        int          `json:"max_tokens"`
	DelayUntilMS     int64        `json:"delay_until_ms,omitempty"`
	Messages         []dumpRoleMsg `json:"messages"`
}

type dumpRoleMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func writePromptDump(path string, reqs []*workload.Request) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, r := range reqs {
		msgs := make([]dumpRoleMsg, len(r.Messages))
		for i, m := range r.Messages {
			msgs[i] = dumpRoleMsg{Role: m.Role, Content: m.Content}
		}
		rec := promptDumpRecord{
			CanonicalIndex:   r.CanonicalIndex,
			Prefix:           r.Prefix,
			PredecessorIndex: r.PredecessorIndex,
			MaxTokens:        r.MaxTokens,
			DelayUntilMS:     r.DelayUntil.Milliseconds(),
			Messages:         msgs,
		}
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	return nil
}
