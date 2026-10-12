package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/workload"
)

type Config struct {
	BaseURL string
	APIKey  string
	Model   string
	Timeout time.Duration
	// ChatTemplateKwargs is forwarded verbatim as the top-level
	// `chat_template_kwargs` field in every request body. vLLM passes this
	// through to the model's chat template. Used to disable Qwen3-style
	// thinking via {"enable_thinking": false}.
	ChatTemplateKwargs map[string]any
	// IgnoreEOS, when true, sets `ignore_eos: true` on every request. vLLM
	// extension that keeps generation running to MaxTokens regardless of
	// EOS. Reduces output-length variance on random-word prompts.
	IgnoreEOS bool
	// SendClientHeaders, when true, forwards the workload Request's
	// TenantID as the `l5d-client-id` header. Only workloads that populate
	// TenantID (currently just multiturn) will emit the header; other
	// workloads and requests with an empty TenantID skip it. Used to
	// exercise Linkerd per-client routing / policy against a specific
	// downstream service.
	SendClientHeaders bool
	// OreganoRate, when > 0, appends the word " oregano" to the last
	// user message of a request with probability 1/OreganoRate. Zero
	// (the default) disables it. Used as a low-cost per-request canary
	// marker to verify request-body handling at intermediary routers.
	OreganoRate int
	// KansasRate is identical to OreganoRate but uses the canary word
	// "kansas". Independent coin flip; when both fire the request gets
	// both words appended (in oregano-then-kansas order).
	KansasRate int
}

type Client struct {
	cfg  Config
	http *http.Client
}

func New(cfg Config) *Client {
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Minute
	}
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Timeout: cfg.Timeout,
		},
	}
}

type RequestResult struct {
	TTFT             time.Duration
	ITL              []time.Duration
	E2E              time.Duration
	PromptTokens     int
	CompletionTokens int
	Err              error
}

type chatRequest struct {
	Model              string             `json:"model"`
	Messages           []workload.Message `json:"messages"`
	Stream             bool               `json:"stream"`
	StreamOptions      streamOptions      `json:"stream_options"`
	MaxTokens          int                `json:"max_tokens,omitempty"`
	ChatTemplateKwargs map[string]any     `json:"chat_template_kwargs,omitempty"`
	// IgnoreEOS is a vLLM-specific extension that suppresses EOS-triggered
	// early stops so generation runs to MaxTokens. Useful for benchmarks
	// where you need deterministic output length regardless of prompt
	// content — otherwise random-word prompts often terminate early on
	// random EOS tokens and skew RPS/tps numbers. Omitted from the wire
	// payload when false, so non-vLLM backends aren't affected.
	IgnoreEOS bool `json:"ignore_eos,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

func (c *Client) Do(ctx context.Context, req *workload.Request) RequestResult {
	// Optionally append canary words to the last user message. Independent
	// coin flips per word — a request can pick up neither, either, or both.
	// Copy-on-write: allocate a new Messages slice only when at least one
	// coin flip fires, so the disabled case pays nothing beyond two int
	// compares.
	messages := req.Messages
	var canaries []string
	if c.cfg.OreganoRate > 0 && rand.IntN(c.cfg.OreganoRate) == 0 {
		canaries = append(canaries, "oregano")
	}
	if c.cfg.KansasRate > 0 && rand.IntN(c.cfg.KansasRate) == 0 {
		canaries = append(canaries, "kansas")
	}
	if len(canaries) > 0 && len(messages) > 0 {
		messages = append([]workload.Message(nil), req.Messages...)
		last := &messages[len(messages)-1]
		last.Content = last.Content + " " + strings.Join(canaries, " ")
	}
	body := chatRequest{
		Model:              c.cfg.Model,
		Messages:           messages,
		Stream:             true,
		StreamOptions:      streamOptions{IncludeUsage: true},
		MaxTokens:          req.MaxTokens,
		ChatTemplateKwargs: c.cfg.ChatTemplateKwargs,
		IgnoreEOS:          c.cfg.IgnoreEOS,
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return RequestResult{Err: fmt.Errorf("marshal: %w", err)}
	}

	url := strings.TrimRight(c.cfg.BaseURL, "/") + "/v1/chat/completions"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return RequestResult{Err: fmt.Errorf("build request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if c.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	if req.RequestID != "" {
		httpReq.Header.Set("X-Request-ID", req.RequestID)
	}
	if req.SessionID != "" {
		httpReq.Header.Set("X-Session-ID", req.SessionID)
	}
	if c.cfg.SendClientHeaders && req.TenantID != "" {
		httpReq.Header.Set("l5d-client-id", req.TenantID)
	}

	start := time.Now()
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return RequestResult{Err: fmt.Errorf("send: %w", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return RequestResult{Err: fmt.Errorf("status %d: %s", resp.StatusCode, string(b))}
	}

	result := RequestResult{}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var firstTokenAt time.Time
	var lastTokenAt time.Time

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(line[len("data:"):])
		if len(payload) == 0 {
			continue
		}
		if bytes.Equal(payload, []byte("[DONE]")) {
			break
		}

		var chunk streamChunk
		if err := json.Unmarshal(payload, &chunk); err != nil {
			result.Err = fmt.Errorf("decode chunk: %w", err)
			return result
		}

		now := time.Now()

		if chunk.Usage != nil {
			result.PromptTokens = chunk.Usage.PromptTokens
			result.CompletionTokens = chunk.Usage.CompletionTokens
		}

		for _, choice := range chunk.Choices {
			if choice.Delta.Content == "" {
				continue
			}
			if firstTokenAt.IsZero() {
				firstTokenAt = now
				result.TTFT = now.Sub(start)
			} else {
				result.ITL = append(result.ITL, now.Sub(lastTokenAt))
			}
			lastTokenAt = now
		}
	}
	if err := scanner.Err(); err != nil {
		result.Err = fmt.Errorf("read stream: %w", err)
		return result
	}

	result.E2E = time.Since(start)
	if firstTokenAt.IsZero() && result.Err == nil {
		result.Err = errors.New("no content chunks received")
	}
	return result
}
