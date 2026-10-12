package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/workload"
)

// fakeServer streams a fixed sequence of SSE chunks with a small delay between
// each, then a final usage chunk and [DONE].
func fakeServer(t *testing.T, chunkDelay time.Duration, chunks []string, prompt, comp int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("ResponseWriter does not support flushing")
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", c)
			flusher.Flush()
			time.Sleep(chunkDelay)
		}
		fmt.Fprintf(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":%d,\"completion_tokens\":%d}}\n\n", prompt, comp)
		flusher.Flush()
		fmt.Fprintln(w, "data: [DONE]")
		flusher.Flush()
	}))
}

func TestClientStreamingCapturesMetrics(t *testing.T) {
	srv := fakeServer(t, 20*time.Millisecond, []string{"Hello", " ", "world"}, 7, 3)
	defer srv.Close()

	c := New(Config{BaseURL: srv.URL, Model: "test"})
	res := c.Do(context.Background(), &workload.Request{
		Messages:  []workload.Message{{Role: "user", Content: "hi"}},
		MaxTokens: 10,
	})

	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.PromptTokens != 7 || res.CompletionTokens != 3 {
		t.Errorf("token counts = (%d, %d), want (7, 3)", res.PromptTokens, res.CompletionTokens)
	}
	if res.TTFT <= 0 {
		t.Errorf("TTFT not set: %v", res.TTFT)
	}
	if len(res.ITL) != 2 {
		t.Errorf("ITL count = %d, want 2 (3 content chunks → 2 inter-arrival samples)", len(res.ITL))
	}
	if res.E2E < res.TTFT {
		t.Errorf("E2E %v < TTFT %v", res.E2E, res.TTFT)
	}
}

func TestClientForwardsChatTemplateKwargs(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	c := New(Config{
		BaseURL:            srv.URL,
		Model:              "qwen3",
		ChatTemplateKwargs: map[string]any{"enable_thinking": false},
	})
	res := c.Do(context.Background(), &workload.Request{
		Messages: []workload.Message{{Role: "user", Content: "hi"}},
	})
	if res.Err != nil {
		t.Fatalf("err: %v", res.Err)
	}

	kw, ok := gotBody["chat_template_kwargs"].(map[string]any)
	if !ok {
		t.Fatalf("chat_template_kwargs missing or wrong type: %T %v", gotBody["chat_template_kwargs"], gotBody)
	}
	if v, _ := kw["enable_thinking"].(bool); v {
		t.Errorf("enable_thinking = %v, want false", kw["enable_thinking"])
	}
}

func TestClientOmitsChatTemplateKwargsWhenUnset(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	c := New(Config{BaseURL: srv.URL, Model: "stub"})
	_ = c.Do(context.Background(), &workload.Request{
		Messages: []workload.Message{{Role: "user", Content: "hi"}},
	})

	if _, present := gotBody["chat_template_kwargs"]; present {
		t.Errorf("chat_template_kwargs should be omitted when unset, got %v", gotBody["chat_template_kwargs"])
	}
}

func TestClientForwardsSessionIDHeader(t *testing.T) {
	var gotSession string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSession = r.Header.Get("X-Session-ID")
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL, Model: "stub"})

	// With SessionID set, header should be present.
	_ = c.Do(context.Background(), &workload.Request{
		Messages:  []workload.Message{{Role: "user", Content: "hi"}},
		SessionID: "bench-42-c7",
	})
	if gotSession != "bench-42-c7" {
		t.Errorf("X-Session-ID = %q, want %q", gotSession, "bench-42-c7")
	}

	// Without it, header should be absent.
	gotSession = ""
	_ = c.Do(context.Background(), &workload.Request{
		Messages: []workload.Message{{Role: "user", Content: "hi"}},
	})
	if gotSession != "" {
		t.Errorf("X-Session-ID should be absent when SessionID empty; got %q", gotSession)
	}
}

func TestClientForwardsRequestIDHeader(t *testing.T) {
	var gotID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = r.Header.Get("X-Request-ID")
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	c := New(Config{BaseURL: srv.URL, Model: "stub"})

	// With RequestID set, header should be present.
	res := c.Do(context.Background(), &workload.Request{
		Messages:  []workload.Message{{Role: "user", Content: "hi"}},
		RequestID: "bench-42-7",
	})
	if res.Err != nil {
		t.Fatalf("err: %v", res.Err)
	}
	if gotID != "bench-42-7" {
		t.Errorf("X-Request-ID = %q, want %q", gotID, "bench-42-7")
	}

	// With RequestID empty, header should be absent (gotID stays "").
	gotID = ""
	_ = c.Do(context.Background(), &workload.Request{
		Messages: []workload.Message{{Role: "user", Content: "hi"}},
	})
	if gotID != "" {
		t.Errorf("X-Request-ID should be absent when RequestID empty; got %q", gotID)
	}
}

func TestClientReturnsServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "model not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(Config{BaseURL: srv.URL, Model: "test"})
	res := c.Do(context.Background(), &workload.Request{
		Messages: []workload.Message{{Role: "user", Content: "hi"}},
	})
	if res.Err == nil || !strings.Contains(res.Err.Error(), "404") {
		t.Errorf("want 404 error, got %v", res.Err)
	}
}
