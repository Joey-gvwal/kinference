package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/buoyantio/claude-model-monitor/benchmark/internal/client"
)

func TestRunWarmupSendsRequestsWithVaryingContent(t *testing.T) {
	type observed struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		RequestID string `json:"-"`
	}
	var mu sync.Mutex
	var observations []observed
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var o observed
		_ = json.Unmarshal(body, &o)
		o.RequestID = r.Header.Get("X-Request-ID")
		mu.Lock()
		observations = append(observations, o)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
		f.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	defer srv.Close()

	cl := client.New(client.Config{BaseURL: srv.URL, Model: "stub"})
	if err := runWarmup(context.Background(), cl, 5, "A"); err != nil {
		t.Fatalf("runWarmup: %v", err)
	}
	if len(observations) != 5 {
		t.Fatalf("expected 5 warmup requests, got %d", len(observations))
	}
	// Every request should have 4 messages, with roles in the expected order.
	wantRoles := []string{"system", "user", "assistant", "user"}
	for i, o := range observations {
		if len(o.Messages) != 4 {
			t.Errorf("req %d: got %d messages, want 4", i, len(o.Messages))
			continue
		}
		for j, want := range wantRoles {
			if o.Messages[j].Role != want {
				t.Errorf("req %d msg %d: role=%q, want %q", i, j, o.Messages[j].Role, want)
			}
		}
	}
	// User-message #1 (the second message, which embeds the counter + ts) must
	// be unique across all 5 requests.
	seen := map[string]bool{}
	for i, o := range observations {
		if len(o.Messages) < 2 {
			continue
		}
		content := o.Messages[1].Content
		if seen[content] {
			t.Errorf("req %d: duplicate user content %q seen across warmup requests", i, content)
		}
		seen[content] = true
	}
	if len(seen) != 5 {
		t.Errorf("expected 5 distinct user contents, got %d", len(seen))
	}

	// Request IDs must be the conventional warmup-<i> sequence in send order.
	for i, o := range observations {
		want := fmt.Sprintf("warmup-%d", i)
		if o.RequestID != want {
			t.Errorf("req %d: X-Request-ID = %q, want %q", i, o.RequestID, want)
		}
	}
}

func TestRunWarmupZeroIsNoOp(t *testing.T) {
	called := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	cl := client.New(client.Config{BaseURL: srv.URL, Model: "stub"})
	if err := runWarmup(context.Background(), cl, 0, "A"); err != nil {
		t.Fatalf("runWarmup(n=0): %v", err)
	}
	if called != 0 {
		t.Errorf("expected 0 requests, got %d", called)
	}
}

func TestRunWarmupPropagatesServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "model not found", http.StatusBadRequest)
	}))
	defer srv.Close()
	cl := client.New(client.Config{BaseURL: srv.URL, Model: "stub"})
	err := runWarmup(context.Background(), cl, 3, "A")
	if err == nil {
		t.Fatal("expected error from failing warmup, got nil")
	}
}
