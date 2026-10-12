package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
)

type shareGPTConversation struct {
	ID            string            `json:"id"`
	Conversations []shareGPTTurn    `json:"conversations"`
}

type shareGPTTurn struct {
	From  string `json:"from"`
	Value string `json:"value"`
}

type ShareGPTConfig struct {
	Path             string
	MaxConversations int
	MaxTurnsPerConvo int
	MaxOutputToks    int
	Seed             uint64
}

type ShareGPT struct {
	cfg      ShareGPTConfig
	requests []*Request

	mu     sync.Mutex
	cursor int
}

func NewShareGPT(cfg ShareGPTConfig) (*ShareGPT, error) {
	if cfg.MaxOutputToks <= 0 {
		cfg.MaxOutputToks = 256
	}

	f, err := os.Open(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("sharegpt: open %s: %w", cfg.Path, err)
	}
	defer f.Close()

	var convos []shareGPTConversation
	dec := json.NewDecoder(f)
	if err := dec.Decode(&convos); err != nil {
		return nil, fmt.Errorf("sharegpt: decode: %w", err)
	}

	r := rand.New(rand.NewPCG(cfg.Seed, 1))
	r.Shuffle(len(convos), func(i, j int) { convos[i], convos[j] = convos[j], convos[i] })

	if cfg.MaxConversations > 0 && len(convos) > cfg.MaxConversations {
		convos = convos[:cfg.MaxConversations]
	}

	var reqs []*Request
	for _, c := range convos {
		var history []Message
		turns := 0
		for _, t := range c.Conversations {
			role := mapRole(t.From)
			if role == "" {
				continue
			}
			history = append(history, Message{Role: role, Content: t.Value})

			// Emit a request after each *user* turn — the prompt is the
			// growing history up to and including that turn.
			if role == "user" {
				msgs := make([]Message, len(history))
				copy(msgs, history)
				reqs = append(reqs, &Request{
					Messages:  msgs,
					MaxTokens: cfg.MaxOutputToks,
				})
				turns++
				if cfg.MaxTurnsPerConvo > 0 && turns >= cfg.MaxTurnsPerConvo {
					break
				}
			}
		}
	}

	if len(reqs) == 0 {
		return nil, fmt.Errorf("sharegpt: no user turns in %s", cfg.Path)
	}

	return &ShareGPT{cfg: cfg, requests: reqs}, nil
}

func (s *ShareGPT) Name() string { return "sharegpt" }

func (s *ShareGPT) Total() int { return len(s.requests) }

func (s *ShareGPT) Next(_ context.Context) (*Request, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cursor >= len(s.requests) {
		return nil, false
	}
	r := s.requests[s.cursor]
	s.cursor++
	return r, true
}

func mapRole(from string) string {
	switch from {
	case "human", "user":
		return "user"
	case "gpt", "assistant", "chatgpt", "bing", "bard":
		return "assistant"
	case "system":
		return "system"
	default:
		return ""
	}
}
