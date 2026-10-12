package workload

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"
)

// Mooncake trace record shape (kvcache-ai/Mooncake conversation trace JSONL):
//
//   {"timestamp": <ms>, "input_length": N, "output_length": M, "hash_ids": [int, ...]}
//
// Each hash_id represents a deterministic block of `BlockTokens` tokens.
// Requests that share leading hash_ids share leading prompt text — which is
// exactly what a prefix-aware router can exploit.
type MooncakeRecord struct {
	Timestamp    int64  `json:"timestamp"`
	InputLength  int    `json:"input_length"`
	OutputLength int    `json:"output_length"`
	HashIDs      []int  `json:"hash_ids"`
}

type MooncakeConfig struct {
	Path        string
	BlockTokens int     // default 512
	Speedup     float64 // 1.0 = real time, 10.0 = 10x faster
	Limit       int     // 0 = no limit
	Seed        uint64
}

type Mooncake struct {
	cfg     MooncakeConfig
	records []MooncakeRecord
	t0      int64 // timestamp of first record, in ms

	mu      sync.Mutex
	cursor  int
	blocks  map[int]string // hash_id -> deterministic text
}

func NewMooncake(cfg MooncakeConfig) (*Mooncake, error) {
	if cfg.BlockTokens <= 0 {
		cfg.BlockTokens = 512
	}
	if cfg.Speedup <= 0 {
		cfg.Speedup = 1.0
	}

	f, err := os.Open(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("mooncake: open %s: %w", cfg.Path, err)
	}
	defer f.Close()

	var records []MooncakeRecord
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec MooncakeRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("mooncake: parse line: %w", err)
		}
		records = append(records, rec)
		if cfg.Limit > 0 && len(records) >= cfg.Limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("mooncake: scan: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("mooncake: no records in %s", cfg.Path)
	}

	return &Mooncake{
		cfg:     cfg,
		records: records,
		t0:      records[0].Timestamp,
		blocks:  make(map[int]string),
	}, nil
}

func (m *Mooncake) Name() string { return "mooncake" }

func (m *Mooncake) Total() int { return len(m.records) }

func (m *Mooncake) Next(_ context.Context) (*Request, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cursor >= len(m.records) {
		return nil, false
	}
	rec := m.records[m.cursor]
	m.cursor++

	// Assemble prompt: concat deterministic block text for each hash_id, then a
	// random tail to bring the *token count* close to rec.InputLength.
	var b strings.Builder
	b.Grow(rec.InputLength * 6)
	for i, h := range rec.HashIDs {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(m.blockText(h))
	}

	tail := rec.InputLength - len(rec.HashIDs)*m.cfg.BlockTokens
	if tail > 0 {
		// Deterministic tail keyed off cursor — varies per request, no reuse.
		r := rand.New(rand.NewPCG(m.cfg.Seed, uint64(m.cursor)))
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(generateWords(r, tail))
	}

	delay := time.Duration(float64(rec.Timestamp-m.t0)/m.cfg.Speedup) * time.Millisecond

	maxOut := rec.OutputLength
	if maxOut <= 0 {
		maxOut = 128
	}

	return &Request{
		Messages: []Message{
			{Role: "user", Content: b.String()},
		},
		MaxTokens:  maxOut,
		DelayUntil: delay,
	}, true
}

func (m *Mooncake) blockText(hashID int) string {
	if s, ok := m.blocks[hashID]; ok {
		return s
	}
	r := rand.New(rand.NewPCG(uint64(hashID), 0xC0FFEE))
	s := generateWords(r, m.cfg.BlockTokens)
	m.blocks[hashID] = s
	return s
}
