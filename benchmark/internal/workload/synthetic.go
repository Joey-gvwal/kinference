package workload

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
)

type SyntheticConfig struct {
	PrefixCount    int
	PrefixTokens   int
	TailTokens     int
	TotalRequests  int
	MaxOutputToks  int
	ZipfS          float64 // 0 = uniform; >1 = skewed toward prefix 0
	Seed           uint64
}

type Synthetic struct {
	cfg      SyntheticConfig
	prefixes []string

	mu       sync.Mutex
	emitted  int
	pickRand *rand.Rand
	tailRand *rand.Rand
	zipf     *rand.Zipf
}

func NewSynthetic(cfg SyntheticConfig) (*Synthetic, error) {
	if cfg.PrefixCount <= 0 || cfg.PrefixTokens <= 0 || cfg.TailTokens < 0 || cfg.TotalRequests <= 0 {
		return nil, fmt.Errorf("synthetic: bad config %+v", cfg)
	}
	if cfg.MaxOutputToks <= 0 {
		cfg.MaxOutputToks = 128
	}

	prefixes := make([]string, cfg.PrefixCount)
	for i := range prefixes {
		r := rand.New(rand.NewPCG(cfg.Seed, uint64(i)+1))
		prefixes[i] = generateWords(r, cfg.PrefixTokens)
	}

	pickRand := rand.New(rand.NewPCG(cfg.Seed, 0xDEAD))
	tailRand := rand.New(rand.NewPCG(cfg.Seed, 0xBEEF))

	var zipf *rand.Zipf
	if cfg.ZipfS > 1.0 {
		zipf = rand.NewZipf(pickRand, cfg.ZipfS, 1.0, uint64(cfg.PrefixCount-1))
	}

	return &Synthetic{
		cfg:      cfg,
		prefixes: prefixes,
		pickRand: pickRand,
		tailRand: tailRand,
		zipf:     zipf,
	}, nil
}

func (s *Synthetic) Name() string { return "synthetic" }

func (s *Synthetic) Total() int { return s.cfg.TotalRequests }

func (s *Synthetic) Next(_ context.Context) (*Request, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.emitted >= s.cfg.TotalRequests {
		return nil, false
	}
	s.emitted++

	var idx int
	if s.zipf != nil {
		idx = int(s.zipf.Uint64())
	} else {
		idx = s.pickRand.IntN(s.cfg.PrefixCount)
	}

	prefix := s.prefixes[idx]
	tail := generateWords(s.tailRand, s.cfg.TailTokens)

	var b strings.Builder
	b.Grow(len(prefix) + len(tail) + 2)
	b.WriteString(prefix)
	if tail != "" {
		b.WriteByte(' ')
		b.WriteString(tail)
	}

	return &Request{
		Messages: []Message{
			{Role: "user", Content: b.String()},
		},
		MaxTokens: s.cfg.MaxOutputToks,
		Prefix:    "p" + strconv.Itoa(idx),
	}, true
}

func generateWords(r *rand.Rand, n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(n * 6)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(commonWords[r.IntN(len(commonWords))])
	}
	return b.String()
}
