package main

import (
	"strings"
	"testing"
	"time"
)

func TestFormatProgress(t *testing.T) {
	start := time.Date(2026, 5, 21, 14, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		now         time.Time
		done, total int
		errs        int
		mustContain []string
		mustNot     []string
	}{
		{
			name:        "midrun with ETA",
			now:         start.Add(60 * time.Second),
			done:        25, total: 100, errs: 0,
			mustContain: []string{"[1m0s]", "25/100", "(25%)", "0 errors", "eta 3m0s"},
		},
		{
			name:        "completed, no ETA",
			now:         start.Add(120 * time.Second),
			done:        100, total: 100, errs: 2,
			mustContain: []string{"[2m0s]", "100/100", "(100%)", "2 errors"},
			mustNot:     []string{"eta"},
		},
		{
			name:        "long elapsed in h/m/s",
			now:         start.Add(3725 * time.Second),
			done:        50, total: 100, errs: 0,
			mustContain: []string{"[1h2m5s]"},
		},
		{
			name:        "sub-minute elapsed",
			now:         start.Add(12 * time.Second),
			done:        3, total: 100, errs: 0,
			mustContain: []string{"[12s]"},
		},
		{
			name:        "unknown total",
			now:         start.Add(5 * time.Second),
			done:        7, total: 0, errs: 0,
			mustContain: []string{"[5s]", "7 done", "0 errors"},
			mustNot:     []string{"/0", "eta", "%"},
		},
		{
			name:        "zero done",
			now:         start.Add(5 * time.Second),
			done:        0, total: 100, errs: 0,
			mustContain: []string{"0 done"},
			mustNot:     []string{"eta", "%"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatProgress(tt.now, start, tt.done, tt.total, tt.errs)
			for _, want := range tt.mustContain {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q in %q", want, got)
				}
			}
			for _, no := range tt.mustNot {
				if strings.Contains(got, no) {
					t.Errorf("should not contain %q: %q", no, got)
				}
			}
		})
	}
}

func TestFormatPairProgress(t *testing.T) {
	tests := []struct {
		name         string
		elapsed      time.Duration
		doneA, errsA int
		doneB, errsB int
		total        int
		mustContain  []string
		mustNot      []string
	}{
		{
			name:    "both sides midrun, B slower bounds ETA",
			elapsed: 60 * time.Second,
			doneA:   80, errsA: 0, doneB: 40, errsB: 1, total: 100,
			mustContain: []string{"[1m0s]", "router: 80/100", "errs=0", "baseline: 40/100", "errs=1", "eta 1m30s"},
		},
		{
			name:    "no total",
			elapsed: 5 * time.Second,
			doneA:   3, errsA: 0, doneB: 2, errsB: 0, total: 0,
			mustContain: []string{"router: 3", "baseline: 2"},
			mustNot:     []string{"/0", "eta", "%"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatPairProgress(tt.elapsed, tt.doneA, tt.errsA, tt.doneB, tt.errsB, tt.total, "router", "baseline")
			for _, want := range tt.mustContain {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q in %q", want, got)
				}
			}
			for _, no := range tt.mustNot {
				if strings.Contains(got, no) {
					t.Errorf("should not contain %q: %q", no, got)
				}
			}
		})
	}
}
