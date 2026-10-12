package workload

import (
	"context"
	"time"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Request struct {
	Messages   []Message
	MaxTokens  int
	DelayUntil time.Duration
	// Prefix is the workload's identifier for the shared-prefix portion of
	// this request, propagated to the pairwise per-request table for
	// diagnostic purposes. Synthetic sets it to "p<N>" where N is the
	// prefix-pool index. Empty when the workload doesn't set it.
	Prefix string
	// RequestID, when non-empty, is forwarded as the X-Request-ID header so
	// the request can be correlated with server-side logs. Set by the runner
	// (not the workload) before dispatch.
	RequestID string
	// PredecessorIndex, when non-nil, names the canonical index of a request
	// that must complete on the server before this one is sent. The runner
	// blocks the worker until that index's response has been recorded. Used
	// by the multiturn workload to enforce per-conversation turn ordering
	// under high concurrency — without this, worker A can send turn k+1
	// while worker B is still processing turn k, leaving the server's cache
	// empty for turn k's prefix when turn k+1 arrives.
	PredecessorIndex *int
	// CanonicalIndex, when non-nil, is the workload-assigned index of this
	// request, used by the runner to signal completion to any later
	// requests that name it as a PredecessorIndex. Workloads that don't
	// have downstream dependencies leave this nil.
	CanonicalIndex *int
	// SessionID, when non-empty, is forwarded as the X-Session-ID header.
	// Used by the multiturn workload to give every turn of a conversation
	// the same stable identifier (so the server / router can correlate
	// turns of the same session). Empty for workloads without sessions.
	SessionID string
	// TenantID, when non-empty AND the client's SetTenantID flag is true,
	// is forwarded as the l5d-client-id header (Linkerd's per-client
	// identifier — useful for routing/policy tests). Multiturn sets it
	// per-conversation as "tenant-<j>". Empty for workloads without a
	// tenant notion; the header is skipped when empty regardless of the
	// client flag.
	TenantID string
}

type Workload interface {
	Name() string
	// Total returns the total number of requests this workload will emit, or
	// 0 if unknown. Used only for progress reporting (% complete, ETA).
	Total() int
	Next(ctx context.Context) (*Request, bool)
}
