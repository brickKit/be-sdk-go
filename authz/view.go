package authz

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/clock"
	"github.com/brickKit/be-sdk-go/v3/errs"
)

// Parameters of security 03 (starting values, P8.9-P8.10).
const (
	WRead    = 30 * time.Second
	WHigh    = 5 * time.Second
	Drift    = 0.001
	TStall   = 2 * time.Second
	Interval = WHigh / 2
)

// Revision is a RevisionRef (security 09a).
type Revision struct {
	Domain   string `json:"domain"`
	Value    string `json:"value"`
	Epoch    string `json:"epoch,omitempty"`
	Instance string `json:"instance,omitempty"`
}

// SubjectEntry is one principal's effective segments in a shard.
type SubjectEntry struct {
	Segments         []Segment `json:"segments"`
	AccountsDisabled bool      `json:"accounts_disabled"`
}

// Shard is one shard of the authorization view (be.security.view).
type Shard struct {
	Shard              string                  `json:"shard"`
	RecoveryGeneration Revision                `json:"recovery_generation"`
	SecurityRevisions  []Revision              `json:"security_revisions"`
	Applicability      []string                `json:"applicability"`
	Subjects           map[string]SubjectEntry `json:"subjects"`
	Capabilities       []string                `json:"capabilities"`
	Digest             string                  `json:"digest"`
}

// Held names a shard the component holds.
type Held struct {
	Shard  string `json:"shard"`
	Digest string `json:"digest"`
}

// VerifyRequest is be.security.view_verify_request.
type VerifyRequest struct {
	Component string `json:"component"`
	Held      []Held `json:"held"`
	Nonce     string `json:"nonce"`
}

// ShardAnswer is one shard's status in an answer.
type ShardAnswer struct {
	Shard  string `json:"shard"`
	Status string `json:"status"` // unchanged, changed, closed
	View   *Shard `json:"view,omitempty"`
}

// VerifyAnswer is be.security.view_verify_answer.
type VerifyAnswer struct {
	Nonce              string        `json:"nonce"`
	ComputedAt         string        `json:"computed_at"`
	Shards             []ShardAnswer `json:"shards"`
	RecoveryGeneration Revision      `json:"recovery_generation"`
}

// Source answers verification requests: the authorization member over the system surface.
type Source interface {
	Verify(ctx context.Context, req VerifyRequest) (VerifyAnswer, error)
}

type held struct {
	shard  Shard
	sentAt time.Duration // suspend-aware reading when the request that renewed it was sent
}

// Holder keeps a member's view shards and their validity. One per member.
type Holder struct {
	Component string
	Source    Source
	Log       *slog.Logger
	Boot      func() time.Duration // the suspend-aware clock; replaceable in tests
	Wall      func() time.Time

	mu       sync.RWMutex
	shards   map[string]*held
	recovery int64
	stopped  bool // a rollback was detected: nothing is used until a valid answer arrives
	lastTick time.Duration
	lastSkew time.Duration
	verifyMu sync.Mutex
}

// NewHolder creates an empty holder: after a start there is no trusted view (P8.10).
func NewHolder(component string, src Source, log *slog.Logger) *Holder {
	return &Holder{Component: component, Source: src, Log: log, Boot: clock.Boot, Wall: time.Now, shards: map[string]*held{}}
}

// Run verifies every Interval until ctx ends, and detects stalls.
func (h *Holder) Run(ctx context.Context) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		h.tick()
		vctx, cancel := context.WithTimeout(ctx, Interval) // calls need a deadline; a verification never outlives its period
		err := h.Verify(vctx)
		cancel()
		if err != nil && ctx.Err() == nil {
			h.Log.Warn("authorization view verification failed; the view stays valid only until it expires", "error", err.Error())
		}
		t.Reset(Interval)
	}
}

// tick detects a stall: a scheduling gap beyond Interval + TStall, or a jump between the boot clock and the wall
// clock beyond TStall, invalidates every shard (P8.10).
func (h *Holder) tick() {
	now, wall := h.Boot(), h.Wall()
	skew := time.Duration(wall.UnixNano()) - now
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastTick != 0 {
		gap := now - h.lastTick
		jump := skew - h.lastSkew
		if gap > Interval+TStall || jump > TStall || jump < -TStall {
			if len(h.shards) > 0 {
				h.Log.Warn("a stall was detected; every authorization view shard is invalidated", "gap_ms", gap.Milliseconds())
			}
			h.shards = map[string]*held{}
		}
	}
	h.lastTick, h.lastSkew = now, skew
}

func nonce() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Verify sends one verification request and applies the answer (P8.9, P8.11).
func (h *Holder) Verify(ctx context.Context) error {
	h.verifyMu.Lock()
	defer h.verifyMu.Unlock()
	h.mu.RLock()
	req := VerifyRequest{Component: h.Component, Nonce: nonce()}
	for _, s := range h.shards {
		req.Held = append(req.Held, Held{Shard: s.shard.Shard, Digest: s.shard.Digest})
	}
	h.mu.RUnlock()
	sent := h.Boot() // validity counts from the moment the request leaves
	ans, err := h.Source.Verify(ctx, req)
	if err != nil {
		return err
	}
	if ans.Nonce != req.Nonce {
		return fmt.Errorf("an answer for another request (nonce mismatch) was discarded")
	}
	gen, err := strconv.ParseInt(ans.RecoveryGeneration.Value, 10, 64)
	if err != nil {
		return fmt.Errorf("recovery generation %q", ans.RecoveryGeneration.Value)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case gen < h.recovery:
		h.shards, h.stopped = map[string]*held{}, true
		h.Log.Error("the authorization member answered with an older recovery generation: a rollback; the view is not used",
			"held", h.recovery, "answered", gen)
		return nil
	case gen > h.recovery:
		h.shards, h.recovery = map[string]*held{}, gen // every older shard is void
	}
	h.stopped = false
	for _, a := range ans.Shards {
		switch a.Status {
		case "unchanged":
			if s, ok := h.shards[a.Shard]; ok {
				s.sentAt = sent
			}
		case "changed":
			if a.View != nil {
				h.shards[a.Shard] = &held{shard: *a.View, sentAt: sent}
			}
		case "closed": // not renewed: it runs out on its own
		}
	}
	return nil
}

// Grant is what a lookup gives a request: the subject's segments for a resource type and the cutoff.
type Grant struct {
	Segments []Segment
	Cutoff   time.Duration // suspend-aware reading after which the evidence is no longer valid
}

// Lookup finds the subject's segments for a resource type in a valid shard; risk high uses W_high.
func (h *Holder) Lookup(resourceType, principal string, risk string) (Grant, error) {
	w := WRead
	if risk == "high" {
		w = WHigh
	}
	valid := time.Duration(float64(w) * (1 - Drift))
	now := h.Boot()
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.stopped {
		for _, s := range h.shards {
			if !slices.Contains(s.shard.Applicability, resourceType) || now >= s.sentAt+valid {
				continue
			}
			entry := s.shard.Subjects[principal]
			var segs []Segment
			if !entry.AccountsDisabled {
				for _, seg := range entry.Segments {
					if seg.ResourceType == "" || seg.ResourceType == resourceType {
						segs = append(segs, seg)
					}
				}
			}
			return Grant{Segments: segs, Cutoff: s.sentAt + valid}, nil
		}
	}
	return Grant{}, errs.New(errs.ReasonAuthorizationViewExpired, "no valid authorization view").With("resource_type", resourceType)
}
