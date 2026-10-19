package authz

import (
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestViewAcceptVectors: the protocol's view-accept vector, one answer applied to a held state (P8.11).
func TestViewAcceptVectors(t *testing.T) {
	const before, sent = time.Second, 9 * time.Second
	for _, raw := range vectorCases(t, "view-accept") {
		var c struct {
			ID   string
			Held struct {
				Recovery *int64
				Instance *string
				Shards   map[string]struct {
					Revisions []Revision
					Stopped   bool
					Suspended bool
				}
			}
			Answer struct {
				Recovery int64
				Instance string
				Shards   []struct {
					Shard, Status string
					View          *struct {
						SecurityRevisions []Revision `json:"security_revisions"`
					}
				}
			}
			Expect struct {
				Answer   string
				Instance *string
				Shards   map[string]string
			}
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		h := NewHolder("demo/orders", nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if c.Held.Recovery != nil {
			h.recovery, h.hasRecovery = *c.Held.Recovery, true
		}
		if c.Held.Instance != nil {
			h.instance = *c.Held.Instance
		}
		for name, s := range c.Held.Shards {
			h.shards[name] = &held{shard: Shard{Shard: name, SecurityRevisions: s.Revisions, Digest: "old"}, sentAt: before,
				stopped: s.Stopped, suspended: s.Suspended}
		}
		ans := VerifyAnswer{Instance: c.Answer.Instance,
			RecoveryGeneration: Revision{Domain: "authz.recovery", Value: strconv.FormatInt(c.Answer.Recovery, 10)}}
		for _, a := range c.Answer.Shards {
			sa := ShardAnswer{Shard: a.Shard, Status: a.Status}
			if a.View != nil {
				sa.View = &Shard{Shard: a.Shard, SecurityRevisions: a.View.SecurityRevisions, Digest: "new"}
			}
			ans.Shards = append(ans.Shards, sa)
		}
		accepted := h.apply(ans, c.Answer.Recovery, sent)
		got := map[string]string{}
		for name := range c.Held.Shards {
			got[name] = "dropped"
		}
		for name, s := range h.shards {
			switch {
			case s.stopped:
				got[name] = "stopped"
			case s.suspended:
				got[name] = "suspended"
			case s.sentAt == sent && s.shard.Digest == "new":
				got[name] = "replaced"
			case s.sentAt == sent:
				got[name] = "renewed"
			default:
				got[name] = "kept"
			}
		}
		assert.Equal(t, c.Expect.Answer == "accepted", accepted, c.ID)
		assert.Equal(t, c.Expect.Shards, got, c.ID)
		if c.Expect.Instance != nil {
			assert.Equal(t, *c.Expect.Instance, h.instance, c.ID)
		} else {
			assert.Empty(t, h.instance, c.ID)
		}
	}
}
