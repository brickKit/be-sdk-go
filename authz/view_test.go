package authz

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const alice = "0190f3a1-0000-7000-8000-0000000000a1"

type fakeSource struct {
	gen      string
	status   string
	shard    Shard
	delay    time.Duration
	clock    *time.Duration
	badNonce bool
}

func (f *fakeSource) Verify(_ context.Context, req VerifyRequest) (VerifyAnswer, error) {
	*f.clock += f.delay // the answer arrives late
	n := req.Nonce
	if f.badNonce {
		n = "replayed"
	}
	a := VerifyAnswer{Nonce: n, RecoveryGeneration: Revision{Domain: "authz.recovery", Value: f.gen}}
	s := f.shard
	a.Shards = []ShardAnswer{{Shard: "orders", Status: f.status, View: &s}}
	return a, nil
}

func holderWith(src *fakeSource) (*Holder, *time.Duration) {
	now := time.Duration(1000) * time.Second
	src.clock = &now
	h := NewHolder("demo/orders", src, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.Boot = func() time.Duration { return now }
	wall := time.Unix(1792200000, 0)
	h.Wall = func() time.Time { return wall.Add(now) }
	return h, &now
}

func shard() Shard {
	return Shard{Shard: "orders", Applicability: []string{"demo.order"}, Digest: "sha256:1",
		Subjects: map[string]SubjectEntry{alice: {Segments: []Segment{{ResourceType: "demo.order", Operation: "demo.order.read",
			Atoms: map[string]Atom{}, Valid: Window{From: "2026-01-01T00:00:00Z"}}}}}}
}

func expired(t *testing.T, err error) {
	t.Helper()
	assert.True(t, errs.Is(err, "be", errs.ReasonAuthorizationViewExpired), "%v", err)
}

func TestNoViewAfterStart(t *testing.T) {
	h, _ := holderWith(&fakeSource{gen: "1", status: "changed", shard: shard()})
	_, err := h.Lookup("demo.order", alice, "low")
	expired(t, err)
}

func TestValidityCountsFromTheRequest(t *testing.T) { // P8.10, CK-AZ-03
	src := &fakeSource{gen: "1", status: "changed", shard: shard(), delay: 10 * time.Second}
	h, now := holderWith(src)
	sent := *now
	require.NoError(t, h.Verify(context.Background()))
	g, err := h.Lookup("demo.order", alice, "low")
	require.NoError(t, err)
	assert.Len(t, g.Segments, 1)
	assert.Equal(t, sent+time.Duration(float64(WRead)*(1-Drift)), g.Cutoff, "a late answer does not extend the validity")
	_, err = h.Lookup("demo.order", alice, "high")
	expired(t, err) // 10 s after sending: past W_high

	*now = sent + 29*time.Second
	_, err = h.Lookup("demo.order", alice, "low")
	require.NoError(t, err)
	*now = sent + 30*time.Second
	_, err = h.Lookup("demo.order", alice, "low")
	expired(t, err)
}

func TestAnswersThatDoNotRenew(t *testing.T) { // P8.9, P8.11
	src := &fakeSource{gen: "1", status: "changed", shard: shard()}
	h, now := holderWith(src)
	require.NoError(t, h.Verify(context.Background()))
	start := *now

	src.badNonce, src.status = true, "unchanged"
	*now = start + 20*time.Second
	assert.Error(t, h.Verify(context.Background()), "a replayed answer is discarded")
	src.badNonce = false
	src.status = "closed"
	require.NoError(t, h.Verify(context.Background()))
	*now = start + 31*time.Second
	_, err := h.Lookup("demo.order", alice, "low")
	expired(t, err) // neither renewed it

	src.status = "changed"
	require.NoError(t, h.Verify(context.Background()))
	_, err = h.Lookup("demo.order", alice, "low")
	require.NoError(t, err)
	src.gen = "0" // an older recovery generation: a rollback
	require.NoError(t, h.Verify(context.Background()))
	_, err = h.Lookup("demo.order", alice, "low")
	expired(t, err)
	src.gen = "2" // a newer one is adopted
	require.NoError(t, h.Verify(context.Background()))
	_, err = h.Lookup("demo.order", alice, "low")
	require.NoError(t, err)
	_, err = h.Lookup("demo.invoice", alice, "low")
	expired(t, err) // the shard does not apply to this type
}

func TestAStallInvalidatesEveryShard(t *testing.T) { // P8.10
	src := &fakeSource{gen: "1", status: "changed", shard: shard()}
	h, now := holderWith(src)
	h.tick()
	require.NoError(t, h.Verify(context.Background()))
	*now += Interval + TStall + time.Second // the process was frozen
	h.tick()
	_, err := h.Lookup("demo.order", alice, "low")
	expired(t, err)
}

func TestDisabledAccountsHaveNoSegments(t *testing.T) {
	s := shard()
	s.Subjects[alice] = SubjectEntry{Segments: s.Subjects[alice].Segments, AccountsDisabled: true}
	h, _ := holderWith(&fakeSource{gen: "1", status: "changed", shard: s})
	require.NoError(t, h.Verify(context.Background()))
	g, err := h.Lookup("demo.order", alice, "low")
	require.NoError(t, err)
	assert.Empty(t, g.Segments)
}
