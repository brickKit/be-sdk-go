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
	readOnly bool
}

func (f *fakeSource) Verify(_ context.Context, req VerifyRequest) (VerifyAnswer, error) {
	*f.clock += f.delay // the answer arrives late
	n := req.Nonce
	if f.badNonce {
		n = "replayed"
	}
	a := VerifyAnswer{Nonce: n, RecoveryGeneration: Revision{Domain: "authz.recovery", Value: f.gen}}
	s := f.shard
	a.Shards = []ShardAnswer{{Shard: "orders", Status: f.status, View: &s, ReadOnly: f.readOnly}}
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

func TestReadOnlyRenewalLeavesTheHighRiskValidity(t *testing.T) { // P8.11, security 03 (renewal and closing)
	src := &fakeSource{gen: "1", status: "changed", shard: shard()}
	h, now := holderWith(src)
	require.NoError(t, h.Verify(context.Background()))
	full := *now
	_, err := h.Lookup("demo.order", alice, "high")
	require.NoError(t, err, "the precondition: a full renewal is valid for high-risk operations")

	src.status, src.readOnly = "unchanged", true
	*now = full + 3*time.Second
	require.NoError(t, h.Verify(context.Background()))
	g, err := h.Lookup("demo.order", alice, "high")
	require.NoError(t, err)
	assert.Equal(t, full+time.Duration(float64(WHigh)*(1-Drift)), g.Cutoff, "the high-risk cutoff stays at the full renewal")
	g, err = h.Lookup("demo.order", alice, "low")
	require.NoError(t, err)
	assert.Equal(t, *now+time.Duration(float64(WRead)*(1-Drift)), g.Cutoff, "ordinary reads are renewed")

	src.status = "changed" // a new view, still read-only: it keeps the old high-risk validity
	src.shard.Digest = "sha256:2"
	*now = full + 4*time.Second
	require.NoError(t, h.Verify(context.Background()))
	g, err = h.Lookup("demo.order", alice, "high")
	require.NoError(t, err)
	assert.Equal(t, full+time.Duration(float64(WHigh)*(1-Drift)), g.Cutoff)

	*now = full + WHigh
	_, err = h.Lookup("demo.order", alice, "high")
	expired(t, err)
	_, err = h.Lookup("demo.order", alice, "low")
	require.NoError(t, err, "ordinary reads go on")

	src.readOnly = false // the source caught up: a full renewal opens high-risk operations again
	require.NoError(t, h.Verify(context.Background()))
	_, err = h.Lookup("demo.order", alice, "high")
	require.NoError(t, err)
}

func TestTighteningOnlyOutlivesAStaleSource(t *testing.T) { // P8.29
	src := &fakeSource{gen: "1", status: "changed", shard: shard()}
	h, now := holderWith(src)
	require.NoError(t, h.Verify(context.Background()))
	full := *now
	_, err := h.LookupStale("demo.order", alice)
	expired(t, err) // fully renewed and fresh: the ordinary lookup decides, not this one

	// the member says the source is stale (read_only), then closed; past both validities the last view still decides
	src.status, src.readOnly = "unchanged", true
	*now = full + time.Second
	require.NoError(t, h.Verify(context.Background()))
	src.status, src.readOnly = "closed", false
	*now = full + WRead + WHigh
	require.NoError(t, h.Verify(context.Background()))
	_, err = h.Lookup("demo.order", alice, "low")
	expired(t, err)
	g, err := h.LookupStale("demo.order", alice)
	require.NoError(t, err, "the member said the source is stale")
	assert.True(t, g.Stale)
	assert.Len(t, g.Segments, 1)
	assert.Equal(t, h.Wall().Add(full-*now), g.Since, "the last full renewal")
	assert.Greater(t, g.Cutoff, *now, "the change may commit")

	// a full renewal, then the member stops answering: that is no stale source, the view closes as before
	src.status = "unchanged"
	require.NoError(t, h.Verify(context.Background()))
	_, err = h.Lookup("demo.order", alice, "high")
	require.NoError(t, err, "fresh again: the ordinary lookup decides")
	*now += WRead + WHigh
	_, err = h.Lookup("demo.order", alice, "low")
	expired(t, err)
	_, err = h.LookupStale("demo.order", alice)
	expired(t, err)
}

func TestCheckAdmitsTighteningOnlyOnAStaleView(t *testing.T) { // P8.29
	src := &fakeSource{gen: "1", status: "changed", shard: shard()}
	s := src.shard.Subjects[alice]
	s.Segments = append(s.Segments, Segment{ResourceType: "demo.order", Operation: "demo.order.revoke", Atoms: map[string]Atom{},
		Valid: Window{From: "2026-01-01T00:00:00Z"}}, Segment{ResourceType: "demo.order", Operation: "demo.order.approve",
		Atoms: map[string]Atom{}, Valid: Window{From: "2026-01-01T00:00:00Z"}})
	src.shard.Subjects = map[string]SubjectEntry{alice: s}
	h, now := holderWith(src)
	require.NoError(t, h.Verify(context.Background()))
	src.status = "closed"
	*now += WRead + WHigh
	require.NoError(t, h.Verify(context.Background()))
	defs := &Definitions{Component: "demo/orders", Resources: []Resource{{Type: "demo.order", ViewOperation: "demo.order.read",
		Dimensions: map[string]Dimension{}, Fields: map[string]Field{}}},
		Operations: []Operation{{Key: "demo.order.read", ResourceType: "demo.order", Purpose: "read", RiskClass: "low"},
			{Key: "demo.order.revoke", ResourceType: "demo.order", Purpose: "manage", RiskClass: "high",
				SideEffects: []string{"security_change"}, TighteningOnly: true},
			{Key: "demo.order.approve", ResourceType: "demo.order", Purpose: "action", RiskClass: "high"}}}
	require.NoError(t, defs.Validate())
	sub := &Subject{Principal: alice, Defs: defs, Holder: h, At: time.Unix(1792200000, 0)}

	ctx, _, err := sub.Check(context.Background(), "demo.order.revoke", Facts{})
	require.NoError(t, err, "a tightening-only operation on the stale source's last view")
	since, stale := StaleBasis(ctx)
	assert.True(t, stale)
	assert.False(t, since.IsZero())
	_, _, err = sub.Check(context.Background(), "demo.order.approve", Facts{})
	expired(t, err)
	ctx, _, err = sub.Check(context.Background(), "demo.order.read", Facts{})
	expired(t, err)
	_, stale = StaleBasis(ctx)
	assert.False(t, stale)
}

func TestTighteningOnlyNeedsAFullRenewalOnce(t *testing.T) { // P8.29
	src := &fakeSource{gen: "1", status: "changed", shard: shard(), readOnly: true}
	h, now := holderWith(src)
	require.NoError(t, h.Verify(context.Background()))
	*now += WRead + WHigh
	_, err := h.LookupStale("demo.order", alice)
	expired(t, err)
}

func TestAReadOnlyFirstViewIsNotValidForHighRisk(t *testing.T) { // P8.11
	src := &fakeSource{gen: "1", status: "changed", shard: shard(), readOnly: true}
	h, now := holderWith(src)
	*now = time.Second // just after the machine started: a zero high-risk renewal time would still look valid
	require.NoError(t, h.Verify(context.Background()))
	_, err := h.Lookup("demo.order", alice, "low")
	require.NoError(t, err)
	_, err = h.Lookup("demo.order", alice, "high")
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

type failingAfterFirst struct {
	*fakeSource
	calls int
}

func (f *failingAfterFirst) Verify(ctx context.Context, req VerifyRequest) (VerifyAnswer, error) {
	f.calls++
	if f.calls == 1 {
		return f.fakeSource.Verify(ctx, req)
	}
	*f.clock += Interval - 100*time.Millisecond // the authorization member is slow: the call runs into its deadline
	return VerifyAnswer{}, context.DeadlineExceeded
}

func TestAVerificationThatTimesOutIsNotAStall(t *testing.T) { // P8.10: only the wait between verifications is timed
	src := &fakeSource{gen: "1", status: "changed", shard: shard()}
	h, now := holderWith(src)
	slow := &failingAfterFirst{fakeSource: src}
	h.Source = slow
	h.tick()
	require.NoError(t, h.Verify(context.Background()))
	require.Error(t, h.Verify(context.Background()))
	h.rest()
	*now += Interval // the wait, on time
	h.tick()
	_, err := h.Lookup("demo.order", alice, "low")
	require.NoError(t, err, "the view renewed by the first answer is still valid")
}
