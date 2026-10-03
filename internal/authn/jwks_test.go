package authn

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerifierDefaults(t *testing.T) {
	v := NewVerifier(Config{JWKSURL: "http://iam.invalid/.well-known/jwks.json", Issuer: testIssuer, Audience: testAudience})
	require.Equal(t, time.Hour, v.refreshEvery)
	require.Equal(t, 30*time.Second, v.refetchGap)
	require.Equal(t, 3*time.Second, v.fetchTimeout)
	require.Equal(t, 500*time.Millisecond, v.backoffBase)
	require.Equal(t, 15*time.Second, v.backoffMax)
	require.NotSame(t, http.DefaultClient, v.client)
	require.NotNil(t, v.now)
	require.False(t, v.Ready())
}

// P5.4: an unknown kid triggers one refetch, at most once per 30 s, then fails.
func TestUnknownKidRefetchIsRateLimited(t *testing.T) {
	a, b := newRSAKey(t, "a", 2048), newEdKey(t, "b")
	s := newJWKSServer(t, a)
	clock := newFakeClock(testNow)
	v := newTestVerifier(s, clock)

	_, err := v.Verify(bg(), "Bearer "+token(t, a, nil))
	require.NoError(t, err, "keys load on first use")
	require.EqualValues(t, 1, s.fetches.Load())
	require.True(t, v.Ready())

	clock.advance(31 * time.Second)
	_, err = v.Verify(bg(), "Bearer "+token(t, b, nil))
	requireInvalid(t, err, RuleKid)
	require.EqualValues(t, 2, s.fetches.Load(), "one refetch for the unknown kid")

	s.set(a, b) // the key appears, but the last refetch was less than 30 s ago
	clock.advance(29 * time.Second)
	_, err = v.Verify(bg(), "Bearer "+token(t, b, nil))
	requireInvalid(t, err, RuleKid)
	require.EqualValues(t, 2, s.fetches.Load(), "no refetch within 30 s")

	clock.advance(time.Second)
	_, err = v.Verify(bg(), "Bearer "+token(t, b, nil))
	require.NoError(t, err)
	require.EqualValues(t, 3, s.fetches.Load())

	_, err = v.Verify(bg(), "Bearer "+token(t, a, nil))
	require.NoError(t, err)
	require.EqualValues(t, 3, s.fetches.Load(), "a known kid never fetches")
}

// P5.4: each fetch times out after 3 s (shortened here); a failed fetch keeps the keys held.
func TestFetchTimeoutIsFailStatic(t *testing.T) {
	a, b := newRSAKey(t, "a", 2048), newECKey(t, "b")
	s := newJWKSServer(t, a)
	clock := newFakeClock(testNow)
	v := newTestVerifier(s, clock)
	v.fetchTimeout = 50 * time.Millisecond
	_, err := v.Verify(bg(), "Bearer "+token(t, a, nil))
	require.NoError(t, err)

	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	s.setHang(hang)
	clock.advance(time.Minute)
	start := time.Now()
	_, err = v.Verify(bg(), "Bearer "+token(t, b, nil))
	requireInvalid(t, err, RuleKid)
	require.Less(t, time.Since(start), 2*time.Second, "the refetch is bounded by the fetch timeout")
	require.EqualValues(t, 2, s.fetches.Load())

	_, err = v.Verify(bg(), "Bearer "+token(t, a, nil))
	require.NoError(t, err, "the keys held survive a failed fetch")
}

func TestFailedFetchStatusIsFailStatic(t *testing.T) {
	a, b := newRSAKey(t, "a", 2048), newECKey(t, "b")
	s := newJWKSServer(t, a)
	clock := newFakeClock(testNow)
	v := newTestVerifier(s, clock)
	_, err := v.Verify(bg(), "Bearer "+token(t, a, nil))
	require.NoError(t, err)

	s.setStatus(http.StatusInternalServerError)
	clock.advance(time.Minute)
	_, err = v.Verify(bg(), "Bearer "+token(t, b, nil))
	requireInvalid(t, err, RuleKid)

	s.setStatus(http.StatusOK)
	s.setRaw() // an empty key set is a failed fetch too
	clock.advance(time.Minute)
	_, err = v.Verify(bg(), "Bearer "+token(t, b, nil))
	requireInvalid(t, err, RuleKid)
	_, err = v.Verify(bg(), "Bearer "+token(t, a, nil))
	require.NoError(t, err)
}

// P5.4: during a rotation, tokens of the current and the previous key both verify.
func TestRotation(t *testing.T) {
	prev, cur, next := newRSAKey(t, "k1", 2048), newRSAKey(t, "k2", 2048), newECKey(t, "k3")
	s := newJWKSServer(t, cur, prev)
	clock := newFakeClock(testNow)
	v := newTestVerifier(s, clock)
	for _, k := range []testKey{cur, prev} {
		_, err := v.Verify(bg(), "Bearer "+token(t, k, nil))
		require.NoError(t, err, k.kid)
	}

	s.set(next, cur) // k1 retired, k3 now signs
	clock.advance(time.Minute)
	_, err := v.Verify(bg(), "Bearer "+token(t, next, nil))
	require.NoError(t, err)
	_, err = v.Verify(bg(), "Bearer "+token(t, cur, nil))
	require.NoError(t, err)
	_, err = v.Verify(bg(), "Bearer "+token(t, prev, nil))
	requireInvalid(t, err, RuleKid)
}

func TestRunLoadsWithBackoffAndReportsReady(t *testing.T) {
	a := newRSAKey(t, "a", 2048)
	s := newJWKSServer(t, a)
	s.setStatus(http.StatusServiceUnavailable)
	v := newTestVerifier(s, newFakeClock(testNow))
	v.backoffBase, v.backoffMax = 60*time.Millisecond, 240*time.Millisecond
	cancel := runVerifier(t, v)
	defer cancel()

	require.Eventually(t, func() bool { return s.fetches.Load() >= 5 }, 5*time.Second, time.Millisecond)
	require.False(t, v.Ready())
	times := s.fetchTimes()
	for i, want := range []time.Duration{60, 120, 240, 240} { // doubling, capped
		gap := times[i+1].Sub(times[i])
		require.GreaterOrEqual(t, gap, want*time.Millisecond, "gap %d", i)
		require.Less(t, gap, want*time.Millisecond+50*time.Millisecond, "gap %d", i)
	}
	s.setStatus(http.StatusOK)
	require.Eventually(t, v.Ready, 5*time.Second, time.Millisecond)
	_, err := v.Verify(bg(), "Bearer "+token(t, a, nil))
	require.NoError(t, err)
}

func TestRunRefreshesPeriodically(t *testing.T) {
	a, b := newRSAKey(t, "a", 2048), newEdKey(t, "b")
	s := newJWKSServer(t, a)
	clock := newFakeClock(testNow) // frozen: the unknown-kid refetch stays rate-limited
	v := newTestVerifier(s, clock)
	v.refreshEvery = 20 * time.Millisecond
	cancel := runVerifier(t, v)
	defer cancel()
	require.Eventually(t, v.Ready, 5*time.Second, time.Millisecond)

	s.set(a, b)
	require.Eventually(t, func() bool {
		_, err := v.Verify(bg(), "Bearer "+token(t, b, nil))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRunReturnsOnCancel(t *testing.T) {
	v := NewVerifier(Config{JWKSURL: "http://127.0.0.1:1/jwks.json", Issuer: testIssuer, Audience: testAudience})
	ctx, cancel := context.WithCancel(bg())
	done := make(chan error, 1)
	go func() { done <- v.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}

func runVerifier(t *testing.T, v *Verifier) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(bg())
	done := make(chan error, 1)
	go func() { done <- v.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	}
}
