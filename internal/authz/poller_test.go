package authz

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// bundleServer is a scriptable provider: each request takes the next response, the last one repeats.
type bundleServer struct {
	mu        sync.Mutex
	responses []func(w http.ResponseWriter, r *http.Request)
	requests  []*http.Request
	times     []time.Time
	srv       *httptest.Server
}

func newBundleServer(t *testing.T, responses ...func(w http.ResponseWriter, r *http.Request)) *bundleServer {
	s := &bundleServer{responses: responses}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		n := len(s.requests)
		s.requests = append(s.requests, r.Clone(context.Background()))
		s.times = append(s.times, time.Now())
		h := s.responses[min(n, len(s.responses)-1)]
		s.mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *bundleServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *bundleServer) request(i int) *http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[i]
}

func serveBundle(etag, revision, contract string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if etag != "" && r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"contract":"` + contract + `","revision":"` + revision +
			`","capabilities":{"core":true},"roles":{"r":["a.b.c"]},"grants":{},"stale_since":{}}`))
	}
}

func serveStatus(code int) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }
}

// startPoller runs p in the background and stops it when the test ends.
func startPoller(t *testing.T, p *Poller) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 5*time.Second, 5*time.Millisecond)
}

func TestPollerDefaults(t *testing.T) {
	p := NewPoller(PollerConfig{URL: "http://authz.invalid/authz/v2/bundle"})
	require.Equal(t, 15*time.Second, p.cfg.Interval)
	require.Equal(t, 3*time.Second, p.cfg.Timeout)
	require.Equal(t, 500*time.Millisecond, p.backoffBase)
	require.Equal(t, 15*time.Second, p.backoffMax)
	require.NotNil(t, p.client)
	require.NotSame(t, http.DefaultClient, p.client)
	require.Nil(t, p.Current())
	require.True(t, p.LoadedAt().IsZero())
}

func TestPollerLoadsThenUsesETag(t *testing.T) {
	s := newBundleServer(t, serveBundle(`"r1"`, "1", "authz/2.0"))
	p := NewPoller(PollerConfig{URL: s.srv.URL, Interval: 20 * time.Millisecond})
	startPoller(t, p)

	eventually(t, func() bool { return p.Current() != nil })
	first := p.Current()
	require.Equal(t, "1", first.Revision)
	loaded := p.LoadedAt()
	require.False(t, loaded.IsZero())

	eventually(t, func() bool { return s.count() >= 3 })
	require.Empty(t, s.request(0).Header.Get("If-None-Match"))
	require.Equal(t, `"r1"`, s.request(1).Header.Get("If-None-Match"))
	require.Same(t, first, p.Current(), "a 304 keeps the bundle held")
	require.True(t, p.LoadedAt().After(loaded), "a 304 confirms the bundle is current")
}

func TestPollerReplacesBundleOnNewRevision(t *testing.T) {
	s := newBundleServer(t, serveBundle(`"r1"`, "1", "authz/2.0"), serveBundle(`"r2"`, "2", "authz/2.1"))
	p := NewPoller(PollerConfig{URL: s.srv.URL, Interval: 20 * time.Millisecond})
	startPoller(t, p)
	eventually(t, func() bool { b := p.Current(); return b != nil && b.Revision == "2" })
}

func TestPollerRefusedContractKeepsPreviousBundle(t *testing.T) {
	var refused atomic.Int32
	s := newBundleServer(t, serveBundle(`"r1"`, "1", "authz/2.0"), serveBundle(`"r9"`, "9", "authz/1.0"))
	p := NewPoller(PollerConfig{URL: s.srv.URL, Interval: 20 * time.Millisecond, OnRefused: func(err error) {
		require.ErrorIs(t, err, ErrBundleRefused)
		refused.Add(1)
	}})
	startPoller(t, p)
	eventually(t, func() bool { return refused.Load() >= 2 })
	require.Equal(t, "1", p.Current().Revision)
	require.Equal(t, `"r1"`, s.request(s.count()-1).Header.Get("If-None-Match"),
		"the ETag of a refused bundle is never sent")
}

func TestPollerRefusedFirstBundleStaysNotReady(t *testing.T) {
	var refused atomic.Int32
	s := newBundleServer(t, serveBundle("", "1", "authz/1.0"))
	p := NewPoller(PollerConfig{URL: s.srv.URL, OnRefused: func(error) { refused.Add(1) }})
	p.backoffBase, p.backoffMax = time.Millisecond, 2*time.Millisecond
	startPoller(t, p)
	eventually(t, func() bool { return refused.Load() >= 3 })
	require.Nil(t, p.Current())
}

func TestPollerFailStaticOnServerError(t *testing.T) {
	s := newBundleServer(t, serveBundle(`"r1"`, "1", "authz/2.0"), serveStatus(http.StatusInternalServerError))
	p := NewPoller(PollerConfig{URL: s.srv.URL, Interval: 10 * time.Millisecond})
	startPoller(t, p)
	eventually(t, func() bool { return s.count() >= 4 })
	require.NotNil(t, p.Current())
	require.Equal(t, "1", p.Current().Revision)
}

func TestPollerFailStaticOnTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	hang := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}
	s := newBundleServer(t, serveBundle(`"r1"`, "1", "authz/2.0"), hang)
	p := NewPoller(PollerConfig{URL: s.srv.URL, Interval: 10 * time.Millisecond, Timeout: 30 * time.Millisecond})
	startPoller(t, p)
	eventually(t, func() bool { return s.count() >= 3 }) // the poller moved on past a hung fetch
	require.Equal(t, "1", p.Current().Revision)
}

func TestPollerFirstLoadBacksOffDoubling(t *testing.T) {
	s := newBundleServer(t,
		serveStatus(http.StatusServiceUnavailable), serveStatus(http.StatusServiceUnavailable),
		serveStatus(http.StatusServiceUnavailable), serveStatus(http.StatusServiceUnavailable),
		serveStatus(http.StatusServiceUnavailable), serveBundle(`"r1"`, "1", "authz/2.0"))
	p := NewPoller(PollerConfig{URL: s.srv.URL, Interval: time.Hour})
	p.backoffBase, p.backoffMax = 60*time.Millisecond, 240*time.Millisecond
	startPoller(t, p)
	eventually(t, func() bool { return p.Current() != nil })

	s.mu.Lock()
	times := append([]time.Time(nil), s.times...)
	s.mu.Unlock()
	require.Len(t, times, 6)
	wants := []time.Duration{60, 120, 240, 240, 240} // doubling, capped
	for i, want := range wants {
		gap := times[i+1].Sub(times[i])
		require.GreaterOrEqual(t, gap, want*time.Millisecond, "gap %d", i)
		require.Less(t, gap, want*time.Millisecond+50*time.Millisecond, "gap %d", i)
	}
}

func TestPollerPokeFetchesImmediately(t *testing.T) {
	s := newBundleServer(t, serveBundle(`"r1"`, "1", "authz/2.0"), serveBundle(`"r2"`, "2", "authz/2.0"))
	p := NewPoller(PollerConfig{URL: s.srv.URL, Interval: time.Hour})
	startPoller(t, p)
	eventually(t, func() bool { return p.Current() != nil })
	require.Equal(t, 1, s.count())

	p.Poke()
	eventually(t, func() bool { return p.Current().Revision == "2" })
}

func TestPollerPokesCoalesce(t *testing.T) {
	gate := make(chan struct{})
	var once sync.Once
	slow := func(w http.ResponseWriter, r *http.Request) {
		<-gate
		serveBundle("", "2", "authz/2.0")(w, r)
	}
	s := newBundleServer(t, serveBundle("", "1", "authz/2.0"), slow)
	p := NewPoller(PollerConfig{URL: s.srv.URL, Interval: time.Hour})
	startPoller(t, p)
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	eventually(t, func() bool { return p.Current() != nil })

	p.Poke()
	eventually(t, func() bool { return s.count() == 2 }) // the first poke's fetch is in flight
	for range 50 {
		p.Poke()
	}
	once.Do(func() { close(gate) })
	eventually(t, func() bool { return p.Current().Revision == "2" })
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 3, s.count(), "pokes during a fetch coalesce into one more fetch")
}

func TestPollerPokeDuringFirstLoadBackoff(t *testing.T) {
	s := newBundleServer(t, serveStatus(http.StatusBadGateway), serveBundle("", "1", "authz/2.0"))
	p := NewPoller(PollerConfig{URL: s.srv.URL})
	p.backoffBase = time.Hour
	startPoller(t, p)
	eventually(t, func() bool { return s.count() == 1 })
	p.Poke()
	eventually(t, func() bool { return p.Current() != nil })
}

func TestPollerRunReturnsOnCancel(t *testing.T) {
	p := NewPoller(PollerConfig{URL: "http://127.0.0.1:1/unreachable"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}
