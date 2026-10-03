package authz

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Defaults from be-protocol P6.1.
const (
	defaultInterval    = 15 * time.Second
	defaultTimeout     = 3 * time.Second
	defaultBackoffBase = 500 * time.Millisecond
	defaultBackoffMax  = 15 * time.Second
	// maxBundleBytes bounds one bundle body; the spec sets no size, a bundle of thousands of roles is
	// well below it. A larger body is a failed fetch (fail-static).
	maxBundleBytes = 16 << 20
)

// PollerConfig configures a Poller. Zero values take the P6.1 defaults.
type PollerConfig struct {
	// URL is {AUTHZ_URL}/authz/v2/bundle; the caller builds it.
	URL string
	// Client is the caller's HTTP client; nil uses a client with its own transport (no shared state).
	Client *http.Client
	// Interval between polls once a bundle is held (default 15 s).
	Interval time.Duration
	// Timeout of one fetch (default 3 s).
	Timeout time.Duration
	// Logger receives fetch failures (WARN) and refused bundles (ERROR); nil discards.
	Logger *slog.Logger
	// OnRefused, when set, is called with each refused bundle's error (for a metric).
	OnRefused func(error)
}

// Poller holds the current authz bundle and keeps it fresh (P6.1): first load with backoff, a poll
// every Interval with If-None-Match, an immediate fetch on Poke, fail-static on every error, and E1
// acceptance. Run is called once, by the caller's supervisor; the other methods are safe from any
// goroutine.
type Poller struct {
	cfg         PollerConfig
	client      *http.Client
	log         *slog.Logger
	backoffBase time.Duration
	backoffMax  time.Duration

	current  atomic.Pointer[Bundle]
	loadedAt atomic.Int64 // unix nanoseconds, 0 before the first accepted bundle
	poke     chan struct{}
	etag     string // only touched by Run's goroutine
}

// NewPoller returns a Poller that has not fetched anything yet (P6.1).
func NewPoller(c PollerConfig) *Poller {
	if c.Interval <= 0 {
		c.Interval = defaultInterval
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
	p := &Poller{
		cfg: c, client: c.Client, log: c.Logger,
		backoffBase: defaultBackoffBase, backoffMax: defaultBackoffMax,
		poke: make(chan struct{}, 1),
	}
	if p.client == nil {
		p.client = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	if p.log == nil {
		p.log = slog.New(slog.DiscardHandler)
	}
	return p
}

// Current returns the accepted bundle, or nil before the first one (P6.2: AUTHZ_NOT_READY).
func (p *Poller) Current() *Bundle { return p.current.Load() }

// LoadedAt is when the provider last confirmed the held bundle (an accepted 200 or a 304); zero
// before the first accepted bundle. now − LoadedAt is be_authz_bundle_age_seconds (P18).
func (p *Poller) LoadedAt() time.Time {
	ns := p.loadedAt.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// Poke asks for an immediate fetch, as on infra.authz.changed.v1 (P6.1). Pokes that arrive while one
// is pending coalesce into a single fetch; Poke never blocks.
func (p *Poller) Poke() {
	select {
	case p.poke <- struct{}{}:
	default:
	}
}

// Run fetches until ctx ends and then returns nil (P6.1). Until a bundle is accepted it retries with
// a backoff from 0.5 s doubling to 15 s; afterwards it polls every Interval. A poke fetches at once in
// either phase.
func (p *Poller) Run(ctx context.Context) error {
	delay := p.backoffBase
	for p.current.Load() == nil {
		p.fetch(ctx)
		if p.current.Load() != nil {
			break
		}
		if !p.wait(ctx, delay) {
			return nil
		}
		delay = min(delay*2, p.backoffMax)
	}
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-p.poke:
		}
		p.fetch(ctx)
	}
}

// wait sleeps for d or until a poke; it returns false when ctx ends first.
func (p *Poller) wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
	case <-p.poke:
	}
	return true
}

// fetch performs one conditional GET and applies its outcome; every failure keeps what is held.
func (p *Poller) fetch(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	body, etag, notModified, err := p.get(ctx)
	switch {
	case err != nil:
		p.log.WarnContext(ctx, "authz bundle fetch failed; keeping the bundle held", "error", err.Error())
	case notModified:
		p.loadedAt.Store(time.Now().UnixNano())
	default:
		p.accept(ctx, body, etag)
	}
}

// accept parses a fetched body and installs it, or refuses it (E1).
func (p *Poller) accept(ctx context.Context, body []byte, etag string) {
	b, err := ParseBundle(body)
	if err != nil {
		p.log.ErrorContext(ctx, "authz bundle refused; keeping the bundle held", "error", err.Error())
		if p.cfg.OnRefused != nil {
			p.cfg.OnRefused(err)
		}
		return
	}
	prev := p.current.Swap(b)
	p.etag = etag
	p.loadedAt.Store(time.Now().UnixNano())
	if prev == nil || prev.Revision != b.Revision {
		p.log.InfoContext(ctx, "authz bundle loaded", "revision", b.Revision, "contract", b.Contract)
	}
}

// get sends the request with the P6.1 timeout. notModified is true for a 304.
func (p *Poller) get(ctx context.Context) (body []byte, etag string, notModified bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.URL, nil)
	if err != nil {
		return nil, "", false, err
	}
	req.Header.Set("Accept", "application/json")
	if p.etag != "" && p.current.Load() != nil {
		req.Header.Set("If-None-Match", p.etag)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, "", false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotModified:
		return nil, "", true, nil
	case http.StatusOK:
	default:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, "", false, fmt.Errorf("authz bundle: HTTP %d", resp.StatusCode)
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, maxBundleBytes+1))
	if err != nil {
		return nil, "", false, err
	}
	if len(body) > maxBundleBytes {
		return nil, "", false, fmt.Errorf("authz bundle: body larger than %d bytes", maxBundleBytes)
	}
	return body, resp.Header.Get("ETag"), false, nil
}
