// Package acl keeps a component's projection of direct authorization tuples (be-protocol P6.12,
// ddl/07-authz-projection.sql): it pulls the provider's changefeed into besdk_authz_acl and
// besdk_authz_cursor, rebuilds from a snapshot after a 410, answers the consistency token of P6.11 and
// loads one record's rows for a single-record decision (EVALUATION.md E10). The projection holds
// direct tuples only; subject-side expansion happens at query time (P6.4).
package acl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// Defaults from be-protocol P6.11 and P6.12.
const (
	defaultInterval   = 5 * time.Second
	defaultTimeout    = 3 * time.Second
	defaultWaitBudget = 300 * time.Millisecond
	defaultPageSize   = 1000 // tuples page_size, the contract's maximum
	changesLimit      = 500
	maxBodyBytes      = 16 << 20
)

// ErrCapabilityUnavailable: the provider answered 501, it offers neither sharing nor relation_sync
// (capabilities.yaml). The projection stays empty and @s_acl stays false through the bundle.
var ErrCapabilityUnavailable = errors.New("acl: provider lacks the sharing capability")

// errExpired is the provider's 410 CHANGES_EXPIRED.
var errExpired = errors.New("acl: changefeed position expired")

// Config configures a Projection. Zero durations take the protocol defaults.
type Config struct {
	URL      string        // AUTHZ_URL, the provider's base URL
	Caller   string        // the component ID, sent as be-caller
	Types    []string      // the pulled type set: own types plus inherited ones (Catalog.PulledTypes)
	Store    *pg.Store     // the component's store; the projection tables live in its schema
	Client   *http.Client  // nil: a client with its own transport
	Interval time.Duration // between pulls in Run (default 5 s)
	Timeout  time.Duration // one fetch (default 3 s)
	// WaitBudget bounds WaitFor's synchronous pull (default 300 ms, P6.11).
	WaitBudget time.Duration
	PageSize   int // tuples page_size while rebuilding (default 1000)
	Logger     *slog.Logger
	// Active is asked before every pull; false skips it (for example while the bundle offers neither
	// sharing nor relation_sync). nil means always.
	Active func() bool
}

// Projection is one component's ACL projection (P6.12). Sync, Poke, Watermark and WaitFor are safe from
// any goroutine; Run is called once by the caller's supervisor.
type Projection struct {
	cfg       Config
	client    *http.Client
	log       *slog.Logger
	scope     string // besdk_authz_cursor.scope: the sorted type set joined by ","
	types     []string
	sem       chan struct{} // one pull at a time in this process
	poke      chan struct{}
	watermark atomic.Int64
}

// New checks the configuration.
func New(c Config) (*Projection, error) {
	if c.Store == nil {
		return nil, errors.New("acl: a store is required")
	}
	if c.URL == "" {
		return nil, errors.New("acl: AUTHZ_URL is required")
	}
	c.URL = strings.TrimRight(c.URL, "/")
	c.Interval = orDefault(c.Interval, defaultInterval)
	c.Timeout = orDefault(c.Timeout, defaultTimeout)
	c.WaitBudget = orDefault(c.WaitBudget, defaultWaitBudget)
	if c.PageSize <= 0 || c.PageSize > defaultPageSize {
		c.PageSize = defaultPageSize
	}
	types := slices.Clone(c.Types)
	slices.Sort(types)
	types = slices.Compact(types)
	p := &Projection{cfg: c, client: c.Client, log: c.Logger, types: types, scope: strings.Join(types, ","),
		sem: make(chan struct{}, 1), poke: make(chan struct{}, 1)}
	if p.client == nil {
		p.client = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	if p.log == nil {
		p.log = slog.New(slog.DiscardHandler)
	}
	return p, nil
}

func orDefault(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}

// Watermark is the last revision this process knows the projection reached: every change at or below
// it is applied (P6.11).
func (p *Projection) Watermark() int64 { return p.watermark.Load() }

// Poke asks Run for an immediate pull, as on infra.authz.changed.v1 (P6.12). Pokes coalesce; Poke
// never blocks.
func (p *Projection) Poke() {
	select {
	case p.poke <- struct{}{}:
	default:
	}
}

// Run pulls at start, then every Interval and on every poke, until ctx ends; it returns nil then. A
// failed pull is logged and retried at the next tick (P6.12). This is the be.authz.changes job.
func (p *Projection) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	for {
		p.logSync(ctx, p.Sync(ctx))
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-p.poke:
		}
	}
}

func (p *Projection) logSync(ctx context.Context, err error) {
	switch {
	case err == nil, ctx.Err() != nil:
	case errors.Is(err, ErrCapabilityUnavailable):
		p.log.DebugContext(ctx, "authz changefeed not offered; the projection stays empty")
	default:
		p.log.WarnContext(ctx, "authz changefeed pull failed; retrying at the next tick", "error", err.Error())
	}
}

// WaitFor is P6.11's consistency token: true when the projection has reached revision, after at most
// one synchronous pull bounded by WaitBudget. False means still behind: a single read falls back to the
// provider's Check with at_least = revision, a list answers X-Authz-Consistency: stale.
func (p *Projection) WaitFor(ctx context.Context, revision int64) bool {
	return p.WaitForWithin(ctx, revision, p.cfg.WaitBudget)
}

// WaitForWithin is WaitFor with the caller's budget: a _shares write answers only once the projection
// reached the revision the provider returned (P6.11).
func (p *Projection) WaitForWithin(ctx context.Context, revision int64, budget time.Duration) bool {
	if p.watermark.Load() >= revision {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if rev, err := p.readCursor(ctx); err == nil && rev >= revision {
		p.advance(rev)
		return true
	}
	_ = p.Sync(ctx)
	return p.watermark.Load() >= revision
}

// advance raises the in-memory watermark, never lowers it.
func (p *Projection) advance(rev int64) {
	for {
		cur := p.watermark.Load()
		if rev <= cur || p.watermark.CompareAndSwap(cur, rev) {
			return
		}
	}
}

// Sync pulls until caught up (P6.12): pages of changes after the cursor, each applied with the cursor
// in one transaction, following next; once a page is short the cursor advances to the watermark. A 410
// rebuilds from the snapshot and continues after its revision. It never holds a transaction across a
// network call (P8.4).
func (p *Projection) Sync(ctx context.Context) error {
	if len(p.types) == 0 || (p.cfg.Active != nil && !p.cfg.Active()) {
		return nil
	}
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	rebuilt := false
	for {
		after, err := p.readCursor(ctx)
		if err != nil {
			return err
		}
		p.advance(after)
		page, err := p.fetchChanges(ctx, after)
		switch {
		case errors.Is(err, errExpired) && !rebuilt:
			rebuilt = true
			if err := p.rebuild(ctx, after); err != nil {
				return err
			}
			continue
		case err != nil:
			return err
		}
		caughtUp := len(page.changes) < changesLimit
		to := page.next
		if caughtUp {
			to = max(page.next, page.watermark)
		} else if page.next <= after {
			return fmt.Errorf("acl: changefeed page of %d changes did not advance past %d", len(page.changes), after)
		}
		rev, err := p.apply(ctx, page.changes, to)
		if err != nil {
			return err
		}
		p.advance(rev)
		if caughtUp {
			return nil
		}
	}
}

// fetchChanges is one GET /authz/v2/changes.
func (p *Projection) fetchChanges(ctx context.Context, after int64) (changesPage, error) {
	body, err := p.get(ctx, p.changesURL(after))
	if err != nil {
		return changesPage{}, err
	}
	return decodeChanges(body)
}

func (p *Projection) changesURL(after int64) string {
	q := url.Values{"types": {p.scope}, "after": {strconv.FormatInt(after, 10)}, "limit": {strconv.Itoa(changesLimit)}}
	return p.cfg.URL + "/authz/v2/changes?" + q.Encode()
}

func (p *Projection) tuplesURL(typ, cursor string) string {
	q := url.Values{"type": {typ}, "cursor": {cursor}, "page_size": {strconv.Itoa(p.cfg.PageSize)}}
	return p.cfg.URL + "/authz/v2/tuples?" + q.Encode()
}

// get sends one request with the fetch timeout: 200 returns the body, 410 errExpired, 501
// ErrCapabilityUnavailable, anything else an error.
func (p *Projection) get(ctx context.Context, u string) ([]byte, error) {
	if err := pg.GuardNetwork(ctx, "authz changefeed"); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if p.cfg.Caller != "" {
		req.Header.Set("be-caller", p.cfg.Caller)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusGone:
		return nil, errExpired
	case http.StatusNotImplemented:
		return nil, ErrCapabilityUnavailable
	default:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("acl: %s: HTTP %d", req.URL.Path, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBodyBytes {
		return nil, fmt.Errorf("acl: %s: body larger than %d bytes", req.URL.Path, maxBodyBytes)
	}
	return body, nil
}
