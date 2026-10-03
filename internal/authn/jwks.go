package authn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Defaults from be-protocol P5.4.
const (
	defaultRefreshEvery = time.Hour        // the JWKS is cached for at most 1 h
	defaultRefetchGap   = 30 * time.Second // an unknown kid refetches at most once per 30 s
	defaultFetchTimeout = 3 * time.Second  // each fetch
	defaultBackoffBase  = 500 * time.Millisecond
	defaultBackoffMax   = 15 * time.Second
	// maxJWKSBytes bounds one JWKS body (at most three keys, TOKENS.md); larger is a failed fetch.
	maxJWKSBytes = 1 << 20
)

// keyFor returns the key for kid (P5.4). An unknown kid triggers one refetch when the last fetch
// attempt is at least refetchGap old (by the injected clock); a failed refetch keeps the keys held.
func (v *Verifier) keyFor(ctx context.Context, kid string) (publicKey, bool) {
	if k, ok := (*v.keys.Load())[kid]; ok {
		return k, true
	}
	v.fetchMu.Lock()
	defer v.fetchMu.Unlock()
	if k, ok := (*v.keys.Load())[kid]; ok { // another caller fetched while this one waited
		return k, true
	}
	if !v.lastFetch.IsZero() && v.now().Sub(v.lastFetch) < v.refetchGap {
		return publicKey{}, false
	}
	v.fetchLocked(ctx, "unknown kid")
	k, ok := (*v.keys.Load())[kid]
	return k, ok
}

// refresh fetches the JWKS once, serialised with every other fetch; it reports success.
func (v *Verifier) refresh(ctx context.Context, why string) bool {
	v.fetchMu.Lock()
	defer v.fetchMu.Unlock()
	return v.fetchLocked(ctx, why)
}

// fetchLocked performs one fetch with fetchMu held. Failure keeps the keys held (fail-static).
func (v *Verifier) fetchLocked(ctx context.Context, why string) bool {
	v.lastFetch = v.now()
	keys, err := v.get(ctx)
	if err != nil {
		v.log.WarnContext(ctx, "jwks fetch failed; keeping the keys held", "reason", why, "error", err.Error())
		return false
	}
	v.keys.Store(&keys)
	v.ready.Store(true)
	return true
}

// get sends one GET with the P5.4 timeout and parses the body.
func (v *Verifier) get(ctx context.Context) (map[string]publicKey, error) {
	ctx, cancel := context.WithTimeout(ctx, v.fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.cfg.JWKSURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("jwks: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxJWKSBytes {
		return nil, fmt.Errorf("jwks: body larger than %d bytes", maxJWKSBytes)
	}
	return parseJWKS(body)
}

// Run loads the JWKS and keeps it fresh until ctx ends, then returns nil (P5.4). The first load
// retries with a backoff from 0.5 s doubling to 15 s; afterwards the keys are refetched every hour,
// so none is cached longer than 1 h. The caller supervises it.
func (v *Verifier) Run(ctx context.Context) error {
	delay := v.backoffBase
	for !v.Ready() {
		if v.refresh(ctx, "initial load") {
			break
		}
		if !sleep(ctx, delay) {
			return nil
		}
		delay = min(delay*2, v.backoffMax)
	}
	ticker := time.NewTicker(v.refreshEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			v.refresh(ctx, "periodic refresh")
		}
	}
}

// Ready reports whether keys have been loaded at least once (P1.4 readiness input).
func (v *Verifier) Ready() bool { return v.ready.Load() }

// sleep waits d; it returns false when ctx ends first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
