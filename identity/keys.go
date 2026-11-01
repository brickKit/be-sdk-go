// Package identity verifies who is calling (protocol P8.1-P8.6): user access tokens against the identity member's
// published keys, and service identities by the deployment's mTLS certificates.
package identity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// Key-set parameters (P8.2).
const (
	KeyRefresh        = 10 * time.Minute
	UnknownKIDRefresh = 30 * time.Second
	KeyMaxAge         = time.Hour
)

// Errors of the key set.
var (
	errUnknownKID      = errors.New("unknown kid")
	ErrKeysUnavailable = errors.New("identity: the issuer's keys could not be obtained recently enough")
)

// KeySet holds the issuer's public keys, fetched only from the configured URL. One per process (protocol 17).
type KeySet struct {
	url    string
	client *http.Client
	now    func() time.Time

	mu          sync.Mutex
	kf          keyfunc.Keyfunc
	kids        map[string]bool
	fetchedAt   time.Time
	lastUnknown time.Time
	inflight    chan struct{} // closed when the refresh an unknown kid started has ended; nil when none runs
}

// NewKeySet creates the key set and fetches the keys once; a failing first fetch is reported but not fatal (the
// identity member may start after the component).
func NewKeySet(ctx context.Context, url string) (*KeySet, error) {
	k := &KeySet{url: url, client: &http.Client{Timeout: 10 * time.Second}, now: time.Now}
	err := k.refresh(ctx)
	go k.loop(ctx)
	return k, err
}

func (k *KeySet) loop(ctx context.Context) {
	t := time.NewTicker(KeyRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = k.refresh(ctx)
		}
	}
}

func (k *KeySet) refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.url, nil)
	if err != nil {
		return err
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetching the issuer's keys: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetching the issuer's keys: status %d %v", resp.StatusCode, err)
	}
	var set struct{ Keys []json.RawMessage }
	if err := json.Unmarshal(body, &set); err != nil {
		return fmt.Errorf("parsing the issuer's keys: %w", err)
	}
	// a key this code does not understand is skipped and the rest are used (P8.2, RFC 7517 section 5): one key of a
	// new type must not make every token unverifiable
	var usable []json.RawMessage
	kids := map[string]bool{}
	for _, key := range set.Keys {
		one, _ := json.Marshal(map[string]any{"keys": []json.RawMessage{key}})
		var k struct{ Kid string }
		if _, err := keyfunc.NewJWKSetJSON(one); err != nil || json.Unmarshal(key, &k) != nil || k.Kid == "" {
			continue
		}
		usable, kids[k.Kid] = append(usable, key), true
	}
	all, _ := json.Marshal(map[string]any{"keys": usable})
	kf, err := keyfunc.NewJWKSetJSON(all)
	if err != nil {
		return fmt.Errorf("parsing the issuer's keys: %w", err)
	}
	k.mu.Lock()
	k.kf, k.kids, k.fetchedAt = kf, kids, k.now()
	k.mu.Unlock()
	return nil
}

// keyfunc resolves a token's key: an unknown kid refreshes the set at most once per 30 s, and every request that
// meets an unknown kid while that refresh runs waits for it (P8.2); keys older than an hour are not used.
func (k *KeySet) keyfunc(t *jwt.Token) (any, error) {
	kid, _ := t.Header["kid"].(string)
	k.mu.Lock()
	known := k.kids[kid]
	wait := k.inflight
	if !known && wait == nil && k.now().Sub(k.lastUnknown) >= UnknownKIDRefresh {
		k.lastUnknown = k.now()
		wait = make(chan struct{})
		k.inflight = wait
		go func() { // the key set's refresh, not the request's: bounded by the client's timeout
			_ = k.refresh(context.Background())
			k.mu.Lock()
			k.inflight = nil
			k.mu.Unlock()
			close(wait)
		}()
	}
	k.mu.Unlock()
	if !known && wait != nil {
		<-wait
	}
	k.mu.Lock()
	kf, fresh, known := k.kf, k.now().Sub(k.fetchedAt) <= KeyMaxAge, k.kids[kid]
	k.mu.Unlock()
	if kf == nil || !fresh {
		return nil, ErrKeysUnavailable
	}
	if !known {
		return nil, errUnknownKID
	}
	return kf.Keyfunc(t)
}
