package identity

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentRequestsWaitForTheRefreshInFlight: the identity member was not up when the process started (the
// first fetch failed); the first burst of requests meets no keys at all. One refresh is made and every request of the
// burst waits for it, instead of the ones after the first answering DEPENDENCY_UNAVAILABLE at once (P8.2; step 13
// smoke, K13-E1: two of eight confirmations on a quiet sales replica failed so).
func TestConcurrentRequestsWaitForTheRefreshInFlight(t *testing.T) {
	f, cases := loadJWT(t)
	var valid string
	for _, raw := range cases {
		var c struct {
			Token  string
			Expect struct{ Valid bool }
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		if c.Expect.Valid && valid == "" {
			valid = c.Token
		}
	}
	var up atomic.Bool
	var hits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		if !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		time.Sleep(200 * time.Millisecond) // the refresh takes a while: the burst arrives meanwhile
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(f.JWKS)
	}))
	t.Cleanup(s.Close)
	keys, err := NewKeySet(context.Background(), s.URL)
	require.Error(t, err, "the identity member is not up yet")
	up.Store(true)
	v := &Verifier{Issuer: f.Issuer, Audience: f.Audience, Keys: keys, Now: func() time.Time { return time.Unix(f.Now, 0) }}
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = v.Verify(valid)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		assert.NoError(t, err, "request %d", i)
	}
	assert.Equal(t, int32(2), hits.Load(), "the failed first fetch and one refresh for the whole burst")
}
