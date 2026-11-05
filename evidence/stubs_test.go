package evidence

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stubStore answers as the test sets it, slowly when asked.
type stubStore struct {
	mu    sync.Mutex
	got   []Sealed
	down  bool
	delay time.Duration
	reply func(Sealed) string
}

func (s *stubStore) Append(ctx context.Context, recs []Sealed) ([]string, error) {
	s.mu.Lock()
	down, delay := s.down, s.delay
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if down {
		return nil, errors.New("the evidence store is down")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(recs))
	for i, r := range recs {
		s.got = append(s.got, r)
		out[i] = "accepted"
		if s.reply != nil {
			out[i] = s.reply(r)
		}
	}
	return out, nil
}

type stubAlert struct {
	mu   sync.Mutex
	got  []any
	fail bool
}

func (a *stubAlert) Alert(_ context.Context, body any) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail {
		return false
	}
	a.got = append(a.got, body)
	return true
}

func fields(t *testing.T, s Sealed) map[string]any {
	var m map[string]any
	require.NoError(t, json.Unmarshal(s.Record, &m))
	return m
}

var order = Object{Type: "demo.order", ID: "0190f3a1-0000-7000-8000-0000000000f1", Version: "3"}
