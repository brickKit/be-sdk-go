package rpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

func TestClientBulkheadRefusesThe65thCallAtOnce(t *testing.T) {
	ts := startServer(t, nil)
	release := make(chan struct{})
	ts.probe.set(func(ctx context.Context, _, id string) (string, error) {
		if id == "block" {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return "ok", nil
	})
	c := newTestConns(t, nil)
	busy := probeClient(t, c, "erp/inventory", ts.addr)

	var wg sync.WaitGroup
	errs := make(chan error, DefaultBulkhead)
	for range DefaultBulkhead {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := busy.Read(context.Background(), &probepb.ProbeRequest{Id: "block"})
			errs <- err
		}()
	}
	require.Eventually(t, func() bool { return ts.probe.calls.Load() == DefaultBulkhead },
		2*time.Second, 5*time.Millisecond)

	start := time.Now()
	_, err := busy.Read(context.Background(), &probepb.ProbeRequest{Id: "free"})
	require.Less(t, time.Since(start), 100*time.Millisecond, "the 65th call is refused at once, never queued")
	require.True(t, problem.Is(err, "be", "OUTBOUND_LIMIT"), "got %v", err)
	require.Equal(t, codes.ResourceExhausted, problem.From(err).Code)
	require.EqualValues(t, DefaultBulkhead, ts.probe.calls.Load(), "the 65th call is never sent")

	// the bulkhead is per dependency: another dependency on the same address is not affected
	_, err = probeClient(t, c, "erp/finance", ts.addr).Read(context.Background(), &probepb.ProbeRequest{Id: "free"})
	require.NoError(t, err)

	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	// slots are given back
	_, err = busy.Read(context.Background(), &probepb.ProbeRequest{Id: "free"})
	require.NoError(t, err)
}

func TestClientBulkheadLimitIsConfigurable(t *testing.T) {
	ts := startServer(t, nil)
	release := make(chan struct{})
	ts.probe.set(func(ctx context.Context, _, _ string) (string, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return "ok", nil
	})
	pc := probeClient(t, newTestConns(t, func(cfg *ClientConfig) { cfg.Bulkhead = 1 }), "d", ts.addr)
	done := make(chan error, 1)
	go func() {
		_, err := pc.Read(context.Background(), &probepb.ProbeRequest{})
		done <- err
	}()
	require.Eventually(t, func() bool { return ts.probe.calls.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
	_, err := pc.Read(context.Background(), &probepb.ProbeRequest{})
	require.True(t, problem.Is(err, "be", "OUTBOUND_LIMIT"), "got %v", err)
	close(release)
	require.NoError(t, <-done)
}
