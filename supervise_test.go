package besdk

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testSupervisor(t *testing.T) (*supervisor, *bytes.Buffer, context.CancelFunc) {
	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	s := newSupervisor(ctx, slog.New(slog.NewJSONHandler(&syncWriter{w: &buf}, nil)))
	s.minBackoff, s.maxBackoff = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { cancel(); s.Wait() })
	return s, &buf, cancel
}

func TestSupervisorRestartsFailingAndPanickingWork(t *testing.T) {
	s, buf, cancel := testSupervisor(t)
	var fails, panics atomic.Int32
	s.Go("fails", func(ctx context.Context) error {
		if fails.Add(1) < 3 {
			return errors.New("boom")
		}
		<-ctx.Done()
		return nil
	})
	s.Go("panics", func(ctx context.Context) error {
		if panics.Add(1) < 3 {
			panic("kaboom")
		}
		<-ctx.Done()
		return nil
	})
	deadline := time.Now().Add(2 * time.Second)
	for (fails.Load() < 3 || panics.Load() < 3) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fails.Load() < 3 || panics.Load() < 3 {
		t.Fatalf("work not restarted: fails=%d panics=%d", fails.Load(), panics.Load())
	}
	cancel()
	s.Wait()
	if !strings.Contains(buf.String(), "kaboom") || !strings.Contains(buf.String(), "boom") {
		t.Fatalf("failures not logged: %s", buf.String())
	}
}

func TestSupervisorOneStoppingNeverStopsAnother(t *testing.T) {
	s, _, cancel := testSupervisor(t)
	var ticks atomic.Int32
	s.Go("done-at-once", func(ctx context.Context) error { return nil })
	s.Go("ticker", func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Millisecond):
				ticks.Add(1)
			}
		}
	})
	time.Sleep(50 * time.Millisecond)
	if ticks.Load() == 0 {
		t.Fatal("ticker stopped")
	}
	cancel()
	s.Wait()
}

func TestSupervisorBackoffDoublesAndCaps(t *testing.T) {
	s, _, _ := testSupervisor(t)
	s.minBackoff, s.maxBackoff = time.Second, 5*time.Minute
	got := []time.Duration{}
	d := time.Duration(0)
	for i := 0; i < 12; i++ {
		d = s.nextBackoff(d)
		got = append(got, d)
	}
	if got[0] != time.Second || got[1] != 2*time.Second || got[11] != 5*time.Minute {
		t.Fatalf("backoff %v", got)
	}
}
