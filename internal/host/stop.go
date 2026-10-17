package host

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Exit codes (protocol chapter 01).
const (
	ExitOK            = 0
	ExitFailure       = 1
	ExitUsage         = 64
	ExitStartupCheck  = 69
	ExitConfiguration = 78
)

// StopAll is the graceful stop of protocol P1.14-P1.17 for one or more members: every member is not-ready at once,
// the drain delay passes while the listeners still answer, all servers shut down in parallel letting requests
// finish, then every module stops in parallel; telemetry exporters close last. Everything ends by the stop timeout,
// counted from the signal. Returns the exit code.
func StopAll(log *slog.Logger, members []*Member, servers []*http.Server, stopBackground context.CancelFunc,
	drain, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for _, m := range members {
		m.SetState(Stopping)
	}
	log.Info("stopping", "event", "stopping")
	if d := min(drain, time.Until(deadline)); d > 0 {
		time.Sleep(d)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	var mu sync.Mutex
	code := ExitOK
	fail := func(msg string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		log.Warn(msg, args...)
		code = ExitFailure
	}
	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Add(1)
		go func(s *http.Server) {
			defer wg.Done()
			if err := s.Shutdown(ctx); err != nil {
				fail("requests still running at the stop deadline", "addr", s.Addr, "error", err.Error())
			}
		}(s)
	}
	wg.Wait()
	stopBackground()
	for _, m := range members {
		wg.Add(1)
		go func(m *Member) {
			defer wg.Done()
			if err := m.Stop(ctx); err != nil {
				fail("a member did not stop cleanly", "member", m.Def.ID, "error", err.Error())
			}
		}(m)
	}
	wg.Wait()
	for _, m := range members {
		_ = m.Tracers.Shutdown(context.Background())
	}
	log.Info("stopped", "event", "stopped", "exit_code", code)
	return code
}
