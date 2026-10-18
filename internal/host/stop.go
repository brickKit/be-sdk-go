package host

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"google.golang.org/grpc"
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
func StopAll(log *slog.Logger, members []*Member, servers []Server, stopBackground context.CancelFunc,
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
		go func(s Server) {
			defer wg.Done()
			if err := s.Shutdown(ctx); err != nil {
				fail("requests still running at the stop deadline", "server", s.Name(), "error", err.Error())
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

// Server is a listener the stop shuts down: the user surface (HTTP) or the system surface (gRPC).
type Server interface {
	Shutdown(ctx context.Context) error
	Close() error
	Name() string
}

// ServerIdleTimeout keeps an idle keep-alive connection open longer than any caller's client does (P1.20): a server
// that closes first races the client reusing the connection, and a request that cannot be retried fails with EOF.
const ServerIdleTimeout = 120 * time.Second

// NewHTTPServer is the HTTP server of every surface that listens: request headers within 10 s, idle connections kept
// for ServerIdleTimeout.
func NewHTTPServer(h http.Handler) *http.Server {
	return &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: ServerIdleTimeout}
}

// HTTPServer adapts an *http.Server.
type HTTPServer struct{ *http.Server }

// Name names the server in logs.
func (s HTTPServer) Name() string { return "http " + s.Addr }

// GRPCServer adapts a *grpc.Server: a graceful stop until ctx ends, then a hard stop.
type GRPCServer struct {
	*grpc.Server
	Addr string
}

// Name names the server in logs.
func (s GRPCServer) Name() string { return "grpc " + s.Addr }

// Shutdown stops accepting calls and waits for the running ones until ctx ends.
func (s GRPCServer) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() { s.GracefulStop(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.Stop()
		return ctx.Err()
	}
}

// Close stops at once.
func (s GRPCServer) Close() error { s.Stop(); return nil }

// ReadyExit maps a failed database readiness wait to the exit code of protocol chapter 01.
func ReadyExit(err error) int {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ExitOK // stopped while waiting for the database
	case errors.Is(err, db.ErrCredentials):
		return ExitConfiguration
	case errors.Is(err, db.ErrStartCheck):
		return ExitStartupCheck
	}
	return ExitFailure
}
