package launcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	sdk "github.com/brickKit/be-sdk-go/v3"
	"github.com/brickKit/be-sdk-go/v3/clock"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/runtime"
)

const (
	defaultDrainDelay  = 2 * time.Second
	defaultStopTimeout = 30 * time.Second
	secretRereadEvery  = 30 * time.Second
	stateStarting      = "starting"
	stateReady         = "ready"
	stateStopping      = "stopping"
)

// ListenFunc opens the user-surface listener.
type ListenFunc func(addr string) (net.Listener, error)

func reportConfig(w interface{ Write([]byte) (int, error) }, member string, problems []config.Problem) {
	for _, p := range problems {
		line, _ := json.Marshal(map[string]string{"level": "ERROR", "msg": "configuration", "component": member,
			"key": p.Key, "kind": p.Kind, "expected": p.Expected, "got": p.Got})
		fmt.Fprintln(w, string(line))
	}
}

func serve(ctx context.Context, def component.Definition, o Options) int {
	started := time.Now()
	values, plat, problems := parseAll(def, o.Env)
	if len(problems) > 0 {
		reportConfig(o.Stderr, def.ID, problems)
		return ExitConfiguration
	}
	member := obs.Member{ID: def.ID, Version: def.Version}
	log := obs.NewLogger(member, obs.ParseLevel(values.String("LOG_LEVEL")), o.Stdout)
	if err := clock.Check(); err != nil {
		log.Error("start check failed: no clock that counts suspension (CLOCK_BOOTTIME)", "error", err.Error())
		return ExitStartupCheck
	}

	tp, err := obs.NewTracerProvider(ctx, member, values.String("OTEL_URL"))
	if err != nil {
		log.Error("tracing exporter", "error", err.Error())
		return ExitConfiguration
	}
	defer func() { _ = tp.Shutdown(context.Background()) }()
	registry := obs.NewRegistry()
	rt := runtime.New(member, values, log, registry, tp.Tracer(def.ID), o.Env)

	mod, err := build(def, rt)
	if err != nil {
		log.Error("the module could not be built", "error", err.Error())
		return ExitFailure
	}
	var routes httpx.Routes
	if err := mod.Routes(&routes); err != nil {
		log.Error("the module's routes could not be registered", "error", err.Error())
		return ExitFailure
	}

	var state atomic.Value
	state.Store(stateStarting)
	engine, err := httpx.NewEngine(httpx.Deps{Member: member, Logger: log, Registry: registry, Tracer: rt.Tracer,
		Platform: httpx.Platform{
			Ready: func() (bool, string, []string) {
				s := state.Load().(string)
				return s == stateReady, s, nil
			},
			Describe: func() any {
				return map[string]any{"component_id": def.ID, "version": def.Version, "protocol": protocol.Version,
					"sdk": "be-sdk-go/" + sdk.Version, "capabilities": nonNil(def.Capabilities)}
			},
		}}, &routes)
	if err != nil {
		log.Error("the user surface could not be built", "error", err.Error())
		return ExitFailure
	}

	listen := o.Listen
	if listen == nil {
		listen = func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }
	}
	ln, err := listen(":" + strconv.FormatInt(plat.Int("PORT"), 10))
	if err != nil {
		log.Error("cannot listen on PORT", "error", err.Error())
		return ExitFailure
	}
	srv := &http.Server{Handler: engine, ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	bg, stopBg := context.WithCancel(context.Background())
	defer stopBg()
	if s, ok := mod.(component.Starter); ok {
		if err := s.Start(bg); err != nil {
			log.Error("the module could not start", "error", err.Error())
			_ = srv.Close()
			return ExitFailure
		}
	}
	go rereadSecrets(bg, values, o.Env, log)

	state.Store(stateReady)
	log.Info("ready", "event", "ready", "protocol", protocol.Version, "sdk", sdk.Version,
		"startup_ms", time.Since(started).Milliseconds())

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		log.Error("the user surface stopped unexpectedly", "error", fmt.Sprint(err))
		return ExitFailure
	}
	return stop(log, &state, srv, mod, stopBg, values)
}

// stop: not ready at once, wait the drain delay, close the listener and let requests finish, stop the module; all
// within the stop timeout counted from the signal (P1.14, P1.15).
func stop(log *slog.Logger, state *atomic.Value, srv *http.Server, mod component.Module, stopBg context.CancelFunc,
	values *config.Values) int {
	state.Store(stateStopping)
	deadline := time.Now().Add(values.Duration("STOP_TIMEOUT"))
	log.Info("stopping", "event", "stopping")
	if d := min(values.Duration("STOP_DRAIN_DELAY"), time.Until(deadline)); d > 0 {
		time.Sleep(d)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	code := ExitOK
	if err := srv.Shutdown(ctx); err != nil {
		log.Warn("requests still running at the stop deadline", "error", err.Error())
		code = ExitFailure
	}
	stopBg()
	if s, ok := mod.(component.Stopper); ok {
		if err := s.Stop(ctx); err != nil {
			log.Warn("the module did not stop cleanly", "error", err.Error())
			code = ExitFailure
		}
	}
	log.Info("stopped", "event", "stopped", "exit_code", code)
	return code
}

// build calls the module entry, turning a panic into an error: a module never takes the process down.
func build(def component.Definition, rt *runtime.Runtime) (m component.Module, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic in New: %v", p)
		}
	}()
	if def.New == nil {
		return nil, errors.New("the definition has no New")
	}
	return def.New(rt)
}

func rereadSecrets(ctx context.Context, v *config.Values, env config.Env, log *slog.Logger) {
	secrets := v.Secrets()
	if len(secrets) == 0 {
		return
	}
	t := time.NewTicker(secretRereadEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, s := range secrets {
				if changed, err := s.Refresh(env.ReadFile); err != nil {
					log.Warn("a secret file could not be re-read; keeping the previous value", "error", err.Error())
				} else if changed {
					log.Info("a secret file changed; new connections use the new value")
				}
			}
		}
	}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
