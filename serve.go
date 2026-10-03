package besdk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/httpx"
	"github.com/brickKit/be-sdk-go/internal/telemetry"
	"google.golang.org/grpc"
)

const (
	newTimeout    = 30 * time.Second // the component's New and Start (P1.8)
	shutdownFlush = 5 * time.Second  // telemetry flush after the grace period
)

// process is one standalone component while it serves.
type process struct {
	b        *boot
	rt       *Runtime
	mod      *Module
	sup      *supervisor
	bgCancel context.CancelFunc
	ready    *readiness
	auth     *authSetup
	httpSrv  *http.Server
	httpLn   net.Listener
	grpcSrv  *grpc.Server
	grpcLn   net.Listener
	grace    time.Duration
	closers  []func(ctx context.Context)
	dbm      *telemetry.DBMetrics
	evm      *telemetry.EventMetrics
	bus      *jetstream.Bus
	fatal    chan error // a fatal condition found in the background (P1.8)
}

// serve runs the serving entry point (P1.2): configuration is checked, the ports open, the
// dependencies connect in the background, the module starts; SIGTERM stops it (P1.6).
func serve(ctx context.Context, b *boot, pe processEnv) int {
	p := &process{b: b, grace: optDuration(b.vals, "SHUTDOWN_GRACE", 25*time.Second), fatal: make(chan error, 1)}
	if code := p.open(pe); code != exitOK {
		return code
	}
	bg, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p.bgCancel = cancel
	p.sup = newSupervisor(bg, b.log)
	if code := p.build(bg); code != exitOK {
		p.stop()
		return code
	}
	p.startServers()
	if err := p.startModule(bg); err != nil {
		b.log.Error("module start failed", slog.String("error", err.Error()))
		p.stop()
		return exitFailure
	}
	b.log.Info("serving", slog.Int("http_port", portOf(p.httpLn)), slog.Int("grpc_port", portOf(p.grpcLn)))
	select {
	case <-ctx.Done():
		b.log.Info("stopping", slog.Duration("grace", p.grace))
		p.stop()
		return exitOK
	case err := <-p.fatal:
		b.log.Error("fatal; stopping", slog.String("error", err.Error()))
		p.stop()
		return exitFailure
	}
}

// fail reports a fatal condition found by background work (P1.8); the first one stops the process.
func (p *process) fail(err error) {
	select {
	case p.fatal <- err:
	default:
	}
}

// open opens the secrets and the ports before anything connects (P1.2 step 2).
func (p *process) open(pe processEnv) int {
	if code := p.openRuntime(); code != exitOK {
		return code
	}
	port := p.b.man.Deployment.Port
	if v, ok := pe.lookup("PORT"); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			port = n
		}
	}
	var err error
	if p.httpLn, err = httpx.Listen(port); err != nil {
		p.b.log.Error("cannot open the main port", slog.String("error", err.Error()))
		return exitFailure
	}
	if gp := p.b.man.extraPort("grpc"); gp > 0 {
		if p.grpcLn, err = httpx.Listen(gp); err != nil {
			p.b.log.Error("cannot open the grpc port", slog.String("error", err.Error()))
			return exitFailure
		}
	}
	return exitOK
}

// openRuntime opens the secret files (P2.12) and builds the component's Runtime.
func (p *process) openRuntime() int {
	secrets, errs := openSecrets(p.b.vals, p.secretOptions, "PG_OWNER_PASSWORD_FILE")
	if len(errs) > 0 {
		logConfigErrors(p.b.log, &configFailure{errs: errs})
		return exitConfig
	}
	p.rt = &Runtime{id: p.b.id, version: p.b.version, cfg: &Config{vals: p.b.vals, secrets: secrets},
		log: p.b.log, tel: p.b.member, catalogue: p.b.catalogue, locale: p.b.locale, clock: time.Now}
	return exitOK
}

func (p *process) secretOptions(key string) config.SecretOptions {
	return config.SecretOptions{
		OnChange: func(k string) { p.b.log.Info("secret file changed; re-read", slog.String("key", k)) },
		OnError: func(k string, err error) {
			p.b.log.Error("secret file re-read failed; keeping the last good value", slog.String("key", k),
				slog.String("error", err.Error()))
			p.b.secretM.ReloadFailures.WithLabelValues(k).Inc()
		},
	}
}

// build wires the runtime's dependencies, calls the component's New and assembles the servers.
func (p *process) build(bg context.Context) int {
	var err error
	if p.auth, err = newAuthSetup(p.b, p.rt); err != nil {
		p.b.log.Error("authorization setup failed", slog.String("error", err.Error()))
		return exitConfig
	}
	conds := p.readinessConditions()
	p.ready = newReadiness(conds...)
	if err := p.wireDeps(bg); err != nil {
		p.b.log.Error("start failed", slog.String("error", err.Error()))
		return exitFailure
	}
	p.mod, err = callNew(bg, p.b.spec, p.rt)
	if err != nil {
		return p.failCode("component New failed", err)
	}
	if err := p.wireEvents(); err != nil {
		return p.failCode("events", err)
	}
	if err := p.wireJobs(); err != nil {
		return p.failCode("jobs", err)
	}
	if err := p.assembleHTTP(); err != nil {
		return p.failCode("routes", err)
	}
	if err := p.assembleGRPC(); err != nil {
		return p.failCode("grpc", err)
	}
	p.superviseBackground()
	return exitOK
}

func (p *process) failCode(what string, err error) int {
	var cfgErr *config.Error
	if errors.As(err, &cfgErr) {
		logConfigErrors(p.b.log, &configFailure{errs: []*config.Error{cfgErr}})
		return exitConfig
	}
	p.b.log.Error(what, slog.String("error", err.Error()))
	return exitFailure
}

// callNew runs the component's New within 30 s; a configuration panic (a getter on an undeclared or
// unparsable key) comes back as its *config.Error (exit 78), any other panic as an error (P1.8).
func callNew(ctx context.Context, s Spec, rt *Runtime) (mod *Module, err error) {
	ctx, cancel := context.WithTimeout(ctx, newTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			if ce, ok := config.AsError(r); ok {
				err = ce
				return
			}
			err = fmt.Errorf("panic in New: %v", r)
		}
	}()
	if s.New == nil {
		return nil, fmt.Errorf("Spec.New is nil")
	}
	mod, err = s.New(ctx, rt)
	if err == nil && mod == nil {
		err = fmt.Errorf("New returned no module")
	}
	return mod, err
}

func (p *process) startModule(ctx context.Context) error {
	if p.mod.Start == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, newTimeout)
	defer cancel()
	return p.mod.Start(ctx)
}

func portOf(ln net.Listener) int {
	if ln == nil {
		return 0
	}
	if a, ok := ln.Addr().(*net.TCPAddr); ok {
		return a.Port
	}
	return 0
}
