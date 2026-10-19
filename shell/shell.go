// Package shell is the shell launcher (protocol 17, "shell obligations"): one process hosting the members it compiles
// in, each on its own port with its own runtime, configuration, logger, metrics and tracer; nothing but the process is
// merged. It refuses to start, naming the members, when what BrickKit asks it to host does not match what is compiled
// in, or when any member's configuration is invalid.
package shell

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	sdk "github.com/brickKit/be-sdk-go/v3"
	"github.com/brickKit/be-sdk-go/v3/clock"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/internal/host"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Identity is the shell's own component identity.
type Identity struct {
	ID      string // e.g. be/go-core
	Version string
}

// Options are the process-facing inputs of Run.
type Options struct {
	Args   []string
	Env    config.Env
	Stdout io.Writer
	Stderr io.Writer
	Listen func(addr string) (net.Listener, error)
	// BuildInfo and ModuleOf are replaced in tests; by default the binary's own build info is used.
	BuildInfo func() (*debug.BuildInfo, bool)
	ModuleOf  func(component.Definition, *debug.BuildInfo) (*debug.Module, string)
}

// Main runs the shell as a process and exits with its exit code.
func Main(self Identity, members ...component.Definition) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	os.Exit(Run(ctx, self, members, Options{Args: os.Args[1:], Env: config.ProcessEnv(), Stdout: os.Stdout, Stderr: os.Stderr}))
}

var shellKeys = []config.Key{
	{Name: "COMPONENT_ID", Type: config.String, Required: true},
	{Name: "COMPONENT_VERSION", Type: config.String, Required: true},
	{Name: "PORT", Type: config.Integer, Required: true},
	{Name: "BRICKKIT_SERVED_MEMBERS_CONFIG", Type: config.String, Required: true},
	{Name: "SHELL_ALLOW_REPLACE", Type: config.Boolean, Default: false},
	// the shell's login role for its shared pools; it holds the members' runtime roles WITH INHERIT FALSE and switches
	// to one per transaction (protocol 14, "the shell's shared pool"); required when a member owns a database
	{Name: "PG_SHELL_USER", Type: config.String},
	{Name: "PG_SHELL_PASSWORD", Type: config.String, Secret: true},
	{Name: "LOG_LEVEL", Type: config.Enum, Values: []string{"debug", "info", "warn", "error"}, Default: "info"},
	{Name: "OTEL_URL", Type: config.String},
	{Name: "STOP_DRAIN_DELAY", Type: config.Duration, Default: host.DefaultDrainDelay},
	{Name: "STOP_TIMEOUT", Type: config.Duration, Default: host.DefaultStopTimeout},
}

// Run runs the shell until ctx is cancelled and returns the exit code.
func Run(ctx context.Context, self Identity, defs []component.Definition, o Options) int {
	if len(o.Args) > 0 && o.Args[0] != "serve" {
		obs.NewLogger(obs.Member{ID: self.ID, Version: self.Version}, slog.LevelInfo, o.Stderr).
			Error("a shell only serves; migrations run from each member's own image", "entry", o.Args[0])
		return host.ExitUsage
	}
	started := time.Now()
	sv, problems := config.ParsePlatform(shellKeys, o.Env)
	if sv != nil {
		if id := sv.String("COMPONENT_ID"); id != self.ID {
			problems = append(problems, config.Problem{Key: "COMPONENT_ID", Kind: "invalid", Expected: self.ID, Got: id})
		}
		if v := sv.String("COMPONENT_VERSION"); v != self.Version {
			problems = append(problems, config.Problem{Key: "COMPONENT_VERSION", Kind: "invalid", Expected: self.Version, Got: v})
		}
	}
	if len(problems) > 0 {
		host.ReportConfig(o.Stderr, obs.Member{ID: self.ID, Version: self.Version}, problems)
		return host.ExitConfiguration
	}
	buildInfo, modOf := o.BuildInfo, o.ModuleOf
	if buildInfo == nil {
		buildInfo = debug.ReadBuildInfo
	}
	if modOf == nil {
		modOf = moduleOf
	}
	bi, _ := buildInfo()
	plans, refusals, warnings := check(sv.String("BRICKKIT_SERVED_MEMBERS_CONFIG"), defs, bi, modOf, sv.Bool("SHELL_ALLOW_REPLACE"))
	if needsDB(plans) && (sv.String("PG_SHELL_USER") == "" || sv.Secret("PG_SHELL_PASSWORD") == nil) {
		refusals = append(refusals, refusal{self.ID, "PG_SHELL_USER", "missing",
			"the shell's login role and PG_SHELL_PASSWORD: a hosted member owns a database"})
	}
	if len(refusals) > 0 {
		var ps []config.Problem
		for _, r := range refusals {
			ps = append(ps, r.problem())
		}
		host.ReportConfig(o.Stderr, obs.Member{ID: self.ID, Version: self.Version}, ps)
		return host.ExitConfiguration
	}

	me := obs.Member{ID: self.ID, Version: self.Version}
	log := obs.NewLogger(me, obs.ParseLevel(sv.String("LOG_LEVEL")), o.Stdout)
	for _, w := range warnings {
		log.Warn("shell built for development", "detail", w)
	}
	if err := clock.Check(); err != nil {
		log.Error("start check failed: no clock that counts suspension (CLOCK_BOOTTIME)", "error", err.Error())
		return host.ExitStartupCheck
	}
	return run(ctx, self, sv, plans, log, o, started)
}

func run(ctx context.Context, self Identity, sv *config.Values, plans []planned, log *slog.Logger, o Options,
	started time.Time) int {
	listen := o.Listen
	if listen == nil {
		listen = func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }
	}
	var members []*host.Member
	var servers []host.Server
	fail := func(member, msg string, err error) int {
		log.Error(msg, "member", member, "error", err.Error())
		for _, s := range servers {
			_ = s.Close()
		}
		return host.ExitFailure
	}
	pools, err := openPools(ctx, self, sv, plans, log)
	defer func() {
		for _, p := range pools {
			p.Close()
		}
	}()
	if err != nil {
		log.Error("the shell's database pools could not be opened", "error", err.Error())
		return host.ExitConfiguration
	}
	transport, err := openBus(ctx, self, sv, plans, log)
	if err != nil {
		log.Error("the bus is not usable", "error", err.Error())
		return host.ExitStartupCheck
	}
	if transport != nil {
		defer transport.Close()
	}
	var keys *identity.KeySet // one per process (protocol 17); IAM_JWKS_URL agrees across the members
	if len(plans) > 0 {
		if keys, err = host.OpenKeys(ctx, plans[0].values); err != nil {
			log.Warn("the identity keys could not be fetched yet", "error", err.Error())
		}
	}
	for _, p := range plans {
		var database *db.DB
		if p.def.Migrations != nil {
			s := db.SettingsFrom(p.values)
			database = db.NewDB(p.def.ID, pools[s.Target], s)
		}
		m, err := host.Build(ctx, p.def, p.values, host.Shared{Addresses: config.Map(p.served.Config), Read: o.Env.ReadFile,
			Out: o.Stdout, DB: database, Transport: transport, Keys: keys})
		var cfgErr *host.ConfigError
		if errors.As(err, &cfgErr) {
			host.ReportConfig(o.Stderr, obs.Member{ID: p.def.ID, Version: p.def.Version}, cfgErr.Problems)
			for _, s := range servers {
				_ = s.Close()
			}
			return host.ExitConfiguration
		}
		if err != nil {
			return fail(p.def.ID, "a member could not be built", err)
		}
		ln, err := listen(":" + strconv.Itoa(p.served.HTTPPort))
		if err != nil {
			return fail(p.def.ID, "cannot listen on the member's port", err)
		}
		srv := host.NewHTTPServer(m.Engine)
		go func() { _ = srv.Serve(ln) }()
		members, servers = append(members, m), append(servers, host.HTTPServer{Server: srv})
		if m.System != nil {
			port := p.served.extraPort("grpc")
			if port <= 0 {
				return fail(p.def.ID, "the member registers gRPC services but BrickKit gave it no grpc port", errors.New("no extra port grpc"))
			}
			gln, err := listen(":" + strconv.Itoa(port))
			if err != nil {
				return fail(p.def.ID, "cannot listen on the member's grpc port", err)
			}
			go func() { _ = m.System.Serve(gln) }()
			servers = append(servers, host.GRPCServer{Server: m.System, Addr: gln.Addr().String()})
		}
	}

	own, err := ownSurface(self, sv, members)
	if err != nil {
		return fail(self.ID, "the shell's own endpoints could not be built", err)
	}
	ln, err := listen(":" + strconv.FormatInt(sv.Int("PORT"), 10))
	if err != nil {
		return fail(self.ID, "cannot listen on PORT", err)
	}
	ownSrv := host.NewHTTPServer(own)
	go func() { _ = ownSrv.Serve(ln) }()
	servers = append(servers, host.HTTPServer{Server: ownSrv})

	for _, p := range pools {
		if err := p.Ready(ctx, log); err != nil {
			log.Error("a database is not usable", "target", p.Target.String(), "error", err.Error())
			for _, s := range servers {
				_ = s.Close()
			}
			return host.ReadyExit(err)
		}
	}
	bg, stopBg := context.WithCancel(context.Background())
	defer stopBg()
	// each member says it is ready, with its own identity, as it does alone (P1.12; protocol 17: a member alone and
	// in a shell behaves the same); the shell says so when the last of them is
	var pending atomic.Int64
	pending.Store(int64(len(members)))
	shellReady := func() {
		if pending.Add(-1) == 0 {
			log.Info("ready", "event", "ready", "protocol", protocol.Version, "sdk", sdk.Version, "members", len(members),
				"startup_ms", time.Since(started).Milliseconds())
		}
	}
	for _, m := range members { // in the order BrickKit lists them
		if err := m.Start(bg); err != nil {
			return fail(m.Def.ID, "a member could not start", err)
		}
		m.MarkReady(started, shellReady)
	}
	<-ctx.Done()

	drain, timeout := sv.Duration("STOP_DRAIN_DELAY"), sv.Duration("STOP_TIMEOUT")
	for _, m := range members { // the shell's stop grace is never shorter than any member's (P1.17)
		drain = max(drain, m.Values.Duration("STOP_DRAIN_DELAY"))
		timeout = max(timeout, m.Values.Duration("STOP_TIMEOUT"))
	}
	shellLog := obs.NewLogger(obs.Member{ID: self.ID, Version: self.Version}, obs.ParseLevel(sv.String("LOG_LEVEL")), o.Stdout)
	return host.StopAll(shellLog, members, servers, stopBg, drain, timeout)
}

// ownSurface is the shell's own port: health, readiness of all members, self-description with the members, and the
// metrics of every member labelled with its component.
func ownSurface(self Identity, sv *config.Values, members []*host.Member) (http.Handler, error) {
	me := obs.Member{ID: self.ID, Version: self.Version}
	tp, err := obs.NewTracerProvider(context.Background(), me, sv.String("OTEL_URL"))
	if err != nil {
		return nil, err
	}
	var gatherers prometheus.Gatherers
	for _, m := range members {
		gatherers = append(gatherers, labelled{m.Registry, m.Def.ID})
	}
	return httpx.NewEngine(httpx.Deps{Member: me, Logger: obs.NewLogger(me, obs.ParseLevel(sv.String("LOG_LEVEL")), io.Discard),
		Registry: obs.NewRegistry(), Tracer: tp.Tracer(self.ID),
		Platform: httpx.Platform{
			Ready: func() (bool, string, []string) {
				var waiting []string
				for _, m := range members {
					if !m.IsReady() {
						waiting = append(waiting, m.Def.ID)
					}
				}
				if len(waiting) == 0 {
					return true, host.Ready, nil
				}
				state := host.Starting
				if members[0].State() == host.Stopping {
					state = host.Stopping
				}
				return false, state, waiting
			},
			Describe: func() any {
				var ms []any
				for _, m := range members {
					ms = append(ms, m.Describe())
				}
				return map[string]any{"component_id": self.ID, "version": self.Version, "protocol": protocol.Version,
					"sdk": "be-sdk-go/" + sdk.Version, "capabilities": []string{}, "members": ms}
			},
			Metrics: promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{}),
		}}, &httpx.Routes{})
}
