package launcher

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/v3/clock"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/bus"
	"github.com/brickKit/be-sdk-go/v3/internal/host"
	"github.com/brickKit/be-sdk-go/v3/obs"
)

// ListenFunc opens a listener.
type ListenFunc func(addr string) (net.Listener, error)

func serve(ctx context.Context, def component.Definition, o Options) int {
	started := time.Now()
	member := obs.Member{ID: def.ID, Version: def.Version}
	values, plat, problems := parseAll(def, o.Env)
	if len(problems) > 0 {
		host.ReportConfig(o.Stderr, member, problems)
		return ExitConfiguration
	}
	// the process's startup lines before the member's own logger exists: the same structured form (P10.2)
	log := obs.NewLogger(member, obs.ParseLevel(values.String("LOG_LEVEL")), o.Stdout)
	var pool *db.Pool
	var database *db.DB
	if def.Migrations != nil {
		// alone, the component connects with its own runtime login role (protocol 14, "connection pools")
		s := db.SettingsFrom(values)
		var err error
		pool, err = db.Open(ctx, db.PoolConfig{Target: s.Target, User: s.User, Password: s.Password, AppName: def.ID,
			MaxConns: int32(s.Foreground + s.Background), Prepared: s.Prepared, Log: log})
		if err != nil {
			log.Error("the database is not usable", "error", err.Error())
			return ExitConfiguration
		}
		defer pool.Close()
		database = db.NewDB(def.ID, pool, s)
	}
	var transport events.Transport
	if len(def.Streams) > 0 || def.Subscribes {
		// one bus connection per process (protocol 17)
		var err error
		if transport, err = bus.Open(ctx, values.Secret("BUS_URL").Value(), def.ID, log); err != nil {
			log.Error("the bus is not usable", "error", err.Error())
			return ExitStartupCheck
		}
		defer transport.Close()
	}
	keys, err := host.OpenKeys(ctx, values)
	if err != nil {
		log.Warn("the identity keys could not be fetched yet", "error", err.Error())
	}
	m, err := host.Build(ctx, def, values, host.Shared{Addresses: o.Env, Read: o.Env.ReadFile, Out: o.Stdout,
		DB: database, Transport: transport, Keys: keys})
	var cfgErr *host.ConfigError
	if errors.As(err, &cfgErr) {
		host.ReportConfig(o.Stderr, member, cfgErr.Problems)
		return ExitConfiguration
	}
	if err != nil {
		log.Error("the member could not be built", "error", err.Error())
		return ExitFailure
	}
	log = m.Logger
	if err := clock.Check(); err != nil {
		log.Error("start check failed: no clock that counts suspension (CLOCK_BOOTTIME)", "error", err.Error())
		return ExitStartupCheck
	}
	if pool != nil {
		if err := pool.Ready(ctx, log); err != nil {
			log.Error("the database is not usable", "error", err.Error())
			return host.ReadyExit(err)
		}
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
	srv := host.NewHTTPServer(m.Engine)
	serveErr := make(chan error, 2)
	go func() { serveErr <- srv.Serve(ln) }()
	servers := []host.Server{host.HTTPServer{Server: srv}}
	if m.System != nil {
		if def.GRPCPort <= 0 {
			log.Error("the module registers gRPC services but the definition has no GRPCPort")
			_ = srv.Close()
			return ExitFailure
		}
		gln, err := listen(":" + strconv.Itoa(def.GRPCPort))
		if err != nil {
			log.Error("cannot listen on the grpc port", "error", err.Error())
			_ = srv.Close()
			return ExitFailure
		}
		go func() { serveErr <- m.System.Serve(gln) }()
		servers = append(servers, host.GRPCServer{Server: m.System, Addr: gln.Addr().String()})
	}

	bg, stopBg := context.WithCancel(context.Background())
	defer stopBg()
	if err := m.Start(bg); err != nil {
		log.Error("the module could not start", "error", err.Error())
		for _, s := range servers {
			_ = s.Close()
		}
		return ExitFailure
	}
	m.MarkReady(started, nil)

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		log.Error("the user surface stopped unexpectedly", "error", fmt.Sprint(err))
		return ExitFailure
	}
	return host.StopAll(log, []*host.Member{m}, servers, stopBg,
		values.Duration("STOP_DRAIN_DELAY"), values.Duration("STOP_TIMEOUT"))
}
