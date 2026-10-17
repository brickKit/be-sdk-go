package launcher

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	sdk "github.com/brickKit/be-sdk-go/v3"
	"github.com/brickKit/be-sdk-go/v3/clock"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/internal/host"
)

// ListenFunc opens a listener.
type ListenFunc func(addr string) (net.Listener, error)

func serve(ctx context.Context, def component.Definition, o Options) int {
	started := time.Now()
	values, plat, problems := parseAll(def, o.Env)
	if len(problems) > 0 {
		host.ReportConfig(o.Stderr, def.ID, problems)
		return ExitConfiguration
	}
	m, err := host.Build(ctx, def, values, o.Env, o.Env.ReadFile, o.Stdout)
	if err != nil {
		fmt.Fprintf(o.Stdout, "{\"level\":\"ERROR\",\"component\":%q,\"msg\":%q}\n", def.ID, err.Error())
		return ExitFailure
	}
	log := m.Logger
	if err := clock.Check(); err != nil {
		log.Error("start check failed: no clock that counts suspension (CLOCK_BOOTTIME)", "error", err.Error())
		return ExitStartupCheck
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
	srv := &http.Server{Handler: m.Engine, ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	bg, stopBg := context.WithCancel(context.Background())
	defer stopBg()
	if err := m.Start(bg); err != nil {
		log.Error("the module could not start", "error", err.Error())
		_ = srv.Close()
		return ExitFailure
	}
	m.SetState(host.Ready)
	log.Info("ready", "event", "ready", "protocol", protocol.Version, "sdk", sdk.Version,
		"startup_ms", time.Since(started).Milliseconds())

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		log.Error("the user surface stopped unexpectedly", "error", fmt.Sprint(err))
		return ExitFailure
	}
	return host.StopAll(log, []*host.Member{m}, []*http.Server{srv}, stopBg,
		values.Duration("STOP_DRAIN_DELAY"), values.Duration("STOP_TIMEOUT"))
}
