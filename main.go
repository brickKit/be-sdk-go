package besdk

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/brickKit/be-sdk-go/internal/logx"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
)

// Main is the component's entry point (P1.1): no argument serves; `migrate up | down <n> | status`
// migrates; `job run <name>` runs one job once (P14.8, not offered by this version). It is the only
// place of the SDK that reads the process environment and exits the process.
func Main(s Spec) {
	gin.SetMode(gin.ReleaseMode) // the process owner decides gin's process-wide mode, once
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	host, _ := os.Hostname()
	code := run(ctx, s, os.Args[1:], processEnv{lookup: os.LookupEnv, stdout: os.Stdout, instanceID: host})
	stop()
	os.Exit(code)
}

// run is Main without the process: it returns the exit code (P1 "Exit codes").
func run(ctx context.Context, s Spec, args []string, pe processEnv) int {
	early := logx.New(logx.Options{ComponentID: s.ID, Writer: pe.stdout})
	cmd, err := parseArgs(args)
	if err != nil {
		early.Error("usage error", slog.String("error", err.Error()))
		return exitUsage
	}
	if _, ok := pe.lookup("COMPONENT_ID"); !ok {
		early.Error("usage error: no COMPONENT_ID in the environment; the process was not started by the platform (P1.2)")
		return exitUsage
	}
	b, err := bootstrap(ctx, s, pe)
	if err != nil {
		var cf *configFailure
		if errors.As(err, &cf) {
			logConfigErrors(early, cf)
			return exitConfig
		}
		early.Error("start failed", slog.String("error", err.Error()))
		return exitFailure
	}
	defer shutdownPlatform(b)
	g := b.platform.Globals()
	otel.SetTextMapPropagator(g.Propagator) // standalone: the process is this component
	otel.SetTracerProvider(g.TracerProvider)
	switch cmd.kind {
	case cmdMigrateUp, cmdMigrateDown, cmdMigrateStatus:
		return runMigrate(ctx, b, cmd)
	case cmdJobRun:
		return runJob(ctx, b, cmd.job)
	}
	return serve(ctx, b, pe)
}

func shutdownPlatform(b *boot) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownFlush)
	defer cancel()
	_ = b.member.Shutdown(ctx)
	_ = b.platform.Shutdown(ctx)
}
