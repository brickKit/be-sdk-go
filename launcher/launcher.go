// Package launcher runs one component on its own (protocol chapter 01): entries, configuration, the user surface on
// PORT, readiness, and the graceful stop. Main is the only place that touches the real process (environment, signals,
// exit); Run takes all of that as arguments so it can be tested.
package launcher

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/internal/host"
	"github.com/brickKit/be-sdk-go/v3/obs"
)

// Exit codes (protocol chapter 01).
const (
	ExitOK            = host.ExitOK
	ExitFailure       = host.ExitFailure
	ExitUsage         = host.ExitUsage
	ExitStartupCheck  = host.ExitStartupCheck
	ExitConfiguration = host.ExitConfiguration
)

// Options are the process-facing inputs of Run.
type Options struct {
	Args   []string   // arguments after the program name
	Env    config.Env // the environment (and files) to read configuration from
	Stdout io.Writer  // JSON logs
	Stderr io.Writer  // configuration errors before a logger exists
	// Listen overrides net.Listen for tests (nil: real TCP).
	Listen ListenFunc
}

// Main runs the component as a process and exits with its exit code. Call it from the component's cmd/server.
func Main(def component.Definition) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	os.Exit(Run(ctx, def, Options{Args: os.Args[1:], Env: config.ProcessEnv(), Stdout: os.Stdout, Stderr: os.Stderr}))
}

// Run runs one entry of the component until ctx is cancelled (the stop signal) and returns the exit code.
func Run(ctx context.Context, def component.Definition, o Options) int {
	entry := "serve"
	if len(o.Args) > 0 {
		entry = o.Args[0]
	}
	switch entry {
	case "serve":
		return serve(ctx, def, o)
	case "migrate":
		return migrate(ctx, def, o)
	}
	obs.NewLogger(obs.Member{ID: def.ID, Version: def.Version}, slog.LevelInfo, o.Stderr).
		Error("unknown entry; use serve or migrate", "entry", entry)
	return ExitUsage
}

// platformKeys are injected by BrickKit; the launcher reads them, the module never does.
var platformKeys = []config.Key{
	{Name: "COMPONENT_ID", Type: config.String, Required: true},
	{Name: "COMPONENT_VERSION", Type: config.String, Required: true},
	{Name: "PORT", Type: config.Integer, Required: true},
}

// parseAll reads the component's keys and the SDK's keys; the platform keys are checked against the definition.
func parseAll(def component.Definition, src config.Source) (*config.Values, *config.Values, []config.Problem) {
	values, problems := config.Parse(host.Keys(def), src)
	plat, pp := parsePlatform(src)
	problems = append(problems, pp...)
	// compared on the raw values, so a mismatch is reported even when another platform key is missing
	if id, ok := src.Lookup("COMPONENT_ID"); ok && id != "" && id != def.ID {
		problems = append(problems, config.Problem{Key: "COMPONENT_ID", Kind: "invalid", Expected: def.ID, Got: id})
	}
	if v, ok := src.Lookup("COMPONENT_VERSION"); ok && v != "" && v != def.Version {
		problems = append(problems, config.Problem{Key: "COMPONENT_VERSION", Kind: "invalid",
			Expected: def.Version + " (the version compiled into this image)", Got: v})
	}
	return values, plat, problems
}

func parsePlatform(src config.Source) (*config.Values, []config.Problem) {
	return config.ParsePlatform(platformKeys, src)
}
