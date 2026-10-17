// Package launcher runs one component on its own (protocol chapter 01): entries, configuration, the user surface on
// PORT, readiness, and the graceful stop. Main is the only place that touches the real process (environment, signals,
// exit); Run takes all of that as arguments so it can be tested.
package launcher

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/gin-gonic/gin"
)

// Exit codes (protocol chapter 01).
const (
	ExitOK            = 0
	ExitFailure       = 1
	ExitUsage         = 64
	ExitStartupCheck  = 69
	ExitConfiguration = 78
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
		fmt.Fprintln(o.Stderr, `{"level":"ERROR","msg":"migrate: no migrations are wired into this component yet"}`)
		return ExitFailure
	}
	fmt.Fprintf(o.Stderr, "{\"level\":\"ERROR\",\"msg\":\"unknown entry %q; use serve or migrate\"}\n", entry)
	return ExitUsage
}

// sdkKeys are the keys the SDK reads for every component, on top of its own (P1.14, P1.15, protocol chapter 02).
func sdkKeys() []config.Key {
	return []config.Key{
		{Name: "LOG_LEVEL", Type: config.Enum, Values: []string{"debug", "info", "warn", "error"}, Default: "info"},
		{Name: "OTEL_URL", Type: config.String, Process: true},
		{Name: "STOP_DRAIN_DELAY", Type: config.Duration, Default: defaultDrainDelay},
		{Name: "STOP_TIMEOUT", Type: config.Duration, Default: defaultStopTimeout},
	}
}

// platformKeys are injected by BrickKit; the launcher reads them, the module never does.
var platformKeys = []config.Key{
	{Name: "COMPONENT_ID", Type: config.String, Required: true},
	{Name: "COMPONENT_VERSION", Type: config.String, Required: true},
	{Name: "PORT", Type: config.Integer, Required: true},
}

// parseAll reads the component's keys and the SDK's keys; the platform keys are checked against the definition.
func parseAll(def component.Definition, src config.Source) (*config.Values, *config.Values, []config.Problem) {
	keys := append(append([]config.Key(nil), def.Keys...), sdkKeys()...)
	values, problems := config.Parse(keys, src)
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

func init() { gin.SetMode(gin.ReleaseMode) } // process-level Gin mode: set once by the SDK, never by a module
