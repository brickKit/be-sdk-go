package jobs

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/stretchr/testify/require"
)

func noop(context.Context) error { return nil }

func everyJob(name string) Job {
	return Job{Name: name, Kind: Every, Interval: time.Second, Timeout: time.Second, Run: noop}
}

func cronJob(name, expr string) Job {
	return Job{Name: name, Kind: Cron, Cron: expr, Timeout: time.Second, Run: noop}
}

func worker(kind string) Worker {
	return Worker{Kind: kind, Timeout: time.Second, Run: func(context.Context, QueuedJob) error { return nil },
		OnDead: func(context.Context, *pg.Tx, QueuedJob) error { return nil }}
}

// configErr asserts err is a *config.Error (exit 78) naming key.
func configErr(t *testing.T, err error, key string) {
	t.Helper()
	require.Error(t, err)
	var ce *config.Error
	require.True(t, errors.As(err, &ce), "want *config.Error, got %T: %v", err, err)
	require.Equal(t, key, ce.Key, "error: %v", err)
}

func TestCompileValidDeclarations(t *testing.T) {
	plan, err := compile(Config{}, Declarations{
		Jobs: []Job{
			everyJob("widget.sweep"),
			{Name: "widget.partitions", Kind: Singleton, Interval: time.Hour, Timeout: time.Minute, Run: noop},
			cronJob("widget.daily", "0 3 * * *"),
			{Name: "widget.berlin", Kind: Cron, Cron: "0 3 * * *", TZ: "Europe/Berlin", Timeout: time.Minute, Run: noop},
		},
		Workers: []Worker{worker("widget.notify")},
		Runtime: []Job{cronJob("be.cleanup", "@every 1h")},
	})
	require.NoError(t, err)
	require.Len(t, plan.entries, 6)
	daily := plan.byName["widget.daily"]
	require.Equal(t, "Asia/Shanghai", daily.loc.String(), "cron defaults to BUSINESS_TIMEZONE's default")
	require.Equal(t, "Europe/Berlin", plan.byName["widget.berlin"].loc.String())
	require.Equal(t, kindQueue, plan.byName["widget.notify"].kind)
	w := plan.byName["widget.notify"].worker
	require.Equal(t, 1, w.Concurrency)
	require.Equal(t, DefaultMaxAttempts, w.MaxAttempts)
}

func TestCompileUsesBusinessTimezone(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	plan, err := compile(Config{Zone: berlin}, Declarations{Jobs: []Job{cronJob("widget.daily", "0 3 * * *")}})
	require.NoError(t, err)
	require.Equal(t, "Europe/Berlin", plan.byName["widget.daily"].loc.String())
}

func TestCompileRejects(t *testing.T) {
	cases := []struct {
		name string
		d    Declarations
		key  string
	}{
		{"missing timeout", Declarations{Jobs: []Job{{Name: "widget.a", Kind: Every, Interval: time.Second, Run: noop}}}, "job widget.a"},
		{"missing run", Declarations{Jobs: []Job{{Name: "widget.a", Kind: Every, Interval: time.Second, Timeout: time.Second}}}, "job widget.a"},
		{"every without interval", Declarations{Jobs: []Job{{Name: "widget.a", Kind: Every, Timeout: time.Second, Run: noop}}}, "job widget.a"},
		{"invalid cron", Declarations{Jobs: []Job{cronJob("widget.a", "0 3 * *")}}, "job widget.a"},
		{"@daily", Declarations{Jobs: []Job{cronJob("widget.a", "@daily")}}, "job widget.a"},
		{"invalid zone", Declarations{Jobs: []Job{{Name: "widget.a", Kind: Cron, Cron: "0 3 * * *", TZ: "Mars/Olympus", Timeout: time.Second, Run: noop}}}, "job widget.a"},
		{"bad name", Declarations{Jobs: []Job{everyJob("Widget A")}}, "job Widget A"},
		{"duplicate name", Declarations{Jobs: []Job{everyJob("widget.a")}, Workers: []Worker{worker("widget.a")}}, "job widget.a"},
		{"component job with be. prefix", Declarations{Jobs: []Job{everyJob("be.outbox")}}, "job be.outbox"},
		{"runtime job without be. prefix", Declarations{Runtime: []Job{everyJob("cleanup")}}, "job cleanup"},
		{"worker without timeout", Declarations{Workers: []Worker{{Kind: "widget.q", Run: worker("x").Run}}}, "job widget.q"},
		{"worker without run", Declarations{Workers: []Worker{{Kind: "widget.q", Timeout: time.Second}}}, "job widget.q"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := compile(Config{}, c.d)
			configErr(t, err, c.key)
		})
	}
}

func TestOverridesApply(t *testing.T) {
	var logs bytes.Buffer
	cfg := Config{Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		Overrides: `{"be.cleanup":{"cron":"@every 2s"},"widget.sweep":{"interval":"200ms"},` +
			`"widget.daily":{"enabled":false},"widget.notify":{"enabled":false},"widget.gone":{"enabled":true}}`}
	plan, err := compile(cfg, Declarations{
		Jobs:    []Job{everyJob("widget.sweep"), cronJob("widget.daily", "0 3 * * *")},
		Workers: []Worker{worker("widget.notify")},
		Runtime: []Job{cronJob("be.cleanup", "@every 1h")},
	})
	require.NoError(t, err)
	require.Equal(t, "@every 2s", plan.byName["be.cleanup"].sched.String())
	require.Equal(t, 200*time.Millisecond, plan.byName["widget.sweep"].interval)
	require.False(t, plan.byName["widget.daily"].enabled)
	require.False(t, plan.byName["widget.notify"].enabled)
	require.True(t, plan.byName["widget.sweep"].enabled)
	require.Contains(t, logs.String(), `"level":"WARN"`)
	require.Contains(t, logs.String(), "widget.gone")
}

func TestOverridesReject(t *testing.T) {
	d := Declarations{
		Jobs:    []Job{everyJob("widget.sweep"), cronJob("widget.daily", "0 3 * * *")},
		Workers: []Worker{worker("widget.notify")},
	}
	for _, raw := range []string{
		`not json`,
		`[]`,
		`{"widget.daily":{}}`,
		`{"widget.daily":{"cron":"0 3 * *"}}`,
		`{"widget.daily":{"cron":"@every 500ms"}}`,
		`{"widget.daily":{"cron":"@daily"}}`,
		`{"widget.sweep":{"interval":"2x"}}`,
		`{"widget.sweep":{"interval":"-1s"}}`,
		`{"widget.sweep":{"interval":"0s"}}`,
		`{"widget.sweep":{"every":"1s"}}`,
		`{"widget.sweep":{"enabled":"no"}}`,
		`{"Widget":{"enabled":false}}`,
		`{"widget.daily":{"interval":"1s"}}`,
		`{"widget.sweep":{"cron":"0 3 * * *"}}`,
		`{"widget.notify":{"interval":"1s"}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			_, err := compile(Config{Overrides: raw}, d)
			configErr(t, err, "JOBS_OVERRIDES")
		})
	}
}

func TestEmptyOverrides(t *testing.T) {
	for _, raw := range []string{"", "{}"} {
		_, err := compile(Config{Overrides: raw}, Declarations{Jobs: []Job{everyJob("widget.sweep")}})
		require.NoError(t, err)
	}
}
