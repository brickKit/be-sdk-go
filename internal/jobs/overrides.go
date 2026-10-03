package jobs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/jobs/schedule"
)

// The value patterns of jobs-overrides.schema.json.
var (
	intervalPattern = regexp.MustCompile(`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`)
	cronPattern     = regexp.MustCompile(`^(\S+( \S+){4}|@every ([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+)$`)
)

// override is one job's entry of JOBS_OVERRIDES (P14.5).
type override struct {
	Interval *string `json:"interval"`
	Cron     *string `json:"cron"`
	Enabled  *bool   `json:"enabled"`
}

// overridesErr is an invalid JOBS_OVERRIDES (exit 78); detail names the job, never the value.
func overridesErr(format string, args ...any) *config.Error {
	return &config.Error{Reason: config.ReasonInvalid, Key: "JOBS_OVERRIDES", Detail: fmt.Sprintf(format, args...)}
}

// parseOverrides reads JOBS_OVERRIDES with the shape of jobs-overrides.schema.json: an object keyed by
// job name, each value an object with at least one of interval, cron, enabled and nothing else.
func parseOverrides(raw string) (map[string]override, error) {
	trimmed := bytes.TrimSpace([]byte(raw))
	if len(trimmed) == 0 {
		return nil, nil
	}
	if trimmed[0] != '{' {
		return nil, overridesErr("not a JSON object")
	}
	var byName map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &byName); err != nil {
		return nil, overridesErr("not a JSON object of job names")
	}
	out := make(map[string]override, len(byName))
	for name, v := range byName {
		if !namePattern.MatchString(name) {
			return nil, overridesErr("a job name matches ^[a-z][a-z0-9_.-]*$")
		}
		o, err := decodeOverride(name, v)
		if err != nil {
			return nil, err
		}
		out[name] = o
	}
	return out, nil
}

func decodeOverride(name string, v json.RawMessage) (override, error) {
	var o override
	dec := json.NewDecoder(bytes.NewReader(v))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&o); err != nil || bytes.TrimSpace(v)[0] != '{' {
		return o, overridesErr("job %s: an object with interval, cron or enabled only", name)
	}
	if o.Interval == nil && o.Cron == nil && o.Enabled == nil {
		return o, overridesErr("job %s: an override sets at least one of interval, cron, enabled", name)
	}
	if o.Interval != nil && !intervalPattern.MatchString(*o.Interval) {
		return o, overridesErr("job %s: interval is not a Go duration", name)
	}
	if o.Cron != nil && !cronPattern.MatchString(*o.Cron) {
		return o, overridesErr("job %s: cron is not a five-field expression or @every <duration>", name)
	}
	return o, nil
}

// applyOverrides applies JOBS_OVERRIDES to the plan (P14.5): an unknown name is logged at WARN and
// ignored; a field that does not apply to the job's kind or an invalid schedule is a configuration
// error.
func (p *plan) applyOverrides(raw string, log *slog.Logger) error {
	byName, err := parseOverrides(raw)
	if err != nil {
		return err
	}
	for name, o := range byName {
		e, ok := p.byName[name]
		if !ok {
			log.Warn("JOBS_OVERRIDES names an unknown job; ignored (P14.5)", slog.String("job", name))
			continue
		}
		if err := e.apply(o); err != nil {
			return err
		}
	}
	return nil
}

func (e *entry) apply(o override) error {
	if o.Interval != nil {
		if e.kind != kindEvery && e.kind != kindSingleton && e.kind != kindReconciler {
			return overridesErr("job %s: interval applies to every, singleton and reconciler jobs, not %s", e.name, e.kind)
		}
		d, err := time.ParseDuration(*o.Interval)
		if err != nil || d <= 0 {
			return overridesErr("job %s: interval must be a positive Go duration", e.name)
		}
		e.interval = d
	}
	if o.Cron != nil {
		if e.kind != kindCron {
			return overridesErr("job %s: cron applies to cron jobs, not %s", e.name, e.kind)
		}
		s, err := schedule.Parse(*o.Cron, e.loc)
		if err != nil { // the parser's text quotes the value: not repeated here (P2.7)
			return overridesErr("job %s: invalid schedule: five fields or @every of at least 1s (P14.6)", e.name)
		}
		e.sched = s
	}
	if o.Enabled != nil {
		e.enabled = *o.Enabled
	}
	return nil
}
