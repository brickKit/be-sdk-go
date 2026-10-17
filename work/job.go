// Package work is protocol chapter 07: background work items with atomic claims, leases and epochs, effect classes
// that keep effects from happening twice, failures and dead letters, cron slots with a materialization watermark,
// singletons and per-replica timers.
package work

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
)

// Kind of a job.
type Kind string

// Kinds (protocol 01's four kinds of work).
const (
	Queue     Kind = "queue"
	Cron      Kind = "cron"
	Singleton Kind = "singleton"
	Every     Kind = "every"
)

// Effect is a job's declared effect class (P7.4).
type Effect string

// Effect classes.
const (
	Local                 Effect = "local"
	IdempotentExternal    Effect = "idempotent_external"
	NonIdempotentExternal Effect = "non_idempotent_external"
	EmitOnly              Effect = "emit_only"
)

// Parameters of protocol 01.
const (
	DefaultLease       = 30 * time.Second
	DefaultMaxAttempts = 10
	BackoffStart       = time.Second
	BackoffCap         = 5 * time.Minute
)

// Job declares one job of a module.
type Job struct {
	Name        string
	Kind        Kind
	Effect      Effect
	MaxAttempts int // default 10
	// Cron jobs: the schedule, its time zone (IANA), the backfill policy and its limit.
	Cron       string
	TimeZone   string
	Backfill   Backfill
	MaxBacklog int
	// Singleton jobs run again this long after a run completes; Every jobs run this often on every replica.
	Interval time.Duration
	// MaxWait bounds a business wait (Wait errors) before it counts as a permanent failure; 0: no limit.
	MaxWait time.Duration
	// Handle runs one claim of a work item. For Every jobs it is called with a nil Run.
	Handle func(ctx context.Context, r *Run) error
	// DeadLetter runs in the transaction that marks the item dead: local writes and the outbox only.
	DeadLetter func(ctx context.Context, tx db.Tx, item Item, cause error) error

	schedule Schedule
	loc      *time.Location
}

func (j *Job) prepare() error {
	if j.Name == "" || j.Handle == nil {
		return errors.New("work: a job needs a name and a handler")
	}
	if j.MaxAttempts <= 0 {
		j.MaxAttempts = DefaultMaxAttempts
	}
	if j.Effect == "" && j.Kind != Every {
		return fmt.Errorf("work: job %s must declare its effect class", j.Name)
	}
	switch j.Kind {
	case Cron:
		s, err := ParseCron(j.Cron)
		if err != nil {
			return fmt.Errorf("work: job %s: %w", j.Name, err)
		}
		tz := j.TimeZone
		if tz == "" {
			return fmt.Errorf("work: cron job %s must declare its time zone", j.Name)
		}
		if j.loc, err = time.LoadLocation(tz); err != nil {
			return fmt.Errorf("work: job %s: %w", j.Name, err)
		}
		j.schedule = s
		if j.Backfill == "" {
			j.Backfill = Latest
		}
		if j.MaxBacklog <= 0 {
			j.MaxBacklog = DefaultMaxBacklog
		}
	case Singleton, Every:
		if j.Interval <= 0 {
			return fmt.Errorf("work: job %s needs an interval", j.Name)
		}
	case Queue:
	default:
		return fmt.Errorf("work: job %s: kind %q", j.Name, j.Kind)
	}
	return nil
}

// Jobs collects a module's jobs.
type Jobs struct{ list []Job }

// Add declares a job.
func (j *Jobs) Add(job Job) { j.list = append(j.list, job) }

// Worker is implemented by modules with background work.
type Worker interface {
	Jobs(j *Jobs) error
}

// Item is a work item as a handler sees it.
type Item struct {
	ID          string
	Job         string
	Kind        Kind
	Attempts    int
	MaxAttempts int
	OperationID string
	Args        []byte // JSON
	Progress    []byte // JSON, singletons
	Slot        string // cron
	CallResults map[string]string
}

// Error kinds a handler returns.
type kindError struct {
	kind string
	err  error
}

func (e *kindError) Error() string { return e.err.Error() }
func (e *kindError) Unwrap() error { return e.err }

// Permanent marks an error that retrying cannot fix: the item goes to dead.
func Permanent(err error) error { return &kindError{"permanent", err} }

// NotEffective marks a failed non-idempotent call the handler confirmed did not take effect (refused before sending,
// or rejected by the other side): it may be retried.
func NotEffective(err error) error { return &kindError{"not_effective", err} }

// Wait marks a business wait (a source cannot be proven yet, the other side says "processing"): retried later
// without spending an attempt, up to the job's MaxWait.
func Wait(err error) error { return &kindError{"wait", err} }

func kindOf(err error) string {
	var k *kindError
	if errors.As(err, &k) {
		return k.kind
	}
	return "retryable"
}

// ErrLost is returned by the Run methods when this worker no longer holds the item: it was taken over, or the
// database epoch changed. The handler must stop.
var ErrLost = errors.New("work: the claim was lost")

// backoff is the delay after attempt n: 1 s doubling, at most 5 minutes.
func backoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	if n > 20 {
		return BackoffCap
	}
	return min(BackoffCap, BackoffStart<<(n-1))
}
