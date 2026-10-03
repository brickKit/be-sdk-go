package jobs

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/prometheus/client_golang/prometheus"
)

// Defaults (P14).
const (
	DefaultLeaseTTL    = 30 * time.Second // singleton lease TTL, renewed every TTL/3
	DefaultPoll        = time.Second      // a queue worker's polling while idle
	DefaultMaxAttempts = 10               // a worker's or reconciler's attempts when it declares none
	DefaultBatch       = 100              // a reconciler pass's candidates when it declares none
	DefaultZone        = "Asia/Shanghai"  // BUSINESS_TIMEZONE's catalogue default
	leaseGrace         = 30 * time.Second // a queue or reconcile lease outlives the run's timeout by this
)

// Kind is a declared job's kind (P14.1). Queue workers and reconcilers are declared separately.
type Kind int

// The scheduled kinds (P14 "The five kinds").
const (
	Every     Kind = iota // on every replica, on its own timer
	Singleton             // one holder at a time, through besdk_job_lease
	Cron                  // each slot once, through besdk_job_slot
)

// Job is one scheduled job (P14.1, sdk-redesign-apis §2.9).
type Job struct {
	Name     string        // unique within the component; runtime-owned jobs start with "be."
	Kind     Kind          // Every, Singleton or Cron
	Interval time.Duration // Every and Singleton: time between run starts
	Cron     string        // Cron: five-field expression or "@every <duration>" (P14.6)
	TZ       string        // Cron: IANA zone; "" = BUSINESS_TIMEZONE
	Timeout  time.Duration // required: the run's context is cancelled at the timeout (P14.2)
	Run      func(ctx context.Context) error
}

// Worker consumes one kind of queued job (P14 "queue").
type Worker struct {
	Kind        string          // the queue kind; also the name used by JOBS_OVERRIDES and job run
	Concurrency int             // handlers at once in this process; 0 = 1
	MaxAttempts int             // 0 = DefaultMaxAttempts
	Backoff     []time.Duration // delay before attempt n+1 is Backoff[min(n, len)-1]; nil = 1 s doubling to 5 min
	Timeout     time.Duration   // required: one handler run
	// Run handles one job outside any transaction; it deduplicates by UniqueKey or a business key.
	Run func(ctx context.Context, j QueuedJob) error
	// OnDead is called in a transaction once the attempts are used up; nil = only mark the row dead.
	OnDead func(ctx context.Context, tx *pg.Tx, j QueuedJob) error
}

// QueuedJob is one besdk_job_queue row as a handler sees it.
type QueuedJob struct {
	ID          string // UUIDv7
	Kind        string
	Args        json.RawMessage
	UniqueKey   string // "" = none
	Attempt     int    // 1 on the first run
	MaxAttempts int
	LastError   string // the previous attempt's error, "" on the first run
	TraceParent string // captured at enqueue (P18.1)
	CausationID string // captured at enqueue (P12.8)
	HopCount    int    // captured at enqueue (P12.8)
	EnqueuedAt  time.Time
	RunAt       time.Time
}

// Decode unmarshals the job's arguments into v.
func (j QueuedJob) Decode(v any) error { return json.Unmarshal(j.Args, v) }

// Declarations is everything a member runs in the background: the module's jobs, workers and
// reconcilers, and the runtime's own be.* jobs.
type Declarations struct {
	Jobs        []Job
	Workers     []Worker
	Reconcilers []*Reconciler
	Runtime     []Job // be.outbox, be.cleanup, …: names must start with "be."
}

// RunInfo describes one run to Config.Wrap.
type RunInfo struct {
	Name      string    // the job name, the worker kind or the reconciler name
	Kind      string    // "every", "singleton", "cron", "queue", "reconciler"
	Slot      time.Time // cron: the slot being run
	Epoch     int64     // singleton: the lease epoch (fencing token)
	Queued    *QueuedJob
	OneShot   bool // started by job run (P14.8)
	Holder    string
	StartedAt time.Time
}

// Config binds an engine to one member (P14, P19).
type Config struct {
	Store       *pg.Store
	ComponentID string         // holder prefix, the member's ID in a shell
	InstanceID  string         // service.instance.id: the container or pod
	Zone        *time.Location // BUSINESS_TIMEZONE; nil = Asia/Shanghai
	Overrides   string         // JOBS_OVERRIDES as configured; "" = none
	Logger      *slog.Logger   // nil discards
	Registerer  prometheus.Registerer
	Now         func() time.Time // nil = time.Now; the slot arithmetic reads it
	LeaseTTL    time.Duration    // 0 = 30 s (tests shorten it)
	Poll        time.Duration    // 0 = 1 s
	// Wrap, when set, runs around every run: the root starts the span, adds the "job" log field and
	// the publishing origin to the context. It must call run and return its error.
	Wrap func(ctx context.Context, info RunInfo, run func(ctx context.Context) error) error
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c Config) leaseTTL() time.Duration {
	if c.LeaseTTL > 0 {
		return c.LeaseTTL
	}
	return DefaultLeaseTTL
}

func (c Config) poll() time.Duration {
	if c.Poll > 0 {
		return c.Poll
	}
	return DefaultPoll
}

// holder is the in-process holder, "<component ID>/<instance id>" (P14).
func (c Config) holder() string { return c.ComponentID + "/" + c.InstanceID }

// oneShotHolder is job run's holder, "<component ID>/job-run:<instance id>" (P14.8).
func (c Config) oneShotHolder() string { return c.ComponentID + "/job-run:" + c.InstanceID }

func (c Config) logger() *slog.Logger {
	if c.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return c.Logger
}

// backoffFor is the delay after the n-th failed attempt (n >= 1): the declared list, its last entry
// repeated; without a list 1 s doubling up to 5 min.
func backoffFor(list []time.Duration, n int) time.Duration {
	if n < 1 {
		n = 1
	}
	if len(list) > 0 {
		return list[min(n, len(list))-1]
	}
	if n > 9 {
		return 5 * time.Minute
	}
	return min(time.Second<<(n-1), 5*time.Minute)
}
