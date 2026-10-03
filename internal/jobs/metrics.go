package jobs

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Run results, the `result` label of be_job_runs_total (P14.3).
const (
	ResultOK        = "ok"
	ResultError     = "error"      // the run returned an error or panicked
	ResultTimeout   = "timeout"    // the run's timeout cancelled it
	ResultLeaseLost = "lease_lost" // a singleton lost its lease during the run
	ResultCancelled = "cancelled"  // the process is stopping
)

// metrics are the P14.3 series. The member's registerer adds component=<ID> (P18.3).
type metrics struct {
	runs        *prometheus.CounterVec   // be_job_runs_total{job,result}
	duration    *prometheus.HistogramVec // be_job_duration_seconds{job}
	lastSuccess *prometheus.GaugeVec     // be_job_last_success_timestamp_seconds{job}
	queueDepth  *prometheus.GaugeVec     // be_queue_depth{kind,state}
	queueOldest *prometheus.GaugeVec     // be_queue_oldest_age_seconds{kind}
	recPending  *prometheus.GaugeVec     // be_reconcile_pending{name}
	recOldest   *prometheus.GaugeVec     // be_reconcile_oldest_age_seconds{name}
	recGiveups  *prometheus.CounterVec   // be_reconcile_giveups_total{name}
}

// jobBuckets: seconds, 10 ms to 1 h (jobs run far longer than requests).
func jobBuckets() []float64 {
	return []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 900, 3600}
}

// newMetrics registers the P14.3 series on reg, all or nothing; nil reg = unregistered collectors.
func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	g := func(name, help string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	}
	c := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	m := &metrics{
		runs: c("be_job_runs_total", "Background job runs by result.", "job", "result"),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "be_job_duration_seconds",
			Help: "Background job run duration in seconds.", Buckets: jobBuckets()}, []string{"job"}),
		lastSuccess: g("be_job_last_success_timestamp_seconds", "Unix time of the last successful run.", "job"),
		queueDepth:  g("be_queue_depth", "Queued jobs by state.", "kind", "state"),
		queueOldest: g("be_queue_oldest_age_seconds", "Age of the oldest due ready job.", "kind"),
		recPending:  g("be_reconcile_pending", "Items under reconciliation.", "name"),
		recOldest:   g("be_reconcile_oldest_age_seconds", "Age of the oldest stuck candidate.", "name"),
		recGiveups:  c("be_reconcile_giveups_total", "Items a reconciler gave up on.", "name"),
	}
	if reg == nil {
		return m, nil
	}
	cs := []prometheus.Collector{m.runs, m.duration, m.lastSuccess, m.queueDepth, m.queueOldest,
		m.recPending, m.recOldest, m.recGiveups}
	for i, col := range cs {
		if err := reg.Register(col); err != nil {
			for _, done := range cs[:i] {
				reg.Unregister(done)
			}
			return nil, fmt.Errorf("jobs: register metrics: %w", err)
		}
	}
	return m, nil
}

// observe records one finished run.
func (m *metrics) observe(job, result string, took time.Duration, end time.Time) {
	m.runs.WithLabelValues(job, result).Inc()
	m.duration.WithLabelValues(job).Observe(took.Seconds())
	if result == ResultOK {
		m.lastSuccess.WithLabelValues(job).Set(float64(end.UnixNano()) / 1e9)
	}
}
