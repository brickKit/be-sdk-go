package telemetry

import "github.com/prometheus/client_golang/prometheus"

// poolWaitBuckets are the seconds buckets of a database connection wait: 0.5 ms to 10 s.
func poolWaitBuckets() []float64 {
	return []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
}

func gauge(name, help string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
}

// DBMetrics are the database metrics (P18.3).
type DBMetrics struct {
	PoolInUse  prometheus.Gauge       // be_db_pool_in_use
	PoolWait   prometheus.Histogram   // be_db_pool_wait_seconds
	TxRetries  *prometheus.CounterVec // be_tx_retries_total{sqlstate} (P18.3)
	IdentityOK prometheus.Gauge       // be_db_identity_ok (1 when the runtime identity checks out)
}

// NewDBMetrics registers the database metrics (P18.3).
func NewDBMetrics(reg prometheus.Registerer) (*DBMetrics, error) {
	m := &DBMetrics{
		PoolInUse: gauge("be_db_pool_in_use", "Database connections in use by this member."),
		PoolWait: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "be_db_pool_wait_seconds",
			Help: "Time waited for a database connection in seconds.", Buckets: poolWaitBuckets()}),
		TxRetries:  counterVec("be_tx_retries_total", "Transactions retried, by the SQLSTATE that caused the retry.", "sqlstate"),
		IdentityOK: gauge("be_db_identity_ok", "1 when the database identity check passed, else 0."),
	}
	return m, register(reg, m.PoolInUse, m.PoolWait, m.TxRetries, m.IdentityOK)
}

// SecretMetrics are the secret reload metrics (P18.3, P2.9); key is the key name, never the value.
type SecretMetrics struct {
	ReloadFailures *prometheus.CounterVec // be_secret_reload_failures_total{key}
}

// NewSecretMetrics registers the secret reload metrics (P18.3).
func NewSecretMetrics(reg prometheus.Registerer) (*SecretMetrics, error) {
	m := &SecretMetrics{ReloadFailures: counterVec("be_secret_reload_failures_total", "Failed secret reloads, by key name.", "key")}
	return m, register(reg, m.ReloadFailures)
}

// EventMetrics are the outbox, publish and consumer metrics (P18.3); result is applied, skipped, nak or dlq.
type EventMetrics struct {
	OutboxPending   prometheus.Gauge       // be_outbox_pending
	OutboxOldestAge prometheus.Gauge       // be_outbox_oldest_age_seconds
	Published       *prometheus.CounterVec // be_events_published_total{subject}
	ConsumerHandled *prometheus.CounterVec // be_consumer_handled_total{subject,result}
	ConsumerLag     *prometheus.GaugeVec   // be_consumer_lag_seconds{subject}
	DLQ             *prometheus.CounterVec // be_dlq_messages_total{subject}
}

// NewEventMetrics registers the event metrics (P18.3).
func NewEventMetrics(reg prometheus.Registerer) (*EventMetrics, error) {
	m := &EventMetrics{
		OutboxPending:   gauge("be_outbox_pending", "Outbox rows not yet published."),
		OutboxOldestAge: gauge("be_outbox_oldest_age_seconds", "Age of the oldest unpublished outbox row in seconds."),
		Published:       counterVec("be_events_published_total", "Events published, by subject.", "subject"),
		ConsumerHandled: counterVec("be_consumer_handled_total", "Events handled, by subject and result.", "subject", "result"),
		ConsumerLag:     gaugeVec("be_consumer_lag_seconds", "Consumer lag in seconds, by subject.", "subject"),
		DLQ:             counterVec("be_dlq_messages_total", "Events sent to the dead-letter queue, by subject.", "subject"),
	}
	return m, register(reg, m.OutboxPending, m.OutboxOldestAge, m.Published, m.ConsumerHandled, m.ConsumerLag, m.DLQ)
}

// AuthzMetrics are the authorization metrics (P18.3).
type AuthzMetrics struct {
	BundleAge     prometheus.Gauge       // be_authz_bundle_age_seconds
	ProjectionLag prometheus.Gauge       // be_authz_projection_lag
	Denied        *prometheus.CounterVec // be_authz_denied_total{reason}
}

// NewAuthzMetrics registers the authorization metrics (P18.3).
func NewAuthzMetrics(reg prometheus.Registerer) (*AuthzMetrics, error) {
	m := &AuthzMetrics{
		BundleAge:     gauge("be_authz_bundle_age_seconds", "Age of the authorization bundle in use, in seconds."),
		ProjectionLag: gauge("be_authz_projection_lag", "Authorization projection lag."),
		Denied:        counterVec("be_authz_denied_total", "Requests denied by authorization, by reason.", "reason"),
	}
	return m, register(reg, m.BundleAge, m.ProjectionLag, m.Denied)
}
