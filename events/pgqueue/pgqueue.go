// Package pgqueue is the PostgreSQL queue adapter of the events transport port (protocol P6.16-P6.23): messages in a
// table of the bus database, read per durable subscription in (transaction ID, sequence) order below
// pg_snapshot_xmin, deliveries claimed with FOR UPDATE SKIP LOCKED under a lease, and LISTEN/NOTIFY for best-effort
// notifications.
package pgqueue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Parameters of the adapter.
const (
	MaxMessage   = 1 << 20
	DefaultPoll  = 100 * time.Millisecond
	advanceBatch = 500
	notifyChan   = "be_bus_notify"
)

// Transport is the adapter.
type Transport struct {
	pool    *pgxpool.Pool
	cfg     *pgx.ConnConfig
	schema  string
	poll    time.Duration
	onError atomic.Pointer[func(error)]
	stop    context.CancelFunc
	purged  chan struct{}
}

// SetOnError sets where the errors of the background readers and the purge go (logged by the runtime); unset:
// dropped. It may be called while the transport runs.
func (t *Transport) SetOnError(fn func(error)) { t.onError.Store(&fn) }

// Purge timing (P6.27): every process with the transport purges soon after it opens and then every PurgeEvery, so
// even processes restarted more often than hourly purge; the deletes are idempotent.
const (
	FirstPurgeAfter = time.Minute
	PurgeEvery      = time.Hour
)

func (t *Transport) purgeLoop(ctx context.Context) {
	defer close(t.purged)
	wait := time.NewTimer(FirstPurgeAfter)
	defer wait.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wait.C:
			_, err := t.Purge(ctx, events.TransportRetention)
			if ctx.Err() == nil {
				t.report(err)
			}
			wait.Reset(PurgeEvery)
		}
	}
}

func (t *Transport) report(err error) {
	if fn := t.onError.Load(); err != nil && fn != nil && *fn != nil {
		(*fn)(err)
	}
}

// Config parses a pgqueue:// BUS_URL: pgqueue://user:password@host:port/database?schema=be_bus.
func Config(busURL string) (*pgx.ConnConfig, string, error) {
	u, err := url.Parse(busURL)
	if err != nil || u.Scheme != "pgqueue" {
		return nil, "", fmt.Errorf("bus: %q is not a pgqueue:// URL", redact(busURL))
	}
	q := u.Query()
	schema := q.Get("schema")
	if schema == "" {
		schema = "be_bus"
	}
	q.Del("schema")
	u.RawQuery = q.Encode()
	u.Scheme = "postgres"
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		return nil, "", fmt.Errorf("bus %s: %w", redact(busURL), err)
	}
	cfg.RuntimeParams["timezone"] = "UTC"
	return cfg, schema, nil
}

func redact(s string) string {
	if u, err := url.Parse(s); err == nil && u.User != nil {
		u.User = url.User(u.User.Username())
		return u.String()
	}
	return s
}

// Connect opens the adapter's own pool to the bus database; name labels its connections.
func Connect(ctx context.Context, busURL, name string, maxConns int32) (*Transport, error) {
	cfg, schema, err := Config(busURL)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["application_name"] = name + " bus"
	pc, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, err
	}
	pc.ConnConfig = cfg
	pc.MaxConns = max(maxConns, 4)
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("bus %s: %w", redact(busURL), err)
	}
	t := &Transport{pool: pool, cfg: cfg, schema: schema, poll: DefaultPoll, purged: make(chan struct{})}
	var purgeCtx context.Context
	purgeCtx, t.stop = context.WithCancel(context.Background())
	go t.purgeLoop(purgeCtx)
	return t, nil
}

// t qualifies a table name with the bus schema.
func (t *Transport) t(name string) string { return pgx.Identifier{t.schema, name}.Sanitize() }

// MaxMessage implements events.Transport.
func (t *Transport) MaxMessage() int { return MaxMessage }

// EnsureStream implements events.Transport.
func (t *Transport) EnsureStream(ctx context.Context, s events.Stream) error {
	_, err := t.pool.Exec(ctx, "INSERT INTO "+t.t("be_bus_stream")+" (name) VALUES ($1) ON CONFLICT DO NOTHING", s.Name)
	return err
}

func size(m events.Message) int {
	n := len(m.Data) + len(m.Subject)
	for k, v := range m.Headers {
		n += len(k) + len(v)
	}
	return n
}

// Publish implements events.Transport: the insert's commit is the acknowledgement; a repeated message ID is a no-op.
func (t *Transport) Publish(ctx context.Context, m events.Message) error {
	if size(m) > MaxMessage {
		return events.ErrTooLarge
	}
	headers, err := json.Marshal(m.Headers)
	if err != nil {
		return err
	}
	var id any
	if m.ID() != "" {
		id = m.ID()
	}
	_, err = t.pool.Exec(ctx, "INSERT INTO "+t.t("be_bus_message")+` (msg_id, subject, headers, data) VALUES ($1, $2, $3, $4)
		ON CONFLICT (msg_id) WHERE msg_id IS NOT NULL DO NOTHING`, id, m.Subject, headers, m.Data)
	return err
}

type subscription struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (s *subscription) Stop() error { s.cancel(); s.wg.Wait(); return nil }

// Subscribe implements events.Transport: the durable is created with spec when missing, otherwise its stored
// settings apply.
func (t *Transport) Subscribe(ctx context.Context, spec events.SubscriptionSpec, h events.Handler) (events.Subscription, error) {
	spec = spec.Defaults()
	backoff := make([]int32, len(spec.Backoff))
	for i, b := range spec.Backoff {
		backoff[i] = int32(b.Milliseconds())
	}
	filters := spec.Filters
	if filters == nil {
		filters = []string{}
	}
	if _, err := t.pool.Exec(ctx, "INSERT INTO "+t.t("be_bus_subscription")+` (durable, streams, filters, max_deliver,
		ack_wait_ms, backoff_ms) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`, spec.Durable, spec.Streams,
		filters, spec.MaxDeliver, int32(spec.AckWait.Milliseconds()), backoff); err != nil {
		return nil, err
	}
	stored, err := t.load(ctx, spec.Durable, spec.Concurrency)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	sub := &subscription{cancel: cancel}
	for i := 0; i < stored.Concurrency; i++ {
		sub.wg.Add(1)
		go func() { defer sub.wg.Done(); t.work(runCtx, stored, h) }()
	}
	return sub, nil
}

func (t *Transport) load(ctx context.Context, durable string, concurrency int) (events.SubscriptionSpec, error) {
	s := events.SubscriptionSpec{Durable: durable, Concurrency: concurrency}
	var ackMs int32
	var backoff []int32
	err := t.pool.QueryRow(ctx, "SELECT streams, filters, max_deliver, ack_wait_ms, backoff_ms FROM "+
		t.t("be_bus_subscription")+" WHERE durable = $1", durable).Scan(&s.Streams, &s.Filters, &s.MaxDeliver, &ackMs, &backoff)
	s.AckWait = time.Duration(ackMs) * time.Millisecond
	for _, b := range backoff {
		s.Backoff = append(s.Backoff, time.Duration(b)*time.Millisecond)
	}
	return s.Defaults(), err
}

func matches(s events.SubscriptionSpec, subject string) bool {
	in := false
	for _, st := range s.Streams {
		if events.SubjectMatches("be."+st+".>", subject) {
			in = true
		}
	}
	if !in {
		return false
	}
	if len(s.Filters) == 0 {
		return true
	}
	for _, f := range s.Filters {
		if events.SubjectMatches(f, subject) {
			return true
		}
	}
	return false
}

func (t *Transport) work(ctx context.Context, s events.SubscriptionSpec, h events.Handler) {
	for ctx.Err() == nil {
		if err := t.advance(ctx, s); err != nil && ctx.Err() == nil {
			t.report(fmt.Errorf("bus subscription %s: %w", s.Durable, err))
			sleep(ctx, t.poll)
			continue
		}
		d, token, err := t.claim(ctx, s)
		if err != nil && ctx.Err() == nil {
			t.report(fmt.Errorf("bus subscription %s: %w", s.Durable, err))
		}
		if err != nil || d == nil {
			sleep(ctx, t.poll)
			continue
		}
		d.InProgress = func() error {
			_, err := t.pool.Exec(context.WithoutCancel(ctx), "UPDATE "+t.t("be_bus_delivery")+` SET lease_until =
				clock_timestamp() + make_interval(secs => $3) WHERE durable = $1 AND lease_token = $2 AND in_flight`,
				s.Durable, token, s.AckWait.Seconds())
			return err
		}
		t.settle(context.WithoutCancel(ctx), s, d, token, h(ctx, d))
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// advance moves the subscription's read position over committed messages below pg_snapshot_xmin, queueing a
// delivery for every matching one (P6.23).
func (t *Transport) advance(ctx context.Context, s events.SubscriptionSpec) error {
	tx, err := t.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var posXid, posSeq int64
	if err := tx.QueryRow(ctx, "SELECT pos_xid::text::bigint, pos_seq FROM "+t.t("be_bus_subscription")+
		" WHERE durable = $1 FOR UPDATE", s.Durable).Scan(&posXid, &posSeq); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, "SELECT xid::text::bigint, seq, subject FROM "+t.t("be_bus_message")+`
		WHERE (xid, seq) > ($1::bigint::text::xid8, $2) AND xid < pg_snapshot_xmin(pg_current_snapshot())
		ORDER BY xid, seq LIMIT $3`, posXid, posSeq, advanceBatch)
	if err != nil {
		return err
	}
	var seqs []int64
	lastXid, lastSeq := posXid, posSeq
	for rows.Next() {
		var x, q int64
		var subject string
		if err := rows.Scan(&x, &q, &subject); err != nil {
			rows.Close()
			return err
		}
		lastXid, lastSeq = x, q
		if matches(s, subject) {
			seqs = append(seqs, q)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if lastXid == posXid && lastSeq == posSeq {
		return nil
	}
	if len(seqs) > 0 {
		if _, err := tx.Exec(ctx, "INSERT INTO "+t.t("be_bus_delivery")+` (durable, seq)
			SELECT $1, unnest($2::bigint[]) ON CONFLICT DO NOTHING`, s.Durable, seqs); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, "UPDATE "+t.t("be_bus_subscription")+
		" SET pos_xid = $2::bigint::text::xid8, pos_seq = $3 WHERE durable = $1", s.Durable, lastXid, lastSeq); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// claim takes one due delivery: ready and past its backoff, or in flight past its lease with deliveries left.
// Exhausted deliveries whose lease expired are given up.
func (t *Transport) claim(ctx context.Context, s events.SubscriptionSpec) (*events.Delivery, string, error) {
	// exhausted leases go, and so do deliveries whose message the retention purge removed (P6.27)
	if _, err := t.pool.Exec(ctx, "DELETE FROM "+t.t("be_bus_delivery")+` d WHERE durable = $1 AND (
			(in_flight AND lease_until < clock_timestamp() AND attempts >= $2) OR
			NOT EXISTS (SELECT 1 FROM `+t.t("be_bus_message")+` m WHERE m.seq = d.seq))`, s.Durable, s.MaxDeliver); err != nil {
		return nil, "", err
	}
	token := ids.New().String()
	var seq int64
	var attempts int
	var subject string
	var headers, data []byte
	err := t.pool.QueryRow(ctx, "WITH due AS (SELECT seq FROM "+t.t("be_bus_delivery")+` WHERE durable = $1
			AND ((NOT in_flight AND not_before <= clock_timestamp()) OR (in_flight AND lease_until < clock_timestamp()))
			ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED)
		UPDATE `+t.t("be_bus_delivery")+` d SET in_flight = true, attempts = d.attempts + 1, lease_token = $2,
			lease_until = clock_timestamp() + make_interval(secs => $3)
		FROM due WHERE d.durable = $1 AND d.seq = due.seq
		RETURNING d.seq, d.attempts, (SELECT subject FROM `+t.t("be_bus_message")+` m WHERE m.seq = d.seq),
			(SELECT headers FROM `+t.t("be_bus_message")+` m WHERE m.seq = d.seq),
			(SELECT data FROM `+t.t("be_bus_message")+` m WHERE m.seq = d.seq)`,
		s.Durable, token, s.AckWait.Seconds()).Scan(&seq, &attempts, &subject, &headers, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	m := events.Message{Subject: subject, Data: data, Headers: map[string]string{}}
	if err := json.Unmarshal(headers, &m.Headers); err != nil {
		return nil, "", err
	}
	return &events.Delivery{Message: m, Attempt: attempts}, token, nil
}

// settle applies the handler's outcome, unless the lease was lost meanwhile (then the late outcome changes nothing).
func (t *Transport) settle(ctx context.Context, s events.SubscriptionSpec, d *events.Delivery, token string, out events.Outcome) {
	switch {
	case out == events.Ack || out == events.Term || (out == events.Nak && d.Attempt >= s.MaxDeliver):
		_, _ = t.pool.Exec(ctx, "DELETE FROM "+t.t("be_bus_delivery")+" WHERE durable = $1 AND lease_token = $2 AND in_flight",
			s.Durable, token)
	default:
		_, _ = t.pool.Exec(ctx, "UPDATE "+t.t("be_bus_delivery")+` SET in_flight = false, lease_token = NULL,
			not_before = clock_timestamp() + make_interval(secs => $3) WHERE durable = $1 AND lease_token = $2 AND in_flight`,
			s.Durable, token, s.BackoffFor(d.Attempt).Seconds())
	}
}

type notification struct {
	Subject string `json:"s"`
	Data    string `json:"d"`
}

// Notify implements events.Transport with NOTIFY: nothing is stored (P6.22).
func (t *Transport) Notify(ctx context.Context, subject string, data []byte) error {
	b, _ := json.Marshal(notification{Subject: subject, Data: base64.StdEncoding.EncodeToString(data)})
	_, err := t.pool.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChan, string(b))
	return err
}

type listener struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (l *listener) Stop() error { l.cancel(); <-l.done; return nil }

// Listen implements events.Transport on a dedicated connection.
func (t *Transport) Listen(ctx context.Context, subject string, fn func([]byte)) (events.Subscription, error) {
	conn, err := pgx.ConnectConfig(ctx, t.cfg.Copy())
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, "LISTEN "+notifyChan); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	l := &listener{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(l.done)
		defer conn.Close(context.Background())
		for {
			n, err := conn.WaitForNotification(runCtx)
			if err != nil {
				return
			}
			var msg notification
			if json.Unmarshal([]byte(n.Payload), &msg) != nil || !events.SubjectMatches(subject, msg.Subject) {
				continue
			}
			if data, err := base64.StdEncoding.DecodeString(msg.Data); err == nil {
				fn(data)
			}
		}
	}()
	return l, nil
}

// Purge deletes messages older than retention whether or not every subscription read them, with their deliveries
// (P6.27): the queue is a transport buffer, and a subscriber away longer recovers through redelivery and alignment.
func (t *Transport) Purge(ctx context.Context, retention time.Duration) (int64, error) {
	var n int64
	err := t.pool.QueryRow(ctx, `WITH gone AS (DELETE FROM `+t.t("be_bus_message")+`
			WHERE published_at < now() - make_interval(secs => $1) RETURNING seq),
		deliveries AS (DELETE FROM `+t.t("be_bus_delivery")+` WHERE seq IN (SELECT seq FROM gone))
		SELECT count(*) FROM gone`, retention.Seconds()).Scan(&n)
	return n, err
}

// Close implements events.Transport.
func (t *Transport) Close() error { t.stop(); <-t.purged; t.pool.Close(); return nil }

// Migrate creates or updates the queue's tables in the bus schema, as the bus owner (provisioning, tests).
func Migrate(ctx context.Context, ownerURL string, log *slog.Logger) error {
	cfg, schema, err := Config(ownerURL)
	if err != nil {
		return err
	}
	values, problems := config.Parse([]config.Key{{Name: "PW", Type: config.String, Secret: true}}, config.Map{"PW": cfg.Password})
	if len(problems) > 0 {
		return fmt.Errorf("bus owner password: %v", problems)
	}
	files, _ := fs.Sub(protocol.BusMigrations, "migrations-bus")
	return db.Migrate(ctx, db.MigrateConfig{Target: db.Target{Host: cfg.Host, Port: int(cfg.Port), Database: cfg.Database},
		User: cfg.User, Password: values.Secret("PW"), Schema: schema, Member: "bus", Log: log,
		LockWait: db.MigrationLockWait, Attempts: db.MigrationLockAttempts},
		db.MigrationSet{Name: "be-bus", Table: "be_bus_migrations", Files: files})
}
