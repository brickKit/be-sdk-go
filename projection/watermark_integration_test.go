//go:build integration

package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"testing/fstest"
)

// A watermark source (the people of demo/wmsrc) and a consumer keeping a projection of it, each in its own schema; the
// pull goes over a real system surface (mTLS), the stream through the memory transport when a test turns it on.
type wmWorld struct {
	t        *testing.T
	src      *db.DB
	srcAdmin *pgx.Conn
	producer *events.Producer
	con      *db.DB
	conAdmin *pgx.Conn
	w        *Watermarked
	bus      *events.Memory
	failing  atomic.Value // a record ID whose input fails
}

var peopleStream = events.Stream{Name: "demo.people", Partitions: 2,
	Watermark: &events.Watermark{Operation: "demo/wmsrc.pull", Pullers: []string{"demo/wmcon"}}}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newWMWorld(t *testing.T, name string) *wmWorld {
	ww := &wmWorld{t: t}
	ww.src, ww.srcAdmin = dbtest.Schema(t, name+"_src", fstest.MapFS{"0001_people.up.sql": {Data: []byte(
		"CREATE TABLE people (id text PRIMARY KEY, version bigint NOT NULL, name text, deleted boolean NOT NULL DEFAULT false);")}})
	ww.con, ww.conAdmin = dbtest.Schema(t, name+"_con", nil)
	var err error
	ww.producer, err = events.NewProducer("demo/wmsrc", []events.Stream{peopleStream}, nil)
	require.NoError(t, err)
	source := &events.WatermarkSource{DB: ww.src, Producer: ww.producer, Source: "demo/wmsrc", Impl: "impl-1",
		Deployment: "dep-1", Log: quiet()}
	m, ok, err := source.Method()
	require.NoError(t, err)
	require.True(t, ok)
	world := sectest.New(t)
	var services grpcx.Services
	services.Register(&sourcepb.WatermarkService_ServiceDesc, source, map[string]grpcx.Method{"Pull": m})
	member := obs.Member{ID: "demo/wmsrc", Version: "3.0.0"}
	srv, err := grpcx.NewServer(grpcx.Deps{Member: member, Logger: obs.NewLogger(member, slog.LevelError, io.Discard),
		Registry: prometheus.NewRegistry(), Tracer: sdktrace.NewTracerProvider().Tracer("t"), TLS: world.TLS(t, "demo/wmsrc")}, &services)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	cm, err := calls.NewMember(prometheus.NewRegistry())
	require.NoError(t, err)
	cm.SetTLS(world.TLS(t, "demo/wmcon"))
	t.Cleanup(cm.Close)
	conn, err := cm.GRPC("demo/wmsrc", ln.Addr().String())
	require.NoError(t, err)
	ww.failing.Store("")
	ww.w = &Watermarked{Projection: Projection{Name: "people"}, DB: ww.con, Source: "demo/wmsrc", Stream: peopleStream,
		Deployment: "dep-1", Freshness: 4 * time.Second, Pull: grpcPuller{conn}, Snapshot: peopleSnapshot{ww.src}, Log: quiet(),
		Input: func(e events.Received) (Input, error) {
			if e.RecordID == ww.failing.Load().(string) {
				return Input{}, errors.New("this record's input fails (the test)")
			}
			kind := map[events.Projection]string{events.Full: Full, events.Partial: Partial, events.Delete: Delete}[e.Projection]
			return Input{RecordID: e.RecordID, Kind: kind, Version: e.Version, Data: e.Data}, nil
		}}
	require.NoError(t, ww.w.Install(context.Background()))
	return ww
}

type grpcPuller struct{ conn *grpc.ClientConn }

func (g grpcPuller) Pull(ctx context.Context, req *sourcepb.PullRequest) (*sourcepb.PullResponse, error) {
	resp := new(sourcepb.PullResponse)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return resp, g.conn.Invoke(ctx, "/be.source.v1.WatermarkService/Pull", req, resp, calls.Idempotent())
}

// peopleSnapshot is the source's snapshot and batch read (S3), straight from its table.
type peopleSnapshot struct{ d *db.DB }

func (s peopleSnapshot) Snapshot(ctx context.Context, cursor string) ([]Record, string, error) {
	var out []Record
	err := s.d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT id, version, name FROM people WHERE NOT deleted AND id > $1 ORDER BY id", cursor)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, name string
			var v int64
			if err := rows.Scan(&id, &v, &name); err != nil {
				return err
			}
			b, _ := json.Marshal(map[string]string{"id": id, "name": name})
			out = append(out, Record{ID: id, Version: v, Data: b})
		}
		return rows.Err()
	})
	return out, "", err
}

func (s peopleSnapshot) Read(ctx context.Context, ids []string) ([]ReadResult, error) {
	var out []ReadResult
	err := s.d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		for _, id := range ids {
			var v int64
			var name string
			var deleted bool
			err := tx.QueryRow(ctx, "SELECT version, name, deleted FROM people WHERE id = $1", id).Scan(&v, &name, &deleted)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				out = append(out, ReadResult{ID: id, State: Purged})
			case err != nil:
				return err
			case deleted:
				out = append(out, ReadResult{ID: id, State: Gone, Version: v})
			default:
				b, _ := json.Marshal(map[string]string{"id": id, "name": name})
				out = append(out, ReadResult{ID: id, State: Exists, Version: v, Data: b})
			}
		}
		return nil
	})
	return out, err
}

func (s peopleSnapshot) Identity(context.Context) (string, string, error) {
	return "", "", errors.New("a watermark source declares its identity in every response")
}

// set changes one person on the source and emits the event in the same transaction.
func (ww *wmWorld) set(id, name string) {
	require.NoError(ww.t, ww.src.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		var v int64
		if err := tx.QueryRow(ctx, `INSERT INTO people (id, version, name) VALUES ($1, 1, $2)
			ON CONFLICT (id) DO UPDATE SET version = people.version + 1, name = excluded.name RETURNING version`, id, name).
			Scan(&v); err != nil {
			return err
		}
		_, err := ww.producer.Emit(ctx, tx, "demo.people", events.Event{Type: "demo.person.changed.v1", RecordID: id,
			Version: v, Projection: events.Full, Data: map[string]string{"id": id, "name": name}, Subjects: []string{"person:" + id}})
		return err
	}))
}

// step runs the consumer's upkeep n times.
func (ww *wmWorld) step(n int) {
	for range n {
		require.NoError(ww.t, ww.w.Step(context.Background()))
	}
}

func (ww *wmWorld) judge(id string) string {
	var got string
	require.NoError(ww.t, ww.con.BackgroundTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		var err error
		got, err = ww.w.Judge(ctx, tx, id, []string{"person:" + id})
		return err
	}))
	return got
}

func (ww *wmWorld) state() string {
	var s string
	require.NoError(ww.t, ww.conAdmin.QueryRow(context.Background(), "SELECT status FROM be_projection_source WHERE projection = 'people'").Scan(&s))
	return s
}

func (ww *wmWorld) name(id string) string {
	var d []byte
	err := ww.conAdmin.QueryRow(context.Background(), "SELECT data FROM be_projection WHERE projection = 'people' AND record_id = $1", id).Scan(&d)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	require.NoError(ww.t, err)
	var v struct{ Name string }
	_ = json.Unmarshal(d, &v)
	return v.Name
}

// deliver hands the source's numbered rows of one partition to the consumer's handler as the stream would, in order;
// skip leaves those positions out (lost on the way).
func (ww *wmWorld) deliver(partition int, skip ...int64) {
	_, err := events.Number(context.Background(), ww.src, "demo.people", partition, 1000)
	require.NoError(ww.t, err)
	rows, err := ww.srcAdmin.Query(context.Background(), `SELECT event_id::text, type, record_id, version, projection, digest,
		seq, epoch_generation || '.' || epoch_timeline, coalesce(subjects::text, '[]'), payload FROM be_outbox
		WHERE stream = 'demo.people' AND partition = $1 AND seq IS NOT NULL ORDER BY epoch_generation, epoch_timeline, seq`, partition)
	require.NoError(ww.t, err)
	type r struct {
		id, typ, rec, proj, digest, epoch, subjects string
		v, seq                                      int64
		payload                                     []byte
	}
	var all []r
	for rows.Next() {
		var x r
		require.NoError(ww.t, rows.Scan(&x.id, &x.typ, &x.rec, &x.v, &x.proj, &x.digest, &x.seq, &x.epoch, &x.subjects, &x.payload))
		all = append(all, x)
	}
	require.NoError(ww.t, rows.Err())
	skipped := map[int64]bool{}
	for _, s := range skip {
		skipped[s] = true
	}
	for _, x := range all {
		if skipped[x.seq] {
			continue
		}
		e := events.Received{ID: x.id, Type: x.typ, RecordID: x.rec, Version: x.v, Projection: events.Projection(x.proj),
			Stream: "demo.people", Partition: partition, Seq: x.seq, Epoch: x.epoch, Impl: "impl-1", Data: x.payload,
			Headers: map[string]string{events.HeaderDigest: x.digest, events.HeaderSubjects: x.subjects}}
		_ = ww.con.BackgroundTx(context.Background(), func(ctx context.Context, tx db.Tx) error { return ww.w.Handle(ctx, tx, e) })
	}
}

func (ww *wmWorld) contiguous(partition int) *int64 {
	var s *int64
	require.NoError(ww.t, ww.conAdmin.QueryRow(context.Background(),
		"SELECT seq FROM be_watermark_partition WHERE projection = 'people' AND partition = $1", partition).Scan(&s))
	return s
}

func partitionOf(id string) int { return peopleStream.PartitionOf(id) }

var _ = fmt.Sprint
var _ = strconv.Itoa
var _ = pgtest.Exec

func TestAWatermarkProvesTheProjectionFresh(t *testing.T) { // CK-WM-05, CK-WM-06, P9.21, P9.23, P9.24, PJ19, PJ28
	ww := newWMWorld(t, "wm1")
	ww.set("p1", "Ann")
	assert.Equal(t, Unprovable, ww.judge("p1"), "nothing adopted yet")
	ww.step(1) // the first pull establishes the source: aligning
	assert.Equal(t, Aligning, ww.state())
	assert.Equal(t, Unprovable, ww.judge("p1"))
	ww.step(1) // the re-fetch from P0
	assert.Equal(t, Aligned, ww.state())
	assert.Equal(t, "Ann", ww.name("p1"), "the snapshot brought the row")
	assert.Equal(t, Unprovable, ww.judge("p1"), "aligned, but no echo of a pull after P0 yet")
	ww.step(1) // the first pull after the alignment: its echo is the watermark
	assert.Equal(t, Usable, ww.judge("p1"))
	assert.Equal(t, ProvablyAbsent, ww.judge("nobody"))

	// the stream is down the whole test (nothing delivers): changes still arrive by the pulls, the watermark stays fresh
	ww.set("p1", "Anna")
	ww.set("p2", "Bob")
	ww.step(1)
	assert.Equal(t, "Ann", ww.name("p1"), "a watermark that is not yet half its freshness old pulls nothing")
	time.Sleep(ww.w.Freshness/2 + 200*time.Millisecond)
	ww.step(1)
	assert.Equal(t, "Anna", ww.name("p1"))
	assert.Equal(t, "Bob", ww.name("p2"))
	assert.Equal(t, Usable, ww.judge("p2"))

	// no pull for longer than the freshness: unprovable
	time.Sleep(ww.w.Freshness + 200*time.Millisecond)
	assert.Equal(t, Unprovable, ww.judge("p1"), "an expired watermark")
}

func TestAHoleStopsTheContiguousPosition(t *testing.T) { // CK-WM-04, P9.19, PJ27
	ww := newWMWorld(t, "wm2")
	ww.step(3) // established, aligned, watermark
	require.Equal(t, Aligned, ww.state())
	p := partitionOf("x1")
	start := *ww.contiguous(p)
	// three changes of records in partition p; the middle one is lost on the way
	var ids []string
	for i := 0; len(ids) < 3; i++ {
		if id := "x" + strconv.Itoa(i); partitionOf(id) == p {
			ids = append(ids, id)
			ww.set(id, "v"+strconv.Itoa(i))
		}
	}
	ww.deliver(p, start+2)
	assert.Equal(t, start+1, *ww.contiguous(p), "the position stops before the hole")
	assert.Equal(t, "", ww.name(ids[2]), "the row after the hole is not applied")
	time.Sleep(ww.w.Freshness/2 + 200*time.Millisecond)
	ww.step(1) // the pull fills the hole
	assert.Equal(t, "v"+ids[2][1:], ww.name(ids[2]))
	assert.Greater(t, *ww.contiguous(p), start+3)
	assert.Equal(t, Usable, ww.judge(ids[2]))
}

// ready brings the consumer to aligned with a fresh watermark.
func (ww *wmWorld) ready() {
	ww.step(3)
	require.Equal(ww.t, Aligned, ww.state())
}

func (ww *wmWorld) pullNow() error { return ww.w.pull(context.Background()) }

func TestALargerEpochIsAdoptedThroughAnAlignment(t *testing.T) { // CK-WM-07, P9.27, P9.28, PJ26, PJ32
	ww := newWMWorld(t, "wm3")
	ww.set("p1", "Ann")
	ww.ready()
	require.Equal(t, Usable, ww.judge("p1"))
	var before string
	require.NoError(t, ww.conAdmin.QueryRow(context.Background(), "SELECT epoch FROM be_projection_source WHERE projection = 'people'").Scan(&before))

	// the source's database was restored and its recovery generation advanced: a new epoch, positions reused
	pgtest.Exec(t, ww.srcAdmin, "UPDATE be_recovery_generation SET generation = generation + 1")
	ww.set("p1", "Anne")
	require.NoError(t, ww.pullNow())
	assert.Equal(t, Aligning, ww.state(), "a larger epoch is adopted through an alignment")
	assert.Equal(t, Unprovable, ww.judge("p1"), "unprovable until the alignment completes")
	var after string
	require.NoError(t, ww.conAdmin.QueryRow(context.Background(), "SELECT epoch FROM be_projection_source WHERE projection = 'people'").Scan(&after))
	assert.Equal(t, Generation(before)+1, Generation(after))

	// an event of the old epoch arriving late is ignored
	p := partitionOf("p1")
	late := events.Received{ID: "00000000-0000-7000-8000-000000000001", Type: "demo.person.changed.v1", RecordID: "p1",
		Version: 99, Projection: events.Full, Stream: "demo.people", Partition: p, Seq: 1, Epoch: before, Impl: "impl-1",
		Data: []byte(`{"id":"p1","name":"Old"}`), Headers: map[string]string{}}
	require.NoError(t, ww.con.BackgroundTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		return ww.w.Handle(ctx, tx, late)
	}))
	ww.step(2) // the re-fetch, then the first pull of the new epoch
	assert.Equal(t, Aligned, ww.state())
	assert.Equal(t, "Anne", ww.name("p1"), "the new epoch's content, not the late old event")
	assert.Equal(t, Usable, ww.judge("p1"))
}

func TestARollbackStallsUntilTheGenerationAdvances(t *testing.T) { // CK-WM-08, P9.29, PJ44, PJ48
	ww := newWMWorld(t, "wm4")
	ww.set("p1", "Ann")
	ww.ready()
	require.Equal(t, Usable, ww.judge("p1"))
	// restored without advancing the recovery generation: the row at the consumer's position is another one now
	p := partitionOf("p1")
	pgtest.Exec(t, ww.srcAdmin, fmt.Sprintf(`UPDATE be_outbox SET record_id = 'replaced' WHERE stream = 'demo.people'
		AND partition = %d AND seq = %d`, p, *ww.contiguous(p)))
	require.NoError(t, ww.pullNow())
	assert.Equal(t, Stalled, ww.state(), "rollback_detected")
	assert.Equal(t, Unprovable, ww.judge("p1"))
	ww.step(1) // the re-fetch runs, the source stays stalled
	assert.Equal(t, Stalled, ww.state())
	require.NoError(t, ww.pullNow())
	assert.Equal(t, Stalled, ww.state(), "a pull of the same generation does not lift it")

	pgtest.Exec(t, ww.srcAdmin, "UPDATE be_recovery_generation SET generation = generation + 1")
	require.NoError(t, ww.pullNow())
	assert.Equal(t, Aligning, ww.state(), "a larger recovery generation is the exit")
	ww.step(2)
	assert.Equal(t, Aligned, ww.state())
	assert.Equal(t, Usable, ww.judge("p1"))
}

func TestAFailingEventQuarantinesItsSubjectsOnly(t *testing.T) { // CK-WM-09, P9.25, PJ30, PJ37
	ww := newWMWorld(t, "wm5")
	ww.set("ok1", "Ok")
	ww.ready()
	ww.failing.Store("bad1")
	ww.set("bad1", "Bad")
	ww.set("ok2", "Ok2")
	for range QuarantineAttempts {
		assert.Error(t, ww.pullNow(), "the failing row stops the pull")
	}
	require.NoError(t, ww.pullNow(), "quarantined: the pull goes on past it")
	var quarantined int
	require.NoError(t, ww.conAdmin.QueryRow(context.Background(), `SELECT count(*) FROM be_watermark_quarantine
		WHERE projection = 'people' AND subject = 'person:bad1'`).Scan(&quarantined))
	assert.Equal(t, 1, quarantined)
	assert.Equal(t, Unprovable, ww.judge("bad1"), "its subject is quarantined")
	assert.Equal(t, Usable, ww.judge("ok2"), "the others go on")
	assert.Equal(t, "Ok2", ww.name("ok2"))
}

func TestAnUnknownInstanceBlocksUntilAConfirmedCorrection(t *testing.T) { // CK-WM-10, P9.26, PJ77
	ww := newWMWorld(t, "wm6")
	ww.ready()
	foreign := events.Received{ID: "00000000-0000-7000-8000-000000000002", Type: "demo.person.changed.v1", RecordID: "z",
		Version: 1, Projection: events.Full, Stream: "demo.people", Partition: 0, Seq: 1, Epoch: "9.1", Impl: "impl-other",
		Data: []byte(`{}`), Headers: map[string]string{}}
	require.NoError(t, ww.con.BackgroundTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		return ww.w.Handle(ctx, tx, foreign)
	}))
	assert.Equal(t, Blocked, ww.state())
	assert.Equal(t, "", ww.name("z"), "not applied")
	var echoes int
	count := func() int {
		require.NoError(t, ww.srcAdmin.QueryRow(context.Background(), "SELECT count(*) FROM be_outbox WHERE type = $1", events.EchoType).Scan(&echoes))
		return echoes
	}
	n := count()
	time.Sleep(ww.w.Freshness/2 + 200*time.Millisecond)
	ww.step(2)
	assert.Equal(t, n, count(), "blocked: no pull writes echoes anywhere")
	require.NoError(t, ww.w.Confirm(context.Background()))
	ww.step(1) // the next primary answer: the adopted instance, its row check passes
	assert.Equal(t, Aligning, ww.state())
	ww.step(2)
	assert.Equal(t, Aligned, ww.state())
}

// rewritingPuller declares another system identifier than the source's: the database restored onto another cluster.
type rewritingPuller struct {
	Puller
	system string
}

func (r rewritingPuller) Pull(ctx context.Context, req *sourcepb.PullRequest) (*sourcepb.PullResponse, error) {
	resp, err := r.Puller.Pull(ctx, req)
	if err == nil && resp.GetSource() != nil {
		resp.Source.SystemId = r.system
	}
	return resp, err
}

func TestAnotherSystemWithoutALargerGenerationStalls(t *testing.T) { // CK-WM-08, P9.27, PJ49
	ww := newWMWorld(t, "wm7")
	ww.ready()
	ww.w.Pull = rewritingPuller{Puller: ww.w.Pull, system: "another-cluster"}
	require.NoError(t, ww.pullNow())
	assert.Equal(t, Stalled, ww.state(), "a new system identifier and the same recovery generation: restored without advancing it")
	pgtest.Exec(t, ww.srcAdmin, "UPDATE be_recovery_generation SET generation = generation + 1")
	require.NoError(t, ww.pullNow())
	assert.Equal(t, Aligning, ww.state(), "with a larger generation the new system is adopted (a major upgrade, a migration)")
	ww.step(2)
	assert.Equal(t, Aligned, ww.state())
}
