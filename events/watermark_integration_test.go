//go:build integration

package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var people = Stream{Name: "demo.people", Partitions: 1,
	Watermark: &Watermark{Operation: "demo/wmsrc.pull", Pullers: []string{"demo/consumer"}}}

// watermarkSource provisions a source schema and serves its pull on a real system surface (mTLS).
func watermarkSource(t *testing.T, name string, s ...Stream) (*db.DB, *pgx.Conn, *Producer, *sectest.World, string) {
	d, admin := source(t, name)
	p, err := NewProducer("demo/wmsrc", s, nil)
	require.NoError(t, err)
	src := &WatermarkSource{DB: d, Producer: p, Source: "demo/wmsrc", Impl: "impl-1", Deployment: "dep-1",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	m, ok, err := src.Method()
	require.NoError(t, err)
	require.True(t, ok)
	w := sectest.New(t)
	var services grpcx.Services
	services.Register(&sourcepb.WatermarkService_ServiceDesc, src, map[string]grpcx.Method{"Pull": m})
	member := obs.Member{ID: "demo/wmsrc", Version: "3.0.0"}
	srv, err := grpcx.NewServer(grpcx.Deps{Member: member, Logger: obs.NewLogger(member, slog.LevelError, io.Discard),
		Registry: prometheus.NewRegistry(), Tracer: sdktrace.NewTracerProvider().Tracer("t"), TLS: w.TLS(t, "demo/wmsrc")}, &services)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return d, admin, p, w, ln.Addr().String()
}

// pull calls the source as caller and follows its pages.
func pull(t *testing.T, w *sectest.World, addr, caller string, req *sourcepb.PullRequest) ([]*sourcepb.PullResponse, error) {
	m, err := calls.NewMember(prometheus.NewRegistry())
	require.NoError(t, err)
	m.SetTLS(w.TLS(t, caller))
	t.Cleanup(m.Close)
	conn, err := m.GRPC("demo/wmsrc", addr)
	require.NoError(t, err)
	var pages []*sourcepb.PullResponse
	for {
		resp := new(sourcepb.PullResponse)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := conn.Invoke(ctx, "/be.source.v1.WatermarkService/Pull", req, resp, calls.Idempotent())
		cancel()
		if err != nil {
			return pages, err
		}
		pages = append(pages, resp)
		if resp.GetNextPageToken() == "" {
			return pages, nil
		}
		req = &sourcepb.PullRequest{PageToken: resp.GetNextPageToken()}
	}
}

func nonce() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func emitPerson(ctx context.Context, tx db.Tx, p *Producer) (string, error) {
	id := ids.New().String()
	_, err := p.Emit(ctx, tx, "demo.people", Event{Type: "demo.person.changed.v1", RecordID: id, Version: 1,
		Projection: Full, Data: map[string]string{"id": id}, Subjects: []string{"person:" + id}})
	return id, err
}

func emitPeople(t *testing.T, d *db.DB, p *Producer, n int) []string {
	var out []string
	for range n {
		require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			id, err := emitPerson(ctx, tx, p)
			out = append(out, id)
			return err
		}))
	}
	return out
}

// events of a pull's pages for partition 0, and the echo position of its last page
func pulled(pages []*sourcepb.PullResponse) (records []string, types []string, echo *sourcepb.Position, need sourcepb.NeedSnapshot) {
	for _, pg := range pages {
		for _, r := range pg.GetPartitions() {
			for _, e := range r.GetEvents() {
				records = append(records, e.GetHeaders()[HeaderSubject])
				types = append(types, e.GetHeaders()[HeaderType])
			}
			if r.GetEcho() != nil {
				echo = r.GetEcho()
			}
			if r.GetNeedSnapshot() != sourcepb.NeedSnapshot_NEED_SNAPSHOT_UNSPECIFIED {
				need = r.GetNeedSnapshot()
			}
		}
	}
	return
}

func TestOnlyTheDeclaredPullersMayPull(t *testing.T) { // CK-WM-01, P9.13
	// two watermark streams, each with its own puller: the system surface admits both callers, the stream decides
	teams := Stream{Name: "demo.teams", Partitions: 1,
		Watermark: &Watermark{Operation: "demo/wmsrc.pull", Pullers: []string{"demo/other"}}}
	d, _, p, w, addr := watermarkSource(t, "wmsrc1", people, teams)
	_, err := pull(t, w, addr, "demo/other", &sourcepb.PullRequest{Stream: "demo.people", Nonce: nonce()})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "a component the stream does not list")
	_, err = pull(t, w, addr, "demo/consumer", &sourcepb.PullRequest{Stream: "demo.teams", Nonce: nonce()})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "a puller of another stream")
	_, err = pull(t, w, addr, "demo/stranger", &sourcepb.PullRequest{Stream: "demo.teams", Nonce: nonce()})
	assert.Error(t, err, "a component no stream lists is refused by the system surface already")
	pages, err := pull(t, w, addr, "demo/consumer", &sourcepb.PullRequest{Stream: "demo.people", Nonce: nonce()})
	require.NoError(t, err)
	decl := pages[0].GetSource()
	assert.True(t, decl.GetPrimary())
	assert.Equal(t, "dep-1", decl.GetDeployment())
	assert.Equal(t, "impl-1", decl.GetImpl())
	assert.NotEmpty(t, decl.GetSystemId())
	assert.Regexp(t, `^[0-9]+\.[0-9]+$`, decl.GetEpoch())

	err = d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := p.Emit(ctx, tx, "demo.people", Event{Type: "demo.person.changed.v1", RecordID: "r", Version: 1,
			Projection: Full})
		return err
	})
	assert.Error(t, err, "an event on a watermark stream lists its subjects")
}

func TestAPullEchoesAfterTheChangesBeforeIt(t *testing.T) { // CK-WM-02, P9.16, PJ23, PJ25, PJ29
	d, _, p, w, addr := watermarkSource(t, "wmsrc2", people)
	before := emitPeople(t, d, p, 3)
	// a rolled-back change and a long transaction still open when the pull comes
	require.Error(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := emitPerson(ctx, tx, p)
		require.NoError(t, err)
		return assert.AnError
	}))
	held, release, longDone := make(chan string), make(chan struct{}), make(chan error)
	go func() {
		longDone <- d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			id, err := emitPerson(ctx, tx, p)
			held <- id
			<-release
			return err
		})
	}()
	long := <-held

	// echo only: the echo comes after the three committed changes; the rollback left no gap, the open
	// transaction's row is not numbered yet
	n1 := nonce()
	pages, err := pull(t, w, addr, "demo/consumer", &sourcepb.PullRequest{Stream: "demo.people", Nonce: n1})
	require.NoError(t, err)
	_, types, echo, _ := pulled(pages)
	require.NotNil(t, echo)
	assert.Equal(t, []string{EchoType}, types, "echo only returns the echo")
	assert.Equal(t, uint64(4), echo.GetSeq(), "the three committed changes are 1-3, before the echo")

	// from the echo on: the long transaction commits now and is numbered after the echo, then the later changes
	close(release)
	require.NoError(t, <-longDone)
	after := emitPeople(t, d, p, 2)
	pages, err = pull(t, w, addr, "demo/consumer", &sourcepb.PullRequest{Stream: "demo.people", Nonce: nonce(),
		Partitions: []*sourcepb.PartitionPosition{{Partition: 0, Position: echo,
			Row: &sourcepb.RowIdentity{Row: &sourcepb.RowIdentity_EchoNonce{EchoNonce: n1}}}}})
	require.NoError(t, err)
	recs, types, echo2, need := pulled(pages)
	assert.Equal(t, sourcepb.NeedSnapshot_NEED_SNAPSHOT_UNSPECIFIED, need)
	require.Len(t, recs, 4)
	assert.Equal(t, []string{long, after[0], after[1]}, recs[:3], "the long transaction's row only delays itself")
	assert.Equal(t, EchoType, types[3])
	assert.Equal(t, uint64(8), echo2.GetSeq())
	assert.Len(t, before, 3)
}

func TestAPullPagesWithTheEchoOnTheLastPage(t *testing.T) { // CK-WM-02, P9.16, PJ29
	d, _, p, w, addr := watermarkSource(t, "wmsrc3", people)
	n1 := nonce()
	pages, err := pull(t, w, addr, "demo/consumer", &sourcepb.PullRequest{Stream: "demo.people", Nonce: n1})
	require.NoError(t, err)
	_, _, echo, _ := pulled(pages)
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		for range 2*PullPage + 100 {
			if _, err := emitPerson(ctx, tx, p); err != nil {
				return err
			}
		}
		return nil
	}))
	pages, err = pull(t, w, addr, "demo/consumer", &sourcepb.PullRequest{Stream: "demo.people", Nonce: nonce(),
		Partitions: []*sourcepb.PartitionPosition{{Partition: 0, Position: echo,
			Row: &sourcepb.RowIdentity{Row: &sourcepb.RowIdentity_EchoNonce{EchoNonce: n1}}}}})
	require.NoError(t, err)
	require.Len(t, pages, 3)
	for i, pg := range pages {
		r := pg.GetPartitions()[0]
		if i < 2 {
			assert.Len(t, r.GetEvents(), PullPage)
			assert.Nil(t, r.GetEcho(), "no echo before the last page")
		} else {
			assert.Len(t, r.GetEvents(), 101, "the rest and the echo")
			assert.NotNil(t, r.GetEcho())
		}
		assert.Equal(t, pages[0].GetSource().GetEpoch(), pg.GetSource().GetEpoch(), "every page declares the source")
	}
	want := int64(echo.GetSeq()) + 1 // every position after the one asked, in order, none twice, none missing
	for _, pg := range pages {
		for _, e := range pg.GetPartitions()[0].GetEvents() {
			n, _ := strconv.ParseInt(e.GetHeaders()[HeaderSeq], 10, 64)
			require.Equal(t, want, n)
			want++
		}
	}
}

func TestThePullChecksTheRowAtThePosition(t *testing.T) { // CK-WM-03, P9.15, SK17, PJ44, PJ48
	d, admin, p, w, addr := watermarkSource(t, "wmsrc4", people)
	emitPeople(t, d, p, 3)
	n1 := nonce()
	pages, err := pull(t, w, addr, "demo/consumer", &sourcepb.PullRequest{Stream: "demo.people", Nonce: n1})
	require.NoError(t, err)
	_, _, echo, _ := pulled(pages)
	require.Equal(t, uint64(4), echo.GetSeq())
	var rec, digest string
	var version int64
	require.NoError(t, admin.QueryRow(context.Background(), `SELECT record_id, version, digest FROM be_outbox WHERE seq = 2`).
		Scan(&rec, &version, &digest))
	at := func(seq uint64, row *sourcepb.RowIdentity) *sourcepb.PullRequest {
		return &sourcepb.PullRequest{Stream: "demo.people", Nonce: nonce(), Partitions: []*sourcepb.PartitionPosition{{
			Partition: 0, Position: &sourcepb.Position{Impl: echo.GetImpl(), Epoch: echo.GetEpoch(), Seq: seq}, Row: row}}}
	}
	record := func(id string, v int64, dg string) *sourcepb.RowIdentity {
		return &sourcepb.RowIdentity{Row: &sourcepb.RowIdentity_Record_{Record: &sourcepb.RowIdentity_Record{
			RecordId: id, Version: v, Digest: dg}}}
	}
	outcome := func(req *sourcepb.PullRequest) (sourcepb.NeedSnapshot, []string) {
		pages, err := pull(t, w, addr, "demo/consumer", req)
		require.NoError(t, err)
		recs, _, _, need := pulled(pages)
		return need, recs
	}
	need, recs := outcome(at(2, record(rec, version, digest)))
	assert.Equal(t, sourcepb.NeedSnapshot_NEED_SNAPSHOT_UNSPECIFIED, need, "the row at the position is the one it names")
	assert.Len(t, recs, 3, "after seq 2 up to the new echo: the third change, the first echo, the new echo")

	// restored and committed anew past the position: the row at seq 2 is another one now
	pgtest.Exec(t, admin, "UPDATE be_outbox SET version = version + 1 WHERE seq = 2")
	need, recs = outcome(at(2, record(rec, version, digest)))
	assert.Equal(t, sourcepb.NeedSnapshot_NEED_SNAPSHOT_ROLLBACK_DETECTED, need)
	assert.Empty(t, recs, "no rows and no echo for a partition that needs a full snapshot")
	need, _ = outcome(at(99, record(rec, version, digest)))
	assert.Equal(t, sourcepb.NeedSnapshot_NEED_SNAPSHOT_ROLLBACK_DETECTED, need, "a position past the source's counter")
	pgtest.Exec(t, admin, "DELETE FROM be_outbox WHERE seq = 1")
	need, _ = outcome(at(1, record("gone", 1, "x")))
	assert.Equal(t, sourcepb.NeedSnapshot_NEED_SNAPSHOT_BEYOND_RETENTION, need, "the row was purged by the retention")
	need, recs = outcome(&sourcepb.PullRequest{Stream: "demo.people", Nonce: nonce(), Partitions: []*sourcepb.PartitionPosition{{
		Partition: 0, Position: &sourcepb.Position{Impl: echo.GetImpl(), Epoch: "0.1", Seq: 2}, Row: record(rec, version, digest)}}})
	assert.Equal(t, sourcepb.NeedSnapshot_NEED_SNAPSHOT_UNSPECIFIED, need, "another epoch is not checked")
	assert.Len(t, recs, 1, "echo only")
}
