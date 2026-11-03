//go:build integration

package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/db"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing/fstest"
)

// tableRecords are record reads over a table (id, version, data, deleted); a row that is not there is purged.
func tableRecords(table string, readers ...string) *Records {
	return &Records{Operation: "demo/wmsrc.read", Readers: readers,
		Read: func(ctx context.Context, tx db.Tx, ids []string) ([]ReadResult, error) {
			var out []ReadResult
			for _, id := range ids {
				var v int64
				var data json.RawMessage
				var deleted bool
				err := tx.QueryRow(ctx, "SELECT version, data, deleted FROM "+table+" WHERE id = $1", id).Scan(&v, &data, &deleted)
				switch {
				case errors.Is(err, pgx.ErrNoRows):
					out = append(out, ReadResult{ID: id, State: Purged})
				case err != nil:
					return nil, err
				case deleted:
					out = append(out, ReadResult{ID: id, State: Tombstone, Version: v})
				default:
					out = append(out, ReadResult{ID: id, State: Exists, Version: v, Data: data})
				}
			}
			return out, nil
		},
		Page: func(ctx context.Context, tx db.Tx, after string, limit int) ([]Record, error) {
			rows, err := tx.Query(ctx, "SELECT id, version, data FROM "+table+
				` WHERE NOT deleted AND id > $1 ORDER BY id COLLATE "C" LIMIT $2`, after, limit)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			var out []Record
			for rows.Next() {
				var r Record
				var data json.RawMessage
				if err := rows.Scan(&r.ID, &r.Version, &data); err != nil {
					return nil, err
				}
				r.Data = data
				out = append(out, r)
			}
			return out, rows.Err()
		}}
}

// memClaims is object storage in memory.
type memClaims struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func (m *memClaims) Put(_ context.Context, id string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs["mem://"+id] = data
	return "mem://" + id, nil
}

// recordSource provisions a source schema with the table "rows" and serves its record reads on a real system
// surface (mTLS); conn calls it as caller.
type recordWorld struct {
	d      *db.DB
	admin  *pgx.Conn
	p      *Producer
	claims *memClaims
	w      *sectest.World
	addr   string
}

func recordSource(t *testing.T, name string, s ...Stream) *recordWorld {
	d, admin := dbtest.Schema(t, name, fstest.MapFS{"0001_rows.up.sql": {Data: []byte(
		"CREATE TABLE rows (id text PRIMARY KEY, version bigint NOT NULL, data jsonb NOT NULL, deleted boolean NOT NULL DEFAULT false);")}})
	rw := &recordWorld{d: d, admin: admin, claims: &memClaims{objs: map[string][]byte{}}}
	var err error
	rw.p, err = NewProducer("demo/wmsrc", s, rw.claims)
	require.NoError(t, err)
	src := &RecordSource{DB: d, Producer: rw.p, Impl: "impl-1", Deployment: "dep-1",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	m, ok, err := src.Method()
	require.NoError(t, err)
	require.True(t, ok)
	rw.w = sectest.New(t)
	var services grpcx.Services
	services.Register(&sourcepb.RecordService_ServiceDesc, src, map[string]grpcx.Method{"Read": m, "Snapshot": m})
	member := obs.Member{ID: "demo/wmsrc", Version: "3.0.0"}
	srv, err := grpcx.NewServer(grpcx.Deps{Member: member, Logger: obs.NewLogger(member, slog.LevelError, io.Discard),
		Registry: prometheus.NewRegistry(), Tracer: sdktrace.NewTracerProvider().Tracer("t"), TLS: rw.w.TLS(t, "demo/wmsrc")}, &services)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	rw.addr = ln.Addr().String()
	return rw
}

func (rw *recordWorld) conn(t *testing.T, caller string) *grpc.ClientConn {
	m, err := calls.NewMember(prometheus.NewRegistry())
	require.NoError(t, err)
	m.SetTLS(rw.w.TLS(t, caller))
	t.Cleanup(m.Close)
	conn, err := m.GRPC("demo/wmsrc", rw.addr)
	require.NoError(t, err)
	return conn
}

func call[Q, R any](t *testing.T, conn *grpc.ClientConn, method string, req *Q) (*R, error) {
	out := new(R)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return out, conn.Invoke(ctx, method, req, out, calls.Idempotent())
}

func readRows(t *testing.T, conn *grpc.ClientConn, stream string, ids ...string) (*sourcepb.ReadResponse, error) {
	return call[sourcepb.ReadRequest, sourcepb.ReadResponse](t, conn, sourcepb.RecordService_Read_FullMethodName,
		&sourcepb.ReadRequest{Stream: stream, RecordIds: ids})
}

func snapshotPage(t *testing.T, conn *grpc.ClientConn, req *sourcepb.SnapshotRequest) (*sourcepb.SnapshotResponse, error) {
	return call[sourcepb.SnapshotRequest, sourcepb.SnapshotResponse](t, conn, sourcepb.RecordService_Snapshot_FullMethodName, req)
}

var rowsStream = Stream{Name: "demo.rows", Partitions: 2, Records: tableRecords("rows", "demo/consumer")}

func TestOnlyTheDeclaredReadersMayRead(t *testing.T) { // CK-PJ-04, P9.30
	other := Stream{Name: "demo.other", Partitions: 1, Records: tableRecords("rows", "demo/other")}
	rw := recordSource(t, "rec1", rowsStream, other)
	_, err := readRows(t, rw.conn(t, "demo/other"), "demo.rows", "a")
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "a reader of another stream")
	_, err = snapshotPage(t, rw.conn(t, "demo/other"), &sourcepb.SnapshotRequest{Stream: "demo.rows"})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "the snapshot too")
	_, err = readRows(t, rw.conn(t, "demo/stranger"), "demo.rows", "a")
	assert.Error(t, err, "a component no stream lists is refused by the system surface already")
	_, err = readRows(t, rw.conn(t, "demo/consumer"), "demo.rows", "a")
	require.NoError(t, err, "the declared reader")
}

func TestAReadAnswersEveryIDInOrder(t *testing.T) { // CK-PJ-04, P9.31, P9.33
	rw := recordSource(t, "rec2", rowsStream)
	pgtest.Exec(t, rw.admin, `INSERT INTO rows VALUES ('live', 3, '{"n":1}', false), ('gone', 4, '{}', true)`)
	for range 3 { // numbered events on the partition of "live": the high point is the last of them
		require.NoError(t, rw.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			_, err := rw.p.Emit(ctx, tx, "demo.rows", Event{Type: "demo.row.changed.v1", RecordID: "live", Version: 3,
				Projection: Full, Data: map[string]int{"n": 1}})
			return err
		}))
	}
	part := rowsStream.PartitionOf("live")
	_, err := Number(context.Background(), rw.d, "demo.rows", part, DefaultBatch)
	require.NoError(t, err)
	conn := rw.conn(t, "demo/consumer")
	resp, err := readRows(t, conn, "demo.rows", "never", "gone", "live")
	require.NoError(t, err)
	got := []string{}
	for _, r := range resp.GetResults() {
		got = append(got, fmt.Sprintf("%s %v %d %s", r.GetRecordId(), r.GetState(), r.GetVersion(), r.GetPayload()))
	}
	assert.Equal(t, []string{"never READ_STATE_PURGED 0 ", "gone READ_STATE_TOMBSTONE 4 ", `live READ_STATE_EXISTS 3 {"n":1}`}, got, "encoded as the producer encodes")
	highs := map[uint32]uint64{}
	for _, h := range resp.GetHighs() {
		highs[h.GetPartition()] = h.GetSeq()
	}
	assert.Equal(t, map[uint32]uint64{uint32(part): 3, uint32(1 - part): 0}, highs, "every partition, in the same snapshot")
	var system string
	require.NoError(t, rw.admin.QueryRow(context.Background(), "SELECT system_identifier::text FROM pg_control_system()").Scan(&system))
	d := resp.GetSource()
	assert.Equal(t, []any{"dep-1", "impl-1", system, "1.1", true},
		[]any{d.GetDeployment(), d.GetImpl(), d.GetSystemId(), d.GetEpoch(), d.GetPrimary()}, "the declaration (P9.18)")

	_, err = readRows(t, conn, "demo.rows")
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "no ID")
	many := make([]string, RecordPage+1)
	for i := range many {
		many[i] = fmt.Sprint(i)
	}
	_, err = readRows(t, conn, "demo.rows", many...)
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "more than one read takes")
}

func TestASnapshotPagesByRecordID(t *testing.T) { // CK-PJ-04, P9.32
	rw := recordSource(t, "rec3", rowsStream)
	pgtest.Exec(t, rw.admin, `INSERT INTO rows SELECT 'r' || lpad(g::text, 5, '0'), g, jsonb_build_object('g', g), g % 10 = 0
		FROM generate_series(1, 1300) g`)
	conn := rw.conn(t, "demo/consumer")
	var ids []string
	var sizes []int
	req := &sourcepb.SnapshotRequest{Stream: "demo.rows"}
	for {
		resp, err := snapshotPage(t, conn, req)
		require.NoError(t, err)
		require.Len(t, resp.GetHighs(), 2, "every page carries the high points")
		sizes = append(sizes, len(resp.GetRecords()))
		for _, r := range resp.GetRecords() {
			ids = append(ids, r.GetRecordId())
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		req = &sourcepb.SnapshotRequest{PageToken: resp.GetNextPageToken()}
	}
	assert.Equal(t, []int{500, 500, 170}, sizes, "1 170 records that exist (every tenth is deleted)")
	assert.IsIncreasing(t, ids)
	assert.NotContains(t, ids, "r00010")
}

func TestASnapshotPageTokenIsBoundToTheEpoch(t *testing.T) { // CK-PJ-04, P9.32
	rw := recordSource(t, "rec4", rowsStream)
	pgtest.Exec(t, rw.admin, `INSERT INTO rows SELECT 'r' || lpad(g::text, 5, '0'), g, '{}', false FROM generate_series(1, 600) g`)
	conn := rw.conn(t, "demo/consumer")
	first, err := snapshotPage(t, conn, &sourcepb.SnapshotRequest{Stream: "demo.rows"})
	require.NoError(t, err)
	require.NotEmpty(t, first.GetNextPageToken(), "the precondition: a second page")
	pgtest.Exec(t, rw.admin, "UPDATE be_recovery_generation SET generation = generation + 1")
	_, err = snapshotPage(t, conn, &sourcepb.SnapshotRequest{PageToken: first.GetNextPageToken()})
	assert.Equal(t, codes.Aborted, status.Code(err), "QUERY_PLAN_CHANGED: align again")
	again, err := snapshotPage(t, conn, &sourcepb.SnapshotRequest{Stream: "demo.rows"})
	require.NoError(t, err)
	assert.Equal(t, "2.1", again.GetSource().GetEpoch(), "a new first page declares the new epoch")
}

func TestABigRecordIsAClaimCheck(t *testing.T) { // P9.30, P6.14
	rw := recordSource(t, "rec5", rowsStream)
	big := `{"text": "` + strings.Repeat("x", MaxPayload) + `"}`
	_, err := rw.admin.Exec(context.Background(), "INSERT INTO rows VALUES ('big', 1, $1, false), ('small', 1, '{}', false)", big)
	require.NoError(t, err)
	conn := rw.conn(t, "demo/consumer")
	resp, err := readRows(t, conn, "demo.rows", "big", "small")
	require.NoError(t, err)
	r := resp.GetResults()[0]
	require.True(t, r.GetClaim(), "over 64 KiB: a claim check")
	assert.False(t, resp.GetResults()[1].GetClaim())
	var ref struct{ Claim ClaimRef }
	require.NoError(t, json.Unmarshal(r.GetPayload(), &ref))
	stored := rw.claims.objs[ref.Claim.URI]
	assert.Equal(t, ref.Claim.Size, len(stored))
	assert.Equal(t, sha256Hex(stored), ref.Claim.SHA256)
	assert.JSONEq(t, big, string(stored), "the object is the record's content")
	snap, err := snapshotPage(t, conn, &sourcepb.SnapshotRequest{Stream: "demo.rows"})
	require.NoError(t, err)
	assert.True(t, snap.GetRecords()[0].GetClaim(), "the snapshot too")
}
