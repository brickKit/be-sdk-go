//go:build integration

package projection_test

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/projection"
	"github.com/stretchr/testify/require"
)

// source is an in-memory projected component: current records, tombstones, purged IDs.
type source struct {
	impl, epoch string
	live        map[string]projection.Record
	gone        map[string]int64
	purged      map[string]bool
	pageSize    int
	onSnapshot  func() // runs once, after the first page was cut and before it is returned
}

func newSource() *source {
	return &source{impl: "impl-2", epoch: "1.1", live: map[string]projection.Record{}, gone: map[string]int64{},
		purged: map[string]bool{}, pageSize: 2}
}

func (s *source) put(id string, v int64, data string) {
	s.live[id] = projection.Record{ID: id, Version: v, Data: json.RawMessage(data)}
}

func (s *source) Snapshot(_ context.Context, cursor string) ([]projection.Record, string, error) {
	var ids []string
	for id := range s.live {
		if id > cursor {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	next := ""
	if len(ids) > s.pageSize {
		ids, next = ids[:s.pageSize], ids[s.pageSize-1]
	}
	page := make([]projection.Record, 0, len(ids))
	for _, id := range ids {
		page = append(page, s.live[id])
	}
	if hook := s.onSnapshot; hook != nil {
		s.onSnapshot = nil
		hook()
	}
	return page, next, nil
}

func (s *source) Read(_ context.Context, ids []string) ([]projection.ReadResult, error) {
	out := make([]projection.ReadResult, 0, len(ids))
	for _, id := range ids {
		switch r, ok := s.live[id]; {
		case ok:
			out = append(out, projection.ReadResult{ID: id, State: projection.Exists, Version: r.Version, Data: r.Data})
		case s.gone[id] > 0:
			out = append(out, projection.ReadResult{ID: id, State: projection.Gone, Version: s.gone[id]})
		default:
			out = append(out, projection.ReadResult{ID: id, State: projection.Purged})
		}
	}
	return out, nil
}

func (s *source) Identity(context.Context) (string, string, error) { return s.impl, s.epoch, nil }

type row struct {
	State   string
	Version int64
	Data    string
}

func get(t *testing.T, d *db.DB, p projection.Projection, id string) row {
	t.Helper()
	var r row
	var found bool
	require.NoError(t, d.BackgroundTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		state, v, data, ok, err := p.Get(ctx, tx, id)
		r, found = row{State: state, Version: v}, ok
		if data != nil {
			r.Data = string(data)
		}
		return err
	}))
	require.True(t, found, id)
	return r
}

func apply(t *testing.T, d *db.DB, p projection.Projection, in projection.Input) bool {
	t.Helper()
	return applyOut(t, d, p, in).Fetch
}

func applyOut(t *testing.T, d *db.DB, p projection.Projection, in projection.Input) projection.Outcome {
	t.Helper()
	var out projection.Outcome
	require.NoError(t, d.BackgroundTx(context.Background(), func(ctx context.Context, tx db.Tx) (err error) {
		out, err = p.Apply(ctx, tx, in)
		return err
	}))
	return out
}

func status(t *testing.T, d *db.DB, name string) string {
	t.Helper()
	var s string
	require.NoError(t, d.BackgroundTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT status FROM be_projection_source WHERE projection = $1", name).Scan(&s)
	}))
	return s
}

func TestAlignMatchesSource(t *testing.T) {
	d, _ := dbtest.Schema(t, "proj_align", nil)
	p := projection.Projection{Name: "partners"}
	old := func(id string, kind string, v int64, data string) {
		apply(t, d, p, projection.Input{RecordID: id, Kind: kind, Version: v, Impl: "impl-1", Epoch: "1.1",
			Data: json.RawMessage(data)})
	}
	// local rows from the previous instance: stale, ahead of the restored source, deleted or purged since
	old("a", projection.Full, 1, `{"n":"a1"}`)
	old("b", projection.Full, 10, `{"n":"b10"}`)
	old("c", projection.Full, 4, `{"n":"c4"}`)
	old("d", projection.Full, 2, `{"n":"d2"}`)
	old("e", projection.Partial, 3, `{"n":"e3"}`)
	src := newSource()
	src.put("a", 3, `{"n":"a3"}`)
	src.put("b", 2, `{"n":"b2"}`)
	src.put("e", 7, `{"n":"e7"}`)
	src.put("f", 1, `{"n":"f1"}`)
	src.gone["c"] = 5
	src.purged["d"] = true

	require.NoError(t, p.Align(context.Background(), d, src))
	require.Equal(t, row{projection.Complete, 3, `{"n": "a3"}`}, get(t, d, p, "a"))
	require.Equal(t, row{projection.Complete, 2, `{"n": "b2"}`}, get(t, d, p, "b"), "the snapshot overwrites without comparing versions")
	require.Equal(t, row{projection.Tombstone, 5, ""}, get(t, d, p, "c"))
	require.Equal(t, row{projection.Tombstone, 0, ""}, get(t, d, p, "d"), "purged at the source: a version-0 tombstone")
	require.Equal(t, row{projection.Complete, 7, `{"n": "e7"}`}, get(t, d, p, "e"))
	require.Equal(t, row{projection.Complete, 1, `{"n": "f1"}`}, get(t, d, p, "f"))
	require.Equal(t, "aligned", status(t, d, "partners"))

	// after alignment the adopted instance's events follow the ordinary table
	in := projection.Input{RecordID: "a", Kind: projection.Full, Version: 3, Impl: "impl-2", Epoch: "1.1", Data: json.RawMessage(`{"n":"x"}`)}
	apply(t, d, p, in)
	require.Equal(t, row{projection.Complete, 3, `{"n": "a3"}`}, get(t, d, p, "a"), "the same version is ignored")

	// late inputs of the previous instance or of another epoch never touch the aligned rows
	late := func(impl, epoch, kind string) projection.Outcome {
		return applyOut(t, d, p, projection.Input{RecordID: "a", Kind: kind, Version: 99, Impl: impl, Epoch: epoch,
			Data: json.RawMessage(`{"n":"late"}`)})
	}
	require.Equal(t, projection.OtherInstance, late("impl-1", "1.1", projection.Full).Skipped)
	require.Equal(t, projection.OtherInstance, late("impl-1", "1.1", projection.Delete).Skipped)
	require.Equal(t, projection.OlderEpoch, late("impl-2", "1.0", projection.Full).Skipped)
	require.Equal(t, projection.NewerEpoch, late("impl-2", "2.1", projection.Full).Skipped)
	require.Equal(t, row{projection.Complete, 3, `{"n": "a3"}`}, get(t, d, p, "a"))
}

func TestOlderAlignmentDiscarded(t *testing.T) {
	d, _ := dbtest.Schema(t, "proj_replace", nil)
	p := projection.Projection{Name: "partners"}
	// c is projected locally from the previous instance, so the newer alignment reads it by ID
	apply(t, d, p, projection.Input{RecordID: "c", Kind: projection.Full, Version: 4, Impl: "impl-1", Epoch: "1.1",
		Data: json.RawMessage(`{"n":"c4"}`)})
	src := newSource()
	src.put("a", 1, `{"n":"a1"}`)
	src.put("c", 4, `{"n":"c4"}`)
	var inner error
	src.onSnapshot = func() {
		// the record is deleted after the first alignment cut its page; a newer alignment starts and finishes
		delete(src.live, "c")
		src.gone["c"] = 5
		inner = p.Align(context.Background(), d, src)
	}
	err := p.Align(context.Background(), d, src)
	require.NoError(t, inner)
	require.ErrorIs(t, err, projection.ErrReplaced)
	require.Equal(t, row{projection.Tombstone, 5, ""}, get(t, d, p, "c"), "the older alignment's page never lands")
	require.Equal(t, row{projection.Complete, 1, `{"n": "a1"}`}, get(t, d, p, "a"))
	require.Equal(t, "aligned", status(t, d, "partners"))
}

func TestWrittenDuringAlignment(t *testing.T) {
	d, _ := dbtest.Schema(t, "proj_during", nil)
	p := projection.Projection{Name: "partners"}
	src := newSource()
	src.put("a", 3, `{"n":"a3"}`)
	src.onSnapshot = func() {
		// events of the adopted instance arrive while the snapshot is in flight
		apply(t, d, p, projection.Input{RecordID: "a", Kind: projection.Delete, Version: 4, Impl: "impl-2", Epoch: "1.1"})
		apply(t, d, p, projection.Input{RecordID: "z", Kind: projection.Full, Version: 1, Impl: "impl-2", Epoch: "1.1",
			Data: json.RawMessage(`{"n":"z1"}`)})
	}
	require.NoError(t, p.Align(context.Background(), d, src))
	require.Equal(t, row{projection.Tombstone, 4, ""}, get(t, d, p, "a"), "an older snapshot row never overwrites a newer event")
	require.Equal(t, row{projection.Complete, 1, `{"n": "z1"}`}, get(t, d, p, "z"), "rows written during alignment are left to the stream")
}

func TestApplyAndRepair(t *testing.T) {
	d, _ := dbtest.Schema(t, "proj_repair", nil)
	p := projection.Projection{Name: "partners"}
	in := func(id, kind string, v int64, data string) projection.Input {
		return projection.Input{RecordID: id, Kind: kind, Version: v, Impl: "impl-2", Epoch: "1.1", Data: json.RawMessage(data)}
	}
	require.False(t, apply(t, d, p, in("a", projection.Full, 1, `{"x":1,"y":1}`)))
	require.True(t, apply(t, d, p, in("a", projection.Partial, 2, `{"y":2}`)), "a partial update waits for a fetch")
	require.Equal(t, row{projection.Incomplete, 2, `{"x": 1, "y": 2}`}, get(t, d, p, "a"), "partial fields merge")
	require.True(t, apply(t, d, p, in("b", projection.Partial, 6, `{"y":6}`)))
	require.True(t, apply(t, d, p, in("c", projection.Partial, 2, `{"y":2}`)))
	require.True(t, apply(t, d, p, in("e", projection.Partial, 4, `{"y":4}`)))

	src := newSource()
	src.put("a", 3, `{"x":3,"y":3}`)
	src.put("b", 6, `{"x":6,"y":6}`)
	src.gone["c"] = 3
	src.purged["e"] = true
	n, err := p.Repair(context.Background(), d, src)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, row{projection.Tombstone, 0, ""}, get(t, d, p, "e"), "purged at the source: a version-0 tombstone from incomplete")
	require.Equal(t, row{projection.Complete, 3, `{"x": 3, "y": 3}`}, get(t, d, p, "a"))
	require.Equal(t, row{projection.Complete, 6, `{"x": 6, "y": 6}`}, get(t, d, p, "b"))
	require.Equal(t, row{projection.Tombstone, 3, ""}, get(t, d, p, "c"))
	n, err = p.Repair(context.Background(), d, src)
	require.NoError(t, err)
	require.Zero(t, n)

	// a tombstone is never revived by the same version; an older full is ignored
	require.False(t, apply(t, d, p, in("c", projection.Full, 3, `{"x":0}`)))
	require.False(t, apply(t, d, p, in("a", projection.Full, 2, `{"x":0}`)))
	require.Equal(t, row{projection.Tombstone, 3, ""}, get(t, d, p, "c"))
	require.Equal(t, row{projection.Complete, 3, `{"x": 3, "y": 3}`}, get(t, d, p, "a"))
}

func TestInputsForANewRecordAreSerialized(t *testing.T) { // P9.11: a delete is not undone by a concurrent older full
	d, _ := dbtest.Schema(t, "proj_race", nil)
	p := projection.Projection{Name: "partners"}
	held := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- d.BackgroundTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			if _, err := p.Apply(ctx, tx, projection.Input{RecordID: "n", Kind: projection.Delete, Version: 2,
				Impl: "impl-2", Epoch: "1.1"}); err != nil {
				return err
			}
			close(held)
			time.Sleep(500 * time.Millisecond) // the older full arrives while the delete is uncommitted
			return nil
		})
	}()
	<-held
	out := applyOut(t, d, p, projection.Input{RecordID: "n", Kind: projection.Full, Version: 1, Impl: "impl-2", Epoch: "1.1",
		Data: json.RawMessage(`{"n":"n1"}`)})
	require.NoError(t, <-done)
	require.False(t, out.Changed)
	require.Equal(t, row{projection.Tombstone, 2, ""}, get(t, d, p, "n"))
}
