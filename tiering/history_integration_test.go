//go:build integration

package tiering_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	alice = "0190f3a1-0000-7000-8000-0000000000a1"
	bob   = "0190f3a1-0000-7000-8000-0000000000b2"
)

// grants is the authorization member's answer: alice's segments, changed by the test (a revocation).
type grants struct{ entities []string }

func (g *grants) Verify(_ context.Context, req authz.VerifyRequest) (authz.VerifyAnswer, error) {
	seg := authz.Segment{ResourceType: "sales.order", Operation: "sales.order.read",
		Atoms:      map[string]authz.Atom{"legal_entity": {Kind: "in", Values: g.entities}},
		ReadFields: []string{"id", "code", "lines"}, Valid: authz.Window{From: "2026-01-01T00:00:00Z"}}
	s := authz.Shard{Shard: "s", Applicability: []string{"sales.order"}, Digest: "d",
		Subjects: map[string]authz.SubjectEntry{alice: {Segments: []authz.Segment{seg}}}}
	return authz.VerifyAnswer{Nonce: req.Nonce, RecoveryGeneration: authz.Revision{Value: "1"},
		Shards: []authz.ShardAnswer{{Shard: "s", Status: "changed", View: &s}}}, nil
}

var orderDefs = &authz.Definitions{Component: "test/sales", Resources: []authz.Resource{{Type: "sales.order",
	ViewOperation: "sales.order.read",
	Dimensions:    map[string]authz.Dimension{"legal_entity": {Kind: authz.ValueSet, Column: "legal_entity"}},
	Fields: map[string]authz.Field{"id": {Class: authz.Ordinary}, "code": {Class: authz.Ordinary},
		"total": {Class: authz.Ordinary}, "lines": {Class: authz.Ordinary}}}},
	Operations: []authz.Operation{{Key: "sales.order.read", ResourceType: "sales.order", Purpose: "read", RiskClass: "low"}}}

func facts(row map[string]any) authz.Facts {
	return authz.Facts{"legal_entity": authz.Value(fmt.Sprint(row["legal_entity"]))}
}

func shape(id, code, total string, lines int, d authz.Decision) map[string]any {
	res, _ := orderDefs.Resource("sales.order")
	return authz.Mask(res, map[string]any{"id": id, "code": code, "total": total, "lines": lines}, d.Fields)
}

// orderReader is the component's reader: online orders under the snapshot, cold ones as the SDK hands them.
// slow makes the online part take longer than a short limit.
func orderReader(slow *time.Duration) tiering.Reader {
	cl := class()
	return tiering.Reader{Op: "sales.order.read", Facts: facts,
		Output: func(a tiering.Aggregate, d authz.Decision) (map[string]any, error) {
			if *slow < 0 { // a defect in the component's shaping
				return nil, errors.New("cannot shape the order")
			}
			return shape(fmt.Sprint(a.Root["id"]), fmt.Sprint(a.Root["code"]), fmt.Sprint(a.Root["total"]),
				len(a.Children["order_lines"]), d), nil
		},
		Online: func(ctx context.Context, tx db.Tx, s tiering.Snapshot, sub *authz.Subject, _ json.RawMessage,
			emit func(map[string]any) error) error {
			if *slow > 0 {
				select {
				case <-time.After(*slow):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			ctx, decide, err := sub.Decider(ctx, "sales.order.read")
			if err != nil {
				return err
			}
			rows, err := tx.Query(ctx, "SELECT r.id::text, r.code, r.total::text, r.legal_entity::text, "+
				"(SELECT count(*) FROM order_lines l WHERE l.order_id = r.id) FROM orders r WHERE "+s.OnlineExclusion(cl, "r"))
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id, code, total, le string
				var lines int
				if err := rows.Scan(&id, &code, &total, &le, &lines); err != nil {
					return err
				}
				d := decide(facts(map[string]any{"legal_entity": le}))
				if d.Visible != authz.True {
					continue
				}
				if err := emit(shape(id, code, canonDecimal(total), lines, d)); err != nil {
					return err
				}
			}
			return rows.Err()
		}}
}

func canonDecimal(s string) string {
	c, err := canon.Decimal(s)
	if err != nil {
		panic(err)
	}
	return c
}

type world struct {
	env
	g     *grants
	sub   *authz.Subject
	slow  time.Duration
	le    [3]string // granted, granted, never granted
	store *tiering.Store
}

// historyWorld: three legal entities, each with an open order online and a released unit of two closed orders in
// the object store; alice may read the first two entities' orders, their id, code and lines (the total masked).
func historyWorld(t *testing.T, name string) *world {
	t.Helper()
	w := &world{env: setup(t, name), g: &grants{}}
	w.store, _ = storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	w.cat.Store = w.store
	for i := range w.le {
		w.env.le = ids.New().String()
		w.le[i] = w.env.le
		w.order(t, -1)
		published(t, w.env, tiering.RouteObject)
	}
	w.g.entities = []string{w.le[0], w.le[1]}
	h := authz.NewHolder("test/sales", w.g, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, h.Verify(w.ctx))
	require.NoError(t, orderDefs.Validate())
	w.sub = &authz.Subject{Principal: alice, Defs: orderDefs, Holder: h, At: time.Now()}
	w.cat.Subject = func(p string) *authz.Subject {
		return &authz.Subject{Principal: p, Defs: orderDefs, Holder: h, At: time.Now()}
	}
	require.NoError(t, w.cat.Register("sales.orders", orderReader(&w.slow)))
	return w
}

func (w *world) revoke(t *testing.T, keep ...string) {
	t.Helper()
	w.g.entities = keep
	require.NoError(t, w.sub.Holder.Verify(w.ctx))
}

func idsOf(objs []map[string]any) []string {
	var out []string
	for _, o := range objs {
		out = append(out, fmt.Sprint(o["id"]))
	}
	sort.Strings(out)
	return out
}

// TR12, G4, P15.26: a history read is the online and the cold orders once each, under the caller's authorization now:
// the third entity's never, the total masked; after a revocation the revoked entity's orders are gone from the next
// read.
func TestAHistoryReadIsCompleteUnderTheCurrentAuthorization(t *testing.T) {
	w := historyWorld(t, "tier_history")
	out, pending, err := w.cat.History(w.ctx, w.sub, "sales.orders", nil)
	require.NoError(t, err)
	require.Nil(t, pending)
	require.Len(t, out, 6, "two entities: an open order and two archived ones each")
	assert.Len(t, idsOf(out), 6)
	for _, o := range out {
		assert.Nil(t, o["total"])
		assert.Equal(t, []string{"total"}, o[authz.Masked])
		assert.Equal(t, 2, o["lines"], "the lines read with the order")
	}
	var leases int
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT count(*) FROM be_tier_lease").Scan(&leases))
	assert.Zero(t, leases, "the read's lease ends with it")

	w.revoke(t, w.le[0])
	out, _, err = w.cat.History(w.ctx, w.sub, "sales.orders", nil)
	require.NoError(t, err)
	assert.Len(t, out, 3, "the revoked entity's orders are not read any more")
}

// TR11, TR12, P15.38: a read past the limit is not answered in part: it becomes an export, written by the queue job
// under alice's authorization when it runs (a revocation in between is applied), only alice sees it, its presigned
// address reads exactly what a complete read answers, and it goes when it expires.
func TestAHistoryPastTheLimitBecomesAnExport(t *testing.T) {
	w := historyWorld(t, "tier_export")
	runner := work.NewRunner(w.d, "test/sales", slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, runner.Add(w.cat.ExportJobOf(slog.New(slog.NewTextHandler(io.Discard, nil)))))
	w.cat.Enqueue = runner.Enqueue
	w.cat.ColdSyncLimit, w.slow = 200*time.Millisecond, time.Second
	out, pending, err := w.cat.History(w.ctx, w.sub, "sales.orders", json.RawMessage(`{"history": true}`))
	require.NoError(t, err)
	require.NotNil(t, pending, "past the limit")
	assert.Empty(t, out, "never a part")
	st, err := w.cat.Export(w.ctx, pending.Export, alice)
	require.NoError(t, err)
	assert.Equal(t, "pending", st.State)
	_, err = w.cat.Export(w.ctx, pending.Export, bob)
	assert.True(t, errs.Is(err, "be", errs.ReasonNotFound), "another principal: %v", err)

	w.revoke(t, w.le[1]) // before the job runs: the export applies the authorization then
	w.slow = 0
	ctx, cancel := context.WithCancel(w.ctx)
	require.NoError(t, runner.Start(ctx))
	defer func() { cancel(); runner.Stop() }()
	require.Eventually(t, func() bool {
		st, err = w.cat.Export(w.ctx, pending.Export, alice)
		return err == nil && st.State != "pending"
	}, 30*time.Second, 100*time.Millisecond)
	require.Equal(t, "done", st.State, st.Reason)
	assert.EqualValues(t, 3, st.Rows)
	require.NotEmpty(t, st.URL)

	resp, err := http.Get(st.URL)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	sync, _, err := w.cat.History(w.ctx, w.sub, "sales.orders", nil)
	require.NoError(t, err)
	var want []string
	for _, o := range sync {
		line, err := json.Marshal(o)
		require.NoError(t, err)
		want = append(want, string(line))
	}
	got := []string{}
	for _, l := range bytes.Split(bytes.TrimSuffix(body, []byte("\n")), []byte("\n")) {
		got = append(got, string(l))
	}
	sort.Strings(want)
	sort.Strings(got)
	assert.Equal(t, want, got, "the export is what a complete read answers")

	_, err = w.admin.Exec(w.ctx, "UPDATE be_tier_export SET expires_at = now() - interval '1 second'")
	require.NoError(t, err)
	n, err := w.cat.CleanExports(w.ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	_, err = w.store.Get(w.ctx, w.cat.ExportKey(pending.Export))
	assert.ErrorIs(t, err, tiering.ErrMissing, "the object is gone")
	_, err = w.cat.Export(w.ctx, pending.Export, alice)
	assert.True(t, errs.Is(err, "be", errs.ReasonNotFound), "%v", err)
}

// TR11, P15.38: past the limit with nowhere to export (no store, an object-locked store, no running exports) or
// without a reader, the answer is QUERY_NOT_COMPLETABLE, never a part.
func TestAHistoryThatCannotBeExportedIsNotCompletable(t *testing.T) {
	w := historyWorld(t, "tier_noexport")
	w.cat.Enqueue = func(context.Context, db.Tx, string, any) (string, error) { return "", nil }
	w.cat.ColdSyncLimit, w.slow = 100*time.Millisecond, time.Second
	for name, change := range map[string]func(c *tiering.Catalog){
		"no store":    func(c *tiering.Catalog) { c.Store = nil },
		"locked":      func(c *tiering.Catalog) { c.StoreLocked = true },
		"no exports":  func(c *tiering.Catalog) { c.Enqueue = nil },
		"no reader":   func(c *tiering.Catalog) { c.Classes["other"] = class() },
		"the default": func(*tiering.Catalog) {},
	} {
		t.Run(name, func(t *testing.T) {
			c := &tiering.Catalog{DB: w.cat.DB, Classes: map[string]tiering.Class{"sales.orders": class()},
				Store: w.store, Enqueue: w.cat.Enqueue, Subject: w.cat.Subject, ColdSyncLimit: w.cat.ColdSyncLimit}
			require.NoError(t, c.Register("sales.orders", orderReader(&w.slow)))
			change(c)
			read := "sales.orders"
			if name == "no reader" {
				read = "other"
			}
			out, pending, err := c.History(w.ctx, w.sub, read, nil)
			assert.Empty(t, out)
			if name == "the default" {
				require.NoError(t, err)
				assert.NotNil(t, pending, "the same read with a store becomes an export")
				return
			}
			assert.Nil(t, pending)
			assert.True(t, errs.Is(err, "be", errs.ReasonQueryNotCompletable), "%v", err)
		})
	}
}

// P15.38: an export whose every attempt fails is recorded failed (INTERNAL) on the last one: its requester does not
// see it pending for ever.
func TestAnExportThatKeepsFailingIsRecordedFailed(t *testing.T) {
	w := historyWorld(t, "tier_export_fail")
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := work.NewRunner(w.d, "test/sales", quiet)
	job := w.cat.ExportJobOf(quiet)
	job.MaxAttempts = 2
	require.NoError(t, runner.Add(job))
	w.cat.Enqueue = runner.Enqueue
	id, err := w.cat.StartExport(w.ctx, alice, "sales.orders", nil)
	require.NoError(t, err)
	w.slow = -1
	ctx, cancel := context.WithCancel(w.ctx)
	require.NoError(t, runner.Start(ctx))
	defer func() { cancel(); runner.Stop() }()
	var st tiering.ExportStatus
	require.Eventually(t, func() bool {
		st, err = w.cat.Export(w.ctx, id, alice)
		return err == nil && st.State != "pending"
	}, 60*time.Second, 200*time.Millisecond)
	assert.Equal(t, "failed", st.State)
	assert.Equal(t, errs.ReasonInternal, st.Reason)
	assert.Empty(t, st.URL)
}
