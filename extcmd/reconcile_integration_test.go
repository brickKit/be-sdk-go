//go:build integration

package extcmd

import (
	"context"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/externalpb"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listing is an external system that also lists its records (a Lister): every reference it holds, in the window.
type listing struct {
	*fakeExternal
	lists int
}

func (l *listing) List(_ context.Context, _, _ time.Time) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lists++
	var out []Record
	for ref, s := range l.held {
		out = append(out, Record{Reference: ref, State: s, ExternalID: "ext-" + ref})
	}
	return out, nil
}

func (w *world) put(ref, op, set string) string {
	w.t.Helper()
	var id string
	require.NoError(w.t, w.admin.QueryRow(context.Background(), `INSERT INTO be_external_command (id, requester, reference,
		operation, params, params_digest, irrevocable, state) VALUES (gen_random_uuid(), 'demo/sales', $1, $2, '\x7b7d', 'd',
		false, 'prepared') RETURNING id::text`, ref, op).Scan(&id))
	if set != "" {
		pgtest.Exec(w.t, w.admin, "UPDATE be_external_command SET "+set+" WHERE id = '"+id+"'")
	}
	return id
}

func conclusions(resp *externalpb.ReconcileResponse) map[string]string {
	out := map[string]string{}
	for _, c := range resp.GetConclusions() {
		out[c.GetReference()] = c.GetConclusion() + "/" + c.GetState()
	}
	return out
}

// P13.11-P13.13 by queries: each open command and issued reference asked by its reference; the conclusions applied
// and kept; the same request again answers the same; the external system down fails the call and writes nothing.
func TestReconcileByQueries(t *testing.T) {
	w := newWorld(t, "extcmd_reconcile", map[string]Capabilities{"pay": plain, "pay2": dedupe})
	T := time.Now().Add(-2 * time.Hour).UTC()
	failed := time.Now().Add(-time.Hour).UTC()
	found := w.put("r-found", "pay", "state = 'unknown', first_reached_at = now() - interval '3 hours'")
	w.ext.held["r-found"] = Succeeded
	dd := w.put("r-dedupe", "pay2", "")
	plainID := w.put("r-plain", "pay", "")
	w.put("r-manual", "pay", "state = 'manual'")
	expired := w.put("r-expired", "pay", "state = 'unknown', first_reached_at = now() - interval '3 hours', expires_at = now() - interval '1 minute'")
	w.put("r-done", "pay", "state = 'succeeded'")
	w.ext.held["r-done"] = Succeeded
	w.ext.held["r-issued-held"] = Succeeded
	s := &ReconcileServer{Commands: w.c}
	req := &externalpb.ReconcileRequest{Recovery: "rec-1", T: T.Format(time.RFC3339Nano), WindowBeforeSeconds: 600,
		Issued: []string{"r-issued-lost", "r-issued-held", "r-dedupe"}, FailedAt: failed.Format(time.RFC3339Nano)}

	resp, err := s.Reconcile(context.Background(), req)
	require.NoError(t, err)
	want := map[string]string{
		"r-found":       "matched/succeeded",
		"r-dedupe":      "deadline/prepared",
		"r-plain":       "manual/manual",
		"r-manual":      "manual/manual",
		"r-expired":     "unknown/unknown",
		"r-issued-held": "external_only/",
		"r-issued-lost": "possibly_executed/",
	}
	assert.Equal(t, want, conclusions(resp))
	assert.Equal(t, Succeeded, w.state(found))
	assert.Equal(t, Manual, w.state(plainID))
	assert.Equal(t, Unknown, w.state(expired))
	var first, until, deadline time.Time
	require.NoError(t, w.admin.QueryRow(context.Background(), `SELECT first_reached_at, unrecorded_until, resend_deadline
		FROM be_external_command WHERE id = $1`, dd).Scan(&first, &until, &deadline))
	assert.WithinDuration(t, T, first, time.Millisecond, "prepared: the earliest send is T")
	assert.WithinDuration(t, failed, until, time.Millisecond)
	assert.WithinDuration(t, T.Add(24*time.Hour-150*time.Minute+6*time.Minute), deadline, time.Millisecond,
		"T + window - max(10%, 1h)")
	assert.NotContains(t, w.ext.queries, "r-done", "a final command is not asked about")
	assert.Empty(t, w.ext.sent(), "the reconciliation never sends")

	again, err := s.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, want, conclusions(again), "P13.13: the same request answers the same")

	w.ext.queryDown = true
	_, err = s.Reconcile(context.Background(), &externalpb.ReconcileRequest{Recovery: "rec-2", T: req.T})
	assert.True(t, errs.Is(err, "be", errs.ReasonDependencyUnavailable), "%v", err)
	var kept int
	require.NoError(t, w.admin.QueryRow(context.Background(), "SELECT count(*) FROM be_external_reconciled WHERE recovery = 'rec-2'").Scan(&kept))
	assert.Zero(t, kept, "nothing written")

	_, err = s.Reconcile(context.Background(), &externalpb.ReconcileRequest{Recovery: "rec-3", T: "yesterday"})
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "%v", err)
}

// A listing adapter: one listing covers the window; only an open command that may have been sent before the window
// is queried by its reference.
func TestReconcileByListing(t *testing.T) {
	w := newWorld(t, "extcmd_reconcile_list", map[string]Capabilities{"pay": plain})
	l := &listing{fakeExternal: w.ext}
	w.c.Adapter = l
	T := time.Now().Add(-time.Hour).UTC()
	w.put("r-in-window", "pay", "state = 'unknown', first_reached_at = now() - interval '61 minutes'")
	w.put("r-old", "pay", "state = 'unknown', first_reached_at = now() - interval '3 days'")
	w.ext.held["r-in-window"] = Failed
	w.ext.held["r-old"] = Succeeded
	w.ext.held["r-elsewhere"] = Succeeded
	w.put("r-done", "pay", "state = 'succeeded'") // final here, listed there: neither open nor external_only
	w.ext.held["r-done"] = Succeeded
	resp, err := (&ReconcileServer{Commands: w.c}).Reconcile(context.Background(), &externalpb.ReconcileRequest{
		Recovery: "rec-1", T: T.Format(time.RFC3339Nano), WindowBeforeSeconds: 600})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"r-in-window": "matched/failed", "r-old": "matched/succeeded",
		"r-elsewhere": "external_only/"}, conclusions(resp))
	assert.Equal(t, 1, l.lists)
	assert.Equal(t, []string{"r-old"}, w.ext.queries, "only what the window does not cover")
}
