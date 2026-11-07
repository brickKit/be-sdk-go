//go:build integration

package tiering_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// P15.31, assembly project A159 (G5: an erasure reaches the exports): with an erasure, the class's finished export
// expires (its status answers NOT_FOUND, the cleanup deletes its object); the export being read when the erasure
// commits does not finish with what it read: its job reads again, and only then is it done.
func TestAnErasureVoidsTheExports(t *testing.T) {
	w := historyWorld(t, "tier_export_erased")
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cl := w.cat.Classes["sales.orders"]
	cl.Erasable = []string{"note"}
	w.cat.Classes["sales.orders"] = cl
	w.cat.Evidence = &evidence.Writer{DB: w.d, Store: &keeper{}, Component: "test/sales", Instance: "test-1", Log: quiet}
	runner := work.NewRunner(w.d, "test/sales", quiet)
	runner.Poll = 50 * time.Millisecond
	require.NoError(t, runner.Add(w.cat.ExportJobOf(quiet)))
	w.cat.Enqueue = runner.Enqueue
	w.cat.ColdSyncLimit, w.slow = 200*time.Millisecond, time.Second
	ctx, cancel := context.WithCancel(w.ctx)
	require.NoError(t, runner.Start(ctx))
	defer func() { cancel(); runner.Stop() }()
	done := func(id string) tiering.ExportStatus {
		t.Helper()
		var st tiering.ExportStatus
		require.Eventually(t, func() bool {
			var err error
			st, err = w.cat.Export(w.ctx, id, alice)
			return err == nil && st.State != "pending"
		}, 60*time.Second, 100*time.Millisecond)
		require.Equal(t, "done", st.State, st.Reason)
		return st
	}

	_, first, err := w.cat.History(w.ctx, w.sub, "sales.orders", json.RawMessage(`{"history": true}`))
	require.NoError(t, err)
	require.NotNil(t, first)
	done(first.Export)

	w.slow = 2 * time.Second // the second export's job is still reading when the erasure commits
	_, second, err := w.cat.History(w.ctx, w.sub, "sales.orders", json.RawMessage(`{"history": true}`))
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Eventually(t, func() bool {
		return count(t, w.env, "SELECT count(*) FROM be_work_item WHERE job = $1 AND state = 'running'", tiering.ExportJob) == 1
	}, 10*time.Second, 20*time.Millisecond, "the second export is being read")
	var root string
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT root_id FROM be_tier_member m JOIN be_tier_unit u ON u.id = m.unit "+
		"WHERE u.state = 'released' LIMIT 1").Scan(&root))
	out, err := w.cat.Erase(w.ctx, "sales.orders", root, "a data subject's request")
	require.NoError(t, err)
	require.Equal(t, tiering.Erased, out)

	_, err = w.cat.Export(w.ctx, first.Export, alice)
	assert.True(t, errs.Is(err, "be", errs.ReasonNotFound), "the finished export expired with the erasure: %v", err)
	w.slow = 0
	done(second.Export)
	assert.GreaterOrEqual(t, count(t, w.env, "SELECT attempts FROM be_work_item WHERE job = $1 AND args->>'export' = $2",
		tiering.ExportJob, second.Export), 2, "the read before the erasure did not finish it")
	assert.Equal(t, 0, count(t, w.env, "SELECT count(*) FROM be_tier_export WHERE erased_at IS NOT NULL"), "the mark cleared")
	n, err := w.cat.CleanExports(w.ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the voided export's object and row go")
}
