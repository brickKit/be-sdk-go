//go:build integration

package audit

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const alice = "0190f3a1-0000-7000-8000-0000000000a1"

func count(t *testing.T, admin *pgx.Conn, where string) int {
	var n int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM be_audit_log WHERE "+where).Scan(&n))
	return n
}

func change(op string) Record {
	return Record{Category: BusinessChange, Operation: op, Outcome: Succeeded, Actor: alice,
		Target: map[string]any{"type": "demo.order", "id": "o1"}, Changes: map[string]any{"status": "confirmed"}}
}

func TestChangesAreAuditedWithTheirTransaction(t *testing.T) { // P8.19, CK-AU-01
	d, admin := dbtest.Schema(t, "audit1", nil)
	a := New(d)
	ctx := context.Background()
	err := d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := a.Write(ctx, tx, change("demo.order.confirm"))
		require.NoError(t, err)
		return errors.New("business rollback")
	})
	require.Error(t, err)
	assert.Zero(t, count(t, admin, "true"), "no change, no record")
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := a.Write(ctx, tx, change("demo.order.confirm"))
		return err
	}))
	assert.Equal(t, 1, count(t, admin, "digest LIKE 'sha256:%' AND transfer_state = 'pending'"))

	require.NoError(t, a.Read(ctx, Record{Operation: "demo.order.export", Outcome: Succeeded, Actor: alice}))
	assert.Equal(t, 1, count(t, admin, "category = 'sensitive_read'"))

	a.Capacity = 2
	_, err = a.Backlog(ctx)
	require.NoError(t, err)
	err = d.Tx(ctx, func(ctx context.Context, tx db.Tx) error { _, err := a.Write(ctx, tx, change("x.y")); return err })
	assert.True(t, errs.Is(err, "be", errs.ReasonAuditCapacityExhausted), "%v", err)
	assert.True(t, errs.Is(a.Read(ctx, Record{Operation: "x.y", Outcome: Succeeded}), "be", errs.ReasonAuditCapacityExhausted))
}

func TestDenialsPastTheBudgetAreCountedNotLost(t *testing.T) { // P8.23, CK-AU-05
	d, admin := dbtest.Schema(t, "audit2", nil)
	a := New(d)
	ctx := context.Background()
	for i := 0; i < DenialBudget+5; i++ {
		require.NoError(t, a.Denial(ctx, Record{Operation: "demo.order.read", Reason: "PERMISSION_DENIED", Actor: alice}))
	}
	assert.Equal(t, DenialBudget, count(t, admin, "category = 'security_denial'"))
	n, err := a.Flush(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "the window is still open")
	pgtest.Exec(t, admin, "UPDATE be_audit_denial_budget SET minute = minute - interval '1 minute'")
	n, err = a.Flush(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, count(t, admin, "operation = 'be.audit.denials_over_budget' AND changes->>'count' = '5'"))
	n, err = a.Flush(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "a window is summarized once")
}

type fakeAudit struct {
	mu     sync.Mutex
	stored map[string]string // id -> digest
	seq    int64
	down   bool
	tamper string
}

func (f *fakeAudit) Send(_ context.Context, ws []Wire) ([]Receipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return nil, errors.New("audit component unavailable")
	}
	var out []Receipt
	for _, w := range ws {
		if prev, ok := f.stored[w.ID]; ok && prev != w.Digest || w.ID == f.tamper {
			out = append(out, Receipt{ID: w.ID, Status: "conflict"})
			continue
		}
		if _, ok := f.stored[w.ID]; !ok {
			f.seq++
			f.stored[w.ID] = w.Digest
		}
		out = append(out, Receipt{ID: w.ID, Status: "received", Epoch: "1.1", Seq: f.seq, Chain: "sha256:chain"})
	}
	return out, nil
}

func TestNumberingTransferAndDeletion(t *testing.T) { // P8.20-P8.22, CK-AU-02..04
	d, admin := dbtest.Schema(t, "audit3", nil)
	a := New(d)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := a.Write(ctx, tx, change("demo.order.confirm"))
			return err
		}))
	}
	sender := &fakeAudit{stored: map[string]string{}, down: true}
	n, err := a.Transfer(ctx, sender)
	require.NoError(t, err)
	assert.Zero(t, n, "only numbered rows travel")
	_, err = a.Number(ctx)
	require.NoError(t, err)
	var seqs []int64
	rows, _ := admin.Query(ctx, "SELECT seq FROM be_audit_log ORDER BY seq")
	seqs, _ = pgx.CollectRows(rows, pgx.RowTo[int64])
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, seqs)

	_, err = a.Transfer(ctx, sender)
	assert.Error(t, err)
	assert.Equal(t, 5, count(t, admin, "transfer_state = 'pending'"), "the audit component down: nothing lost")
	var first string
	require.NoError(t, admin.QueryRow(ctx, "SELECT id::text FROM be_audit_log ORDER BY seq LIMIT 1").Scan(&first))
	sender.down, sender.tamper = false, first
	n, err = a.Transfer(ctx, sender)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, 4, count(t, admin, "transfer_state = 'received'"))
	assert.Equal(t, 1, count(t, admin, "transfer_state = 'conflict'"), "a conflict stops only that record")
	n, err = a.Transfer(ctx, sender)
	require.NoError(t, err)
	assert.Zero(t, n, "a conflict is not sent again automatically")

	// deletion: only receipts the backup covers, only after the minimum retention
	pgtest.Exec(t, admin, "UPDATE be_audit_log SET received_at = now() - interval '8 days' WHERE transfer_state = 'received'")
	// the four receipts carry storage sequences 1-4 (the conflicting record got none)
	del, resent, err := a.Purge(ctx, Watermark{Epoch: "1.1", Seq: 2})
	require.NoError(t, err)
	assert.Equal(t, int64(2), del, "receipts 1 and 2 are in the backup")
	assert.Zero(t, resent)
	assert.Equal(t, 2, count(t, admin, "transfer_state = 'received'"), "receipts 3 and 4 wait for the next backup")

	// the audit component was restored from a backup ending at 3: epoch 2.1, the fork of 1.1 at 3
	del, resent, err = a.Purge(ctx, Watermark{Epoch: "2.1", Seq: 0, Ancestors: map[string]int64{"1.1": 3}, BackedUpInEpoch: true})
	require.NoError(t, err)
	assert.Equal(t, int64(1), del, "receipt 3 is contained in the new epoch, which has a backup")
	assert.Equal(t, int64(1), resent, "receipt 4 was lost with the restore: sent again")
	assert.Equal(t, 1, count(t, admin, "transfer_state = 'pending'"))
	assert.Equal(t, 1, count(t, admin, "transfer_state = 'conflict'"), "never deleted")

	// a receipt from an epoch on a branch the audit component no longer has: sent again
	pgtest.Exec(t, admin, "UPDATE be_audit_log SET transfer_state = 'received', receipt_epoch = '1.5', receipt_seq = 9, received_at = now() - interval '8 days' WHERE transfer_state = 'pending'")
	_, resent, err = a.Purge(ctx, Watermark{Epoch: "2.1", Seq: 0, Ancestors: map[string]int64{"1.1": 3}, BackedUpInEpoch: true})
	require.NoError(t, err)
	assert.Equal(t, int64(1), resent)
}
