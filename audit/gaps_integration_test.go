//go:build integration

package audit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The audit expectations of protocol 03 that had no named test (assembly item 4, T4e).

// AU07: the sensitive read's record cannot be written, so the read is refused (P8.19).
func TestASensitiveReadIsRefusedWhenItsRecordCannotBeWritten(t *testing.T) {
	d, admin := dbtest.Schema(t, "audit_au07", nil)
	a := New(d)
	ctx := context.Background()
	pgtest.Exec(t, admin, "ALTER TABLE be_audit_log ADD CONSTRAINT au07_refuse CHECK (false) NOT VALID")
	err := a.Read(ctx, Record{Operation: "demo.customer.export", Outcome: Succeeded, Actor: alice})
	assert.Error(t, err, "no record, no data: the caller refuses the read")
	assert.Zero(t, count(t, admin, "true"))
}

// AU08: the record is committed before the data is released: once Read returns, another connection sees it, so a
// crash after it leaves a record of a read that may not have been delivered, never a delivery without a record.
func TestTheReadsRecordIsCommittedBeforeTheDataIsReleased(t *testing.T) {
	d, admin := dbtest.Schema(t, "audit_au08", nil)
	a := New(d)
	require.NoError(t, a.Read(context.Background(), Record{Operation: "demo.customer.export", Outcome: Succeeded, Actor: alice}))
	assert.Equal(t, 1, count(t, admin, "category = 'sensitive_read'"), "visible to another connection: committed")
}

// AU10: over capacity only the required audited operations are refused; other work goes on, and the transfer job
// alerts.
func TestOverCapacityOnlyRequiredAuditsAreRefused(t *testing.T) {
	d, admin := dbtest.Schema(t, "audit_au10", nil)
	a := New(d)
	ctx := context.Background()
	for range 2 {
		require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error { _, err := a.Write(ctx, tx, change("x.y")); return err }))
	}
	a.Capacity = 2
	var logs bytes.Buffer
	require.NoError(t, a.Job(nil, slog.New(slog.NewJSONHandler(&logs, nil))).Handle(ctx, nil))
	assert.Contains(t, logs.String(), "the audit backlog reached its capacity", "alerted")
	err := d.Tx(ctx, func(ctx context.Context, tx db.Tx) error { _, err := a.Write(ctx, tx, change("x.y")); return err })
	assert.True(t, errs.Is(err, "be", errs.ReasonAuditCapacityExhausted), "%v", err)
	assert.True(t, errs.Is(a.Read(ctx, Record{Operation: "x.y", Outcome: Succeeded}), "be", errs.ReasonAuditCapacityExhausted))
	pgtest.Exec(t, admin, "CREATE TABLE au10_plain (id int); GRANT ALL ON au10_plain TO PUBLIC")
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO au10_plain VALUES (1)")
		return err
	}), "work that needs no audit goes on")
	require.NoError(t, a.Denial(ctx, Record{Operation: "x.y", Reason: "PERMISSION_DENIED", Actor: alice}),
		"a denial is still recorded: it changes nothing and is budgeted")
}

// AU15: a restricted field of the target's resource is kept as a digest of its value, never in clear.
func TestRestrictedFieldsAreKeptAsDigests(t *testing.T) {
	d, admin := dbtest.Schema(t, "audit_au15", nil)
	a := New(d)
	a.Restricted = map[string]map[string]bool{"demo.customer": {"tax_id": true}}
	ctx := context.Background()
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := a.Write(ctx, tx, Record{Category: BusinessChange, Operation: "demo.customer.edit", Outcome: Succeeded,
			Actor: alice, Target: map[string]any{"type": "demo.customer", "id": "c1"},
			Changes: map[string]any{"tax_id": "B12345678", "name": "Acme"}})
		return err
	}))
	var record []byte
	var changes map[string]any
	var raw []byte
	require.NoError(t, admin.QueryRow(ctx, "SELECT record, changes FROM be_audit_log").Scan(&record, &raw))
	require.NoError(t, json.Unmarshal(raw, &changes))
	assert.NotContains(t, string(record), "B12345678", "never in clear, in the record")
	assert.NotContains(t, string(raw), "B12345678", "nor in the column")
	b, err := canon.Encode("B12345678")
	require.NoError(t, err)
	sum := sha256.Sum256(b)
	assert.Equal(t, "sha256:"+hex.EncodeToString(sum[:]), changes["tax_id"])
	assert.Equal(t, "Acme", changes["name"], "an ordinary field as it is")
}

// AU22 (the strict tier): an export whose operation declares audit: strict is released only once its record has a
// receipt; with the audit component out of reach it is refused and the record stays local, to be sent later.
func TestAStrictReadIsReleasedOnlyWithItsReceipt(t *testing.T) {
	d, admin := dbtest.Schema(t, "audit_au22", nil)
	a := New(d)
	sender := &fakeAudit{stored: map[string]string{}}
	a.Sender, a.Strict = sender, map[string]bool{"demo.customer.export": true}
	ctx := context.Background()
	require.NoError(t, a.Read(ctx, Record{Operation: "demo.customer.export", Outcome: Succeeded, Actor: alice}))
	assert.Equal(t, 1, count(t, admin, "transfer_state = 'received'"), "the receipt before the release")

	sender.down = true
	err := a.Read(ctx, Record{Operation: "demo.customer.export", Outcome: Succeeded, Actor: alice})
	assert.True(t, errs.Is(err, "be", errs.ReasonDependencyUnavailable), "refused: %v", err)
	assert.Equal(t, 1, count(t, admin, "transfer_state = 'pending'"), "the record stays local, sent later")
	require.NoError(t, a.Read(ctx, Record{Operation: "demo.order.read", Outcome: Succeeded, Actor: alice}),
		"the default tier does not wait for the audit component")

	a.Sender = conflicting{}
	err = a.Read(ctx, Record{Operation: "demo.customer.export", Outcome: Succeeded, Actor: alice})
	assert.True(t, errs.Is(err, "be", errs.ReasonDependencyUnavailable), "a receipt that is not received is none: %v", err)
}

// conflicting answers every record with a conflict.
type conflicting struct{}

func (conflicting) Send(_ context.Context, ws []Wire) ([]Receipt, error) {
	out := make([]Receipt, len(ws))
	for i, w := range ws {
		out[i] = Receipt{ID: w.ID, Status: "conflict"}
	}
	return out, nil
}

// AU23: two transfer replicas run at once and the receipts' storage sequences interleave with the local order;
// deletion compares each receipt with the backed-up watermark only, so nothing trips and nothing is sent again.
func TestInterleavedReceiptsTripNothing(t *testing.T) {
	d, admin := dbtest.Schema(t, "audit_au23", nil)
	a := New(d)
	ctx := context.Background()
	for range 6 {
		require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error { _, err := a.Write(ctx, tx, change("x.y")); return err }))
	}
	_, err := a.Number(ctx)
	require.NoError(t, err)
	sender := &reversing{fakeAudit: fakeAudit{stored: map[string]string{}}}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := a.Transfer(ctx, sender); assert.NoError(t, err) }()
	}
	wg.Wait()
	assert.Equal(t, 6, count(t, admin, "transfer_state = 'received'"))
	assert.Equal(t, 6, count(t, admin, "receipt_seq IS NOT NULL"))
	pgtest.Exec(t, admin, "UPDATE be_audit_log SET received_at = now() - interval '8 days'")
	del, resent, err := a.Purge(ctx, Watermark{Epoch: "1.1", Seq: 3})
	require.NoError(t, err)
	assert.Equal(t, int64(3), del, "the receipts up to the watermark, whatever their local order")
	assert.Zero(t, resent, "an interleaving is no trip")
	del, resent, err = a.Purge(ctx, Watermark{Epoch: "1.1", Seq: 6})
	require.NoError(t, err)
	assert.Equal(t, int64(3), del)
	assert.Zero(t, resent)
}

// reversing stores a batch in reverse order: storage sequences run against the local production order.
type reversing struct{ fakeAudit }

func (r *reversing) Send(ctx context.Context, ws []Wire) ([]Receipt, error) {
	rev := make([]Wire, len(ws))
	for i, w := range ws {
		rev[len(ws)-1-i] = w
	}
	return r.fakeAudit.Send(ctx, rev)
}
