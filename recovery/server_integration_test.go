//go:build integration

package recovery

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/recoverypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func outcomes(t *testing.T, s *Server) map[string]*recoverypb.CheckResult {
	t.Helper()
	resp, err := s.Check(context.Background(), &recoverypb.CheckRequest{Recovery: "rec-1"})
	require.NoError(t, err)
	out := map[string]*recoverypb.CheckResult{}
	for _, r := range resp.GetResults() {
		out[r.GetCheck()] = r
	}
	return out
}

// P14.1, P14.2: the SDK's own checks and the component's, each read-only; a check that cannot run is unprovable.
func TestChecks(t *testing.T) {
	d, admin := dbtest.Schema(t, "recovery_checks", nil)
	reg := &Checks{}
	require.NoError(t, reg.Add(Check{Name: "demo.writes", Basis: "test", Run: InTx(func(ctx context.Context, tx db.Tx) (Result, error) {
		_, err := tx.Exec(ctx, "INSERT INTO be_doc_series (series, last_number, gapless) VALUES ('x', 1, true)")
		return Result{Outcome: Pass}, err
	})}))
	reads := 0
	require.NoError(t, reg.Add(Check{Name: "demo.twice", Basis: "test", Run: func(ctx context.Context, read Read) (Result, error) {
		for range 2 { // a read, a call to the other side in between, a read
			if err := read(ctx, func(ctx context.Context, tx db.Tx) error {
				reads++
				_, err := tx.Exec(ctx, "INSERT INTO be_doc_series (series, last_number, gapless) VALUES ('y', 1, true)")
				return err
			}); err != nil {
				return Result{}, err
			}
		}
		return Result{Outcome: Pass}, nil
	}}))
	require.NoError(t, reg.Add(Check{Name: "demo.broken", Basis: "test", Run: func(context.Context, Read) (Result, error) {
		return Result{}, errors.New("cannot tell")
	}}))
	assert.Error(t, reg.Add(Check{Name: "demo.broken", Run: func(context.Context, Read) (Result, error) { return Result{}, nil }}))
	s := NewServer(d, true, reg)

	got := outcomes(t, s)
	assert.Equal(t, Pass, got[CheckProjections].GetOutcome())
	assert.Equal(t, Pass, got[CheckAuditBacklog].GetOutcome())
	assert.Equal(t, Diff, got[CheckConsumerEpoch].GetOutcome(), "never recorded in this epoch")
	assert.Equal(t, Pass, got["demo.writes"].GetOutcome())
	assert.Equal(t, Pass, got["demo.twice"].GetOutcome())
	assert.Equal(t, 2, reads)
	assert.Equal(t, Unprovable, got["demo.broken"].GetOutcome())
	assert.Contains(t, got["demo.broken"].GetDisposal(), "cannot tell")
	var n int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM be_doc_series").Scan(&n))
	assert.Zero(t, n, "a check's writes are rolled back, every read")
	assert.NotContains(t, outcomes(t, NewServer(d, false, nil)), CheckConsumerEpoch, "only a member that subscribes")

	pgtest.Exec(t, admin, "INSERT INTO be_consumer_epoch (epoch_generation, epoch_timeline) SELECT generation, timeline FROM be_current_db_epoch()")
	pgtest.Exec(t, admin, "INSERT INTO be_projection_source (projection, status) VALUES ('p.ok', 'aligned'), ('p.late', 'stalled')")
	pgtest.Exec(t, admin, `INSERT INTO be_watermark_quarantine (projection, subject, impl, epoch, align_gen, partition, seq, event)
		VALUES ('p.ok', 'customer-7', 'i', 'e', 1, 0, 9, '{}')`)
	pgtest.Exec(t, admin, `INSERT INTO be_audit_log (id, occurred_at, category, operation, outcome, digest)
		VALUES (gen_random_uuid(), now(), 'business_change', 'demo.op', 'succeeded', 'd')`)
	got = outcomes(t, s)
	assert.Equal(t, Pass, got[CheckConsumerEpoch].GetOutcome(), "recorded in the current epoch")
	pgtest.Exec(t, admin, "UPDATE be_consumer_epoch SET epoch_timeline = epoch_timeline + 1")
	assert.Equal(t, Diff, outcomes(t, s)[CheckConsumerEpoch].GetOutcome(), "another timeline")
	pgtest.Exec(t, admin, "UPDATE be_consumer_epoch SET epoch_timeline = epoch_timeline - 1, epoch_generation = epoch_generation + 1")
	assert.Equal(t, Diff, outcomes(t, s)[CheckConsumerEpoch].GetOutcome(), "another generation")
	pgtest.Exec(t, admin, "UPDATE be_consumer_epoch SET epoch_generation = epoch_generation - 1")
	require.Equal(t, Diff, got[CheckProjections].GetOutcome())
	var objs []string
	for _, d := range got[CheckProjections].GetDifferences() {
		objs = append(objs, d.GetObjectType()+":"+d.GetObjectId())
	}
	assert.Equal(t, []string{"projection:p.late", "projection_subject:p.ok/customer-7"}, objs)
	require.Equal(t, Diff, got[CheckAuditBacklog].GetOutcome())
	assert.Contains(t, got[CheckAuditBacklog].GetDifferences()[0].GetDetail(), "1 records not transferred")
	pgtest.Exec(t, admin, "UPDATE be_audit_log SET transfer_state = 'conflict'")
	got = outcomes(t, s)
	require.Equal(t, Diff, got[CheckAuditBacklog].GetOutcome())
	require.Len(t, got[CheckAuditBacklog].GetDifferences(), 1)
	assert.Equal(t, "conflict", got[CheckAuditBacklog].GetDifferences()[0].GetObjectId())
	pgtest.Exec(t, admin, "UPDATE be_audit_log SET transfer_state = 'received'")
	assert.Equal(t, Pass, outcomes(t, s)[CheckAuditBacklog].GetOutcome(), "transferred")

	pgtest.Exec(t, admin, `INSERT INTO be_resend_gap (subscription, source_component, source_stream, source_partition, kind)
		VALUES ('s', 'erp/sales', 'orders', 0, 'beyond_retention')`)
	assert.Equal(t, Diff, outcomes(t, s)[CheckConsumerEpoch].GetOutcome(), "a gap the redelivery could not cover")
}

// P14.3: present at the revision asked or later, older, absent; a type without a question, a version that is no
// revision; a table's presence.
func TestPresent(t *testing.T) {
	d, _ := dbtest.Schema(t, "recovery_present", fstest.MapFS{"0001_invoice.up.sql": {Data: []byte(
		`CREATE TABLE invoice (id uuid PRIMARY KEY, version bigint NOT NULL);
		INSERT INTO invoice VALUES ('0190f3a1-0000-7000-8000-0000000000b1', 3);`)}})
	reg := &Checks{}
	reg.Presence("invoice", TablePresence("invoice", "id", "version"))
	reg.Presence("order", func(_ context.Context, _ db.Tx, ids []string) (map[string]int64, error) {
		out := map[string]int64{}
		for _, id := range ids {
			if id != "o-gone" {
				out[id] = 5
			}
		}
		return out, nil
	})
	s := NewServer(d, false, reg)
	resp, err := s.Present(context.Background(), &recoverypb.PresentRequest{Recovery: "rec-1", Objects: []*recoverypb.ObjectRef{
		{Type: "order", Id: "o-1", Version: "5"}, {Type: "order", Id: "o-2", Version: "7"}, {Type: "order", Id: "o-gone", Version: "1"},
		{Type: "order", Id: "o-3"}}})
	require.NoError(t, err)
	var got []string
	for _, a := range resp.GetAnswers() {
		got = append(got, a.GetObject().GetId()+":"+a.GetAnswer()+":"+a.GetVersion())
	}
	assert.Equal(t, []string{"o-1:present:5", "o-2:older:5", "o-gone:absent:", "o-3:present:5"}, got)
	inv, err := s.Present(context.Background(), &recoverypb.PresentRequest{Recovery: "rec-1", Objects: []*recoverypb.ObjectRef{
		{Type: "invoice", Id: "0190f3a1-0000-7000-8000-0000000000b1", Version: "4"}, {Type: "invoice", Id: "not-a-uuid"}}})
	require.NoError(t, err)
	assert.Equal(t, Older, inv.GetAnswers()[0].GetAnswer())
	assert.Equal(t, "3", inv.GetAnswers()[0].GetVersion())
	assert.Equal(t, Absent, inv.GetAnswers()[1].GetAnswer())
	_, err = s.Present(context.Background(), &recoverypb.PresentRequest{Recovery: "rec-1", Objects: []*recoverypb.ObjectRef{{Type: "shipment", Id: "i-1"}}})
	assert.True(t, errs.Is(err, "be", errs.ReasonCapabilityUnsupported), "%v", err)
	_, err = s.Present(context.Background(), &recoverypb.PresentRequest{Recovery: "rec-1", Objects: []*recoverypb.ObjectRef{{Type: "order", Id: "o-1", Version: "v5"}}})
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "%v", err)
}

// P14.5: the continuous series and their used maximum; a raise never lowers, creates a series not there, has the
// component void the skipped numbers in the same transaction; a series with gaps is refused.
func TestNumbers(t *testing.T) {
	d, admin := dbtest.Schema(t, "recovery_numbers", nil)
	ctx := context.Background()
	inv := db.Series{LegalEntity: "le-1", Code: "INV", Period: "2026"}
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		for range 3 {
			if _, err := db.NextNumber(ctx, tx, inv); err != nil {
				return err
			}
		}
		return nil
	}))
	_, err := d.NextGappedNumber(ctx, db.Series{LegalEntity: "le-1", Code: "DRAFT"})
	require.NoError(t, err)
	type voided struct {
		code     string
		from, to int64
	}
	var void []voided
	reg := &Checks{}
	reg.Void(func(_ context.Context, _ db.Tx, s db.Series, from, to int64) error {
		void = append(void, voided{s.Code, from, to})
		return nil
	})
	s := NewServer(d, false, reg)
	nums, err := s.Numbers(ctx, &recoverypb.NumbersRequest{Recovery: "rec-1"})
	require.NoError(t, err)
	require.Len(t, nums.GetSeries(), 1, "only the continuous series")
	assert.Equal(t, int64(3), nums.GetSeries()[0].GetMaxUsed())
	assert.Equal(t, "INV", nums.GetSeries()[0].GetCode())
	assert.Equal(t, "2026", nums.GetSeries()[0].GetPeriod())

	raise := func(srv *Server, series ...*recoverypb.Series) ([]int64, error) {
		resp, err := srv.AdvanceNumbers(ctx, &recoverypb.AdvanceNumbersRequest{Recovery: "rec-1", Series: series})
		var out []int64
		for _, s := range resp.GetSeries() {
			out = append(out, s.GetMaxUsed())
		}
		return out, err
	}
	got, err := raise(s, &recoverypb.Series{LegalEntity: "le-1", Code: "INV", Period: "2026", MaxUsed: 7},
		&recoverypb.Series{LegalEntity: "le-2", Code: "INV", Period: "2026", MaxUsed: 2})
	require.NoError(t, err)
	assert.Equal(t, []int64{7, 2}, got)
	assert.Equal(t, []voided{{"INV", 4, 7}, {"INV", 1, 2}}, void)
	got, err = raise(s, &recoverypb.Series{LegalEntity: "le-1", Code: "INV", Period: "2026", MaxUsed: 5})
	require.NoError(t, err)
	assert.Equal(t, []int64{7}, got, "never lowered")
	assert.Len(t, void, 2, "nothing skipped")

	_, err = raise(s, &recoverypb.Series{LegalEntity: "le-1", Code: "DRAFT", MaxUsed: 9})
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "%v", err)
	_, err = raise(NewServer(d, false, nil), &recoverypb.Series{LegalEntity: "le-1", Code: "INV", Period: "2026", MaxUsed: 9})
	assert.True(t, errs.Is(err, "be", errs.ReasonCapabilityUnsupported), "%v", err)
	var last int64
	require.NoError(t, admin.QueryRow(ctx, "SELECT last_number FROM be_doc_series WHERE series = 'le-1/INV/2026'").Scan(&last))
	assert.Equal(t, int64(7), last, "a refused raise writes nothing")
}
