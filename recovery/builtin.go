package recovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/jackc/pgx/v5"
)

// The SDK's own checks (P14.2), which every component with a database has.
const (
	CheckProjections   = "be.projections"
	CheckConsumerEpoch = "be.consumer_epoch"
	CheckAuditBacklog  = "be.audit_backlog"
)

// builtins are the SDK's own checks; the consumer epoch only for a member that subscribes.
func builtins(subscribes bool) []Check {
	out := []Check{
		{Name: CheckProjections, Basis: "装配项目协议 02；be-protocol P9.25", Run: InTx(projections)},
	}
	if subscribes {
		out = append(out, Check{Name: CheckConsumerEpoch, Basis: "be-protocol P6.39、P6.40；装配项目协议 15 的 RC04", Run: InTx(consumerEpoch)})
	}
	return append(out, Check{Name: CheckAuditBacklog, Basis: "装配项目协议 03 的积压与故障", Run: InTx(auditBacklog)})
}

func outcome(diffs []Difference, disposal string) Result {
	if len(diffs) == 0 {
		return Result{Outcome: Pass}
	}
	return Result{Outcome: Diff, Differences: diffs, Disposal: disposal}
}

func collect(ctx context.Context, tx db.Tx, sql string, args ...any) ([]Difference, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Difference, error) {
		var d Difference
		return d, r.Scan(&d.ObjectType, &d.ObjectID, &d.Detail)
	})
}

// projections: every projection's source aligned, and no quarantined subject.
func projections(ctx context.Context, tx db.Tx) (Result, error) {
	diffs, err := collect(ctx, tx, `SELECT 'projection', projection, 'source ' || status FROM be_projection_source
		WHERE status <> 'aligned'
		UNION ALL
		SELECT 'projection_subject', projection || '/' || subject, 'quarantined: ' || count(*) || ' events'
			FROM be_watermark_quarantine GROUP BY projection, subject
		ORDER BY 1, 2`)
	if err != nil {
		return Result{}, err
	}
	return outcome(diffs, "等投影对齐（补取与补发）；被隔离的主体按协议 02 修复后重做检查"), nil
}

// consumerEpoch: the consumer side has recorded its progress in the database's current epoch (the redelivery after
// the rollback answered), and no redelivery left a gap.
func consumerEpoch(ctx context.Context, tx db.Tx) (Result, error) {
	var gen, tl int64
	var recorded bool
	err := tx.QueryRow(ctx, `SELECT c.epoch_generation, c.epoch_timeline, true FROM be_consumer_epoch c`).Scan(&gen, &tl, &recorded)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Result{}, err
	}
	var curGen, curTL int64
	if err := tx.QueryRow(ctx, "SELECT generation, timeline FROM be_current_db_epoch()").Scan(&curGen, &curTL); err != nil {
		return Result{}, err
	}
	var diffs []Difference
	if !recorded || gen != curGen || tl != curTL {
		diffs = append(diffs, Difference{ObjectType: "consumer_epoch", ObjectID: fmt.Sprintf("%d.%d", curGen, curTL),
			Detail: "the redelivery after the database went back has not answered (P6.39)"})
	}
	gaps, err := collect(ctx, tx, `SELECT 'resend_gap', subscription || ':' || source_component || '/' || source_stream || '/' ||
			source_partition, kind || coalesce(' after ' || progress, '')
		FROM be_resend_gap ORDER BY id`)
	if err != nil {
		return Result{}, err
	}
	return outcome(append(diffs, gaps...), "等补发答完；补发不能覆盖的（beyond_retention、ahead）列进恢复报告，按协议 15 的 RC04 处置"), nil
}

// auditBacklog: the local audit records not yet transferred (or in conflict), how many and since when.
func auditBacklog(ctx context.Context, tx db.Tx) (Result, error) {
	var pending, conflict int64
	var oldest *time.Time
	if err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE transfer_state = 'pending'),
			count(*) FILTER (WHERE transfer_state = 'conflict'), min(occurred_at) FILTER (WHERE transfer_state <> 'received')
		FROM be_audit_log`).Scan(&pending, &conflict, &oldest); err != nil {
		return Result{}, err
	}
	var diffs []Difference
	if pending > 0 {
		diffs = append(diffs, Difference{ObjectType: "audit_backlog", ObjectID: "pending",
			Detail: fmt.Sprintf("%d records not transferred, the oldest at %s", pending, oldest.UTC().Format(time.RFC3339))})
	}
	if conflict > 0 {
		diffs = append(diffs, Difference{ObjectType: "audit_backlog", ObjectID: "conflict",
			Detail: fmt.Sprintf("%d records the audit store answered with a conflict", conflict)})
	}
	return outcome(diffs, "等审计转运送完（AUDIT_GRPC_URL）后重做；冲突的记录按协议 03 核对"), nil
}
