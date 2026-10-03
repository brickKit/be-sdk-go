package pg

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// DefaultOutboxAhead is how many weeks after the current one the outbox window covers (lifecycle
// default `ahead: 2`, P16.6).
const DefaultOutboxAhead = 2

// outboxParent is the partitioned outbox table of the reference DDL (ddl/02-outbox.sql).
const outboxParent = "besdk_outbox"

// weekPartition is one weekly RANGE partition [From, To) of besdk_outbox.
type weekPartition struct {
	Name     string
	From, To time.Time
}

// outboxWindow lists the outbox partitions for the week containing now and the `ahead` weeks after
// it: weeks start Monday 00:00 UTC and are named besdk_outbox_<ISO year>w<ISO week, 2 digits>
// (P11.3, P16.6). A negative ahead counts as 0.
func outboxWindow(now time.Time, ahead int) []weekPartition {
	now = now.UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	monday := day.AddDate(0, 0, -((int(day.Weekday()) + 6) % 7))
	out := make([]weekPartition, 0, max(ahead, 0)+1)
	for i := 0; i <= max(ahead, 0); i++ {
		from := monday.AddDate(0, 0, 7*i)
		year, week := from.ISOWeek()
		out = append(out, weekPartition{
			Name: fmt.Sprintf("%s_%dw%02d", outboxParent, year, week),
			From: from, To: from.AddDate(0, 0, 7),
		})
	}
	return out
}

// rowQuerier is what the window needs: a *Tx (runtime) or the owner's *sql.Tx (migration).
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// EnsureOutboxWindow creates, through the owner's SECURITY DEFINER function besdk_ensure_range_partition,
// the outbox partitions for the week containing now and the ahead weeks after it that do not exist
// yet, and returns their names (P10.12, P16.6). It is idempotent.
func EnsureOutboxWindow(ctx context.Context, tx *Tx, now time.Time, ahead int) ([]string, error) {
	return ensureWindow(ctx, tx, now, ahead)
}

func ensureWindow(ctx context.Context, q rowQuerier, now time.Time, ahead int) ([]string, error) {
	var created []string
	for _, w := range outboxWindow(now, ahead) {
		var ok bool
		err := q.QueryRowContext(ctx, `SELECT besdk_ensure_range_partition($1, $2, $3, $4)`,
			outboxParent, w.Name, w.From, w.To).Scan(&ok)
		if err != nil {
			return created, err
		}
		if ok {
			created = append(created, w.Name)
		}
	}
	return created, nil
}

// ensureWindow is the platform migration's window step, as the owner in one transaction (P11.3).
func (r *runner) ensureWindow(ctx context.Context) ([]string, error) {
	db, err := r.openDB(true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var created []string
	err = r.withLockRetry(ctx, nil, func() error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if created, err = ensureWindow(ctx, tx, r.c.now(), orDefault(r.c.OutboxAhead, DefaultOutboxAhead)); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		return nil, problem.From(err)
	}
	return created, nil
}
