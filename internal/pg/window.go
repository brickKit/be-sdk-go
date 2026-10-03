package pg

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/internal/lifecycle"
	"github.com/brickKit/be-sdk-go/internal/problem"
)

// DefaultOutboxAhead is how many weeks after the current one the outbox window covers (lifecycle
// default `ahead: 2`, P16.6).
const DefaultOutboxAhead = lifecycle.OutboxAhead

// EnsureOutboxWindow creates, through the owner's SECURITY DEFINER function besdk_ensure_range_partition,
// the outbox partitions for the week containing now and the ahead weeks after it that do not exist
// yet (named besdk_outbox_<ISO year>w<ISO week>), and returns their names (P10.12, P16.6). It is
// idempotent. The lifecycle engine (be.lifecycle) keeps the outbox and every declared window ahead
// with lifecycle.EnsureWindows; this remains for a runtime without the engine.
func EnsureOutboxWindow(ctx context.Context, tx *Tx, now time.Time, ahead int) ([]string, error) {
	var created []string
	for _, w := range lifecycle.OutboxWindow(now, ahead) {
		var ok bool
		err := tx.QueryRowContext(ctx, `SELECT besdk_ensure_range_partition($1, $2, $3, $4)`,
			w.Table, w.Name, w.From, w.To).Scan(&ok)
		if err != nil {
			return created, err
		}
		if ok {
			created = append(created, w.Name)
		}
	}
	return created, nil
}

// ensureWindow is the platform migration's window step, as the owner in one transaction (P11.3,
// P16.6): the outbox's window and the window of every range-partitioned table of lifecycle.yaml
// (MigrateConfig.Lifecycle; nil = the outbox only), followers included.
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
		res, err := lifecycle.EnsureWindows(ctx, tx, r.c.Lifecycle, r.c.now(), "migration")
		if err != nil {
			return err
		}
		for _, s := range res.Skipped {
			r.log.Warn("partition skipped: an existing partition overlaps its range", "partition", s)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		created = res.Created
		return nil
	})
	if err != nil {
		return nil, problem.From(err)
	}
	return created, nil
}
