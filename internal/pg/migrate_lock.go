package pg

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

// blocker is a backend that held a lock the migration waited for.
type blocker struct {
	PID   int    `json:"pid"`
	Query string `json:"query"` // first 200 characters; "<insufficient privilege>" for another role's backend
}

// withLockRetry runs op; a lock timeout (55P03) is retried up to MigrateLockRetries times with
// backoff 1 s · 2^n (P11.1). A step that failed on 55P03 rolled back, so a dirty state it left is
// forced back to the version before it. The final failure logs the blocking backends at ERROR.
func (r *runner) withLockRetry(ctx context.Context, m *gomigrate.Migrate, op func() error) error {
	before := -1
	if m != nil {
		if v, _, err := m.Version(); err == nil {
			before = int(v)
		}
	}
	backoff := orDefaultDuration(r.c.retryBackoff, time.Second)
	for attempt := 0; ; attempt++ {
		stop := r.watchBlockers(ctx)
		err := op()
		blockers := stop()
		if err == nil || SQLState(err) != "55P03" {
			return err
		}
		if rerr := restore(m, before); rerr != nil {
			return rerr
		}
		if attempt == MigrateLockRetries {
			r.log.Error("migration lock timeout, giving up", "attempts", attempt+1, "blockers", blockers)
			return problem.Wrap(err, "LOCK_TIMEOUT", nil)
		}
		r.log.Warn("migration lock timeout, retrying", "attempt", attempt+1, "blockers", blockers)
		if err := sleepCtx(ctx, backoff<<attempt); err != nil {
			return problem.From(err)
		}
	}
}

// restore forces the state back to before when a step that failed on 55P03 (and so rolled back)
// left it dirty.
func restore(m *gomigrate.Migrate, before int) error {
	if m == nil {
		return nil
	}
	if _, dirty, err := m.Version(); err == nil && dirty {
		if ferr := m.Force(before); ferr != nil {
			return problem.Wrap(ferr, "INTERNAL", nil)
		}
	}
	return nil
}

// watchBlockers polls, until stopped, the backends blocking this step's connection (found by its
// unique application_name) and returns the last non-empty set it saw.
func (r *runner) watchBlockers(ctx context.Context) func() []blocker {
	db, err := r.openDB(false)
	if err != nil {
		return func() []blocker { return nil }
	}
	wctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	var last []blocker
	done := make(chan struct{})
	go func() {
		defer close(done)
		// a step that ends before half its lock timeout never opens the watcher's connection
		first := time.NewTimer(orDefaultDuration(r.c.lockTimeout, MigrateLockTimeout) / 2)
		defer first.Stop()
		select {
		case <-wctx.Done():
			return
		case <-first.C:
		}
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			if b := r.blockers(wctx, db); len(b) > 0 {
				mu.Lock()
				last = b
				mu.Unlock()
			}
			select {
			case <-wctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return func() []blocker {
		cancel()
		<-done
		_ = db.Close()
		mu.Lock()
		defer mu.Unlock()
		return last
	}
}

func (r *runner) blockers(ctx context.Context, db *sql.DB) []blocker {
	rows, err := db.QueryContext(ctx, `SELECT b.pid, left(coalesce(b.query, ''), 200)
	  FROM pg_stat_activity w
	  CROSS JOIN LATERAL unnest(pg_blocking_pids(w.pid)) AS bp(pid)
	  JOIN pg_stat_activity b ON b.pid = bp.pid
	 WHERE w.application_name = left($1, 63)
	 ORDER BY b.pid`, r.appName)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var out []blocker
	for rows.Next() {
		var b blocker
		if rows.Scan(&b.PID, &b.Query) == nil {
			out = append(out, b)
		}
	}
	return out
}
