package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
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

// DefaultContractProbe is how long a contract migration watches pg_stat_activity before deciding
// (P11.4): a pooled session shows "<id>@<version>" while idle and the bare member ID inside a
// transaction (SET LOCAL application_name), so one look is not enough.
const DefaultContractProbe = 2 * time.Second

// versionedBackend is a backend of this component that a contract migration waits for.
type versionedBackend struct {
	PID     int
	Version string // "" = never seen outside a transaction: unknown
}

// AppName is the session-level application_name every SDK pool connection of a component carries:
// "<component id>@<version>" (P11.4 gating; inside a transaction SET LOCAL keeps the bare member ID,
// P10.2).
func AppName(componentID, version string) string { return componentID + "@" + version }

// oldBackends samples pg_stat_activity of this database for DefaultContractProbe and returns every
// backend of this component whose session name showed a version <= after (P11.4). Sessions are matched
// by application_name only; the owner sees it for every role's backend. A backend seen only under the
// bare member ID is inside a transaction and does not count: every serving process also keeps a
// versioned presence session (P10.5), which is how an old process is seen.
func (r *runner) oldBackends(ctx context.Context, after string) ([]versionedBackend, error) {
	db, err := r.openDB(false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	seen := map[int]map[string]bool{} // pid → versions seen ("" = bare ID)
	deadline := time.Now().Add(orDefaultDuration(r.c.contractProbe, DefaultContractProbe))
	for {
		if err := r.sampleBackends(ctx, db, seen); err != nil {
			return nil, err
		}
		if time.Now().After(deadline) {
			break
		}
		if err := sleepCtx(ctx, 100*time.Millisecond); err != nil {
			return nil, problem.From(err)
		}
	}
	return judgeBackends(seen, after), nil
}

func (r *runner) sampleBackends(ctx context.Context, db *sql.DB, seen map[int]map[string]bool) error {
	rows, err := db.QueryContext(ctx, `SELECT pid, application_name FROM pg_stat_activity
	  WHERE datname = current_database() AND pid <> pg_backend_pid()
	    AND (application_name = $1 OR left(application_name, length($1) + 1) = $1 || '@')`, r.c.ComponentID)
	if err != nil {
		return problem.Wrap(err, "INTERNAL", nil)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var pid int
		var name string
		if err := rows.Scan(&pid, &name); err != nil {
			return problem.Wrap(err, "INTERNAL", nil)
		}
		if seen[pid] == nil {
			seen[pid] = map[string]bool{}
		}
		_, v, _ := strings.Cut(name, "@")
		seen[pid][v] = true
	}
	return rows.Err()
}

// judgeBackends: a backend that ever showed a version <= after is old; the rest (newer versions, bare
// member IDs, unparsable versions) do not block.
func judgeBackends(seen map[int]map[string]bool, after string) []versionedBackend {
	var out []versionedBackend
	for pid, versions := range seen {
		old := ""
		for v := range versions {
			if c, err := compareSemver(v, after); err == nil && c <= 0 {
				old = v
			}
		}
		if old != "" {
			out = append(out, versionedBackend{PID: pid, Version: old})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PID < out[j].PID })
	return out
}

// contractGate stops before a contract file while an old version still runs (P11.4).
func (r *runner) contractGate(headers map[uint]fileHeader) func(ctx context.Context, next uint) error {
	return func(ctx context.Context, next uint) error {
		h := headers[next]
		if !h.Contract {
			return nil
		}
		old, err := r.oldBackends(ctx, h.After)
		if err != nil || len(old) == 0 {
			return err
		}
		names, _ := fs.Glob(r.c.Component, fmt.Sprintf("%d_*.up.sql", next))
		parts := make([]string, len(old))
		for i, b := range old {
			v := b.Version
			if v == "" {
				v = "unknown version"
			}
			parts[i] = fmt.Sprintf("%s (pid %d)", v, b.PID)
		}
		msg := fmt.Sprintf("contract migration %s (after=%s) waits until no version <= %s of %s runs; still running: %s",
			strings.Join(names, ","), h.After, h.After, r.c.ComponentID, strings.Join(parts, ", "))
		r.log.Error("contract migration blocked by older versions", "migration", strings.Join(names, ","), "after", h.After,
			"backends", old)
		return problem.Wrap(errors.New(msg), "INTERNAL", nil)
	}
}
