package pg

import (
	"context"
	"strings"
)

// The advisory lock key of P10.8: the first half is the schema plus the lock name, so two shell
// members using one name never collide, and a component running standalone and in a shell at once
// (one schema) excludes itself.
const (
	lockSQL    = `SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':' || $1), hashtext($2))`
	tryLockSQL = `SELECT pg_try_advisory_xact_lock(hashtext(current_schema() || ':' || $1), hashtext($2))`
)

// Lock takes the transaction-level advisory lock for name and parts (joined by '|'), waiting at most
// lock_timeout (P10.8); it is released at commit or rollback.
func (t *Tx) Lock(ctx context.Context, name string, parts ...string) error {
	_, err := t.ExecContext(ctx, lockSQL, name, strings.Join(parts, "|"))
	return err
}

// TryLock takes the same lock as Lock without waiting; false when another transaction holds it (P10.8).
func (t *Tx) TryLock(ctx context.Context, name string, parts ...string) (bool, error) {
	var ok bool
	err := t.QueryRowContext(ctx, tryLockSQL, name, strings.Join(parts, "|")).Scan(&ok)
	return ok, err
}
