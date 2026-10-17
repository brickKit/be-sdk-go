package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// CheckDurable is the start check of protocol P1.10: fsync on, synchronous_commit not weaker than on. A failure
// keeps the component out of ready (exit 69).
func (p *Pool) CheckDurable(ctx context.Context) error {
	var fsync, syncCommit string
	if err := p.pool.QueryRow(ctx, "SELECT current_setting('fsync'), current_setting('synchronous_commit')").
		Scan(&fsync, &syncCommit); err != nil {
		return fmt.Errorf("reading the durable settings of %s: %w", p.Target, err)
	}
	if fsync != "on" {
		return fmt.Errorf("%s: fsync is %q, must be on", p.Target, fsync)
	}
	switch syncCommit {
	case "on", "remote_write", "remote_apply":
		return nil
	}
	return fmt.Errorf("%s: synchronous_commit is %q, must be on, remote_write or remote_apply", p.Target, syncCommit)
}

// Errors of Ready that end the start instead of being retried.
var (
	ErrCredentials = errors.New("the database refused the credentials")
	ErrStartCheck  = errors.New("a start check of the database failed")
)

// Ready waits until the target answers (the database may start after the component), then runs the durable check.
// Wrong credentials, a missing database and weak durable settings end the wait; connection failures are retried until
// ctx ends. The component stays "starting" meanwhile.
func (p *Pool) Ready(ctx context.Context, log *slog.Logger) error {
	for attempt := 1; ; attempt++ {
		err := p.Ping(ctx)
		if err == nil {
			break
		}
		var pg *pgconn.PgError
		if errors.As(err, &pg) {
			switch pg.Code {
			case "28P01", "28000":
				return fmt.Errorf("%w: %s: %s", ErrCredentials, p.Target, pg.Message)
			case "3D000":
				return fmt.Errorf("%w: %s: %s (not provisioned)", ErrStartCheck, p.Target, pg.Message)
			}
		}
		if attempt == 1 || attempt%10 == 0 {
			log.Warn("waiting for the database", "target", p.Target.String(), "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if err := p.CheckDurable(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrStartCheck, err)
	}
	return nil
}
