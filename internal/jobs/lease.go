package jobs

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// leaseInsert creates the lease row once, expired and held by nobody, so the first take is epoch 1.
const leaseInsert = `INSERT INTO besdk_job_lease (name, holder, epoch, expires_at)
VALUES ($1, '', 0, now() - interval '1 second') ON CONFLICT (name) DO NOTHING`

// leaseTake is the normative take-or-renew statement of P14 "Statements".
const leaseTake = `UPDATE besdk_job_lease
   SET holder = $2, epoch = CASE WHEN holder = $2 THEN epoch ELSE epoch + 1 END,
       expires_at = now() + $3::bigint * interval '1 millisecond'
 WHERE name = $1 AND (expires_at < now() OR holder = $2)
RETURNING epoch`

// leaseRelease lets another holder take over at once after a graceful stop.
const leaseRelease = `UPDATE besdk_job_lease SET expires_at = now() - interval '1 millisecond'
 WHERE name = $1 AND holder = $2 AND epoch = $3`

// take runs the take-or-renew statement: ok is false when another holder has the lease.
func (e *Engine) take(ctx context.Context, name, holder string) (epoch int64, ok bool, err error) {
	ttl := e.cfg.leaseTTL()
	err = e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		if _, err := tx.ExecContext(ctx, leaseInsert, name); err != nil {
			return err
		}
		err := tx.QueryRowContext(ctx, leaseTake, name, holder, ttl.Milliseconds()).Scan(&epoch)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			ok = false
			return nil
		case err != nil:
			return err
		}
		ok = true
		return nil
	})
	return epoch, ok, err
}

// lease is a held singleton lease: ctx is cancelled with errLeaseLost when it is lost.
type lease struct {
	e            *Engine
	name, holder string
	epoch        int64
	ctx          context.Context
	cancel       context.CancelCauseFunc
	renewed      chan struct{} // closed when the renewal goroutine has returned
	validUntil   time.Time
}

// acquire takes the lease for holder and keeps it renewed every TTL/3 until release (P14
// "singleton"). ok is false, with no error, when another holder has it.
func (e *Engine) acquire(ctx context.Context, name, holder string) (*lease, bool, error) {
	start := time.Now()
	epoch, ok, err := e.take(ctx, name, holder)
	if err != nil || !ok {
		return nil, false, err
	}
	lctx, cancel := context.WithCancelCause(ctx)
	l := &lease{e: e, name: name, holder: holder, epoch: epoch, ctx: lctx, cancel: cancel,
		renewed: make(chan struct{}), validUntil: start.Add(e.cfg.leaseTTL())}
	go l.renew()
	return l, true, nil
}

// renew keeps the lease (decision tree per tick):
//   - the statement returned our epoch: valid for another TTL from when it was sent;
//   - it returned no row or another epoch: someone else took over, the lease is lost;
//   - it failed (database unreachable): keep trying while the last confirmed TTL lasts, then lost.
//
// A lost lease cancels l.ctx with errLeaseLost, which cancels the run (P14 "singleton").
func (l *lease) renew() {
	defer close(l.renewed)
	ttl := l.e.cfg.leaseTTL()
	tick := time.NewTicker(ttl / 3)
	defer tick.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-tick.C:
		}
		start := time.Now()
		epoch, ok, err := l.e.take(l.ctx, l.name, l.holder)
		switch {
		case l.ctx.Err() != nil:
			return
		case err == nil && ok && epoch == l.epoch:
			l.validUntil = start.Add(ttl)
		case err == nil:
			l.lose("another holder took the lease")
			return
		case time.Now().After(l.validUntil):
			l.lose("lease not renewed within its TTL: " + errorText(err))
			return
		default:
			l.e.log.Warn("lease renewal failed; retrying", slog.String("job", l.name), slog.String("error", errorText(err)))
		}
	}
}

func (l *lease) lose(why string) {
	l.e.log.Warn("singleton lease lost; the run is cancelled", slog.String("job", l.name),
		slog.Int64("epoch", l.epoch), slog.String("reason", why))
	l.cancel(errLeaseLost)
}

// release stops the renewal and, unless the lease was lost, gives it up so another holder can take
// over at once.
func (l *lease) release() {
	l.cancel(context.Canceled)
	<-l.renewed
	if errors.Is(context.Cause(l.ctx), errLeaseLost) {
		return
	}
	ctx, cancel := detached(l.ctx)
	defer cancel()
	err := l.e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, leaseRelease, l.name, l.holder, l.epoch)
		return err
	})
	if err != nil {
		l.e.log.Warn("lease release failed; it expires within its TTL", slog.String("job", l.name), slog.String("error", errorText(err)))
	}
}
