package pg

import (
	"context"
	"time"
)

// DefaultPresencePing is how often HoldPresence checks its session.
const DefaultPresencePing = 30 * time.Second

// HoldPresence keeps one connection of the pool open, idle and outside any transaction, for as long
// as ctx lives (P10.5): the pool never closes its last connection, so a contract migration sees this
// process under its session-level name <component ID>@<version> (P10.2, P11.4). The session is
// pinged every ping; a broken one is replaced on the next round. It returns nil when ctx ends and
// the error of a failed acquire or ping otherwise, so a supervisor restarts it with backoff.
//
// Go note: the held connection is one of the PG_POOL_MAX connections; the pool's other work shares
// the rest.
func (p *Pool) HoldPresence(ctx context.Context, ping time.Duration) error {
	if ping <= 0 {
		ping = DefaultPresencePing
	}
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return ignoreCancel(ctx, err)
	}
	defer func() { _ = conn.Close() }()
	t := time.NewTicker(ping)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := conn.PingContext(ctx); err != nil {
				return ignoreCancel(ctx, err)
			}
		}
	}
}

func ignoreCancel(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}
