package db

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync/atomic"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool is one physical connection pool to a target. The launcher opens it with the member's runtime role; the shell
// opens one per target with the shell's login role, which holds the members' roles WITH INHERIT FALSE.
type Pool struct {
	Target  Target
	pool    *pgxpool.Pool
	version atomic.Int64 // server_version_num, read once
}

// PoolConfig opens a pool.
type PoolConfig struct {
	Target   Target
	User     string
	Password *config.Secret
	// AppName labels the connections (the component, or the shell); members add their own per transaction.
	AppName  string
	MaxConns int32
	// Prepared=false switches pgx to CacheDescribe, which works behind a pooler without prepared statements (0-12).
	Prepared bool
}

// Open connects; the password is read from the secret at every new connection, so a rotated file applies to new
// connections (protocol P2.8).
func Open(ctx context.Context, c PoolConfig) (*Pool, error) {
	u := url.URL{Scheme: "postgres", User: url.User(c.User), Host: net.JoinHostPort(c.Target.Host, strconv.Itoa(c.Target.Port)),
		Path: "/" + c.Target.Database}
	q := u.Query()
	q.Set("sslmode", "prefer")
	u.RawQuery = q.Encode()
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("database target %s: %w", c.Target, err)
	}
	cfg.MaxConns = max(c.MaxConns, 1)
	cfg.ConnConfig.RuntimeParams["application_name"] = c.AppName
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	if !c.Prepared {
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	}
	cfg.BeforeConnect = func(_ context.Context, cc *pgx.ConnConfig) error {
		if c.Password != nil {
			cc.Password = c.Password.Value()
		}
		return nil
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("database target %s: %w", c.Target, err)
	}
	return &Pool{Target: c.Target, pool: p}, nil
}

// Close closes the pool (the launcher or the shell, after every member has stopped).
func (p *Pool) Close() { p.pool.Close() }

// serverVersion returns server_version_num (0 when it cannot be read).
func (p *Pool) serverVersion(ctx context.Context) int64 {
	if v := p.version.Load(); v != 0 {
		return v
	}
	var s string
	if err := p.pool.QueryRow(ctx, "SELECT current_setting('server_version_num')").Scan(&s); err != nil {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	p.version.Store(v)
	return v
}

// Ping checks connectivity (used at start, not by health checks).
func (p *Pool) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }
