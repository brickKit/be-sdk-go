package db

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"sync"
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
	log     *slog.Logger
	mu      sync.Mutex
	base    map[*pgx.Conn][]string // each connection's reported settings when it connected (P5.31)
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
	// Log receives a connection closed for a session setting a transaction left on it (P5.31); nil discards.
	Log *slog.Logger
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
	pool := &Pool{Target: c.Target, log: c.Log, base: map[*pgx.Conn][]string{}}
	if pool.log == nil {
		pool.log = slog.New(slog.DiscardHandler)
	}
	cfg.AfterConnect = func(_ context.Context, cn *pgx.Conn) error {
		pool.mu.Lock()
		pool.base[cn] = reportedOf(cn)
		pool.mu.Unlock()
		return nil
	}
	cfg.BeforeClose = func(cn *pgx.Conn) {
		pool.mu.Lock()
		delete(pool.base, cn)
		pool.mu.Unlock()
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("database target %s: %w", c.Target, err)
	}
	pool.pool = p
	return pool, nil
}

// reported are the settings the server reports to the client whenever they change (ParameterStatus), also when a
// function changes them, so a connection's values are known without a query (P5.31). Role, search_path and the
// timeouts are not among them; every transaction sets those itself (P5.5).
var reported = []string{"TimeZone", "DateStyle", "IntervalStyle", "client_encoding", "standard_conforming_strings",
	"application_name"}

func reportedOf(cn *pgx.Conn) []string {
	out := make([]string, len(reported))
	for i, k := range reported {
		out[i] = cn.PgConn().ParameterStatus(k)
	}
	return out
}

// release returns a connection after a member's transaction, or closes it when a reported setting differs from the
// value it connected with: the transaction left session state that the next borrower, possibly another member of the
// shell, would inherit (P5.31).
func (p *Pool) release(c *pgxpool.Conn, member string) {
	cn := c.Conn()
	p.mu.Lock()
	base, known := p.base[cn]
	p.mu.Unlock()
	if known {
		for i, now := range reportedOf(cn) {
			if now != base[i] {
				p.log.Error("a transaction left a session setting on its pooled connection; the connection is closed "+
					"(protocol P5.31)", "member", member, "parameter", reported[i], "was", base[i], "now", now)
				p.mu.Lock()
				delete(p.base, cn) // a hijacked connection no longer passes BeforeClose
				p.mu.Unlock()
				_ = c.Hijack().Close(context.Background())
				return
			}
		}
	}
	c.Release()
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
