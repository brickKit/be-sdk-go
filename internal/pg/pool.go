package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Pool defaults (be-protocol P2 configuration catalogue, P10.5; r1-04b for the statement cache).
const (
	DefaultPoolMax                = 10
	DefaultConnMaxLifetime        = 30 * time.Minute
	DefaultConnMaxIdleTime        = 5 * time.Minute
	DefaultStatementCacheCapacity = 512
	DefaultSSLMode                = "prefer"
)

// PoolConfig is one physical pool: a standalone component's, or the one pool a shell shares between
// its members (P10.5). Password is called for every new physical connection, so a rotated secret
// applies to the connections opened after the change (P2.9).
type PoolConfig struct {
	Host     string
	Port     int // 0 = 5432
	Database string
	User     string // the login role: PG_USER standalone, the shell's login role in a shell
	Password func() string
	SSLMode  string // libpq sslmode; "" = prefer

	MaxConns               int           // PG_POOL_MAX; 0 = 10
	MinIdle                int           // PG_POOL_MIN_IDLE; database/sql keeps no idle floor (see OpenPool)
	MaxLifetime            time.Duration // PG_CONN_MAX_LIFETIME; 0 = 30 min
	MaxIdleTime            time.Duration // PG_CONN_MAX_IDLE_TIME; 0 = 5 min
	StatementCacheCapacity int           // pgx statements per connection; 0 = 512 (a shell scales it by members)
	ApplicationName        string        // the startup application_name; each transaction sets its own (P10.2)
}

// Pool is the physical connection pool: database/sql over pgx/v5 stdlib in the default
// QueryExecModeCacheStatement (r1-04b), session TimeZone UTC as a startup parameter (P10.3).
type Pool struct {
	db *sql.DB
}

// OpenPool builds the pool without touching the network (P10.5). Every idle connection up to MaxConns
// is kept until MaxIdleTime: database/sql has no minimum-idle floor, so MinIdle is only validated.
func OpenPool(c PoolConfig) (*Pool, error) { return openPool(c, nil) }

// openPool is OpenPool with an after-connect hook for tests (wire tracing).
func openPool(c PoolConfig, afterConnect func(*pgx.Conn)) (*Pool, error) {
	if c.Password == nil {
		return nil, problem.Wrap(errors.New("pg pool: Password func is nil"), "INTERNAL", nil)
	}
	if c.Host == "" || c.Database == "" || c.User == "" {
		return nil, problem.Wrap(errors.New("pg pool: Host, Database and User are required"), "INTERNAL", nil)
	}
	cfg, err := connConfig(c.Host, c.Port, c.Database, c.User, c.SSLMode)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["TimeZone"] = "UTC"
	if c.ApplicationName != "" {
		cfg.RuntimeParams["application_name"] = c.ApplicationName
	}
	cfg.StatementCacheCapacity = orDefault(c.StatementCacheCapacity, DefaultStatementCacheCapacity)
	password := c.Password
	opts := []stdlib.OptionOpenDB{stdlib.OptionBeforeConnect(func(_ context.Context, cc *pgx.ConnConfig) error {
		cc.Password = password()
		return nil
	})}
	if afterConnect != nil {
		opts = append(opts, stdlib.OptionAfterConnect(func(_ context.Context, conn *pgx.Conn) error {
			afterConnect(conn)
			return nil
		}))
	}
	db := stdlib.OpenDB(*cfg, opts...)
	maxConns := orDefault(c.MaxConns, DefaultPoolMax)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(orDefaultDuration(c.MaxLifetime, DefaultConnMaxLifetime))
	db.SetConnMaxIdleTime(orDefaultDuration(c.MaxIdleTime, DefaultConnMaxIdleTime))
	return &Pool{db: db}, nil
}

// connConfig parses an explicit URL, so no field the caller sets is taken from the environment.
func connConfig(host string, port int, database, user, sslMode string) (*pgx.ConnConfig, error) {
	if port == 0 {
		port = 5432
	}
	if sslMode == "" {
		sslMode = DefaultSSLMode
	}
	u := url.URL{Scheme: "postgres", User: url.User(user), Host: net.JoinHostPort(host, strconv.Itoa(port)),
		Path: "/" + database, RawQuery: url.Values{"sslmode": {sslMode}}.Encode()}
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		return nil, problem.Wrap(fmt.Errorf("pg config: %w", err), "INTERNAL", nil)
	}
	return cfg, nil
}

// Close closes every connection of the pool.
func (p *Pool) Close() error { return p.db.Close() }

// ServerVersionNum is the server's version as server_version_num (160004), read from the startup
// parameter of one pooled connection without a query.
func (p *Pool) ServerVersionNum(ctx context.Context) (int, error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return 0, connectError(ctx, err)
	}
	defer func() { _ = conn.Close() }()
	return serverVersionOf(conn)
}

// serverVersionOf reads the server version of a held connection.
func serverVersionOf(conn *sql.Conn) (int, error) {
	var v int
	err := conn.Raw(func(dc any) error {
		sc, ok := dc.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %T", dc)
		}
		v = parseServerVersion(sc.Conn().PgConn().ParameterStatus("server_version"))
		return nil
	})
	if err != nil {
		return 0, problem.Wrap(err, "INTERNAL", nil)
	}
	return v, nil
}

// parseServerVersion turns "16.4 (Debian …)" into 160004 and "17beta1" into 170000; 0 when unknown.
func parseServerVersion(s string) int {
	major, rest := leadingInt(s)
	if major == 0 {
		return 0
	}
	minor := 0
	if len(rest) > 0 && rest[0] == '.' {
		minor, _ = leadingInt(rest[1:])
	}
	return major*10000 + minor
}

func leadingInt(s string) (int, string) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	n, _ := strconv.Atoi(s[:i])
	return n, s[i:]
}

// connectError classifies a failure to obtain a connection: 53300 is DB_TOO_MANY_CONNECTIONS (P10.4);
// the caller's cancellation or deadline is reported as such; an unreachable database is
// DEPENDENCY_UNAVAILABLE {dependency: db} (stage-B ruling); anything else is INTERNAL.
func connectError(ctx context.Context, err error) error {
	if SQLState(err) == "53300" {
		return problem.Wrap(err, "DB_TOO_MANY_CONNECTIONS", nil)
	}
	if ctx.Err() != nil {
		return problem.From(ctx.Err())
	}
	if IsConnectionFailure(err) {
		return problem.DBUnavailable(err)
	}
	return problem.Wrap(err, "INTERNAL", nil)
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func orDefaultDuration(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
