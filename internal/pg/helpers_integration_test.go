package pg

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// poolFor opens a pool logged in as user and closes it when the test ends.
func poolFor(t *testing.T, id testpg.Identity, user, password string, maxConns int) *Pool {
	t.Helper()
	p, err := OpenPool(PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: user,
		Password: func() string { return password }, SSLMode: "disable", MaxConns: maxConns})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// standalone returns a pool logged in as the identity's runtime role and its store.
func standalone(t *testing.T, id testpg.Identity, maxConns int) (*Pool, *Store) {
	t.Helper()
	p := poolFor(t, id, id.User, id.Password, maxConns)
	return p, NewStore(p, StoreConfig{ComponentID: "conformance/widget", Role: id.User, Schema: id.Schema})
}

// asOwner runs DDL as the identity's owner with search_path = its schema.
func asOwner(t *testing.T, id testpg.Identity, stmts ...string) {
	t.Helper()
	db := testpg.Open(t, id.DSN(id.Owner, id.OwnerPassword))
	all := append([]string{fmt.Sprintf(`SET search_path TO %s`, pgx.Identifier{id.Schema}.Sanitize())}, stmts...)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, s := range all {
		if _, err := conn.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("owner exec %q: %v", s, err)
		}
	}
}

// within gives a context with a deadline d from now.
func within(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// wireTrace collects the protocol messages of every connection a pool opens.
type wireTrace struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *wireTrace) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *wireTrace) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *wireTrace) attach(c *pgx.Conn) {
	c.PgConn().Frontend().Trace(w, pgproto3.TracerOptions{SuppressTimestamps: true})
}
