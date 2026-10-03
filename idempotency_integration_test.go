package besdk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/testpg"
)

// storeRuntime is a Runtime over a fresh migrated schema (platform tables included), without serving.
func storeRuntime(t *testing.T) *Runtime {
	t.Helper()
	id := testpg.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pg.MigrateUp(ctx, pg.MigrateConfig{Host: id.Host, Port: id.Port, Database: id.Database, SSLMode: "disable",
		Owner: id.Owner, OwnerPassword: id.OwnerPassword, Schema: id.Schema, ComponentID: "test/thing",
		Component: thingMigrations}); err != nil {
		t.Fatal(err)
	}
	pool, err := pg.OpenPool(pg.PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: id.User,
		Password: func() string { return id.Password }, SSLMode: "disable"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	rt := &Runtime{id: "test/thing", version: "3.0.0", clock: time.Now}
	rt.deps.store = pg.NewStore(pool, pg.StoreConfig{ComponentID: "test/thing", Role: id.User, Schema: id.Schema})
	return rt
}

func asUser(sub string) context.Context {
	return context.WithValue(context.Background(), accessKey{}, &Access{user: User{Sub: sub}})
}

// P13: one-step idempotent command through the public API — executes once, replays the stored
// result to the same caller only, mismatches on a different body, and needs a caller.
func TestIdempotentCommand(t *testing.T) {
	rt := storeRuntime(t)
	store, _ := rt.Store()
	type out struct{ ID string }
	runs := 0
	cmd := func(ctx context.Context, req any) (out, bool, error) {
		var res out
		var replayed bool
		err := store.Tx(ctx, func(ctx context.Context, tx *Tx) error {
			var err error
			res, replayed, err = Idempotent(ctx, tx, Command{Key: "k1", Name: "test.thing.create", Request: req, Status: 201},
				func() (out, error) { runs++; return out{ID: "w1"}, nil })
			return err
		})
		return res, replayed, err
	}
	alice := asUser("alice")
	if r, replayed, err := cmd(alice, map[string]string{"name": "A"}); err != nil || replayed || r.ID != "w1" {
		t.Fatalf("first: %+v %v %v", r, replayed, err)
	}
	if r, replayed, err := cmd(alice, map[string]string{"name": "A"}); err != nil || !replayed || r.ID != "w1" || runs != 1 {
		t.Fatalf("replay: %+v %v %v runs=%d", r, replayed, err, runs)
	}
	if _, _, err := cmd(alice, map[string]string{"name": "B"}); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("mismatch: %v", err)
	}
	if _, replayed, err := cmd(asUser("bob"), map[string]string{"name": "A"}); err != nil || replayed || runs != 2 {
		t.Fatalf("other caller: %v %v runs=%d", replayed, err, runs)
	}
	if _, _, err := cmd(context.Background(), map[string]string{"name": "A"}); err == nil {
		t.Fatal("a request without a caller must not use idempotency")
	}
	if got := CallerOf(withHandling(context.Background(), envelopeEventForTest())); got != "system" {
		t.Fatalf("background caller %q", got)
	}
}

func envelopeEventForTest() envelope.Event { return envelope.Event{ID: "e1"} }
