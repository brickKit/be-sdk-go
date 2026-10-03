package acl

import (
	"context"
	"fmt"
	"io/fs"
	"slices"
	"sync"
	"testing"
	"time"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

const (
	orderType = "erp.sales.order"
	otherType = "erp.sales.quote"
	foreign   = "crm.opportunity.opportunity"
)

func within(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// newStore is a fresh schema holding the projection tables, created by the owner from the pinned
// be-protocol's ddl/07-authz-projection.sql.
func newStore(t *testing.T) *pg.Store {
	t.Helper()
	id := testpg.New(t)
	ddl, err := fs.ReadFile(beprotocol.FS, "ddl/07-authz-projection.sql")
	require.NoError(t, err)
	owner := testpg.Open(t, id.DSN(id.Owner, id.OwnerPassword))
	_, err = owner.Exec(`SET search_path TO "` + id.Schema + `"; ` + string(ddl))
	require.NoError(t, err)
	p, err := pg.OpenPool(pg.PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: id.User,
		Password: func() string { return id.Password }, SSLMode: "disable", MaxConns: 6})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return pg.NewStore(p, pg.StoreConfig{ComponentID: "erp/sales", Role: id.User, Schema: id.Schema, Budget: 6})
}

func newProjection(t *testing.T, s *pg.Store, f *fakeProvider, mut ...func(*Config)) *Projection {
	c := Config{URL: f.srv.URL, Caller: "erp/sales", Types: []string{orderType, otherType}, Store: s}
	for _, m := range mut {
		m(&c)
	}
	p, err := New(c)
	require.NoError(t, err)
	return p
}

// projected is the projection's rows of the given types as provider keys, sorted.
func projected(t *testing.T, s *pg.Store, types ...string) []string {
	t.Helper()
	out := []string{}
	err := s.Run(within(t, 10*time.Second), pg.TxOptions{ReadOnly: true}, func(ctx context.Context, tx *pg.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT rtype, rid, relation, subject FROM besdk_authz_acl WHERE rtype = ANY($1::text[])`, types)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var a, b, c, d string
			if err := rows.Scan(&a, &b, &c, &d); err != nil {
				return err
			}
			out = append(out, a+"|"+b+"|"+c+"|"+d)
		}
		return rows.Err()
	})
	require.NoError(t, err)
	slices.Sort(out)
	return out
}

func cursorOf(t *testing.T, s *pg.Store, scope string) (rev int64, rebuilt bool) {
	t.Helper()
	err := s.Run(within(t, 10*time.Second), pg.TxOptions{ReadOnly: true}, func(ctx context.Context, tx *pg.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT revision, rebuilt_at IS NOT NULL FROM besdk_authz_cursor WHERE scope = $1`, scope).Scan(&rev, &rebuilt)
	})
	require.NoError(t, err)
	return rev, rebuilt
}

const scope = orderType + "," + otherType

func TestSyncPullsAppliesAndAdvancesToWatermark(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	f.upsert(orderType, "o1", "viewer", "user:u_a")
	f.upsert(orderType, "o1", "editor", "role:clerk")
	f.upsert(foreign, "p1", "member", "user:u_a")
	f.upsert(otherType, "q1", "viewer", "dept_tree:/1/")
	f.remove(orderType, "o1", "viewer", "user:u_a")
	f.upsert(orderType, "o1", "viewer", "user:u_a")
	f.remove(orderType, "o1", "editor", "role:clerk")
	head := f.upsert(foreign, "p2", "member", "user:u_b")

	p := newProjection(t, s, f)
	require.NoError(t, p.Sync(within(t, 10*time.Second)))
	require.Equal(t, f.stateOf(orderType, otherType), projected(t, s, orderType, otherType))
	require.Empty(t, projected(t, s, foreign), "only the component's own and inherited types are pulled (P6.12)")
	rev, rebuilt := cursorOf(t, s, scope)
	require.Equal(t, head, rev, "a caught-up consumer advances to the watermark")
	require.False(t, rebuilt)
	require.Equal(t, head, p.Watermark())
	require.Contains(t, f.requestLog()[0], "/authz/v2/changes?")
	require.Contains(t, f.requestLog()[0], "after=0")
	require.Contains(t, f.requestLog()[0], "limit=500")
	require.Contains(t, f.requestLog()[0], "types=erp.sales.order%2Cerp.sales.quote")
	require.Equal(t, "erp/sales", f.callers[0])

	f.remove(otherType, "q1", "viewer", "dept_tree:/1/")
	require.NoError(t, p.Sync(within(t, 10*time.Second)))
	require.Equal(t, f.stateOf(orderType, otherType), projected(t, s, orderType, otherType))
	require.Contains(t, f.requestLog()[1], fmt.Sprintf("after=%d", head))
}

func TestSyncFollowsNextAcrossPages(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	for i := range 1203 {
		f.upsert(orderType, fmt.Sprintf("o%04d", i), "viewer", "user:u_a")
	}
	p := newProjection(t, s, f)
	require.NoError(t, p.Sync(within(t, 30*time.Second)))
	require.Len(t, projected(t, s, orderType), 1203)
	require.Len(t, f.requestLog(), 3, "500 + 500 + 203")
	rev, _ := cursorOf(t, s, scope)
	require.EqualValues(t, 1203, rev)
}

func TestSyncStoresExpiry(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	at := time.Date(2026, 10, 9, 8, 53, 20, 0, time.UTC)
	f.write("upsert", fakeTuple{Type: orderType, ID: "o1", Relation: "viewer", Subject: "user:u_a", ExpiresAt: &at})
	f.upsert(orderType, "o1", "viewer", "user:u_b")
	p := newProjection(t, s, f)
	require.NoError(t, p.Sync(within(t, 10*time.Second)))
	var rows []authz.ACLRow
	require.NoError(t, s.Run(within(t, 10*time.Second), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		var err error
		rows, err = Load(ctx, tx, orderType, "o1")
		return err
	}))
	require.Len(t, rows, 2)
	require.Equal(t, "user:u_a", rows[0].Subject)
	require.NotNil(t, rows[0].ExpiresAt)
	require.True(t, at.Equal(*rows[0].ExpiresAt))
	require.Nil(t, rows[1].ExpiresAt)
	require.Equal(t, authz.ACLRow{RType: orderType, RID: "o1", Relation: "viewer", Subject: "user:u_b"}, rows[1])
}

func TestSync410RebuildsFromSnapshotAndContinues(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	f.upsert(orderType, "o1", "viewer", "user:gone")
	f.upsert(foreign, "keep", "member", "user:u_a")
	p := newProjection(t, s, f, func(c *Config) { c.PageSize = 2 })
	require.NoError(t, p.Sync(within(t, 10*time.Second)))
	// A foreign type's row in the same table is not ours to touch.
	require.NoError(t, s.Run(within(t, 10*time.Second), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO besdk_authz_acl VALUES ($1,'x','member','user:u_a',NULL,1)`, foreign)
		return err
	}))

	f.remove(orderType, "o1", "viewer", "user:gone")
	for i := range 5 {
		f.upsert(orderType, fmt.Sprintf("n%d", i), "viewer", "role:r")
	}
	f.upsert(otherType, "q1", "viewer", "user:u_a")
	f.compact()
	snapRev := f.head
	require.NoError(t, p.Sync(within(t, 10*time.Second)))
	require.Equal(t, f.stateOf(orderType, otherType), projected(t, s, orderType, otherType))
	require.Equal(t, []string{foreign + "|x|member|user:u_a"}, projected(t, s, foreign))
	rev, rebuilt := cursorOf(t, s, scope)
	require.Equal(t, snapRev, rev)
	require.True(t, rebuilt)
	require.Equal(t, 2, f.snapshots, "one snapshot walk per type")

	later := f.upsert(orderType, "after", "viewer", "user:u_a")
	require.NoError(t, p.Sync(within(t, 10*time.Second)))
	require.Equal(t, f.stateOf(orderType, otherType), projected(t, s, orderType, otherType))
	rev, _ = cursorOf(t, s, scope)
	require.Equal(t, later, rev, "continues after the snapshot's revision")
}

func TestSyncCapabilityOffChangesNothing(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	f.upsert(orderType, "o1", "viewer", "user:u_a")
	f.set(func(f *fakeProvider) { f.off = true })
	p := newProjection(t, s, f)
	require.ErrorIs(t, p.Sync(within(t, 10*time.Second)), ErrCapabilityUnavailable)
	require.Empty(t, projected(t, s, orderType))

	inactive := newProjection(t, s, f, func(c *Config) { c.Active = func() bool { return false } })
	n := len(f.requestLog())
	require.NoError(t, inactive.Sync(within(t, 10*time.Second)))
	require.Len(t, f.requestLog(), n, "an inactive projection never calls the provider")
}

func TestSyncTimesOutAFetch(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	f.set(func(f *fakeProvider) { f.delay = 2 * time.Second })
	p := newProjection(t, s, f, func(c *Config) { c.Timeout = 100 * time.Millisecond })
	start := time.Now()
	require.Error(t, p.Sync(within(t, 10*time.Second)))
	require.Less(t, time.Since(start), time.Second)
}

func TestConcurrentReplicasConverge(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	a, b := newProjection(t, s, f), newProjection(t, s, f)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for _, p := range []*Projection{a, b} {
		wg.Add(1)
		go func(p *Projection) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = p.Sync(context.Background())
				}
			}
		}(p)
	}
	for i := range 300 {
		id := fmt.Sprintf("o%d", i%7)
		if i%3 == 2 {
			f.remove(orderType, id, "viewer", "user:u_a")
		} else {
			f.upsert(orderType, id, "viewer", "user:u_a")
		}
	}
	close(stop)
	wg.Wait()
	require.NoError(t, a.Sync(within(t, 10*time.Second)))
	require.Equal(t, f.stateOf(orderType, otherType), projected(t, s, orderType, otherType))
}

// A replica whose page was read before another replica applied newer changes never undoes them.
func TestStalePageNeverUndoesNewerChanges(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	slow := newProjection(t, s, f, func(c *Config) { c.Caller = "slow" })
	fast := newProjection(t, s, f, func(c *Config) { c.Caller = "fast" })
	f.set(func(f *fakeProvider) { f.lag = map[string]time.Duration{"slow": 500 * time.Millisecond} })
	f.upsert(orderType, "o1", "viewer", "user:u_a")
	done := make(chan error, 1)
	go func() { done <- slow.Sync(context.Background()) }()
	require.Eventually(t, func() bool { return len(f.requestLog()) == 1 }, 2*time.Second, 5*time.Millisecond)
	head := f.remove(orderType, "o1", "viewer", "user:u_a")
	require.NoError(t, fast.Sync(within(t, 5*time.Second)))
	require.Empty(t, projected(t, s, orderType))
	require.NoError(t, <-done)
	require.Empty(t, projected(t, s, orderType), "the stale upsert at revision 1 is skipped")
	rev, _ := cursorOf(t, s, scope)
	require.Equal(t, head, rev, "the cursor never goes back")
}

func TestRunPullsOnPoke(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	p := newProjection(t, s, f, func(c *Config) { c.Interval = time.Hour })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	require.Eventually(t, func() bool { return len(f.requestLog()) >= 1 }, 5*time.Second, 10*time.Millisecond, "a first pull at start")
	rev := f.upsert(orderType, "o1", "viewer", "user:u_a")
	p.Poke()
	require.Eventually(t, func() bool { return p.Watermark() == rev }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{orderType + "|o1|viewer|user:u_a"}, projected(t, s, orderType))
}

func TestWaitForConsistencyToken(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	p := newProjection(t, s, f)
	require.True(t, p.WaitFor(within(t, time.Second), 0))
	rev := f.upsert(orderType, "o1", "viewer", "user:u_a")
	require.True(t, p.WaitFor(within(t, 5*time.Second), rev), "behind: one synchronous pull catches up (P6.11)")
	require.Equal(t, rev, p.Watermark())

	other := newProjection(t, s, f)
	require.True(t, other.WaitFor(within(t, 5*time.Second), rev), "another replica's progress counts")

	f.set(func(f *fakeProvider) { f.delay = time.Second })
	next := f.upsert(orderType, "o2", "viewer", "user:u_a")
	start := time.Now()
	require.False(t, p.WaitFor(within(t, 5*time.Second), next), "still behind after the 300 ms budget")
	require.Less(t, time.Since(start), 800*time.Millisecond)
	require.False(t, p.WaitFor(within(t, 5*time.Second), next+100))
}

func TestLoadManyGroupsRowsByRecord(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	f.upsert(orderType, "o1", "viewer", "user:u_a")
	f.upsert(orderType, "o1", "editor", "role:r")
	f.upsert(orderType, "o2", "viewer", "user:u_b")
	f.upsert(otherType, "o1", "viewer", "user:u_c")
	require.NoError(t, newProjection(t, s, f).Sync(within(t, 10*time.Second)))
	var got map[string][]authz.ACLRow
	require.NoError(t, s.Run(within(t, 10*time.Second), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		var err error
		got, err = LoadMany(ctx, tx, orderType, []string{"o1", "o2", "o3"})
		return err
	}))
	require.Equal(t, map[string][]authz.ACLRow{
		"o1": {{RType: orderType, RID: "o1", Relation: "editor", Subject: "role:r"}, {RType: orderType, RID: "o1", Relation: "viewer", Subject: "user:u_a"}},
		"o2": {{RType: orderType, RID: "o2", Relation: "viewer", Subject: "user:u_b"}},
	}, got)
}

// A _shares write waits for its own revision with a budget of its choosing (P6.11).
func TestWaitForWithinTakesTheCallersBudget(t *testing.T) {
	s, f := newStore(t), newFakeProvider(t)
	p := newProjection(t, s, f)
	f.set(func(f *fakeProvider) { f.delay = 500 * time.Millisecond })
	rev := f.upsert(orderType, "o1", "viewer", "user:u_a")
	require.False(t, p.WaitFor(within(t, 5*time.Second), rev))
	require.True(t, p.WaitForWithin(within(t, 5*time.Second), rev, 3*time.Second))
}
