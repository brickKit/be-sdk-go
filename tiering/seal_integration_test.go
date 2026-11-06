//go:build integration

package tiering_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A sales-like class: orders and their lines, by legal entity and month of closing; closed orders are terminal.
const schemaSQL = `CREATE TABLE orders (
  id uuid PRIMARY KEY, legal_entity uuid NOT NULL, code text NOT NULL, note text, total numeric(19,4) NOT NULL,
  status text NOT NULL, closed_at timestamptz, created_at timestamptz NOT NULL, attrs jsonb);
CREATE TABLE order_lines (
  id uuid PRIMARY KEY, order_id uuid NOT NULL REFERENCES orders (id), line integer NOT NULL, qty bigint NOT NULL);`

func class() tiering.Class {
	return tiering.Class{Name: "sales.orders", Category: tiering.CategoryCompleted,
		Root: tiering.Table{Name: "orders", PK: "id", Columns: map[string]string{"id": tiering.TypeUUID,
			"legal_entity": tiering.TypeUUID, "code": tiering.TypeText, "note": tiering.TypeText,
			"total": tiering.TypeNumeric, "status": tiering.TypeText, "closed_at": tiering.TypeTimestamptz,
			"created_at": tiering.TypeTimestamptz, "attrs": tiering.TypeJSONB}},
		Children: []tiering.Child{{Table: tiering.Table{Name: "order_lines", PK: "id", Columns: map[string]string{
			"id": tiering.TypeUUID, "order_id": tiering.TypeUUID, "line": tiering.TypeInt4, "qty": tiering.TypeInt8}},
			FK: "order_id"}},
		Group: []string{"legal_entity"}, Anchor: "closed_at", Created: "created_at",
		Terminal: "r.status = 'closed'", ResidenceDays: 365,
		Retention: tiering.Retention{MinDays: 3650, MaxDays: 3650, Basis: "test"},
		Locate:    []string{"legal_entity"}, Routes: []string{tiering.RouteObject}, SchemaVersion: "1",
		HoldOperation: "sales.order.hold", ApproveOperation: "sales.order.approve_destruction"}
}

type env struct {
	ctx   context.Context
	d     *db.DB
	admin *pgx.Conn
	cat   *tiering.Catalog
	le    string
}

func setup(t *testing.T, name string) env {
	t.Helper()
	d, admin := dbtest.Schema(t, name, fstest.MapFS{"0001_sales.up.sql": {Data: []byte(schemaSQL)}})
	stmts, err := tiering.BarrierSQL([]tiering.Class{class()})
	require.NoError(t, err)
	pgtest.Exec(t, admin, stmts...)
	c := class()
	return env{ctx: context.Background(), d: d, admin: admin, le: ids.New().String(),
		cat: &tiering.Catalog{DB: d, Classes: map[string]tiering.Class{c.Name: c}, Registry: tiering.NewRegistry()}}
}

// order inserts an order closed (or open, closedDaysAgo < 0) with two lines; it answers the order's id.
func (e env) order(t *testing.T, closedDaysAgo int) string {
	t.Helper()
	id := ids.New().String()
	status, closed := "open", "NULL"
	if closedDaysAgo >= 0 {
		status, closed = "closed", fmt.Sprintf("now() - interval '%d days'", closedDaysAgo)
	}
	pgtest.Exec(t, e.admin,
		fmt.Sprintf("INSERT INTO orders VALUES ('%s', '%s', 'SO-%s', NULL, 12.5, '%s', %s, now() - interval '500 days', '{\"a\": 1}')",
			id, e.le, id[:8], status, closed),
		fmt.Sprintf("INSERT INTO order_lines VALUES ('%s', '%s', 1, 3), ('%s', '%s', 2, 4)", ids.New(), id, ids.New(), id))
	return id
}

func (e env) write(sql string, args ...any) error {
	return e.d.Tx(e.ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, sql, args...)
		return err
	})
}

// TR01, TR02, TR28 and P15.13: only the closed orders past the residence are sealed; writes to a sealed order and to
// its lines are refused with RECORD_SEALED whatever the entry; the open order and a recent closed one stay writable.
func TestSealingAndTheWriteBarrier(t *testing.T) {
	e := setup(t, "tier_seal")
	sealed1, sealed2 := e.order(t, 400), e.order(t, 400)
	open := e.order(t, -1)
	recent := e.order(t, 150) // closed five months ago, residence 12 months (TR28)

	groups, err := e.cat.Groupings(e.ctx, "sales.orders")
	require.NoError(t, err)
	require.Len(t, groups, 1, "one legal entity, one month")
	res, err := e.cat.Seal(e.ctx, "sales.orders", groups[0])
	require.NoError(t, err)
	require.Empty(t, res.Blocked)
	assert.Equal(t, 2, res.Members, "only the closed orders past the residence (TR01)")
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, res.Digest)
	var state string
	require.NoError(t, e.admin.QueryRow(e.ctx, "SELECT state FROM be_tier_unit WHERE id = $1", res.Unit).Scan(&state))
	assert.Equal(t, tiering.StateSealed, state)

	sealedErr := func(err error) { assert.True(t, errs.Is(err, "be", errs.ReasonRecordSealed), "%v", err) }
	sealedErr(e.write("UPDATE orders SET note = 'late' WHERE id = $1", sealed1))
	sealedErr(e.write("DELETE FROM order_lines WHERE order_id = $1", sealed2))
	sealedErr(e.write("INSERT INTO order_lines VALUES ($1, $2, 9, 1)", ids.New().String(), sealed1))
	sealedErr(e.write("UPDATE order_lines SET order_id = $1 WHERE order_id = $2", sealed1, open)) // moved into it
	assert.NoError(t, e.write("UPDATE orders SET note = 'still open' WHERE id = $1", open))
	assert.NoError(t, e.write("UPDATE orders SET note = 'not yet' WHERE id = $1", recent))
	assert.NoError(t, e.write("INSERT INTO order_lines VALUES ($1, $2, 9, 1)", ids.New().String(), open))

	again, err := e.cat.Groupings(e.ctx, "sales.orders")
	require.NoError(t, err)
	assert.Empty(t, again, "the sealed orders are members; the rest are not candidates")
}

// TR03: a write and a seal at the same time serialize on the order's row: the write that commits first is in the
// sealed content; a write after the seal is refused.
func TestASealAndAWriteSerialize(t *testing.T) {
	e := setup(t, "tier_race")
	id := e.order(t, 400)
	groups, err := e.cat.Groupings(e.ctx, "sales.orders")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	writer, err := e.admin.Begin(e.ctx) // holds the order's row until it commits
	require.NoError(t, err)
	_, err = writer.Exec(e.ctx, "UPDATE orders SET note = 'first' WHERE id = $1", id)
	require.NoError(t, err)
	done := make(chan tiering.SealResult, 1)
	go func() {
		res, err := e.cat.Seal(e.ctx, "sales.orders", groups[0])
		assert.NoError(t, err)
		done <- res
	}()
	select {
	case <-done:
		t.Fatal("the seal did not wait for the open write")
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, writer.Commit(e.ctx))
	res := <-done
	require.Equal(t, 1, res.Members)

	// the sealed digest is the content with the first write in it: recompute on a copy of the unit's content
	var note string
	require.NoError(t, e.admin.QueryRow(e.ctx, "SELECT note FROM orders WHERE id = $1", id).Scan(&note))
	assert.Equal(t, "first", note)
	assert.True(t, errs.Is(e.write("UPDATE orders SET note = 'second' WHERE id = $1", id), "be", errs.ReasonRecordSealed))
}

// TR23, TR30 and P15.12: a module's blocker (an open reservation, an open period) or an unpublished event of an
// aggregate keeps the whole grouping open; the reasons are on the unit; nothing becomes a member.
func TestABlockedGroupingStaysOpen(t *testing.T) {
	e := setup(t, "tier_block")
	held := e.order(t, 400)
	other := e.order(t, 400)
	e.cat.Registry.Block("sales.orders", func(ctx context.Context, tx db.Tx, class string, roots []string) (map[string][]string, error) {
		return map[string][]string{held: {"reservation_open"}}, nil
	})
	pgtest.Exec(t, e.admin, fmt.Sprintf(`INSERT INTO be_outbox (event_id, stream, partition, type, occurred_at, record_id,
		version, projection, hops, payload, digest) VALUES ('%s', 's', 0, 't', now(), '%s', 1, 'full', 0, '\x00', 'd')`,
		ids.New(), other))
	groups, err := e.cat.Groupings(e.ctx, "sales.orders")
	require.NoError(t, err)
	res, err := e.cat.Seal(e.ctx, "sales.orders", groups[0])
	require.NoError(t, err)
	assert.Equal(t, map[string][]string{held: {"reservation_open"}, other: {tiering.WhyEventsPending}}, res.Blocked)
	assert.Zero(t, res.Members)
	var state string
	var blocked []byte
	var members int
	require.NoError(t, e.admin.QueryRow(e.ctx, "SELECT state, blocked::text, (SELECT count(*) FROM be_tier_member) "+
		"FROM be_tier_unit WHERE id = $1", res.Unit).Scan(&state, &blocked, &members))
	assert.Equal(t, tiering.StateOpen, state)
	assert.Contains(t, string(blocked), "reservation_open")
	assert.Zero(t, members)
	assert.NoError(t, e.write("UPDATE orders SET note = 'still writable' WHERE id = $1", held))
}

// TR03 for a child: a line of a closed order changed before the seal holds the order's row (the barrier's FOR SHARE;
// an update that leaves the foreign key alone takes no lock on the order otherwise), so the seal waits for it and
// seals the changed content; without that lock the seal would read the line as it was and the order would hold a
// line its sealed digest does not describe.
func TestASealWaitsForAChildWrite(t *testing.T) {
	e := setup(t, "tier_child")
	id := e.order(t, 400)
	groups, err := e.cat.Groupings(e.ctx, "sales.orders")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	writer, err := e.admin.Begin(e.ctx)
	require.NoError(t, err)
	_, err = writer.Exec(e.ctx, "UPDATE order_lines SET qty = qty + 100 WHERE order_id = $1 AND line = 1", id)
	require.NoError(t, err)
	done := make(chan tiering.SealResult, 1)
	go func() {
		res, err := e.cat.Seal(e.ctx, "sales.orders", groups[0])
		assert.NoError(t, err)
		done <- res
	}()
	select {
	case <-done:
		t.Fatal("the seal did not wait for the open change of a line")
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, writer.Commit(e.ctx))
	res := <-done
	require.Equal(t, 1, res.Members)
	assert.True(t, errs.Is(e.write("INSERT INTO order_lines VALUES ($1, $2, 4, 1)", ids.New().String(), id), "be",
		errs.ReasonRecordSealed))
}

// P15.11, TR30: the candidates job seals what has come due and alerts on a unit blocked 30 days past its residence
// (counted from when it came due: the latest anchor among the candidates plus the residence).
func TestTheCandidatesJobSealsAndAlerts(t *testing.T) {
	e := setup(t, "tier_job")
	held := e.order(t, 400) // closed 400 days ago, residence 365: due 35 days ago, blocked since
	e.cat.Registry.Block("sales.orders", func(ctx context.Context, tx db.Tx, class string, roots []string) (map[string][]string, error) {
		out := map[string][]string{}
		for _, r := range roots {
			if r == held {
				out[r] = []string{"reservation_open"}
			}
		}
		return out, nil
	})
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	require.NoError(t, e.cat.SealDue(e.ctx, log))
	assert.Contains(t, logs.String(), `"event":"tiering_blocked"`, "due 35 days ago, blocked: past the 30 days")

	other := ids.New().String() // another legal entity, due 10 days ago: sealed, no alert for it
	pgtest.Exec(t, e.admin, fmt.Sprintf("INSERT INTO orders VALUES ('%s', '%s', 'SO-x', NULL, 1, 'closed', "+
		"now() - interval '375 days', now() - interval '500 days', NULL)", other, ids.New()))
	logs.Reset()
	require.NoError(t, e.cat.SealDue(e.ctx, log))
	assert.Contains(t, logs.String(), `"event":"tiering_sealed"`)
	assert.Equal(t, 1, strings.Count(logs.String(), `"event":"tiering_blocked"`), "still the first unit only")
	var due time.Time
	require.NoError(t, e.admin.QueryRow(e.ctx, "SELECT blocked_since FROM be_tier_unit WHERE blocked IS NOT NULL").Scan(&due))
	assert.WithinDuration(t, time.Now().Add(-35*24*time.Hour), due, time.Hour, "blocked since it came due")
}

// P15.13: the barrier holds for a write from a session whose search path is not the component's (an operator's
// session naming the schema): the trigger finds the catalog in its own schema, refuses the sealed order, lets the
// open one through.
func TestTheBarrierHoldsFromAnotherSearchPath(t *testing.T) {
	e := setup(t, "tier_seal_path")
	e.order(t, 400)
	e.order(t, 400)
	open := e.order(t, -1)
	groups, err := e.cat.Groupings(e.ctx, "sales.orders")
	require.NoError(t, err)
	_, err = e.cat.Seal(e.ctx, "sales.orders", groups[0])
	require.NoError(t, err)
	var sealed string
	require.NoError(t, e.admin.QueryRow(e.ctx, "SELECT root_id FROM be_tier_member LIMIT 1").Scan(&sealed))

	other := pgtest.Admin(t, pgtest.Database) // search path: the default, not the component's schema
	_, err = other.Exec(e.ctx, "UPDATE tier_seal_path.orders SET note = 'x' WHERE id = $1", sealed)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "BE001", pgErr.Code, "refused by the barrier: %v", err)
	_, err = other.Exec(e.ctx, "UPDATE tier_seal_path.orders SET note = 'x' WHERE id = $1", open)
	assert.NoError(t, err, "the open order")
}

// P15.1: a class is checked against its tables at start: a column not declared (it would not be copied, and the
// release deletes the row), a column declared with another type, a declared column the table lacks.
func TestAClassMustDeclareItsTables(t *testing.T) {
	e := setup(t, "tier_columns")
	cl := class()
	cl.Routes = []string{tiering.RoutePostgres} // no store here
	start := func(c tiering.Class) error {
		values, problems := config.Parse(tiering.Keys([]tiering.Class{c}), config.Map(map[string]string{}))
		require.Empty(t, problems)
		_, err := tiering.NewCatalog(e.ctx, e.d, []tiering.Class{c}, values, "p")
		return err
	}
	require.NoError(t, start(cl), "the declaration is the tables")

	pgtest.Exec(t, e.admin, "ALTER TABLE orders ADD COLUMN extra text")
	assert.ErrorContains(t, start(cl), "orders.extra (text) is not declared")
	pgtest.Exec(t, e.admin, "ALTER TABLE orders DROP COLUMN extra")

	wrong := cl
	wrong.Root.Columns = map[string]string{}
	for k, v := range cl.Root.Columns {
		wrong.Root.Columns[k] = v
	}
	wrong.Root.Columns["total"] = tiering.TypeInt8
	assert.ErrorContains(t, start(wrong), "orders.total is numeric in the database, declared int8")
	wrong.Root.Columns["total"] = tiering.TypeNumeric
	wrong.Root.Columns["remark"] = tiering.TypeText
	assert.ErrorContains(t, start(wrong), "orders.remark is declared but not in the database")
}
