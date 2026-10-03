package authz

import (
	"context"
	"database/sql"
	"io/fs"
	"slices"
	"testing"
	"time"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

const listComponent = "conformance/widget"

// itemsDDL is the component's own table: every resource type of the list tests reads it through the
// columns its dimensions need.
const itemsDDL = `CREATE TABLE items (id text PRIMARY KEY, owner_id text, dept_path text, warehouse_id text, legal_entity text)`

var itemColumns = Columns{Alias: "o", ID: "id", Owner: "owner_id", DeptPath: "dept_path",
	Dims: map[string]string{"warehouse": "warehouse_id", "legal_entity": "legal_entity"}}

// dbEnv is a fresh schema with the projection tables (the pinned be-protocol's ddl/07) and items, both
// created by the owner.
type dbEnv struct {
	id    testpg.Identity
	store *pg.Store
}

func newDBEnv(t *testing.T) *dbEnv {
	t.Helper()
	id := testpg.New(t)
	ddl, err := fs.ReadFile(beprotocol.FS, "ddl/07-authz-projection.sql")
	require.NoError(t, err)
	owner := testpg.Open(t, id.DSN(id.Owner, id.OwnerPassword))
	_, err = owner.Exec(`SET search_path TO "` + id.Schema + `"; ` + string(ddl) + "; " + itemsDDL)
	require.NoError(t, err)
	p, err := pg.OpenPool(pg.PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: id.User,
		Password: func() string { return id.Password }, SSLMode: "disable", MaxConns: 4})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return &dbEnv{id: id, store: pg.NewStore(p, pg.StoreConfig{ComponentID: listComponent, Role: id.User, Schema: id.Schema, Budget: 4})}
}

// dbRow is a row of items; nil pointers are NULL.
type dbRow struct {
	id                        string
	owner, dept, wh, legalEnt *string
}

func (r dbRow) row() Row {
	return Row{ID: r.id, Owner: deref(r.owner), DeptPath: deref(r.dept),
		Values: map[string]string{"warehouse": deref(r.wh), "legal_entity": deref(r.legalEnt)}}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func sp(s string) *string { return &s }

// reset replaces the items and the projection.
func (e *dbEnv) reset(t require.TestingT, rows []dbRow, acl []ACLRow) {
	err := e.store.Run(context.Background(), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM items`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM besdk_authz_acl`); err != nil {
			return err
		}
		for _, r := range rows {
			if _, err := tx.ExecContext(ctx, `INSERT INTO items VALUES ($1, $2, $3, $4, $5)`,
				r.id, r.owner, r.dept, r.wh, r.legalEnt); err != nil {
				return err
			}
		}
		for _, a := range acl {
			if _, err := tx.ExecContext(ctx, `INSERT INTO besdk_authz_acl (rtype, rid, relation, subject, expires_at, revision)
				VALUES ($1, $2, $3, $4, $5, 1) ON CONFLICT DO NOTHING`, a.RType, a.RID, a.Relation, a.Subject, a.ExpiresAt); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// list runs SELECT o.id FROM items o WHERE <predicate> and returns the ids, sorted.
func (e *dbEnv) list(t require.TestingT, p *Predicate, params ScopeParams, now time.Time) []string {
	q, args := p.SQL(params, now, 1)
	return e.ids(t, `SELECT o.id FROM items o WHERE `+q, args)
}

// listBranches runs the three disjoint branches as one UNION ALL and returns the ids, sorted, with
// duplicates kept.
func (e *dbEnv) listBranches(t require.TestingT, p *Predicate, params ScopeParams, now time.Time) []string {
	out := []string{}
	for _, f := range p.Branches(params, now, 1) {
		out = append(out, e.ids(t, `SELECT o.id FROM items o WHERE `+f.SQL, f.Args)...)
	}
	slices.Sort(out)
	return out
}

func (e *dbEnv) ids(t require.TestingT, q string, args []any) []string {
	out := []string{}
	err := e.store.Run(context.Background(), pg.TxOptions{ReadOnly: true}, func(ctx context.Context, tx *pg.Tx) error {
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer func(rows *sql.Rows) { _ = rows.Close() }(rows)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			out = append(out, id)
		}
		return rows.Err()
	})
	require.NoError(t, err, q)
	slices.Sort(out)
	return out
}

// visible is the single-record side of P6.7: the ids whose vis(K, row) holds, sorted.
func visibleIDs(e *Evaluator, rt *ResourceType, ka KeyAccess, rows []dbRow, acl []ACLRow) []string {
	out := []string{}
	for _, r := range rows {
		if e.Vis(rt, ka, r.row(), acl) {
			out = append(out, r.id)
		}
	}
	slices.Sort(out)
	return out
}
