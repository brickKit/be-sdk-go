package tiering

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/jackc/pgx/v5"
)

// Reads (be-protocol P15.22-P15.25, P15.27): by a root's key through the locate index, and lists under a catalog
// snapshot that reads each unit once by its route at the snapshot. What leaves the online tables is read back as
// canonical rows (P15.8: text, decimals and instants as strings, nulls absent), so every route answers the same rows.

// Aggregate is one aggregate read from a route: its root and its children by table, canonical rows.
type Aggregate struct {
	Root     map[string]any
	Children map[string][]map[string]any
	Unit     string
	Route    string
}

// Visible decides a cold aggregate under the current authorization (P15.26): whether its root is visible, and the
// fields readable (the caller builds the facts from the root's columns and asks authz.Subject.Check). nil: all.
type Visible func(root map[string]any) (bool, []string, error)

// LeaseTTL is a snapshot's lease, renewed while the read goes on (protocol 11: 15 minutes).
const LeaseTTL = 15 * time.Minute

// unitRoute is a unit's route and manifest as a read needs them.
type unitRoute struct {
	id, class, state, route string
	version                 int
	routeVersion            int64
	thawVersion             int
}

func (c *Catalog) routeOf(ctx context.Context, tx db.Tx, unit string) (unitRoute, error) {
	u := unitRoute{id: unit}
	var rv *int64
	var thaw *int
	err := tx.QueryRow(ctx, "SELECT u.class, u.state, u.route, u.manifest_version, u.route_version, t.version "+
		"FROM be_tier_unit u LEFT JOIN be_tier_thaw t ON t.unit = u.id AND t.expires_at > now() WHERE u.id = $1", unit).
		Scan(&u.class, &u.state, &u.route, &u.version, &rv, &thaw)
	if rv != nil {
		u.routeVersion = *rv
	}
	if thaw != nil {
		u.thawVersion = *thaw
	}
	return u, err
}

// Get reads one aggregate by its root's key (P15.22). found=false: it has not left the online tables (or does not
// exist): the caller reads its online tables as usual.
func (c *Catalog) Get(ctx context.Context, class, root string) (Aggregate, bool, error) {
	var u unitRoute
	err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		var unit string
		err := tx.QueryRow(ctx, "SELECT unit::text FROM be_tier_member WHERE class = $1 AND root_id = $2", class, root).Scan(&unit)
		if err != nil {
			return err
		}
		u, err = c.routeOf(ctx, tx, unit)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (u.route == RouteOnline || u.state == StateDestroyed)) {
		return Aggregate{}, false, nil
	}
	if err != nil {
		return Aggregate{}, false, err
	}
	aggs, err := c.readUnitRoute(ctx, u, root)
	if err != nil {
		return Aggregate{}, false, err
	}
	if len(aggs) == 0 {
		return Aggregate{}, false, c.unavailable(u, fmt.Errorf("the root %s is not in its unit's copy", root))
	}
	return aggs[0], true, nil
}

// unavailable is a route that cannot answer for a unit the catalog says is there: never "not found" (P15.22, TR09).
func (c *Catalog) unavailable(u unitRoute, cause error) error {
	dep := "blob"
	if u.route == RoutePostgres {
		dep = "db"
	}
	return errs.New(errs.ReasonDependencyUnavailable, "an archived record cannot be read now").With("dependency", dep).
		With("unit", u.id).Wrap(cause)
}

// readUnitRoute reads a unit's aggregates (only one root when root is not empty) from its route, or its thawed copy.
func (c *Catalog) readUnitRoute(ctx context.Context, u unitRoute, root string) ([]Aggregate, error) {
	cl := c.Classes[u.class]
	tables := map[string][]map[string]any{}
	route, version := u.route, u.version
	if u.thawVersion != 0 {
		route, version = RoutePostgres, u.thawVersion
	}
	switch route {
	case RoutePostgres:
		err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
			rows, err := tx.Query(ctx, "SELECT tbl, row FROM be_tier_row WHERE unit = $1 AND version = $2", u.id, version)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var tbl string
				var raw []byte
				if err := rows.Scan(&tbl, &raw); err != nil {
					return err
				}
				var obj map[string]any
				if err := json.Unmarshal(raw, &obj); err != nil {
					return err
				}
				tables[tbl] = append(tables[tbl], obj)
			}
			return rows.Err()
		})
		if err != nil {
			return nil, err
		}
		if len(tables) == 0 {
			return nil, c.unavailable(u, errors.New("no warm rows"))
		}
	case RouteObject:
		if c.Store == nil {
			return nil, c.unavailable(u, errors.New("no object store configured"))
		}
		var raw []byte
		if err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT manifest FROM be_tier_manifest WHERE unit = $1 AND version = $2", u.id, version).Scan(&raw)
		}); err != nil {
			return nil, err
		}
		var m Manifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		for _, mt := range m.Tables {
			t, _, ok := tableOf(cl, mt.Name)
			if !ok {
				return nil, c.unavailable(u, fmt.Errorf("the manifest names %s", mt.Name))
			}
			for _, o := range mt.Objects {
				data, err := c.Store.Get(ctx, o.Key)
				if err != nil || sha(data) != o.SHA256 {
					if err == nil {
						err = errors.New("the object is not the manifest's")
					}
					return nil, c.unavailable(u, err)
				}
				rows, err := ReadParquet(t, data)
				if err != nil {
					return nil, c.unavailable(u, err)
				}
				for _, r := range rows {
					b, err := Row(t.Columns, r)
					if err != nil {
						return nil, err
					}
					var obj map[string]any
					if err := json.Unmarshal(b, &obj); err != nil {
						return nil, err
					}
					tables[t.Name] = append(tables[t.Name], obj)
				}
			}
		}
	default:
		return nil, fmt.Errorf("tiering: unit %s has no route to read", u.id)
	}
	return assemble(cl, tables, u, root)
}

// assemble groups a unit's canonical rows into aggregates by the root's key.
func assemble(cl Class, tables map[string][]map[string]any, u unitRoute, root string) ([]Aggregate, error) {
	key := func(t Table, col string, r map[string]any) (string, error) {
		v, err := Value(t.Columns[col], r[col])
		return fmt.Sprint(v), err
	}
	want := ""
	if root != "" {
		v, err := Value(cl.Root.Columns[cl.Root.PK], root)
		if err != nil {
			return nil, err
		}
		want = fmt.Sprint(v)
	}
	var out []Aggregate
	index := map[string]int{}
	for _, r := range tables[cl.Root.Name] {
		k, err := key(cl.Root, cl.Root.PK, r)
		if err != nil {
			return nil, err
		}
		if want != "" && k != want {
			continue
		}
		index[k] = len(out)
		out = append(out, Aggregate{Root: r, Children: map[string][]map[string]any{}, Unit: u.id, Route: u.route})
	}
	for _, ch := range cl.Children {
		for _, r := range tables[ch.Name] {
			k, err := key(ch.Table, ch.FK, r)
			if err != nil {
				return nil, err
			}
			if i, ok := index[k]; ok {
				out[i].Children[ch.Name] = append(out[i].Children[ch.Name], r)
			}
		}
	}
	return out, nil
}

// Snapshot is a read under one catalog version with its lease (P15.24, P15.25).
type Snapshot struct {
	Version int64
	Lease   string
}

// Snapshot registers a lease at the current catalog version: a unit published after it is not released while the
// lease lives, so the read finds its rows online.
func (c *Catalog) Snapshot(ctx context.Context, purpose string) (Snapshot, error) {
	s := Snapshot{Lease: ids.New().String()}
	err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT version FROM be_tier_catalog").Scan(&s.Version); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO be_tier_lease (id, catalog_version, purpose, expires_at) VALUES "+
			"($1, $2, $3, now() + make_interval(secs => $4))", s.Lease, s.Version, purpose, LeaseTTL.Seconds())
		return err
	})
	return s, err
}

// Renew extends a snapshot's lease; QUERY_PLAN_CHANGED when it ran out (what it read under may be gone).
func (c *Catalog) Renew(ctx context.Context, s Snapshot) error {
	return c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE be_tier_lease SET expires_at = now() + make_interval(secs => $2) WHERE id = $1 "+
			"AND expires_at > now()", s.Lease, LeaseTTL.Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return errs.New(errs.ReasonQueryPlanChanged, "the archive snapshot of this read has expired; start again")
		}
		return nil
	})
}

// End ends a snapshot's lease.
func (c *Catalog) End(ctx context.Context, s Snapshot) error {
	return c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "DELETE FROM be_tier_lease WHERE id = $1", s.Lease)
		return err
	})
}

// OnlineExclusion is the condition an online query over a class's root (alias) adds under a snapshot: leave out the
// aggregates whose unit had left the online tables at the snapshot's version; they are read from their route.
func (s Snapshot) OnlineExclusion(cl Class, alias string) string {
	return fmt.Sprintf("NOT EXISTS (SELECT 1 FROM be_tier_member m JOIN be_tier_unit u ON u.id = m.unit WHERE m.class = '%s' "+
		"AND m.root_id = %s.%s::text AND u.route <> 'online' AND u.route_version <= %d)",
		strings.ReplaceAll(cl.Name, "'", "''"), alias, ident(cl.Root.PK), s.Version)
}

// ColdUnits are a class's units the snapshot reads from their routes: off the online tables at its version.
func (c *Catalog) ColdUnits(ctx context.Context, s Snapshot, class string) ([]string, error) {
	var out []string
	err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT id::text FROM be_tier_unit WHERE class = $1 AND route <> 'online' AND "+
			"route_version <= $2 AND state IN ('published', 'released') ORDER BY id", class, s.Version)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return out, err
}

// ReadUnit reads a cold unit's aggregates under a snapshot, those the caller may see, their fields masked to the
// readable ones (P15.26).
func (c *Catalog) ReadUnit(ctx context.Context, s Snapshot, unit string, visible Visible) ([]Aggregate, error) {
	if err := c.Renew(ctx, s); err != nil {
		return nil, err
	}
	var u unitRoute
	if err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) (err error) {
		u, err = c.routeOf(ctx, tx, unit)
		return err
	}); err != nil {
		return nil, err
	}
	aggs, err := c.readUnitRoute(ctx, u, "")
	if err != nil || visible == nil {
		return aggs, err
	}
	out := aggs[:0]
	for _, a := range aggs {
		ok, fields, err := visible(a.Root)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		readable := map[string]bool{}
		for _, f := range fields {
			readable[f] = true
		}
		for k := range a.Root {
			if !readable[k] {
				delete(a.Root, k)
			}
		}
		out = append(out, a)
	}
	return out, nil
}

// Thaw copies a unit's rows to a read-only area for ttl (P15.27): reads by key prefer it; nothing reopens writes.
func (c *Catalog) Thaw(ctx context.Context, unit string, ttl time.Duration) error {
	var u unitRoute
	if err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) (err error) {
		u, err = c.routeOf(ctx, tx, unit)
		return err
	}); err != nil {
		return err
	}
	if u.route == RouteOnline {
		return fmt.Errorf("tiering: unit %s is online", unit)
	}
	u.thawVersion = 0
	aggs, err := c.readUnitRoute(ctx, u, "")
	if err != nil {
		return err
	}
	cl := c.Classes[u.class]
	const thawVersion = -1 // a version no manifest has
	return c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, "DELETE FROM be_tier_row WHERE unit = $1 AND version = $2", unit, thawVersion); err != nil {
			return err
		}
		put := func(t Table, r map[string]any) error {
			k, err := PK(t.Columns, []string{t.PK}, r)
			if err != nil {
				return err
			}
			b, err := Row(t.Columns, r)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "INSERT INTO be_tier_row (unit, version, tbl, pk, row) VALUES ($1, $2, $3, $4, $5)",
				unit, thawVersion, t.Name, string(k), b)
			return err
		}
		for _, a := range aggs {
			if err := put(cl.Root, a.Root); err != nil {
				return err
			}
			for _, ch := range cl.Children {
				for _, r := range a.Children[ch.Name] {
					if err := put(ch.Table, r); err != nil {
						return err
					}
				}
			}
		}
		_, err := tx.Exec(ctx, "INSERT INTO be_tier_thaw (unit, version, expires_at) VALUES ($1, $2, now() + make_interval(secs => $3)) "+
			"ON CONFLICT (unit) DO UPDATE SET version = EXCLUDED.version, expires_at = EXCLUDED.expires_at", unit, thawVersion, ttl.Seconds())
		return err
	})
}
