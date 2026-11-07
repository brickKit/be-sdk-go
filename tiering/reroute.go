package tiering

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/jackc/pgx/v5"
)

// Writing a published or released unit again (be-protocol P15.31, P15.33, P15.35, P15.39): its content read from its
// route (converted when its structure is older), changed or not, copied to a route as a new manifest version,
// verified in full and published with its state unchanged. An erasure changes an aggregate; a route change moves the
// unit to its class's first route. The old version's data goes once nothing can need it any more.

// Converter turns a canonical row of an older structure version of a class's table into the current one (P15.33).
type Converter func(table string, row map[string]any) (map[string]any, error)

// Convert registers the converter from one older structure version of a class.
func (c *Catalog) Convert(class, from string, f Converter) error {
	cl, ok := c.Classes[class]
	if !ok || from == "" || from == cl.SchemaVersion || f == nil {
		return fmt.Errorf("tiering: a converter of %s needs an older structure version than %s", class, cl.SchemaVersion)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.converters == nil {
		c.converters = map[string]Converter{}
	}
	c.converters[class+"@"+from] = f
	return nil
}

// convert turns a unit's canonical rows of an older structure into the class's, checking the result (P15.33): every
// row kept, its columns and types the class's, the roots' locate columns there. No converter: QUERY_NOT_COMPLETABLE.
func (c *Catalog) convert(cl Class, from string, tables map[string][]map[string]any) (map[string][]map[string]any, error) {
	c.mu.Lock()
	f := c.converters[cl.Name+"@"+from]
	c.mu.Unlock()
	if f == nil {
		return nil, errs.New(errs.ReasonQueryNotCompletable, fmt.Sprintf("%s has no converter from structure version %s",
			cl.Name, from)).With("schema_version", from)
	}
	out := map[string][]map[string]any{}
	for name, rows := range tables {
		t, _, ok := tableOf(cl, name)
		if !ok {
			return nil, fmt.Errorf("tiering: structure version %s of %s has the table %s the class no longer has", from, cl.Name, name)
		}
		for _, r := range rows {
			conv, err := f(name, r)
			if err != nil {
				return nil, fmt.Errorf("tiering: converting %s from %s: %w", name, from, err)
			}
			if conv == nil {
				return nil, fmt.Errorf("tiering: the converter of %s from %s dropped a row", name, from)
			}
			if _, err := FromCanonical(t, conv); err != nil {
				return nil, fmt.Errorf("tiering: converted %s from %s: %w", name, from, err)
			}
			if name == cl.Root.Name {
				for _, col := range append([]string{cl.Root.PK}, cl.Locate...) {
					if _, ok := conv[col]; !ok {
						return nil, fmt.Errorf("tiering: a converted %s from %s has no %s", name, from, col)
					}
				}
			}
			out[name] = append(out[name], conv)
		}
	}
	return out, nil
}

// manifestTable is a table as a manifest describes it (an older structure's columns and types).
func manifestTable(mt ManifestTable) Table {
	t := Table{Name: mt.Name, Columns: map[string]string{}}
	if len(mt.PK) == 1 {
		t.PK = mt.PK[0]
	} else {
		t.Key = mt.PK
	}
	for _, col := range mt.Columns {
		t.Columns[col.Name] = col.Type
	}
	return t
}

// rewrite writes a published or released unit again on a route ("" its own) as a new manifest version, its
// aggregates changed by edit (nil: as they are), with also in the publish's transaction.
func (c *Catalog) rewrite(ctx context.Context, unit, route string, edit func(*Aggregate),
	also func(context.Context, db.Tx, Claim) error) (Manifest, error) {
	claim, err := c.claim(ctx, unit, StatePublished, StateReleased)
	if err != nil {
		return Manifest{}, err
	}
	cl := c.Classes[claim.Class]
	var u unitRoute
	if err := c.tx(ctx, "rewrite.route", func(ctx context.Context, tx db.Tx) (err error) {
		u, err = c.routeOf(ctx, tx, unit)
		return err
	}); err != nil {
		return Manifest{}, err
	}
	u.thawVersion = 0 // the route's own copy, not a thawed one
	aggs, err := c.readUnitRoute(ctx, u, "")
	if err != nil {
		return Manifest{}, err
	}
	tables := map[string][]map[string]any{}
	var sums []TableSum
	add := func(t Table, row map[string]any) error {
		d, err := FromCanonical(t, row)
		if err == nil {
			tables[t.Name] = append(tables[t.Name], d)
		}
		return err
	}
	for i := range aggs {
		if edit != nil {
			edit(&aggs[i])
		}
		if err := add(cl.Root, aggs[i].Root); err != nil {
			return Manifest{}, err
		}
		for _, ch := range cl.Children {
			for _, r := range aggs[i].Children[ch.Name] {
				if err := add(ch.Table, r); err != nil {
					return Manifest{}, err
				}
			}
		}
	}
	for _, t := range cl.Tables() {
		var h TableHash
		for _, r := range tables[t.Name] {
			k, err := PK(t.Columns, t.KeyColumns(), r)
			if err != nil {
				return Manifest{}, err
			}
			b, err := Row(t.Columns, r)
			if err != nil {
				return Manifest{}, err
			}
			h.Add(k, b)
		}
		sums = append(sums, TableSum{Name: t.Name, Rows: h.Len(), SHA256: h.Sum()})
	}
	if claim.Digest, err = UnitDigest(cl.Name, cl.SchemaVersion, sums); err != nil {
		return Manifest{}, err
	}
	if route == "" {
		route = u.route
	}
	m, err := c.copyTables(ctx, claim, route, tables)
	if err != nil {
		return Manifest{}, err
	}
	if err := c.Verify(ctx, m); err != nil {
		return Manifest{}, err
	}
	var publishAlso func(context.Context, db.Tx) error
	if also != nil {
		publishAlso = func(ctx context.Context, tx db.Tx) error { return also(ctx, tx, claim) }
	}
	return m, c.publish(ctx, claim, m, publishAlso)
}

// Reroute moves a published or released unit to another route (P15.35, TR20, TR26): a new manifest version there,
// its state unchanged; the old route's data goes by ReleaseOld.
func (c *Catalog) Reroute(ctx context.Context, unit, route string) (Manifest, error) {
	return c.rewrite(ctx, unit, route, nil, nil)
}

// rerouteDue are the published and released units on another route than their class's first.
func (c *Catalog) rerouteDue(ctx context.Context) (map[string]string, error) {
	due := map[string]string{}
	err := c.tx(ctx, "reroute.due", func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT id::text, class, route FROM be_tier_unit WHERE state IN ('published', 'released')")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, class, route string
			if err := rows.Scan(&id, &class, &route); err != nil {
				return err
			}
			if cl, ok := c.Classes[class]; ok && len(cl.Routes) > 0 && cl.Routes[0] != route {
				due[id] = cl.Routes[0]
			}
		}
		return rows.Err()
	})
	return due, err
}

// Why an old version's data is kept for now (P15.39), besides WhyGrace, WhyLease and WhyNoBackup.
const WhyNothingOld = "nothing_old"

// ReleaseOld deletes the data of a unit's versions a newer one replaced (P15.39) once the newer one is past the grace,
// no lease from before its route changed is alive, and a backup finished after it; it answers why it did not.
func (c *Catalog) ReleaseOld(ctx context.Context, unit string) ([]string, error) {
	type old struct {
		version    int
		route      string
		generation int64
	}
	var olds []old
	var class string
	var why []string
	finished, haveBackup, err := c.latestBackup(ctx)
	if err != nil {
		return nil, err
	}
	err = c.tx(ctx, "releaseold.decide", func(ctx context.Context, tx db.Tx) error {
		var current int
		var routeVersion *int64
		var createdAt, now time.Time
		if err := tx.QueryRow(ctx, "SELECT u.class, u.manifest_version, u.route_version, m.created_at, now() FROM be_tier_unit u "+
			"JOIN be_tier_manifest m ON m.unit = u.id AND m.version = u.manifest_version WHERE u.id::text = $1 AND "+
			"u.state IN ('published', 'released')", unit).Scan(&class, &current, &routeVersion, &createdAt, &now); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, "SELECT version, route, generation FROM be_tier_manifest WHERE unit::text = $1 AND version < $2 "+
			"AND superseded_at IS NULL ORDER BY version", unit, current)
		if err != nil {
			return err
		}
		olds, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (old, error) {
			var o old
			return o, r.Scan(&o.version, &o.route, &o.generation)
		})
		if err != nil || len(olds) == 0 {
			return err
		}
		if createdAt.Add(c.Grace).After(now) {
			why = append(why, WhyGrace)
		}
		var leases int
		rv := int64(0)
		if routeVersion != nil {
			rv = *routeVersion
		}
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM be_tier_lease WHERE catalog_version < $1 AND expires_at > now()", rv).
			Scan(&leases); err != nil {
			return err
		}
		if leases > 0 {
			why = append(why, WhyLease)
		}
		if !haveBackup || !finished.After(createdAt) {
			why = append(why, WhyNoBackup)
		}
		return nil
	})
	switch {
	case err != nil:
		return nil, err
	case len(olds) == 0:
		return []string{WhyNothingOld}, nil
	case len(why) > 0:
		return why, nil
	}
	for _, o := range olds {
		if o.route == RouteObject {
			if c.Store == nil {
				return nil, errors.New("tiering: an old version on the object route and no store to delete it from")
			}
			objects, err := c.Store.List(ctx, fmt.Sprintf("%s/%s/%s/%d.%d/", c.Store.Prefix, class, unit, o.generation, o.version))
			if err != nil {
				return nil, err
			}
			for _, obj := range objects {
				if err := c.Store.Delete(ctx, obj.Key); err != nil {
					return nil, err
				}
				crash("releaseold.object/done")
			}
		}
		if err := c.tx(ctx, "releaseold.delete", func(ctx context.Context, tx db.Tx) error {
			if _, err := tx.Exec(ctx, "DELETE FROM be_tier_row WHERE unit::text = $1 AND version = $2", unit, o.version); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "UPDATE be_tier_manifest SET superseded_at = now() WHERE unit::text = $1 AND version = $2",
				unit, o.version)
			return err
		}); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// oldVersions are the units with a version whose data is not released yet.
func (c *Catalog) oldVersions(ctx context.Context) ([]string, error) {
	var out []string
	err := c.tx(ctx, "releaseold.list", func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT DISTINCT m.unit::text FROM be_tier_manifest m JOIN be_tier_unit u ON u.id = m.unit "+
			"WHERE m.version < u.manifest_version AND m.superseded_at IS NULL AND u.state IN ('published', 'released')")
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return out, err
}

// CheckReadable reads one unit of every older structure version a class still has (P15.34): what each fails with,
// by structure version; empty when every one reads. A component's tests run it before a new structure version ships.
func (c *Catalog) CheckReadable(ctx context.Context, class string) (map[string]error, error) {
	cl, ok := c.Classes[class]
	if !ok {
		return nil, fmt.Errorf("tiering: no class %q", class)
	}
	var samples [][2]string
	if err := c.tx(ctx, "readable.list", func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT DISTINCT ON (v) v, unit FROM (SELECT convert_from(m.manifest, 'UTF8')::jsonb ->> "+
			"'schema_version' AS v, u.id::text AS unit FROM be_tier_unit u JOIN be_tier_manifest m ON m.unit = u.id AND "+
			"m.version = u.manifest_version WHERE u.class = $1 AND u.state IN ('published', 'released')) s WHERE v <> $2 ORDER BY v, unit",
			class, cl.SchemaVersion)
		if err != nil {
			return err
		}
		samples, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) ([2]string, error) {
			var s [2]string
			return s, r.Scan(&s[0], &s[1])
		})
		return err
	}); err != nil {
		return nil, err
	}
	failed := map[string]error{}
	for _, s := range samples {
		var u unitRoute
		if err := c.tx(ctx, "readable.read", func(ctx context.Context, tx db.Tx) (err error) {
			u, err = c.routeOf(ctx, tx, s[1])
			return err
		}); err != nil {
			return nil, err
		}
		if _, err := c.readUnitRoute(ctx, u, ""); err != nil {
			failed[s[0]] = err
		}
	}
	return failed, nil
}
