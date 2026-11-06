package tiering

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/jackc/pgx/v5"
)

// The transfer (be-protocol P15.14-P15.18, P15.20): a sealed unit is claimed (its generation up, the database epoch
// recorded), copied outside any transaction from a read-only snapshot to its route, verified by reading everything
// back, and published in one local transaction that checks the state, the generation, the epoch and the digest.
// What a worker that lost its claim wrote is never published: the cleanup removes it after the verification window.

// PartRows bounds one Parquet object.
const PartRows = 200_000

// OrphanWindow is how long an unpublished object or row is kept before the cleanup removes it (protocol 11: 7 days).
const OrphanWindow = 7 * 24 * time.Hour

// ErrVerification is a copy that does not read back as what was sealed (P15.17).
var ErrVerification = errors.New("tiering: the copy does not verify")

// ErrStaleClaim is a publish refused because the unit changed since the claim (P15.18).
var ErrStaleClaim = errors.New("tiering: the claim is stale")

// Claim is one worker's claim of a unit: a sealed unit's transfer, or a published or released unit written again
// (an erasure's new version, a route change: P15.31, P15.35).
type Claim struct {
	Unit, Class, Epoch string
	Generation         int64
	// Digest is the content this claim writes; Was the unit's digest when claimed (equal for a transfer).
	Digest, Was string
	// State and Base are the unit's state and manifest version when claimed; Version the one this claim writes.
	State   string
	Base    int
	Version int
}

func epochOf(ctx context.Context, tx db.Tx) (string, error) {
	var g, tl int64
	if err := tx.QueryRow(ctx, "SELECT generation, timeline FROM be_current_db_epoch()").Scan(&g, &tl); err != nil {
		return "", err
	}
	return strconv.FormatInt(g, 10) + "." + strconv.FormatInt(tl, 10), nil
}

// ClaimUnit claims a sealed unit for its transfer (P15.14).
func (c *Catalog) ClaimUnit(ctx context.Context, unit string) (Claim, error) {
	return c.claim(ctx, unit, StateSealed)
}

// claim raises a unit's generation for a worker, the unit in one of the states.
func (c *Catalog) claim(ctx context.Context, unit string, states ...string) (Claim, error) {
	var cl Claim
	err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var version int
		if err := tx.QueryRow(ctx, "SELECT class, state, coalesce(digest, ''), manifest_version FROM be_tier_unit "+
			"WHERE id = $1 FOR UPDATE", unit).Scan(&cl.Class, &cl.State, &cl.Digest, &version); err != nil {
			return err
		}
		if !slices.Contains(states, cl.State) {
			return fmt.Errorf("tiering: unit %s is %s, not %s", unit, cl.State, strings.Join(states, " or "))
		}
		cl.Was, cl.Base = cl.Digest, version
		epoch, err := epochOf(ctx, tx)
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, "UPDATE be_tier_unit SET generation = generation + 1, claim_epoch = $2 WHERE id = $1 "+
			"RETURNING generation", unit, epoch).Scan(&cl.Generation); err != nil {
			return err
		}
		cl.Unit, cl.Epoch, cl.Version = unit, epoch, version+1
		return nil
	})
	return cl, err
}

// readUnit reads a unit's tables from one read-only snapshot: the roots (members) and their children.
func (c *Catalog) readUnit(ctx context.Context, cl Class, unit string) (map[string][]map[string]any, error) {
	out := map[string][]map[string]any{}
	err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		read := func(t Table, where string) error {
			names, list := selectList(t, "t")
			rows, err := tx.Query(ctx, fmt.Sprintf("SELECT %s FROM %s t WHERE %s", list, ident(t.Name), where), unit, cl.Name)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					return err
				}
				m := make(map[string]any, len(names))
				for i, n := range names {
					m[n] = vals[i]
				}
				out[t.Name] = append(out[t.Name], m)
			}
			return rows.Err()
		}
		members := "(SELECT m.root_id FROM be_tier_member m WHERE m.unit = $1 AND m.class = $2)"
		if err := read(cl.Root, "t."+ident(cl.Root.PK)+"::text IN "+members); err != nil {
			return err
		}
		for _, ch := range cl.Children {
			if err := read(ch.Table, "t."+ident(ch.FK)+"::text IN "+members); err != nil {
				return err
			}
		}
		return nil
	}, db.Snapshot())
	return out, err
}

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// Copy writes the claimed unit to a route and answers its manifest (P15.15, P15.16).
func (c *Catalog) Copy(ctx context.Context, claim Claim, route string) (Manifest, error) {
	tables, err := c.readUnit(ctx, c.Classes[claim.Class], claim.Unit)
	if err != nil {
		return Manifest{}, err
	}
	return c.copyTables(ctx, claim, route, tables)
}

// copyTables writes a unit's content (the driver's values by table) to a route as the claim's version.
func (c *Catalog) copyTables(ctx context.Context, claim Claim, route string, tables map[string][]map[string]any) (Manifest, error) {
	cl := c.Classes[claim.Class]
	m := Manifest{RecordType: ManifestRecordType, Schema: "1.0", Unit: claim.Unit, Class: claim.Class,
		Version: claim.Version, Route: route, Generation: claim.Generation, Epoch: claim.Epoch,
		SchemaVersion: cl.SchemaVersion, DigestVersion: DigestVersion, Digest: claim.Digest,
		CreatedAt: canon.Time(time.Now())}
	switch route {
	case RouteObject:
		if c.Store == nil {
			return Manifest{}, errors.New("tiering: the object route has no store (TIERING_STORE_URL)")
		}
		m.Format = FormatParquet
	case RoutePostgres:
		m.Format = FormatPgRows
	default:
		return Manifest{}, fmt.Errorf("tiering: unknown route %q", route)
	}
	for _, t := range cl.Tables() {
		rows := tables[t.Name]
		mt := ManifestTable{Name: t.Name, PK: []string{t.PK}, Rows: strconv.Itoa(len(rows))}
		for _, n := range columnNames(t) {
			mt.Columns = append(mt.Columns, ManifestColumn{Name: n, Type: t.Columns[n], Nullable: true})
		}
		if route == RouteObject {
			for part := 0; part == 0 || part*PartRows < len(rows); part++ {
				chunk := rows[part*PartRows : min(len(rows), (part+1)*PartRows)]
				data, err := WriteParquet(t, chunk)
				if err != nil {
					return Manifest{}, err
				}
				key := ObjectKey(c.Store.Prefix, claim.Class, claim.Unit, claim.Generation, claim.Version, t.Name, part)
				if err := c.Store.Put(ctx, key, data); err != nil {
					return Manifest{}, err
				}
				mt.Objects = append(mt.Objects, ManifestObject{Key: key, Rows: strconv.Itoa(len(chunk)),
					Bytes: strconv.Itoa(len(data)), SHA256: sha(data)})
			}
		} else if err := c.copyRows(ctx, claim, t, rows); err != nil {
			return Manifest{}, err
		}
		m.Tables = append(m.Tables, mt)
	}
	if route == RouteObject {
		raw, err := m.Canonical()
		if err != nil {
			return Manifest{}, err
		}
		if err := c.Store.Put(ctx, ManifestKey(c.Store.Prefix, claim.Class, claim.Unit, claim.Generation, claim.Version), raw); err != nil {
			return Manifest{}, err
		}
	}
	return m, nil
}

// copyRows writes a table's canonical rows into the warm route under the claim's version (not read before published).
func (c *Catalog) copyRows(ctx context.Context, claim Claim, t Table, rows []map[string]any) error {
	return c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		// a stale worker's rows of the same version would be another generation's: the generation decides (P15.18)
		if _, err := tx.Exec(ctx, "DELETE FROM be_tier_row WHERE unit = $1 AND version = $2 AND tbl = $3",
			claim.Unit, claim.Version, t.Name); err != nil {
			return err
		}
		for _, r := range rows {
			k, err := PK(t.Columns, []string{t.PK}, r)
			if err != nil {
				return err
			}
			b, err := Row(t.Columns, r)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "INSERT INTO be_tier_row (unit, version, tbl, pk, row) VALUES ($1, $2, $3, $4, $5)",
				claim.Unit, claim.Version, t.Name, string(k), b); err != nil {
				return err
			}
		}
		return nil
	})
}

// Verify reads the copy back in full and checks it against the manifest and the sealed digest (P15.17).
func (c *Catalog) Verify(ctx context.Context, m Manifest) error {
	cl := c.Classes[m.Class]
	var sums []TableSum
	roots := map[string]bool{}
	children := map[string][]any{} // child table -> its rows' root keys
	for _, mt := range m.Tables {
		t, fk, ok := tableOf(cl, mt.Name)
		if !ok {
			return fmt.Errorf("%w: table %s is not the class's", ErrVerification, mt.Name)
		}
		var h TableHash
		add := func(r map[string]any) error {
			k, err := PK(t.Columns, []string{t.PK}, r)
			if err != nil {
				return err
			}
			b, err := Row(t.Columns, r)
			if err != nil {
				return err
			}
			h.Add(k, b)
			if t.Name == cl.Root.Name {
				v, _ := Value(t.Columns[t.PK], r[t.PK])
				roots[fmt.Sprint(v)] = true
			} else {
				children[t.Name] = append(children[t.Name], r[fk])
			}
			return nil
		}
		switch m.Route {
		case RouteObject:
			for _, o := range mt.Objects {
				data, err := c.Store.Get(ctx, o.Key)
				if err != nil {
					return err
				}
				if strconv.Itoa(len(data)) != o.Bytes || sha(data) != o.SHA256 {
					return fmt.Errorf("%w: object %s is not what the manifest says", ErrVerification, o.Key)
				}
				rows, err := ReadParquet(t, data)
				if err != nil {
					return fmt.Errorf("%w: %v", ErrVerification, err)
				}
				for _, r := range rows {
					if err := add(r); err != nil {
						return err
					}
				}
			}
		case RoutePostgres:
			if err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
				rows, err := tx.Query(ctx, "SELECT pk, row FROM be_tier_row WHERE unit = $1 AND version = $2 AND tbl = $3",
					m.Unit, m.Version, mt.Name)
				if err != nil {
					return err
				}
				defer rows.Close()
				for rows.Next() {
					var pk string
					var raw []byte
					if err := rows.Scan(&pk, &raw); err != nil {
						return err
					}
					h.Add([]byte(pk), raw)
					var obj map[string]any
					if err := json.Unmarshal(raw, &obj); err != nil {
						return err
					}
					if t.Name == cl.Root.Name {
						roots[fmt.Sprint(obj[t.PK])] = true
					} else {
						children[t.Name] = append(children[t.Name], obj[fk])
					}
				}
				return rows.Err()
			}); err != nil {
				return err
			}
		}
		if strconv.Itoa(h.Len()) != mt.Rows {
			return fmt.Errorf("%w: %s has %d rows, the manifest %s", ErrVerification, mt.Name, h.Len(), mt.Rows)
		}
		sums = append(sums, TableSum{Name: mt.Name, Rows: h.Len(), SHA256: h.Sum()})
	}
	for name, keys := range children { // every child's root is in the unit (主子完整性)
		ch, _, _ := tableOf(cl, name)
		for _, k := range keys {
			v, err := Value(ch.Columns[childFK(cl, name)], k)
			if err != nil || !roots[fmt.Sprint(v)] {
				return fmt.Errorf("%w: a row of %s whose root is not in the unit", ErrVerification, name)
			}
		}
	}
	for _, col := range cl.Locate { // the columns reads need are in the copy
		if _, ok := cl.Root.Columns[col]; !ok {
			return fmt.Errorf("%w: the locate column %s is missing", ErrVerification, col)
		}
	}
	digest, err := UnitDigest(cl.Name, m.SchemaVersion, sums)
	if err != nil {
		return err
	}
	if digest != m.Digest {
		return fmt.Errorf("%w: the content's digest %s is not the sealed %s", ErrVerification, digest, m.Digest)
	}
	return nil
}

func tableOf(cl Class, name string) (Table, string, bool) {
	if cl.Root.Name == name {
		return cl.Root, "", true
	}
	for _, ch := range cl.Children {
		if ch.Name == name {
			return ch.Table, ch.FK, true
		}
	}
	return Table{}, "", false
}

func childFK(cl Class, name string) string {
	_, fk, _ := tableOf(cl, name)
	return fk
}

// Publish makes a verified copy the unit's authority in one local transaction (P15.18): the unit still in the state
// and at the manifest version it was claimed in, the claim's generation and epoch current, its digest unchanged since
// the claim; otherwise ErrStaleClaim and nothing changes. A sealed unit turns published; a published or released
// one written again keeps its state (P15.31, P15.35).
func (c *Catalog) Publish(ctx context.Context, claim Claim, m Manifest) error {
	return c.publish(ctx, claim, m, nil)
}

// publish is Publish with more work in its transaction (an erasure's online rows and evidence).
func (c *Catalog) publish(ctx context.Context, claim Claim, m Manifest, also func(context.Context, db.Tx) error) error {
	raw, err := m.Canonical()
	if err != nil {
		return err
	}
	return c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var state, digest, route string
		var gen int64
		var version int
		var epoch *string
		if err := tx.QueryRow(ctx, "SELECT state, coalesce(digest, ''), generation, claim_epoch, manifest_version, route "+
			"FROM be_tier_unit WHERE id = $1 FOR UPDATE", claim.Unit).Scan(&state, &digest, &gen, &epoch, &version, &route); err != nil {
			return err
		}
		now, err := epochOf(ctx, tx)
		if err != nil {
			return err
		}
		switch {
		case state != claim.State || version != claim.Base:
			return fmt.Errorf("%w: unit %s is %s at version %d, the claim's %s at %d", ErrStaleClaim, claim.Unit, state,
				version, claim.State, claim.Base)
		case gen != claim.Generation || epoch == nil || *epoch != claim.Epoch:
			return fmt.Errorf("%w: unit %s is at generation %d, the claim %d", ErrStaleClaim, claim.Unit, gen, claim.Generation)
		case claim.Epoch != now:
			return fmt.Errorf("%w: the database's epoch is %s, the claim's %s", ErrStaleClaim, now, claim.Epoch)
		case digest != claim.Was || m.Digest != claim.Digest:
			return fmt.Errorf("%w: the sealed content changed", ErrStaleClaim)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO be_tier_manifest (unit, version, route, generation, manifest, digest) "+
			"VALUES ($1, $2, $3, $4, $5, $6)", claim.Unit, m.Version, m.Route, claim.Generation, raw, m.Digest); err != nil {
			return err
		}
		var catalog int64
		if err := tx.QueryRow(ctx, "UPDATE be_tier_catalog SET version = version + 1 RETURNING version").Scan(&catalog); err != nil {
			return err
		}
		if state == StateSealed {
			_, err = tx.Exec(ctx, "UPDATE be_tier_unit SET route = $2, manifest_version = $3, state = 'published', "+
				"published_at = now(), route_version = $4 WHERE id = $1", claim.Unit, m.Route, m.Version, catalog)
		} else { // written again: the route version moves only when the route does (older leases read the old one)
			_, err = tx.Exec(ctx, "UPDATE be_tier_unit SET route = $2, manifest_version = $3, digest = $4, "+
				"route_version = CASE WHEN route = $2 THEN route_version ELSE $5 END WHERE id = $1", claim.Unit, m.Route,
				m.Version, m.Digest, catalog)
		}
		if err != nil || also == nil {
			return err
		}
		return also(ctx, tx)
	})
}

// Transfer claims, copies, verifies and publishes one sealed unit to its class's first route.
func (c *Catalog) Transfer(ctx context.Context, unit string) (Manifest, error) {
	claim, err := c.ClaimUnit(ctx, unit)
	if err != nil {
		return Manifest{}, err
	}
	route := c.Classes[claim.Class].Routes[0]
	m, err := c.Copy(ctx, claim, route)
	if err != nil {
		return Manifest{}, err
	}
	if err := c.Verify(ctx, m); err != nil {
		return Manifest{}, err
	}
	return m, c.Publish(ctx, claim, m)
}

// CleanOrphans removes what no manifest of a unit names (P15.20): objects under the unit's prefix older than the window,
// and warm rows of a version no manifest names that no transfer is writing.
func (c *Catalog) CleanOrphans(ctx context.Context, unit, class string, window time.Duration) (int, error) {
	var published []string // "<generation>.<version>/" of every manifest of the unit
	var versions []int
	err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT generation, version FROM be_tier_manifest WHERE unit = $1", unit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var g int64
			var v int
			if err := rows.Scan(&g, &v); err != nil {
				return err
			}
			published = append(published, fmt.Sprintf("%d.%d/", g, v))
			versions = append(versions, v)
		}
		return rows.Err()
	})
	if err != nil {
		return 0, err
	}
	removed := 0
	if c.Store != nil {
		prefix := fmt.Sprintf("%s/%s/%s/", c.Store.Prefix, class, unit)
		objects, err := c.Store.List(ctx, prefix)
		if err != nil {
			return 0, err
		}
		for _, o := range objects {
			rest := strings.TrimPrefix(o.Key, prefix)
			named := false
			for _, p := range published {
				if strings.HasPrefix(rest, p) {
					named = true
				}
			}
			if named || time.Since(o.LastModified) < window {
				continue
			}
			if err := c.Store.Delete(ctx, o.Key); err != nil {
				return removed, err
			}
			removed++
		}
	}
	err = c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		// a warm row is an orphan when no manifest names its version and it is not the version a transfer may be
		// writing now (the next one)
		tag, err := tx.Exec(ctx, "DELETE FROM be_tier_row r WHERE r.unit = $1 AND NOT (r.version = ANY($2)) AND r.version >= 0 "+
			"AND r.version <> (SELECT manifest_version + 1 FROM be_tier_unit WHERE id = $1)", unit, versions)
		if err != nil {
			return err
		}
		removed += int(tag.RowsAffected())
		// a thawed copy (version -1) goes when its time is up (P15.27)
		tag, err = tx.Exec(ctx, "DELETE FROM be_tier_row WHERE unit = $1 AND version = -1 AND EXISTS (SELECT 1 FROM be_tier_thaw "+
			"WHERE unit = $1 AND expires_at <= now())", unit)
		if err != nil {
			return err
		}
		removed += int(tag.RowsAffected())
		_, err = tx.Exec(ctx, "DELETE FROM be_tier_thaw WHERE unit = $1 AND expires_at <= now()", unit)
		return err
	})
	return removed, err
}

// TransferJob is the SDK's job that transfers the sealed units (P15.14-P15.18) and cleans their orphans (P15.20).
func (c *Catalog) TransferJob(log *slog.Logger) work.Job {
	return work.Job{Name: "be.tiering.transfer", Kind: work.Singleton, Effect: work.IdempotentExternal,
		Interval: time.Minute, MaxAttempts: work.Unlimited, Timeout: time.Hour,
		Handle: func(ctx context.Context, run *work.Run) error {
			units, err := c.unitsIn(ctx, StateSealed)
			if err != nil {
				return err
			}
			for _, u := range units {
				m, err := c.Transfer(ctx, u)
				if err != nil {
					log.Error("an archive unit's transfer failed; its online rows stay", "event", "tiering_transfer_failed",
						"unit", u, "error", err.Error())
					continue
				}
				log.Info("an archive unit is published", "event", "tiering_published", "unit", u, "route", m.Route,
					"version", m.Version)
			}
			due, err := c.rerouteDue(ctx) // a class's first route changed: its units move there (P15.35)
			if err != nil {
				return err
			}
			for u, route := range due {
				m, err := c.Reroute(ctx, u, route)
				if err != nil {
					log.Error("an archive unit's route change failed; it stays on its route", "event", "tiering_reroute_failed",
						"unit", u, "route", route, "error", err.Error())
					continue
				}
				log.Info("an archive unit moved to another route", "event", "tiering_rerouted", "unit", u, "route", m.Route,
					"version", m.Version)
			}
			return run.Complete(ctx, func(context.Context, db.Tx) error { return nil })
		}}
}

func (c *Catalog) unitsIn(ctx context.Context, state string) ([]string, error) {
	var out []string
	err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT id::text FROM be_tier_unit WHERE state = $1 ORDER BY sealed_at LIMIT 100", state)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return out, err
}
