package tiering

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/recovery"
)

// The recovery check be.tiering (be-protocol P15.36; the assembly protocol 15's RC11, protocol 11's TR17): after a
// full recovery the catalog, the manifests, the warm rows and the objects are compared. One read-only transaction
// reads the catalog, then the component's prefix in the cold store is listed once, outside it.

// CheckName is the check's name.
const CheckName = "be.tiering"

// The check's object types.
const (
	ObjectUnit   = "tier_unit"   // a unit whose copy is missing or short, or released with online rows left
	ObjectStored = "tier_object" // an object of no live manifest version
	ObjectRows   = "tier_rows"   // warm rows of no live manifest version
	ObjectSchema = "tier_schema" // an older structure version that cannot be read
)

// RecoveryCheck is the catalog's check, registered by the host before the component's own.
func (c *Catalog) RecoveryCheck() recovery.Check {
	return recovery.Check{Name: CheckName, Basis: "装配项目协议 11 的恢复时的核对、协议 15 的 RC11；be-protocol P15.36", Run: c.check}
}

// liveUnit is a published or released unit as the check reads it.
type liveUnit struct {
	id, class, state, route string
	manifest                Manifest
	live                    map[string]bool // "<generation>.<version>" of its manifests whose data must be there
	versions                map[int]bool    // the same, by version (warm rows); -1 when thawed
	leftOnline              int64
}

func (c *Catalog) check(ctx context.Context, read recovery.Read) (recovery.Result, error) {
	units := map[string]*liveUnit{}
	known := map[string]bool{}    // every unit of the catalog, any state
	warm := map[[2]string]int64{} // (unit, version) -> rows
	err := read(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT u.id::text, u.class, u.state, u.route, u.manifest_version, m.version, m.generation, "+
			"m.superseded_at IS NULL, m.manifest, EXISTS (SELECT 1 FROM be_tier_thaw t WHERE t.unit = u.id) FROM be_tier_unit u "+
			"LEFT JOIN be_tier_manifest m ON m.unit = u.id ORDER BY u.id, m.version")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, class, state, route string
			var current int
			var version *int
			var gen *int64
			var live *bool
			var raw []byte
			var thawed bool
			if err := rows.Scan(&id, &class, &state, &route, &current, &version, &gen, &live, &raw, &thawed); err != nil {
				return err
			}
			known[id] = true
			if state != StatePublished && state != StateReleased || version == nil {
				continue
			}
			u := units[id]
			if u == nil {
				u = &liveUnit{id: id, class: class, state: state, route: route, live: map[string]bool{}, versions: map[int]bool{}}
				units[id] = u
				if thawed {
					u.versions[-1] = true
				}
			}
			if *version == current {
				if err := json.Unmarshal(raw, &u.manifest); err != nil {
					return fmt.Errorf("tiering: unit %s: its manifest: %w", id, err)
				}
			}
			if *live || *version == current {
				u.live[strconv.FormatInt(*gen, 10)+"."+strconv.Itoa(*version)] = true
				u.versions[*version] = true
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		counts, err := tx.Query(ctx, "SELECT unit::text, version, count(*) FROM be_tier_row GROUP BY unit, version")
		if err != nil {
			return err
		}
		defer counts.Close()
		for counts.Next() {
			var unit string
			var version int
			var n int64
			if err := counts.Scan(&unit, &version, &n); err != nil {
				return err
			}
			warm[[2]string{unit, strconv.Itoa(version)}] = n
		}
		if err := counts.Err(); err != nil {
			return err
		}
		for _, cl := range c.Classes { // released units with online rows left
			left, err := tx.Query(ctx, fmt.Sprintf("SELECT m.unit::text, count(*) FROM be_tier_member m JOIN be_tier_unit u "+
				"ON u.id = m.unit JOIN %s r ON r.%s::text = m.root_id WHERE u.state = 'released' AND u.class = $1 GROUP BY m.unit",
				ident(cl.Root.Name), ident(cl.Root.PK)), cl.Name)
			if err != nil {
				return err
			}
			for left.Next() {
				var unit string
				var n int64
				if err := left.Scan(&unit, &n); err != nil {
					left.Close()
					return err
				}
				if u := units[unit]; u != nil {
					u.leftOnline = n
				}
			}
			left.Close()
			if err := left.Err(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return recovery.Result{}, err
	}

	var diffs []recovery.Difference
	add := func(typ, id, detail string) {
		diffs = append(diffs, recovery.Difference{ObjectType: typ, ObjectID: id, Detail: detail})
	}
	var objects map[string]int64 // key -> size
	for _, u := range units {
		if u.route == RouteObject && c.Store == nil {
			return recovery.Result{}, errors.New("units on the object route and no object store configured (TIERING_STORE_URL)")
		}
	}
	if c.Store != nil {
		listed, err := c.Store.List(ctx, c.Store.Prefix+"/")
		if err != nil {
			return recovery.Result{}, err
		}
		objects = map[string]int64{}
		for _, o := range listed {
			if !strings.HasPrefix(o.Key, c.Store.Prefix+"/exports/") {
				objects[o.Key] = o.Size
			}
		}
	}
	for id, u := range units {
		switch u.route {
		case RoutePostgres:
			want := int64(0)
			for _, t := range u.manifest.Tables {
				n, _ := strconv.ParseInt(t.Rows, 10, 64)
				want += n
			}
			if got := warm[[2]string{id, strconv.Itoa(u.manifest.Version)}]; got != want {
				add(ObjectUnit, id, fmt.Sprintf("the warm route has %d of the manifest's %d rows (version %d)", got, want, u.manifest.Version))
			}
		case RouteObject:
			var missing []string
			for _, t := range u.manifest.Tables {
				for _, o := range t.Objects {
					if size, ok := objects[o.Key]; !ok || strconv.FormatInt(size, 10) != o.Bytes {
						missing = append(missing, o.Key)
					}
				}
			}
			if len(missing) > 0 {
				add(ObjectUnit, id, "the manifest's objects missing or not their size: "+strings.Join(missing, ", "))
			}
		}
		if u.leftOnline > 0 {
			add(ObjectUnit, id, fmt.Sprintf("released with %d online roots left: the daily destruction job deletes them", u.leftOnline))
		}
	}
	for key := range objects { // <prefix>/<class>/<unit>/<generation>.<version>/<file>
		parts := strings.Split(strings.TrimPrefix(key, c.Store.Prefix+"/"), "/")
		if len(parts) == 4 {
			if u := units[parts[1]]; u != nil && u.live[parts[2]] {
				continue
			}
		}
		add(ObjectStored, key, "of no live manifest version: an orphan or an unfinished transfer, never read")
	}
	for k, n := range warm {
		version, _ := strconv.Atoi(k[1])
		if u := units[k[0]]; u != nil && u.versions[version] {
			continue
		}
		add(ObjectRows, k[0]+"/"+k[1], fmt.Sprintf("%d warm rows of no live manifest version: never read", n))
	}
	for name := range c.Classes {
		failed, err := c.CheckReadable(ctx, name)
		if err != nil {
			return recovery.Result{}, err
		}
		for version, err := range failed {
			add(ObjectSchema, name+"@"+version, "an older structure cannot be read: "+err.Error())
		}
	}
	if len(diffs) == 0 {
		return recovery.Result{Outcome: recovery.Pass}, nil
	}
	sort.Slice(diffs, func(i, j int) bool {
		if diffs[i].ObjectType != diffs[j].ObjectType {
			return diffs[i].ObjectType < diffs[j].ObjectType
		}
		return diffs[i].ObjectID < diffs[j].ObjectID
	})
	return recovery.Result{Outcome: recovery.Diff, Differences: diffs, Disposal: "tier_unit：副本缺失或不全的单元读取答暂时不可用，" +
		"从备份或副本恢复对象后重做核对（RC11）；仍有在线行的已释放单元由每天的销毁作业继续删除。tier_object、tier_rows：恢复点之后" +
		"写下的或未完成的转层，读取只认清单所以不开放，转层作业重做、孤儿清理过了窗口删除（TR17）。tier_schema：登记那个结构版本的转换器"}, nil
}

// continueReleases deletes the online rows a released unit still has (the recovery check's finding), a class at a time.
func (c *Catalog) continueReleases(ctx context.Context) ([]string, error) {
	var done []string
	for _, cl := range c.Classes {
		var units []string
		if err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			rows, err := tx.Query(ctx, fmt.Sprintf("SELECT DISTINCT m.unit::text FROM be_tier_member m JOIN be_tier_unit u ON "+
				"u.id = m.unit JOIN %s r ON r.%s::text = m.root_id WHERE u.state = 'released' AND u.class = $1",
				ident(cl.Root.Name), ident(cl.Root.PK)), cl.Name)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var u string
				if err := rows.Scan(&u); err != nil {
					return err
				}
				units = append(units, u)
			}
			return rows.Err()
		}); err != nil {
			return done, err
		}
		for _, u := range units {
			if err := c.deleteOnline(ctx, u, cl, nil); err != nil {
				return done, err
			}
			done = append(done, u)
		}
	}
	return done, nil
}
