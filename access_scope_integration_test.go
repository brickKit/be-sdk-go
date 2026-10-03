package besdk

import (
	"context"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/testpg"
)

const thingCatalog = `{"resource_types": [{"type": "test.thing.thing", "owner_component": "test/thing",
  "view_key": "test.thing.view", "keys": ["test.thing.view", "test.thing.edit"], "dimensions": ["owner"],
  "relations": {"viewer": {"grants": ["test.thing.view"]}},
  "share": {"key": "test.thing.share", "relations": ["viewer"], "subjects": ["user", "role", "dept", "dept_tree"]},
  "fields": [{"set": "pricing", "columns": ["price"], "read": "test.thing.price", "edit": "test.thing.price_edit"}],
  "derivation": "direct"}]}`

const thingBundle = `{"contract": "authz/2.0", "revision": "7", "catalog_digest": "sha256:00",
  "capabilities": {"core": true, "sharing": true, "relation_sync": true, "delegation": false},
  "roles": {"rep": ["test.thing.view", "test.thing.edit", "test.thing.share"]}, "grants": {}, "profiles": {},
  "delegations": [], "stale_since": {}, "revoked_grants": {}}`

type thing struct {
	ID    string  `json:"id"`
	Owner string  `json:"owner"`
	Name  string  `json:"name"`
	Price *string `json:"price"`
}

func (t thing) AuthzRef() Ref     { return Ref{Type: "test.thing.thing", ID: t.ID} }
func (t thing) AuthzAttrs() Attrs { return Attrs{Owner: t.Owner} }

// accessRuntime is a store runtime whose schema has the ACL projection, with the thing catalogue.
func accessRuntime(t *testing.T) (*Runtime, testpg.Identity) {
	t.Helper()
	id := testpg.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pg.MigrateUp(ctx, pg.MigrateConfig{Host: id.Host, Port: id.Port, Database: id.Database, SSLMode: "disable",
		Owner: id.Owner, OwnerPassword: id.OwnerPassword, Schema: id.Schema, ComponentID: "test/thing",
		Component: thingMigrations, AuthzProjection: true}); err != nil {
		t.Fatal(err)
	}
	pool, err := pg.OpenPool(pg.PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: id.User,
		Password: func() string { return id.Password }, SSLMode: "disable"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	cat, err := authz.ParseCatalog([]byte(thingCatalog))
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{id: "test/thing", version: "3.0.0", clock: time.Now, authzCatalog: cat}
	rt.deps.store = pg.NewStore(pool, pg.StoreConfig{ComponentID: "test/thing", Role: id.User, Schema: id.Schema})
	return rt, id
}

func accessFor(t *testing.T, rt *Runtime, sub string, key PermKey) *Access {
	b, err := authz.ParseBundle([]byte(thingBundle))
	if err != nil {
		t.Fatal(err)
	}
	tok := authz.Token{Sub: sub, IssuedAt: time.Now(), Roles: []string{"rep"}, DeptPath: "/1/"}
	return &Access{user: User{Sub: sub}, token: tok, bundle: b, key: key, now: rt.clock, rt: rt}
}

// P6.5–P6.8 through Access: the scope's parameters, the single-record decision (404 for an
// invisible record, a share makes it visible), field masks and their write and sort checks.
func TestAccessScopeCanAndFields(t *testing.T) {
	rt, id := accessRuntime(t)
	ctx := context.Background()
	alice := accessFor(t, rt, "alice", "test.thing.view")

	s := alice.Scope("test.thing.thing")
	if p := s.Params(); p.All || len(p.Owners) != 1 || p.Owners[0] != "alice" {
		t.Fatalf("params %+v", p)
	}
	where, args := s.Where(Columns{Alias: "t", ID: "id", Owner: "owner_id"}, 1)
	if where == "" || len(args) == 0 {
		t.Fatalf("where %q %v", where, args)
	}

	mine, bobs := thing{ID: "t1", Owner: "alice"}, thing{ID: "t2", Owner: "bob"}
	if d, err := alice.Can(ctx, nil, "test.thing.edit", mine); err != nil || !d.Allowed {
		t.Fatalf("own record: %+v %v", d, err)
	}
	d, err := alice.Can(ctx, nil, "test.thing.edit", bobs)
	if err != nil || d.Visible || d.Reason != "NOT_FOUND" {
		t.Fatalf("other's record: %+v %v", d, err)
	}
	if e := problem.From(d.Err()); e.HTTPStatus() != 404 {
		t.Fatalf("invisible answers %d", e.HTTPStatus())
	}
	super := testpg.Open(t, id.SuperDSN)
	if _, err := super.Exec(`INSERT INTO ` + id.Schema + `.besdk_authz_acl (rtype, rid, relation, subject, revision)
	  VALUES ('test.thing.thing', 't2', 'viewer', 'user:alice', 1)`); err != nil {
		t.Fatal(err)
	}
	d, err = alice.Can(ctx, nil, "test.thing.edit", bobs)
	if err != nil || !d.Visible || d.Allowed || d.Reason != "OUT_OF_SCOPE" {
		t.Fatalf("shared record: %+v %v", d, err)
	}
	if e := problem.From(d.Err()); e.HTTPStatus() != 403 || e.Reason != "OUT_OF_SCOPE" {
		t.Fatalf("shared, not editable: %+v", e)
	}

	p := "9.99"
	row := thing{ID: "t1", Owner: "alice", Name: "A", Price: &p}
	if masked := alice.Mask("test.thing.thing", &row); len(masked) != 1 || masked[0] != "price" || row.Price != nil {
		t.Fatalf("mask %v %+v", masked, row)
	}
	if e := problem.From(alice.CheckWritable("test.thing.thing", []string{"name", "price"})); e == nil || e.Reason != "FIELD_FORBIDDEN" {
		t.Fatalf("writable: %v", e)
	}
	if e := problem.From(alice.CheckSortable("test.thing.thing", "price")); e == nil || e.Reason != "SORT_FORBIDDEN" {
		t.Fatalf("sortable: %v", e)
	}
	if err := alice.CheckSortable("test.thing.thing", "name"); err != nil {
		t.Fatalf("name is sortable: %v", err)
	}
}
