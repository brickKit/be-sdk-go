package besdk

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/testpg"
)

var lifecycleMigrations = fstest.MapFS{
	"0001_things.up.sql": {Data: []byte(`CREATE TABLE things (id uuid PRIMARY KEY, name text NOT NULL);
CREATE TABLE thing_log (id uuid NOT NULL, created_at timestamptz NOT NULL, note text,
  PRIMARY KEY (id, created_at)) PARTITION BY RANGE (created_at);`)},
	"0001_things.down.sql": {Data: []byte("DROP TABLE thing_log; DROP TABLE things;")},
	"lifecycle.yaml": {Data: []byte(`lifecycle: v1
tables:
  things: {class: master}
  thing_log: {class: audit, partition: {by: created_at, grain: month, ahead: 1}}
`)},
}

func lifecycleSpec(migrations fstest.MapFS) Spec {
	return Spec{ID: "test/thing", Manifest: []byte(dbManifest), Migrations: migrations,
		New: func(context.Context, *Runtime) (*Module, error) { return &Module{}, nil }}
}

// P16.6, P11.3: the platform migration creates the declared partition window; P10.5, P11.4: the
// serving process keeps a session named <id>@<version>; the lifecycle engine is the job be.lifecycle.
func TestLifecycleThroughTheRuntime(t *testing.T) {
	id := testpg.New(t)
	env := dbEnv(t, id)
	spec := lifecycleSpec(lifecycleMigrations)
	if code, out := runOnce(t, spec, env, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %d\n%s", code, out)
	}
	super := testpg.Open(t, id.SuperDSN)
	now := time.Now().UTC()
	part := fmt.Sprintf("thing_log_%04dm%02d", now.Year(), int(now.Month()))
	var n int
	if err := super.QueryRow(`SELECT count(*) FROM pg_tables WHERE schemaname = $1 AND tablename = $2`, id.Schema, part).Scan(&n); err != nil || n != 1 {
		t.Fatalf("partition %s: %d %v", part, n, err)
	}
	p := startProc(t, spec, env)
	waitReady(t, p)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := super.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE application_name = 'test/thing@3.0.0' AND usename = $1`, id.User).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n == 0 {
		t.Fatal("no session named test/thing@3.0.0")
	}
	if code, out := runOnce(t, spec, env, "job", "run", "be.lifecycle"); code != 0 {
		t.Fatalf("job run be.lifecycle: %d\n%s", code, out)
	}
}

// P16.1: a database component without a valid lifecycle.yaml does not start (exit 78).
func TestInvalidLifecycleDeclarationExits78(t *testing.T) {
	id := testpg.New(t)
	bad := fstest.MapFS{}
	for k, v := range lifecycleMigrations {
		bad[k] = v
	}
	bad["lifecycle.yaml"] = &fstest.MapFile{Data: []byte("lifecycle: v1\ntables:\n  things: {class: master}\n")}
	if code, out := runOnce(t, lifecycleSpec(bad), dbEnv(t, id), "migrate", "up"); code != 78 || !strings.Contains(out, "thing_log") {
		t.Fatalf("undeclared table: %d\n%s", code, out)
	}
}
