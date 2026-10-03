package besdk

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/gin-gonic/gin"
)

const dbManifest = `apiVersion: brickkit/v1
kind: Component
metadata: {id: test/thing, name: Thing, version: 3.0.0}
configSchema:
  type: object
  properties:
    PG_HOST: {type: string}
    PG_PORT: {type: integer, default: 5432}
    PG_DATABASE: {type: string}
    PG_USER: {type: string}
    PG_PASSWORD_FILE: {type: string, secret: true, mount: file}
    PG_OWNER_USER: {type: string}
    PG_OWNER_PASSWORD_FILE: {type: string, secret: true, mount: file}
    PG_SCHEMA: {type: string}
    PG_POOL_MAX: {type: integer, default: 10}
    PG_POOL_ACQUIRE_TIMEOUT: {type: string, default: 5s}
    PG_MIGRATION_HOST: {type: string}
    PG_MIGRATION_PORT: {type: integer}
    SHUTDOWN_GRACE: {type: string, default: 2s}
    THING_TOKEN_FILE: {type: string, secret: true, mount: file}
  required: [PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD_FILE, PG_OWNER_USER, PG_OWNER_PASSWORD_FILE, PG_SCHEMA]
deployment:
  port: PORT_PLACEHOLDER
`

var thingMigrations = fstest.MapFS{
	"0001_things.up.sql":   {Data: []byte("CREATE TABLE things (id uuid PRIMARY KEY, name text NOT NULL);")},
	"0001_things.down.sql": {Data: []byte("DROP TABLE things;")},
	"lifecycle.yaml":       {Data: []byte("version: 1\ntables: {}\n")},
}

func dbEnv(t *testing.T, id testpg.Identity) map[string]string {
	return map[string]string{
		"PG_HOST": id.Host, "PG_PORT": fmt.Sprint(id.Port), "PG_DATABASE": id.Database,
		"PG_USER": id.User, "PG_PASSWORD_FILE": id.PasswordFile,
		"PG_OWNER_USER": id.Owner, "PG_OWNER_PASSWORD_FILE": id.OwnerPasswordFile, "PG_SCHEMA": id.Schema,
		"THING_TOKEN_FILE": secretFile(t, "x"),
	}
}

func dbSpec() Spec {
	return Spec{ID: "test/thing", Manifest: []byte(dbManifest), Migrations: thingMigrations,
		New: func(_ context.Context, rt *Runtime) (*Module, error) {
			store, err := rt.Store()
			if err != nil {
				return nil, err
			}
			return &Module{HTTP: func(r *Router) {
				POST(r, "/things/:name", Public, func(c *gin.Context) {
					var n int
					err := store.Tx(c.Request.Context(), func(ctx context.Context, tx *Tx) error {
						if _, err := tx.ExecContext(ctx, `INSERT INTO things (id, name) VALUES (gen_random_uuid(), $1)`, c.Param("name")); err != nil {
							return err
						}
						return tx.QueryRowContext(ctx, `SELECT count(*) FROM things`).Scan(&n)
					})
					if err != nil {
						Fail(c, err)
						return
					}
					Respond(c, 201, map[string]any{"count": n, "schema": store.Identity().Schema})
				})
			}}, nil
		}}
}

func TestMigrateThenServeWithDatabase(t *testing.T) {
	id := testpg.New(t)
	env := dbEnv(t, id)
	for i := 0; i < 2; i++ { // idempotent (P1.1)
		if code, out := runOnce(t, dbSpec(), env, "migrate", "up"); code != 0 {
			t.Fatalf("migrate up #%d: %d\n%s", i+1, code, out)
		}
	}
	if code, out := runOnce(t, dbSpec(), env, "migrate", "status"); code != 0 || !strings.Contains(out, `"to":1`) {
		t.Fatalf("status: %d\n%s", code, out)
	}
	p := startProc(t, dbSpec(), env)
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, body := p.get(t, "/readyz")
		if code == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never ready: %s\n%s", body, p.out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	resp, err := httpPost(fmt.Sprintf("http://127.0.0.1:%d/test/thing/things/a", p.port))
	if err != nil || resp.code != 201 || !strings.Contains(resp.body, `"count":1`) || !strings.Contains(resp.body, id.Schema) {
		t.Fatalf("write: %v %+v", err, resp)
	}
	_, info := p.get(t, "/_be/info")
	if !strings.Contains(info, `"component":"0001"`) || !strings.Contains(info, `"platform":1`) {
		t.Fatalf("info %s", info)
	}
}

func TestServeWaitsForMigrations(t *testing.T) {
	id := testpg.New(t)
	p := startProc(t, dbSpec(), dbEnv(t, id))
	time.Sleep(700 * time.Millisecond)
	code, body := p.get(t, "/readyz")
	if code != 503 || !strings.Contains(body, "migrations") {
		t.Fatalf("readyz before migrations: %d %s", code, body)
	}
	if code, _ := p.get(t, "/healthz"); code != 200 {
		t.Fatalf("healthz must stay 200: %d", code)
	}
}

type postResult struct {
	code int
	body string
}

func httpPost(url string) (postResult, error) {
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return postResult{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return postResult{code: resp.StatusCode, body: string(b)}, nil
}
