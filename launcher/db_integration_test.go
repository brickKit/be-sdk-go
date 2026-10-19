//go:build integration

package launcher

import (
	"context"
	"io"
	"maps"
	"net/http"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type notes struct{ db *db.DB }

func (n *notes) Routes(r *httpx.Routes) error {
	r.POST("/api/notes", "demo.notes.write", func(c *httpx.Context) error {
		err := n.db.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			_, err := tx.Exec(ctx, "INSERT INTO notes (id) VALUES ($1)", ids.New())
			return err
		})
		if err != nil {
			return err
		}
		return c.NoContent(201)
	})
	r.GET("/api/notes/count", "demo.notes.read", func(c *httpx.Context) error {
		var count int
		err := n.db.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT count(*) FROM notes").Scan(&count)
		})
		if err != nil {
			return err
		}
		return c.JSON(200, map[string]int{"count": count})
	})
	return nil
}

func notesDefinition() component.Definition {
	return component.Definition{ID: "demo/notes", Version: "3.0.0",
		Authz:      sectest.Defs("demo/notes", "demo.notes", "demo.notes.read", "demo.notes.write"),
		Migrations: fstest.MapFS{"0001_notes.up.sql": {Data: []byte("CREATE TABLE notes (id uuid PRIMARY KEY);")}},
		New:        func(rt *runtime.Runtime) (component.Module, error) { return &notes{db: rt.DB}, nil }}
}

func TestDatabaseComponentMigratesAndServes(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "notes", "")
	vars := pgtest.MemberEnv(host, port, "notes")

	for i := 0; i < 2; i++ {
		var out syncBuffer
		code := Run(context.Background(), notesDefinition(), Options{Args: []string{"migrate"}, Env: env(vars), Stdout: &out, Stderr: &out})
		require.Equal(t, ExitOK, code, out.String())
		assert.Contains(t, out.String(), `"msg":"migrate done"`)
	}

	httpPort := freePort(t)
	base := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	serveVars := maps.Clone(vars)
	maps.Copy(serveVars, map[string]string{"COMPONENT_ID": "demo/notes", "COMPONENT_VERSION": "3.0.0",
		"PORT": strconv.Itoa(httpPort), "STOP_DRAIN_DELAY": "50ms"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	var stdout syncBuffer
	go func() {
		done <- Run(ctx, notesDefinition(), Options{Env: env(secured(t, notesDefinition(), serveVars)), Stdout: &stdout, Stderr: io.Discard})
	}()
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 10*time.Second, 20*time.Millisecond, stdout.String())

	_, body := get(t, base+"/api/notes/count")
	assert.JSONEq(t, `{"count":0}`, string(body))
	assert.Equal(t, 201, post(t, base+"/api/notes"))
	_, body = get(t, base+"/api/notes/count")
	assert.JSONEq(t, `{"count":1}`, string(body))
	cancel()
	assert.Equal(t, ExitOK, <-done)
}

func TestDatabaseStartChecks(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "notes", "")
	base := secured(t, notesDefinition(), pgtest.MemberEnv(host, port, "notes"))
	maps.Copy(base, map[string]string{"COMPONENT_ID": "demo/notes", "COMPONENT_VERSION": "3.0.0", "PORT": strconv.Itoa(freePort(t))})

	wrong := maps.Clone(base)
	wrong["PG_PASSWORD"] = "not-the-password"
	var out syncBuffer
	assert.Equal(t, ExitConfiguration, Run(context.Background(), notesDefinition(), Options{Env: env(wrong), Stdout: &out, Stderr: &out}))
	assert.Contains(t, out.String(), "the database refused the credentials")
	assert.NotContains(t, out.String(), "not-the-password", "a secret never reaches the logs")

	missing := maps.Clone(base)
	missing["PG_DATABASE"] = "be_sdk_test_missing"
	out = syncBuffer{}
	assert.Equal(t, ExitStartupCheck, Run(context.Background(), notesDefinition(), Options{Env: env(missing), Stdout: &out, Stderr: &out}))

	// nothing listens: the component waits (starting) until stopped
	away := maps.Clone(base)
	away["PG_PORT"] = strconv.Itoa(freePort(t))
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	out = syncBuffer{}
	assert.Equal(t, ExitOK, Run(ctx, notesDefinition(), Options{Env: env(away), Stdout: &out, Stderr: &out}))
	assert.Contains(t, out.String(), "waiting for the database")

	noOwner := maps.Clone(base)
	delete(noOwner, "PG_OWNER_PASSWORD")
	out = syncBuffer{}
	assert.Equal(t, ExitConfiguration, Run(context.Background(), notesDefinition(), Options{Args: []string{"migrate"}, Env: env(noOwner), Stdout: &out, Stderr: &out}))
	assert.Contains(t, out.String(), `"key":"PG_OWNER_PASSWORD"`)
}

func post(t *testing.T, url string) int {
	req, _ := http.NewRequest(http.MethodPost, url, nil)
	authorize(t, req)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}
