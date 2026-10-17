//go:build integration

package launcher

import (
	"context"
	"maps"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/internal/sectest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type reports struct{ rt *runtime.Runtime }

func (r *reports) Routes(rs *httpx.Routes) error {
	rs.POST("/api/reports", "demo.reports.create", func(c *httpx.Context) error {
		return r.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			_, err := r.rt.Work.Enqueue(ctx, tx, "build-report", map[string]string{"id": ids.New().String()})
			return err
		})
	})
	rs.GET("/api/reports/count", "demo.reports.read", func(c *httpx.Context) error {
		var n int
		err := r.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT count(*) FROM reports").Scan(&n)
		})
		if err != nil {
			return err
		}
		return c.JSON(200, map[string]int{"count": n})
	})
	return nil
}

func (r *reports) Jobs(j *work.Jobs) error {
	j.Add(work.Job{Name: "build-report", Kind: work.Queue, Effect: work.Local, Handle: func(ctx context.Context, run *work.Run) error {
		return run.Complete(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := tx.Exec(ctx, "INSERT INTO reports (id) VALUES ($1)", run.ID)
			return err
		})
	}})
	return nil
}

func TestQueuedWorkRunsInTheComponent(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "reports", "")
	vars := pgtest.MemberEnv(host, port, "reports")
	def := component.Definition{ID: "demo/reports", Version: "3.0.0",
		Authz:      sectest.Defs("demo/reports", "demo.reports", "demo.reports.read", "demo.reports.create"),
		Migrations: fstest.MapFS{"0001_reports.up.sql": {Data: []byte("CREATE TABLE reports (id text PRIMARY KEY);")}},
		New:        func(rt *runtime.Runtime) (component.Module, error) { return &reports{rt: rt}, nil }}
	var out syncBuffer
	require.Equal(t, ExitOK, Run(context.Background(), def, Options{Args: []string{"migrate"}, Env: env(vars), Stdout: &out, Stderr: &out}), out.String())
	httpPort := freePort(t)
	serve := maps.Clone(vars)
	maps.Copy(serve, map[string]string{"COMPONENT_ID": def.ID, "COMPONENT_VERSION": def.Version, "PORT": strconv.Itoa(httpPort), "STOP_DRAIN_DELAY": "50ms"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- Run(ctx, def, Options{Env: env(secured(t, def, serve)), Stdout: &out, Stderr: &out}) }()
	base := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 10*time.Second, 20*time.Millisecond)
	for i := 0; i < 3; i++ {
		post(t, base+"/api/reports")
	}
	require.Eventually(t, func() bool {
		_, b := get(t, base+"/api/reports/count")
		return string(b) == "{\"count\":3}\n" || string(b) == `{"count":3}`
	},
		10*time.Second, 50*time.Millisecond)
	cancel()
	assert.Equal(t, ExitOK, <-done)
}

func TestPartitionsAreKeptAheadInTheComponent(t *testing.T) { // P5.24
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "ledgers", "")
	vars := pgtest.MemberEnv(host, port, "ledgers")
	def := component.Definition{ID: "demo/ledgers", Version: "3.0.0",
		Authz: sectest.Defs("demo/ledgers", "demo.reports", "demo.reports.read", "demo.reports.create"),
		Migrations: fstest.MapFS{"0001_ledger.up.sql": {Data: []byte(`
CREATE TABLE ledger (id uuid NOT NULL, posted_at timestamptz NOT NULL, PRIMARY KEY (id, posted_at)) PARTITION BY RANGE (posted_at);
SELECT be_declare_monthly_partitions('ledger', 1);`)}},
		New: func(rt *runtime.Runtime) (component.Module, error) { return &reports{rt: rt}, nil }}
	var out syncBuffer
	require.Equal(t, ExitOK, Run(context.Background(), def, Options{Args: []string{"migrate"}, Env: env(vars), Stdout: &out, Stderr: &out}), out.String())
	next := time.Now().UTC().AddDate(0, 0, -time.Now().UTC().Day()+1).AddDate(0, 1, 0).Format("200601")
	admin := pgtest.Admin(t, pgtest.Database)
	pgtest.Exec(t, admin, "DROP TABLE ledgers.ledger_p"+next)
	httpPort := freePort(t)
	serve := maps.Clone(vars)
	maps.Copy(serve, map[string]string{"COMPONENT_ID": def.ID, "COMPONENT_VERSION": def.Version, "PORT": strconv.Itoa(httpPort), "STOP_DRAIN_DELAY": "50ms"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- Run(ctx, def, Options{Env: env(secured(t, def, serve)), Stdout: &out, Stderr: &out}) }()
	require.Eventually(t, func() bool {
		var n int
		_ = admin.QueryRow(context.Background(), "SELECT count(*) FROM pg_class WHERE relname = $1 AND relnamespace = 'ledgers'::regnamespace",
			"ledger_p"+next).Scan(&n)
		return n == 1
	}, 10*time.Second, 50*time.Millisecond, "the maintenance job recreates next month's partition")
	cancel()
	assert.Equal(t, ExitOK, <-done)
	assert.Contains(t, out.String(), `"event":"partitions_created"`)
}
