package besdk

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/gin-gonic/gin"
)

var jobsMigrations = fstest.MapFS{
	"0001_things.up.sql":   {Data: []byte("CREATE TABLE things (id uuid PRIMARY KEY, name text NOT NULL);")},
	"0001_things.down.sql": {Data: []byte("DROP TABLE things;")},
	"lifecycle.yaml":       {Data: []byte("lifecycle: v1\ntables:\n  things: {class: master}\n")},
}

// jobsSpec declares a cron job, a queue worker fed by a route, and counts their runs.
func jobsSpec(cron, queued *atomic.Int64) Spec {
	return Spec{ID: "test/thing", Manifest: []byte(dbManifest), Migrations: jobsMigrations,
		New: func(_ context.Context, rt *Runtime) (*Module, error) {
			store, err := rt.Store()
			if err != nil {
				return nil, err
			}
			return &Module{
				Jobs: []Job{{Name: "thing.tick", Kind: Cron, Cron: "@every 1s", Timeout: 5 * time.Second,
					Run: func(context.Context) error { cron.Add(1); return nil }}},
				Workers: []Worker{{Kind: "thing.queued", Timeout: 5 * time.Second,
					Run: func(context.Context, QueuedJob) error { queued.Add(1); return nil }}},
				HTTP: func(r *Router) {
					POST(r, "/enqueue", Public, func(c *gin.Context) {
						err := store.Tx(c.Request.Context(), func(ctx context.Context, tx *Tx) error {
							return tx.Enqueue(ctx, "thing.queued", map[string]string{"x": "1"}, EnqueueOptions{UniqueKey: "k1"})
						})
						if err != nil {
							Fail(c, err)
							return
						}
						Respond(c, 202, map[string]bool{"ok": true})
					})
				},
			}, nil
		}}
}

func TestJobsRunThroughTheRuntime(t *testing.T) {
	id := testpg.New(t)
	env := dbEnv(t, id)
	var cron, queued atomic.Int64
	if code, out := runOnce(t, jobsSpec(&cron, &queued), env, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %d\n%s", code, out)
	}
	p := startProc(t, jobsSpec(&cron, &queued), env)
	waitReady(t, p)
	resp, err := httpPost(fmt.Sprintf("http://127.0.0.1:%d/test/thing/enqueue", p.port))
	if err != nil || resp.code != 202 {
		t.Fatalf("enqueue: %v %+v", err, resp)
	}
	deadline := time.Now().Add(15 * time.Second)
	for (cron.Load() == 0 || queued.Load() == 0) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if cron.Load() == 0 || queued.Load() != 1 {
		t.Fatalf("cron runs %d, queued runs %d\n%s", cron.Load(), queued.Load(), p.out.String())
	}
	if _, info := p.get(t, "/_be/info"); !strings.Contains(info, `"job_run"`) {
		t.Fatalf("capabilities: %s", info)
	}
}

// P14.8: `job run <name>` runs one job once through the same tables and exits 0; an unknown name 64;
// JOBS_OVERRIDES enabled:false does not stop it.
func TestJobRunEntryPoint(t *testing.T) {
	id := testpg.New(t)
	env := dbEnv(t, id)
	var cron, queued atomic.Int64
	if code, out := runOnce(t, jobsSpec(&cron, &queued), env, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %d\n%s", code, out)
	}
	if code, out := runOnce(t, jobsSpec(&cron, &queued), env, "job", "run", "thing.tick"); code != 0 || cron.Load() != 1 {
		t.Fatalf("job run: %d, runs %d\n%s", code, cron.Load(), out)
	}
	if code, out := runOnce(t, jobsSpec(&cron, &queued), env, "job", "run", "no.such"); code != 64 {
		t.Fatalf("unknown job: %d\n%s", code, out)
	}
}
