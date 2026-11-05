//go:build integration

package launcher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/pgqueue"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rcProducer is rdProducer with a job of its own that leaves a row each run, a read, and a write served while open
// read-only.
type rcProducer struct{ rdProducer }

func (p *rcProducer) Routes(r *httpx.Routes) error {
	if err := p.rdProducer.Routes(r); err != nil {
		return err
	}
	r.GET("/api/ticks", "demo.redeliv.create", func(c *httpx.Context) error { return c.JSON(200, map[string]int{}) })
	r.Add(httpx.Route{Method: "POST", Path: "/api/code", Operation: "demo.redeliv.create", ReadOnlyOpen: true,
		Handle: func(c *httpx.Context) error { return c.JSON(200, map[string]int{}) }})
	return nil
}

func (p *rcProducer) Jobs(j *work.Jobs) error {
	j.Add(work.Job{Name: "demo.tick", Kind: work.Every, Effect: work.Local, Interval: 100 * time.Millisecond,
		Handle: func(ctx context.Context, run *work.Run) error {
			if err := p.rt.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
				_, err := tx.Exec(ctx, "INSERT INTO ticks DEFAULT VALUES")
				return err
			}); err != nil {
				return err
			}
			return run.Complete(ctx, nil)
		}})
	return nil
}

// T8b of foundations item 4 (P1.22-P1.24): a member started with RECOVERY_PHASE is recovery_ready (its readiness
// answers 200 with the phase, it says the phase in its self-description); frozen runs no forwarding, subscription or
// job and answers MAINTENANCE on the user surface; redelivering runs forwarding and subscriptions and still no job;
// without the phase everything runs again. A phase it does not know is a configuration error.
func TestRecoveryPhases(t *testing.T) {
	admin := os.Getenv("BE_SDK_TEST_PG_ADMIN")
	u, err := url.Parse(admin)
	require.NoError(t, err)
	u.Scheme, u.Path = "pgqueue", "/"+pgtest.Database
	q := u.Query()
	schema := "be_bus_rc_" + ids.New().String()[24:]
	q.Set("schema", schema)
	u.RawQuery = q.Encode()
	busURL := u.String()
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Exec(t, pgtest.Admin(t, pgtest.Database), "CREATE SCHEMA "+schema)
	t.Cleanup(func() { pgtest.Exec(t, pgtest.Admin(t, pgtest.Database), "DROP SCHEMA "+schema+" CASCADE") })
	require.NoError(t, pgqueue.Migrate(context.Background(), busURL, slog.New(slog.NewTextHandler(io.Discard, nil))))
	pgtest.Member(t, "rcprod", "")
	pgtest.Member(t, "rccons", "")
	conn := pgtest.Admin(t, pgtest.Database) // one connection: a new one each poll ran the instance out of clients
	count := func(q string) int {
		t.Helper()
		var n int
		require.NoError(t, conn.QueryRow(context.Background(), q).Scan(&n))
		return n
	}
	prodEnv := pgtest.MemberEnv(host, port, "rcprod")
	prodEnv["BUS_URL"], prodEnv["IMPLEMENTATION_INSTANCE"] = busURL, ids.New().String()
	consEnv := pgtest.MemberEnv(host, port, "rccons")
	consEnv["BUS_URL"] = busURL
	prodDef := component.Definition{ID: "demo/rcprod", Version: "3.0.0", Recovery: "demo.redeliv.recover", Streams: []events.Stream{redeliv},
		Resend: "demo.redeliv.resend", GRPCPort: freePort(t),
		Authz:      sectest.Defs("demo/rcprod", "demo.redeliv", "demo.redeliv.create", "demo.redeliv.resend", "demo.redeliv.recover"),
		Migrations: fstest.MapFS{"0001_ticks.up.sql": {Data: []byte("CREATE TABLE ticks (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY);")}},
		New:        func(rt *runtime.Runtime) (component.Module, error) { return &rcProducer{rdProducer{rt: rt}}, nil }}
	consDef := component.Definition{ID: "demo/rccons", Version: "3.0.0", Recovery: "demo.rseen.recover", GRPCPort: portNow(), Subscribes: true,
		Authz: sectest.Defs("demo/rccons", "demo.rseen", "demo.rseen.read", "demo.rseen.recover"),
		Migrations: fstest.MapFS{"0001_rseen.up.sql": {Data: []byte(`CREATE TABLE rseen (event_id uuid PRIMARY KEY);
			CREATE TABLE repaired (source text NOT NULL, partition int NOT NULL);`)}},
		New: func(rt *runtime.Runtime) (component.Module, error) { return &rdConsumer{rt: rt}, nil }}
	phased := func(vars map[string]string, phase string) map[string]string {
		out := maps.Clone(vars)
		if phase != "" {
			out["RECOVERY_PHASE"] = phase
		}
		return out
	}
	readiness := func(base string) (int, map[string]any) {
		code, body := get(t, base+"/readyz")
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		return code, m
	}

	// running: two events go through, the job ticks
	prodURL, _, stopProd := runLogged(t, prodDef, phased(prodEnv, ""))
	consURL, _, stopCons := runLogged(t, consDef, phased(consEnv, ""))
	require.Equal(t, 201, post(t, prodURL+"/api/r"))
	require.Equal(t, 201, post(t, prodURL+"/api/r"))
	require.Eventually(t, func() bool { return count("SELECT count(*) FROM rccons.rseen") == 2 }, 10*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return count("SELECT count(*) FROM rcprod.ticks") > 0 }, 10*time.Second, 50*time.Millisecond)
	stopCons()
	stopProd()

	// frozen: readiness 200 with the phase; the user surface answers MAINTENANCE; nothing is forwarded, received or run
	pgtest.Exec(t, pgtest.Admin(t, pgtest.Database), `INSERT INTO rcprod.be_outbox (event_id, stream, partition, type,
		occurred_at, record_id, version, projection, hops, payload, digest) SELECT gen_random_uuid(), stream, partition, type,
		occurred_at, gen_random_uuid()::text, version, projection, hops, payload, digest FROM rcprod.be_outbox LIMIT 1`)
	ticks := count("SELECT count(*) FROM rcprod.ticks")
	runs := count("SELECT count(*) FROM rccons.be_subscription_run")
	prodURL, prodLogs, stopProd := runLogged(t, prodDef, phased(prodEnv, "frozen"))
	assert.Regexp(t, `"event":"ready".*"phase":"frozen"`, prodLogs.String(), "the ready line says the phase (P1.22)")
	consURL, _, stopCons = runLogged(t, consDef, phased(consEnv, "frozen"))
	code, ready := readiness(prodURL)
	assert.Equal(t, 200, code, "recovery_ready is routable")
	assert.Equal(t, map[string]any{"state": "recovery_ready", "phase": "frozen"}, ready)
	_, self := get(t, prodURL+"/.well-known/be-component")
	assert.Contains(t, string(self), `"recovery_phase":"frozen"`)
	code, body := postBody(t, prodURL+"/api/r")
	assert.Equal(t, 503, code)
	assert.Contains(t, body, `"reason":"MAINTENANCE"`)
	assert.Contains(t, body, `"phase":"frozen"`)
	time.Sleep(1500 * time.Millisecond) // the forwarder's poll, the job's interval, many times over
	assert.Equal(t, 1, count("SELECT count(*) FROM rcprod.be_outbox WHERE seq IS NULL"), "not forwarded")
	assert.Equal(t, ticks, count("SELECT count(*) FROM rcprod.ticks"), "no job")
	assert.Equal(t, runs, count("SELECT count(*) FROM rccons.be_subscription_run"), "no subscription")
	stopCons()
	stopProd()

	// redelivering: forwarding and subscriptions run, the user surface still answers MAINTENANCE, still no job
	prodURL, _, stopProd = runLogged(t, prodDef, phased(prodEnv, "redelivering"))
	consURL, _, stopCons = runLogged(t, consDef, phased(consEnv, "redelivering"))
	_, ready = readiness(consURL)
	assert.Equal(t, "redelivering", ready["phase"])
	require.Eventually(t, func() bool { return count("SELECT count(*) FROM rccons.rseen") == 3 }, 10*time.Second,
		50*time.Millisecond, "the row written while frozen is forwarded and received")
	code, _ = postBody(t, prodURL+"/api/r")
	assert.Equal(t, 503, code)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, ticks, count("SELECT count(*) FROM rcprod.ticks"), "still no job")
	stopCons()
	stopProd()

	// replaying (step 6): as redelivering, forwarding and subscriptions run, still no job
	pgtest.Exec(t, pgtest.Admin(t, pgtest.Database), `INSERT INTO rcprod.be_outbox (event_id, stream, partition, type,
		occurred_at, record_id, version, projection, hops, payload, digest) SELECT gen_random_uuid(), stream, partition, type,
		occurred_at, gen_random_uuid()::text, version, projection, hops, payload, digest FROM rcprod.be_outbox LIMIT 1`)
	prodURL, _, stopProd = runLogged(t, prodDef, phased(prodEnv, "replaying"))
	consURL, _, stopCons = runLogged(t, consDef, phased(consEnv, "replaying"))
	_, ready = readiness(prodURL)
	assert.Equal(t, "replaying", ready["phase"])
	// the redelivering run before held the partitions' forwarding leases (10 s, P6.11): this run waits them out
	require.Eventually(t, func() bool { return count("SELECT count(*) FROM rccons.rseen") == 4 }, 25*time.Second,
		50*time.Millisecond, "forwarded and received while replaying")
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, ticks, count("SELECT count(*) FROM rcprod.ticks"), "no job while replaying")
	stopCons()
	stopProd()

	// verifying (steps 7-9): forwarding and subscriptions run; nothing on the user surface, not even reads; no job
	pgtest.Exec(t, pgtest.Admin(t, pgtest.Database), `INSERT INTO rcprod.be_outbox (event_id, stream, partition, type,
		occurred_at, record_id, version, projection, hops, payload, digest) SELECT gen_random_uuid(), stream, partition, type,
		occurred_at, gen_random_uuid()::text, version, projection, hops, payload, digest FROM rcprod.be_outbox LIMIT 1`)
	prodURL, _, stopProd = runLogged(t, prodDef, phased(prodEnv, "verifying"))
	consURL, _, stopCons = runLogged(t, consDef, phased(consEnv, "verifying"))
	_, ready = readiness(prodURL)
	assert.Equal(t, "verifying", ready["phase"])
	require.Eventually(t, func() bool { return count("SELECT count(*) FROM rccons.rseen") == 5 }, 25*time.Second,
		50*time.Millisecond, "forwarded and received while verifying")
	code, _ = get(t, prodURL+"/api/ticks")
	assert.Equal(t, 503, code, "no reads while verifying")
	code, _ = postBody(t, prodURL+"/api/code")
	assert.Equal(t, 503, code)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, ticks, count("SELECT count(*) FROM rcprod.ticks"), "no job while verifying")
	stopCons()
	stopProd()

	// read_only (the read-only opening): reads and the writes a route declares open; other writes MAINTENANCE; no job
	prodURL, _, stopProd = runLogged(t, prodDef, phased(prodEnv, "read_only"))
	code, _ = get(t, prodURL+"/api/ticks")
	assert.Equal(t, 200, code, "a read")
	code, _ = postBody(t, prodURL+"/api/code")
	assert.Equal(t, 200, code, "a write declared ReadOnlyOpen")
	code, body = postBody(t, prodURL+"/api/r")
	assert.Equal(t, 503, code)
	assert.Contains(t, body, `"phase":"read_only"`)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, ticks, count("SELECT count(*) FROM rcprod.ticks"), "no job while open read-only")
	stopProd()

	// running again: everything
	prodURL, _, stopProd = runLogged(t, prodDef, phased(prodEnv, ""))
	defer stopProd()
	code, _ = get(t, prodURL+"/readyz")
	assert.Equal(t, 200, code)
	_, self = get(t, prodURL+"/.well-known/be-component")
	assert.NotContains(t, string(self), "recovery_phase")
	assert.Equal(t, 201, post(t, prodURL+"/api/r"))
	require.Eventually(t, func() bool { return count("SELECT count(*) FROM rcprod.ticks") > ticks }, 10*time.Second, 50*time.Millisecond)

	// a phase this version does not know: a configuration error before any port opens (P1.9, P2.6)
	var out syncBuffer
	bad := phased(prodEnv, "thawing")
	maps.Copy(bad, map[string]string{"COMPONENT_ID": prodDef.ID, "COMPONENT_VERSION": prodDef.Version,
		"PORT": strconv.Itoa(freePort(t))})
	assert.Equal(t, ExitConfiguration, Run(context.Background(), prodDef, Options{Env: env(secured(t, prodDef, bad)),
		Stdout: &out, Stderr: &out}), out.String())
}

// postBody is post with the answer's body.
func postBody(t *testing.T, url string) (int, string) {
	req, _ := http.NewRequest(http.MethodPost, url, nil)
	authorize(t, req)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}
