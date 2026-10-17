//go:build integration

package launcher

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/jetstream"
	"github.com/brickKit/be-sdk-go/v3/events/pgqueue"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/internal/s3test"
	"github.com/brickKit/be-sdk-go/v3/internal/sectest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var things = events.Stream{Name: "demo.things", Partitions: 2}

type producer struct{ rt *runtime.Runtime }

func (p *producer) Routes(r *httpx.Routes) error {
	r.POST("/api/things", "demo.things.create", func(c *httpx.Context) error {
		id := ids.New().String()
		err := p.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			if _, err := tx.Exec(ctx, "INSERT INTO things VALUES ($1)", id); err != nil {
				return err
			}
			data := map[string]string{"id": id}
			if c.Request().URL.Query().Get("big") != "" { // over 64 KiB: through object storage (P6.14)
				data["blob"] = strings.Repeat("z", events.MaxPayload+1000)
			}
			_, err := p.rt.Events.Emit(ctx, tx, things.Name, events.Event{Type: "demo.things.created.v1", RecordID: id,
				Version: 1, Projection: events.Full, Data: data})
			return err
		})
		if err != nil {
			return err
		}
		return c.JSON(201, map[string]string{"id": id})
	})
	return nil
}

type consumer struct{ rt *runtime.Runtime }

func (c *consumer) Routes(r *httpx.Routes) error {
	r.GET("/api/seen", "demo.seen.read", func(x *httpx.Context) error {
		var n int
		err := c.rt.DB.Tx(x.Ctx(), func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT count(*) + count(*) FILTER (WHERE size > 65536) * 100 FROM seen").Scan(&n)
		})
		if err != nil {
			return err
		}
		return x.JSON(200, map[string]int{"count": n})
	})
	return nil
}

func (c *consumer) Subscriptions(s *events.Subscriptions) error {
	s.Add(events.Sub{Name: "things", Streams: []string{things.Name}, Types: []string{"demo.things.created.v1"},
		Handle: func(ctx context.Context, tx db.Tx, e events.Received) error {
			var data map[string]string
			if err := e.Decode(&data); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO seen (event_id, record_id, size) VALUES ($1, $2, $3)", e.ID, e.RecordID,
				len(data["blob"]))
			return err
		}})
	return nil
}

func runComponent(t *testing.T, def component.Definition, vars0 map[string]string) (string, func()) {
	var out syncBuffer
	require.Equal(t, ExitOK, Run(context.Background(), def, Options{Args: []string{"migrate"}, Env: env(vars0), Stdout: &out, Stderr: &out}), out.String())
	httpPort := freePort(t)
	vars := maps.Clone(vars0)
	maps.Copy(vars, map[string]string{"COMPONENT_ID": def.ID, "COMPONENT_VERSION": def.Version, "PORT": strconv.Itoa(httpPort),
		"STOP_DRAIN_DELAY": "50ms"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	var logs syncBuffer
	go func() { done <- Run(ctx, def, Options{Env: env(secured(t, def, vars)), Stdout: &logs, Stderr: &logs}) }()
	base := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 10*time.Second, 20*time.Millisecond, logs.String())
	return base, func() {
		cancel()
		assert.Equal(t, ExitOK, <-done, logs.String())
	}
}

func TestEventsFlowBetweenComponentsOnBothTransports(t *testing.T) {
	admin := os.Getenv("BE_SDK_TEST_PG_ADMIN")
	natsURL := os.Getenv("BE_SDK_TEST_NATS_URL")
	require.NotEmpty(t, natsURL, "BE_SDK_TEST_NATS_URL is not set")
	u, err := url.Parse(admin)
	require.NoError(t, err)
	u.Scheme, u.Path = "pgqueue", "/"+pgtest.Database
	q := u.Query()
	q.Set("schema", "be_bus_e2e")
	u.RawQuery = q.Encode()
	pgqueueURL := u.String()
	pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Exec(t, pgtest.Admin(t, pgtest.Database), "CREATE SCHEMA IF NOT EXISTS be_bus_e2e")
	require.NoError(t, pgqueue.Migrate(context.Background(), pgqueueURL, slog.New(slog.NewTextHandler(io.Discard, nil))))

	for name, busURL := range map[string]string{"jetstream": natsURL, "pgqueue": pgqueueURL} {
		t.Run(name, func(t *testing.T) {
			if name == "jetstream" {
				tr, err := jetstream.Connect(busURL, "cleanup")
				require.NoError(t, err)
				_ = tr.DeleteStream(context.Background(), things.Name)
				t.Cleanup(func() { _ = tr.DeleteStream(context.Background(), things.Name); _ = tr.Close() })
			}
			host, port := pgtest.EnsureDatabase(t, pgtest.Database)
			pgtest.Member(t, "evprod", "")
			pgtest.Member(t, "evcons", "")
			prodEnv := pgtest.MemberEnv(host, port, "evprod")
			prodEnv["BUS_URL"], prodEnv["IMPLEMENTATION_INSTANCE"] = busURL, ids.New().String()
			maps.Copy(prodEnv, s3test.Env(t, "demo-evprod/")) // the producer's own prefix; the consumer holds no credentials
			consEnv := pgtest.MemberEnv(host, port, "evcons")
			consEnv["BUS_URL"] = busURL
			prodDef := component.Definition{ID: "demo/evprod", Version: "3.0.0", Streams: []events.Stream{things},
				Authz:      sectest.Defs("demo/evprod", "demo.things", "demo.things.read", "demo.things.create"),
				Migrations: fstest.MapFS{"0001_things.up.sql": {Data: []byte("CREATE TABLE things (id uuid PRIMARY KEY);")}},
				New:        func(rt *runtime.Runtime) (component.Module, error) { return &producer{rt: rt}, nil }}
			consDef := component.Definition{ID: "demo/evcons", Version: "3.0.0", Subscribes: true,
				Authz:      sectest.Defs("demo/evcons", "demo.seen", "demo.seen.read"),
				Migrations: fstest.MapFS{"0001_seen.up.sql": {Data: []byte("CREATE TABLE seen (event_id uuid PRIMARY KEY, record_id text NOT NULL, size int NOT NULL);")}},
				New:        func(rt *runtime.Runtime) (component.Module, error) { return &consumer{rt: rt}, nil }}

			prodURL, stopProd := runComponent(t, prodDef, prodEnv)
			consURL, stopCons := runComponent(t, consDef, consEnv)
			for i := 0; i < 5; i++ {
				assert.Equal(t, 201, post(t, prodURL+"/api/things"))
			}
			assert.Equal(t, 201, post(t, prodURL+"/api/things?big=1"))
			require.Eventually(t, func() bool {
				_, body := get(t, consURL+"/api/seen")
				return strings.TrimSpace(string(body)) == `{"count":106}`
			}, 15*time.Second, 50*time.Millisecond, "six events, the large one with its whole payload")
			stopCons()
			stopProd()
		})
	}
}
