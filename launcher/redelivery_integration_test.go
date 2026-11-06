//go:build integration

package launcher

import (
	"context"
	"fmt"
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
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var redeliv = events.Stream{Name: "demo.redeliv", Partitions: 1}

type rdProducer struct{ rt *runtime.Runtime }

func (p *rdProducer) Routes(r *httpx.Routes) error {
	r.POST("/api/r", "demo.redeliv.create", func(c *httpx.Context) error {
		id := ids.New().String()
		err := p.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			_, err := p.rt.Events.Emit(ctx, tx, redeliv.Name, events.Event{Type: "demo.redeliv.created.v1", RecordID: id,
				Version: 1, Projection: events.Full, Data: map[string]string{"id": id}})
			return err
		})
		if err != nil {
			return err
		}
		return c.JSON(201, map[string]string{"id": id})
	})
	return nil
}

type rdConsumer struct{ rt *runtime.Runtime }

func (c *rdConsumer) Routes(r *httpx.Routes) error {
	r.GET("/api/rseen", "demo.rseen.read", func(x *httpx.Context) error {
		var n int
		err := c.rt.DB.Tx(x.Ctx(), func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT count(*) FROM rseen").Scan(&n)
		})
		if err != nil {
			return err
		}
		return x.JSON(200, map[string]int{"count": n})
	})
	return nil
}

func (c *rdConsumer) Subscriptions(s *events.Subscriptions) error {
	s.Add(events.Sub{Name: "redeliv", Streams: []string{redeliv.Name},
		Handle: func(ctx context.Context, tx db.Tx, e events.Received) error {
			_, err := tx.Exec(ctx, "INSERT INTO rseen (event_id) VALUES ($1)", e.ID) // twice would fail: applied once
			return err
		},
		Repair: func(ctx context.Context, k events.SourceKey) error { // a read model's snapshot repair, recorded
			return c.rt.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
				_, err := tx.Exec(ctx, "INSERT INTO repaired (source, partition) VALUES ($1, $2)", k.Component, k.Partition)
				return err
			})
		}})
	return nil
}

// runLogged is runComponent with the member's logs kept for the test.
func runLogged(t *testing.T, def component.Definition, vars0 map[string]string) (string, *syncBuffer, func()) {
	var out syncBuffer
	require.Equal(t, ExitOK, Run(context.Background(), def, Options{Args: []string{"migrate"}, Env: env(vars0), Stdout: &out, Stderr: &out}), out.String())
	httpPort := freePort(t)
	vars := maps.Clone(vars0)
	maps.Copy(vars, map[string]string{"COMPONENT_ID": def.ID, "COMPONENT_VERSION": def.Version, "PORT": strconv.Itoa(httpPort),
		"STOP_DRAIN_DELAY": "50ms"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	logs := &syncBuffer{}
	go func() { done <- Run(ctx, def, Options{Env: env(secured(t, def, vars)), Stdout: logs, Stderr: logs}) }()
	base := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 10*time.Second, 20*time.Millisecond, logs.String())
	return base, logs, func() {
		cancel()
		assert.Equal(t, ExitOK, <-done, logs.String())
	}
}

// T7d of foundations item 4 (P6.36-P6.40): a consumer's database goes back to an older backup. At its next start it
// notices from the epoch, asks the producer for a redelivery from its progress (the cut by runs and the delivery
// lifetime, not the largest position handled), and ends with every event applied once; when the rows after its
// progress were purged at the producer, the gap is recorded and the read model repaired.
func TestRedeliveryAfterTheConsumersDatabaseWentBack(t *testing.T) {
	admin := os.Getenv("BE_SDK_TEST_PG_ADMIN")
	natsURL := os.Getenv("BE_SDK_TEST_NATS_URL")
	require.NotEmpty(t, natsURL, "BE_SDK_TEST_NATS_URL is not set")
	u, err := url.Parse(admin)
	require.NoError(t, err)
	u.Scheme, u.Path = "pgqueue", "/"+pgtest.Database
	q := u.Query()
	schema := "be_bus_rd_" + ids.New().String()[24:]
	q.Set("schema", schema)
	u.RawQuery = q.Encode()
	pgqueueURL := u.String()
	pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Exec(t, pgtest.Admin(t, pgtest.Database), "CREATE SCHEMA "+schema)
	t.Cleanup(func() { pgtest.Exec(t, pgtest.Admin(t, pgtest.Database), "DROP SCHEMA "+schema+" CASCADE") })
	require.NoError(t, pgqueue.Migrate(context.Background(), pgqueueURL, slog.New(slog.NewTextHandler(io.Discard, nil))))

	for name, busURL := range map[string]string{"jetstream": natsURL, "pgqueue": pgqueueURL} {
		t.Run(name, func(t *testing.T) {
			if name == "jetstream" {
				tr, err := jetstream.Connect(busURL, "cleanup")
				require.NoError(t, err)
				_ = tr.DeleteStream(context.Background(), redeliv.Name)
				t.Cleanup(func() { _ = tr.DeleteStream(context.Background(), redeliv.Name); _ = tr.Close() })
			}
			host, port := pgtest.EnsureDatabase(t, pgtest.Database)
			pgtest.Member(t, "rdprod", "")
			pgtest.Member(t, "rdcons", "")
			admin := pgtest.Admin(t, pgtest.Database) // one connection: a new one each poll runs the instance out of clients
			sql := func(format string, a ...any) {
				t.Helper()
				pgtest.Exec(t, admin, fmt.Sprintf(format, a...))
			}
			count := func(q string) int {
				t.Helper()
				var n int
				require.NoError(t, admin.QueryRow(context.Background(), q).Scan(&n))
				return n
			}
			grpcPort := freePort(t)
			prodEnv := pgtest.MemberEnv(host, port, "rdprod")
			prodEnv["BUS_URL"], prodEnv["IMPLEMENTATION_INSTANCE"] = busURL, ids.New().String()
			consEnv := pgtest.MemberEnv(host, port, "rdcons")
			consEnv["BUS_URL"] = busURL
			consEnv["DEMO_RDPROD_GRPC_ENDPOINT"] = "127.0.0.1:" + strconv.Itoa(grpcPort)
			prodDef := component.Definition{ID: "demo/rdprod", Version: "3.0.0", Recovery: "demo.redeliv.recover", Streams: []events.Stream{redeliv},
				Resend: "demo.redeliv.resend", GRPCPort: grpcPort,
				Authz:      sectest.Defs("demo/rdprod", "demo.redeliv", "demo.redeliv.create", "demo.redeliv.resend", "demo.redeliv.recover"),
				Migrations: fstest.MapFS{"0001_none.up.sql": {Data: []byte("SELECT 1;")}},
				New:        func(rt *runtime.Runtime) (component.Module, error) { return &rdProducer{rt: rt}, nil }}
			consDef := component.Definition{ID: "demo/rdcons", Version: "3.0.0", Recovery: "demo.rseen.recover", GRPCPort: portNow(), Subscribes: true,
				Authz: sectest.Defs("demo/rdcons", "demo.rseen", "demo.rseen.read", "demo.rseen.recover"),
				Migrations: fstest.MapFS{"0001_rseen.up.sql": {Data: []byte(`CREATE TABLE rseen (event_id uuid PRIMARY KEY);
					CREATE TABLE repaired (source text NOT NULL, partition int NOT NULL);`)}},
				New: func(rt *runtime.Runtime) (component.Module, error) { return &rdConsumer{rt: rt}, nil }}
			seen := func(url string) int {
				_, body := get(t, url+"/api/rseen")
				var n int
				_, _ = fmt.Sscanf(strings.TrimSpace(string(body)), `{"count":%d}`, &n)
				return n
			}

			prodURL, prodLogs, stopProd := runLogged(t, prodDef, prodEnv)
			defer stopProd()
			consURL, _, stopCons := runLogged(t, consDef, consEnv)
			for i := 0; i < 5; i++ {
				require.Equal(t, 201, post(t, prodURL+"/api/r"))
			}
			require.Eventually(t, func() bool { return seen(consURL) == 5 }, 15*time.Second, 50*time.Millisecond)
			stopCons()
			// the backup: the run long ago, the five handled five minutes apart, the last 25 minutes after the start
			// (the progress is the third: handled at least L = 10 minutes before the last handling)
			sql(`UPDATE rdcons.be_subscription_run SET started_at = now() - interval '2 hours'`)
			sql(`WITH o AS (SELECT event_id, row_number() OVER (ORDER BY seq) k FROM rdcons.be_inbox)
				UPDATE rdcons.be_inbox i SET processed_at = now() - interval '2 hours' + o.k * interval '5 minutes'
				FROM o WHERE i.event_id = o.event_id`)
			for _, tbl := range []string{"be_inbox", "rseen", "be_subscription_run"} {
				sql(`CREATE TABLE rdcons.bak_%s AS SELECT * FROM rdcons.%s`, tbl, tbl)
			}
			require.Equal(t, 5, count(`SELECT count(*) FROM rdcons.be_inbox WHERE seq IS NOT NULL AND source_component = 'demo/rdprod'`),
				"the inbox keeps the source position (P6.36)")

			consURL, consLogs, stopCons := runLogged(t, consDef, consEnv)
			for i := 0; i < 5; i++ {
				require.Equal(t, 201, post(t, prodURL+"/api/r"))
			}
			require.Eventually(t, func() bool { return seen(consURL) == 10 }, 15*time.Second, 50*time.Millisecond)
			assert.NotContains(t, consLogs.String(), "event_resend_needed", "the epoch did not change")
			stopCons()

			// the database goes back to the backup: its tables as they were, the recovery generation advanced (P6.7)
			back := func() {
				for _, tbl := range []string{"be_inbox", "rseen", "be_subscription_run"} {
					sql(`DELETE FROM rdcons.%s; INSERT INTO rdcons.%s OVERRIDING SYSTEM VALUE SELECT * FROM rdcons.bak_%s`,
						tbl, tbl, tbl)
				}
				sql(`UPDATE rdcons.be_recovery_generation SET generation = generation + 1`)
			}
			back()
			consURL, consLogs, stopCons = runLogged(t, consDef, consEnv)
			require.Eventually(t, func() bool { return seen(consURL) == 10 }, 20*time.Second, 50*time.Millisecond,
				"every event once again: the five lost with the backup redelivered")
			assert.Contains(t, consLogs.String(), "event_resend_needed")
			require.Eventually(t, func() bool { return strings.Contains(prodLogs.String(), `"event":"event_resent"`) },
				5*time.Second, 50*time.Millisecond)
			assert.Contains(t, prodLogs.String(), `"resent":7`, "from the progress (the third), not from the largest (the fifth)")
			require.Eventually(t, func() bool {
				return count(`SELECT count(*) FROM rdcons.be_consumer_epoch e, rdcons.be_recovery_generation g
					WHERE e.epoch_generation = g.generation`) == 1
			}, 5*time.Second, 50*time.Millisecond, "the epoch recorded once the producer answered")
			assert.Zero(t, count(`SELECT count(*) FROM rdcons.be_resend_gap`))
			stopCons()

			// the rows after the progress purged at the producer: the gap is recorded and the read model repaired
			sql(`DELETE FROM rdprod.be_outbox WHERE seq <= 5`)
			sql(`UPDATE rdprod.be_outbox_partition SET purged_generation = epoch_generation, purged_timeline = epoch_timeline,
				purged_seq = 5`)
			back()
			consURL, consLogs, stopCons = runLogged(t, consDef, consEnv)
			defer stopCons()
			require.Eventually(t, func() bool { return count(`SELECT count(*) FROM rdcons.be_resend_gap`) == 1 },
				20*time.Second, 50*time.Millisecond)
			assert.Equal(t, 1, count(`SELECT count(*) FROM rdcons.be_resend_gap WHERE kind = 'beyond_retention'
				AND subscription = 'redeliv' AND source_component = 'demo/rdprod' AND progress LIKE '%/3'`))
			assert.Contains(t, consLogs.String(), `"event":"event_resend_beyond_retention"`)
			// the gap is committed first and the repair runs after it (a repair that fails leaves the gap recorded)
			require.Eventually(t, func() bool { return count(`SELECT count(*) FROM rdcons.repaired`) == 1 }, 10*time.Second,
				50*time.Millisecond, "the read model's repair ran")
			require.Eventually(t, func() bool { return seen(consURL) == 10 }, 20*time.Second, 50*time.Millisecond,
				"what is still kept arrives (6 to 10); 4 and 5 were handled already, but after the cut: the gap from the "+
					"progress (3) to the purge (5) is reported all the same, the consumer cannot know what it may lack there")
		})
	}
}
