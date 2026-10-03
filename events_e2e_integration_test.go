package besdk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
)

// A component that publishes one event from a request and consumes it itself: outbox in the business
// transaction, pump, durable created at migration, Apply in the cursor's transaction (P12.1–P12.7).
func TestPublishAndConsumeThroughRuntime(t *testing.T) {
	natsURL := envOrSkip(t, "TEST_NATS_URL")
	id := testpg.New(t)
	seg := "t" + randomHex(t) // a stream of our own on the shared server
	subject := seg + ".thing.created.v1"
	manifest := strings.Replace(dbManifest, "    SHUTDOWN_GRACE:", "    NATS_URL: {type: string}\n    EVENTS_MAX_DELIVER: {type: integer, default: 8}\n    EVENTS_BACKOFF: {type: string, default: \"1s,10s,1m,5m,15m,30m,1h\"}\n    SHUTDOWN_GRACE:", 1) +
		fmt.Sprintf("events:\n  publishes: [%s]\n  subscribes: [%s]\n", subject, subject)
	contracts := fstest.MapFS{"events/thing.events.json": {Data: []byte(fmt.Sprintf(`{"events": [{"subject": %q,
		"x-aggregate-type": "%s.thing.thing", "x-consumption": "state",
		"payload": {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}}}]}`, subject, seg))}}
	migrations := fstest.MapFS{
		"0001_things.up.sql":   {Data: []byte("CREATE TABLE things (id uuid PRIMARY KEY, name text NOT NULL); CREATE TABLE seen (name text PRIMARY KEY, hop int NOT NULL);")},
		"0001_things.down.sql": {Data: []byte("DROP TABLE seen; DROP TABLE things;")},
	}
	spec := Spec{ID: "test/thing", Manifest: []byte(manifest), Migrations: migrations, Contracts: contracts,
		New: func(_ context.Context, rt *Runtime) (*Module, error) {
			store, err := rt.Store()
			if err != nil {
				return nil, err
			}
			return &Module{
				HTTP: func(r *Router) {
					POST(r, "/things/:name", Public, func(c *gin.Context) {
						err := store.Tx(c.Request.Context(), func(ctx context.Context, tx *Tx) error {
							return tx.Publish(ctx, Event{Subject: subject, AggregateID: c.Param("name"), Version: 1,
								Payload: map[string]string{"name": c.Param("name")}})
						})
						if err != nil {
							Fail(c, err)
							return
						}
						Respond(c, 202, "ok")
					})
				},
				Events: Events{Publishes: []string{subject}, Subscribe: []Subscription{{Subject: subject,
					Apply: func(ctx context.Context, tx *Tx, ev Event) error {
						p, err := Decode[struct{ Name string }](ev)
						if err != nil {
							return err
						}
						_, err = tx.ExecContext(ctx, `INSERT INTO seen (name, hop) VALUES ($1, $2)`, p.Name, ev.HopCount)
						return err
					}}}},
			}, nil
		}}
	env := dbEnv(t, id)
	env["NATS_URL"] = natsURL
	t.Cleanup(func() { deleteStream(natsURL, "BE_"+strings.ToUpper(seg)) })
	if code, out := runOnce(t, spec, env, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %d\n%s", code, out)
	}
	p := startProc(t, spec, env)
	waitReady(t, p)
	if r, err := httpPost(fmt.Sprintf("http://127.0.0.1:%d/test/thing/things/a", p.port)); err != nil || r.code != 202 {
		t.Fatalf("publish request: %v %+v", err, r)
	}
	db := testpg.Open(t, id.SuperDSN)
	deadline := time.Now().Add(15 * time.Second)
	for {
		var n int
		_ = db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s.seen WHERE name = 'a' AND hop = 0`, id.Schema)).Scan(&n)
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("event not consumed\n%s", p.out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	var cursor int
	_ = db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s.besdk_event_cursor`, id.Schema)).Scan(&cursor)
	if cursor != 1 {
		t.Fatalf("cursor rows %d", cursor)
	}
}

func waitReady(t *testing.T, p *testProc) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		code, body := p.get(t, "/readyz")
		if code == 200 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never ready: %s\n%s", body, p.out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func envOrSkip(t *testing.T, key string) string {
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s not set", key)
	}
	return v
}

func randomHex(t *testing.T) string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func deleteStream(url, name string) {
	nc, err := nats.Connect(url)
	if err != nil {
		return
	}
	defer nc.Close()
	js, err := natsjs.New(nc)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = js.DeleteStream(ctx, name)
}
