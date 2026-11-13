//go:build integration

package launcher

import (
	"context"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lateProxy starts listening on a free port only after delay, then forwards every connection to target: a bus that
// starts after the component (the machine boots both; the component's unit can't wait for it).
func lateProxy(t *testing.T, target string, delay time.Duration) int {
	port := freePort(t)
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		select {
		case <-stop:
			return
		case <-time.After(delay):
		}
		ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			return
		}
		go func() { <-stop; _ = ln.Close() }()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				up, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer up.Close()
				go func() { _, _ = io.Copy(up, c) }()
				_, _ = io.Copy(c, up)
			}()
		}
	}()
	return port
}

func subscriber() component.Definition {
	return component.Definition{PublicPrefix: testPrefix, ID: "demo/evcons", Version: "3.0.0", Recovery: "demo.seen.recover", GRPCPort: portNow(), Subscribes: true,
		Authz:      sectest.Defs("demo/evcons", "demo.seen", "demo.seen.read", "demo.seen.recover"),
		Migrations: fstest.MapFS{"0001_seen.up.sql": {Data: []byte("CREATE TABLE IF NOT EXISTS seen (event_id uuid PRIMARY KEY, record_id text NOT NULL, size int NOT NULL);")}},
		New:        func(rt *runtime.Runtime) (component.Module, error) { return &consumer{rt: rt}, nil }}
}

// CK-BUS-10 (P6.34): the bus starts after the component; the component waits for it and then turns ready, the same
// as for a late database (CK-DB-04). With no bus at all it waits until stopped and exits 0; refused credentials exit
// 78 without the password in the logs.
func TestTheComponentWaitsForALateBus(t *testing.T) {
	natsURL := os.Getenv("BE_SDK_TEST_NATS_URL")
	require.NotEmpty(t, natsURL, "BE_SDK_TEST_NATS_URL is not set")
	nu, err := url.Parse(natsURL)
	require.NoError(t, err)
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "evcons", "")
	base := pgtest.MemberEnv(host, port, "evcons")
	def := subscriber()

	late := maps.Clone(base)
	late["BUS_URL"] = "nats://127.0.0.1:" + strconv.Itoa(lateProxy(t, nu.Host, 1500*time.Millisecond))
	start := time.Now()
	baseURL, stop := runComponent(t, def, late)
	assert.GreaterOrEqual(t, time.Since(start), 1500*time.Millisecond, "ready only once the bus answers")
	code, _ := get(t, baseURL+"/readyz")
	assert.Equal(t, 200, code)
	stop()

	away := maps.Clone(base)
	away["BUS_URL"] = "nats://127.0.0.1:" + strconv.Itoa(freePort(t))
	maps.Copy(away, map[string]string{"COMPONENT_ID": def.ID, "COMPONENT_VERSION": def.Version, "PORT": strconv.Itoa(freePort(t))})
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	var out syncBuffer
	assert.Equal(t, ExitOK, Run(ctx, def, Options{Env: env(secured(t, def, away)), Stdout: &out, Stderr: &out}))
	assert.Contains(t, out.String(), "waiting for the bus")

	refused := maps.Clone(away)
	pu, err := url.Parse(os.Getenv("BE_SDK_TEST_PG_ADMIN"))
	require.NoError(t, err)
	pu.Scheme, pu.Path, pu.User = "pgqueue", "/"+pgtest.Database, url.UserPassword(pu.User.Username(), "not-the-password")
	refused["BUS_URL"] = pu.String() + "&schema=be_bus_e2e"
	if pu.RawQuery == "" {
		refused["BUS_URL"] = pu.String() + "?schema=be_bus_e2e"
	}
	out = syncBuffer{}
	assert.Equal(t, ExitConfiguration, Run(context.Background(), def, Options{Env: env(secured(t, def, refused)), Stdout: &out, Stderr: &out}))
	assert.Contains(t, out.String(), "the bus refused the connection settings")
	assert.NotContains(t, out.String(), "not-the-password", "a secret never reaches the logs")
}
