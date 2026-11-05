//go:build integration

package launcher

import (
	"bytes"
	"context"
	"maps"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/internal/evidencepb"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type replayer struct{ rt *runtime.Runtime }

func (r *replayer) Routes(*httpx.Routes) error { return nil }

func (r *replayer) Replays(rp *evidence.Replays) error {
	rp.Add("security_tightening", func(ctx context.Context, tx db.Tx, rec evidence.Replayed) error {
		_, err := tx.Exec(ctx, "INSERT INTO replayed (id) VALUES ($1)", rec.ID)
		return err
	})
	return nil
}

// The replay as a member serves it (P12.19): registered when the module replays its evidence, under the operation the
// definition names, callable by the recovery tool's identity only; a module that replays without naming the
// operation does not start.
func TestTheReplayIsTheRecoveryToolsOnly(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "rpowner", "")
	grpcPort := freePort(t)
	def := component.Definition{ID: "demo/rpowner", Version: "3.0.0", GRPCPort: grpcPort, Replay: "demo.rp.replay",
		Authz:      sectest.Defs("demo/rpowner", "demo.rp", "demo.rp.replay"),
		Migrations: fstest.MapFS{"0001_replayed.up.sql": {Data: []byte("CREATE TABLE replayed (id uuid PRIMARY KEY);")}},
		New:        func(rt *runtime.Runtime) (component.Module, error) { return &replayer{rt: rt}, nil }}
	_, _, stop := runLogged(t, def, pgtest.MemberEnv(host, port, "rpowner"))
	defer stop()

	w := worldOf(t)
	sealed, err := evidence.Seal(map[string]any{"record_type": evidence.RecordType, "schema": "1.0",
		"id": "0190f3a1-0000-7000-8000-0000000000a1", "recorder": map[string]any{"component": "demo/rpowner", "instance": "i"},
		"category": "security_tightening", "phase": "effective", "object": map[string]any{"type": "demo.rp", "id": "x"},
		"change": map[string]any{"termination": "revoke"}, "at": "2026-11-05T10:00:00.000000Z",
		"position": map[string]any{"epoch": "1.1", "commit_seq": 1}})
	require.NoError(t, err)
	call := func(as string) (*evidencepb.ReplayResponse, error) {
		tlsCfg := w.TLS(t, as).ClientConfig("demo/rpowner")
		conn, err := grpc.NewClient("127.0.0.1:"+strconv.Itoa(grpcPort), grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
		require.NoError(t, err)
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5e9)
		defer cancel()
		return evidencepb.NewReplayServiceClient(conn).Replay(ctx, &evidencepb.ReplayRequest{Record: sealed.Record, Recovery: "r"})
	}
	_, err = call("demo/other")
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "only the recovery tool: %v", err)
	out, err := call("ops/recover")
	require.NoError(t, err)
	assert.Equal(t, evidence.Applied, out.GetOutcome())
	out, err = call("ops/recover")
	require.NoError(t, err)
	assert.Equal(t, evidence.AlreadyReflected, out.GetOutcome())

	noOp := def
	noOp.Replay = ""
	var logs bytes.Buffer
	assert.NotEqual(t, ExitOK, Run(context.Background(), noOp, Options{Env: env(secured(t, noOp, withIdentity(pgtest.MemberEnv(host, port, "rpowner"), noOp))),
		Stdout: &logs, Stderr: &logs}), "a module that replays names the operation")
	assert.Contains(t, logs.String(), "Definition.Replay")
}

// withIdentity adds what serve needs besides the member's own configuration.
func withIdentity(vars map[string]string, def component.Definition) map[string]string {
	out := maps.Clone(vars)
	maps.Copy(out, map[string]string{"COMPONENT_ID": def.ID, "COMPONENT_VERSION": def.Version, "PORT": "0", "STOP_DRAIN_DELAY": "50ms"})
	return out
}
