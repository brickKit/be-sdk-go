//go:build integration

package launcher

import (
	"context"
	"encoding/json"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recorder writes one general evidence record on POST /api/change, its ID in the answer.
type recorder struct{ rt *runtime.Runtime }

func (a *recorder) Routes(r *httpx.Routes) error {
	r.POST("/api/change", "demo.evp.change", func(c *httpx.Context) error {
		var id string
		err := a.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) (err error) {
			id, err = a.rt.Evidence.General(ctx, tx, "security_widening", sectest.Alice,
				evidence.Object{Type: "demo.evp", ID: ids.New().String(), Version: "1"}, map[string]any{"kind": "test"})
			return err
		})
		if err != nil {
			return err
		}
		return c.JSON(201, map[string]string{"id": id})
	})
	return nil
}

// P1.23 (be-protocol alpha.92): the evidence transfer runs in the recovery phases but frozen, as the audit transfer
// does. A record written while the evidence store is down stays local; frozen does not send it; redelivering does.
func TestTheEvidenceTransferRunsInTheRecoveryPhases(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "evph", "")
	def := component.Definition{ID: "demo/evph", Version: "3.0.0", GRPCPort: portNow(), Recovery: "demo.evp.recover",
		Authz: sectest.Defs("demo/evph", "demo.evp", "demo.evp.change", "demo.evp.recover"), Migrations: noMigrations(),
		New: func(rt *runtime.Runtime) (component.Module, error) { return &recorder{rt: rt}, nil }}
	vars := pgtest.MemberEnv(host, port, "evph")
	w := worldOf(t)

	// the store down while running: the record stays local
	w.EvidenceDown(true)
	running := secured(t, def, withIdentity(vars, def))
	var logs syncBuffer
	require.Equal(t, ExitOK, Run(context.Background(), def, Options{Args: []string{"migrate"}, Env: env(running), Stdout: &logs, Stderr: &logs}))
	httpPort := freePort(t)
	running["PORT"] = strconv.Itoa(httpPort)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- Run(ctx, def, Options{Env: env(running), Stdout: &logs, Stderr: &logs}) }()
	base := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 10*time.Second, 20*time.Millisecond, logs.String())
	code, body := postBody(t, base+"/api/change")
	require.Equal(t, 201, code, body)
	var out map[string]string
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	key := out["id"] + "/general"
	time.Sleep(evidence.TransferEvery + time.Second)
	cancel()
	require.Equal(t, ExitOK, <-done)
	require.NotContains(t, w.Evidence(), key, "the store was down")
	w.EvidenceDown(false)

	phased := func(phase string) map[string]string {
		out := maps.Clone(vars)
		out["RECOVERY_PHASE"] = phase
		return out
	}
	_, _, stop := runLogged(t, def, phased("frozen"))
	time.Sleep(evidence.TransferEvery + 2*time.Second)
	assert.NotContains(t, w.Evidence(), key, "frozen: nothing runs")
	stop()

	_, _, stop = runLogged(t, def, phased("redelivering"))
	defer stop()
	require.Eventually(t, func() bool { _, ok := w.Evidence()[key]; return ok }, 3*evidence.TransferEvery, 200*time.Millisecond,
		"redelivering: the evidence store never goes back, the record is sent before the replay")
}
