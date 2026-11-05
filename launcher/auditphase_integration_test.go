//go:build integration

package launcher

import (
	"context"
	"encoding/json"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/audit"
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

// auditor writes one business change's audit on POST /api/change, its ID in the answer.
type auditor struct{ rt *runtime.Runtime }

func (a *auditor) Routes(r *httpx.Routes) error {
	r.POST("/api/change", "demo.aud.change", func(c *httpx.Context) error {
		var id string
		err := a.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) (err error) {
			id, err = a.rt.Audit.Write(ctx, tx, audit.Record{Category: audit.BusinessChange, Operation: "demo.aud.change",
				Outcome: audit.Succeeded, Actor: sectest.Alice, Target: map[string]any{"type": "demo.aud", "id": ids.New().String()}})
			return err
		})
		if err != nil {
			return err
		}
		return c.JSON(201, map[string]string{"id": id})
	})
	return nil
}

// P1.23 (be-protocol alpha.90): the audit transfer runs in the recovery phases but frozen. A record written while no
// audit address is configured stays local; frozen does not send it; verifying does.
func TestTheAuditTransferRunsInTheRecoveryPhases(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "audph", "")
	def := component.Definition{ID: "demo/audph", Version: "3.0.0", GRPCPort: portNow(), Recovery: "demo.aud.recover",
		Authz: sectest.Defs("demo/audph", "demo.aud", "demo.aud.change", "demo.aud.recover"), Migrations: noMigrations(),
		New: func(rt *runtime.Runtime) (component.Module, error) { return &auditor{rt: rt}, nil }}
	vars := pgtest.MemberEnv(host, port, "audph")
	w := worldOf(t)

	// no audit address while running: the record stays local
	noAudit := secured(t, def, withIdentity(vars, def))
	noAudit["AUDIT_GRPC_URL"] = ""
	var logs syncBuffer
	require.Equal(t, ExitOK, Run(context.Background(), def, Options{Args: []string{"migrate"}, Env: env(noAudit), Stdout: &logs, Stderr: &logs}))
	httpPort := freePort(t)
	noAudit["PORT"] = strconv.Itoa(httpPort)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- Run(ctx, def, Options{Env: env(noAudit), Stdout: &logs, Stderr: &logs}) }()
	base := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 10*time.Second, 20*time.Millisecond, logs.String())
	code, body := postBody(t, base+"/api/change")
	require.Equal(t, 201, code, body)
	var out map[string]string
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	id := out["id"]
	cancel()
	require.Equal(t, ExitOK, <-done)

	phased := func(phase string) map[string]string {
		out := maps.Clone(vars)
		out["RECOVERY_PHASE"] = phase
		return out
	}
	_, _, stop := runLogged(t, def, phased("frozen"))
	time.Sleep(audit.TransferEvery + 2*time.Second)
	assert.NotContains(t, w.Audited(), id, "frozen: nothing runs")
	stop()

	_, _, stop = runLogged(t, def, phased("verifying"))
	defer stop()
	require.Eventually(t, func() bool { _, ok := w.Audited()[id]; return ok }, 3*audit.TransferEvery, 200*time.Millisecond,
		"verifying: the audit store never goes back, the record is sent")
}
