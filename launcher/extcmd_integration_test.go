//go:build integration

package launcher

import (
	"bytes"
	"context"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/extcmd"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type payer struct {
	rt   *runtime.Runtime
	sent chan string
}

func (p *payer) Routes(*httpx.Routes) error { return nil }

func (p *payer) ExternalCommands() (extcmd.Adapter, map[string]extcmd.Capabilities) {
	return p, map[string]extcmd.Capabilities{"pay": {Query: true}}
}

func (p *payer) Send(_ context.Context, c extcmd.Command) (extcmd.Result, error) {
	p.sent <- c.Reference
	return extcmd.Result{Class: extcmd.Succeeded}, nil
}

func (p *payer) Query(context.Context, extcmd.Command) (extcmd.Answer, error) {
	return extcmd.Answer{}, nil
}

// A component that sends external commands (be-protocol chapter 13): the definition says so, the module provides the
// adapter, and the SDK's job sends what rt.External took; a module and a definition that disagree do not start.
func TestTheExternalCommandsAreTheSDKs(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "payer", "")
	built := make(chan *payer, 1)
	def := component.Definition{ID: "demo/payer", Version: "3.0.0", ExternalCommands: true,
		Migrations: noMigrations(),
		New: func(rt *runtime.Runtime) (component.Module, error) {
			p := &payer{rt: rt, sent: make(chan string, 4)}
			built <- p
			return p, nil
		}}
	_, _, stop := runLogged(t, def, pgtest.MemberEnv(host, port, "payer"))
	defer stop()
	p := <-built
	require.NotNil(t, p.rt.External)
	require.NoError(t, p.rt.DB.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, _, err := p.rt.External.Request(ctx, tx, "demo/sales", "ref-1", "pay", map[string]any{"amount": "1.00"}, nil)
		return err
	}))
	select {
	case ref := <-p.sent:
		assert.Equal(t, "ref-1", ref)
	case <-time.After(15 * time.Second):
		t.Fatal("the SDK's job did not send the command")
	}

	for _, wrong := range []component.Definition{
		{ID: "demo/payer", Version: "3.0.0", Migrations: noMigrations(), // the module provides, the definition does not say
			New: func(rt *runtime.Runtime) (component.Module, error) { return &payer{rt: rt}, nil }},
		{ID: "demo/payer", Version: "3.0.0", Migrations: noMigrations(), ExternalCommands: true, // says, does not provide
			New: func(rt *runtime.Runtime) (component.Module, error) { return quiet{}, nil }}} {
		var logs bytes.Buffer
		assert.NotEqual(t, ExitOK, Run(context.Background(), wrong, Options{Env: env(withIdentity(pgtest.MemberEnv(host, port, "payer"), wrong)),
			Stdout: &logs, Stderr: &logs}))
		assert.Contains(t, logs.String(), "ExternalCommands")
	}
}

// quiet is a module with nothing: no routes, no adapter.
type quiet struct{}

func (quiet) Routes(*httpx.Routes) error { return nil }

// noMigrations is a component's own migration set with one harmless table (its schema needs one).
func noMigrations() fstest.MapFS {
	return fstest.MapFS{"0001_payer.up.sql": {Data: []byte("CREATE TABLE IF NOT EXISTS payer_note (id int);")}}
}
