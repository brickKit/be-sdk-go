package shell

import (
	"strings"

	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/internal/echotest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type echoMember struct{ id string }

func (e *echoMember) Routes(*httpx.Routes) error { return nil }
func (e *echoMember) Services(s *grpcx.Services) error {
	s.Register(&echotest.Desc, echotest.Say(func(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String(e.id + ": " + in.Value), nil
	}), map[string]grpcx.Method{"Say": {Operation: strings.ReplaceAll(e.id, "/", ".") + ".say", Callers: []string{"demo/client"}}})
	return nil
}

func echoDef(id string) component.Definition {
	return component.Definition{ID: id, Version: "0.1.0", Keys: []config.Key{},
		Authz: sectest.Defs(id, strings.ReplaceAll(id, "/", ".")+".echo", strings.ReplaceAll(id, "/", ".")+".say"),
		New:   func(*runtime.Runtime) (component.Module, error) { return &echoMember{id: id}, nil }}
}

func TestEachMemberServesItsSystemSurfaceOnItsOwnPort(t *testing.T) {
	pa, ga, pb, gb, ps := freePort(t), freePort(t), freePort(t), freePort(t), freePort(t)
	js := served(t, secure(t, []component.Definition{echoDef("demo/alpha"), echoDef("demo/beta")},
		Served{ComponentID: "demo/alpha", Version: "0.1.0", HTTPPort: pa, ExtraPorts: []Port{{Name: "grpc", Port: ga}}, Config: map[string]string{}},
		Served{ComponentID: "demo/beta", Version: "0.1.0", HTTPPort: pb, ExtraPorts: []Port{{Name: "grpc", Port: gb}}, Config: map[string]string{}})...)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, self, []component.Definition{echoDef("demo/alpha"), echoDef("demo/beta")},
			Options{Env: shellEnv(ps, js), Stdout: io.Discard, Stderr: io.Discard,
				BuildInfo: buildInfo(map[string]string{"demo/alpha": "v0.1.0", "demo/beta": "v0.1.0"}), ModuleOf: moduleByID})
	}()
	require.Eventually(t, func() bool { c, _ := get(t, "http://127.0.0.1:"+strconv.Itoa(ps)+"/readyz"); return c == 200 },
		shellReady, 20*time.Millisecond)
	out, err := calls.NewMember(nil)
	require.NoError(t, err)
	defer out.Close()
	out.SetTLS(worldOf(t).TLS(t, "demo/client"))
	for id, port := range map[string]int{"demo/alpha": ga, "demo/beta": gb} {
		conn, err := out.GRPC(id, "127.0.0.1:"+strconv.Itoa(port))
		require.NoError(t, err)
		cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
		reply := new(wrapperspb.StringValue)
		require.NoError(t, conn.Invoke(cctx, echotest.Method, wrapperspb.String("hi"), reply, calls.Idempotent()))
		ccancel()
		assert.Equal(t, id+": hi", reply.Value)
	}
	cancel()
	assert.Equal(t, 0, <-done)
}

func TestRefusesClashingExtraPorts(t *testing.T) {
	js := served(t,
		Served{ComponentID: "demo/alpha", Version: "0.1.0", HTTPPort: 18081, ExtraPorts: []Port{{Name: "grpc", Port: 19081}}, Config: map[string]string{}},
		Served{ComponentID: "demo/beta", Version: "0.1.0", HTTPPort: 18082, ExtraPorts: []Port{{Name: "grpc", Port: 19081}}, Config: map[string]string{}})
	out := refuse(t, shellEnv(18080, js), buildInfo(map[string]string{"demo/alpha": "v0.1.0", "demo/beta": "v0.1.0"}),
		echoDef("demo/alpha"), echoDef("demo/beta"))
	assert.Contains(t, out, "demo/beta:extraPorts.grpc")
}
