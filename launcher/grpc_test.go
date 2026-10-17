package launcher

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/internal/echotest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type system struct{ release chan struct{} }

func (s *system) Routes(*httpx.Routes) error { return nil }
func (s *system) Services(r *grpcx.Services) error {
	r.Register(&echotest.Desc, echotest.Say(func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if in.Value == "slow" {
			<-s.release
		}
		return wrapperspb.String("echo " + in.Value), nil
	}), map[string]grpcx.Method{"Say": {Operation: "demo.system.say"}})
	return nil
}

func TestServesTheSystemSurfaceAndStopsItGracefully(t *testing.T) {
	port, grpcPort := freePort(t), freePort(t)
	s := &system{release: make(chan struct{})}
	def := component.Definition{ID: "demo/system", Version: "3.0.0", GRPCPort: grpcPort,
		New: func(*runtime.Runtime) (component.Module, error) { return s, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, def, Options{Env: env(map[string]string{"COMPONENT_ID": "demo/system", "COMPONENT_VERSION": "3.0.0",
			"PORT": strconv.Itoa(port), "STOP_DRAIN_DELAY": "200ms"}), Stdout: io.Discard, Stderr: io.Discard})
	}()
	base := "http://127.0.0.1:" + strconv.Itoa(port)
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 5*time.Second, 20*time.Millisecond)

	out, err := calls.NewMember(nil)
	require.NoError(t, err)
	defer out.Close()
	conn, err := out.GRPC("demo/system", "127.0.0.1:"+strconv.Itoa(grpcPort))
	require.NoError(t, err)
	call := func(v string) (string, error) {
		cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ccancel()
		reply := new(wrapperspb.StringValue)
		err := conn.Invoke(cctx, echotest.Method, wrapperspb.String(v), reply, calls.Idempotent())
		return reply.Value, err
	}
	got, err := call("hi")
	require.NoError(t, err)
	assert.Equal(t, "echo hi", got)

	slow := make(chan string, 1)
	go func() { v, _ := call("slow"); slow <- v }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)
	close(s.release)
	assert.Equal(t, "echo slow", <-slow, "a call in flight finishes during the stop")
	assert.Equal(t, ExitOK, <-done)
}

func TestSystemModuleWithoutGRPCPortFails(t *testing.T) {
	def := component.Definition{ID: "demo/system", Version: "3.0.0",
		New: func(*runtime.Runtime) (component.Module, error) { return &system{}, nil }}
	var out syncBuffer
	code := Run(context.Background(), def, Options{Env: env(map[string]string{"COMPONENT_ID": "demo/system",
		"COMPONENT_VERSION": "3.0.0", "PORT": strconv.Itoa(freePort(t))}), Stdout: &out, Stderr: &out})
	assert.Equal(t, ExitFailure, code)
	assert.Contains(t, out.String(), "no GRPCPort")
}
