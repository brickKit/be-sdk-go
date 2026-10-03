package besdk

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc"
	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type probeServer struct {
	probepb.UnimplementedProbeServer
}

func (probeServer) Read(ctx context.Context, in *probepb.ProbeRequest) (*probepb.ProbeReply, error) {
	c, _ := rpc.CallerFrom(ctx)
	if in.Id == "missing" {
		return nil, problem.New(codes.NotFound, "test/thing", "THING_GONE", map[string]string{"id": in.Id}, "gone")
	}
	return &probepb.ProbeReply{Note: c.Caller + "|" + c.ActorSub}, nil
}

// A component calls its own gRPC port through rt.Conn, as it would call a dependency: the caller
// metadata, the error identity and the batch limit all cross the real network (P7.2, P7.10, P4.2).
func TestGRPCThroughRuntime(t *testing.T) {
	grpcPort := freePort(t)
	manifest := strings.Replace(testManifest, "deployment:\n  port: PORT_PLACEHOLDER\n",
		fmt.Sprintf("deployment:\n  port: PORT_PLACEHOLDER\n  extraPorts:\n    - {name: grpc, port: %d, protocol: grpc}\n", grpcPort), 1)
	var rt *Runtime
	spec := Spec{ID: "test/thing", Manifest: []byte(manifest), New: func(_ context.Context, r *Runtime) (*Module, error) {
		rt = r
		return &Module{
			GRPC: func(s *grpc.Server) { probepb.RegisterProbeServer(s, probeServer{}) },
			HTTP: func(r *Router) { GET(r, "/noop", Public, func(*gin.Context) {}) },
		}, nil
	}}
	env := map[string]string{"THING_TOKEN_FILE": secretFile(t, "x"),
		"TEST_THING_GRPC_ENDPOINT": fmt.Sprintf("http://127.0.0.1:%d", grpcPort)}
	p := startProc(t, spec, env)
	p.get(t, "/healthz")
	conn, err := rt.Conn("test/thing", "grpc")
	if err != nil {
		t.Fatal(err)
	}
	client := probepb.NewProbeClient(conn)
	ctx, cancel := context.WithTimeout(withUser(context.Background(), "u7", "Bearer t"), 2*time.Second)
	defer cancel()
	reply, err := client.Read(ctx, &probepb.ProbeRequest{Id: "1"})
	if err != nil || reply.Note != "test/thing|u7" {
		t.Fatalf("read: %v %v", reply, err)
	}
	_, err = client.Read(ctx, &probepb.ProbeRequest{Id: "missing"})
	if !problem.Is(err, "test/thing", "THING_GONE") {
		t.Fatalf("domain error not restored: %v", err)
	}
	_, err = client.BatchGet(ctx, &probepb.BatchRequest{Ids: []string{"a", "b", "c", "d"}})
	if !problem.Is(err, "be", "BATCH_TOO_LARGE") {
		t.Fatalf("batch limit: %v", err)
	}
	_, body := p.get(t, "/_be/info")
	if !strings.Contains(body, fmt.Sprintf(`"grpc":%d`, grpcPort)) || !strings.Contains(body, `"grpc"`) {
		t.Fatalf("info: %s", body)
	}
}

func TestErrorfTakesTheComponentDomain(t *testing.T) {
	eng, r, _ := newTestRouter(t)
	GET(r, "/e", Public, func(c *gin.Context) {
		Fail(c, WithRetryAfter(Errorf(codes.FailedPrecondition, "NOT_DRAFT", map[string]string{"s": "DONE"}, "order is %s", "DONE"), 0))
	})
	_, body := call(eng, "GET", "/erp/sales/e", "")
	if body["domain"] != "erp/sales" || body["reason"] != "NOT_DRAFT" || body["code"] != "FAILED_PRECONDITION" {
		t.Fatalf("%v", body)
	}
	if d, r, ok := ReasonOf(Errorf(codes.NotFound, "X_GONE", nil, "")); !ok || d != "" || r != "X_GONE" {
		t.Fatal("ReasonOf")
	}
}
