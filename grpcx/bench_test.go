package grpcx_test

// Benchmarks of what every request pays inside the SDK, in one process and without a container network: the user
// surface (middleware, access line, metrics, span), one mTLS system call, and both together as the probes' call
// operation does. Run with -benchmem and -cpuprofile to see where the time goes:
//
//	go test -run '^$' -bench . -benchmem -cpuprofile cpu.out ./grpcx/ && go tool pprof -top cpu.out

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/internal/echotest"
	"github.com/brickKit/be-sdk-go/v3/internal/sectest"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/prometheus/client_golang/prometheus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var kib = strings.Repeat("x", 1024)

// benchMember is what a running member has: its logger writes JSON lines (to nowhere), its own registry and a tracer
// provider without an exporter, as in a deployment with no OTEL_URL.
func benchMember(w *sectest.World, b *testing.B, id string) grpcx.Deps {
	m := obs.Member{ID: id, Version: "3.0.0"}
	return grpcx.Deps{Member: m, Logger: obs.NewLogger(m, slog.LevelInfo, io.Discard), Registry: prometheus.NewRegistry(),
		Tracer: sdktrace.NewTracerProvider().Tracer(id), TLS: w.TLS(b, id)}
}

func benchEcho(b *testing.B, w *sectest.World) string {
	var s grpcx.Services
	s.Register(&echotest.Desc, echotest.Say(func(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return in, nil
	}), map[string]grpcx.Method{"Say": {Operation: "demo/b.say", Callers: []string{"demo/a"}}})
	srv, err := grpcx.NewServer(benchMember(w, b, "demo/b"), &s)
	if err != nil {
		b.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	b.Cleanup(srv.Stop)
	return ln.Addr().String()
}

func benchEngine(b *testing.B, d grpcx.Deps, handle func(*httpx.Context) error) http.Handler {
	var r httpx.Routes
	r.Add(httpx.Route{Method: "POST", Path: "/api/echo", Operation: "demo.a.echo", Public: true, Handle: handle})
	e, err := httpx.NewEngine(httpx.Deps{Member: d.Member, Logger: d.Logger, Registry: d.Registry, Tracer: d.Tracer,
		Platform: httpx.Platform{Ready: func() (bool, string, []string) { return true, "", nil },
			Describe: func() any { return nil }}}, &r)
	if err != nil {
		b.Fatal(err)
	}
	return e
}

func benchRequests(b *testing.B, h http.Handler) {
	body := `{"payload":"` + kib + `"}`
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		req := httptest.NewRequest("POST", "/api/echo", strings.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkUserSurfaceRequest(b *testing.B) {
	w := sectest.New(b)
	h := benchEngine(b, benchMember(w, b, "demo/a"), func(x *httpx.Context) error {
		var in struct {
			Payload string `json:"payload"`
		}
		if err := x.BindJSON(&in); err != nil {
			return err
		}
		return x.JSON(http.StatusOK, map[string]string{"payload": in.Payload})
	})
	benchRequests(b, h)
}

func BenchmarkSystemCallOverMTLS(b *testing.B) {
	w := sectest.New(b)
	addr := benchEcho(b, w)
	out, err := calls.NewMember(prometheus.NewRegistry())
	if err != nil {
		b.Fatal(err)
	}
	out.SetTLS(w.TLS(b, "demo/a"))
	b.Cleanup(out.Close)
	conn, err := out.GRPC("demo/b", addr)
	if err != nil {
		b.Fatal(err)
	}
	in := wrapperspb.String(kib)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := conn.Invoke(context.Background(), echotest.Method, in, new(wrapperspb.StringValue), calls.Idempotent()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUserSurfaceThenSystemCall(b *testing.B) {
	w := sectest.New(b)
	addr := benchEcho(b, w)
	out, err := calls.NewMember(prometheus.NewRegistry())
	if err != nil {
		b.Fatal(err)
	}
	out.SetTLS(w.TLS(b, "demo/a"))
	b.Cleanup(out.Close)
	h := benchEngine(b, benchMember(w, b, "demo/a"), func(x *httpx.Context) error {
		var in struct {
			Payload string `json:"payload"`
		}
		if err := x.BindJSON(&in); err != nil {
			return err
		}
		conn, err := out.GRPC("demo/b", addr)
		if err != nil {
			return err
		}
		res := new(wrapperspb.StringValue)
		if err := conn.Invoke(x.Ctx(), echotest.Method, wrapperspb.String(in.Payload), res, calls.Idempotent()); err != nil {
			return err
		}
		return x.JSON(http.StatusOK, map[string]string{"payload": res.GetValue()})
	})
	benchRequests(b, h)
}
