package besdk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/httpx"
	"github.com/brickKit/be-sdk-go/internal/logx"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc"
	"github.com/brickKit/be-sdk-go/internal/telemetry"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func (p *process) readinessConditions() []string {
	var conds []string
	if p.auth != nil {
		conds = append(conds, "bundle")
	}
	if p.hasDatabase() {
		conds = append(conds, "db_identity", "migrations")
	}
	return conds
}

func (p *process) hasDatabase() bool { return declared(p.b.vals, "PG_SCHEMA") }

func (p *process) defaultDeadline() time.Duration {
	return optDuration(p.b.vals, "HTTP_DEFAULT_TIMEOUT", 10*time.Second)
}

// assembleHTTP builds the main port: outer layer (httpx) → operations endpoints → gin with the
// component's routes.
func (p *process) assembleHTTP() (err error) {
	eng := gin.New()
	eng.HandleMethodNotAllowed = false
	cfg := routerConfig{componentID: p.b.id, catalogue: p.b.catalogue, locale: p.b.locale,
		defaultDeadline: p.defaultDeadline(), guardian: p.auth.guardian(p.rt)}
	notFound := func(c *gin.Context) { fail(c, &cfg, problem.Be("NOT_FOUND", nil)) }
	eng.NoRoute(notFound)
	if p.mod.HTTP != nil {
		defer func() {
			if r := recover(); r != nil {
				if ce, ok := config.AsError(r); ok {
					err = ce
					return
				}
				err = fmt.Errorf("registering routes: %v", r)
			}
		}()
		p.mod.HTTP(newRouter(eng, cfg))
	}
	httpM, err := telemetry.NewHTTPServerMetrics(p.b.member.Registerer())
	if err != nil {
		return err
	}
	ops := &opsHandler{ready: p.ready, gatherer: p.b.member.Gatherer(), catalogue: p.b.catalogue,
		locale: p.b.locale, info: p.info, next: eng}
	outer := &httpx.Handler{Inner: ops, Tracer: p.b.member.Tracer(), Propagator: p.b.member.Propagator(),
		Logger: p.b.log, Catalogue: p.b.catalogue, Locale: p.b.locale, DefaultDeadline: p.defaultDeadline(),
		Observe: httpObserver(httpM), Fields: logx.WithFields, Quiet: isOpsPath}
	p.httpSrv = httpx.NewServer(outer, p.defaultDeadline())
	return nil
}

// assembleGRPC builds the system-plane server when the component registers services; a declared grpc
// port without services, or services without the port, is a start failure naming the port (P7.14).
func (p *process) assembleGRPC() error {
	switch {
	case p.mod.GRPC == nil && p.grpcLn == nil:
		return nil
	case p.mod.GRPC == nil:
		return fmt.Errorf("component.yaml declares the extra port grpc but the module registers no gRPC service")
	case p.grpcLn == nil:
		return &config.Error{Reason: config.ReasonMissing, Key: "deployment.extraPorts",
			Detail: "the module registers gRPC services but component.yaml declares no extra port named grpc"}
	}
	gm, err := telemetry.NewGRPCServerMetrics(p.b.member.Registerer())
	if err != nil {
		return err
	}
	p.grpcSrv = rpc.NewServer(rpc.ServerConfig{
		MaxConnectionAge: optDuration(p.b.vals, "GRPC_MAX_CONNECTION_AGE", rpc.DefaultMaxConnectionAge),
		Logger:           p.b.log, Catalogue: p.b.catalogue, Locale: p.b.locale,
		TracerProvider: p.b.member.TracerProvider(), MeterProvider: p.b.member.MeterProvider(),
		Propagator: p.b.member.Propagator(), Metrics: grpcServerMetrics{gm}, Domain: p.b.id,
		Inbound: func(ctx context.Context, reqID string, c rpc.Caller) context.Context {
			ctx = httpx.WithRequestID(ctx, reqID)
			return logx.WithFields(ctx, slog.String("request_id", reqID), slog.String("caller", c.Caller))
		},
	})
	healthpb.RegisterHealthServer(p.grpcSrv, health.NewServer())
	p.mod.GRPC(p.grpcSrv)
	return nil
}

func (p *process) superviseBackground() {
	for _, s := range p.rt.cfg.secrets {
		s := s
		p.sup.Go("be.secret."+s.Key(), func(ctx context.Context) error { s.Run(ctx); return nil })
	}
	if p.auth != nil {
		p.auth.run(p.sup, p.ready)
	}
	p.superviseDeps()
}

func (p *process) startServers() {
	go func() {
		if err := p.httpSrv.Serve(p.httpLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.b.log.Error("http server stopped", slog.String("error", err.Error()))
		}
	}()
	if p.grpcSrv != nil {
		go func() {
			if err := p.grpcSrv.Serve(p.grpcLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				p.b.log.Error("grpc server stopped", slog.String("error", err.Error()))
			}
		}()
	}
}

// stop is P1.6: stop taking requests and let in-flight ones finish within SHUTDOWN_GRACE, then stop
// background work, then the module's Stop, then the dependencies.
func (p *process) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), p.grace)
	defer cancel()
	if p.httpSrv != nil {
		_ = p.httpSrv.Shutdown(ctx)
	} else if p.httpLn != nil {
		_ = p.httpLn.Close()
	}
	if p.grpcSrv != nil {
		stopGRPC(ctx, p.grpcSrv)
	} else if p.grpcLn != nil {
		_ = p.grpcLn.Close()
	}
	if p.bgCancel != nil {
		p.bgCancel()
		p.sup.Wait()
	}
	if p.mod != nil && p.mod.Stop != nil {
		if err := p.mod.Stop(ctx); err != nil {
			p.b.log.Error("module stop failed", slog.String("error", err.Error()))
		}
	}
	for i := len(p.closers) - 1; i >= 0; i-- {
		p.closers[i](ctx)
	}
}

func stopGRPC(ctx context.Context, s *grpc.Server) {
	done := make(chan struct{})
	go func() { s.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		s.Stop()
	}
}

// info is GET /_be/info (P20.4).
func (p *process) info() Info {
	ports := map[string]int{"http": portOf(p.httpLn)}
	if p.grpcLn != nil {
		ports["grpc"] = portOf(p.grpcLn)
	}
	return Info{ComponentID: p.b.id, ComponentVersion: p.b.version, Protocol: ProtocolVersion,
		SDK:      &SDKInfo{Name: "be-sdk-go", Version: Version},
		Language: LanguageInfo{Name: "go", Version: strings.TrimPrefix(runtime.Version(), "go")},
		Profiles: p.profiles(), Ports: ports, Migrations: p.migrationsInfo()}
}

func (p *process) profiles() []string {
	out := []string{"core", "obs", "err"}
	if p.auth != nil {
		out = append(out, "auth")
	}
	if p.grpcSrv != nil {
		out = append(out, "grpc")
	}
	if p.hasDatabase() {
		out = append(out, "db")
	}
	return append(out, p.eventProfiles()...)
}
