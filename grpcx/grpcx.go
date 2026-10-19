// Package grpcx is the system surface: one gRPC server per member, the SDK's interceptor chain (panic recovery,
// request ID, trace, deadline, access log and metrics, error mapping to google.rpc.Status) and service registration
// in which every method declares its operation, like the user surface's routes.
package grpcx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Default deadlines (protocol P4.3), the same as the user surface's.
const (
	DefaultDeadline       = 10 * time.Second
	OrchestrationDeadline = 15 * time.Second
)

// Method declares one method of a registered service.
type Method struct {
	// Operation is the operation this method performs, as declared in the component's resource definition. Required:
	// the system surface has no public methods.
	Operation string
	// Orchestration methods get the longer default deadline.
	Orchestration bool
	// Timeout overrides the default deadline.
	Timeout time.Duration
	// Callers are the components allowed to call this method (P8.6); a caller is known by its verified certificate.
	// AnyComponent admits every component of this deployment: a family contract's methods that every component
	// calls (the authorization member's view verification, the audit intake); the method still learns the caller.
	Callers []string
}

// AnyComponent in Method.Callers admits any caller whose certificate this deployment's trust root issued.
const AnyComponent = "*"

func (m Method) deadline() time.Duration {
	switch {
	case m.Timeout > 0:
		return m.Timeout
	case m.Orchestration:
		return OrchestrationDeadline
	}
	return DefaultDeadline
}

// Services collects a module's gRPC services.
type Services struct {
	list    []service
	methods map[string]Method // full method name -> declaration
}

type service struct {
	desc *grpc.ServiceDesc
	impl any
}

// Register adds a service; methods maps each method name of desc (e.g. "GetBalance") to its declaration.
func (s *Services) Register(desc *grpc.ServiceDesc, impl any, methods map[string]Method) {
	if s.methods == nil {
		s.methods = map[string]Method{}
	}
	s.list = append(s.list, service{desc: desc, impl: impl})
	for name, m := range methods {
		s.methods["/"+desc.ServiceName+"/"+name] = m
	}
}

// Empty reports whether nothing was registered.
func (s *Services) Empty() bool { return len(s.list) == 0 }

// Validate refuses methods without a declared operation and declarations of methods that do not exist; streams are
// not used on the system surface yet.
func (s *Services) Validate() error {
	var problems []string
	known := map[string]bool{}
	for _, svc := range s.list {
		for _, m := range svc.desc.Methods {
			full := "/" + svc.desc.ServiceName + "/" + m.MethodName
			known[full] = true
			if d, ok := s.methods[full]; !ok || d.Operation == "" {
				problems = append(problems, full+": no operation declared")
			} else if len(d.Callers) == 0 {
				problems = append(problems, full+": no callers declared")
			}
		}
		for _, st := range svc.desc.Streams {
			problems = append(problems, "/"+svc.desc.ServiceName+"/"+st.StreamName+": streams are not supported")
		}
	}
	for full := range s.methods {
		if !known[full] {
			problems = append(problems, full+": declared but not a method of a registered service")
		}
	}
	if len(problems) > 0 {
		return errors.New("system surface: " + strings.Join(problems, "; "))
	}
	return nil
}

// Deps are what the server needs from the member's runtime.
type Deps struct {
	Member   obs.Member
	Logger   *slog.Logger
	Registry *prometheus.Registry
	Tracer   trace.Tracer
	// TLS is the member's service identity; the system surface never runs without it (P8.5).
	TLS *identity.ServiceTLS
}

// Operations lists the operations the registered methods declare (checked against the component's declarations).
func (s *Services) Operations() []string {
	var out []string
	for _, m := range s.methods {
		out = append(out, m.Operation)
	}
	return out
}

type callerKey struct{}

// Caller is the component a system-surface call came from, as its verified certificate says (P8.5, P8.6).
func Caller(ctx context.Context) (string, bool) {
	c, ok := ctx.Value(callerKey{}).(string)
	return c, ok
}

var requestIDRe = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// NewServer builds the member's system-surface server.
func NewServer(d Deps, s *Services) (*grpc.Server, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if d.TLS == nil {
		return nil, errors.New("system surface: no service identity (SERVICE_TLS_CERT_FILE, SERVICE_TLS_KEY_FILE, SERVICE_TLS_CA_FILE)")
	}
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "be_grpc_server_duration_seconds",
		Help: "System-surface call duration by method and code.", Buckets: obs.DurationBuckets}, []string{"method", "code"})
	if err := d.Registry.Register(duration); err != nil {
		return nil, fmt.Errorf("register grpc metrics: %w", err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(d.TLS.ServerConfig())),
		grpc.ChainUnaryInterceptor(interceptor(d, s, duration)))
	for _, svc := range s.list {
		srv.RegisterService(svc.desc, svc.impl)
	}
	return srv, nil
}

func interceptor(d Deps, s *Services, duration *prometheus.HistogramVec) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		decl := s.methods[info.FullMethod]
		md, _ := metadata.FromIncomingContext(ctx)
		rid := first(md, calls.MetadataRequestID)
		if !requestIDRe.MatchString(rid) {
			rid = uuid.Must(uuid.NewV7()).String()
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs(calls.MetadataRequestID, rid))
		ctx = reqctx.WithRequestID(ctx, rid)
		ctx = obs.Propagator.Extract(ctx, calls.MetadataCarrier(md))
		ctx, span := d.Tracer.Start(ctx, info.FullMethod, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		log := d.Logger.With("request_id", rid, "trace_id", span.SpanContext().TraceID().String())

		// the effective deadline is the shorter of the method's and the caller's (P4.3)
		ctx, cancel := context.WithTimeout(ctx, decl.deadline())
		defer cancel()

		caller, ok := identity.Caller(ctx, d.TLS.Deployment)
		if !ok || !(slices.Contains(decl.Callers, caller) || slices.Contains(decl.Callers, AnyComponent)) {
			log.Warn("a system call from a component not allowed to call this method", "method", info.FullMethod, "caller", caller)
			return nil, errs.New(errs.ReasonPermissionDenied, "this component may not call this method").GRPCStatus().Err()
		}
		ctx = context.WithValue(ctx, callerKey{}, caller)
		func() {
			defer func() {
				if p := recover(); p != nil {
					log.Error("panic while serving a call", "panic", fmt.Sprint(p), "stack", string(debug.Stack()),
						"method", info.FullMethod)
					resp, err = nil, errs.New(errs.ReasonInternal, "")
				}
			}()
			resp, err = handler(ctx, req)
		}()

		code := "OK"
		if err != nil {
			e := errs.From(err)
			if lvl, ok := errs.LogLevel(e.Code); ok {
				log.Log(ctx, lvl, "call failed", "method", info.FullMethod, "reason", e.Reason, "domain", e.Domain,
					"error", e.Error())
			}
			err = e.GRPCStatus().Err()
			code = string(e.Code)
		}
		elapsed := time.Since(start)
		duration.WithLabelValues(info.FullMethod, code).Observe(elapsed.Seconds())
		log.Log(ctx, accessLevel(err), "access", "method", info.FullMethod, "code", code, "duration_ms", elapsed.Milliseconds())
		return resp, err
	}
}

func accessLevel(err error) slog.Level {
	if err == nil {
		return slog.LevelInfo
	}
	return errs.AccessLogLevel(errs.HTTPStatus(errs.CodeOf(status.Code(err))))
}

func first(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}
