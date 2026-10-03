package rpc

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// healthPrefix is the method prefix of the gRPC health service, exempt from be-caller (P7.2).
const healthPrefix = "/grpc.health.v1.Health/"

// serverChain holds one server's interceptors and the state they share.
type serverChain struct {
	log       *slog.Logger
	catalogue *problem.Catalogue
	locale    string
	metrics   ServerMetrics
	inbound   func(ctx context.Context, requestID string, c Caller) context.Context
	domain    string
	batch     batchLimits
	user      map[string]bool // full method names of user-facing rpcs (P7.3)
	bad       malformed       // requests whose bytes did not decode (REQUEST_INVALID)
}

func newServerChain(c ServerConfig) *serverChain {
	s := &serverChain{log: c.Logger, catalogue: c.Catalogue, locale: c.Locale, metrics: c.Metrics, inbound: c.Inbound, domain: c.Domain}
	if s.log == nil {
		s.log = discardLogger()
	}
	if len(c.UserFacing) > 0 {
		s.user = make(map[string]bool, len(c.UserFacing))
		for _, m := range c.UserFacing {
			s.user[m] = true
		}
	}
	if s.catalogue == nil {
		s.catalogue = problem.NewCatalogue()
	}
	return s
}

// splitMethod splits "/pkg.Service/Method" into its service and method.
func splitMethod(full string) (service, method string) {
	full = strings.TrimPrefix(full, "/")
	if i := strings.LastIndex(full, "/"); i >= 0 {
		return full[:i], full[i+1:]
	}
	return "", full
}

// recovered turns a panic value into an INTERNAL error whose cause (the value and the stack) goes
// to the log only (P4.3).
func recovered(r any) error {
	return problem.Wrap(fmt.Errorf("panic: %v\n%s", r, debug.Stack()), "INTERNAL", nil)
}

// finish normalises the outcome of one call (P4.2, P4.3), writes its grpc_request log line at the
// level of P4.6 and records the RED metrics. It returns the error to send: nil, or a status error
// whose INTERNAL details never carry the original.
func (s *serverChain) finish(ctx context.Context, fullMethod string, start time.Time, err error) error {
	elapsed := time.Since(start)
	service, method := splitMethod(fullMethod)
	md, _ := metadata.FromIncomingContext(ctx)
	attrs := []slog.Attr{
		slog.String("rpc.service", service), slog.String("rpc.method", method),
		slog.Float64("duration_ms", float64(elapsed.Microseconds())/1000),
		slog.String("caller", firstMD(md, MDCaller)), slog.String("request_id", firstMD(md, MDRequestID)),
	}
	var out error
	code, level, logged := codes.OK, slog.LevelInfo, true
	if err != nil {
		pe := problem.WithDomain(problem.From(err), s.domain)
		st := s.catalogue.ToStatus(pe, s.locale)
		out, code = st.Err(), st.Code()
		level, logged = problem.LogLevel(code)
		attrs = append(attrs, slog.String("error", pe.Error()),
			slog.String("error.code", problem.CodeName(code)), slog.String("error.reason", pe.Reason))
	}
	attrs = append(attrs, slog.String("rpc.grpc.status_code", problem.CodeName(code)))
	if logged {
		s.log.LogAttrs(ctx, level, "grpc_request", attrs...)
	}
	if s.metrics != nil {
		s.metrics.Handled(service, method, problem.CodeName(code), elapsed)
	}
	return out
}

// reportUnary is the outermost layer: panic recovery, then normalisation, log and metrics of
// whatever the inner layers and the service returned.
func (s *serverChain) reportUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	next grpc.UnaryHandler) (resp any, err error) {
	start := time.Now()
	defer func() {
		s.bad.forget(req)
		if r := recover(); r != nil {
			resp, err = nil, recovered(r)
		}
		err = s.finish(ctx, info.FullMethod, start, err)
	}()
	return next(ctx, req)
}

// principal reads the system principal and the request ID of an inbound call (P7.2, P7.3); a call
// without be-caller is MISSING_CALLER, except to the health service; a user-facing rpc is
// TOKEN_INVALID, since a system call carries no user token.
func (s *serverChain) principal(ctx context.Context, fullMethod string) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	c := Caller{Caller: firstMD(md, MDCaller), ActorSub: firstMD(md, MDActorSub), Act: firstMD(md, MDActorAct)}
	exempt := strings.HasPrefix(fullMethod, healthPrefix)
	if c.Caller == "" && !exempt {
		return ctx, problem.Be("MISSING_CALLER", nil)
	}
	if s.user[fullMethod] {
		return ctx, problem.Be("TOKEN_INVALID", nil)
	}
	if !exempt {
		ctx = withCallerValue(ctx, c)
	}
	if s.inbound != nil {
		rid := firstMD(md, MDRequestID)
		if rid == "" {
			rid = newRequestID()
		}
		ctx = s.inbound(ctx, rid, c)
	}
	return ctx, nil
}

func newRequestID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}

func (s *serverChain) identityUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	next grpc.UnaryHandler) (any, error) {
	ctx, err := s.principal(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	return next(ctx, req)
}

// withFloor gives a call without a deadline the inbound floor of P7.4.
func withFloor(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, InboundDeadlineFloor)
}

func (s *serverChain) deadlineUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo,
	next grpc.UnaryHandler) (any, error) {
	ctx, cancel := withFloor(ctx)
	defer cancel()
	return next(ctx, req)
}

func (s *serverChain) batchUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo,
	next grpc.UnaryHandler) (any, error) {
	if e := s.bad.take(req); e != nil {
		return nil, e
	}
	if e := s.batch.check(req); e != nil {
		return nil, e
	}
	return next(ctx, req)
}
