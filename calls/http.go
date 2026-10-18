package calls

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
	"go.opentelemetry.io/otel/propagation"
)

// Headers of protocol chapter 04 (P4.2, P4.3).
const (
	HeaderRequestID      = "X-Request-Id"
	HeaderTimeoutMs      = "Be-Timeout-Ms"
	HeaderIdempotencyKey = "Idempotency-Key"
)

// maxResponse bounds what is read from a dependency's answer.
const maxResponse = 16 << 20

// HTTP calls one dependency's user surface. Build it with Member.HTTP; it is safe for concurrent use.
type HTTP struct {
	dep    *Dependency
	base   string
	client *http.Client
}

// Request is one user-surface call.
type Request struct {
	Method string
	Path   string // with the query string, e.g. /api/orders?status=open
	Body   any    // encoded as JSON; nil: no body
	// Idempotent marks a method the contract declares free of side effects or idempotent; GET and HEAD always are.
	Idempotent bool
	// IdempotencyKey is sent with commands that have side effects (P4.2); the same key on every attempt.
	IdempotencyKey string
	// ForwardUser forwards the caller's own credential: reads on the user's behalf go through the user surface with
	// the original credential, never with the service's identity (component implementation conventions).
	ForwardUser bool
}

// HTTP returns the client for a dependency's user surface at base (its *_ENDPOINT URL).
func (m *Member) HTTP(id, base string) *HTTP {
	client := m.httpClient()
	return &HTTP{dep: m.Dependency(id, "http"), base: strings.TrimRight(base, "/"), client: client}
}

// Do sends the request and decodes a 2xx JSON answer into out (nil: ignore the body). Errors are the protocol's.
func (h *HTTP) Do(ctx context.Context, req Request, out any) error {
	var body []byte
	if req.Body != nil {
		b, err := json.Marshal(req.Body)
		if err != nil {
			return errs.From(fmt.Errorf("encoding the request to %s: %w", h.dep.ID, err))
		}
		body = b
	}
	auth := ""
	if req.ForwardUser {
		if auth = reqctx.UserCredential(ctx); auth == "" {
			return errs.From(errors.New("calls: ForwardUser without a user credential in the context"))
		}
	}
	idempotent := req.Idempotent || req.Method == http.MethodGet || req.Method == http.MethodHead
	var answer []byte
	err := h.dep.Do(ctx, idempotent, func(actx context.Context) Result {
		r, b := h.attempt(actx, req, body, auth)
		if r.Err == nil {
			answer = b
		}
		return r
	})
	if err != nil || out == nil || len(answer) == 0 {
		return err
	}
	if err := json.Unmarshal(answer, out); err != nil {
		return errs.From(fmt.Errorf("decoding the answer of %s: %w", h.dep.ID, err))
	}
	return nil
}

func (h *HTTP) attempt(ctx context.Context, req Request, body []byte, auth string) (Result, []byte) {
	hr, err := http.NewRequestWithContext(ctx, req.Method, h.base+req.Path, bytes.NewReader(body))
	if err != nil {
		return Result{Err: err, Code: errs.Internal}, nil
	}
	if body != nil {
		hr.Header.Set("Content-Type", "application/json")
	}
	hr.Header.Set("Accept", "application/json")
	if dl, ok := ctx.Deadline(); ok {
		hr.Header.Set(HeaderTimeoutMs, strconv.FormatInt(max(1, time.Until(dl).Milliseconds()), 10))
	}
	if id := reqctx.RequestID(ctx); id != "" {
		hr.Header.Set(HeaderRequestID, id)
	}
	if req.IdempotencyKey != "" {
		hr.Header.Set(HeaderIdempotencyKey, req.IdempotencyKey)
	}
	if auth != "" {
		hr.Header.Set("Authorization", auth)
	}
	obs.Propagator.Inject(ctx, propagation.HeaderCarrier(hr.Header))

	resp, err := h.client.Do(hr)
	if err != nil {
		return transportResult(ctx, err), nil
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return transportResult(ctx, err), nil
	}
	if resp.StatusCode < 300 {
		return Result{Code: "OK"}, b
	}
	if e, ok := errs.FromHTTP(resp, b); ok {
		return Result{Err: e, Code: e.Code, Remote: e}, nil
	}
	return Result{Err: fmt.Errorf("%s answered %d", h.dep.ID, resp.StatusCode), Code: errs.CodeOfStatus(resp.StatusCode)}, nil
}

// transportResult classifies a failure without an answer: the attempt's deadline, a dial failure (nothing was sent),
// or a failure after sending.
func transportResult(ctx context.Context, err error) Result {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return Result{Err: err, Code: errs.DeadlineExceeded}
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return Result{Err: err, Code: errs.Cancelled}
	}
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return Result{Err: err, Code: errs.Unavailable, ConnectFailed: true}
	}
	return Result{Err: err, Code: errs.Unavailable}
}

// ClientIdleTimeout drops an idle outbound connection; servers keep theirs longer (P1.20), so the client is the
// one that lets a connection go.
const ClientIdleTimeout = 90 * time.Second

// httpClient is the member's own transport (never the process-wide default client).
func (m *Member) httpClient() *http.Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client == nil {
		t := &http.Transport{Proxy: nil, MaxIdleConnsPerHost: OutboundLimit, IdleConnTimeout: ClientIdleTimeout,
			DialContext: (&net.Dialer{Timeout: MaxAttemptTimeout, KeepAlive: 30 * time.Second}).DialContext}
		m.client = &http.Client{Transport: t}
	}
	return m.client
}

// Close releases the member's connections (at stop).
func (m *Member) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client != nil {
		m.client.CloseIdleConnections()
	}
	for _, c := range m.conns {
		_ = c.Close()
	}
	m.conns = nil
}
