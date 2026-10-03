package besdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"syscall"
	"time"

	"github.com/brickKit/be-sdk-go/internal/httpx"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
)

// UserHTTP calls another component's user plane on behalf of the current user (P8.1, P8.2): it
// forwards the caller's own Authorization header, so the callee decides with its own rules.
type UserHTTP struct {
	o      *outbound
	dep    string
	base   string // http://host:port
	client *http.Client
	slots  chan struct{} // the bulkhead of P7.9
}

type authzRevisionKey struct{}

// UserHTTP returns the user-plane client of a dependency; an optional dependency that is not
// installed answers ErrDependencyAbsent.
func (rt *Runtime) UserHTTP(dep string) (*UserHTTP, error) {
	o, err := rt.outbound()
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if c, ok := o.users[dep]; ok {
		return c, nil
	}
	addr, err := rt.endpoint(dep, "")
	if err != nil {
		return nil, err
	}
	c := &UserHTTP{o: o, dep: dep, base: "http://" + addr, client: &http.Client{Transport: newTransport(64)},
		slots: make(chan struct{}, outboundBulkhead)}
	o.users[dep] = c
	return c, nil
}

// Do sends req (its URL may be a path, joined to the dependency's address) with the user's token,
// the request ID, the trace context and X-Authz-Revision. Without a user in ctx it fails with
// UNAUTHENTICATED and never switches to a system identity (P8.1).
func (c *UserHTTP) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if c.o.inTx(ctx) {
		return nil, networkInTx("user-plane " + c.dep)
	}
	token := rawToken(ctx)
	if token == "" {
		return nil, ErrUnauthenticated
	}
	d, err := budget(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	default:
		return nil, outboundLimit(c.dep)
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	ctx, span := c.o.rt.tel.Tracer().Start(ctx, req.Method+" "+c.dep, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	req = req.Clone(ctx)
	if req.URL.Host == "" {
		u, _ := req.URL.Parse(c.base + req.URL.Path)
		u.RawQuery = req.URL.RawQuery
		req.URL, req.Host = u, u.Host
	}
	req.Header.Set("Authorization", token)
	if id := httpx.RequestID(ctx); id != "" {
		req.Header.Set("X-Request-Id", id)
	}
	if rev, _ := ctx.Value(authzRevisionKey{}).(string); rev != "" {
		req.Header.Set("X-Authz-Revision", rev)
	}
	c.o.rt.tel.Propagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	return c.send(req)
}

// send counts the call and retries a GET once on a connection reset (P8.2).
func (c *UserHTTP) send(req *http.Request) (*http.Response, error) {
	start := time.Now()
	c.o.grpcM.Inflight.WithLabelValues(c.dep).Inc()
	defer c.o.grpcM.Inflight.WithLabelValues(c.dep).Dec()
	resp, err := c.client.Do(req)
	if err != nil && req.Method == http.MethodGet && errors.Is(err, syscall.ECONNRESET) {
		resp, err = c.client.Do(req)
	}
	status := "0"
	if resp != nil {
		status = strconv.Itoa(resp.StatusCode)
	}
	c.o.httpM.Requests.WithLabelValues(c.dep, req.Method, status).Inc()
	c.o.httpM.Duration.WithLabelValues(c.dep, req.Method).Observe(time.Since(start).Seconds())
	if err != nil {
		return nil, unavailable(err, req.Context())
	}
	return resp, nil
}

// JSON sends in as JSON to path and decodes a 2xx answer into out; a problem+json answer comes back
// as the same code, reason and domain (P8.2).
func (c *UserHTTP) JSON(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return unavailable(err, ctx)
	}
	if resp.StatusCode >= 400 {
		return problem.RestoreHTTP(resp.StatusCode, raw)
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// unavailable classifies a transport failure: the caller's deadline → DEADLINE_EXCEEDED, anything
// else UNAVAILABLE.
func unavailable(err error, ctx context.Context) error {
	if ctx.Err() != nil {
		return problem.From(ctx.Err())
	}
	return &problem.Error{Code: codes.Unavailable, Cause: err}
}

func newTransport(maxConns int) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxConnsPerHost = maxConns
	t.MaxIdleConnsPerHost = maxConns
	t.ResponseHeaderTimeout = 0 // bounded by the request's deadline
	return t
}
