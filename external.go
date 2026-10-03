package besdk

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ExternalOptions tunes a third-party client (P8.3); the component reads the values from its own keys.
type ExternalOptions struct {
	Timeout  time.Duration // default 10 s
	MaxConns int           // per host, default 16
}

// ExternalHTTP returns a client for a third party (DingTalk, Casdoor, …) named name (P8.3): a 10 s
// default timeout, traced and counted as be_http_client_*{target=name}; it never forwards an internal
// header (Authorization, be-*, X-Authz-*, X-Request-Id) and refuses to start inside a transaction (P8.4).
func (rt *Runtime) ExternalHTTP(name string, o ExternalOptions) *http.Client {
	if o.Timeout <= 0 {
		o.Timeout = 10 * time.Second
	}
	if o.MaxConns <= 0 {
		o.MaxConns = 16
	}
	return &http.Client{Timeout: o.Timeout, Transport: &externalTransport{rt: rt, name: name, base: newTransport(o.MaxConns)}}
}

type externalTransport struct {
	rt   *Runtime
	name string
	base http.RoundTripper
}

func (t *externalTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	o, err := t.rt.outbound()
	if err != nil {
		return nil, err
	}
	ctx := req.Context()
	if o.inTx(ctx) {
		return nil, networkInTx("external " + t.name)
	}
	ctx, span := t.rt.tel.Tracer().Start(ctx, req.Method+" "+t.name, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	req = req.Clone(ctx)
	stripInternalHeaders(req.Header)
	t.rt.tel.Propagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	status := "0"
	if resp != nil {
		status = strconv.Itoa(resp.StatusCode)
	}
	o.httpM.Requests.WithLabelValues(t.name, req.Method, status).Inc()
	o.httpM.Duration.WithLabelValues(t.name, req.Method).Observe(time.Since(start).Seconds())
	return resp, err
}

// stripInternalHeaders removes every header that belongs to the project's own planes (P8.3); W3C
// trace context may still be sent.
func stripInternalHeaders(h http.Header) {
	for k := range h {
		lk := strings.ToLower(k)
		if lk == "authorization" || lk == "x-request-id" || strings.HasPrefix(lk, "be-") || strings.HasPrefix(lk, "x-authz-") ||
			lk == "baggage" {
			h.Del(k)
		}
	}
}
