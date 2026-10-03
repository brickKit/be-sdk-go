// Package logx is the protocol log handler (P18.2): one JSON object per line, the envelope fields
// first, trace and request-scoped fields from the context, personal-data keys redacted and long lines
// shortened so that they stay valid JSON. Everything is per instance: a shell gives each member its own
// handler (P19.4); nothing here is process-wide.
package logx

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// DefaultMaxLine is the longest line written, newline included (P18.2: at most 2 KiB per line).
const DefaultMaxLine = 2048

// envelopeTime is RFC 3339 with nanoseconds, always nine digits (P18.2 `time`).
const envelopeTime = "2006-01-02T15:04:05.000000000Z07:00"

// Options configures one member's handler.
type Options struct {
	ComponentID      string       // `component_id` on every line (the member's own in a shell)
	ComponentVersion string       // `component_version` on every line
	Level            slog.Leveler // minimum level (LOG_LEVEL); nil means info
	Writer           io.Writer    // where lines go: the caller passes stdout
	MaxLine          int          // longest line in bytes, newline included; 0 means DefaultMaxLine
}

// Handler is the P18.2 slog handler. Each line is written with one Write call under the handler's
// mutex; a line of at most 2 KiB is below PIPE_BUF, so handlers of several members sharing stdout do not
// interleave either.
type Handler struct {
	opts   Options
	out    *sink
	attrs  []groupedAttr // from WithAttrs, each with the group prefix current at the time
	prefix string        // dotted group prefix of later attributes ("" or "a.b.")
}

// groupedAttr is an attribute added by WithAttrs and the group prefix it was added under.
type groupedAttr struct {
	prefix string
	attr   slog.Attr
}

// sink is the writer and the mutex shared by a handler and its WithAttrs/WithGroup derivatives.
type sink struct {
	mu sync.Mutex
	w  io.Writer
}

// New returns a logger over NewHandler(o) (P18.2).
func New(o Options) *slog.Logger { return slog.New(NewHandler(o)) }

// NewHandler returns the P18.2 handler for one member.
func NewHandler(o Options) *Handler {
	if o.Level == nil {
		o.Level = slog.LevelInfo
	}
	if o.MaxLine <= 0 {
		o.MaxLine = DefaultMaxLine
	}
	if o.Writer == nil {
		o.Writer = io.Discard
	}
	return &Handler{opts: o, out: &sink{w: o.Writer}}
}

// Enabled reports whether a record at this level is written (LOG_LEVEL, P18.2).
func (h *Handler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.opts.Level.Level() }

// WithAttrs returns a handler whose lines also carry attrs, qualified by the current group.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	c := *h
	c.attrs = make([]groupedAttr, 0, len(h.attrs)+len(attrs))
	c.attrs = append(c.attrs, h.attrs...)
	for _, a := range attrs {
		c.attrs = append(c.attrs, groupedAttr{h.prefix, a})
	}
	return &c
}

// WithGroup returns a handler whose later attributes are qualified by name (dotted keys).
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.prefix = h.prefix + name + "."
	return &c
}

// Handle writes one record as one line (P18.2): envelope, trace, context fields, handler attributes,
// record attributes; then redaction of non-envelope fields and, when too long, truncation.
func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	rec := newRecord(8 + r.NumAttrs() + len(h.attrs))
	t := r.Time
	if t.IsZero() {
		t = time.Now()
	}
	rec.set("time", t.UTC().Format(envelopeTime))
	rec.set("level", levelName(r.Level))
	rec.set("msg", r.Message)
	rec.set("component_id", h.opts.ComponentID)
	rec.set("component_version", h.opts.ComponentVersion)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.set("trace_id", sc.TraceID().String())
		rec.set("span_id", sc.SpanID().String())
	}
	rec.fixed = len(rec.fields)
	for _, a := range ContextFields(ctx) {
		rec.addAttr("", a)
	}
	for _, g := range h.attrs {
		rec.addAttr(g.prefix, g.attr)
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.addAttr(h.prefix, a)
		return true
	})
	for i := rec.fixed; i < len(rec.fields); i++ {
		rec.fields[i].val = redactTop(rec.fields[i].key, rec.fields[i].val)
	}
	line := fitLine(newLineEncoder(), rec, h.opts.MaxLine)
	line = append(line, '\n')
	h.out.mu.Lock()
	defer h.out.mu.Unlock()
	_, err := h.out.w.Write(line)
	return err
}
