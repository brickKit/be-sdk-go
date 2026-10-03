package events

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
)

// syncBuffer is a log sink safe for concurrent writers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testLogger() (*slog.Logger, *syncBuffer) {
	b := &syncBuffer{}
	return slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})), b
}

// fakePublisher records every message and answers with fail (nil = stored).
type fakePublisher struct {
	mu    sync.Mutex
	sent  []jetstream.Message
	fail  func(m jetstream.Message) error
	block chan struct{} // when set, PublishBatch waits for it to close first
}

func (f *fakePublisher) PublishBatch(ctx context.Context, ms []jetstream.Message) []error {
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	errs := make([]error, len(ms))
	for i, m := range ms {
		if f.fail != nil {
			errs[i] = f.fail(m)
		}
		if errs[i] == nil {
			f.sent = append(f.sent, m)
		}
	}
	return errs
}

func (f *fakePublisher) messages() []jetstream.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]jetstream.Message(nil), f.sent...)
}

func (f *fakePublisher) setFail(fn func(m jetstream.Message) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = fn
}

// fakeOutbox is an in-memory besdk_outbox with the claim rule of ddl/02 (claimed_until ignored).
type fakeOutbox struct {
	mu        sync.Mutex
	rows      []*claimedRow
	status    map[string]string
	failed    map[string]failedRow
	claimErr  error
	claims    int
	statsSeen int
}

func newFakeOutbox(rows ...claimedRow) *fakeOutbox {
	f := &fakeOutbox{status: map[string]string{}, failed: map[string]failedRow{}}
	for i := range rows {
		r := rows[i]
		f.rows = append(f.rows, &r)
		f.status[r.id] = "PENDING"
	}
	return f
}

func (f *fakeOutbox) claim(ctx context.Context) ([]claimedRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	var out []claimedRow
	for _, r := range f.rows {
		if f.status[r.id] == "PENDING" && len(out) < BatchSize {
			f.status[r.id] = "SENDING"
			r.attempts++
			out = append(out, *r)
		}
	}
	return out, nil
}

func (f *fakeOutbox) markPublished(ctx context.Context, rows []claimedRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range rows {
		f.status[r.id] = "PUBLISHED"
	}
	return ctx.Err()
}

func (f *fakeOutbox) markFailed(ctx context.Context, rows []failedRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range rows {
		f.status[r.id] = "FAILED" // a test re-opens it with reopen
		f.failed[r.id] = r
	}
	return ctx.Err()
}

func (f *fakeOutbox) stats(ctx context.Context) (int64, float64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statsSeen++
	var n int64
	for _, s := range f.status {
		if s != "PUBLISHED" {
			n++
		}
	}
	return n, 1.5, nil
}

func (f *fakeOutbox) reopen() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, s := range f.status {
		if s == "FAILED" {
			f.status[id] = "PENDING"
		}
	}
}

func (f *fakeOutbox) statusOf(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status[id]
}

func (f *fakeOutbox) failure(id string) (failedRow, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.failed[id]
	return r, ok
}

func waitFor(cond func() bool, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
