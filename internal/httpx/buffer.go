package httpx

import (
	"bytes"
	"net/http"
	"sync"
)

// buffer is the ResponseWriter the inner handler writes to: the outer handler copies it to the
// connection when the handler finished in time, and discards it when the deadline answered first, so a
// late handler never produces a second answer or a dropped connection (P3.5). Responses are small
// (bodies are capped, lists are paged), so buffering is affordable.
type buffer struct {
	mu     sync.Mutex
	header http.Header
	status int
	body   bytes.Buffer
}

func newBuffer() *buffer { return &buffer{header: http.Header{}} }

func (b *buffer) Header() http.Header { return b.header }

func (b *buffer) WriteHeader(status int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.status == 0 {
		b.status = status
	}
}

func (b *buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.status == 0 {
		b.status = http.StatusOK
	}
	return b.body.Write(p)
}

// Flush is a no-op: nothing is streamed (P7.11 has no streaming; REST responses are whole).
func (b *buffer) Flush() {}

func (b *buffer) code() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.status == 0 {
		return http.StatusOK
	}
	return b.status
}

// copyTo writes the buffered response to the connection.
func (b *buffer) copyTo(w http.ResponseWriter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	dst := w.Header()
	for k, v := range b.header {
		dst[k] = v
	}
	status := b.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(b.body.Bytes())
}
