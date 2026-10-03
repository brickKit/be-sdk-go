package acl

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTuple is a direct tuple in the fake provider's state.
type fakeTuple struct {
	Type, ID, Relation, Subject string
	ExpiresAt                   *time.Time
}

func (f fakeTuple) key() string { return f.Type + "|" + f.ID + "|" + f.Relation + "|" + f.Subject }

type fakeChange struct {
	rev int64
	op  string
	t   fakeTuple
}

// fakeProvider implements GET /authz/v2/changes and /authz/v2/tuples of contract-infra-authz
// (changefeed.schema.json): a change log with a floor below which it answers 410, and snapshots at
// the head. Revisions are global; every write takes the next one.
type fakeProvider struct {
	mu        sync.Mutex
	log       []fakeChange
	state     map[string]fakeTuple
	head      int64
	floor     int64 // after < floor answers 410
	off       bool  // answers 501 CAPABILITY_UNAVAILABLE
	delay     time.Duration
	lag       map[string]time.Duration // per be-caller: the answer is computed, then held back this long
	requests  []string
	callers   []string
	snapshots int
	srv       *httptest.Server
}

func newFakeProvider(t *testing.T) *fakeProvider {
	f := &fakeProvider{state: map[string]fakeTuple{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeProvider) write(op string, t fakeTuple) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.head++
	f.log = append(f.log, fakeChange{rev: f.head, op: op, t: t})
	if op == "upsert" {
		f.state[t.key()] = t
	} else {
		delete(f.state, t.key())
	}
	return f.head
}

func (f *fakeProvider) upsert(typ, id, rel, subj string) int64 {
	return f.write("upsert", fakeTuple{Type: typ, ID: id, Relation: rel, Subject: subj})
}

func (f *fakeProvider) remove(typ, id, rel, subj string) int64 {
	return f.write("delete", fakeTuple{Type: typ, ID: id, Relation: rel, Subject: subj})
}

// compact forgets the log up to the head: every after below it answers 410.
func (f *fakeProvider) compact() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.floor = f.head
	f.log = nil
}

func (f *fakeProvider) set(fn func(f *fakeProvider)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeProvider) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.URL.Path+"?"+r.URL.RawQuery)
	f.callers = append(f.callers, r.Header.Get("be-caller"))
	delay, off := f.delay, f.off
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if off {
		problem(w, 501, "CAPABILITY_UNAVAILABLE")
		return
	}
	rec := httptest.NewRecorder()
	switch r.URL.Path {
	case "/authz/v2/changes":
		f.changes(rec, r)
	case "/authz/v2/tuples":
		f.tuples(rec, r)
	default:
		http.NotFound(rec, r)
	}
	f.mu.Lock()
	lag := f.lag[r.Header.Get("be-caller")]
	f.mu.Unlock()
	time.Sleep(lag)
	for k, v := range rec.Header() {
		w.Header()[k] = v
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

func (f *fakeProvider) changes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	types := strings.Split(q.Get("types"), ",")
	after, err := strconv.ParseInt(q.Get("after"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	if err != nil || limit < 1 || limit > 500 {
		problem(w, 400, "REQUEST_INVALID")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if after < f.floor || after > f.head {
		problem(w, 410, "CHANGES_EXPIRED")
		return
	}
	page := []any{}
	next := after
	for _, c := range f.log {
		if c.rev <= after || !slices.Contains(types, c.t.Type) {
			continue
		}
		if len(page) == limit {
			break
		}
		page = append(page, map[string]any{"revision": strconv.FormatInt(c.rev, 10), "op": c.op, "tuple": fakeWire(c.t)})
		next = c.rev
	}
	writeJSON(w, map[string]any{"changes": page, "next": strconv.FormatInt(next, 10), "watermark": strconv.FormatInt(f.head, 10)})
}

func (f *fakeProvider) tuples(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	typ := q.Get("type")
	size, _ := strconv.Atoi(q.Get("page_size"))
	if typ == "" || size < 1 || size > 1000 {
		problem(w, 400, "REQUEST_INVALID")
		return
	}
	start := 0
	if c := q.Get("cursor"); c != "" {
		start, _ = strconv.Atoi(c)
	} else {
		f.mu.Lock()
		f.snapshots++
		f.mu.Unlock()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []fakeTuple
	for _, t := range f.state {
		if t.Type == typ {
			all = append(all, t)
		}
	}
	slices.SortFunc(all, func(a, b fakeTuple) int { return strings.Compare(a.key(), b.key()) })
	end := min(start+size, len(all))
	out := []any{}
	for _, t := range all[start:end] {
		out = append(out, fakeWire(t))
	}
	next := ""
	if end < len(all) {
		next = strconv.Itoa(end)
	}
	writeJSON(w, map[string]any{"tuples": out, "next_cursor": next, "revision": strconv.FormatInt(f.head, 10)})
}

func fakeWire(t fakeTuple) map[string]any {
	m := map[string]any{"object": map[string]string{"type": t.Type, "id": t.ID}, "relation": t.Relation, "subject": t.Subject}
	if t.ExpiresAt != nil {
		m["expires_at"] = t.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return m
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func problem(w http.ResponseWriter, code int, reason string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": code, "reason": reason})
}

// stateOf is the provider's current tuples of the given types, as sorted keys.
func (f *fakeProvider) stateOf(types ...string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []string{}
	for k, t := range f.state {
		if slices.Contains(types, t.Type) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

func (f *fakeProvider) requestLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}
