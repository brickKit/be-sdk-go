package besdk

import "sync"

// readiness is the state behind GET /readyz (P1.4): every declared condition (bundle, db_identity,
// migrations) must hold at once; from then on the process stays ready whatever happens outside it, so
// a downstream hiccup never takes every replica out of service.
type readiness struct {
	mu    sync.Mutex
	order []string
	met   map[string]bool
	ready bool
}

func newReadiness(conditions ...string) *readiness {
	r := &readiness{order: conditions, met: map[string]bool{}}
	r.ready = len(conditions) == 0
	return r
}

// Set records a condition; ignored once ready, and for a condition that was not declared.
func (r *readiness) Set(name string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ready {
		return
	}
	if _, declared := r.index(name); !declared {
		return
	}
	r.met[name] = ok
	for _, c := range r.order {
		if !r.met[c] {
			return
		}
	}
	r.ready = true
}

func (r *readiness) index(name string) (int, bool) {
	for i, c := range r.order {
		if c == name {
			return i, true
		}
	}
	return 0, false
}

// Ready reports whether every condition has held at once.
func (r *readiness) Ready() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ready
}

// Waiting lists the conditions not met yet, in declaration order (metadata.waiting).
func (r *readiness) Waiting() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ready {
		return nil
	}
	var out []string
	for _, c := range r.order {
		if !r.met[c] {
			out = append(out, c)
		}
	}
	return out
}
