package transporttest

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// Run runs every case as a parallel subtest.
func Run(t *testing.T, f Factory) {
	for _, c := range Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			c.Run(t, f)
		})
	}
}

// Probe runs one case outside the testing package and reports whether it failed and why: used to show that the
// suite fails against a broken adapter.
func Probe(c Case, f Factory) (failed bool, messages []string) {
	p := &probe{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer p.cleanup()
		c.Run(p, f)
	}()
	<-done
	return p.failed, p.messages
}

type probe struct {
	mu       sync.Mutex
	failed   bool
	messages []string
	cleanups []func()
}

func (p *probe) Helper() {}
func (p *probe) Errorf(format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failed = true
	p.messages = append(p.messages, fmt.Sprintf(format, args...))
}
func (p *probe) FailNow() {
	p.mu.Lock()
	p.failed = true
	p.mu.Unlock()
	runtime.Goexit()
}
func (p *probe) Cleanup(fn func()) { p.mu.Lock(); p.cleanups = append(p.cleanups, fn); p.mu.Unlock() }
func (p *probe) cleanup() {
	for i := len(p.cleanups) - 1; i >= 0; i-- {
		p.cleanups[i]()
	}
}
