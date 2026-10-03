package problem

import "testing"

// Aborted is the panic value of a programming-error guard in a test build.
type Aborted struct{ Err *Error }

func (a Aborted) Error() string { return "aborted: " + a.Err.Error() }

// Abort returns e, a programming error caught by an internal guard (NESTED_TX, NETWORK_IN_TX); in a
// test binary it panics instead, so the bug fails the test that caused it rather than becoming a
// silent INTERNAL answer (P4 "Internal guard failures", P8.4, P10.6).
func Abort(e *Error) *Error {
	if testing.Testing() {
		panic(Aborted{Err: e})
	}
	return e
}

// Catch runs fn and returns the error a guard aborted with, or fn's own error: the SDK's tests use it
// to assert a guard fired.
func Catch(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			a, ok := r.(Aborted)
			if !ok {
				panic(r)
			}
			err = a.Err
		}
	}()
	return fn()
}
