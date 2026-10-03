package events

import "errors"

// permanentError marks a handler error that redelivery cannot fix (P12.7).
type permanentError struct{ err error }

func (p *permanentError) Error() string { return "permanent: " + p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent wraps err so the consumer dead-letters the event at once (reason PERMANENT) instead
// of redelivering it (P12.7). Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err's chain holds an error wrapped by Permanent (P12.7).
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
