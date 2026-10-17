// Package clock reads the clock that keeps counting while the system is suspended (protocol P1.11; security 03 counts
// validity periods on it). On Linux that is CLOCK_BOOTTIME.
package clock

import (
	"time"

	"golang.org/x/sys/unix"
)

// SinceBoot returns the time since boot including suspension.
func SinceBoot() (time.Duration, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0, err
	}
	return time.Duration(ts.Nano()), nil
}

// Check fails when the clock is not available; the launcher then refuses to start (exit 69).
func Check() error {
	a, err := SinceBoot()
	if err != nil {
		return err
	}
	b, err := SinceBoot()
	if err != nil {
		return err
	}
	if b < a {
		return errNotMonotonic
	}
	return nil
}

type clockError string

func (e clockError) Error() string { return string(e) }

const errNotMonotonic = clockError("CLOCK_BOOTTIME went backwards")

// Boot returns the suspend-aware clock reading; the start check has already proven it works.
func Boot() time.Duration {
	d, _ := SinceBoot()
	return d
}
