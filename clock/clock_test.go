package clock

import (
	"testing"
	"time"
)

func TestBootClockAdvances(t *testing.T) {
	if err := Check(); err != nil {
		t.Fatal(err)
	}
	a, _ := SinceBoot()
	time.Sleep(5 * time.Millisecond)
	b, _ := SinceBoot()
	if b-a < 4*time.Millisecond {
		t.Fatalf("CLOCK_BOOTTIME advanced %v in 5ms", b-a)
	}
}
