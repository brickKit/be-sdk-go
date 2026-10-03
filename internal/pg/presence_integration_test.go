package pg

import (
	"context"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/testpg"
)

// P10.5 / P11.4 (rc.2): while it serves, a component keeps one session open under its versioned name
// "<id>@<version>", even after every other idle connection expired, so a contract migration sees it;
// inside a transaction the member's own name applies (SET LOCAL, P10.2).
func TestPresenceKeepsAVersionedSessionOpen(t *testing.T) {
	id := testpg.New(t)
	p, err := OpenPool(PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: id.User,
		Password: func() string { return id.Password }, SSLMode: "disable", MaxConns: 4,
		MaxIdleTime: 50 * time.Millisecond, ApplicationName: AppName("conformance/widget", "3.1.0")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.HoldPresence(ctx, 20*time.Millisecond) }()
	super := testpg.Open(t, id.SuperDSN)
	count := func() int {
		var n int
		if err := super.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE application_name = 'conformance/widget@3.1.0'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	deadline := time.Now().Add(5 * time.Second)
	for count() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // well past MaxIdleTime
	if n := count(); n != 1 {
		t.Fatalf("versioned sessions = %d, want 1", n)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("HoldPresence returned %v on cancel", err)
	}
}
