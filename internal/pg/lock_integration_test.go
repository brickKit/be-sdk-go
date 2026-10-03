package pg

import (
	"context"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

// Advisory locks are transaction-level and keyed by schema + name + parts (P10.8).
func TestAdvisoryLocks(t *testing.T) {
	a, b := testpg.New(t), testpg.New(t)
	_, sa := standalone(t, a, 2)
	_, sb := standalone(t, b, 2)

	held, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- sa.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
			if err := tx.Lock(ctx, "widget", "a", "b"); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	try := func(s *Store, name string, parts ...string) bool {
		var ok bool
		require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
			var err error
			ok, err = tx.TryLock(ctx, name, parts...)
			return err
		}))
		return ok
	}
	require.False(t, try(sa, "widget", "a", "b"), "same schema, name and parts: taken")
	require.True(t, try(sa, "widget", "a", "c"), "other parts")
	require.True(t, try(sa, "other", "a", "b"), "other name")
	require.True(t, try(sb, "widget", "a", "b"), "another member's schema never collides")
	close(release)
	require.NoError(t, <-done)
	require.True(t, try(sa, "widget", "a", "b"), "released at commit")
}
