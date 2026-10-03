package pg

import (
	"context"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

// Two members of a shell (NOINHERIT login role, PostgreSQL 16) with differently shaped same-name
// tables alternate on one physical connection without 0A000 / 42804: the per-member SQL prefix keeps
// pgx's statement cache apart (P10.2, r1-04b).
func TestShellMembersShareOneConnection(t *testing.T) {
	a, b := testpg.New(t), testpg.New(t)
	asOwner(t, a, `CREATE TABLE besdk_thing (id int PRIMARY KEY, payload text)`)
	asOwner(t, b, `CREATE TABLE besdk_thing (id int PRIMARY KEY, payload jsonb, extra int)`)
	role, password := testpg.ShellRole(t, a, b)
	p := poolFor(t, a, role, password, 1)
	sa := NewStore(p, StoreConfig{ComponentID: "conformance/a", Role: a.User, Schema: a.Schema})
	sb := NewStore(p, StoreConfig{ComponentID: "conformance/b", Role: b.User, Schema: b.Schema})

	for i := 1; i <= 4; i++ {
		require.NoError(t, sa.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO besdk_thing (id, payload) VALUES ($1, $2)`, i, "a-text"); err != nil {
				return err
			}
			var n int
			var s string
			if err := tx.QueryRowContext(ctx, `SELECT * FROM besdk_thing WHERE id = $1`, i).Scan(&n, &s); err != nil {
				return err
			}
			require.Equal(t, "a-text", s)
			return nil
		}), "member a, round %d", i)
		require.NoError(t, sb.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO besdk_thing (id, payload) VALUES ($1, $2)`, i, `{"b":1}`); err != nil {
				return err
			}
			var n, extra *int
			var s string
			if err := tx.QueryRowContext(ctx, `SELECT * FROM besdk_thing WHERE id = $1`, i).Scan(&n, &s, &extra); err != nil {
				return err
			}
			require.JSONEq(t, `{"b":1}`, s)
			return nil
		}), "member b, round %d", i)
	}
	// each member's identity in its own transactions; the shell role itself sees nothing
	var who string
	require.NoError(t, sb.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		return tx.QueryRowContext(ctx, `SELECT current_user || '/' || session_user || '/' || current_setting('application_name')`).Scan(&who)
	}))
	require.Equal(t, b.User+"/"+role+"/conformance/b", who)
	_, err := p.db.ExecContext(context.Background(), `SELECT 1 FROM `+a.Schema+`.besdk_thing`)
	require.Error(t, err, "the shell login role has no privilege of its own")
}
