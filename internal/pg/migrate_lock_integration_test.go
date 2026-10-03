package pg

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

// lockedSetup migrates widget to version 2, then has the owner's own session hold an ACCESS
// EXCLUSIVE lock on widget (so the owner can read the holder's query text), and returns a component
// FS whose version 3 needs that lock.
func lockedSetup(t *testing.T) (testpg.Identity, fstest.MapFS, *sql.Tx, int) {
	id := testpg.New(t)
	_, err := MigrateUp(within(t, 60e9), migrateConfig(id, widgetFS(), nil))
	require.NoError(t, err)
	holderDB := testpg.Open(t, id.DSN(id.Owner, id.OwnerPassword))
	holder, err := holderDB.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Rollback() })
	var pid int
	require.NoError(t, holder.QueryRow(`SELECT pg_backend_pid()`).Scan(&pid))
	_, err = holder.Exec(`LOCK TABLE ` + id.Schema + `.widget IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	fs := widgetFS()
	fs["3_widget_color.up.sql"] = &fstest.MapFile{Data: []byte(`ALTER TABLE widget ADD COLUMN color text;`)}
	fs["3_widget_color.down.sql"] = &fstest.MapFile{Data: []byte(`ALTER TABLE widget DROP COLUMN color;`)}
	return id, fs, holder, pid
}

func fastLocks(c MigrateConfig) MigrateConfig {
	c.lockTimeout, c.retryBackoff = 300*time.Millisecond, 50*time.Millisecond
	return c
}

func TestMigrateRetriesLockTimeout(t *testing.T) {
	id, fs, holder, pid := lockedSetup(t)
	var buf bytes.Buffer
	c := fastLocks(migrateConfig(id, fs, slog.New(slog.NewJSONHandler(&buf, nil))))
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = holder.Rollback()
	}()
	r, err := MigrateUp(within(t, 60e9), c)
	require.NoError(t, err)
	require.Equal(t, uint(3), r.To)
	require.Contains(t, buf.String(), "migration lock timeout, retrying")
	require.Contains(t, buf.String(), `"pid":`+strconv.Itoa(pid))
}

func TestMigrateGivesUpAfterRetriesAndStaysClean(t *testing.T) {
	id, fs, _, pid := lockedSetup(t)
	var buf bytes.Buffer
	c := fastLocks(migrateConfig(id, fs, slog.New(slog.NewJSONHandler(&buf, nil))))
	start := time.Now()
	_, err := MigrateUp(within(t, 60e9), c)
	require.True(t, problem.Is(err, problem.DomainBe, "LOCK_TIMEOUT"), "%v", err)
	require.Less(t, time.Since(start), 10*time.Second)
	require.Contains(t, buf.String(), `"level":"ERROR","msg":"migration lock timeout, giving up"`)
	require.Contains(t, buf.String(), `"pid":`+strconv.Itoa(pid))
	require.Contains(t, buf.String(), "LOCK TABLE "+id.Schema+".widget", "the blocker's query, first 200 characters")
	st, err := MigrateStatus(within(t, 60e9), c)
	require.NoError(t, err)
	require.Equal(t, uint(2), st.From)
	require.False(t, st.Dirty, "the failed step rolled back, so its version is restored")
}
