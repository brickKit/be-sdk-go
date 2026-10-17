//go:build integration

package work

import (
	"context"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPurgeKeepsWhatIsNotYetDue(t *testing.T) { // P7.19, CK-WK-10
	d, admin := dbtest.Schema(t, "work_purge", nil)
	pgtest.Exec(t, admin, "INSERT INTO be_cron_watermark VALUES ('nightly', now() - interval '1 day', NULL)")
	row := func(id, kind, state, finishedAgo, runAt, pause string) {
		pause = map[bool]string{true: "NULL", false: "'" + pause + "'"}[pause == ""]
		pgtest.Exec(t, admin, `INSERT INTO be_work_item (id, job, kind, state, max_attempts, effect_class, operation_id,
			run_at, finished_at, pause_reason) VALUES ('`+id+`', '`+map[bool]string{true: "nightly", false: "build"}[kind == "cron"]+
			`', '`+kind+`', '`+state+`', 3, 'local', '`+id+`', `+runAt+`, now() - interval '`+finishedAgo+`', `+pause+`)`)
	}
	row("q-old-done", "queue", "done", "8 days", "now()", "")
	row("q-old-resolved", "queue", "resolved", "8 days", "now()", "")
	row("q-recent-done", "queue", "done", "6 days", "now()", "")
	row("q-old-dead", "queue", "dead", "90 days", "now()", "")
	row("q-old-paused", "queue", "paused", "90 days", "now()", "outcome_unknown")
	row("nightly@old-behind", "cron", "done", "31 days", "now() - interval '40 days'", "")
	row("nightly@old-ahead", "cron", "done", "31 days", "now()", "")
	row("nightly@recent", "cron", "done", "10 days", "now() - interval '10 days'", "")

	n, err := Purge(context.Background(), d)
	require.NoError(t, err)
	assert.EqualValues(t, 3, n)
	rows, err := admin.Query(context.Background(), "SELECT id FROM be_work_item ORDER BY id")
	require.NoError(t, err)
	left, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	assert.Equal(t, []string{"nightly@old-ahead", "nightly@recent", "q-old-dead", "q-old-paused", "q-recent-done"}, left,
		"dead and paused stay; cron slots ahead of the watermark stay so they are never materialized again")
}
