//go:build integration

package events

import (
	"context"
	"strconv"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPurgeKeepsUnpublishedAndRecentRows(t *testing.T) { // P6.12, CK-EV-05
	d, admin := source(t, "outbox_purge")
	insert := func(record, published string) {
		pgtest.Exec(t, admin, `INSERT INTO be_outbox (event_id, stream, partition, type, occurred_at, record_id, version,
			projection, hops, payload, digest, published_at) VALUES ('`+ids.New().String()+`', 'demo.s', 0, 'demo.x.v1',
			now() - interval '30 days', '`+record+`', 1, 'full', 0, '\x7b7d', 'd', `+published+`)`)
	}
	insert("published-8-days-ago", "now() - interval '8 days'")
	insert("published-6-days-ago", "now() - interval '6 days'")
	insert("never-published", "NULL")
	// a backlog larger than one batch goes too: each batch is its own transaction under the statement timeout
	pgtest.Exec(t, admin, `INSERT INTO be_outbox (event_id, stream, partition, type, occurred_at, record_id, version,
		projection, hops, payload, digest, published_at) SELECT gen_random_uuid(), 'demo.s', 0, 'demo.x.v1', now(),
		'backlog', 1, 'full', 0, '\x7b7d', 'd', now() - interval '9 days' FROM generate_series(1, 2500)`)

	// numbered rows note how far the purge went on their partition (P6.12, P6.38): the largest position deleted
	pgtest.Exec(t, admin, `INSERT INTO be_outbox_partition (stream, partition, epoch_generation, epoch_timeline, last_seq)
		VALUES ('demo.s', 0, 1, 1, 12)`)
	for seq, age := range map[int]string{7: "9 days", 9: "8 days", 12: "1 day"} {
		pgtest.Exec(t, admin, `INSERT INTO be_outbox (event_id, stream, partition, type, occurred_at, record_id, version,
			projection, hops, payload, digest, seq, epoch_generation, epoch_timeline, published_at) VALUES ('`+
			ids.New().String()+`', 'demo.s', 0, 'demo.x.v1', now(), 'numbered', 1, 'full', 0, '\x7b7d', 'd', `+
			strconv.Itoa(seq)+`, 1, 1, now() - interval '`+age+`')`)
	}

	n, err := Purge(context.Background(), d, RedeliveryRetention)
	require.NoError(t, err)
	assert.EqualValues(t, 2503, n)
	var purged int64
	require.NoError(t, admin.QueryRow(context.Background(), `SELECT purged_seq FROM be_outbox_partition
		WHERE stream = 'demo.s' AND partition = 0 AND purged_generation = 1 AND purged_timeline = 1`).Scan(&purged))
	assert.EqualValues(t, 9, purged, "the largest position deleted; the one kept (12) is not")
	rows, err := admin.Query(context.Background(), "SELECT record_id FROM be_outbox ORDER BY record_id")
	require.NoError(t, err)
	left, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	assert.Equal(t, []string{"never-published", "numbered", "published-6-days-ago"}, left)
}
