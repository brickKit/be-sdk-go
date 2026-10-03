package lifecycle

import (
	"io/fs"
	"testing"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/stretchr/testify/require"
)

func widgetYAML(t *testing.T) []byte {
	b, err := fs.ReadFile(beprotocol.FS, "fixtures/widget/migrations/lifecycle.yaml")
	require.NoError(t, err)
	return b
}

func TestParseWidgetFixture(t *testing.T) {
	d, err := Parse(widgetYAML(t))
	require.NoError(t, err)
	require.Equal(t, []string{"owner_snapshots", "widget_audit", "widget_jobs", "widget_kinds", "widget_ledger",
		"widget_lines", "widget_owners", "widgets"}, d.Names())

	w := d.Tables["widgets"]
	require.Equal(t, Document, w.Class)
	require.Equal(t, &Partition{By: "created_at", Grain: Month, Ahead: 3}, w.Partition)
	require.Equal(t, &Closed{Column: "status", In: []string{"APPROVED", "SUSPENDED"}, At: "updated_at"}, w.Closed)
	require.Equal(t, Seal{Kind: SealAfter, After: After{Span{18, "mo"}, "closed"}}, w.Seal)

	l := d.Tables["widget_lines"]
	require.Equal(t, "widgets", l.Follows)
	require.Equal(t, Document, l.Class, "a follower takes its parent's class")
	require.Equal(t, w.Partition, l.Partition, "and its partitioning")
	require.Equal(t, w.Seal, l.Seal)
	require.Equal(t, []string{"widget_lines"}, d.Followers("widgets"))

	j := d.Tables["widget_jobs"]
	require.Equal(t, Queue, j.Class)
	require.Equal(t, &Partition{By: "created_at", Grain: Week, Ahead: 2}, j.Partition)
	require.Equal(t, &After{Span{30, "d"}, "created"}, j.RetentionMin)

	require.Equal(t, Seal{Kind: SealImmediate}, d.Tables["widget_ledger"].Seal)
	require.Nil(t, d.Tables["widget_kinds"].Partition)
	require.Nil(t, d.Tables["owner_snapshots"].RetentionMin)
}

func TestPartitionAheadDefaultsToTwo(t *testing.T) {
	d, err := Parse([]byte("lifecycle: v1\ntables:\n  t: {class: audit, partition: {by: created_at, grain: year}}\n"))
	require.NoError(t, err)
	require.Equal(t, 2, d.Tables["t"].Partition.Ahead)
}

// YAML 1.2 core schema: on / yes are strings, never booleans (P16.9).
func TestParseReadsYAML12Strings(t *testing.T) {
	d, err := Parse([]byte(`lifecycle: v1
tables:
  on:
    class: queue
    partition: {by: created_at, grain: week}
    closed: {column: done, in: [yes, on, off], at: updated_at}
`))
	require.NoError(t, err)
	require.Equal(t, []string{"yes", "on", "off"}, d.Tables["on"].Closed.In)
}

func TestParseRejectsAndNamesTheTable(t *testing.T) {
	cases := map[string]struct{ yaml, table string }{
		"ledger pii": {`lifecycle: v1
tables:
  gl: {class: ledger, pii: [note]}`, "gl"},
		"ledger erasure columns": {`lifecycle: v1
tables:
  gl: {class: ledger, erasure: {subject: customer, key: customer_id, columns: {name: anonymize}}}`, "gl"},
		"queue cold": {`lifecycle: v1
tables:
  jobs: {class: queue, tiers: {cold: 1y after created}}`, "jobs"},
		"snapshot retention": {`lifecycle: v1
tables:
  snap: {class: snapshot, retention: {min: 1y after created}}`, "snap"},
		"follows unknown": {`lifecycle: v1
tables:
  lines: {follows: orders}`, "lines"},
		"follows a follower": {`lifecycle: v1
tables:
  orders: {class: document}
  lines: {follows: orders}
  notes: {follows: lines}`, "notes"},
		"follower partitions itself": {`lifecycle: v1
tables:
  orders: {class: document, partition: {by: created_at, grain: month}}
  lines: {follows: orders, partition: {by: created_at, grain: week}}`, "lines"},
		"closed anchor without closed": {`lifecycle: v1
tables:
  orders: {class: document, tiers: {seal: 1mo after closed}}`, "orders"},
		"partitioned name too long": {`lifecycle: v1
tables:
  a_very_long_table_name_that_leaves_no_room_for_a_partition_suffix: {class: audit, partition: {by: created_at, grain: month}}`,
			"a_very_long_table_name_that_leaves_no_room_for_a_partition_suffix"},
		"unknown field": {`lifecycle: v1
tables:
  orders: {class: document, colour: red}`, "orders"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(c.yaml))
			require.Error(t, err)
			require.Contains(t, err.Error(), "table "+c.table+":")
		})
	}
}

func TestParseRejectsTheWholeDocument(t *testing.T) {
	for name, y := range map[string]string{
		"no version":     "tables: {}\n",
		"wrong version":  "lifecycle: v2\ntables: {}\n",
		"not yaml":       "lifecycle: [\n",
		"duplicate key":  "lifecycle: v1\ntables:\n  a: {class: master}\n  a: {class: master}\n",
		"tenant key set": "lifecycle: v1\ntenant_key: tenant_id\ntables: {}\n",
	} {
		_, err := Parse([]byte(y))
		require.Error(t, err, name)
		require.Contains(t, err.Error(), "lifecycle.yaml", name)
	}
}

func TestLedgerMessageSaysWhy(t *testing.T) {
	_, err := Parse([]byte("lifecycle: v1\ntables:\n  gl: {class: ledger, pii: [note]}\n"))
	require.ErrorContains(t, err, "table gl: a ledger table has no pii column")
}
