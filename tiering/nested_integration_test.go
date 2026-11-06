//go:build integration

package tiering_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An aggregate of three levels, with foreign keys at every level: an invoice, its lines, and under each line its
// taxes (one also pointing at its sibling table) and a free note (personal data).
const nestedSQL = `CREATE TABLE invoices (id uuid PRIMARY KEY, entity uuid NOT NULL, closed_at timestamptz,
  created_at timestamptz NOT NULL, status text NOT NULL);
CREATE TABLE invoice_lines (id uuid PRIMARY KEY, invoice_id uuid NOT NULL REFERENCES invoices (id), amount numeric(19,4) NOT NULL);
CREATE TABLE line_taxes (id uuid PRIMARY KEY, line_id uuid NOT NULL REFERENCES invoice_lines (id), rate numeric(9,6) NOT NULL,
  remark text);`

func nestedClass() tiering.Class {
	return tiering.Class{Name: "billing.invoices", Category: tiering.CategoryCompleted,
		Root: tiering.Table{Name: "invoices", PK: "id", Columns: map[string]string{"id": tiering.TypeUUID,
			"entity": tiering.TypeUUID, "closed_at": tiering.TypeTimestamptz, "created_at": tiering.TypeTimestamptz,
			"status": tiering.TypeText}},
		Children: []tiering.Child{
			{Table: tiering.Table{Name: "line_taxes", PK: "id", Columns: map[string]string{"id": tiering.TypeUUID,
				"line_id": tiering.TypeUUID, "rate": tiering.TypeNumeric, "remark": tiering.TypeText}}, FK: "line_id",
				Parent: "invoice_lines"}, // declared before its parent: the order of the declaration does not matter
			{Table: tiering.Table{Name: "invoice_lines", PK: "id", Columns: map[string]string{"id": tiering.TypeUUID,
				"invoice_id": tiering.TypeUUID, "amount": tiering.TypeNumeric}}, FK: "invoice_id"}},
		Group: []string{"entity"}, Anchor: "closed_at", Created: "created_at", Terminal: "r.status = 'closed'",
		ResidenceDays: 365, Retention: tiering.Retention{MinDays: 3650, MaxDays: 3650, Basis: "test"},
		Locate: []string{"entity"}, Erasable: []string{"remark"}, Routes: []string{tiering.RoutePostgres},
		SchemaVersion: "1", HoldOperation: "billing.invoice.hold", ApproveOperation: "billing.invoice.approve"}
}

// invoice inserts an invoice (closed 400 days ago, or open) with two lines of two taxes each; it answers its id and
// the id of one tax row.
func invoice(t *testing.T, admin *pgx.Conn, entity string, closed bool) (string, string) {
	t.Helper()
	id, tax := ids.New().String(), ""
	status, at := "open", "NULL"
	if closed {
		status, at = "closed", "now() - interval '400 days'"
	}
	pgtest.Exec(t, admin, fmt.Sprintf("INSERT INTO invoices VALUES ('%s', '%s', %s, now() - interval '500 days', '%s')", id, entity, at, status))
	for l := 0; l < 2; l++ {
		line := ids.New().String()
		pgtest.Exec(t, admin, fmt.Sprintf("INSERT INTO invoice_lines VALUES ('%s', '%s', %d.5)", line, id, l+1))
		for x := 0; x < 2; x++ {
			tax = ids.New().String()
			pgtest.Exec(t, admin, fmt.Sprintf("INSERT INTO line_taxes VALUES ('%s', '%s', 0.21, 'Mr Example, +34 600')", tax, line))
		}
	}
	return id, tax
}

// P15.1, P15.13, P15.19, P15.22, P15.31, A155: an aggregate of three levels is sealed with every level, a write to a
// tax of a sealed invoice is refused (its trigger finds the root up the chain), the unit is copied, read back by key
// with every level as it was online, released in batches of one row (the deepest level first: no foreign key stops
// it), and a tax's remark is erased in the archived copy.
func TestAnAggregateOfThreeLevels(t *testing.T) {
	for _, route := range []string{tiering.RoutePostgres, tiering.RouteObject} {
		t.Run(route, func(t *testing.T) {
			ctx := context.Background()
			d, admin := dbtest.Schema(t, "tier_nested_"+strings.ReplaceAll(route, "-", "_"),
				fstest.MapFS{"0001_billing.up.sql": {Data: []byte(nestedSQL)}})
			cl := nestedClass()
			cl.Routes = []string{route}
			require.NoError(t, cl.Check(90, nil))
			stmts, err := tiering.BarrierSQL([]tiering.Class{cl})
			require.NoError(t, err)
			pgtest.Exec(t, admin, stmts...)
			store, s3admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
			ensureBucket(t, s3admin, tierBucket, false)
			cat := &tiering.Catalog{DB: d, Classes: map[string]tiering.Class{cl.Name: cl}, Registry: tiering.NewRegistry(),
				Store: store, Batch: 1, Backups: backupAt(time.Now().Add(time.Hour)),
				Evidence: &evidence.Writer{DB: d, Store: &keeper{}, Component: "test/billing", Instance: "t-1",
					Log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
			entity := ids.New().String()
			a, tax := invoice(t, admin, entity, true)
			b, _ := invoice(t, admin, entity, true)
			open, openTax := invoice(t, admin, entity, false)

			online := func(id string) string { // the invoice's lower levels as one sorted text, values canonical
				rows, err := admin.Query(ctx, `SELECT 'line ' || l.id || ' ' || l.amount FROM invoice_lines l WHERE l.invoice_id = $1
					UNION ALL SELECT 'tax ' || x.id || ' ' || x.line_id || ' ' || x.rate || ' ' || coalesce(x.remark, '-')
					FROM line_taxes x JOIN invoice_lines l ON l.id = x.line_id WHERE l.invoice_id = $1`, id)
				require.NoError(t, err)
				texts, err := pgx.CollectRows(rows, pgx.RowTo[string])
				require.NoError(t, err)
				var out []string
				for _, x := range texts {
					f := strings.SplitN(x, " ", 5)
					if f[0] == "line" {
						out = append(out, "line "+f[1]+" "+decimal(t, f[2]))
					} else {
						out = append(out, "tax "+f[1]+" "+f[2]+" "+decimal(t, f[3])+" "+f[4])
					}
				}
				sort.Strings(out)
				return strings.Join(out, "|")
			}
			before := online(a)

			groups, err := cat.Groupings(ctx, cl.Name)
			require.NoError(t, err)
			res, err := cat.Seal(ctx, cl.Name, groups[0])
			require.NoError(t, err)
			require.Equal(t, 2, res.Members)
			write := func(id string) error {
				return d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
					_, err := tx.Exec(ctx, "UPDATE line_taxes SET rate = 0.1 WHERE id = $1", id)
					return err
				})
			}
			err = write(tax)
			assert.True(t, errs.Is(err, "be", errs.ReasonRecordSealed), "a tax of a sealed invoice: %v", err)
			assert.NoError(t, write(openTax), "the open invoice's tax")

			_, err = cat.Transfer(ctx, res.Unit)
			require.NoError(t, err)
			agg, found, err := cat.Get(ctx, cl.Name, a)
			require.NoError(t, err)
			require.True(t, found)
			assert.Len(t, agg.Children["invoice_lines"], 2)
			assert.Len(t, agg.Children["line_taxes"], 4, "the third level under its root")
			var got []string
			for _, l := range agg.Children["invoice_lines"] {
				got = append(got, fmt.Sprintf("line %s %s", l["id"], l["amount"]))
			}
			for _, x := range agg.Children["line_taxes"] {
				got = append(got, fmt.Sprintf("tax %s %s %s %s", x["id"], x["line_id"], x["rate"], x["remark"]))
			}
			sort.Strings(got)
			assert.Equal(t, before, strings.Join(got, "|"), "as it was online")

			why, err := cat.Release(ctx, res.Unit)
			require.NoError(t, err)
			require.Empty(t, why)
			for _, q := range []string{"SELECT count(*) FROM invoices WHERE id IN ($1, $2)",
				"SELECT count(*) FROM invoice_lines WHERE invoice_id IN ($1, $2)",
				"SELECT count(*) FROM line_taxes x JOIN invoice_lines l ON l.id = x.line_id WHERE l.invoice_id IN ($1, $2)"} {
				var n int
				require.NoError(t, admin.QueryRow(ctx, q, a, b).Scan(&n))
				assert.Zero(t, n, q)
			}
			var left int
			require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM line_taxes x JOIN invoice_lines l ON l.id = x.line_id "+
				"WHERE l.invoice_id = $1", open).Scan(&left))
			assert.Equal(t, 4, left, "the open invoice keeps its taxes")

			outcome, err := cat.Erase(ctx, cl.Name, a, "request ER-7")
			require.NoError(t, err)
			assert.Equal(t, tiering.Erased, outcome)
			agg, _, err = cat.Get(ctx, cl.Name, a)
			require.NoError(t, err)
			require.Len(t, agg.Children["line_taxes"], 4)
			for _, x := range agg.Children["line_taxes"] {
				assert.NotContains(t, x, "remark", "erased on the third level")
			}
			other, _, err := cat.Get(ctx, cl.Name, b)
			require.NoError(t, err)
			assert.Equal(t, "Mr Example, +34 600", other.Children["line_taxes"][0]["remark"])
		})
	}
}

func decimal(t *testing.T, s string) string {
	t.Helper()
	d, err := canon.Decimal(s)
	require.NoError(t, err)
	return d
}
