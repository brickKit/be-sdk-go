//go:build integration

package db_test

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// partitioned is a component migration with a table partitioned by business date, declared for two months ahead.
var partitioned = fstest.MapFS{"0001_orders.up.sql": {Data: []byte(`
CREATE TABLE orders (id uuid NOT NULL, business_date date NOT NULL, amount numeric NOT NULL CHECK (amount >= 0),
    PRIMARY KEY (id, business_date)) PARTITION BY RANGE (business_date);
SELECT be_declare_monthly_partitions('orders', 2);`)}}

func months(from time.Time, n int) []string {
	var out []string
	for m := 0; m <= n; m++ {
		out = append(out, "orders_p"+time.Date(from.Year(), from.Month()+time.Month(m), 1, 0, 0, 0, 0, time.UTC).Format("200601"))
	}
	return out
}

func partitions(t *testing.T, admin *pgx.Conn, schema string) []string {
	t.Helper()
	rows, err := admin.Query(context.Background(), `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = $1::regclass ORDER BY 1`, schema+".orders")
	require.NoError(t, err)
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return names
}

// dbMonth is the database's current month (UTC), the one the maintenance counts from.
func dbMonth(t *testing.T, admin *pgx.Conn) time.Time {
	t.Helper()
	var m time.Time
	require.NoError(t, admin.QueryRow(context.Background(),
		"SELECT date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'").Scan(&m))
	return m.UTC()
}

func maintain(t *testing.T, d *db.DB, sql ...string) ([]string, error) {
	t.Helper()
	var created []string
	err := d.BackgroundTx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		for _, q := range sql {
			if _, err := tx.Exec(ctx, q); err != nil {
				return err
			}
		}
		var err error
		created, err = db.MaintainPartitions(ctx, tx)
		return err
	})
	return created, err
}

func TestMonthlyPartitions(t *testing.T) { // P5.21-P5.23, CK-DB-08
	d, admin := dbtest.Schema(t, "part_a", partitioned)
	ctx := context.Background()
	month := dbMonth(t, admin)
	require.Equal(t, months(month, 2), partitions(t, admin, "part_a"), "declaring creates this month and two ahead")

	created, err := maintain(t, d)
	require.NoError(t, err)
	require.Empty(t, created, "maintaining again creates nothing")
	require.NoError(t, d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO orders VALUES ($1, current_date, 1)", ids.New())
		return err
	}))

	// the runtime role cannot change the plan (the trigger refuses it), and a temporary table of the same name is
	// not read
	for _, sql := range []string{"UPDATE be_partition_plan SET months_ahead = 24", "DELETE FROM be_partition_plan"} {
		err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := tx.Exec(ctx, sql)
			return err
		})
		require.ErrorContains(t, err, "written by migrations only", sql)
	}
	created, err = maintain(t, d, `CREATE TEMP TABLE be_partition_plan (table_name text, key_type text, months_ahead int) ON COMMIT DROP`,
		`INSERT INTO pg_temp.be_partition_plan VALUES ('orders', 'date', 20)`)
	require.NoError(t, err)
	require.Empty(t, created)

	// another component's runtime role may not run this schema's maintenance even with access to the schema
	pgtest.Member(t, "part_b", "")
	pgtest.Exec(t, admin, "GRANT USAGE ON SCHEMA part_a TO sdk_part_b_rw", "SET ROLE sdk_part_b_rw")
	_, err = admin.Exec(ctx, "SELECT part_a.be_maintain_partitions()")
	require.ErrorContains(t, err, "permission denied")
	pgtest.Exec(t, admin, "RESET ROLE", "SET search_path = part_a")

	// a detached partition of the expected name, or an overlapping one, fails the maintenance
	last := months(month, 2)[2]
	pgtest.Exec(t, admin, "ALTER TABLE orders DETACH PARTITION "+last)
	maintain := func() error {
		return d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := db.MaintainPartitions(ctx, tx)
			return err
		})
	}
	require.ErrorContains(t, maintain(), "detached")
	pgtest.Exec(t, admin, "DROP TABLE "+last)
	start := time.Date(month.Year(), month.Month()+2, 15, 0, 0, 0, 0, time.UTC)
	pgtest.Exec(t, admin, "CREATE TABLE orders_odd PARTITION OF orders FOR VALUES FROM ('"+start.Format(time.DateOnly)+
		"') TO ('"+start.AddDate(0, 1, 0).Format(time.DateOnly)+"')")
	require.ErrorContains(t, maintain(), "overlap")
	pgtest.Exec(t, admin, "DROP TABLE orders_odd")
	require.NoError(t, maintain())
	require.Equal(t, months(month, 2), partitions(t, admin, "part_a"))
}

func TestOnlyRangePartitionedTablesAreDeclared(t *testing.T) { // P5.21
	_, admin := dbtest.Schema(t, "part_c", nil)
	for _, sql := range []string{
		"CREATE TABLE plain (id uuid PRIMARY KEY)",
		"CREATE TABLE by_list (id uuid, region text) PARTITION BY LIST (region)",
		"CREATE TABLE by_text (id uuid, k text) PARTITION BY RANGE (k)",
	} {
		pgtest.Exec(t, admin, sql)
	}
	long := strings.Repeat("x", 56)
	pgtest.Exec(t, admin, "CREATE TABLE "+long+" (id uuid, d date) PARTITION BY RANGE (d)")
	for _, table := range []string{"plain", "by_list", "by_text", "missing", long} {
		_, err := admin.Exec(context.Background(), "SELECT be_declare_monthly_partitions($1)", table)
		require.Error(t, err, table)
	}
}

func TestDroppingADeclaredTableNeedsUndeclaring(t *testing.T) { // P5.21
	d, admin := dbtest.Schema(t, "part_d", partitioned)
	pgtest.Exec(t, admin, "SET ROLE sdk_part_d_owner", "DROP TABLE orders")
	_, err := maintain(t, d)
	require.ErrorContains(t, err, "no longer exists")
	pgtest.Exec(t, admin, "SELECT be_undeclare_monthly_partitions('orders')", "RESET ROLE")
	_, err = maintain(t, d)
	require.NoError(t, err)
}
