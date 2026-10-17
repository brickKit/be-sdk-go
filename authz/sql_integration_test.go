//go:build integration

package authz

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompiledListsMatchTheVectors(t *testing.T) { // P8.17, P8.18, CK-AZ-05
	f, cases := loadEval(t)
	r := f.resource()
	at, err := time.Parse(time.RFC3339Nano, f.At)
	require.NoError(t, err)
	d, _ := dbtest.Schema(t, "authzsql", fstest.MapFS{"0001_orders.up.sql": {Data: []byte(`CREATE TABLE demo_order (
		id text PRIMARY KEY, legal_entity text, dept_path text, owner_id uuid, amount numeric, currency text);`)}})
	ctx := context.Background()
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		for _, rec := range f.Records {
			facts := f.facts(r, rec)
			if facts["legal_entity"].State == "unobtained" || facts["amount"].State == "unobtained" {
				continue // single-object only: a table has no "not obtained"
			}
			val := func(dim string) any {
				if fa := facts[dim]; fa.State == "value" {
					return fa.Value
				}
				return nil
			}
			var currency any
			if fa := facts["amount"]; fa.State == "value" {
				currency = fa.Currency
			}
			if _, err := tx.Exec(ctx, "INSERT INTO demo_order VALUES ($1, $2, $3, $4, $5, $6)", idOf(rec),
				val("legal_entity"), val("dept"), val("owner"), val("amount"), currency); err != nil {
				return err
			}
		}
		return nil
	}))

	list := func(c evalCase, op string, filter map[string]string) (ids []string, fields map[string][]string) {
		p, err := Compile(r, c.Segments, op, c.Principal, at, "o", 0)
		require.NoError(t, err, c.ID)
		where, args := p.Where, append([]any{}, p.Args...)
		for col, v := range filter {
			args = append(args, v)
			where += " AND o." + col + " = $" + string(rune('0'+len(args)))
		}
		sel := "o.id"
		if len(p.Hits) > 0 {
			sel += ", " + strings.Join(p.Hits, ", ")
		}
		fields = map[string][]string{}
		require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
			rows, err := tx.Query(ctx, "SELECT "+sel+" FROM demo_order o WHERE "+where+" ORDER BY o.id", args...)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					return err
				}
				id := vals[0].(string)
				hits := make([]bool, len(vals)-1)
				for i, v := range vals[1:] {
					hits[i], _ = v.(bool)
				}
				ids = append(ids, id)
				fields[id] = p.Fields(hits)
			}
			return rows.Err()
		}), c.ID)
		return ids, fields
	}

	for _, c := range cases {
		switch c.Kind {
		case "evaluate":
			var want struct {
				List struct {
					IDs    []string `json:"ids"`
					Fields map[string][]string
				}
			}
			require.NoError(t, json.Unmarshal(c.Expect, &want))
			ids, fields := list(c, r.ViewOperation, nil)
			assert.ElementsMatch(t, want.List.IDs, ids, c.ID)
			for _, id := range want.List.IDs {
				assert.Equal(t, want.List.Fields[id], fields[id], "%s %s: list and single object give the same fields", c.ID, id)
			}
		case "list":
			var want struct {
				IDs []string `json:"ids"`
			}
			require.NoError(t, json.Unmarshal(c.Expect, &want))
			ids, _ := list(c, r.ViewOperation, c.Filter)
			assert.ElementsMatch(t, want.IDs, ids, c.ID)
		}
	}
}
