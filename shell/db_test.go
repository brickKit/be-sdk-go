package shell

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/stretchr/testify/assert"
)

// shape answers with the role, schema and column count its transactions see: the same SQL in every member.
type shape struct{ db *db.DB }

func (s *shape) Routes(r *httpx.Routes) error {
	r.GET("/api/shape", "demo.shape.read", func(c *httpx.Context) error {
		var user, path string
		var rows, cols int
		err := s.db.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			if err := tx.QueryRow(ctx, "SELECT current_user, current_setting('search_path')").Scan(&user, &path); err != nil {
				return err
			}
			rs, err := tx.Query(ctx, "SELECT * FROM items ORDER BY id")
			if err != nil {
				return err
			}
			defer rs.Close()
			cols = len(rs.FieldDescriptions())
			for rs.Next() {
				rows++
			}
			return rs.Err()
		})
		if err != nil {
			return err
		}
		return c.JSON(200, map[string]any{"user": user, "path": path, "rows": rows, "cols": cols})
	})
	return nil
}

func dbDef(id, migration string) component.Definition {
	return component.Definition{ID: id, Version: "0.1.0",
		Migrations: fstest.MapFS{"0001_items.up.sql": {Data: []byte(migration)}},
		New:        func(rt *runtime.Runtime) (component.Module, error) { return &shape{db: rt.DB}, nil }}
}

func dbConfig(schema string, extra ...string) map[string]string {
	m := map[string]string{"PG_HOST": "db.invalid", "PG_DATABASE": "be", "PG_USER": "sdk_" + schema + "_rw",
		"PG_PASSWORD": "pw", "PG_SCHEMA": schema}
	for i := 0; i+1 < len(extra); i += 2 {
		m[extra[i]] = extra[i+1]
	}
	return m
}

func TestRefusesDatabaseMembersWithoutTheShellLoginRole(t *testing.T) {
	js := served(t, Served{ComponentID: "demo/sha", Version: "0.1.0", HTTPPort: 18081, Config: dbConfig("sha")})
	out := refuse(t, shellEnv(18080, js), buildInfo(map[string]string{"demo/sha": "v0.1.0"}), dbDef("demo/sha", "SELECT 1;"))
	assert.Contains(t, out, `be/go-core:PG_SHELL_USER`)
	assert.Contains(t, out, "a hosted member owns a database")
}

func TestRefusesMembersOfOneTargetThatDisagreeOnPreparedStatements(t *testing.T) {
	js := served(t, Served{ComponentID: "demo/sha", Version: "0.1.0", HTTPPort: 18081, Config: dbConfig("sha")},
		Served{ComponentID: "demo/shb", Version: "0.1.0", HTTPPort: 18082, Config: dbConfig("shb", "PG_POOLER_PREPARED", "false")},
		// another target may differ
		Served{ComponentID: "demo/shc", Version: "0.1.0", HTTPPort: 18083, Config: dbConfig("shc", "PG_DATABASE", "other", "PG_POOLER_PREPARED", "false")})
	out := refuse(t, shellEnv(18080, js, "PG_SHELL_USER", "shell", "PG_SHELL_PASSWORD", "pw"),
		buildInfo(map[string]string{"demo/sha": "v0.1.0", "demo/shb": "v0.1.0", "demo/shc": "v0.1.0"}),
		dbDef("demo/sha", "SELECT 1;"), dbDef("demo/shb", "SELECT 1;"), dbDef("demo/shc", "SELECT 1;"))
	assert.Contains(t, out, `demo/shb:PG_POOLER_PREPARED`)
	assert.Contains(t, out, "the same value as demo/sha")
	assert.NotContains(t, out, `demo/shc:PG_POOLER_PREPARED`)
}
