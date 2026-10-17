//go:build integration

package shell

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMembersShareOnePoolAndNeverEachOthersSchema(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	admin := pgtest.Admin(t, pgtest.Database)
	pgtest.Role(t, admin, "sdk_shell_t", "shell-t-pw")
	defs := []component.Definition{
		dbDef("demo/sha", "CREATE TABLE items (id int PRIMARY KEY, name text); INSERT INTO items VALUES (1, 'a'), (2, 'b');"),
		dbDef("demo/shb", "CREATE TABLE items (id int PRIMARY KEY); INSERT INTO items VALUES (1), (2), (3);"),
	}
	var members []Served
	for i, name := range []string{"sha", "shb"} {
		pgtest.Member(t, name, "sdk_shell_t")
		cfg := pgtest.MemberEnv(host, port, name)
		mc, problems := db.MigrateConfigFrom(defs[i].ID, configValues(t, cfg), obs.NewLogger(obs.Member{}, 0, io.Discard))
		require.Empty(t, problems)
		require.NoError(t, db.Migrate(context.Background(), mc, db.SDKMigrations(),
			db.MigrationSet{Name: defs[i].ID, Table: "schema_migrations", Files: defs[i].Migrations}))
		served := maps.Clone(cfg)
		delete(served, "PG_OWNER_PASSWORD") // the shell's members never see the owner's password
		served["PG_POOL_FOREGROUND"], served["PG_POOL_BACKGROUND"] = "1", "1"
		members = append(members, Served{ComponentID: defs[i].ID, Version: "0.1.0", HTTPPort: freePort(t), Config: served})
	}

	ps := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	var stdout syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, self, defs, Options{Env: shellEnv(ps, served(t, members...), "PG_SHELL_USER", "sdk_shell_t",
			"PG_SHELL_PASSWORD", "shell-t-pw"), Stdout: &stdout, Stderr: io.Discard,
			BuildInfo: buildInfo(map[string]string{"demo/sha": "v0.1.0", "demo/shb": "v0.1.0"}), ModuleOf: moduleByID})
	}()
	require.Eventually(t, func() bool { c, _ := get("http://127.0.0.1:" + strconv.Itoa(ps) + "/readyz"); return c == 200 },
		10*time.Second, 20*time.Millisecond, stdout.String())

	want := map[int]map[string]any{
		members[0].HTTPPort: {"user": "sdk_sha_rw", "path": `"sha"`, "rows": 2.0, "cols": 2.0},
		members[1].HTTPPort: {"user": "sdk_shb_rw", "path": `"shb"`, "rows": 3.0, "cols": 1.0},
	}
	for i := 0; i < 20; i++ {
		for p, w := range want {
			code, body := get("http://127.0.0.1:" + strconv.Itoa(p) + "/api/shape")
			require.Equal(t, 200, code, body)
			var got map[string]any
			require.NoError(t, json.Unmarshal([]byte(body), &got))
			assert.Equal(t, w, got)
		}
	}
	var shellConns, memberConns int
	require.NoError(t, admin.QueryRow(context.Background(), `SELECT count(*) FILTER (WHERE usename = 'sdk_shell_t'),
		count(*) FILTER (WHERE usename IN ('sdk_sha_rw', 'sdk_shb_rw'))
		FROM pg_stat_activity WHERE datname = $1`, pgtest.Database).Scan(&shellConns, &memberConns))
	assert.Positive(t, shellConns, "the members' transactions ran on the shell's pool")
	assert.LessOrEqual(t, shellConns, 4, "one pool sized to the members' budgets on this target")
	assert.Zero(t, memberConns, "no member opened a pool of its own")

	cancel()
	assert.Equal(t, 0, <-done)
}

func configValues(t *testing.T, m map[string]string) *config.Values {
	v, problems := config.Parse(db.MigrateKeys(), config.Map(m))
	require.Empty(t, problems)
	return v
}
