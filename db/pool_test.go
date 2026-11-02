package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Behind a pooler that does not keep prepared statements (PG_POOLER_PREPARED=false) the pool describes each
// statement and binds it unnamed instead of preparing it; otherwise it caches prepared statements.
func TestPoolerPreparedChoosesTheExecMode(t *testing.T) { // P5.3 (the exec mode)
	for prepared, want := range map[bool]pgx.QueryExecMode{true: pgx.QueryExecModeCacheStatement, false: pgx.QueryExecModeCacheDescribe} {
		p, err := Open(context.Background(), PoolConfig{Target: Target{Host: "127.0.0.1", Port: 1, Database: "x"}, User: "u",
			AppName: "demo/x", MaxConns: 1, Prepared: prepared})
		require.NoError(t, err, "the pool connects lazily")
		assert.Equal(t, want, p.pool.Config().ConnConfig.DefaultQueryExecMode, "prepared=%v", prepared)
		p.Close()
	}
}
