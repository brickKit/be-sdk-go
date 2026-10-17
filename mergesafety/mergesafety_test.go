package mergesafety

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const bad = `package demo

import (
	"log"
	"os"
	osx "os"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/jackc/pgx/v5/pgxpool"
)

func New() {
	_ = os.Getenv("PG_SCHEMA")
	_, _ = osx.LookupEnv("X")
	gin.SetMode(gin.DebugMode)
	prometheus.MustRegister(nil)
	_ = promauto.NewCounter
	_, _ = pgxpool.New(nil, "")
	log.Fatal("bye")
}
`

func TestFindsEveryKind(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.go"), []byte(bad), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad_test.go"), []byte(bad), 0o600))
	findings, err := CheckDir(dir)
	require.NoError(t, err)
	var calls []string
	for _, f := range findings {
		calls = append(calls, f.Call)
	}
	joined := strings.Join(calls, " ")
	for _, want := range []string{"os.Getenv", "os.LookupEnv", "gin.SetMode", "prometheus.MustRegister",
		"promauto.NewCounter", "pgxpool.New", "log.Fatal"} {
		assert.Contains(t, joined, want)
	}
	assert.Len(t, findings, 7, "test files are not checked")
}

// TestSDKIsMergeSafe holds the SDK to the rule it enforces: only the launcher (and config.ProcessEnv, which only the
// launcher calls) may touch the process; the shell and internal/host are the launcher's other half.
func TestSDKIsMergeSafe(t *testing.T) {
	findings, err := CheckTree("..", "launcher", "shell", "internal/host", "mergesafety")
	require.NoError(t, err)
	var left []string
	for _, f := range findings {
		if strings.HasSuffix(f.Pos.Filename, filepath.Join("config", "values.go")) && f.Call == "os.LookupEnv" {
			continue // ProcessEnv: the launcher's way in
		}
		if strings.HasSuffix(f.Pos.Filename, filepath.Join("httpx", "engine.go")) && f.Call == "github.com/gin-gonic/gin.New" {
			continue // the engine the SDK builds for each member
		}
		left = append(left, f.String())
	}
	assert.Empty(t, left)
}
