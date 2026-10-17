package db

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"strconv"
	"testing"
	"testing/fstest"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vectorCases(t *testing.T, topic string) []json.RawMessage {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/"+topic+".json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	return v.Cases
}

func TestMigrationLintVectors(t *testing.T) {
	type refusal struct{ File, Rule string }
	for _, raw := range vectorCases(t, "migration-lint") {
		var c struct {
			ID      string
			Schema  string
			Files   map[string]string
			Refused []refusal
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		fsys := fstest.MapFS{}
		for name, body := range c.Files {
			fsys[name] = &fstest.MapFile{Data: []byte(body)}
		}
		got := []refusal{}
		for _, p := range LintMigrations(fsys, c.Schema) {
			got = append(got, refusal{p.File, p.Rule})
		}
		if c.Refused == nil {
			c.Refused = []refusal{}
		}
		assert.Equal(t, c.Refused, got, c.ID)
	}
}

// connectError is a real failure before any SQLSTATE: nothing listens on the port.
func connectError(t *testing.T) error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	_, err = pgconn.Connect(context.Background(), "postgres://u@127.0.0.1:"+strconv.Itoa(port)+"/d?connect_timeout=2")
	require.Error(t, err)
	return err
}

func TestDBErrorsVectors(t *testing.T) {
	for _, raw := range vectorCases(t, "db-errors") {
		var c struct {
			ID, Sqlstate, Reason string
			Retried              bool
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		var in error = &pgconn.PgError{Code: c.Sqlstate, Message: c.ID}
		if c.Sqlstate == "connect" {
			in = connectError(t)
		}
		assert.Equal(t, c.Retried, retryable(in), "%s: retried", c.ID)
		out := mapError(context.Background(), in)
		assert.True(t, errs.Is(out, "be", c.Reason), "%s: got %v", c.ID, out)
		if c.Reason == errs.ReasonDependencyUnavailable {
			var e *errs.Error
			require.True(t, errors.As(out, &e))
			assert.Equal(t, "db", e.Metadata["dependency"], c.ID)
		}
	}
}
