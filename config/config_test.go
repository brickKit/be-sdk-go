package config

import (
	"encoding/json"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigParseVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/config-parse.json")
	require.NoError(t, err)
	var file struct {
		Cases []struct {
			ID, Type string
			Value    *string
			Required bool
			Default  any
			Values   []string
			Expect   struct {
				Value any
				Error string
			}
		}
	}
	require.NoError(t, json.Unmarshal(raw, &file))
	for _, c := range file.Cases {
		k := Key{Name: "K", Type: Type(c.Type), Required: c.Required, Values: c.Values}
		switch d := c.Default.(type) {
		case float64:
			k.Default = int64(d)
		case string:
			k.Default = d
		}
		src := Map{}
		if c.Value != nil {
			src["K"] = *c.Value
		}
		v, problems := Parse([]Key{k}, src)
		if c.Expect.Error != "" {
			require.Len(t, problems, 1, c.ID)
			assert.Equal(t, c.Expect.Error, problems[0].Kind, c.ID)
			continue
		}
		require.Empty(t, problems, c.ID)
		var got any
		switch k.Type {
		case Integer:
			got = float64(v.Int("K"))
		case Boolean:
			got = v.Bool("K")
		case Duration:
			got = float64(v.Duration("K") / time.Millisecond)
		default:
			got = v.String("K")
		}
		if c.Expect.Value == nil {
			assert.False(t, v.Has("K"), c.ID)
			continue
		}
		assert.Equal(t, c.Expect.Value, got, c.ID)
	}
}

func TestAllProblemsAtOnceAndNoSecretValues(t *testing.T) {
	keys := []Key{
		{Name: "A", Type: Integer, Required: true},
		{Name: "B", Type: Boolean, Required: true},
		{Name: "PASSWORD", Type: String, Required: true, Secret: true},
		{Name: "LEVEL", Type: Enum, Values: []string{"info", "warn"}, Default: "info"},
	}
	// a secret that fails to parse does not exist for strings, so make it a wrong-typed secret integer instead
	keys = append(keys, Key{Name: "PIN", Type: Integer, Required: true, Secret: true})
	_, problems := Parse(keys, Map{"A": "x1", "LEVEL": "loud", "PIN": "hunter2"})
	var lines []string
	for _, p := range problems {
		lines = append(lines, p.String())
	}
	joined := strings.Join(lines, "\n")
	assert.Len(t, problems, 5, joined)
	assert.Contains(t, joined, "A: invalid")
	assert.Contains(t, joined, "B: missing")
	assert.Contains(t, joined, "PASSWORD: missing")
	assert.Contains(t, joined, "LEVEL: unknown_enum")
	assert.Contains(t, joined, "PIN: invalid")
	assert.NotContains(t, joined, "hunter2", "a secret's value is never printed")
}

func TestDeclarationRules(t *testing.T) {
	_, problems := Parse([]Key{{Name: "DEMO_ALPHA_ENDPOINT", Type: String}, {Name: "PORT", Type: Integer},
		{Name: "lower", Type: String}, {Name: "E", Type: Enum}}, Map{})
	assert.Len(t, problems, 4)
}

type fakeEnv struct {
	env   map[string]string
	files map[string]string
}

func (f *fakeEnv) Lookup(n string) (string, bool) { v, ok := f.env[n]; return v, ok }
func (f *fakeEnv) ReadFile(p string) ([]byte, error) {
	v, ok := f.files[p]
	if !ok {
		return nil, errors.New("no such file")
	}
	return []byte(v), nil
}

func TestSecretFromFileAndRefresh(t *testing.T) {
	env := &fakeEnv{env: map[string]string{"PG_PASSWORD_FILE": "/run/secrets/pg"}, files: map[string]string{"/run/secrets/pg": "first\n"}}
	v, problems := Parse([]Key{{Name: "PG_PASSWORD", Type: String, Required: true, Secret: true}}, env)
	require.Empty(t, problems)
	s := v.Secret("PG_PASSWORD")
	assert.Equal(t, "first", s.Value())
	assert.Equal(t, "first", v.String("PG_PASSWORD"))
	assert.Len(t, v.Secrets(), 1)

	changed, err := s.Refresh(env.ReadFile)
	require.NoError(t, err)
	assert.False(t, changed)
	env.files["/run/secrets/pg"] = "second"
	changed, err = s.Refresh(env.ReadFile)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, "second", v.String("PG_PASSWORD"))

	delete(env.files, "/run/secrets/pg")
	_, err = s.Refresh(env.ReadFile)
	assert.Error(t, err)
	assert.Equal(t, "second", s.Value(), "a failed re-read keeps the old value")
}

func TestSecretValueAndFileConflict(t *testing.T) {
	env := &fakeEnv{env: map[string]string{"PG_PASSWORD": "a", "PG_PASSWORD_FILE": "/x"}, files: map[string]string{"/x": "b"}}
	_, problems := Parse([]Key{{Name: "PG_PASSWORD", Type: String, Required: true, Secret: true}}, env)
	require.Len(t, problems, 1)
	assert.Equal(t, "conflict", problems[0].Kind)
}

func TestRangeAndAccessorMisuse(t *testing.T) {
	lo, hi := int64(1), int64(10)
	_, problems := Parse([]Key{{Name: "N", Type: Integer, Required: true, Min: &lo, Max: &hi}}, Map{"N": "11"})
	require.Len(t, problems, 1)
	assert.Equal(t, "out_of_range", problems[0].Kind)

	v, _ := Parse([]Key{{Name: "N", Type: Integer, Default: 3}}, Map{})
	assert.Equal(t, int64(3), v.Int("N"))
	assert.Panics(t, func() { v.String("N") })
	assert.Panics(t, func() { v.Int("UNDECLARED") })
}
