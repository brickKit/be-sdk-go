package acl

import (
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"

	"github.com/stretchr/testify/require"
)

func TestDecodeChangesPage(t *testing.T) {
	page, err := decodeChanges([]byte(`{"changes":[
		{"revision":"7","op":"upsert","tuple":{"object":{"type":"a.b.c","id":"1"},"relation":"viewer","subject":"user:u","expires_at":"2026-10-09T08:53:20Z","future":1}},
		{"revision":"9","op":"delete","tuple":{"object":{"type":"a.b.c","id":"1"},"relation":"viewer","subject":"role:r","expires_at":null}}],
		"next":"9","watermark":"12","unknown":true}`))
	require.NoError(t, err)
	require.EqualValues(t, 9, page.next)
	require.EqualValues(t, 12, page.watermark)
	require.Len(t, page.changes, 2)
	require.EqualValues(t, 7, page.changes[0].rev)
	require.False(t, page.changes[0].del)
	require.Equal(t, "a.b.c", page.changes[0].row.RType)
	require.True(t, time.Date(2026, 10, 9, 8, 53, 20, 0, time.UTC).Equal(*page.changes[0].row.ExpiresAt))
	require.True(t, page.changes[1].del)
	require.Nil(t, page.changes[1].row.ExpiresAt)
}

func TestDecodeRejectsMalformedPages(t *testing.T) {
	for name, raw := range map[string]string{
		"not json":            `{`,
		"bad revision":        `{"changes":[],"next":"x","watermark":"1"}`,
		"negative revision":   `{"changes":[],"next":"-1","watermark":"1"}`,
		"leading zero":        `{"changes":[],"next":"01","watermark":"1"}`,
		"missing watermark":   `{"changes":[],"next":"1"}`,
		"unknown op":          `{"changes":[{"revision":"1","op":"merge","tuple":{"object":{"type":"a.b.c","id":"1"},"relation":"r","subject":"user:u"}}],"next":"1","watermark":"1"}`,
		"decreasing revision": `{"changes":[{"revision":"2","op":"upsert","tuple":{"object":{"type":"a.b.c","id":"1"},"relation":"r","subject":"user:u"}},{"revision":"1","op":"upsert","tuple":{"object":{"type":"a.b.c","id":"2"},"relation":"r","subject":"user:u"}}],"next":"2","watermark":"2"}`,
		"empty id":            `{"changes":[{"revision":"1","op":"upsert","tuple":{"object":{"type":"a.b.c","id":""},"relation":"r","subject":"user:u"}}],"next":"1","watermark":"1"}`,
		"bad expiry":          `{"changes":[{"revision":"1","op":"upsert","tuple":{"object":{"type":"a.b.c","id":"1"},"relation":"r","subject":"user:u","expires_at":"tomorrow"}}],"next":"1","watermark":"1"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeChanges([]byte(raw))
			require.Error(t, err)
		})
	}
}

func TestDecodeTuplesPage(t *testing.T) {
	page, err := decodeTuples([]byte(`{"tuples":[{"object":{"type":"a.b.c","id":"1"},"relation":"viewer","subject":"dept:/1/"}],"next_cursor":"","revision":"40"}`))
	require.NoError(t, err)
	require.EqualValues(t, 40, page.revision)
	require.Empty(t, page.cursor)
	require.Len(t, page.rows, 1)
	_, err = decodeTuples([]byte(`{"tuples":[],"revision":"40"}`))
	require.Error(t, err, "next_cursor is required")
}

func TestNewChecksConfig(t *testing.T) {
	_, err := New(Config{URL: "http://authz", Types: []string{"a.b.c"}})
	require.Error(t, err, "a store is required")
	_, err = New(Config{Types: []string{"a.b.c"}, Store: pg.NewStore(nil, pg.StoreConfig{})})
	require.Error(t, err, "AUTHZ_URL is required")
	p, err := New(Config{URL: "http://authz/", Types: []string{"b.b.b", "a.b.c", "a.b.c"}, Store: pg.NewStore(nil, pg.StoreConfig{})})
	require.NoError(t, err)
	require.Equal(t, "a.b.c,b.b.b", p.scope)
	require.Equal(t, 5*time.Second, p.cfg.Interval)
	require.Equal(t, 3*time.Second, p.cfg.Timeout)
	require.Equal(t, 300*time.Millisecond, p.cfg.WaitBudget)
	require.Equal(t, "http://authz/authz/v2/changes?after=7&limit=500&types=a.b.c%2Cb.b.b", p.changesURL(7))
	require.Equal(t, "http://authz/authz/v2/tuples?cursor=c1&page_size=1000&type=a.b.c", p.tuplesURL("a.b.c", "c1"))
}
