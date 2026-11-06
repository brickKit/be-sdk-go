package tiering

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/runprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lister []runprofile.Listed

func (l lister) ListAll(context.Context, string) ([]runprofile.Listed, error) { return l, nil }

// P15.21: the latest backup is the greatest finished_at among the backup records whose signature holds; a forged or
// unreadable one does not count; none at all is "cannot tell".
func TestTheLatestBackupComesFromTrustedRecords(t *testing.T) {
	body := func(at time.Time) json.RawMessage {
		b, _ := json.Marshal(map[string]any{"finished_at": at})
		return b
	}
	t0 := time.Date(2026, 11, 6, 2, 0, 0, 0, time.UTC)
	l := lister{
		{Read: true, Record: runprofile.Record{Body: body(t0)}},
		{Read: true, Record: runprofile.Record{Body: body(t0.Add(48 * time.Hour))}, Err: errors.New("the signature does not hold")},
		{Read: true, Record: runprofile.Record{Body: body(t0.Add(24 * time.Hour))}},
		{Read: false, Err: errors.New("not a record")},
		{Read: true, Record: runprofile.Record{Body: json.RawMessage(`{"other":1}`)}},
	}
	at, ok, err := BackupsFrom(l)(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, t0.Add(24*time.Hour), at.UTC())
	_, ok, err = BackupsFrom(lister{})(context.Background())
	require.NoError(t, err)
	assert.False(t, ok)
}
