package tiering

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type vectorCase struct {
	ID       string
	Input    json.RawMessage
	Expected json.RawMessage
}

func vectors(t *testing.T, topic string) []vectorCase {
	t.Helper()
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/"+topic+".json")
	require.NoError(t, err)
	var doc struct{ Cases []vectorCase }
	require.NoError(t, json.Unmarshal(raw, &doc))
	require.NotEmpty(t, doc.Cases)
	return doc.Cases
}

func decode(t *testing.T, raw json.RawMessage, v any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	require.NoError(t, d.Decode(v))
}

// vectorValue is a vector's JSON value as a driver gives it for the column's type: bytea arrives as hex in the vectors.
func vectorValue(t *testing.T, typ string, v any) any {
	t.Helper()
	switch x := v.(type) {
	case nil:
		return nil
	case json.Number:
		return x.String()
	case string:
		if typ == TypeBytea {
			b, err := hex.DecodeString(x)
			require.NoError(t, err)
			return b
		}
	}
	return v
}

func values(t *testing.T, types map[string]string, row map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range row {
		out[k] = vectorValue(t, types[k], v)
	}
	return out
}

// P15.8-P15.10 on the protocol's tier-digest vectors.
func TestTheDigestVectors(t *testing.T) {
	for _, c := range vectors(t, "tier-digest") {
		t.Run(c.ID, func(t *testing.T) {
			var in struct {
				Columns       map[string]string
				Row           map[string]any
				PK            []string
				Rows          []map[string]any
				Class         string
				SchemaVersion string `json:"schema_version"`
				Tables        []struct {
					Name    string
					Columns map[string]string
					PK      []string
					Rows    []map[string]any
				}
			}
			decode(t, c.Input, &in)
			var want map[string]any
			decode(t, c.Expected, &want)
			switch {
			case want["refused"] == true:
				_, err := Row(in.Columns, values(t, in.Columns, in.Row))
				assert.ErrorIs(t, err, ErrRefused)
			case want["canonical_row"] != nil:
				got, err := Row(in.Columns, values(t, in.Columns, in.Row))
				require.NoError(t, err)
				assert.Equal(t, want["canonical_row"], string(got))
			case want["canonical_pk"] != nil:
				got, err := PK(in.Columns, in.PK, values(t, in.Columns, in.Row))
				require.NoError(t, err)
				assert.Equal(t, want["canonical_pk"], string(got))
			case want["sha256"] != nil:
				assert.Equal(t, want["sha256"], tableHash(t, in.Columns, in.PK, in.Rows))
			case want["digest"] != nil:
				var sums []TableSum
				for _, tb := range in.Tables {
					sums = append(sums, TableSum{Name: tb.Name, Rows: len(tb.Rows), SHA256: tableHash(t, tb.Columns, tb.PK, tb.Rows)})
				}
				got, err := UnitDigest(in.Class, in.SchemaVersion, sums)
				require.NoError(t, err)
				assert.Equal(t, want["digest"], got)
			default:
				t.Fatalf("no expectation the test knows: %s", c.Expected)
			}
		})
	}
}

func tableHash(t *testing.T, types map[string]string, pk []string, rows []map[string]any) string {
	t.Helper()
	var h TableHash
	for _, r := range rows {
		vals := values(t, types, r)
		k, err := PK(types, pk, vals)
		require.NoError(t, err)
		b, err := Row(types, vals)
		require.NoError(t, err)
		h.Add(k, b)
	}
	return h.Sum()
}

// P15.15, P15.16 on the protocol's tier-manifest vectors.
func TestTheManifestVectors(t *testing.T) {
	for _, c := range vectors(t, "tier-manifest") {
		t.Run(c.ID, func(t *testing.T) {
			var want struct{ Key, Canonical string }
			decode(t, c.Expected, &want)
			if want.Key != "" {
				var in struct {
					Prefix, Class, Unit, Table string
					Generation                 int64
					Version, Part              int
				}
				decode(t, c.Input, &in)
				assert.Equal(t, want.Key, ObjectKey(in.Prefix, in.Class, in.Unit, in.Generation, in.Version, in.Table, in.Part))
				return
			}
			var in struct{ Manifest Manifest }
			require.NoError(t, json.Unmarshal(c.Input, &in))
			got, err := in.Manifest.Canonical()
			require.NoError(t, err)
			assert.Equal(t, want.Canonical, string(got))
		})
	}
}

// P15.37 on the protocol's tier-plan vectors.
func TestThePlanVectors(t *testing.T) {
	at := func(s string) time.Time {
		if s == "" {
			return time.Time{}
		}
		v, err := time.Parse(time.RFC3339, s)
		require.NoError(t, err)
		return v
	}
	for _, c := range vectors(t, "tier-plan") {
		t.Run(c.ID, func(t *testing.T) {
			var in struct {
				Now       string
				NDays     int `json:"n_days"`
				GraceDays int `json:"grace_days"`
				Class     struct {
					Category      string
					ResidenceDays json.RawMessage `json:"residence_days"`
					Retention     json.RawMessage
					Erasable      bool
				}
				Unit struct {
					State              string
					AnchorMax          string `json:"anchor_max"`
					Blockers           []string
					RouteImmutable     bool   `json:"route_immutable"`
					PublishedAt        string `json:"published_at"`
					NewestCreated      string `json:"newest_created"`
					OldLeases          int    `json:"old_leases"`
					BackupAfterPublish bool   `json:"backup_after_publish"`
					RetentionDue       bool   `json:"retention_due"`
					Approved           bool
					Holds              int
				}
			}
			require.NoError(t, json.Unmarshal(c.Input, &in))
			var want struct {
				Action  string
				Reasons []string
				Alert   bool
			}
			require.NoError(t, json.Unmarshal(c.Expected, &want))
			residence := Never
			if string(in.Class.ResidenceDays) != `"never"` {
				require.NoError(t, json.Unmarshal(in.Class.ResidenceDays, &residence))
			}
			day := 24 * time.Hour
			got := Plan(at(in.Now), time.Duration(in.NDays)*day, time.Duration(in.GraceDays)*day,
				PlanClass{Category: in.Class.Category, ResidenceDays: residence,
					RetentionPending: string(in.Class.Retention) == `"pending"`, Erasable: in.Class.Erasable},
				PlanUnit{State: in.Unit.State, AnchorMax: at(in.Unit.AnchorMax), Blockers: in.Unit.Blockers,
					RouteImmutable: in.Unit.RouteImmutable, PublishedAt: at(in.Unit.PublishedAt),
					NewestCreated: at(in.Unit.NewestCreated), OldLeases: in.Unit.OldLeases,
					BackupAfterPublish: in.Unit.BackupAfterPublish, RetentionDue: in.Unit.RetentionDue,
					Approved: in.Unit.Approved, Holds: in.Unit.Holds})
			assert.Equal(t, want.Action, got.Action)
			assert.Equal(t, want.Reasons, got.Reasons)
			assert.Equal(t, want.Alert, got.Alert)
		})
	}
}
