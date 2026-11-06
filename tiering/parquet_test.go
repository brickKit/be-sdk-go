package tiering

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// G3, the round trip of every column type: written to Parquet and read back, every row's canonical form and the
// table's hash are the source's (nulls stay nulls, an empty string stays an empty string).
func TestEveryColumnTypeRoundTrips(t *testing.T) {
	all := Table{Name: "everything", PK: "id", Columns: map[string]string{"id": TypeUUID, "t": TypeText,
		"i2": TypeInt2, "i4": TypeInt4, "i8": TypeInt8, "n": TypeNumeric, "b": TypeBool, "ts": TypeTimestamptz,
		"d": TypeDate, "j": TypeJSONB, "bin": TypeBytea, "tags": TypeTextArray}}
	at := time.Date(2026, 3, 1, 7, 0, 0, 500000000, time.UTC)
	rows := []map[string]any{
		{"id": [16]byte{1, 2, 3}, "t": "café ünïcode 中文", "i2": int16(-32768), "i4": int32(2147483647),
			"i8": int64(9007199254740993), "n": "-1250.5000", "b": true, "ts": at,
			"d": time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), "j": `{"a": [1, 2], "b": null}`, "bin": []byte{0, 255, 16},
			"tags": []any{"B-2", "café", "A-1"}},
		{"id": [16]byte{9}, "t": "", "i2": nil, "i4": nil, "i8": nil, "n": nil, "b": false, "ts": nil, "d": nil,
			"j": nil, "bin": []byte{}},
		{"id": [16]byte{5}, "t": nil, "i2": int16(0), "i4": int32(-1), "i8": int64(-9007199254740993), "n": "0.000",
			"b": nil, "ts": time.Date(1999, 12, 31, 23, 59, 59, 999999000, time.UTC),
			"d": time.Date(2099, 12, 31, 0, 0, 0, 0, time.UTC), "j": `[]`, "bin": nil},
	}
	data, err := WriteParquet(all, rows)
	require.NoError(t, err)
	back, err := ReadParquet(all, data)
	require.NoError(t, err)
	require.Len(t, back, len(rows))

	hash := func(rs []map[string]any) string {
		var h TableHash
		for _, r := range rs {
			k, err := PK(all.Columns, []string{"id"}, r)
			require.NoError(t, err)
			b, err := Row(all.Columns, r)
			require.NoError(t, err)
			h.Add(k, b)
		}
		return h.Sum()
	}
	for i := range rows {
		want, err := Row(all.Columns, rows[i])
		require.NoError(t, err)
		got, err := Row(all.Columns, back[i])
		require.NoError(t, err)
		assert.Equal(t, string(want), string(got), "row %d", i)
	}
	assert.Equal(t, hash(rows), hash(back))

	other := Table{Name: "everything", PK: "id", Columns: map[string]string{"id": TypeUUID, "t": TypeText}}
	_, err = ReadParquet(other, data)
	assert.Error(t, err, "a file whose columns are not the table's")
}

// A unit read back from a route is canonical rows; FromCanonical turns them into values a copy writes again, to the
// same canonical bytes (every column type, nulls absent) and through Parquet (P15.8, P15.31, P15.35).
func TestACanonicalRowIsWrittenAgainAsItWas(t *testing.T) {
	all := Table{Name: "everything", PK: "id", Columns: map[string]string{"id": TypeUUID, "t": TypeText,
		"i2": TypeInt2, "i4": TypeInt4, "i8": TypeInt8, "n": TypeNumeric, "b": TypeBool, "ts": TypeTimestamptz,
		"d": TypeDate, "j": TypeJSONB, "bin": TypeBytea, "tags": TypeTextArray}}
	rows := []map[string]any{
		{"id": [16]byte{1, 2, 3}, "t": "café 中文", "i2": int16(-32768), "i4": int32(2147483647),
			"i8": int64(9007199254740993), "n": "-1250.5000", "b": true,
			"ts": time.Date(2026, 3, 1, 7, 0, 0, 500000000, time.UTC), "d": time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			"j": `{"a": [1, 2], "b": null}`, "bin": []byte{0, 255, 16}, "tags": []string{"x", "y"}},
		{"id": [16]byte{9}, "t": "", "b": false, "bin": []byte{}, "tags": []string{}},
	}
	for _, r := range rows {
		first, err := Row(all.Columns, r)
		require.NoError(t, err)
		var canonical map[string]any
		require.NoError(t, json.Unmarshal(first, &canonical))
		back, err := FromCanonical(all, canonical)
		require.NoError(t, err)
		again, err := Row(all.Columns, back)
		require.NoError(t, err)
		assert.Equal(t, string(first), string(again))
		_, err = WriteParquet(all, []map[string]any{back})
		require.NoError(t, err, "written to an object again")
	}
	_, err := FromCanonical(all, map[string]any{"i2": "40000"})
	assert.ErrorIs(t, err, ErrRefused, "out of the column's range")
	_, err = FromCanonical(all, map[string]any{"nope": "x"})
	assert.Error(t, err)
}

// P15.8: a text array's elements are NFC each, in their order, whatever form they came in.
func TestATextArrayIsNFC(t *testing.T) {
	cols := map[string]string{"tags": TypeTextArray}
	b, err := Row(cols, map[string]any{"tags": []string{"café", "B"}})
	require.NoError(t, err)
	assert.Equal(t, `{"tags":["caf`+"é"+`","B"]}`, string(b))
}
