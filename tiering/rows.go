package tiering

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
)

// The canonical row and the unit's digest (be-protocol P15.8-P15.10): a row is an object of its columns, each value by
// the column's type; a null column is not written; a table's hash is over its rows in the order of their canonical
// primary keys' bytes, each prefixed by its length; the unit's digest is the domain-separated digest of its tables'
// names, row counts and hashes. Only the content counts: not the route, the file's parts or the column order.

// Column types (P15.8).
const (
	TypeText        = "text"
	TypeInt2        = "int2"
	TypeInt4        = "int4"
	TypeInt8        = "int8"
	TypeNumeric     = "numeric"
	TypeBool        = "bool"
	TypeUUID        = "uuid"
	TypeTimestamptz = "timestamptz"
	TypeDate        = "date"
	TypeJSONB       = "jsonb"
	TypeBytea       = "bytea"
)

// Types are every column type a class's tables may have.
var Types = []string{TypeText, TypeInt2, TypeInt4, TypeInt8, TypeNumeric, TypeBool, TypeUUID, TypeTimestamptz,
	TypeDate, TypeJSONB, TypeBytea}

// DigestVersion is the version of the canonical row and the digest (P15.10).
const DigestVersion = 1

// ErrRefused is a value its column's type does not allow.
var ErrRefused = errors.New("tiering: value not allowed for its column type")

// Value is one column's value in the canonical row: a string or a bool. v is what the database driver or a file
// reader gives: string, int64 (or other integers), bool, time.Time, []byte, [16]byte, *big.Int.
func Value(typ string, v any) (any, error) {
	switch typ {
	case TypeText:
		s, ok := v.(string)
		if !ok {
			return nil, refused(typ, v)
		}
		return canon.Text(s), nil
	case TypeInt2, TypeInt4, TypeInt8:
		s, err := integer(v)
		if err != nil {
			return nil, refused(typ, v)
		}
		return s, nil
	case TypeNumeric:
		s, ok := v.(string)
		if !ok {
			if i, err := integer(v); err == nil {
				s = i
			} else {
				return nil, refused(typ, v)
			}
		}
		d, err := canon.Decimal(s)
		if err != nil {
			return nil, refused(typ, v)
		}
		return d, nil
	case TypeBool:
		b, ok := v.(bool)
		if !ok {
			return nil, refused(typ, v)
		}
		return b, nil
	case TypeUUID:
		switch u := v.(type) {
		case [16]byte:
			return uuidText(u), nil
		case string:
			s := strings.ToLower(u)
			if len(s) != 36 || strings.Count(s, "-") != 4 {
				return nil, refused(typ, v)
			}
			return s, nil
		}
		return nil, refused(typ, v)
	case TypeTimestamptz:
		switch t := v.(type) {
		case time.Time:
			return canon.Time(t), nil
		case string:
			s, err := canon.Instant(t)
			if err != nil {
				return nil, refused(typ, v)
			}
			return s, nil
		}
		return nil, refused(typ, v)
	case TypeDate:
		switch d := v.(type) {
		case time.Time:
			return d.Format(time.DateOnly), nil
		case string:
			t, err := time.Parse(time.DateOnly, d)
			if err != nil {
				return nil, refused(typ, v)
			}
			return t.Format(time.DateOnly), nil
		}
		return nil, refused(typ, v)
	case TypeJSONB:
		s, ok := v.(string)
		if !ok {
			return nil, refused(typ, v)
		}
		return s, nil // the source's text as it came (P15.8)
	case TypeBytea:
		b, ok := v.([]byte)
		if !ok {
			return nil, refused(typ, v)
		}
		return base64.StdEncoding.EncodeToString(b), nil
	}
	return nil, fmt.Errorf("tiering: unknown column type %q", typ)
}

func refused(typ string, v any) error { return fmt.Errorf("%w: %s %v", ErrRefused, typ, v) }

func integer(v any) (string, error) {
	switch n := v.(type) {
	case int:
		return fmt.Sprint(n), nil
	case int16:
		return fmt.Sprint(n), nil
	case int32:
		return fmt.Sprint(n), nil
	case int64:
		return fmt.Sprint(n), nil
	case *big.Int:
		return n.String(), nil
	case string:
		d, err := canon.Decimal(n)
		if err != nil || strings.Contains(d, ".") {
			return "", ErrRefused
		}
		return d, nil
	}
	return "", ErrRefused
}

func uuidText(u [16]byte) string {
	h := hex.EncodeToString(u[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// Row is the canonical row's bytes of the columns' values (nil: null, not written).
func Row(types map[string]string, values map[string]any) ([]byte, error) {
	obj := make(map[string]any, len(values))
	for name, v := range values {
		if v == nil {
			continue
		}
		typ, ok := types[name]
		if !ok {
			return nil, fmt.Errorf("tiering: column %q has no type", name)
		}
		c, err := Value(typ, v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		obj[name] = c
	}
	return canon.Encode(obj)
}

// PK is the canonical primary key: the array of the key columns' values in key order.
func PK(types map[string]string, pk []string, values map[string]any) ([]byte, error) {
	arr := make([]any, len(pk))
	for i, name := range pk {
		v := values[name]
		if v == nil {
			return nil, fmt.Errorf("tiering: primary key column %q is null", name)
		}
		c, err := Value(types[name], v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		arr[i] = c
	}
	return canon.Encode(arr)
}

// TableHash accumulates one table's rows in any order; Sum sorts them by their canonical primary key's bytes.
type TableHash struct {
	rows []keyed
}

type keyed struct{ pk, row []byte }

// Add adds one row (its canonical primary key and row bytes).
func (h *TableHash) Add(pk, row []byte) { h.rows = append(h.rows, keyed{pk, row}) }

// Len is the number of rows added.
func (h *TableHash) Len() int { return len(h.rows) }

// Sum is the table's hash (lower-case hex SHA-256).
func (h *TableHash) Sum() string {
	sort.Slice(h.rows, func(i, j int) bool { return bytes.Compare(h.rows[i].pk, h.rows[j].pk) < 0 })
	s := sha256.New()
	var n [8]byte
	for _, r := range h.rows {
		binary.BigEndian.PutUint64(n[:], uint64(len(r.row)))
		s.Write(n[:])
		s.Write(r.row)
	}
	return hex.EncodeToString(s.Sum(nil))
}

// TableSum is one table's entry in the unit's digest.
type TableSum struct {
	Name   string
	Rows   int
	SHA256 string
}

// UnitDigest is the unit's digest (P15.9) over its tables' sums, in any order.
func UnitDigest(class, schemaVersion string, tables []TableSum) (string, error) {
	sorted := append([]TableSum(nil), tables...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	entries := make([]any, len(sorted))
	for i, t := range sorted {
		entries[i] = map[string]any{"name": t.Name, "rows": fmt.Sprint(t.Rows), "sha256": t.SHA256}
	}
	return canon.Digest("be.tier.unit", 1, map[string]any{"class": class, "schema_version": schemaVersion,
		"tables": entries})
}
