package tiering

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// The cold route's files (A149: parquet-go): one table's rows in a Parquet file whose schema is built from the
// class's column types; every column optional, in name order (parquet.Group sorts them). numeric and jsonb keep the
// source's text; uuid, timestamptz (UTC microseconds) and date carry their logical types, so a third-party reader
// sees the same values. Reading back gives the values the canonical row needs, so the digest of what was read equals
// the digest of what was sealed.

func parquetNode(typ string) (parquet.Node, error) {
	var n parquet.Node
	switch typ {
	case TypeText, TypeNumeric:
		n = parquet.String()
	case TypeJSONB:
		n = parquet.JSON()
	case TypeInt2:
		n = parquet.Int(16)
	case TypeInt4:
		n = parquet.Int(32)
	case TypeInt8:
		n = parquet.Int(64)
	case TypeBool:
		n = parquet.Leaf(parquet.BooleanType)
	case TypeUUID:
		n = parquet.UUID()
	case TypeTimestamptz:
		n = parquet.Timestamp(parquet.Microsecond)
	case TypeDate:
		n = parquet.Date()
	case TypeBytea:
		n = parquet.Leaf(parquet.ByteArrayType)
	default:
		return nil, fmt.Errorf("tiering: no Parquet type for %q", typ)
	}
	return parquet.Optional(n), nil
}

func columnNames(t Table) []string {
	names := make([]string, 0, len(t.Columns))
	for n := range t.Columns {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func parquetSchema(t Table) (*parquet.Schema, error) {
	g := parquet.Group{}
	for name, typ := range t.Columns {
		n, err := parquetNode(typ)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", t.Name, name, err)
		}
		g[name] = n
	}
	return parquet.NewSchema(t.Name, g), nil
}

// parquetValue is one value as the column stores it (the driver's Go value; numeric and jsonb as text).
func parquetValue(typ string, v any) (parquet.Value, error) {
	switch typ {
	case TypeText, TypeNumeric, TypeJSONB:
		s, ok := v.(string)
		if !ok {
			return parquet.Value{}, refused(typ, v)
		}
		return parquet.ByteArrayValue([]byte(s)), nil
	case TypeInt2, TypeInt4:
		switch n := v.(type) {
		case int16:
			return parquet.Int32Value(int32(n)), nil
		case int32:
			return parquet.Int32Value(n), nil
		}
	case TypeInt8:
		if n, ok := v.(int64); ok {
			return parquet.Int64Value(n), nil
		}
	case TypeBool:
		if b, ok := v.(bool); ok {
			return parquet.BooleanValue(b), nil
		}
	case TypeUUID:
		if u, ok := v.([16]byte); ok {
			return parquet.FixedLenByteArrayValue(u[:]), nil
		}
	case TypeTimestamptz:
		if t, ok := v.(time.Time); ok {
			return parquet.Int64Value(t.UnixMicro()), nil
		}
	case TypeDate:
		if t, ok := v.(time.Time); ok {
			return parquet.Int32Value(int32(t.Unix() / 86400)), nil
		}
	case TypeBytea:
		if b, ok := v.([]byte); ok {
			return parquet.ByteArrayValue(b), nil
		}
	}
	return parquet.Value{}, refused(typ, v)
}

// goValue is a stored value back as the canonical row takes it.
func goValue(typ string, v parquet.Value) (any, error) {
	if v.IsNull() {
		return nil, nil
	}
	switch typ {
	case TypeText, TypeNumeric, TypeJSONB:
		return string(v.ByteArray()), nil
	case TypeInt2:
		return int16(v.Int32()), nil
	case TypeInt4:
		return v.Int32(), nil
	case TypeInt8:
		return v.Int64(), nil
	case TypeBool:
		return v.Boolean(), nil
	case TypeUUID:
		var u [16]byte
		if copy(u[:], v.ByteArray()) != 16 {
			return nil, errors.New("tiering: a uuid of another length")
		}
		return u, nil
	case TypeTimestamptz:
		return time.UnixMicro(v.Int64()).UTC(), nil
	case TypeDate:
		return time.Unix(int64(v.Int32())*86400, 0).UTC(), nil
	case TypeBytea:
		return append([]byte(nil), v.ByteArray()...), nil
	}
	return nil, fmt.Errorf("tiering: no type %q", typ)
}

// WriteParquet writes one table's rows (each a map of column to the driver's value, nil for null).
func WriteParquet(t Table, rows []map[string]any) ([]byte, error) {
	schema, err := parquetSchema(t)
	if err != nil {
		return nil, err
	}
	names := columnNames(t)
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema, parquet.Compression(&zstd.Codec{}))
	batch := make([]parquet.Row, 0, 1024)
	flush := func() error {
		_, err := w.WriteRows(batch)
		batch = batch[:0]
		return err
	}
	for _, r := range rows {
		row := make(parquet.Row, len(names))
		for i, name := range names {
			v := r[name]
			if v == nil {
				row[i] = parquet.NullValue().Level(0, 0, i)
				continue
			}
			pv, err := parquetValue(t.Columns[name], v)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", t.Name, name, err)
			}
			row[i] = pv.Level(0, 1, i)
		}
		batch = append(batch, row)
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ReadParquet reads one table's rows back; the file's columns must be the table's (names and order).
func ReadParquet(t Table, data []byte) ([]map[string]any, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	names := columnNames(t)
	fields := f.Schema().Fields()
	if len(fields) != len(names) {
		return nil, fmt.Errorf("tiering: %s has %d columns, the file %d", t.Name, len(names), len(fields))
	}
	for i, fd := range fields {
		if fd.Name() != names[i] {
			return nil, fmt.Errorf("tiering: %s's column %d is %q, the file's %q", t.Name, i, names[i], fd.Name())
		}
	}
	r := parquet.NewReader(f)
	defer r.Close()
	var out []map[string]any
	buf := make([]parquet.Row, 1024)
	for {
		n, err := r.ReadRows(buf)
		for _, row := range buf[:n] {
			m := make(map[string]any, len(names))
			for _, v := range row {
				name := names[v.Column()]
				gv, cerr := goValue(t.Columns[name], v)
				if cerr != nil {
					return nil, cerr
				}
				m[name] = gv
			}
			out = append(out, m)
		}
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
	}
}
