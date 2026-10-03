package logx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"
)

// field is one top-level key of a log line; val is a JSON value (string, json.Number, bool, nil,
// int64, uint64, float64, map[string]any, []any).
type field struct {
	key string
	val any
}

// record is one log line being built: fields in output order, the first `fixed` of them written by the
// handler itself and never overridden by an attribute.
type record struct {
	fields []field
	index  map[string]int
	fixed  int
}

func newRecord(n int) *record {
	return &record{fields: make([]field, 0, n), index: make(map[string]int, n)}
}

// set adds a field, or replaces the value of an earlier one (the later value wins); a key written by the
// handler itself (the fixed prefix) is never replaced.
func (r *record) set(key string, val any) {
	if i, ok := r.index[key]; ok {
		if i >= r.fixed {
			r.fields[i].val = val
		}
		return
	}
	r.index[key] = len(r.fields)
	r.fields = append(r.fields, field{key, val})
}

// remove deletes a field, keeping the order of the others.
func (r *record) remove(key string) {
	i, ok := r.index[key]
	if !ok {
		return
	}
	r.fields = append(r.fields[:i], r.fields[i+1:]...)
	delete(r.index, key)
	for j := i; j < len(r.fields); j++ {
		r.index[r.fields[j].key] = j
	}
}

// addAttr adds one slog attribute under a dotted prefix: groups become dotted keys (error.code), an
// empty group is dropped, a group with an empty key is inlined, and the zero Attr is ignored.
func (r *record) addAttr(prefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		attrs := a.Value.Group()
		if len(attrs) == 0 {
			return
		}
		if a.Key != "" {
			prefix += a.Key + "."
		}
		for _, g := range attrs {
			r.addAttr(prefix, g)
		}
		return
	}
	if a.Key == "" && a.Value.Any() == nil {
		return
	}
	r.set(prefix+a.Key, jsonValue(a.Value))
}

// jsonValue converts a resolved slog value into a JSON value. Non-finite floats become strings, errors
// their text, and any other Go value goes through encoding/json (numbers kept exact as json.Number) so
// redaction can walk its objects; a value encoding/json refuses becomes its %+v text.
func jsonValue(v slog.Value) any {
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return finite(v.Float64())
	case slog.KindBool:
		return v.Bool()
	case slog.KindDuration:
		return int64(v.Duration())
	case slog.KindTime:
		return v.Time().UTC().Format(time.RFC3339Nano)
	}
	return anyValue(v.Any())
}

func finite(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Sprint(f)
	}
	return f
}

func anyValue(x any) any {
	switch t := x.(type) {
	case nil:
		return nil
	case error:
		return t.Error()
	case float64:
		return finite(t)
	case float32:
		return finite(float64(t))
	}
	b, err := json.Marshal(x)
	if err != nil {
		return fmt.Sprintf("%+v", x)
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var out any
	if err := d.Decode(&out); err != nil {
		return fmt.Sprintf("%+v", x)
	}
	return out
}
