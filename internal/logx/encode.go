package logx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// lineEncoder writes records as one JSON object each, without HTML escaping (encoding/json otherwise).
type lineEncoder struct {
	tmp bytes.Buffer
	enc *json.Encoder
}

func newLineEncoder() *lineEncoder {
	e := &lineEncoder{}
	e.enc = json.NewEncoder(&e.tmp)
	e.enc.SetEscapeHTML(false)
	return e
}

// value appends the JSON encoding of v to out.
func (e *lineEncoder) value(out *bytes.Buffer, v any) {
	e.tmp.Reset()
	if err := e.enc.Encode(v); err != nil {
		e.tmp.Reset()
		_ = e.enc.Encode(fmt.Sprintf("%+v", v))
	}
	b := e.tmp.Bytes()
	out.Write(b[:len(b)-1]) // Encode ends with '\n'
}

// record encodes the fields in order as one JSON object, without the trailing newline.
func (e *lineEncoder) record(r *record) []byte {
	var out bytes.Buffer
	out.WriteByte('{')
	for i, f := range r.fields {
		if i > 0 {
			out.WriteByte(',')
		}
		e.value(&out, f.key)
		out.WriteByte(':')
		e.value(&out, f.val)
	}
	out.WriteByte('}')
	return out.Bytes()
}

// stringCost returns the encoded length of s inside its quotes, exactly as lineEncoder writes it:
// encoding/json with HTML escaping off escapes '"' '\\' and the controls (\n \r \t \b \f as two bytes,
// others as \u00XX), writes an invalid byte as � and U+2028/U+2029 as  / .
func stringCost(s string) int {
	n := 0
	for i := 0; i < len(s); {
		c, size := runeCost(s[i:])
		n += c
		i += size
	}
	return n
}

// runeCost returns the encoded length of the first rune of s and its byte size.
func runeCost(s string) (int, int) {
	b := s[0]
	if b < utf8.RuneSelf {
		switch {
		case b == '"' || b == '\\' || b == '\n' || b == '\r' || b == '\t' || b == '\b' || b == '\f':
			return 2, 1
		case b < 0x20:
			return 6, 1
		}
		return 1, 1
	}
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && size == 1 {
		return 6, 1
	}
	if r == ' ' || r == ' ' {
		return 6, size
	}
	return size, size
}
