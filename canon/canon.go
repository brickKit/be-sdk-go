// Package canon is the canonical encoding of security records (protocol P8.8; assembly project security 09a):
// normalized decimals, instants and text, RFC 8785 serialization of strings, integers, booleans, arrays and objects,
// and domain-separated SHA-256 digests. No JSON null and no floating point ever appear.
package canon

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"golang.org/x/text/unicode/norm"
)

// ErrRefused is returned for values the encoding forbids.
var ErrRefused = errors.New("canon: value not allowed in the canonical encoding")

// maxInt is 2^53: integers in digests stay below it in absolute value.
var maxInt = new(big.Int).Lsh(big.NewInt(1), 53)

// Decimal normalizes a decimal: no exponent, no leading "+", no superfluous zeros, "0" for zero; at most 38
// significant digits and 18 fraction digits.
func Decimal(s string) (string, error) {
	d, _, err := apd.NewFromString(s)
	if err != nil || d.Form != apd.Finite {
		return "", fmt.Errorf("%w: decimal %q", ErrRefused, s)
	}
	if d.IsZero() {
		return "0", nil
	}
	var r apd.Decimal
	r.Reduce(d)
	digits := r.Coeff.String()
	if len(digits) > 38 || -r.Exponent > 18 {
		return "", fmt.Errorf("%w: decimal %q", ErrRefused, s)
	}
	out := r.Text('f')
	if strings.Contains(out, ".") {
		out = strings.TrimRight(strings.TrimRight(out, "0"), ".")
	}
	return out, nil
}

// Instant normalizes a time with an offset to UTC with six fraction digits.
func Instant(s string) (string, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return "", fmt.Errorf("%w: instant %q", ErrRefused, s)
	}
	return Time(t), nil
}

// Time renders an instant canonically.
func Time(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000Z") }

// Text normalizes text to Unicode NFC.
func Text(s string) string { return norm.NFC.String(s) }

// Encode serializes v (string, bool, integer types, []any, map[string]any, and slices or maps of those) by RFC 8785.
func Encode(v any) ([]byte, error) {
	var b bytes.Buffer
	if err := encode(&b, v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func encode(b *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		return fmt.Errorf("%w: null", ErrRefused)
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case int:
		return encodeInt(b, big.NewInt(int64(x)))
	case int64:
		return encodeInt(b, big.NewInt(x))
	case int32:
		return encodeInt(b, big.NewInt(int64(x)))
	case *big.Int:
		return encodeInt(b, x)
	case float32, float64:
		return fmt.Errorf("%w: floating point", ErrRefused)
	case json.Number: // decoded with UseNumber: only integers are allowed
		n, ok := new(big.Int).SetString(string(x), 10)
		if !ok {
			return fmt.Errorf("%w: number %s is not an integer", ErrRefused, x)
		}
		return encodeInt(b, n)
	case string:
		encodeString(b, x)
	case []string:
		b.WriteByte('[')
		for i, s := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			encodeString(b, s)
		}
		b.WriteByte(']')
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := encode(b, e); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys) // keys are ASCII: byte order is UTF-16 order
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			encodeString(b, k)
			b.WriteByte(':')
			if err := encode(b, x[k]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	default:
		return fmt.Errorf("%w: type %T", ErrRefused, v)
	}
	return nil
}

func encodeInt(b *bytes.Buffer, n *big.Int) error {
	if new(big.Int).Abs(n).Cmp(maxInt) >= 0 {
		return fmt.Errorf("%w: integer %s is not below 2^53", ErrRefused, n)
	}
	b.WriteString(n.String())
	return nil
}

func encodeString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\b':
			b.WriteString(`\b`)
		case r == '\f':
			b.WriteString(`\f`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20:
			fmt.Fprintf(b, `\u%04x`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}

// Digest is "sha256:" + SHA-256 of "<record type>.v<major>\n" followed by the canonical bytes.
func Digest(recordType string, major int, v any) (string, error) {
	body, err := Encode(v)
	if err != nil {
		return "", err
	}
	return DigestBytes(recordType, major, body), nil
}

// DigestBytes is Digest over bytes that are already canonical (a record as received, D76).
func DigestBytes(recordType string, major int, body []byte) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s.v%d\n", recordType, major)
	h.Write(body)
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// Parse decodes bytes received as a canonical record and refuses them unless they are exactly the canonical
// encoding of what they decode to (key order, no whitespace, minimal escapes, no null or floating point, NFC
// text). Numbers come back as json.Number.
func Parse(raw []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRefused, err)
	}
	if d.More() {
		return nil, fmt.Errorf("%w: data after the value", ErrRefused)
	}
	again, err := Encode(nfc(v))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(again, raw) {
		return nil, fmt.Errorf("%w: the bytes are not in canonical form", ErrRefused)
	}
	return v, nil
}

// nfc normalizes every string and key, so that text not in NFC shows up as a difference.
func nfc(v any) any {
	switch x := v.(type) {
	case string:
		return Text(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = nfc(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[Text(k)] = nfc(e)
		}
		return out
	}
	return v
}

// SortedSet orders a set's elements by their canonical bytes and drops duplicates.
func SortedSet(items []any) ([]any, error) {
	seen := map[string]any{}
	for _, it := range items {
		b, err := Encode(it)
		if err != nil {
			return nil, err
		}
		seen[string(b)] = it
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, len(keys))
	for i, k := range keys {
		out[i] = seen[k]
	}
	return out, nil
}
