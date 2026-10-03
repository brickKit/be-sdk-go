package idem

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
)

// writeJCS appends the RFC 8785 serialisation of a parsed value.
func writeJCS(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, "null"...)
	case bool:
		return strconv.AppendBool(b, x)
	case float64:
		return append(b, esNumber(x)...)
	case string:
		return writeString(b, x)
	case []any:
		b = append(b, '[')
		for i, e := range x {
			if i > 0 {
				b = append(b, ',')
			}
			b = writeJCS(b, e)
		}
		return append(b, ']')
	case object:
		return writeObject(b, x)
	}
	panic("idem: unexpected parsed value") // unreachable: the parser builds only the types above
}

// writeObject writes members sorted by the UTF-16 code units of their names (RFC 8785 §3.2.3).
func writeObject(b []byte, o object) []byte {
	type keyed struct {
		units []uint16
		m     member
	}
	ms := make([]keyed, len(o))
	for i, m := range o {
		ms[i] = keyed{units: utf16.Encode([]rune(m.name)), m: m}
	}
	sort.Slice(ms, func(i, j int) bool { return lessUnits(ms[i].units, ms[j].units) })
	b = append(b, '{')
	for i, k := range ms {
		if i > 0 {
			b = append(b, ',')
		}
		b = writeString(b, k.m.name)
		b = append(b, ':')
		b = writeJCS(b, k.m.value)
	}
	return append(b, '}')
}

func lessUnits(a, b []uint16) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

const hexDigits = "0123456789abcdef"

// writeString escapes only what RFC 8785 §3.2.2.2 requires: `"`, `\`, the five short control
// escapes, other controls as lower-case \u00xx; everything else (U+007F, U+2028, U+2029 included)
// is raw UTF-8.
func writeString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b = append(b, '\\', '"')
		case '\\':
			b = append(b, '\\', '\\')
		case '\b':
			b = append(b, '\\', 'b')
		case '\t':
			b = append(b, '\\', 't')
		case '\n':
			b = append(b, '\\', 'n')
		case '\f':
			b = append(b, '\\', 'f')
		case '\r':
			b = append(b, '\\', 'r')
		default:
			if c < 0x20 {
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			} else {
				b = append(b, c)
			}
		}
	}
	return append(b, '"')
}

// esNumber formats a finite double as ECMAScript's Number.prototype.toString does (RFC 8785
// §3.2.2.3): the shortest round-tripping digits, fixed notation for 1e-6 <= |x| < 1e21, exponent
// notation otherwise, and -0 as 0.
//
// Decision tree, with digits d1…dk and decimal exponent n (value = 0.d1…dk × 10^n):
//
//	k <= n <= 21   → digits followed by n-k zeros
//	0 < n <= 21    → digits with the point after n of them
//	-6 < n <= 0    → "0." then -n zeros then the digits
//	otherwise      → d1[.d2…dk] "e" sign (n-1)
func esNumber(x float64) string {
	if x == 0 {
		return "0"
	}
	if x < 0 {
		return "-" + esNumber(-x)
	}
	if math.IsNaN(x) || math.IsInf(x, 0) {
		panic("idem: non-finite number") // unreachable: the parser refuses them
	}
	e := strconv.FormatFloat(x, 'e', -1, 64) // d.ddde±XX, shortest round-trip
	mant, expPart, _ := strings.Cut(e, "e")
	digits := strings.Replace(mant, ".", "", 1)
	exp, _ := strconv.Atoi(expPart)
	k, n := len(digits), exp+1
	switch {
	case k <= n && n <= 21:
		return digits + strings.Repeat("0", n-k)
	case 0 < n && n <= 21:
		return digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		return "0." + strings.Repeat("0", -n) + digits
	}
	sign := "+"
	if n-1 < 0 {
		sign = "-"
	}
	m := digits[:1]
	if k > 1 {
		m += "." + digits[1:]
	}
	return m + "e" + sign + strconv.Itoa(abs(n-1))
}

func abs(i int) int {
	if i < 0 {
		return -i
	}
	return i
}
