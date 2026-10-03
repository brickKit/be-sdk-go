package idem

import (
	"math"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// maxDepth bounds the nesting of a fingerprint document; deeper input is JSON_INVALID (every input is
// bounded; a request body is at most 1 MiB, P3.6, and no declared fingerprint nests this deep).
const maxDepth = 512

// member is one object member, in input order until the writer sorts them.
type member struct {
	name  string
	value any
}

// object is a parsed JSON object; the parser has already refused duplicate member names (I-JSON).
type object []member

// jsonInvalid is the JSON_INVALID vector class (RFC 7493 I-JSON); the root maps it to REQUEST_INVALID.
func jsonInvalid(detail string) error {
	return &problem.Invalid{Reason: "JSON_INVALID", Detail: detail}
}

// parser is a strict RFC 8259 / RFC 7493 parser over raw bytes. Values are nil, bool, float64,
// string, []any and object.
type parser struct {
	in  []byte
	pos int
}

// parseIJSON parses one I-JSON text: exactly one value surrounded by optional whitespace.
func parseIJSON(in []byte) (any, error) {
	if !utf8.Valid(in) {
		return nil, jsonInvalid("not UTF-8")
	}
	p := &parser{in: in}
	p.ws()
	v, err := p.value(0)
	if err != nil {
		return nil, err
	}
	p.ws()
	if p.pos != len(p.in) {
		return nil, jsonInvalid("text after the value at byte " + strconv.Itoa(p.pos))
	}
	return v, nil
}

func (p *parser) ws() {
	for p.pos < len(p.in) {
		switch p.in[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *parser) fail(what string) error {
	return jsonInvalid(what + " at byte " + strconv.Itoa(p.pos))
}

func (p *parser) value(depth int) (any, error) {
	if depth > maxDepth {
		return nil, p.fail("nesting deeper than 512")
	}
	if p.pos >= len(p.in) {
		return nil, p.fail("unexpected end")
	}
	switch c := p.in[p.pos]; {
	case c == '{':
		return p.object(depth)
	case c == '[':
		return p.array(depth)
	case c == '"':
		return p.str()
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number()
	}
	for lit, v := range map[string]any{"true": true, "false": false, "null": nil} {
		if len(p.in)-p.pos >= len(lit) && string(p.in[p.pos:p.pos+len(lit)]) == lit {
			p.pos += len(lit)
			return v, nil
		}
	}
	return nil, p.fail("unexpected character")
}

func (p *parser) object(depth int) (any, error) {
	p.pos++ // {
	obj := object{}
	seen := map[string]bool{}
	p.ws()
	if p.peek() == '}' {
		p.pos++
		return obj, nil
	}
	for {
		p.ws()
		if p.peek() != '"' {
			return nil, p.fail("member name expected")
		}
		name, err := p.str()
		if err != nil {
			return nil, err
		}
		if seen[name] {
			return nil, p.fail("duplicate member name " + strconv.Quote(name))
		}
		seen[name] = true
		p.ws()
		if p.peek() != ':' {
			return nil, p.fail("':' expected")
		}
		p.pos++
		p.ws()
		v, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		obj = append(obj, member{name: name, value: v})
		if done, err := p.next('}'); err != nil || done {
			return obj, err
		}
	}
}

func (p *parser) array(depth int) (any, error) {
	p.pos++ // [
	arr := []any{}
	p.ws()
	if p.peek() == ']' {
		p.pos++
		return arr, nil
	}
	for {
		p.ws()
		v, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		arr = append(arr, v)
		if done, err := p.next(']'); err != nil || done {
			return arr, err
		}
	}
}

// next consumes the separator after a member or element: ',' (more follow) or the closing byte.
func (p *parser) next(closing byte) (done bool, err error) {
	p.ws()
	switch p.peek() {
	case ',':
		p.pos++
		return false, nil
	case closing:
		p.pos++
		return true, nil
	}
	return false, p.fail("',' or '" + string(closing) + "' expected")
}

func (p *parser) peek() byte {
	if p.pos < len(p.in) {
		return p.in[p.pos]
	}
	return 0
}

// number accepts exactly the RFC 8259 grammar and converts to the nearest double; a value that
// overflows a double is JSON_INVALID (I-JSON), an underflow becomes 0 as in ECMAScript.
func (p *parser) number() (any, error) {
	start := p.pos
	if p.peek() == '-' {
		p.pos++
	}
	switch {
	case p.peek() == '0':
		p.pos++
	case p.peek() >= '1' && p.peek() <= '9':
		p.digits()
	default:
		return nil, p.fail("digit expected")
	}
	if p.peek() == '.' {
		p.pos++
		if !p.digits() {
			return nil, p.fail("digit expected after '.'")
		}
	}
	if c := p.peek(); c == 'e' || c == 'E' {
		p.pos++
		if c := p.peek(); c == '+' || c == '-' {
			p.pos++
		}
		if !p.digits() {
			return nil, p.fail("digit expected in exponent")
		}
	}
	f, err := strconv.ParseFloat(string(p.in[start:p.pos]), 64)
	if err != nil || math.IsInf(f, 0) {
		if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange && !math.IsInf(f, 0) {
			return f, nil // underflow: the nearest double, as ECMAScript does
		}
		return nil, jsonInvalid("number " + string(p.in[start:p.pos]) + " does not fit a double")
	}
	return f, nil
}

func (p *parser) digits() bool {
	start := p.pos
	for p.pos < len(p.in) && p.in[p.pos] >= '0' && p.in[p.pos] <= '9' {
		p.pos++
	}
	return p.pos > start
}

// str parses a string literal. Raw control characters, invalid escapes and lone surrogates (in an
// escape) are JSON_INVALID; the input is already known to be valid UTF-8.
func (p *parser) str() (string, error) {
	p.pos++ // "
	var out []byte
	for {
		if p.pos >= len(p.in) {
			return "", p.fail("unterminated string")
		}
		c := p.in[p.pos]
		switch {
		case c == '"':
			p.pos++
			return string(out), nil
		case c < 0x20:
			return "", p.fail("raw control character in string")
		case c == '\\':
			r, err := p.escape()
			if err != nil {
				return "", err
			}
			out = utf8.AppendRune(out, r)
		default:
			out = append(out, c)
			p.pos++
		}
	}
}

var simpleEscapes = map[byte]rune{'"': '"', '\\': '\\', '/': '/', 'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t'}

// escape decodes one escape sequence starting at the backslash; a surrogate pair written as two
// \u escapes becomes one code point.
func (p *parser) escape() (rune, error) {
	if p.pos+1 >= len(p.in) {
		return 0, p.fail("unterminated escape")
	}
	e := p.in[p.pos+1]
	if r, ok := simpleEscapes[e]; ok {
		p.pos += 2
		return r, nil
	}
	if e != 'u' {
		return 0, p.fail("invalid escape")
	}
	r1, err := p.hex4()
	if err != nil {
		return 0, err
	}
	if !utf16.IsSurrogate(r1) {
		return r1, nil
	}
	if r1 < 0xDC00 && p.pos+1 < len(p.in) && p.in[p.pos] == '\\' && p.in[p.pos+1] == 'u' {
		r2, err := p.hex4()
		if err != nil {
			return 0, err
		}
		if r := utf16.DecodeRune(r1, r2); r != utf8.RuneError {
			return r, nil
		}
	}
	return 0, p.fail("lone surrogate")
}

// hex4 reads `\uXXXX` at pos and advances past it.
func (p *parser) hex4() (rune, error) {
	if p.pos+6 > len(p.in) {
		return 0, p.fail("short \\u escape")
	}
	v, err := strconv.ParseUint(string(p.in[p.pos+2:p.pos+6]), 16, 32)
	if err != nil {
		return 0, p.fail("invalid \\u escape")
	}
	p.pos += 6
	return rune(v), nil
}
