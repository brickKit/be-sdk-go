package logx

import "strings"

// RedactedMark replaces the value of a protected key (P18.2).
const RedactedMark = "[REDACTED]"

// isEnvelope reports whether a top-level key is an envelope field, which redaction and truncation never
// touch (P18.2; vectors redaction README "Envelope fields").
func isEnvelope(key string) bool {
	switch key {
	case "time", "level", "msg", "component_id", "component_version", "trace_id", "span_id", "request_id":
		return true
	}
	return false
}

// Redact returns a copy of one log record with every protected key's value replaced by RedactedMark,
// walking non-matching objects and arrays; envelope fields and values are never scanned (P18.2, vectors
// redaction/redact). The input is not modified.
func Redact(record map[string]any) map[string]any {
	out := make(map[string]any, len(record))
	for k, v := range record {
		out[k] = redactTop(k, v)
	}
	return out
}

// redactTop applies the rule to one top-level field.
func redactTop(key string, v any) any {
	if isEnvelope(key) {
		return v
	}
	return redactField(key, v)
}

// redactField applies the rule to a non-envelope field at any depth.
func redactField(key string, v any) any {
	if ProtectedKey(key) {
		return RedactedMark
	}
	return redactWalk(v)
}

// redactWalk copies objects and arrays, redacting protected keys inside them.
func redactWalk(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = redactField(k, e)
		}
		return m
	case []any:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = redactWalk(e)
		}
		return s
	}
	return v
}

// ProtectedKey reports whether a log field key carries personal data or a secret (P18.2): the key is
// split into words (camelCase, '_', '-', '.'; case-insensitive) and matches when a protected name's words
// appear as one contiguous run, the last one optionally with a plural 's'.
func ProtectedKey(key string) bool {
	ws := keyWords(key)
	for i := range ws {
		if nameAt(ws, i) {
			return true
		}
	}
	return false
}

// nameAt reports whether a protected name starts at word i. The protected names are phone, mobile,
// id_card, password, bank_card, email, token, secret, authorization, cookie, set_cookie, api_key.
func nameAt(ws []string, i int) bool {
	switch ws[i] {
	case "phone", "mobile", "password", "email", "token", "secret", "authorization", "cookie":
		return true
	case "phones", "mobiles", "passwords", "emails", "tokens", "secrets", "authorizations", "cookies":
		return true
	case "id", "bank":
		return wordAt(ws, i+1, "card")
	case "set":
		return wordAt(ws, i+1, "cookie")
	case "api":
		return wordAt(ws, i+1, "key")
	}
	return false
}

// wordAt reports whether word i is w or its plural.
func wordAt(ws []string, i int, w string) bool {
	return i < len(ws) && (ws[i] == w || ws[i] == w+"s")
}

// keyWords splits a key into lower-case words, exactly as the vectors' generator does: insert '_' between
// a lower-case letter or digit and an upper-case letter; between an upper-case run and the upper-case
// letter that starts a capitalised word (IDCard -> ID_Card); treat '-' and '.' as '_'; lower-case; split.
func keyWords(key string) []string {
	var b strings.Builder
	b.Grow(len(key) + 4)
	for i := 0; i < len(key); i++ {
		c := key[i]
		if i > 0 && isUpper(c) {
			p := key[i-1]
			if isLower(p) || isDigit(p) {
				b.WriteByte('_')
			} else if isUpper(p) && i+1 < len(key) && isLower(key[i+1]) {
				b.WriteByte('_')
			}
		}
		if c == '-' || c == '.' {
			c = '_'
		}
		b.WriteByte(c)
	}
	parts := strings.Split(strings.ToLower(b.String()), "_")
	ws := parts[:0]
	for _, p := range parts {
		if p != "" {
			ws = append(ws, p)
		}
	}
	return ws
}

func isUpper(c byte) bool { return c >= 'A' && c <= 'Z' }
func isLower(c byte) bool { return c >= 'a' && c <= 'z' }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
