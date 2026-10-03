package authn

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
)

// maxTokenBytes bounds the compact token before any decoding. TOKENS.md keeps an access token well
// under 4 KiB; the spec sets no limit, so this one only stops pathological input.
const maxTokenBytes = 16 << 10

// allowedAlgs is the P5.2 allow-list.
var allowedAlgs = map[string]bool{"RS256": true, "ES256": true, "EdDSA": true}

// b64url is base64url without padding, rejecting non-canonical trailing bits (RFC 7515 §2).
var b64url = base64.RawURLEncoding.Strict()

// jws is a compact JWS split and decoded (P5.1).
type jws struct {
	signingInput string // header.payload, as received
	header       map[string]json.RawMessage
	payload      map[string]json.RawMessage
	signature    []byte
}

// bearerToken extracts the token from an Authorization header value (P5.1). The scheme is matched
// case-insensitively (RFC 9110 §11.1) and followed by exactly one space.
func bearerToken(authorization string) (string, error) {
	scheme, tok, ok := strings.Cut(authorization, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || tok == "" {
		return "", invalid(RuleFormat, "Authorization is not a Bearer credential")
	}
	if len(tok) > maxTokenBytes {
		return "", invalid(RuleFormat, "token longer than the limit")
	}
	return tok, nil
}

// parseJWS checks TOKENS.md check 1: three base64url segments, header and payload JSON objects.
func parseJWS(tok string) (*jws, error) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return nil, invalid(RuleFormat, "not three segments")
	}
	h, err1 := b64url.DecodeString(parts[0])
	p, err2 := b64url.DecodeString(parts[1])
	sig, err3 := b64url.DecodeString(parts[2])
	if err1 != nil || err2 != nil || err3 != nil {
		return nil, invalid(RuleFormat, "segment is not base64url")
	}
	out := &jws{signingInput: parts[0] + "." + parts[1], signature: sig}
	if !jsonObject(h, &out.header) {
		return nil, invalid(RuleFormat, "header is not a JSON object")
	}
	if !jsonObject(p, &out.payload) {
		return nil, invalid(RuleFormat, "payload is not a JSON object")
	}
	if _, ok := out.header["crit"]; ok {
		return nil, invalid(RuleFormat, "crit header parameters are not supported")
	}
	return out, nil
}

// jsonObject decodes b into dst when b is a JSON object.
func jsonObject(b []byte, dst *map[string]json.RawMessage) bool {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || b[0] != '{' {
		return false
	}
	return json.Unmarshal(b, dst) == nil
}

// headerAlgKid checks the header part of TOKENS.md check 2: alg in the allow-list, kid present.
func headerAlgKid(h map[string]json.RawMessage) (alg, kid string, err error) {
	alg, ok := jsonString(h["alg"])
	if !ok || !allowedAlgs[alg] {
		return "", "", invalid(RuleAlg, "alg is not one of RS256, ES256, EdDSA")
	}
	kid, ok = jsonString(h["kid"])
	if !ok || kid == "" {
		return "", "", invalid(RuleKid, "kid missing")
	}
	return alg, kid, nil
}

// jsonString decodes a JSON string; null, absent and every other type are false.
func jsonString(v json.RawMessage) (string, bool) {
	if len(v) == 0 || v[0] != '"' {
		return "", false
	}
	var s string
	if json.Unmarshal(v, &s) != nil {
		return "", false
	}
	return s, true
}
