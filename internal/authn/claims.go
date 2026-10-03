package authn

import (
	"encoding/json"
	"slices"
	"time"
)

// skew is the clock tolerance for exp, nbf and iat (P5.3).
const skew = 60

// Claims are what a component reads from a verified access token (P5.5). The raw token is never
// kept here (P5.8).
type Claims struct {
	Sub      string
	TenantID string
	DeptPath string
	AZP      string
	Locale   string
	DG       string
	JTI      string
	// OrgID is deprecated (P5.5): read, never used for a decision.
	OrgID     string
	Roles     []string
	Ceil      []string
	Act       *Act
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// Act is one link of the act chain (P5.5, RFC 8693 §4.1); Kind is user, agent or svc.
type Act struct {
	Sub  string
	Kind string
	Act  *Act
}

// actKinds is the act.kind enum (P5.5).
var actKinds = []string{"user", "agent", "svc"}

// checkClaims runs TOKENS.md checks 4–12 in order on a payload whose signature has verified; the
// first failure decides.
func checkClaims(p map[string]json.RawMessage, issuer, audience string, now time.Time) (*Claims, error) {
	c, err := readOptionalClaims(p)
	if err != nil {
		return nil, err
	}
	if iss, ok := jsonString(p["iss"]); !ok || issuer == "" || iss != issuer {
		return nil, invalid(RuleIss, "iss is not IAM_ISSUER")
	}
	if !audContains(p["aud"], audience) {
		return nil, invalid(RuleAud, "aud does not contain TENANT_ID or is not a string or an array of strings")
	}
	if typ, ok := jsonString(p["typ"]); !ok || typ != "access" {
		return nil, invalid(RuleTyp, `typ is not "access"`)
	}
	sub, ok := jsonString(p["sub"])
	if !ok || sub == "" {
		return nil, invalid(RuleSub, "sub is not a non-empty string")
	}
	if err := checkTimes(p, now.Unix(), c); err != nil {
		return nil, err
	}
	jti, ok := jsonString(p["jti"])
	if !ok || jti == "" {
		return nil, invalid(RuleJti, "jti is not a non-empty string")
	}
	c.Sub, c.JTI = sub, jti
	return c, nil
}

// checkTimes is checks 9–11: exp required and now < exp + skew; nbf when present and nbf ≤ now +
// skew; iat required and iat ≤ now + skew. All are integers (access-token.schema.json).
func checkTimes(p map[string]json.RawMessage, now int64, c *Claims) error {
	exp, ok := jsonInt(p["exp"])
	if !ok || now >= exp+skew {
		return invalid(RuleExp, "exp missing, not an integer, or past")
	}
	if raw, present := p["nbf"]; present {
		if nbf, ok := jsonInt(raw); !ok || nbf > now+skew {
			return invalid(RuleNbf, "nbf not an integer or in the future")
		}
	}
	iat, ok := jsonInt(p["iat"])
	if !ok || iat > now+skew {
		return invalid(RuleIat, "iat missing, not an integer, or in the future")
	}
	c.IssuedAt, c.ExpiresAt = time.Unix(iat, 0), time.Unix(exp, 0)
	return nil
}

// readOptionalClaims is check 4: the optional claims, when present, have their JSON types (P5.9).
func readOptionalClaims(p map[string]json.RawMessage) (*Claims, error) {
	c := &Claims{}
	strs := map[string]*string{
		"tenant_id": &c.TenantID, "dept_path": &c.DeptPath, "azp": &c.AZP,
		"locale": &c.Locale, "dg": &c.DG, "org_id": &c.OrgID,
	}
	for name, dst := range strs {
		if raw, ok := p[name]; ok {
			s, ok := jsonString(raw)
			if !ok {
				return nil, invalid(RuleClaimType, name+" is not a string")
			}
			*dst = s
		}
	}
	arrays := map[string]*[]string{"roles": &c.Roles, "ceil": &c.Ceil}
	for name, dst := range arrays {
		if raw, ok := p[name]; ok {
			v, ok := jsonStrings(raw)
			if !ok {
				return nil, invalid(RuleClaimType, name+" is not an array of strings")
			}
			*dst = v
		}
	}
	if raw, ok := p["act"]; ok {
		act, ok := parseAct(raw)
		if !ok {
			return nil, invalid(RuleClaimType, "act is not {sub, kind, act?} with a known kind")
		}
		c.Act = act
	}
	return c, nil
}

// parseAct reads one act object: non-empty sub, kind in the enum, nested act valid when present.
// The chain is bounded by the token size limit.
func parseAct(raw json.RawMessage) (*Act, bool) {
	var obj map[string]json.RawMessage
	if !jsonObject(raw, &obj) {
		return nil, false
	}
	sub, ok := jsonString(obj["sub"])
	if !ok || sub == "" {
		return nil, false
	}
	kind, ok := jsonString(obj["kind"])
	if !ok || !slices.Contains(actKinds, kind) {
		return nil, false
	}
	a := &Act{Sub: sub, Kind: kind}
	if next, present := obj["act"]; present {
		if a.Act, ok = parseAct(next); !ok {
			return nil, false
		}
	}
	return a, true
}

// audContains is check 6: aud is a string or an array of strings, and contains audience.
func audContains(raw json.RawMessage, audience string) bool {
	if audience == "" {
		return false
	}
	if s, ok := jsonString(raw); ok {
		return s == audience
	}
	list, ok := jsonStrings(raw)
	return ok && slices.Contains(list, audience)
}

// jsonStrings decodes a JSON array whose every element is a string.
func jsonStrings(raw json.RawMessage) ([]string, bool) {
	if len(raw) == 0 || raw[0] != '[' {
		return nil, false
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return nil, false
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		s, ok := jsonString(it)
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

// jsonInt decodes a JSON number that is an integer.
func jsonInt(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 || !(raw[0] == '-' || (raw[0] >= '0' && raw[0] <= '9')) {
		return 0, false
	}
	n, err := json.Number(raw).Int64()
	return n, err == nil
}
