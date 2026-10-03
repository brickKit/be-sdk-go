package authn

import "errors"

// ErrTokenInvalid is the one error class of token verification: the request answers 401
// TOKEN_INVALID, domain be (P5.1–P5.3, P5.9). Every error Verify returns matches it with errors.Is.
var ErrTokenInvalid = errors.New("TOKEN_INVALID")

// Rules name the check that failed, in the order Verify runs them (contract-infra-iam TOKENS.md,
// "Verification", checks 1–12; the names are the vectors' rule names). They are for the log only.
const (
	RuleFormat    = "format"     // P5.1: Bearer, compact JWS, JSON header and payload
	RuleAlg       = "alg"        // P5.2: allow-list, equal to the key's alg
	RuleKid       = "kid"        // P5.2, P5.4: kid present and known after at most one refetch
	RuleSignature = "signature"  // P5.2: the signature verifies with the selected key
	RuleClaimType = "claim_type" // P5.9: optional claims of the wrong JSON type
	RuleIss       = "iss"        // P5.3
	RuleAud       = "aud"        // P5.3
	RuleTyp       = "typ"        // P5.3
	RuleSub       = "sub"        // P5.3
	RuleExp       = "exp"        // P5.3
	RuleNbf       = "nbf"        // P5.3
	RuleIat       = "iat"        // P5.3
	RuleJti       = "jti"        // P5.3
)

// InvalidError is the TOKEN_INVALID error with what failed. Rule and Detail go to the log, never to
// the response (TOKENS.md); neither ever contains the token or a claim value (P5.8).
type InvalidError struct {
	Rule   string
	Detail string
}

// Error renders the error for the log.
func (e *InvalidError) Error() string { return "TOKEN_INVALID: " + e.Rule + ": " + e.Detail }

// Is makes errors.Is(err, ErrTokenInvalid) true.
func (e *InvalidError) Is(target error) bool { return target == ErrTokenInvalid }

func invalid(rule, detail string) error { return &InvalidError{Rule: rule, Detail: detail} }
