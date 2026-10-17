package identity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/golang-jwt/jwt/v5"
)

// Token checks (P8.1).
const (
	TokenType = "at+jwt"
	Leeway    = 60 * time.Second
)

// Algorithms are the only signature algorithms accepted.
var Algorithms = []string{"RS256", "ES256", "EdDSA"}

var principalID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// User is a verified user credential.
type User struct {
	Subject string    // the platform principal
	Expiry  time.Time // the credential's own end: part of the request's cutoff
}

// Problem names the first check a token failed (protocol vector jwt-verify); it is logged, never sent.
type Problem string

// TokenError is a refused token: TOKEN_INVALID to the caller, the problem in the logs.
type TokenError struct{ Problem Problem }

func (e *TokenError) Error() string { return "token invalid: " + string(e.Problem) }

// Verifier verifies user access tokens for one deployment.
type Verifier struct {
	Issuer   string
	Audience string
	Keys     *KeySet
	Now      func() time.Time
}

// Verify checks a token. A refused token is a *TokenError; keys that cannot be obtained are DEPENDENCY_UNAVAILABLE.
func (v *Verifier) Verify(token string) (User, error) {
	now := time.Now
	if v.Now != nil {
		now = v.Now
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return User{}, &TokenError{"format"}
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	var h struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
		Kid string `json:"kid"`
	}
	var fields map[string]json.RawMessage
	if err != nil || json.Unmarshal(raw, &fields) != nil || json.Unmarshal(raw, &h) != nil {
		return User{}, &TokenError{"format"}
	}
	// encoding/json matches field names without regard to case; the header's names are exact (RFC 7515)
	for name, dst := range map[string]*string{"alg": &h.Alg, "typ": &h.Typ, "kid": &h.Kid} {
		if _, ok := fields[name]; !ok {
			*dst = ""
		}
	}
	switch {
	case !slices.Contains(Algorithms, h.Alg):
		return User{}, &TokenError{"alg"}
	case h.Typ != TokenType:
		return User{}, &TokenError{"typ"}
	case h.Kid == "":
		return User{}, &TokenError{"kid"}
	}
	claims := jwt.RegisteredClaims{}
	p := jwt.NewParser(jwt.WithValidMethods(Algorithms), jwt.WithIssuer(v.Issuer), jwt.WithAudience(v.Audience),
		jwt.WithLeeway(Leeway), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithTimeFunc(now))
	_, err = p.ParseWithClaims(token, &claims, v.Keys.keyfunc)
	if err != nil {
		return User{}, classify(err)
	}
	if !principalID.MatchString(claims.Subject) {
		return User{}, &TokenError{"sub"}
	}
	return User{Subject: claims.Subject, Expiry: claims.ExpiresAt.Time}, nil
}

func classify(err error) error {
	switch {
	case errors.Is(err, ErrKeysUnavailable):
		return errs.New(errs.ReasonDependencyUnavailable, "the identity keys are unavailable").With("dependency", "iam").Wrap(err)
	case errors.Is(err, errUnknownKID):
		return &TokenError{"kid_unknown"}
	case errors.Is(err, jwt.ErrTokenMalformed):
		return &TokenError{"format"}
	case errors.Is(err, jwt.ErrTokenSignatureInvalid):
		return &TokenError{"signature"}
	case errors.Is(err, jwt.ErrTokenUnverifiable):
		return &TokenError{"alg"} // the key does not fit the algorithm
	case errors.Is(err, jwt.ErrTokenInvalidIssuer):
		return &TokenError{"iss"}
	case errors.Is(err, jwt.ErrTokenInvalidAudience):
		return &TokenError{"aud"}
	case errors.Is(err, jwt.ErrTokenExpired):
		return &TokenError{"exp"}
	case errors.Is(err, jwt.ErrTokenNotValidYet):
		return &TokenError{"nbf"}
	case errors.Is(err, jwt.ErrTokenUsedBeforeIssued):
		return &TokenError{"iat"}
	case errors.Is(err, jwt.ErrTokenRequiredClaimMissing):
		switch {
		case strings.Contains(err.Error(), "aud claim is required"):
			return &TokenError{"aud"}
		case strings.Contains(err.Error(), "iss claim is required"):
			return &TokenError{"iss"}
		}
		return &TokenError{"exp"}
	}
	return &TokenError{"format"}
}

// Refusal is the error a caller gets for a refused token: TOKEN_INVALID, with nothing about why.
func Refusal(err error) error {
	var te *TokenError
	if errors.As(err, &te) {
		return errs.New(errs.ReasonTokenInvalid, "the credential is missing or not valid").Wrap(err)
	}
	return errs.From(err)
}

type userKey struct{}

// WithUser returns ctx carrying the verified user.
func WithUser(ctx context.Context, u User) context.Context {
	return context.WithValue(ctx, userKey{}, u)
}

// UserFrom returns the request's verified user.
func UserFrom(ctx context.Context) (User, bool) { u, ok := ctx.Value(userKey{}).(User); return u, ok }
