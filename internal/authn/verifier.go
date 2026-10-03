// Package authn verifies the platform access token locally against the identity provider's
// published keys (be-protocol P5; contract-infra-iam TOKENS.md "Verification", checks 1–12). The
// bundle-dependent checks (delegation capabilities, stale) are internal/authz's; the root package
// runs both.
package authn

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Config configures a Verifier.
type Config struct {
	// JWKSURL is {IAM_URL}/.well-known/jwks.json (P5.4); the caller builds it.
	JWKSURL string
	// Issuer is IAM_ISSUER; empty matches no token.
	Issuer string
	// Audience is TENANT_ID; empty matches no token.
	Audience string
	// Client is the caller's HTTP client; nil uses a client with its own transport (no shared state).
	Client *http.Client
	// Now is the clock for the time claims and the refetch rate limit; nil is time.Now.
	Now func() time.Time
	// Logger receives fetch failures; nil discards.
	Logger *slog.Logger
}

// Verifier verifies access tokens (P5.1–P5.4, P5.9). It is safe for concurrent use; Run is called
// once, by the caller's supervisor.
type Verifier struct {
	cfg    Config
	client *http.Client
	now    func() time.Time
	log    *slog.Logger

	refreshEvery time.Duration
	refetchGap   time.Duration
	fetchTimeout time.Duration
	backoffBase  time.Duration
	backoffMax   time.Duration

	keys      atomic.Pointer[map[string]publicKey]
	ready     atomic.Bool
	fetchMu   sync.Mutex
	lastFetch time.Time // guarded by fetchMu
}

// NewVerifier returns a Verifier that holds no keys yet; Run or the first unknown kid loads them.
func NewVerifier(c Config) *Verifier {
	v := &Verifier{
		cfg: c, client: c.Client, now: c.Now, log: c.Logger,
		refreshEvery: defaultRefreshEvery, refetchGap: defaultRefetchGap, fetchTimeout: defaultFetchTimeout,
		backoffBase: defaultBackoffBase, backoffMax: defaultBackoffMax,
	}
	if v.client == nil {
		v.client = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	if v.now == nil {
		v.now = time.Now
	}
	if v.log == nil {
		v.log = slog.New(slog.DiscardHandler)
	}
	empty := map[string]publicKey{}
	v.keys.Store(&empty)
	return v
}

// signingMethods maps the allowed algs to their verifiers (P5.2); HMAC and none are absent.
var signingMethods = map[string]jwt.SigningMethod{
	"RS256": jwt.SigningMethodRS256,
	"ES256": jwt.SigningMethodES256,
	"EdDSA": jwt.SigningMethodEdDSA,
}

// Verify checks an Authorization header value and returns the token's claims (P5.1–P5.3, P5.9).
// Every failure matches ErrTokenInvalid and is an *InvalidError naming the failed check. The checks
// run in the order of TOKENS.md; the first failure decides: format, alg allow-list, kid, key lookup
// (one rate-limited refetch), key alg equality, signature, claim types, iss, aud, typ, sub, exp, nbf,
// iat, jti.
func (v *Verifier) Verify(ctx context.Context, authorization string) (*Claims, error) {
	tok, err := bearerToken(authorization)
	if err != nil {
		return nil, err
	}
	j, err := parseJWS(tok)
	if err != nil {
		return nil, err
	}
	alg, kid, err := headerAlgKid(j.header)
	if err != nil {
		return nil, err
	}
	key, ok := v.keyFor(ctx, kid)
	if !ok {
		return nil, invalid(RuleKid, "kid not in the JWKS")
	}
	if key.alg != alg {
		return nil, invalid(RuleAlg, "alg differs from the JWKS key's alg "+key.alg)
	}
	if err := signingMethods[alg].Verify(j.signingInput, j.signature, key.key); err != nil {
		return nil, invalid(RuleSignature, "signature does not verify")
	}
	return checkClaims(j.payload, v.cfg.Issuer, v.cfg.Audience, v.now())
}
