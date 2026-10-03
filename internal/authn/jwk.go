package authn

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
)

// minRSABits is the iam contract's minimum RSA size (TOKENS.md "Signing keys").
const minRSABits = 2048

// publicKey is one usable JWKS key: the alg it signs with and the Go public key.
type publicKey struct {
	alg string
	key crypto.PublicKey
}

// jwk is the subset of RFC 7517 members a verifier reads.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	Crv string `json:"crv"`
	N   string `json:"n"`
	E   string `json:"e"`
	X   string `json:"x"`
	Y   string `json:"y"`
	D   string `json:"d"`
}

// parseJWKS reads a JWKS document (contract-infra-iam schemas/jwks.schema.json) into kid → key.
// A key that does not conform is skipped: no alg (the contract requires it, and P5.2 compares the
// token's alg with it), alg not fitting kty/crv, use other than sig, a private member, RSA below 2048
// bits, an EC point off P-256. The first of duplicate kids wins. No usable key at all is an error.
func parseJWKS(body []byte) (map[string]publicKey, error) {
	var doc struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("jwks: %w", err)
	}
	out := make(map[string]publicKey, len(doc.Keys))
	for _, raw := range doc.Keys {
		var k jwk
		if json.Unmarshal(raw, &k) != nil || k.Kid == "" || k.D != "" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		if _, dup := out[k.Kid]; dup {
			continue
		}
		pub, err := k.publicKey()
		if err != nil {
			continue
		}
		out[k.Kid] = publicKey{alg: k.Alg, key: pub}
	}
	if len(out) == 0 {
		return nil, errors.New("jwks: no usable key")
	}
	return out, nil
}

// publicKey builds the Go key for the three allowed kty/alg pairs.
func (k jwk) publicKey() (crypto.PublicKey, error) {
	switch {
	case k.Kty == "RSA" && k.Alg == "RS256":
		return k.rsa()
	case k.Kty == "EC" && k.Alg == "ES256" && k.Crv == "P-256":
		x, err1 := b64url.DecodeString(k.X)
		y, err2 := b64url.DecodeString(k.Y)
		if err1 != nil || err2 != nil || len(x) != 32 || len(y) != 32 {
			return nil, errors.New("jwk: bad EC coordinates")
		}
		return ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(append([]byte{4}, x...), y...))
	case k.Kty == "OKP" && k.Alg == "EdDSA" && k.Crv == "Ed25519":
		x, err := b64url.DecodeString(k.X)
		if err != nil || len(x) != ed25519.PublicKeySize {
			return nil, errors.New("jwk: bad Ed25519 key")
		}
		return ed25519.PublicKey(x), nil
	default:
		return nil, fmt.Errorf("jwk: unsupported kty %q / alg %q / crv %q", k.Kty, k.Alg, k.Crv)
	}
}

func (k jwk) rsa() (*rsa.PublicKey, error) {
	n, err1 := b64url.DecodeString(k.N)
	e, err2 := b64url.DecodeString(k.E)
	if err1 != nil || err2 != nil || len(e) == 0 || len(e) > 4 {
		return nil, errors.New("jwk: bad RSA members")
	}
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	if pub.N.BitLen() < minRSABits || pub.E < 3 || pub.E%2 == 0 {
		return nil, errors.New("jwk: RSA key too small or bad exponent")
	}
	return pub, nil
}
