package authz

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"

	"github.com/brickKit/be-sdk-go/v3/canon"
)

// OwnerConfirmation is what the deployment owner signs to confirm a protected change (security 08, "只有一名管理员时";
// the assembly project's A124): the object — a saved widening's candidate digest, or a tightening's target at a
// content revision — and how long it holds, in this deployment. The owner's private key is kept offline; the
// authorization member checks the signature with the public key it is configured with. Nothing secret crosses the
// interface, and a signature binds exactly this object.
type OwnerConfirmation struct {
	Deployment, Target string
	CandidateDigest    string // a saved widening
	ExpectedRevision   string // a tightening: the target's content revision
	ExpiresAt          string // an instant in canonical form (canon.Time)
}

// Digest is the confirmation's canonical form under be.security.owner_confirmation, v1: what is signed.
func (o OwnerConfirmation) Digest() (string, error) {
	if (o.CandidateDigest == "") == (o.ExpectedRevision == "") {
		return "", errors.New("an owner confirmation names exactly one of a candidate digest and a revision")
	}
	v := map[string]any{"deployment": o.Deployment, "target": o.Target, "expires_at": o.ExpiresAt}
	if o.CandidateDigest != "" {
		v["candidate_digest"] = o.CandidateDigest
	} else {
		v["expected_revision"] = o.ExpectedRevision
	}
	return canon.Digest("be.security.owner_confirmation", 1, v)
}

// Sign is the owner's signature: Ed25519 over the digest's bytes, base64 (standard, padded).
func (o OwnerConfirmation) Sign(key ed25519.PrivateKey) (string, error) {
	d, err := o.Digest()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ed25519.Sign(key, []byte(d))), nil
}

// Verify checks a signature against the owner's public key.
func (o OwnerConfirmation) Verify(key ed25519.PublicKey, signature string) bool {
	d, err := o.Digest()
	if err != nil {
		return false
	}
	sig, err := base64.StdEncoding.DecodeString(signature)
	return err == nil && len(key) == ed25519.PublicKeySize && ed25519.Verify(key, []byte(d), sig)
}
