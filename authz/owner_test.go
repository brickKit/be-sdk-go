package authz

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOwnerConfirmationSignsItsObject(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	o := OwnerConfirmation{Deployment: "dep-1", Target: "rec-1", ExpectedRevision: "3", ExpiresAt: "2026-11-05T00:00:00.000000Z"}
	want, err := canon.Digest("be.security.owner_confirmation", 1, map[string]any{"deployment": "dep-1", "target": "rec-1",
		"expected_revision": "3", "expires_at": "2026-11-05T00:00:00.000000Z"})
	require.NoError(t, err)
	got, err := o.Digest()
	require.NoError(t, err)
	assert.Equal(t, want, got, "the canonical form under be.security.owner_confirmation, v1")

	sig, err := o.Sign(priv)
	require.NoError(t, err)
	assert.True(t, o.Verify(pub, sig))
	for name, other := range map[string]OwnerConfirmation{
		"another deployment": {Deployment: "dep-2", Target: "rec-1", ExpectedRevision: "3", ExpiresAt: o.ExpiresAt},
		"another target":     {Deployment: "dep-1", Target: "rec-2", ExpectedRevision: "3", ExpiresAt: o.ExpiresAt},
		"another revision":   {Deployment: "dep-1", Target: "rec-1", ExpectedRevision: "4", ExpiresAt: o.ExpiresAt},
		"a later expiry":     {Deployment: "dep-1", Target: "rec-1", ExpectedRevision: "3", ExpiresAt: "2026-11-06T00:00:00.000000Z"},
		"a widening instead": {Deployment: "dep-1", Target: "rec-1", CandidateDigest: "sha256:x", ExpiresAt: o.ExpiresAt},
	} {
		assert.False(t, other.Verify(pub, sig), "the signature binds its object: %s", name)
	}
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	assert.False(t, o.Verify(otherPub, sig), "another key")
	assert.False(t, o.Verify(pub, "not base64!"), "not a signature")

	_, err = OwnerConfirmation{Deployment: "dep-1", Target: "rec-1", ExpiresAt: o.ExpiresAt}.Digest()
	assert.Error(t, err, "exactly one of a candidate digest and a revision")
	_, err = OwnerConfirmation{Deployment: "dep-1", Target: "rec-1", CandidateDigest: "d", ExpectedRevision: "1", ExpiresAt: o.ExpiresAt}.Digest()
	assert.Error(t, err)
}
