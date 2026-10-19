package authz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/records"
	authzv3 "github.com/brickKit/contract-infra-authz/v3/gen/go/be/authz/v3"
)

// Sealed is one definition record as it is submitted (D76): canonical bytes without a digest, and the digest.
type Sealed struct {
	RecordType, Key string
	Raw             []byte
	Digest          string
}

// Records seals every declared definition with the record envelope (security 09a): the component is the authority,
// the identity is stable for the component, kind and key, and the revision is the component's version. Validate
// must have passed.
func (d *Definitions) Records(deployment, version string) ([]Sealed, error) {
	// the revision domain is a Key: a component ID's "/" and "-" are not allowed in one (test/probe-go2 -> test.probe_go2)
	domain := strings.NewReplacer("/", ".", "-", "_").Replace(d.Component) + ".definition"
	env := func(recordType, kind, key string) map[string]any {
		return map[string]any{"record_type": recordType, "schema": "1.0", "deployment": deployment,
			"authority": d.Component, "id": stableID(d.Component, kind, key),
			"revision": map[string]any{"domain": domain, "value": version}}
	}
	var out []Sealed
	seal := func(recordType, kind, key string, body map[string]any) error {
		rec := env(recordType, kind, key)
		for k, v := range body {
			rec[k] = v
		}
		raw, digest, err := records.Seal(recordType, rec)
		if err != nil {
			return fmt.Errorf("%s %s: %w", kind, key, err)
		}
		out = append(out, Sealed{RecordType: recordType, Key: key, Raw: raw, Digest: digest})
		return nil
	}
	for _, r := range d.Resources {
		if err := seal("be.security.resource_definition", "resource", r.Type, r.Record()); err != nil {
			return nil, err
		}
	}
	for _, o := range d.Operations {
		if err := seal("be.security.operation_definition", "operation", o.Key, o.Record()); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// stableID is a definition record's identity: the same for the same component, kind and key in every version and
// deployment. A UUID with the version 7 and variant bits set over SHA-256 bits (the Id type of security 09a); its
// time field carries no time.
func stableID(parts ...string) string {
	sum := sha256.Sum256([]byte("be.security.definition\n" + strings.Join(parts, "\n")))
	b := sum[:16]
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// RejectedError is a submission the authorization member refused: every reason it gave. Retrying the same
// definitions does not help until the registry or the definitions change.
type RejectedError struct{ Reasons []string }

func (e *RejectedError) Error() string {
	return "the authorization member refused the definitions: " + strings.Join(e.Reasons, "; ")
}

// SetDigest is the digest of a submission (be.security.definition_set): the component, the version and the set of
// the definitions' digests.
func SetDigest(component, version string, sealed []Sealed) (string, error) {
	digests := make([]string, len(sealed))
	for i, s := range sealed {
		digests[i] = s.Digest
	}
	slices.Sort(digests)
	digests = slices.Compact(digests)
	set := make([]any, len(digests))
	for i, d := range digests {
		set[i] = d
	}
	_, digest, err := records.Seal("be.security.definition_set",
		map[string]any{"component": component, "version": version, "definitions": set})
	return digest, err
}

// Submit sends the component's definitions to the authorization member (DefinitionService.Submit) and checks that
// the accepted set is the one sent.
func (d *Definitions) Submit(ctx context.Context, c authzv3.DefinitionServiceClient, deployment, version string) error {
	sealed, err := d.Records(deployment, version)
	if err != nil {
		return err
	}
	want, err := SetDigest(d.Component, version, sealed)
	if err != nil {
		return err
	}
	req := &authzv3.SubmitRequest{Component: d.Component, Version: version}
	for _, s := range sealed {
		req.Definitions = append(req.Definitions, &authzv3.Definition{Record: s.Raw, Digest: s.Digest})
	}
	ans, err := c.Submit(ctx, req, calls.Idempotent())
	if err != nil {
		return err
	}
	switch ans.GetStatus() {
	case authzv3.SubmitStatus_SUBMIT_STATUS_ACCEPTED:
		if ans.GetSetDigest() != want {
			return fmt.Errorf("the authorization member accepted another set of definitions (%s, sent %s)",
				ans.GetSetDigest(), want)
		}
		return nil
	case authzv3.SubmitStatus_SUBMIT_STATUS_REJECTED:
		var reasons []string
		for _, r := range ans.GetRejections() {
			reasons = append(reasons, strings.TrimSpace(r.GetReason()+" "+r.GetDigest()+" "+r.GetDetail()))
		}
		return &RejectedError{Reasons: reasons}
	}
	return errors.New("the authorization member answered the submission without a status")
}
