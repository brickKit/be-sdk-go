package besdk

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/problem"
	authzv2 "github.com/brickKit/contract-infra-authz/v2/gen/go/infra/authz/v2"
	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// shareWait bounds how long a share write waits for the component's own projection to reach the
// provider's revision before it answers (P6.11).
const shareWait = 3 * time.Second

type shareJSON struct {
	ShareID   string     `json:"share_id"`
	Subject   string     `json:"subject"`
	Relation  string     `json:"relation"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// shareID names one share by its tuple (relation, subject): the projection keeps no other identity.
func shareID(relation, subject string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(relation + "\n" + subject))
}

func parseShareID(id string) (relation, subject string, ok bool) {
	b, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return "", "", false
	}
	return strings.Cut(string(b), "\n")
}

// visibleRecord answers the common start of every _shares call: 501 without the sharing capability,
// 404 for an unknown type, a type without sharing or a record the caller cannot see.
func (rc *resourceContract) visibleRecord(c *gin.Context) (*Access, *authz.ResourceType, []authz.ACLRow, bool) {
	a, _ := AccessFrom(c.Request.Context())
	if a == nil || a.bundle == nil || !a.bundle.Capabilities.Sharing {
		Fail(c, problem.Be("CAPABILITY_UNAVAILABLE", map[string]string{"capability": "sharing"}))
		return nil, nil, nil, false
	}
	typ, id := ResourceType(c.Param("type")), c.Param("id")
	rt, ok := a.resourceType(typ)
	if !ok || rt.Share == nil {
		Fail(c, problem.Be("NOT_FOUND", nil))
		return nil, nil, nil, false
	}
	recs, acls, err := rc.records(c.Request.Context(), typ, []string{id})
	if err != nil {
		Fail(c, err)
		return nil, nil, nil, false
	}
	r, found := recs[id]
	if !found || !a.evaluator().Decide(rt, rt.ViewKey, rowOf(r), acls[id], nil).Visible {
		Fail(c, problem.Be("NOT_FOUND", nil))
		return nil, nil, nil, false
	}
	return a, rt, acls[id], true
}

// listShares is GET _shares/{type}/{id}: the record's shares from the local projection.
func (rc *resourceContract) listShares(c *gin.Context) {
	_, rt, rows, ok := rc.visibleRecord(c)
	if !ok {
		return
	}
	out := []shareJSON{}
	for _, r := range rows {
		if rel, known := rt.Relations[r.Relation]; known && rel.OwnedBy != authz.OwnedByComponent {
			out = append(out, shareJSON{ShareID: shareID(r.Relation, r.Subject), Subject: r.Subject, Relation: r.Relation,
				ExpiresAt: r.ExpiresAt})
		}
	}
	Respond(c, http.StatusOK, map[string]any{"shares": out})
}

type shareRequest struct {
	Subject        string     `json:"subject"`
	Relation       string     `json:"relation"`
	ExpiresAt      *time.Time `json:"expires_at"`
	IdempotencyKey string     `json:"idempotency_key"`
}

// createShare is POST _shares/{type}/{id}: the share key of the type, a relation and subject kind the
// type allows (403 SHARE_NOT_ALLOWED), then WriteTuples at the provider; it answers once the local
// projection reached the returned revision (P6.11).
func (rc *resourceContract) createShare(c *gin.Context) {
	a, rt, _, ok := rc.visibleRecord(c)
	if !ok {
		return
	}
	req, err := Bind[shareRequest](c)
	if err != nil {
		Fail(c, err)
		return
	}
	if !a.Has(PermKey(rt.Share.Key)) {
		Fail(c, problem.Be("MISSING_PERMISSION", map[string]string{"permission": rt.Share.Key}))
		return
	}
	kind, _, _ := strings.Cut(req.Subject, ":")
	if !slices.Contains(rt.Share.Relations, req.Relation) || !slices.Contains(rt.Share.Subjects, kind) {
		Fail(c, problem.Be("SHARE_NOT_ALLOWED", nil))
		return
	}
	key, err := IdempotencyKey(c, req.IdempotencyKey)
	if err != nil {
		Fail(c, err)
		return
	}
	tuple := &authzv2.Tuple{Object: &authzv2.ObjectRef{Type: rt.Type, Id: c.Param("id")}, Relation: req.Relation,
		Subject: req.Subject}
	if req.ExpiresAt != nil {
		tuple.ExpiresAt = timestamppb.New(*req.ExpiresAt)
	}
	rev, err := rc.writeTuples(c.Request.Context(), a, key, []*authzv2.Tuple{tuple}, nil)
	if err != nil {
		Fail(c, err)
		return
	}
	rc.waitProjection(c, rev)
	Respond(c, http.StatusOK, map[string]any{"revision": rev, "share": shareJSON{ShareID: shareID(req.Relation, req.Subject),
		Subject: req.Subject, Relation: req.Relation, ExpiresAt: req.ExpiresAt}})
}

// deleteShare is DELETE _shares/{type}/{id}/{share_id}: revoke one share.
func (rc *resourceContract) deleteShare(c *gin.Context) {
	a, rt, _, ok := rc.visibleRecord(c)
	if !ok {
		return
	}
	if !a.Has(PermKey(rt.Share.Key)) {
		Fail(c, problem.Be("MISSING_PERMISSION", map[string]string{"permission": rt.Share.Key}))
		return
	}
	rel, subject, ok := parseShareID(c.Param("share_id"))
	if !ok {
		Fail(c, problem.Be("NOT_FOUND", nil))
		return
	}
	tuple := &authzv2.Tuple{Object: &authzv2.ObjectRef{Type: rt.Type, Id: c.Param("id")}, Relation: rel, Subject: subject}
	rev, err := rc.writeTuples(c.Request.Context(), a, "", nil, []*authzv2.Tuple{tuple})
	if err != nil {
		Fail(c, err)
		return
	}
	rc.waitProjection(c, rev)
	Respond(c, http.StatusOK, map[string]any{"revision": rev})
}

// writeTuples calls the provider's WriteTuples at AUTHZ_GRPC_URL (P6.10) as the owning component.
func (rc *resourceContract) writeTuples(ctx context.Context, a *Access, key string, writes, deletes []*authzv2.Tuple) (string, error) {
	target := rc.rt.deps.authzGRPC
	if target == "" || rc.rt.deps.out == nil {
		return "", problem.Wrap(errNoAuthzGRPC, "INTERNAL", nil)
	}
	conn, err := rc.rt.deps.out.conns.Get("infra/authz", target)
	if err != nil {
		return "", err
	}
	if key == "" {
		key = envelope.NewID().String()
	}
	res, err := authzv2.NewAuthzProviderClient(conn).WriteTuples(ctx, &authzv2.WriteTuplesRequest{Writes: writes,
		Deletes: deletes, IdempotencyKey: key, Source: rc.rt.id,
		Actor: &authzv2.Actor{Sub: a.user.Sub, Kind: authzv2.ActorKind_ACTOR_KIND_USER}})
	if err != nil {
		return "", problem.From(err)
	}
	return res.GetRevision(), nil
}

var errNoAuthzGRPC = errors.New("besdk: sharing needs AUTHZ_GRPC_URL in configSchema (P6.10)")

// waitProjection waits, within shareWait, for the local projection to reach rev; behind, the answer
// carries X-Authz-Consistency: stale (P6.11).
func (rc *resourceContract) waitProjection(c *gin.Context, rev string) {
	p := rc.rt.deps.projection
	n, err := strconv.ParseInt(rev, 10, 64)
	if p == nil || err != nil {
		return
	}
	if !p.WaitForWithin(c.Request.Context(), n, shareWait) {
		c.Header("X-Authz-Consistency", "stale")
	}
}
