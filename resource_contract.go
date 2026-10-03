package besdk

import (
	"context"
	"net/http"
	"strconv"

	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/authz/acl"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/gin-gonic/gin"
)

// maxChecks is _authz/check's batch limit (P6.10).
const maxChecks = 500

// SharingLoader loads the records of one resource type for the resource contract (P6.10): every id it
// finds, inside tx; a missing id is simply absent from the map.
type SharingLoader struct {
	Type ResourceType
	Load func(ctx context.Context, tx *Tx, ids []string) (map[string]Resource, error)
}

// resourceContract serves /{d}/{n}/_authz/* and /_shares/* (P6.10, openapi/resource-authz.yaml).
type resourceContract struct {
	rt      *Runtime
	loaders map[ResourceType]SharingLoader
}

// mountResourceContract mounts the resource contract on r when the component declares resource
// types; each endpoint is Authenticated, the share key is checked per type.
func mountResourceContract(r *Router, rt *Runtime, loaders []SharingLoader) {
	if rt.authzCatalog == nil || len(rt.authzCatalog.Types()) == 0 {
		return
	}
	rc := &resourceContract{rt: rt, loaders: map[ResourceType]SharingLoader{}}
	for _, l := range loaders {
		rc.loaders[l.Type] = l
	}
	POST(r, "/_authz/check", Authenticated, rc.check)
	GET(r, "/_authz/explain", Authenticated, rc.explain)
	GET(r, "/_shares/:type/:id", Authenticated, rc.listShares)
	POST(r, "/_shares/:type/:id", Authenticated, rc.createShare)
	DELETE(r, "/_shares/:type/:id/:share_id", Authenticated, rc.deleteShare)
}

// records loads the records and their ACL rows of one type in one read-only transaction.
func (rc *resourceContract) records(ctx context.Context, t ResourceType, ids []string) (map[string]Resource, map[string][]authz.ACLRow, error) {
	recs := map[string]Resource{}
	var acls map[string][]authz.ACLRow
	l, ok := rc.loaders[t]
	if !ok || rc.rt.deps.store == nil {
		return recs, map[string][]authz.ACLRow{}, nil
	}
	err := rc.rt.deps.store.Run(ctx, pg.TxOptions{ReadOnly: true}, func(ctx context.Context, tx *pg.Tx) error {
		var err error
		if recs, err = l.Load(ctx, &Tx{Tx: tx, rt: rc.rt}, ids); err != nil {
			return err
		}
		acls, err = acl.LoadMany(ctx, tx, string(t), ids)
		return err
	})
	return recs, acls, err
}

type checkItem struct {
	Key  string `json:"key"`
	Type string `json:"type"`
	ID   string `json:"id"`
}

type decisionJSON struct {
	Visible bool   `json:"visible"`
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

// check is POST _authz/check: up to 500 (key, type, id) decisions for the caller (E10).
func (rc *resourceContract) check(c *gin.Context) {
	req, err := Bind[struct {
		Checks []checkItem `json:"checks"`
	}](c)
	if err != nil {
		Fail(c, err)
		return
	}
	if n := len(req.Checks); n > maxChecks {
		Fail(c, problem.Be("BATCH_TOO_LARGE", map[string]string{"field": "checks", "max": strconv.Itoa(maxChecks), "got": strconv.Itoa(n)}))
		return
	}
	a, _ := AccessFrom(c.Request.Context())
	byType := map[string][]string{}
	for _, ch := range req.Checks {
		byType[ch.Type] = append(byType[ch.Type], ch.ID)
	}
	type loaded struct {
		recs map[string]Resource
		acls map[string][]authz.ACLRow
	}
	all := map[string]loaded{}
	for typ, ids := range byType {
		recs, acls, err := rc.records(c.Request.Context(), ResourceType(typ), ids)
		if err != nil {
			Fail(c, err)
			return
		}
		all[typ] = loaded{recs, acls}
	}
	out := make([]decisionJSON, len(req.Checks))
	for i, ch := range req.Checks {
		rt, ok := a.resourceType(ResourceType(ch.Type))
		r, found := all[ch.Type].recs[ch.ID]
		if !ok || !found {
			out[i] = decisionJSON{Reason: authz.ReasonNotFound}
			continue
		}
		d := a.evaluator().Decide(rt, ch.Key, rowOf(r), all[ch.Type].acls[ch.ID], nil)
		out[i] = decisionJSON{Visible: d.Visible, Allowed: d.Allowed, Reason: d.Reason}
	}
	Respond(c, http.StatusOK, map[string]any{"results": out})
}

// explain is GET _authz/explain: the decision and its facts (E12). A record that does not exist and
// one the caller cannot see answer alike: not_visible, with only the caller's own side (R62).
func (rc *resourceContract) explain(c *gin.Context) {
	key, typ, id := c.Query("key"), c.Query("type"), c.Query("id")
	if key == "" || typ == "" || id == "" {
		Fail(c, WithViolations(problem.Be("REQUEST_INVALID", nil),
			FieldViolation{Field: "key,type,id", Reason: "REQUIRED", Description: "key, type and id are required"}))
		return
	}
	a, _ := AccessFrom(c.Request.Context())
	rt, ok := a.resourceType(ResourceType(typ))
	if !ok {
		Fail(c, problem.Be("NOT_FOUND", nil))
		return
	}
	recs, acls, err := rc.records(c.Request.Context(), ResourceType(typ), []string{id})
	if err != nil {
		Fail(c, err)
		return
	}
	row := authz.Row{ID: id}
	if r, found := recs[id]; found {
		row = rowOf(r)
	}
	d := a.evaluator().Decide(rt, key, row, acls[id], nil)
	ex := a.evaluator().Explain(rt, key, row, acls[id], nil)
	decision := "not_visible"
	switch {
	case d.Allowed:
		decision = "allowed"
	case d.Visible:
		decision = "visible"
	}
	Respond(c, http.StatusOK, map[string]any{"decision": decision, "reasons": ex.Reasons, "missing": ex.Missing})
}
