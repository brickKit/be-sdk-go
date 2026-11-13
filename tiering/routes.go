package tiering

import (
	"net/http"
	"sort"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/httpx"
)

// The tiering's administration on the user surface (be-protocol P15.40): holds and destruction approvals under each
// class's declared operations, and an export's status for its requester under the class reader's operation. Every
// component with classes has the same routes; none writes its own.

// RouteBase is where the routes are under the component's public prefix (P15.40, P11.11).
func RouteBase(prefix string) string { return prefix + "/tiering/" }

type scopeBody struct {
	ScopeKind string `json:"scope_kind"`
	Scope     string `json:"scope"`
	Source    string `json:"source,omitempty"`
	Reason    string `json:"reason"`
}

func subjectOf(c *httpx.Context) (*authz.Subject, error) {
	sub, ok := authz.From(c.Ctx())
	if !ok {
		return nil, errs.New(errs.ReasonTokenInvalid, "no verified subject")
	}
	return sub, nil
}

// Routes registers the administration routes of every class (holds and approvals where the class declares their
// operations) and the export status of every class with a reader: register the readers first (in the module's
// constructor).
func (c *Catalog) Routes(r *httpx.Routes, prefix string) {
	names := make([]string, 0, len(c.Classes))
	for name := range c.Classes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		cl, base := c.Classes[name], RouteBase(prefix)+name
		if cl.HoldOperation != "" {
			r.POST(base+"/holds", cl.HoldOperation, func(hc *httpx.Context) error {
				var b scopeBody
				if err := hc.BindJSON(&b); err != nil {
					return err
				}
				sub, err := subjectOf(hc)
				if err != nil {
					return err
				}
				id, err := c.Hold(hc.Ctx(), sub, name, Scope{Kind: b.ScopeKind, Value: b.Scope}, b.Source, b.Reason)
				if err != nil {
					return err
				}
				return hc.JSON(http.StatusCreated, map[string]string{"id": id})
			})
			r.POST(base+"/holds/:id/lift", cl.HoldOperation, func(hc *httpx.Context) error {
				sub, err := subjectOf(hc)
				if err != nil {
					return err
				}
				if err := c.LiftHold(hc.Ctx(), sub, name, hc.Param("id")); err != nil {
					return err
				}
				return hc.NoContent(http.StatusNoContent)
			})
		}
		if cl.ApproveOperation != "" {
			r.POST(base+"/approvals", cl.ApproveOperation, func(hc *httpx.Context) error {
				var b scopeBody
				if err := hc.BindJSON(&b); err != nil {
					return err
				}
				sub, err := subjectOf(hc)
				if err != nil {
					return err
				}
				id, err := c.Approve(hc.Ctx(), sub, name, Scope{Kind: b.ScopeKind, Value: b.Scope}, b.Reason)
				if err != nil {
					return err
				}
				return hc.JSON(http.StatusCreated, map[string]string{"id": id})
			})
			r.POST(base+"/approvals/:id/withdraw", cl.ApproveOperation, func(hc *httpx.Context) error {
				sub, err := subjectOf(hc)
				if err != nil {
					return err
				}
				if err := c.Withdraw(hc.Ctx(), sub, name, hc.Param("id")); err != nil {
					return err
				}
				return hc.NoContent(http.StatusNoContent)
			})
		}
		if rd, err := c.reader(name); err == nil {
			r.GET(base+"/exports/:id", rd.Op, func(hc *httpx.Context) error {
				sub, err := subjectOf(hc)
				if err != nil {
					return err
				}
				st, err := c.Export(hc.Ctx(), hc.Param("id"), sub.Principal)
				if err != nil {
					return err
				}
				out := map[string]any{"state": st.State, "rows": st.Rows}
				if st.Reason != "" {
					out["reason"] = st.Reason
				}
				if st.URL != "" {
					out["url"], out["expires_at"] = st.URL, st.Expiry.UTC().Format(time.RFC3339)
				}
				return hc.JSON(http.StatusOK, out)
			})
		}
	}
}
