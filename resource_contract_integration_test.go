package besdk

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc"
	authzv2 "github.com/brickKit/contract-infra-authz/v2/gen/go/infra/authz/v2"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

// accessGuardian lets every protected route through with a fixed Access (the P6.2 chain is tested
// elsewhere).
type accessGuardian struct{ a *Access }

func (g accessGuardian) check(c *gin.Context, gd Guard) *problem.Error {
	if guardKey(gd) != Public {
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), accessKey{}, g.a))
	}
	return nil
}

// fakeProvider records WriteTuples.
type fakeProvider struct {
	authzv2.UnimplementedAuthzProviderServer
	got []*authzv2.WriteTuplesRequest
}

func (f *fakeProvider) WriteTuples(_ context.Context, r *authzv2.WriteTuplesRequest) (*authzv2.WriteTuplesResponse, error) {
	f.got = append(f.got, r)
	return &authzv2.WriteTuplesResponse{Revision: "8"}, nil
}

func contractEngine(t *testing.T, rt *Runtime, a *Access, things map[string]thing) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	eng := gin.New()
	r := newRouter(eng, routerConfig{componentID: "test/thing", catalogue: problem.NewCatalogue(), locale: "en",
		defaultDeadline: 10 * time.Second, guardian: accessGuardian{a}})
	loader := SharingLoader{Type: "test.thing.thing", Load: func(_ context.Context, _ *Tx, ids []string) (map[string]Resource, error) {
		out := map[string]Resource{}
		for _, id := range ids {
			if th, ok := things[id]; ok {
				out[id] = th
			}
		}
		return out, nil
	}}
	mountResourceContract(r, rt, []SharingLoader{loader})
	return eng
}

func rcCall(t *testing.T, eng *gin.Engine, method, path, body string) (int, string) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	eng.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

// P6.10, CP-SCOPE-12…14: _authz/check, _authz/explain and _shares on a component that declares
// resources; an invisible record answers 404, an absent capability 501.
func TestResourceContract(t *testing.T) {
	rt, _ := accessRuntime(t)
	alice := accessFor(t, rt, "alice", Authenticated)
	things := map[string]thing{"t1": {ID: "t1", Owner: "alice"}, "t2": {ID: "t2", Owner: "bob"}}
	eng := contractEngine(t, rt, alice, things)

	code, body := rcCall(t, eng, "POST", "/test/thing/_authz/check", `{"checks":[
	  {"key":"test.thing.edit","type":"test.thing.thing","id":"t1"},
	  {"key":"test.thing.edit","type":"test.thing.thing","id":"t2"},
	  {"key":"test.thing.edit","type":"test.thing.thing","id":"nope"}]}`)
	var res struct{ Results []Decision }
	if code != 200 || json.Unmarshal([]byte(body), &res) != nil || len(res.Results) != 3 ||
		!res.Results[0].Allowed || res.Results[1].Reason != "NOT_FOUND" || res.Results[2].Reason != "NOT_FOUND" {
		t.Fatalf("check: %d %s", code, body)
	}
	many := `{"checks":[` + strings.Repeat(`{"key":"k","type":"t","id":"i"},`, 500) + `{"key":"k","type":"t","id":"i"}]}`
	if code, body := rcCall(t, eng, "POST", "/test/thing/_authz/check", many); code != 400 || !strings.Contains(body, "BATCH_TOO_LARGE") {
		t.Fatalf("501 checks: %d %s", code, body)
	}

	code, body = rcCall(t, eng, "GET", "/test/thing/_authz/explain?key=test.thing.edit&type=test.thing.thing&id=t1", "")
	if code != 200 || !strings.Contains(body, `"decision":"allowed"`) || !strings.Contains(body, `"role_key"`) {
		t.Fatalf("explain: %d %s", code, body)
	}
	if code, body := rcCall(t, eng, "GET", "/test/thing/_authz/explain?key=test.thing.edit&type=test.thing.thing&id=t2", ""); code != 200 || !strings.Contains(body, `"decision":"not_visible"`) {
		t.Fatalf("explain invisible: %d %s", code, body)
	}

	if code, _ := rcCall(t, eng, "GET", "/test/thing/_shares/test.thing.thing/t2", ""); code != 404 {
		t.Fatalf("shares of an invisible record: %d", code)
	}
	if code, body := rcCall(t, eng, "GET", "/test/thing/_shares/test.thing.thing/t1", ""); code != 200 || !strings.Contains(body, `"shares":[]`) {
		t.Fatalf("shares: %d %s", code, body)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	fp := &fakeProvider{}
	authzv2.RegisterAuthzProviderServer(srv, fp)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	rt.deps.authzGRPC = ln.Addr().String()
	rt.deps.out = &outbound{conns: rpc.NewConns(rpc.ClientConfig{CallerID: "test/thing"})}
	code, body = rcCall(t, eng, "POST", "/test/thing/_shares/test.thing.thing/t1", `{"subject":"user:carol","relation":"viewer"}`)
	if code != 200 || !strings.Contains(body, `"revision":"8"`) || len(fp.got) != 1 || fp.got[0].Writes[0].Subject != "user:carol" {
		t.Fatalf("share: %d %s %v", code, body, fp.got)
	}
	if code, body := rcCall(t, eng, "POST", "/test/thing/_shares/test.thing.thing/t1", `{"subject":"user:carol","relation":"owner"}`); code != 403 || !strings.Contains(body, "SHARE_NOT_ALLOWED") {
		t.Fatalf("share with an undeclared relation: %d %s", code, body)
	}

	off := accessFor(t, rt, "alice", Authenticated)
	off.bundle = mustBundle(t, strings.Replace(thingBundle, `"sharing": true`, `"sharing": false`, 1))
	eng = contractEngine(t, rt, off, things)
	if code, body := rcCall(t, eng, "GET", "/test/thing/_shares/test.thing.thing/t1", ""); code != 501 || !strings.Contains(body, `"capability":"sharing"`) {
		t.Fatalf("sharing off: %d %s", code, body)
	}
	_ = http.StatusOK
}

func mustBundle(t *testing.T, raw string) *authz.Bundle {
	b, err := authz.ParseBundle([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
