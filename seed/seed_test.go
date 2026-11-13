package seed

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdkauthz "github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// effectPoint is a stand-in for the authorization member: a root unit, and changes bound to their first result by key.
type effectPoint struct {
	bound   map[string]map[string]any
	changes []map[string]any
}

func (e *effectPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == "POST" && r.URL.Path == "/api/iam/dev/token":
		var in map[string]string
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in["password"] != "pw" {
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"reason":"UNAUTHENTICATED"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "tok-" + in["username"], "principal": "p-" + in["username"]})
	case r.Method == "GET" && r.URL.Path == "/api/authz/records":
		_ = json.NewEncoder(w).Encode(map[string]any{"records": []any{
			map[string]any{"id": "unit-x", "body": map[string]any{"name": "x"}},
			map[string]any{"id": "unit-root", "body": map[string]any{"root": true}}}})
	case r.Method == "POST" && (r.URL.Path == "/api/authz/changes" || r.URL.Path == "/api/authz/tightenings"):
		var in map[string]any
		_ = json.NewDecoder(r.Body).Decode(&in)
		// as the member: a tightening has its own route (infra.authz.security_record.tighten), the changes refuse it
		if (in["direction"] == "tighten") != (r.URL.Path == "/api/authz/tightenings") {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"reason":"REQUEST_INVALID"}`))
			return
		}
		e.changes = append(e.changes, in)
		key := in["change_key"].(string)
		if res, ok := e.bound[key]; ok {
			_ = json.NewEncoder(w).Encode(res)
			return
		}
		res := map[string]any{"change_key": key, "outcome": "applied", "record": "rec-" + key}
		e.bound[key] = res
		_ = json.NewEncoder(w).Encode(res)
	default:
		w.WriteHeader(404)
	}
}

func TestLoginGivesTheTokenAndThePrincipal(t *testing.T) {
	srv := httptest.NewServer(&effectPoint{bound: map[string]map[string]any{}})
	defer srv.Close()
	acc, err := Login(srv.URL, "test.bob", "pw")
	require.NoError(t, err)
	assert.Equal(t, Account{User: "test.bob", Token: "tok-test.bob", Principal: "p-test.bob"}, acc)
	_, err = Login(srv.URL, "test.bob", "wrong")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "make -C components/infra/iam-casdoor seed") // says where the accounts come from
}

func TestChangesAreKeyedByComponentAndLinkedIdentity(t *testing.T) {
	e := &effectPoint{bound: map[string]map[string]any{}}
	srv := httptest.NewServer(e)
	defer srv.Close()
	a, err := NewAuthz(srv.URL, "tok-admin", "mdm/currency")
	require.NoError(t, err)
	assert.Equal(t, "unit-root", a.Root())

	role, err := a.Role("rates", "汇率维护", []sdkauthz.Segment{Segment("mdm.exchange_rate", "mdm.exchange_rate.maintain")})
	require.NoError(t, err)
	assert.Equal(t, "rec-seed:mdm/currency:role:rates", role)
	g1, err := a.GrantRole("rates", "", "p-1", role)
	require.NoError(t, err)
	assert.Equal(t, "rec-seed:mdm/currency:grant:rates:p-1", g1)
	// a second run answers the same records
	again, err := a.GrantRole("rates", "", "p-1", role)
	require.NoError(t, err)
	assert.Equal(t, g1, again)
	// another principal for the same account (the identity member was reset) is another grant
	g2, err := a.GrantRole("rates", "", "p-2", role)
	require.NoError(t, err)
	assert.NotEqual(t, g1, g2)

	// each is security 09a's change request: a widening carries its candidate and the candidate's digest
	last := e.changes[len(e.changes)-1]
	assert.Equal(t, "widen", last["direction"])
	cand := last["candidate"].(map[string]any)
	assert.Equal(t, "grant", cand["record_type"])
	assert.Equal(t, "unit-root", cand["governance_unit"]) // "" is the root
	assert.Equal(t, role, cand["role_revision"])
	assert.Equal(t, "p-2", cand["grantee"])
	raw, _ := json.Marshal(cand)
	digest, err := sdkauthz.CandidateDigest("grant", raw)
	require.NoError(t, err)
	assert.Equal(t, digest, last["candidate_digest"])

	_, err = a.Revoke("old", g1)
	require.NoError(t, err)
	revoke := e.changes[len(e.changes)-1]
	assert.Equal(t, map[string]any{"change_key": "seed:mdm/currency:revoke:old:" + g1, "direction": "tighten", "target": g1}, revoke)
	_, err = a.Unit("east", "", "east", []sdkauthz.Segment{Segment("mdm.exchange_rate", "mdm.exchange_rate.maintain")}, []string{"x"})
	require.NoError(t, err)
	unit := e.changes[len(e.changes)-1]["candidate"].(map[string]any)
	assert.Equal(t, "management_unit", unit["record_type"])
	assert.Equal(t, "unit-root", unit["parent"])
}

func TestSegmentHasNoAtomsAndAFixedStart(t *testing.T) {
	s := Segment("mdm.currency", "mdm.currency.read")
	raw, err := json.Marshal(s)
	require.NoError(t, err)
	assert.JSONEq(t, `{"resource_type":"mdm.currency","operation":"mdm.currency.read","atoms":{},"read_fields":[],
		"valid":{"from":"2026-01-01T00:00:00Z"}}`, string(raw))
	// a resource with dimensions: the named ones hold every value
	raw, err = json.Marshal(Segment("infra.authz.security_record", "infra.authz.security_record.read", "holder"))
	require.NoError(t, err)
	assert.JSONEq(t, `{"resource_type":"infra.authz.security_record","operation":"infra.authz.security_record.read",
		"atoms":{"holder":{"kind":"all"}},"read_fields":[],"valid":{"from":"2026-01-01T00:00:00Z"}}`, string(raw))
}

func TestAMissingRootIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"records":[]}`))
	}))
	defer srv.Close()
	_, err := NewAuthz(srv.URL, "tok", "mdm/org")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "AUTHZ_INSTALL_CLAIM"))
}

func TestWaitGrantedWaitsForTheBodyToBeChecked(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			w.WriteHeader(404) // not visible yet
			return
		}
		w.WriteHeader(400)
	}))
	defer srv.Close()
	require.NoError(t, NewClient(srv.URL, "tok").WaitGranted("POST", "/api/x", 5*time.Second))
	assert.Equal(t, 3, calls)
	calls = -100
	err := NewClient(srv.URL, "tok").WaitGranted("POST", "/api/x", 300*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "404")
}

func TestUntilGrantedSendsTheWriteAgainWhileTheGrantIsMissing(t *testing.T) {
	var calls int
	var keys []string
	never := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		switch {
		case never || calls == 1:
			w.WriteHeader(404) // not visible yet
		case calls == 2:
			w.WriteHeader(403) // visible, not allowed yet
		default:
			_, _ = w.Write([]byte(`{"id":"w1"}`))
		}
	}))
	defer srv.Close()
	code, out, err := NewClient(srv.URL, "tok").UntilGranted(5*time.Second, "POST", "/api/x", map[string]any{"a": 1},
		"Idempotency-Key", "seed:x")
	require.NoError(t, err)
	assert.Equal(t, 200, code)
	assert.Equal(t, "w1", out["id"])
	assert.Equal(t, []string{"seed:x", "seed:x", "seed:x"}, keys, "the same request every time")
	never = true // never granted: the last answer comes back after the timeout
	code, _, err = NewClient(srv.URL, "tok").UntilGranted(300*time.Millisecond, "POST", "/api/x", nil)
	require.NoError(t, err)
	assert.Equal(t, 404, code)
}

// TestDoWaitsOutAnswersThatSayNothingWasDone: a 503 the protocol's catalog marks retryable (not ready, no
// authorization view yet, a dependency down) means the request was not carried out; a seed may meet one right after
// a member restarts (P8.24, P4.27), so the client waits and sends the same request again, up to Patience. Any other
// 503 comes back at once.
func TestDoWaitsOutAnswersThatSayNothingWasDone(t *testing.T) {
	var calls int
	reason := "AUTHORIZATION_VIEW_EXPIRED"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"reason":"` + reason + `","domain":"be","status":503}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"w1"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "tok")
	c.Pause = 10 * time.Millisecond
	out, err := c.Must("POST", "/api/x", map[string]any{"a": 1})
	require.NoError(t, err)
	assert.Equal(t, "w1", out["id"])
	assert.Equal(t, 3, calls)

	calls, reason = 0, "SOMETHING_ELSE"
	code, _, err := c.Do("POST", "/api/x", nil)
	require.NoError(t, err)
	assert.Equal(t, 503, code, "a 503 the catalog does not mark retryable is not sent again")
	assert.Equal(t, 1, calls)

	calls, reason = -1000, "NOT_READY"
	c.Patience = 100 * time.Millisecond
	code, out, err = c.Do("POST", "/api/x", nil)
	require.NoError(t, err)
	assert.Equal(t, 503, code, "after Patience the last answer comes back")
	assert.Equal(t, "NOT_READY", out["reason"])
}

// protected is a stand-in member whose units wait for a confirmation: it answers awaiting_confirmation, gives the
// owner's payload, takes the signed confirmation and then lists the unit as active.
type protected struct {
	pub       ed25519.PublicKey
	confirmed bool
	sig       string
}

func (p *protected) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	payload := sdkauthz.OwnerConfirmation{Deployment: "dep-1", Target: "unit-new", CandidateDigest: "sha256:c",
		ExpiresAt: "2026-11-05T00:00:00.000000Z"}
	switch {
	case r.Method == "GET" && r.URL.Path == "/api/authz/records":
		recs := []any{map[string]any{"id": "unit-root", "body": map[string]any{"root": true}}}
		if p.confirmed {
			recs = append(recs, map[string]any{"id": "unit-new", "body": map[string]any{"name": "x"}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"records": recs})
	case r.Method == "POST" && r.URL.Path == "/api/authz/changes":
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]any{"outcome": "awaiting_confirmation", "record": "unit-new"})
	case r.Method == "GET" && r.URL.Path == "/api/authz/confirmations/owner-payload":
		_ = json.NewEncoder(w).Encode(map[string]any{"deployment": payload.Deployment, "target": r.URL.Query().Get("target"),
			"candidate_digest": payload.CandidateDigest, "expires_at": payload.ExpiresAt})
	case r.Method == "POST" && r.URL.Path == "/api/authz/confirmations":
		var in map[string]string
		_ = json.NewDecoder(r.Body).Decode(&in)
		p.sig = in["signature"]
		if in["basis"] != "deployment_owner" || !payload.Verify(p.pub, in["signature"]) {
			w.WriteHeader(403)
			return
		}
		p.confirmed = true
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "conf-1"})
	default:
		w.WriteHeader(404)
	}
}

func TestAProtectedChangeIsConfirmedWithTheOwnersKey(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	p := &protected{pub: pub}
	srv := httptest.NewServer(p)
	defer srv.Close()
	a, err := NewAuthz(srv.URL, "tok-admin", "mdm/partner")
	require.NoError(t, err)
	_, err = a.Unit("x", "", "x", []sdkauthz.Segment{}, []string{})
	require.Error(t, err, "without the owner's key a protected change stays waiting")
	assert.Contains(t, err.Error(), "SEED_OWNER_KEY")

	unit, err := a.WithOwner(priv).Unit("x", "", "x", []sdkauthz.Segment{}, []string{})
	require.NoError(t, err)
	assert.Equal(t, "unit-new", unit, "confirmed with the owner's signature, then active")
	assert.True(t, p.confirmed)
}

// A read never answers 400 for an empty body: WaitGranted on one waits out its timeout and blames the grant (C10:
// integration/im-dingtalk 3.0.1's seed). It refuses one at once and names UntilGranted.
func TestWaitGrantedRefusesARead(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
	defer srv.Close()
	for _, m := range []string{"GET", "HEAD"} {
		err := NewClient(srv.URL, "tok").WaitGranted(m, "/api/x", 5*time.Second)
		require.ErrorContains(t, err, "UntilGranted")
	}
	assert.Equal(t, 0, calls, "nothing sent")
}
