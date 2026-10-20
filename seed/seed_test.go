package seed

import (
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
	case r.Method == "POST" && r.URL.Path == "/api/authz/changes":
		var in map[string]any
		_ = json.NewDecoder(r.Body).Decode(&in)
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

	last := e.changes[len(e.changes)-1]
	assert.Equal(t, "grant.create", last["action"])
	assert.Equal(t, "unit-root", last["unit"]) // "" is the root
	assert.Equal(t, role, last["role_revision"])
}

func TestSegmentHasNoAtomsAndAFixedStart(t *testing.T) {
	s := Segment("mdm.currency", "mdm.currency.read")
	raw, err := json.Marshal(s)
	require.NoError(t, err)
	assert.JSONEq(t, `{"resource_type":"mdm.currency","operation":"mdm.currency.read","atoms":{},"read_fields":[],
		"valid":{"from":"2026-01-01T00:00:00Z"}}`, string(raw))
}

func TestAMissingRootIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"records":[]}`))
	}))
	defer srv.Close()
	_, err := NewAuthz(srv.URL, "tok", "mdm/org")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "AUTHZ_BOOTSTRAP_ADMIN"))
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
