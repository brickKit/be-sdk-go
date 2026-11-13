package calls

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnExternalErrorKeepsItsStatusAndBody: a non-2xx answer without a protocol error body (an external product's
// own) fails the call as before, and the caller can read the status and body.
func TestAnExternalErrorKeepsItsStatusAndBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()
	m, err := NewMember(nil)
	require.NoError(t, err)
	err = m.HTTP("casdoor", srv.URL).Do(withDeadline(t, 2e9), Request{Method: "POST", Path: "/token", Body: map[string]string{}}, nil)
	require.Error(t, err)
	var se *StatusError
	require.True(t, errors.As(err, &se), "%v", err)
	assert.Equal(t, 400, se.Status)
	assert.JSONEq(t, `{"error":"invalid_grant"}`, string(se.Body))
	assert.Equal(t, errs.ReasonInternal, errs.From(err).Reason, "unchanged for callers that do not look")
}

// P8.26, CK-ID-06: a downstream 401 to a call made for the user is answered as AUTHORIZATION_UNPROVABLE (503,
// retryable), so the edge never replays a request the entry component may have partly done; a 401 to a call made
// without the user's credential is left as it is.
func TestADownstream401ForTheUserIsUnprovable(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get("Authorization"))
		errs.New(r.URL.Query().Get("reason"), "refused").WriteHTTP(w, "")
	}))
	defer srv.Close()
	m, err := NewMember(nil)
	require.NoError(t, err)
	user := reqctx.WithUserCredential(withDeadline(t, 2e9), "Bearer the-users-token")
	for _, reason := range []string{errs.ReasonTokenStale, errs.ReasonTokenInvalid} {
		err := m.HTTP("erp/sales", srv.URL).Do(user, Request{Method: "GET", Path: "/api/x?reason=" + reason, ForwardUser: true}, nil)
		e := errs.From(err)
		assert.Equal(t, errs.ReasonAuthorizationUnprovable, e.Reason, reason)
		assert.Equal(t, 503, errs.HTTPStatus(e.Code), reason)
		assert.Equal(t, "erp/sales", e.Metadata["dependency"], reason)
	}
	assert.Equal(t, []string{"Bearer the-users-token", "Bearer the-users-token"}, got, "precondition: the user's credential was sent")
	err = m.HTTP("erp/sales", srv.URL).Do(user, Request{Method: "GET", Path: "/api/x?reason=" + errs.ReasonTokenInvalid}, nil)
	assert.Equal(t, errs.ReasonTokenInvalid, errs.From(err).Reason, "not for the user: unchanged")
	err = m.HTTP("erp/sales", srv.URL).Do(user, Request{Method: "GET", Path: "/api/x?reason=" + errs.ReasonPermissionDenied, ForwardUser: true}, nil)
	assert.Equal(t, errs.ReasonPermissionDenied, errs.From(err).Reason, "other refusals pass through")
}

// TI06: a call for the user without the user's credential fails; it is never sent as the service.
func TestForwardingWithoutAUserCredentialIsNotSent(t *testing.T) {
	sent := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { sent = true }))
	defer srv.Close()
	m, err := NewMember(nil)
	require.NoError(t, err)
	err = m.HTTP("erp/sales", srv.URL).Do(withDeadline(t, 2e9), Request{Method: "GET", Path: "/api/x", ForwardUser: true}, nil)
	assert.ErrorContains(t, err, "without a user credential")
	assert.False(t, sent)
}

// A form is sent form-encoded in the body (an OAuth token endpoint, an identity product's password change): the
// values never travel in the URL, where access logs keep them.
func TestAFormIsSentFormEncoded(t *testing.T) {
	var ctype, query string
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctype, query = r.Header.Get("Content-Type"), r.URL.RawQuery
		require.NoError(t, r.ParseForm())
		got = r.PostForm
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	m, err := NewMember(nil)
	require.NoError(t, err)
	form := url.Values{"userName": {"alice"}, "newPassword": {"s3cret & more"}}
	require.NoError(t, m.HTTP("casdoor", srv.URL).Do(withDeadline(t, 2e9),
		Request{Method: "POST", Path: "/api/set-password", Form: form}, nil))
	assert.Equal(t, "application/x-www-form-urlencoded", ctype)
	assert.Empty(t, query)
	assert.Equal(t, form, got)
	err = m.HTTP("casdoor", srv.URL).Do(withDeadline(t, 2e9),
		Request{Method: "POST", Path: "/x", Form: form, Body: map[string]string{}}, nil)
	assert.ErrorContains(t, err, "both a body and a form")
}

// An external product's own API credential (Casdoor's client credentials as Basic) goes in the Authorization header,
// never in the URL where access logs keep it; it cannot be combined with forwarding the user's credential.
func TestAnExternalCredentialGoesInTheHeader(t *testing.T) {
	var auth, query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, query = r.Header.Get("Authorization"), r.URL.RawQuery
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	m, err := NewMember(nil)
	require.NoError(t, err)
	require.NoError(t, m.HTTP("casdoor", srv.URL).Do(withDeadline(t, 2e9),
		Request{Method: "GET", Path: "/api/get-users?owner=o", Credential: "Basic Y2xpZW50OnNlY3JldA=="}, nil))
	assert.Equal(t, "Basic Y2xpZW50OnNlY3JldA==", auth)
	assert.Equal(t, "owner=o", query)
	err = m.HTTP("casdoor", srv.URL).Do(withDeadline(t, 2e9),
		Request{Method: "GET", Path: "/x", Credential: "Basic x", ForwardUser: true}, nil)
	assert.ErrorContains(t, err, "both")
}

// An external product's own headers are sent; a connection that fails before sending is told apart (ErrNotSent) from
// one that fails after (be-protocol P13.5).
func TestExternalHeadersAndNotSent(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("x-acs-dingtalk-access-token")
		_, _ = w.Write([]byte(`{}`))
	}))
	m, err := NewMember(nil)
	require.NoError(t, err)
	require.NoError(t, m.HTTP("dingtalk", srv.URL).Do(withDeadline(t, 2e9),
		Request{Method: "POST", Path: "/x", Body: map[string]string{}, Headers: map[string]string{"x-acs-dingtalk-access-token": "t"}}, nil))
	assert.Equal(t, "t", got)
	srv.Close()
	// An address this member never connected to (a kept-alive connection to the closed server would fail after
	// sending, with EOF: not ErrNotSent), where nothing listens: the dial fails.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	url := "http://" + l.Addr().String()
	require.NoError(t, l.Close())
	err = m.HTTP("dingtalk2", url).Do(withDeadline(t, 2e9), Request{Method: "POST", Path: "/x", Body: map[string]string{}}, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotSent)
}

// A request's query may carry an external product's own credential (an old API's access token or application
// secret): a transport error never repeats it, so it cannot reach a log through the error (C10 exam M1).
func TestATransportErrorNeverRepeatsTheQuery(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	url := "http://" + l.Addr().String()
	require.NoError(t, l.Close())
	m, err := NewMember(nil)
	require.NoError(t, err)
	err = m.HTTP("dingtalk-oapi", url).Do(withDeadline(t, 2e9), Request{Method: "GET", Path: "/gettoken?appkey=k&appsecret=s3cr3t"}, nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "s3cr3t")
	assert.NotContains(t, err.Error(), "appsecret")
	assert.ErrorIs(t, err, ErrNotSent, "still told apart")
}
