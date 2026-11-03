package calls

import (
	"errors"
	"net/http"
	"net/http/httptest"
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
