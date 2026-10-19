package calls

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/errs"
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
