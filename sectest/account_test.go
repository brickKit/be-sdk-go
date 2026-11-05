package sectest

import (
	"context"
	"testing"
	"time"

	authzv3 "github.com/brickKit/contract-infra-authz/v3/gen/go/be/authz/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// The fake authorization member answers CheckAccountChange as the test sets it, allowed by default, and remembers
// every request (an identity member's integration test checks what it asked).
func TestTheFakeMemberAnswersAccountChecks(t *testing.T) {
	w := New(t)
	tls := w.TLS(t, "infra/iam-casdoor")
	conn, err := grpc.NewClient(w.AuthzAddr, grpc.WithTransportCredentials(credentials.NewTLS(tls.ClientConfig("infra/authz"))))
	require.NoError(t, err)
	defer conn.Close()
	c := authzv3.NewAccountServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := c.CheckAccountChange(ctx, &authzv3.CheckAccountChangeRequest{Account: Bob, TargetState: "disabled"})
	require.NoError(t, err)
	assert.Equal(t, authzv3.AccountChangeVerdict_ACCOUNT_CHANGE_VERDICT_ALLOWED, out.GetVerdict())
	w.AccountCheck(func(in *authzv3.CheckAccountChangeRequest) *authzv3.CheckAccountChangeResponse {
		return &authzv3.CheckAccountChangeResponse{Verdict: authzv3.AccountChangeVerdict_ACCOUNT_CHANGE_VERDICT_CONFIRMATION_REQUIRED,
			Administrator: in.GetAccount() == Alice}
	})
	out, err = c.CheckAccountChange(ctx, &authzv3.CheckAccountChangeRequest{Account: Alice, TargetState: "security_frozen"})
	require.NoError(t, err)
	assert.Equal(t, authzv3.AccountChangeVerdict_ACCOUNT_CHANGE_VERDICT_CONFIRMATION_REQUIRED, out.GetVerdict())
	assert.True(t, out.GetAdministrator())
	got := w.AccountChecks()
	require.Len(t, got, 2)
	assert.Equal(t, Bob, got[0].GetAccount())
	assert.Equal(t, "security_frozen", got[1].GetTargetState())
}
