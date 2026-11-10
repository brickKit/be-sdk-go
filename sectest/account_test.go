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

// The fake authorization member answers CheckLinkChange (a confirmation given: allowed; none: required; or as the
// test sets it) and remembers the requests; Administrators answers the accounts the test made admins.
func TestTheFakeMemberAnswersLinkChecksAndAdministrators(t *testing.T) {
	w := New(t)
	tls := w.TLS(t, "mdm/employee")
	conn, err := grpc.NewClient(w.AuthzAddr, grpc.WithTransportCredentials(credentials.NewTLS(tls.ClientConfig("infra/authz"))))
	require.NoError(t, err)
	defer conn.Close()
	c := authzv3.NewAccountServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := c.CheckLinkChange(ctx, &authzv3.CheckLinkChangeRequest{Employee: "e1", Account: Bob, Action: "link"})
	require.NoError(t, err)
	assert.Equal(t, authzv3.AccountChangeVerdict_ACCOUNT_CHANGE_VERDICT_CONFIRMATION_REQUIRED, out.GetVerdict())
	out, err = c.CheckLinkChange(ctx, &authzv3.CheckLinkChangeRequest{Employee: "e1", Account: Bob, Action: "link", Confirmation: "c1"})
	require.NoError(t, err)
	assert.Equal(t, authzv3.AccountChangeVerdict_ACCOUNT_CHANGE_VERDICT_ALLOWED, out.GetVerdict())
	w.LinkCheck(func(*authzv3.CheckLinkChangeRequest) *authzv3.CheckLinkChangeResponse {
		return &authzv3.CheckLinkChangeResponse{Verdict: authzv3.AccountChangeVerdict_ACCOUNT_CHANGE_VERDICT_REFUSED, Reason: "used"}
	})
	out, err = c.CheckLinkChange(ctx, &authzv3.CheckLinkChangeRequest{Employee: "e1", Account: Bob, Action: "unlink", Confirmation: "c2"})
	require.NoError(t, err)
	assert.Equal(t, authzv3.AccountChangeVerdict_ACCOUNT_CHANGE_VERDICT_REFUSED, out.GetVerdict())
	require.Len(t, w.LinkChecks(), 3)
	assert.Equal(t, "unlink", w.LinkChecks()[2].GetAction())

	admins, err := c.Administrators(ctx, &authzv3.AdministratorsRequest{Accounts: []string{Alice, Bob}})
	require.NoError(t, err)
	assert.Empty(t, admins.GetAdministrators())
	w.Admins(Alice)
	admins, err = c.Administrators(ctx, &authzv3.AdministratorsRequest{Accounts: []string{Alice, Bob}})
	require.NoError(t, err)
	assert.Equal(t, []string{Alice}, admins.GetAdministrators())
}
