package pg

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/problem"
	gomigrate "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestInTxMarker(t *testing.T) {
	ctx := context.Background()
	require.False(t, InTx(ctx))
	require.True(t, InTx(markInTx(ctx)))
}

func TestGuardNetwork(t *testing.T) {
	require.NoError(t, GuardNetwork(context.Background(), "grpc erp/inventory"))
	err := problem.Catch(func() error { return GuardNetwork(markInTx(context.Background()), "grpc erp/inventory") })
	var pe *problem.Error
	require.ErrorAs(t, err, &pe)
	require.Equal(t, "NETWORK_IN_TX", pe.Reason)
	require.Equal(t, codes.Internal, pe.Code)
	require.Contains(t, pe.Cause.Error(), "grpc erp/inventory")
}

func TestSQLStateHelpers(t *testing.T) {
	unique := fmt.Errorf("insert: %w", &pgconn.PgError{Code: "23505"})
	lock := &pgconn.PgError{Code: "55P03"}
	require.Equal(t, "23505", SQLState(unique))
	require.Equal(t, "", SQLState(errors.New("plain")))
	require.Equal(t, "", SQLState(nil))
	require.True(t, IsUniqueViolation(unique))
	require.False(t, IsUniqueViolation(lock))
	require.True(t, IsLockTimeout(lock))
	require.True(t, IsLockTimeout(problem.Be("LOCK_TIMEOUT", nil)), "a classified lock timeout counts too")
	require.False(t, IsLockTimeout(unique))
	// golang-migrate's database.Error has no Unwrap; the state is still found
	require.Equal(t, "55P03", SQLState(database.Error{OrigErr: lock, Err: "migration failed"}))
	require.Equal(t, "55P03", SQLState(fmt.Errorf("x: %w", &database.Error{OrigErr: lock})))
	require.Equal(t, "55P03", SQLState(gomigrate.NewMultiError(database.Error{OrigErr: lock}, errors.New("unlock"))))
}
