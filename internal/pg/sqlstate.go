package pg

import (
	"database/sql/driver"
	"errors"
	"io"
	"net"

	"github.com/brickKit/be-sdk-go/internal/problem"
	gomigrate "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgx/v5/pgconn"
)

// SQLState returns the SQLSTATE of the first PostgreSQL error in err's chain, or "" (P10.4). It also
// looks inside golang-migrate's database.Error, which does not unwrap.
func SQLState(err error) string {
	for err != nil {
		var pg *pgconn.PgError
		if errors.As(err, &pg) {
			return pg.Code
		}
		err = migrateOrig(err)
	}
	return ""
}

// migrateOrig returns the original error inside golang-migrate's database.Error or the first error of
// its MultiError (neither unwraps), or nil.
func migrateOrig(err error) error {
	var multi gomigrate.MultiError
	if errors.As(err, &multi) && len(multi.Errs) > 0 {
		return multi.Errs[0]
	}
	var v database.Error
	if errors.As(err, &v) {
		return v.OrigErr
	}
	var p *database.Error
	if errors.As(err, &p) && p != nil {
		return p.OrigErr
	}
	return nil
}

// IsUniqueViolation reports SQLSTATE 23505, which a component maps to its own reason, usually
// ALREADY_EXISTS (P10.4 table).
func IsUniqueViolation(err error) bool { return SQLState(err) == "23505" }

// IsLockTimeout reports SQLSTATE 55P03, raw or already classified as LOCK_TIMEOUT (P10.4).
func IsLockTimeout(err error) bool {
	return SQLState(err) == "55P03" || problem.Is(err, problem.DomainBe, "LOCK_TIMEOUT")
}

// IsConnectionFailure reports the database being unreachable (stage-B ruling, DEPENDENCY_UNAVAILABLE
// with dependency db): SQLSTATE class 08 or 57P01–57P03, or, without a SQLSTATE, a failed connect (refused or timed
// out), a network error, an unexpected EOF or a bad driver connection. Any other SQLSTATE — 53300 and
// an authentication failure among them — is not.
func IsConnectionFailure(err error) bool {
	if state := SQLState(err); state != "" {
		return problem.IsDBUnreachableState(state)
	}
	var ce *pgconn.ConnectError
	var ne *net.OpError
	return errors.As(err, &ce) || errors.As(err, &ne) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, driver.ErrBadConn) || errors.Is(err, net.ErrClosed)
}
