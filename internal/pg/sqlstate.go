package pg

import (
	"errors"

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
