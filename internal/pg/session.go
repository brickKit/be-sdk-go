package pg

import (
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// setLocalSQL is the one simple-protocol batch sent right after BEGIN (P10.2, P10.3), in the
// protocol's order. Identifiers are quoted as SQL identifiers and the application name as a string
// literal, so no configured value can break out of its statement.
func setLocalSQL(s session) string {
	var b strings.Builder
	b.WriteString("SET LOCAL ROLE " + pgx.Identifier{s.Role}.Sanitize())
	b.WriteString("; SET LOCAL search_path TO " + pgx.Identifier{s.Schema}.Sanitize())
	b.WriteString("; SET LOCAL application_name = " + quoteLiteral(s.AppName))
	b.WriteString("; SET LOCAL statement_timeout = " + millis(s.Statement))
	b.WriteString("; SET LOCAL lock_timeout = " + millis(s.Lock))
	b.WriteString("; SET LOCAL idle_in_transaction_session_timeout = " + millis(s.Idle))
	if s.Transaction > 0 {
		b.WriteString("; SET LOCAL transaction_timeout = " + millis(s.Transaction))
	}
	return b.String()
}

// quoteLiteral quotes s as a standard-conforming SQL string literal.
func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// millis renders a duration as a quoted PostgreSQL time setting in whole milliseconds.
func millis(d time.Duration) string { return "'" + strconv.FormatInt(d.Milliseconds(), 10) + "ms'" }
