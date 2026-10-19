package db

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
)

// sessionSetConfig is set_config(name, value, false) with literal or parameter arguments.
var sessionSetConfig = regexp.MustCompile(`(?i)set_config\s*\(` + sqlArg + `,` + sqlArg + `,\s*false\s*\)`)

const sqlArg = `\s*(?:'(?:[^']|'')*'|\$\d+)\s*`

// leftOnConnection names what a statement would leave on the pooled connection after the transaction, or ""
// (P5.30): SET without LOCAL (SET TRANSACTION and SET CONSTRAINTS end with the transaction), set_config(…, false),
// RESET, DISCARD, LISTEN, UNLISTEN and session-level advisory locks; a setting made inside a function or a DO block is
// not seen here. Every setting belongs in the transaction (P5.5), so a pooled connection carries nothing to the next
// transaction and needs no session reset on release; a statement that would break that fails where it is written
// instead of being cleaned up unseen.
func leftOnConnection(sql string) string {
	switch word, rest := firstWord(sql); word {
	case "SET":
		switch next, _ := firstWord(rest); next {
		case "LOCAL", "TRANSACTION", "CONSTRAINTS":
			return ""
		}
		return "SET without LOCAL"
	case "RESET", "DISCARD", "LISTEN", "UNLISTEN":
		return word
	}
	if (strings.Contains(sql, "et_config") || strings.Contains(sql, "ET_CONFIG")) && sessionSetConfig.MatchString(sql) {
		return "set_config(…, false)"
	}
	if strings.Contains(sql, "dvisory") || strings.Contains(sql, "DVISORY") {
		lower := strings.ToLower(sql)
		if strings.Contains(lower, "pg_advisory_lock") || strings.Contains(lower, "pg_try_advisory_lock") {
			return "a session-level advisory lock"
		}
	}
	return ""
}

// firstWord is the first SQL keyword in upper case, past blanks and comments, and the text after it.
func firstWord(sql string) (string, string) {
	s := sql
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		switch {
		case strings.HasPrefix(s, "/*"):
			end := strings.Index(s, "*/")
			if end < 0 {
				return "", ""
			}
			s = s[end+2:]
		case strings.HasPrefix(s, "--"):
			end := strings.IndexByte(s, '\n')
			if end < 0 {
				return "", ""
			}
			s = s[end+1:]
		default:
			end := strings.IndexFunc(s, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_') })
			if end < 0 {
				end = len(s)
			}
			return strings.ToUpper(s[:end]), s[end:]
		}
	}
}

func sessionError(what string) error {
	return fmt.Errorf("db: %s would stay on the pooled connection after the transaction; use SET LOCAL, "+
		"set_config(…, true) or pg_advisory_xact_lock (protocol P5.30)", what)
}

// errRow is a row whose Scan reports err.
type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

var _ pgx.Row = errRow{}

// guard checks a statement before it runs in a transaction.
func guard(sql string) error {
	if what := leftOnConnection(sql); what != "" {
		return sessionError(what)
	}
	return nil
}
