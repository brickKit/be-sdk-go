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
	case "DO":
		for _, st := range statements(sql) {
			if what := bodySessionState(st); what != "" {
				return what + " in a DO block"
			}
		}
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

// bodyOwner is a statement whose dollar quoted body runs later or now as procedural code.
var bodyOwner = regexp.MustCompile(`(?i)^(create\s+(or\s+replace\s+)?(function|procedure)|do)\b`)

// bodyStatement finds SET or RESET at the start of a body statement: at the body's start, after a semicolon, or after
// BEGIN, DECLARE, THEN, ELSE or LOOP (RE2 has no look-ahead; the word after SET is checked separately).
var bodyStatement = regexp.MustCompile(`(?i)(?:^|;|\b(?:begin|declare|then|else|loop))\s*\b(set|reset)\b`)

// bodySessionState names the session state a function, procedure or DO body would leave on the connection, or ""
// (P5.31): the statement-level rules of P5.30 applied inside the body. Strings run by a dynamic EXECUTE are not seen.
func bodySessionState(st statement) string {
	if !bodyOwner.MatchString(st.code) {
		return ""
	}
	for _, body := range st.bodies {
		code := withoutComments(body)
		for _, m := range bodyStatement.FindAllStringSubmatchIndex(code, -1) {
			if strings.EqualFold(code[m[2]:m[3]], "reset") {
				return "RESET"
			}
			switch next, _ := firstWord(code[m[3]:]); next {
			case "LOCAL", "TRANSACTION", "CONSTRAINTS":
				continue
			}
			return "SET without LOCAL"
		}
		if sessionSetConfig.MatchString(code) {
			return "set_config(…, false)"
		}
		if lower := strings.ToLower(code); strings.Contains(lower, "pg_advisory_lock") ||
			strings.Contains(lower, "pg_try_advisory_lock") {
			return "a session-level advisory lock"
		}
	}
	return ""
}

// withoutComments drops -- and /* */ comments outside string literals.
func withoutComments(sql string) string {
	var b strings.Builder
	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '\'':
			j := i + 1
			for j < len(sql) && !(sql[j] == '\'' && (j+1 >= len(sql) || sql[j+1] != '\'')) {
				if sql[j] == '\'' {
					j++ // an escaped quote
				}
				j++
			}
			end := min(j+1, len(sql))
			b.WriteString(sql[i:end])
			i = end
		case strings.HasPrefix(sql[i:], "--"):
			j := strings.IndexByte(sql[i:], '\n')
			if j < 0 {
				return b.String()
			}
			i += j
		case strings.HasPrefix(sql[i:], "/*"):
			j := strings.Index(sql[i+2:], "*/")
			if j < 0 {
				return b.String()
			}
			b.WriteByte(' ')
			i += j + 4
		default:
			b.WriteByte(sql[i])
			i++
		}
	}
	return b.String()
}
