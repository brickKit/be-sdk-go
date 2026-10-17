package db

import (
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
)

// LintProblem is one refused migration statement or file; Rule is the rule code of protocol vector migration-lint.
type LintProblem struct {
	File, Rule, Reason, Statement string
}

func (p LintProblem) String() string {
	if p.Statement == "" {
		return fmt.Sprintf("%s: %s (%s)", p.File, p.Reason, p.Rule)
	}
	return fmt.Sprintf("%s: %s (%s): %q", p.File, p.Reason, p.Rule, p.Statement)
}

type lintRule struct {
	code   string
	re     *regexp.Regexp
	reason string
}

var (
	upFile = regexp.MustCompile(`^[0-9]+_[a-z0-9_]+\.up\.sql$`)
	// statement-start rules: migrations hold structure only; roles, grants, schemas and session settings belong to the
	// provisioning script, and every file runs as one implicit transaction (protocol 14, "structure migrations")
	// statement rules in the order of the protocol's list; the first six match at the start of a statement
	statementRules = []lintRule{
		{"SET", regexp.MustCompile(`(?i)^(set|reset)\b`), "SET and RESET are not allowed: the migrate entry sets the schema and timeouts"},
		{"TX_CONTROL", regexp.MustCompile(`(?i)^(begin|start\s+transaction|commit|end|rollback|savepoint|release)\b`), "transaction control is not allowed: every file runs as one transaction"},
		{"GRANT", regexp.MustCompile(`(?i)^(grant|revoke|reassign)\b`), "grants belong to the provisioning script, not to migrations"},
		{"ROLE_SCHEMA", regexp.MustCompile(`(?i)^(create|alter|drop)\s+(role|user|group|schema)\b`), "roles and schemas belong to the provisioning script"},
		{"DEFAULT_PRIVILEGES", regexp.MustCompile(`(?i)^alter\s+default\s+privileges\b`), "default privileges belong to the provisioning script"},
		{"DROP_OWNED", regexp.MustCompile(`(?i)^drop\s+owned\b`), "DROP OWNED is not allowed"},
		{"OWNER", regexp.MustCompile(`(?i)\bowner\s+to\b`), "ownership is fixed by the provisioning script"},
		{"CONCURRENTLY", regexp.MustCompile(`(?i)\bconcurrently\b`), "CONCURRENTLY cannot run inside the file's transaction; use a maintenance task"},
	}
)

// LintMigrations checks a component's migration files before they run: file names, forward-only, and the statements
// protocol 14 forbids. schema is the component's schema; writing its name into a migration is refused as well, since
// the same files must run in whatever schema the assembly assigns.
func LintMigrations(files fs.FS, schema string) []LintProblem {
	var problems []LintProblem
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return []LintProblem{{File: ".", Rule: "UNREADABLE", Reason: "cannot read the migration files: " + err.Error()}}
	}
	own := regexp.MustCompile(`(?i)(^|[^a-z0-9_$])"?` + regexp.QuoteMeta(schema) + `"?\s*\.`)
	versions := map[string]string{}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		if strings.HasSuffix(name, ".down.sql") {
			problems = append(problems, LintProblem{File: name, Rule: "DOWN", Reason: "down migrations are not allowed: production only moves forward"})
			continue
		}
		if !upFile.MatchString(name) {
			problems = append(problems, LintProblem{File: name, Rule: "NAME", Reason: "the name must be <version>_<lower_snake_title>.up.sql"})
			continue
		}
		v := strings.TrimLeft(name[:strings.IndexByte(name, '_')], "0")
		if other, dup := versions[v]; dup {
			problems = append(problems, LintProblem{File: name, Rule: "DUP_VERSION", Reason: "same version as " + other})
		}
		versions[v] = name
		body, err := fs.ReadFile(files, path.Clean(name))
		if err != nil {
			problems = append(problems, LintProblem{File: name, Rule: "UNREADABLE", Reason: "cannot read: " + err.Error()})
			continue
		}
		for _, stmt := range statements(string(body)) {
			for _, r := range statementRules {
				if r.re.MatchString(stmt.code) {
					problems = append(problems, LintProblem{File: name, Rule: r.code, Reason: r.reason, Statement: short(stmt.code)})
				}
			}
			if schema != "" && own.MatchString(stmt.code) {
				problems = append(problems, LintProblem{File: name, Rule: "SCHEMA_NAME", Reason: "the schema name is written into the migration; leave names unqualified", Statement: short(stmt.code)})
			}
		}
	}
	return problems
}

type statement struct{ code string }

// statements splits SQL on top-level semicolons, dropping comments and the contents of string literals and dollar
// quoted bodies (a function body is checked as part of its CREATE statement, never as statements of its own).
func statements(sql string) []statement {
	var out []statement
	var b strings.Builder
	flush := func() {
		if s := strings.TrimSpace(b.String()); s != "" {
			out = append(out, statement{code: s})
		}
		b.Reset()
	}
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '-' && strings.HasPrefix(sql[i:], "--"):
			j := strings.IndexByte(sql[i:], '\n')
			if j < 0 {
				i = len(sql)
			} else {
				i += j
			}
		case c == '/' && strings.HasPrefix(sql[i:], "/*"):
			j := strings.Index(sql[i+2:], "*/")
			if j < 0 {
				i = len(sql)
			} else {
				i += j + 4
			}
			b.WriteByte(' ')
		case c == '\'':
			j := i + 1
			for j < len(sql) {
				if sql[j] == '\'' {
					if j+1 < len(sql) && sql[j+1] == '\'' { // an escaped quote
						j += 2
						continue
					}
					break
				}
				j++
			}
			b.WriteString("''")
			i = j + 1
		case c == '"':
			j := strings.IndexByte(sql[i+1:], '"')
			if j < 0 {
				j = len(sql) - i - 1
			}
			if ident := sql[i : i+j+2]; strings.Contains(ident, ".") {
				b.WriteString(`"_"`) // one identifier with a dot in it, never a qualified name
			} else {
				b.WriteString(ident)
			}
			i += j + 2
		case c == '$':
			if tag := dollarTag(sql[i:]); tag != "" {
				j := strings.Index(sql[i+len(tag):], tag)
				if j < 0 {
					i = len(sql)
				} else {
					i += len(tag) + j + len(tag)
				}
				b.WriteString("$$")
				continue
			}
			b.WriteByte(c)
			i++
		case c == ';':
			flush()
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	flush()
	return out
}

var dollar = regexp.MustCompile(`^\$([A-Za-z_][A-Za-z0-9_]*)?\$`)

func dollarTag(s string) string { return dollar.FindString(s) }

func short(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 80 {
		return s[:77] + "..."
	}
	return s
}
