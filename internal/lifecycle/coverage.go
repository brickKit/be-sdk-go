package lifecycle

import (
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// DeclarationFile is the declaration's name beside the migrations (P16.1).
const DeclarationFile = "lifecycle.yaml"

// Load reads lifecycle.yaml from a component's migrations directory, checks it (Parse) and checks
// that it declares exactly the tables the migrations create (P11.11). Every problem is fatal at start.
func Load(migrations fs.FS) (*Declaration, error) {
	data, err := fs.ReadFile(migrations, DeclarationFile)
	if err != nil {
		return nil, &Error{Msg: fmt.Sprintf("every component with a database ships migrations/%s v1 (P16.1): %v", DeclarationFile, err)}
	}
	d, err := Parse(data)
	if err != nil {
		return nil, err
	}
	created, from, err := scanCreated(migrations)
	if err != nil {
		return nil, err
	}
	for _, name := range created {
		if _, ok := d.Tables[name]; !ok {
			return nil, &Error{Table: name, Msg: fmt.Sprintf("created by %s but not declared (P11.11)", from[name])}
		}
	}
	for _, name := range d.Names() {
		if _, ok := from[name]; !ok {
			return nil, &Error{Table: name, Msg: "declared but no migration creates it (P11.11)"}
		}
	}
	return d, nil
}

// createdTables lists the tables the *.up.sql files leave created, sorted.
func createdTables(migrations fs.FS) ([]string, error) {
	names, _, err := scanCreated(migrations)
	return names, err
}

// The scan is deliberately simple and documented (P11.11): comments are removed; every
// `CREATE [UNLOGGED] TABLE [IF NOT EXISTS] <name>` adds a table and every `DROP TABLE [IF EXISTS]
// <names>` removes one, file by file in version order. An unquoted name is folded to lower case; a
// quoted one is kept as written. TEMP tables, besdk_* tables and the migration-state tables are
// exempt (the latter two both start with besdk_ or schema_migrations_).
var (
	lineComment  = regexp.MustCompile(`--[^\n]*`)
	blockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	createRe     = regexp.MustCompile(`(?i)\bCREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?("[^"]+"|[A-Za-z_][A-Za-z0-9_$.]*)`)
	dropRe       = regexp.MustCompile(`(?i)\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^;]+)`)
	versionRe    = regexp.MustCompile(`^([0-9]+)_.*\.up\.sql$`)
)

func scanCreated(migrations fs.FS) ([]string, map[string]string, error) {
	files, err := upFiles(migrations)
	if err != nil {
		return nil, nil, err
	}
	from := map[string]string{}
	for _, f := range files {
		body, err := fs.ReadFile(migrations, f)
		if err != nil {
			return nil, nil, err
		}
		sql := blockComment.ReplaceAllString(lineComment.ReplaceAllString(string(body), ""), "")
		for _, m := range createRe.FindAllStringSubmatch(sql, -1) {
			if n := tableName(m[1]); !exempt(n) {
				from[n] = f
			}
		}
		for _, m := range dropRe.FindAllStringSubmatch(sql, -1) {
			for _, part := range strings.Split(m[1], ",") {
				fields := strings.Fields(part)
				if len(fields) > 0 {
					delete(from, tableName(fields[0]))
				}
			}
		}
	}
	return sortedKeys(from), from, nil
}

// upFiles lists <version>_<name>.up.sql at the root, by numeric version.
func upFiles(migrations fs.FS) ([]string, error) {
	ents, err := fs.ReadDir(migrations, ".")
	if err != nil {
		return nil, err
	}
	type file struct {
		v    uint64
		name string
	}
	var files []file
	for _, e := range ents {
		m := versionRe.FindStringSubmatch(path.Base(e.Name()))
		if e.IsDir() || m == nil {
			continue
		}
		v, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			return nil, err
		}
		files = append(files, file{v, e.Name()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].v < files[j].v })
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.name
	}
	return out, nil
}

func tableName(raw string) string {
	if strings.HasPrefix(raw, `"`) {
		return strings.Trim(raw, `"`)
	}
	return strings.ToLower(raw)
}

func exempt(name string) bool {
	return strings.HasPrefix(name, "besdk_") || strings.HasPrefix(name, "schema_migrations_")
}
