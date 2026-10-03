package pg

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// LatestVersion is the highest component migration version in component (files
// `<version>_<name>.up.sql` / `.down.sql` at its root; lifecycle.yaml and every other file are
// ignored); 0 when there is none (P1.4: the image's migration version).
func LatestVersion(component fs.FS) (uint, error) {
	src, err := iofs.New(component, ".")
	if err != nil {
		return 0, problem.Wrap(err, "INTERNAL", nil)
	}
	defer func() { _ = src.Close() }()
	return lastVersion(src)
}

// lastVersion walks a source to its last version.
func lastVersion(src source.Driver) (uint, error) {
	v, err := src.First()
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, problem.Wrap(err, "INTERNAL", nil)
	}
	for {
		next, err := src.Next(v)
		if errors.Is(err, os.ErrNotExist) {
			return v, nil
		}
		if err != nil {
			return 0, problem.Wrap(err, "INTERNAL", nil)
		}
		v = next
	}
}

// fileHeader is what the first line of a component migration says (P11.4, "Migration file headers").
type fileHeader struct {
	Contract bool   // `-- be:contract after=<version>`: runs only once no version <= After still runs
	After    string // the last component version that still needs the old shape
	NoTx     bool   // `-- be:no-transaction`: one statement, run outside any transaction block
}

var (
	contractRe = regexp.MustCompile(`^--\s*be:contract after=(\S+)$`)
	noTxRe     = regexp.MustCompile(`^--\s*be:no-transaction$`)
	markerRe   = regexp.MustCompile(`^--\s*be:`)
)

// parseHeader reads a first line. A line that starts with `-- be:` but is not one of the two markers
// is an error: a typo must never turn a contract migration into an ordinary one.
func parseHeader(line string) (fileHeader, error) {
	line = strings.TrimSpace(line)
	if !markerRe.MatchString(line) {
		return fileHeader{}, nil
	}
	if noTxRe.MatchString(line) {
		return fileHeader{NoTx: true}, nil
	}
	if m := contractRe.FindStringSubmatch(line); m != nil {
		if _, err := parseSemver(m[1]); err != nil {
			return fileHeader{}, fmt.Errorf("header %q: after=%s is not a semantic version", line, m[1])
		}
		return fileHeader{Contract: true, After: m[1]}, nil
	}
	return fileHeader{}, fmt.Errorf("header %q is not `-- be:contract after=<version>` or `-- be:no-transaction`", line)
}

var upFileRe = regexp.MustCompile(`^([0-9]+)_.*\.up\.sql$`)

// migrationHeaders reads and checks the header of every <version>_<name>.up.sql before anything runs
// (P11.4): a contract's after must be a semantic version strictly below own, the component's version;
// a no-transaction file holds exactly one statement. Any problem is fatal and names the file.
func migrationHeaders(component fs.FS, own string) (map[uint]fileHeader, error) {
	ents, err := fs.ReadDir(component, ".")
	if err != nil {
		return nil, problem.Wrap(err, "INTERNAL", nil)
	}
	out := map[uint]fileHeader{}
	for _, e := range ents {
		m := upFileRe.FindStringSubmatch(e.Name())
		if e.IsDir() || m == nil {
			continue
		}
		v, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			return nil, problem.Wrap(err, "INTERNAL", nil)
		}
		body, err := fs.ReadFile(component, e.Name())
		if err != nil {
			return nil, problem.Wrap(err, "INTERNAL", nil)
		}
		h, err := checkHeader(string(body), own)
		if err != nil {
			return nil, problem.Wrap(fmt.Errorf("migration %s: %w", e.Name(), err), "INTERNAL", nil)
		}
		out[uint(v)] = h
	}
	return out, nil
}

func checkHeader(body, own string) (fileHeader, error) {
	first, rest, _ := strings.Cut(body, "\n")
	h, err := parseHeader(first)
	switch {
	case err != nil:
		return h, err
	case h.NoTx && statements(rest) != 1:
		return h, fmt.Errorf("a -- be:no-transaction file holds exactly one statement (CREATE INDEX CONCURRENTLY), found %d", statements(rest))
	case !h.Contract:
		return h, nil
	case own == "":
		return h, errors.New("a contract migration needs the component's own version (MigrateConfig.Version)")
	}
	c, err := compareSemver(h.After, own)
	if err != nil {
		return h, fmt.Errorf("component version %q: %w", own, err)
	}
	if c >= 0 {
		return h, fmt.Errorf("after=%s must be below the component's own version %s", h.After, own)
	}
	return h, nil
}

var (
	sqlLineComment  = regexp.MustCompile(`--[^\n]*`)
	sqlBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
)

// statements counts the statements of a plain SQL text: comments removed, split on ';'. It is meant for
// a no-transaction file, which has no strings or bodies containing ';'.
func statements(sql string) int {
	sql = sqlBlockComment.ReplaceAllString(sqlLineComment.ReplaceAllString(sql, ""), "")
	n := 0
	for _, s := range strings.Split(sql, ";") {
		if strings.TrimSpace(s) != "" {
			n++
		}
	}
	return n
}

var semverRe = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

type semver struct {
	core [3]uint64
	pre  []string
}

func parseSemver(s string) (semver, error) {
	m := semverRe.FindStringSubmatch(s)
	if m == nil {
		return semver{}, fmt.Errorf("%q is not a semantic version", s)
	}
	var v semver
	for i := 0; i < 3; i++ {
		v.core[i], _ = strconv.ParseUint(m[i+1], 10, 64)
	}
	if m[4] != "" {
		v.pre = strings.Split(m[4], ".")
	}
	return v, nil
}

// compareSemver orders two semantic versions by SemVer 2.0 precedence (build metadata ignored).
func compareSemver(a, b string) (int, error) {
	va, err := parseSemver(a)
	if err != nil {
		return 0, err
	}
	vb, err := parseSemver(b)
	if err != nil {
		return 0, err
	}
	for i := range 3 {
		if c := cmpUint(va.core[i], vb.core[i]); c != 0 {
			return c, nil
		}
	}
	switch {
	case len(va.pre) == 0 && len(vb.pre) == 0:
		return 0, nil
	case len(va.pre) == 0:
		return 1, nil
	case len(vb.pre) == 0:
		return -1, nil
	}
	for i := 0; i < len(va.pre) && i < len(vb.pre); i++ {
		if c := cmpIdent(va.pre[i], vb.pre[i]); c != 0 {
			return c, nil
		}
	}
	return cmpUint(uint64(len(va.pre)), uint64(len(vb.pre))), nil
}

func cmpIdent(a, b string) int {
	na, ea := strconv.ParseUint(a, 10, 64)
	nb, eb := strconv.ParseUint(b, 10, 64)
	switch {
	case ea == nil && eb == nil:
		return cmpUint(na, nb)
	case ea == nil:
		return -1
	case eb == nil:
		return 1
	}
	return strings.Compare(a, b)
}

func cmpUint(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
