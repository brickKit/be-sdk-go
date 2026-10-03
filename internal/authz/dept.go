package authz

import (
	"regexp"
	"slices"
	"sort"
	"strings"
)

// deptPattern is E6's valid department path: "/" or "/<seg>/…/".
var deptPattern = regexp.MustCompile(`^/([^/]+/)*$`)

// ValidDept reports whether path is a department path (E6): "/" or "/<seg>/…/". An empty or malformed
// path means no department (R60).
func ValidDept(path string) bool { return deptPattern.MatchString(path) }

// DeptAncestors are "/", every ancestor of a valid department path and the path itself, sorted (E8).
func DeptAncestors(dept string) []string {
	out := []string{"/"}
	for i := 1; i < len(dept); i++ {
		if dept[i] == '/' {
			out = append(out, dept[:i+1])
		}
	}
	sort.Strings(out)
	return slices.Compact(out)
}

// DeptPrefix is E6's prefix encoding: the path with \, % and _ escaped by \, followed by %.
func DeptPrefix(path string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(path) + "%"
}

// likeMatch is SQL LIKE with backslash escape: % matches any run, _ one character, \x the character x.
// It matches by rune, as PostgreSQL does for a UTF-8 database.
func likeMatch(s, pattern string) bool {
	return likeRunes([]rune(s), []rune(pattern))
}

func likeRunes(s, p []rune) bool {
	for len(p) > 0 {
		switch c := p[0]; {
		case c == '%':
			for len(p) > 0 && p[0] == '%' {
				p = p[1:]
			}
			if len(p) == 0 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if likeRunes(s[i:], p) {
					return true
				}
			}
			return false
		case c == '_':
			if len(s) == 0 {
				return false
			}
			s, p = s[1:], p[1:]
		default:
			if c == '\\' && len(p) > 1 {
				p = p[1:]
				c = p[0]
			}
			if len(s) == 0 || s[0] != c {
				return false
			}
			s, p = s[1:], p[1:]
		}
	}
	return len(s) == 0
}
