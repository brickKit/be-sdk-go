package config

import (
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var schemeRe = regexp.MustCompile(`^[a-z][a-z0-9+.-]*$`)

// ParseURL checks a url value (P2.3, vectors url): scheme://host[:port][path], lower-case scheme, one
// host, port 1-65535, no whitespace; schemes (when not empty) restricts the scheme. The value is
// returned unchanged.
func ParseURL(key, raw string, schemes []string) (string, error) {
	bad := func(detail string) (string, error) { return "", newErr(ReasonInvalid, key, detail) }
	if strings.ContainsAny(raw, " \t\r\n") {
		return bad("whitespace in URL")
	}
	scheme, _, found := strings.Cut(raw, "://")
	if !found || !schemeRe.MatchString(scheme) {
		return bad("not scheme://host with a lower-case scheme")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" || strings.Contains(u.Host, ",") {
		return bad("not scheme://host[:port][path] with one host")
	}
	if strings.HasSuffix(u.Host, ":") {
		return bad("empty port")
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
			return bad("port out of range 1-65535")
		}
	}
	if len(schemes) > 0 && !slices.Contains(schemes, scheme) {
		return bad("scheme not one of " + strings.Join(schemes, ", "))
	}
	return raw, nil
}
