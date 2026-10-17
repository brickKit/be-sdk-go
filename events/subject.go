package events

import "strings"

// SubjectMatches reports whether subject matches a NATS-style pattern: "*" is one token, ">" the rest.
func SubjectMatches(pattern, subject string) bool {
	p, s := strings.Split(pattern, "."), strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) || (tok != "*" && tok != s[i]) {
			return false
		}
	}
	return len(p) == len(s)
}

// StreamOfSubject returns the event stream a be.<stream>.<partition>.<type> subject belongs to, given the declared
// stream names.
func streamPattern(stream string) string { return "be." + stream + ".>" }
