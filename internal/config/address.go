package config

import (
	"regexp"
	"strings"
)

var (
	componentIDRe = regexp.MustCompile(`^[a-z][a-z0-9]*/[a-z][a-z0-9-]*$`)
	portNameRe    = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	hostLabelRe   = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$`)
	portNumberRe  = regexp.MustCompile(`^[0-9]{1,5}$`)
)

// EndpointName returns the variable brickKit injects for a dependency's port (P2.6, vectors
// endpoint_name): <ID upper-cased, / and - as _>[_<PORT NAME>]_ENDPOINT; port "" is the main port.
func EndpointName(dep, port string) (string, error) {
	if !componentIDRe.MatchString(dep) {
		return "", newErr(ReasonComponentInvalid, "", "dependency id is not <scope>/<name>")
	}
	r := strings.NewReplacer("/", "_", "-", "_")
	name := r.Replace(strings.ToUpper(dep))
	if port != "" {
		if !portNameRe.MatchString(port) {
			return "", newErr(ReasonPortNameInvalid, "", "port name is not lower-case kebab")
		}
		name += "_" + r.Replace(strings.ToUpper(port))
	}
	return name + endpointSuffix, nil
}

// Endpoint reads an injected *_ENDPOINT value (P2.5, P2.6, vectors endpoint_value). Absent
// (present == false) is an optional dependency that is not installed: ok == false, no error. The value
// must be http://host:port with at most one trailing "/"; addr is host:port.
func Endpoint(value string, present bool) (addr string, ok bool, err error) {
	if !present {
		return "", false, nil
	}
	addr, e := httpHostPort("", value)
	if e != nil {
		return "", false, e
	}
	return addr, true, nil
}

// FamilyAddress reads a slot-family address key (P2.10, vectors family_address): AUTHZ_URL and
// IAM_URL give the REST base http://host:port; AUTHZ_GRPC_URL and IAM_GRPC_URL give the gRPC dial
// target host:port. Explicit port, no path, one trailing "/" stripped; absent = the member does not run.
func FamilyAddress(key, value string, present bool) (addr string, ok bool, err error) {
	grpc, known := familyKind(key)
	if !known {
		return "", false, newErr(ReasonKeyInvalid, key, "not a slot-family address key")
	}
	if !present {
		return "", false, nil
	}
	hp, e := httpHostPort(key, value)
	if e != nil {
		return "", false, e
	}
	if grpc {
		return hp, true, nil
	}
	return "http://" + hp, true, nil
}

// familyKind reports whether key is a family address key and whether it is a gRPC one.
func familyKind(key string) (grpc, known bool) {
	switch key {
	case "AUTHZ_URL", "IAM_URL":
		return false, true
	case "AUTHZ_GRPC_URL", "IAM_GRPC_URL":
		return true, true
	}
	return false, false
}

// httpHostPort parses http://host:port[/] into host:port: lower-case DNS host, decimal port 1-65535.
func httpHostPort(key, value string) (string, *Error) {
	rest, ok := strings.CutPrefix(value, "http://")
	if !ok {
		return "", newErr(ReasonInvalid, key, "address is not http://host:port")
	}
	rest = strings.TrimSuffix(rest, "/")
	host, port, found := strings.Cut(rest, ":")
	if !found || !hostLabelRe.MatchString(host) || !portNumberRe.MatchString(port) {
		return "", newErr(ReasonInvalid, key, "address is not http://host:port (explicit port, no path)")
	}
	if n := atoiDigits(port); n < 1 || n > 65535 {
		return "", newErr(ReasonInvalid, key, "port out of range 1-65535")
	}
	return rest, nil
}

// atoiDigits converts a string of at most 5 ASCII digits.
func atoiDigits(s string) int {
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}
