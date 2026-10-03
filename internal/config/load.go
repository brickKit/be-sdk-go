package config

import (
	"os"
	"strings"
)

// Source returns the raw value of one key and whether the variable exists (P2.1): os.LookupEnv when
// standalone, the member's own entry of BRICKKIT_SERVED_MEMBERS_CONFIG in a shell.
type Source func(key string) (string, bool)

// Load parses every declared key from src (P2.3) and returns the values, or every problem found,
// each an *Error with Reason CONFIG_MISSING or CONFIG_INVALID naming the key, in declaration order.
//
// Steps: (1) keys without default_from; (2) keys with default_from: absent or empty takes the other
// key's resolved value when that one is set (an invalid source key is never set, so it is reported once);
// (3) secret files: an absolute path to a readable regular file whose text is not empty when required
// (P2.7, P2.9); (4) json values against the be-protocol schema the catalogue names, and slot-family addresses by
// the stricter P2.10 rule (http://host:port, explicit port, no path); (5) one_of groups:
// a group is in use when any of its keys is declared, and then at least one declared key of it must be
// set (EVENT_BUS_URL or NATS_URL); the error names the group's first declared key.
func Load(s Schema, src Source) (*Values, []*Error) {
	ld := loader{s: s, src: src, vals: make([]value, len(s.Decls)), errs: make([]*Error, len(s.Decls))}
	for i, d := range s.Decls {
		if d.DefaultFrom == "" {
			ld.resolveAt(i, d)
		}
	}
	for i, d := range s.Decls {
		if d.DefaultFrom != "" {
			ld.resolveAt(i, d)
		}
	}
	jv := newJSONValidator()
	for i, d := range s.Decls {
		if ld.errs[i] != nil || !ld.vals[i].set {
			continue
		}
		if d.Secret {
			ld.errs[i] = checkSecretFile(d.Name, ld.vals[i].raw, d.Required)
		} else if d.Format == FormatJSON && d.JSONSchema != "" {
			ld.errs[i] = jv.validate(d.Name, d.JSONSchema, ld.vals[i].json)
		} else if _, family := familyKind(d.Name); family {
			if _, _, err := FamilyAddress(d.Name, ld.vals[i].raw, true); err != nil {
				ld.errs[i], _ = AsError(err)
			}
		}
	}
	ld.checkOneOf()
	return ld.result()
}

// loader holds one Load's intermediate state, indexed like Schema.Decls.
type loader struct {
	s     Schema
	src   Source
	vals  []value
	errs  []*Error
	extra []*Error // one_of errors
}

// resolveAt resolves Decls[i], applying default_from (step 2 of Load).
func (ld *loader) resolveAt(i int, d Decl) {
	raw, present := ld.src(d.Name)
	if d.DefaultFrom != "" && (!present || raw == "") {
		if j, ok := ld.s.index[d.DefaultFrom]; ok && ld.vals[j].set {
			raw, present = ld.vals[j].raw, true
		}
	}
	v, err := resolve(d, raw, present)
	ld.vals[i], ld.errs[i] = v, err
}

// checkOneOf applies step 5 of Load.
func (ld *loader) checkOneOf() {
	done := map[string]bool{}
	for _, d := range ld.s.Decls {
		if len(d.OneOf) == 0 || done[strings.Join(d.OneOf, ",")] {
			continue
		}
		done[strings.Join(d.OneOf, ",")] = true
		first, anySet, anyErr := "", false, false
		for _, k := range d.OneOf {
			j, ok := ld.s.index[k]
			if !ok {
				continue
			}
			if first == "" {
				first = k
			}
			anySet = anySet || ld.vals[j].set
			anyErr = anyErr || ld.errs[j] != nil
		}
		if !anySet && !anyErr {
			ld.extra = append(ld.extra, newErr(ReasonMissing, first, "one of "+strings.Join(d.OneOf, ", ")+" is required"))
		}
	}
}

// result assembles the Values, or the errors in declaration order.
func (ld *loader) result() (*Values, []*Error) {
	var errs []*Error
	for _, e := range ld.errs {
		if e != nil {
			errs = append(errs, e)
		}
	}
	errs = append(errs, ld.extra...)
	if len(errs) > 0 {
		return nil, errs
	}
	vs := &Values{schema: ld.s, src: ld.src, entries: make(map[string]entry, len(ld.s.Decls))}
	for i, d := range ld.s.Decls {
		raw, present := ld.src(d.Name)
		vs.entries[d.Name] = entry{decl: d, v: ld.vals[i], sourceRaw: raw, sourcePresent: present}
	}
	return vs, nil
}

// checkSecretFile checks at start that a secret key's path names a readable regular file whose text
// is not empty when the key is required (P2.9). The file's content never appears in the error.
func checkSecretFile(key, path string, required bool) *Error {
	b, e := readSecretFile(key, path)
	if e != nil {
		return e
	}
	if _, _, err := SecretText(key, b, required); err != nil {
		e, _ := AsError(err)
		return e
	}
	return nil
}

// readSecretFile reads a secret file: it must be a regular file (after symlinks, as Kubernetes
// projected volumes use them) that this process can read.
func readSecretFile(key, path string) ([]byte, *Error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, newErr(ReasonInvalid, key, "secret file cannot be read: "+errClass(err))
	}
	if !fi.Mode().IsRegular() {
		return nil, newErr(ReasonInvalid, key, "secret path is not a regular file")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, newErr(ReasonInvalid, key, "secret file cannot be read: "+errClass(err))
	}
	return b, nil
}

// errClass names a file error without its path.
func errClass(err error) string {
	switch {
	case os.IsNotExist(err):
		return "does not exist"
	case os.IsPermission(err):
		return "permission denied"
	}
	return "I/O error"
}
