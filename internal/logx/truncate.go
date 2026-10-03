package logx

import (
	"bytes"
	"sort"
)

// TruncatedMark ends every string value cut to keep a line within MaxLine (P18.2).
const TruncatedMark = "…[TRUNCATED]"

// fitLine encodes rec as one JSON object that, with its newline, is at most maxLine bytes (P18.2).
//
// Decision tree, each step only when the line is still too long:
//  1. the line fits: write it unchanged;
//  2. add `truncated: true`, then cut string values outside the envelope (nested ones too), longest
//     first, at a character boundary, each ending in TruncatedMark — this is the protocol's rule;
//  3. beyond the protocol, for lines whose bulk is not strings (a long array of numbers): replace whole
//     non-envelope values by TruncatedMark, largest first;
//  4. drop non-envelope fields, largest first.
//
// Envelope fields are never cut: when they alone do not fit, the line is written over the limit.
func fitLine(e *lineEncoder, rec *record, maxLine int) []byte {
	limit := maxLine - 1 // the newline
	line := e.record(rec)
	if len(line) <= limit {
		return line
	}
	rec.set("truncated", true)
	line = cutStrings(e, rec, limit)
	if len(line) > limit {
		line = replaceValues(e, rec, limit)
	}
	if len(line) > limit {
		line = dropFields(e, rec, limit)
	}
	return line
}

// cuttable reports whether field i may be shortened: not an envelope field and not the marker itself.
func cuttable(rec *record, i int) bool {
	k := rec.fields[i].key
	return i >= rec.fixed && !isEnvelope(k) && k != "truncated"
}

// stringSlot is one string value inside the record and how to replace it.
type stringSlot struct {
	s    string
	cost int
	set  func(string)
}

// cutStrings shortens string values longest first until the line fits (step 2) and returns the line.
// It tracks the encoded length arithmetically (stringCost is exact) and re-encodes once at the end.
func cutStrings(e *lineEncoder, rec *record, limit int) []byte {
	var slots []stringSlot
	for i := range rec.fields {
		if cuttable(rec, i) {
			f := &rec.fields[i]
			collectStrings(f.val, func(v any) { f.val = v }, &slots)
		}
	}
	sort.SliceStable(slots, func(a, b int) bool { return slots[a].cost > slots[b].cost })
	length := len(e.record(rec))
	markCost := stringCost(TruncatedMark)
	for _, sl := range slots {
		if length <= limit || sl.cost <= markCost {
			break
		}
		cut := prefixWithin(sl.s, sl.cost-(length-limit)-markCost) + TruncatedMark
		sl.set(cut)
		length -= sl.cost - stringCost(cut)
	}
	return e.record(rec)
}

// collectStrings appends every string leaf of v (objects walked in key order, so the result is
// deterministic) with a setter that replaces it in place.
func collectStrings(v any, set func(any), out *[]stringSlot) {
	switch x := v.(type) {
	case string:
		*out = append(*out, stringSlot{s: x, cost: stringCost(x), set: func(n string) { set(n) }})
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			collectStrings(x[k], func(n any) { x[k] = n }, out)
		}
	case []any:
		for i := range x {
			collectStrings(x[i], func(n any) { x[i] = n }, out)
		}
	}
}

// prefixWithin returns the longest prefix of s, ending at a character boundary, whose encoded length is
// at most budget ("" when budget <= 0).
func prefixWithin(s string, budget int) string {
	used := 0
	for i := 0; i < len(s); {
		c, size := runeCost(s[i:])
		if used+c > budget {
			return s[:i]
		}
		used += c
		i += size
	}
	return s
}

// bySize returns the keys of cuttable fields, largest encoded size first (key included when withKey).
func bySize(e *lineEncoder, rec *record, withKey bool) []string {
	type sized struct {
		key  string
		size int
	}
	var all []sized
	for i := range rec.fields {
		if !cuttable(rec, i) {
			continue
		}
		var b bytes.Buffer
		e.value(&b, rec.fields[i].val)
		if withKey {
			e.value(&b, rec.fields[i].key)
		}
		all = append(all, sized{rec.fields[i].key, b.Len()})
	}
	sort.SliceStable(all, func(a, b int) bool { return all[a].size > all[b].size })
	keys := make([]string, len(all))
	for i, s := range all {
		keys[i] = s.key
	}
	return keys
}

// replaceValues replaces whole values by TruncatedMark, largest first, until the line fits (step 3).
func replaceValues(e *lineEncoder, rec *record, limit int) []byte {
	line := e.record(rec)
	for _, k := range bySize(e, rec, false) {
		if len(line) <= limit {
			break
		}
		if rec.fields[rec.index[k]].val == TruncatedMark {
			continue
		}
		rec.set(k, TruncatedMark)
		line = e.record(rec)
	}
	return line
}

// dropFields removes non-envelope fields, largest first, until the line fits (step 4).
func dropFields(e *lineEncoder, rec *record, limit int) []byte {
	line := e.record(rec)
	for _, k := range bySize(e, rec, true) {
		if len(line) <= limit {
			break
		}
		rec.remove(k)
		line = e.record(rec)
	}
	return line
}
