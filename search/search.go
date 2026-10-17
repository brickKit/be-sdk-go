// Package search builds the normalized search columns of protocol P9.6-P9.8: each field folded (NFKC, lower case,
// Cc and Cf dropped), its full pinyin and its initials from the protocol's shared table, parts joined by U+001F; a
// query matches when it is a literal substring of one part.
package search

import (
	"bufio"
	"strconv"
	"strings"
	"sync"
	"unicode"

	protocol "github.com/brickKit/be-protocol/v3"
	"golang.org/x/text/unicode/norm"
)

// Version is the normalization version stored with each search column; a higher one means a backfill.
const Version = 1

// Sep separates the parts of a search column; a folded query never contains it.
const Sep = "\x1f"

var (
	once   sync.Once
	pinyin map[rune]string
)

func table() map[rune]string {
	once.Do(func() {
		pinyin = map[rune]string{}
		f, err := protocol.Data.Open("data/pinyin.tsv")
		if err != nil {
			return
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "#") {
				continue
			}
			cp, reading, ok := strings.Cut(line, "\t")
			if n, err := strconv.ParseInt(cp, 16, 32); ok && err == nil {
				pinyin[rune(n)] = reading
			}
		}
	})
	return pinyin
}

// Fold is the folding of P9.6.
func Fold(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(norm.NFKC.String(s)) {
		if !unicode.Is(unicode.Cc, r) && !unicode.Is(unicode.Cf, r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Text is the search column of some field values.
func Text(values ...string) string {
	py := table()
	parts := make([]string, 0, 3*len(values))
	for _, v := range values {
		f := Fold(v)
		var full, initials strings.Builder
		for _, r := range f {
			if p, ok := py[r]; ok {
				full.WriteString(p)
				initials.WriteByte(p[0])
			} else {
				full.WriteRune(r)
				initials.WriteRune(r)
			}
		}
		parts = append(parts, f, full.String(), initials.String())
	}
	return strings.Join(parts, Sep)
}

// Query normalizes a query; empty means no condition.
func Query(q string) string { return strings.TrimSpace(Fold(q)) }

// Like is the LIKE pattern of a normalized query (ESCAPE '\'): % and _ match literally.
func Like(q string) string {
	return "%" + strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(q) + "%"
}

// Match reports whether q matches a search column (in memory; SQL uses Like).
func Match(text, q string) bool {
	n := Query(q)
	if n == "" {
		return true
	}
	for _, part := range strings.Split(text, Sep) {
		if strings.Contains(part, n) {
			return true
		}
	}
	return false
}
