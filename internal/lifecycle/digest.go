package lifecycle

import (
	"crypto/sha256"
	"hash"
)

// Digest computes the canonical unit digest of P16 ("Canonical unit digest"): each row encoded
// column by column in declared order, every value in PostgreSQL's text output format, NULL as \N,
// fields separated by 0x1F, rows by 0x1E (separators only between, never trailing), rows in
// primary-key order; the digest is SHA-256 of the whole. Values are not escaped.
type Digest struct {
	h    hash.Hash
	rows int
}

// NewDigest starts an empty unit.
func NewDigest() *Digest { return &Digest{h: sha256.New()} }

var nullText = []byte(`\N`)

// Row adds one row; a nil field is NULL.
func (d *Digest) Row(fields []*string) {
	if d.rows > 0 {
		d.h.Write([]byte{0x1E})
	}
	for i, f := range fields {
		if i > 0 {
			d.h.Write([]byte{0x1F})
		}
		if f == nil {
			d.h.Write(nullText)
		} else {
			d.h.Write([]byte(*f))
		}
	}
	d.rows++
}

// Rows is the number of rows added.
func (d *Digest) Rows() int { return d.rows }

// Sum is the unit digest.
func (d *Digest) Sum() []byte { return d.h.Sum(nil) }

// Chain links a unit into its table's chain: SHA-256(prev || unit); prev is empty for the table's
// first sealed unit (G6).
func Chain(prev, unit []byte) []byte {
	h := sha256.New()
	h.Write(prev)
	h.Write(unit)
	return h.Sum(nil)
}
