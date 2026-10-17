// Package money handles amounts and other decimals (chapter 05, P5.25-P5.28): contract strings are parsed and
// formatted exactly, rounding happens only here, and allocation keeps the parts summing to the total. Nothing goes
// through floating point.
package money

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/cockroachdb/apd/v3"
)

// Mode is a rounding mode.
type Mode string

// Rounding modes (P5.26).
const (
	HalfAwayFromZero Mode = "half_away_from_zero" // the default
	HalfEven         Mode = "half_even"
)

const maxDigits = 38

var (
	grammar = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)
	ctx     = apd.BaseContext.WithPrecision(80)
)

func invalid(s, why string) error {
	return errs.New(errs.ReasonRequestInvalid, fmt.Sprintf("%q is not a valid decimal: %s", s, why))
}

// Parse reads a contract decimal string (P5.25): quantities, prices, rates.
func Parse(s string) (*apd.Decimal, error) {
	if !grammar.MatchString(s) {
		return nil, invalid(s, "expected -digits[.digits]")
	}
	if len(strings.NewReplacer("-", "", ".", "").Replace(s)) > maxDigits {
		return nil, invalid(s, "more than 38 digits")
	}
	d, _, err := apd.NewFromString(s)
	if err != nil {
		return nil, invalid(s, err.Error())
	}
	if d.IsZero() && d.Negative {
		return nil, invalid(s, "negative zero")
	}
	return d, nil
}

// ParseAmount reads an amount of a currency with scale decimal places and returns it at exactly that scale; more
// significant places are refused, never rounded away.
func ParseAmount(s string, scale int32) (*apd.Decimal, error) {
	d, err := Parse(s)
	if err != nil {
		return nil, err
	}
	r := Round(d, scale, HalfAwayFromZero)
	if r.Cmp(d) != 0 {
		return nil, invalid(s, fmt.Sprintf("more than %d decimal places", scale))
	}
	return r, nil // exactly scale places
}

// Format writes d with exactly scale decimal places, rounding half away from zero; zero has no sign.
func Format(d *apd.Decimal, scale int32) string {
	r := Round(d, scale, HalfAwayFromZero)
	if r.IsZero() {
		r.Negative = false
	}
	return r.Text('f')
}

func rounder(m Mode) apd.Rounder {
	if m == HalfEven {
		return apd.RoundHalfEven
	}
	return apd.RoundHalfUp // apd rounds half up by magnitude: away from zero
}

// Round rounds d (finite) to scale decimal places by mode. The precision follows d's own digits, so no amount is
// too large to round.
func Round(d *apd.Decimal, scale int32, mode Mode) *apd.Decimal {
	digits := d.NumDigits() + int64(max(d.Exponent, 0)) + int64(max(scale, 0)) + 2
	c := apd.BaseContext.WithPrecision(uint32(min(digits, int64(apd.MaxExponent))))
	c.Rounding = rounder(mode)
	var r apd.Decimal
	if _, err := c.Quantize(&r, d, -scale); err != nil {
		panic(fmt.Sprintf("money: rounding %s to %d places: %v", d, scale, err)) // only for a non-finite d
	}
	return &r
}

// RoundCash rounds d to a whole multiple of unit (such as 0.05) by mode, then to scale places.
func RoundCash(d, unit *apd.Decimal, scale int32, mode Mode) (*apd.Decimal, error) {
	if unit.Sign() <= 0 {
		return nil, fmt.Errorf("money: the cash unit %s is not positive", unit)
	}
	var q, m apd.Decimal
	if _, err := ctx.Quo(&q, d, unit); err != nil {
		return nil, err
	}
	if _, err := ctx.Mul(&m, Round(&q, 0, mode), unit); err != nil {
		return nil, err
	}
	return Round(&m, scale, mode), nil
}

// Allocate splits total by weights with the largest-remainder method (P5.28): each part is a whole number of
// minor units (10^-scale), the leftover units go to the largest remainders (earlier parts first on ties), a
// negative total is split by its magnitude, and the parts sum exactly to total. total must be exact at scale;
// weights are non-negative with a positive sum.
func Allocate(total *apd.Decimal, weights []*apd.Decimal, scale int32) ([]*apd.Decimal, error) {
	t, ok := new(big.Rat).SetString(total.String())
	if !ok {
		return nil, fmt.Errorf("money: total %s", total)
	}
	units := new(big.Rat).Mul(new(big.Rat).Abs(t), new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)))
	if !units.IsInt() {
		return nil, fmt.Errorf("money: the total %s has more than %d decimal places", total, scale)
	}
	sum := new(big.Rat)
	ws := make([]*big.Rat, len(weights))
	for i, w := range weights {
		if ws[i], ok = new(big.Rat).SetString(w.String()); !ok || ws[i].Sign() < 0 {
			return nil, fmt.Errorf("money: weight %s is not a non-negative number", w)
		}
		sum.Add(sum, ws[i])
	}
	if sum.Sign() == 0 {
		return nil, fmt.Errorf("money: the weights sum to zero")
	}
	parts := largestRemainder(units.Num(), ws, sum)
	out := make([]*apd.Decimal, len(parts))
	for i, p := range parts {
		if t.Sign() < 0 {
			p.Neg(p)
		}
		out[i] = apd.NewWithBigInt(new(apd.BigInt).SetMathBigInt(p), -scale)
	}
	return out, nil
}

func largestRemainder(n *big.Int, ws []*big.Rat, sum *big.Rat) []*big.Int {
	base := make([]*big.Int, len(ws))
	rems := make([]*big.Rat, len(ws))
	given := new(big.Int)
	for i, w := range ws {
		share := new(big.Rat).Quo(new(big.Rat).Mul(new(big.Rat).SetInt(n), w), sum)
		base[i] = new(big.Int).Quo(share.Num(), share.Denom())
		rems[i] = share.Sub(share, new(big.Rat).SetInt(base[i]))
		given.Add(given, base[i])
	}
	order := make([]int, len(ws))
	for i := range order {
		order[i] = i
	}
	// stable: equal remainders keep the earlier part first
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && rems[order[j]].Cmp(rems[order[j-1]]) > 0; j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}
	left := new(big.Int).Sub(n, given).Int64() // fewer than len(ws) units are left
	for k := int64(0); k < left; k++ {
		base[order[k]].Add(base[order[k]], big.NewInt(1))
	}
	return base
}
