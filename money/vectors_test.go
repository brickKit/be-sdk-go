package money

import (
	"encoding/json"
	"strings"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/cockroachdb/apd/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dec(t *testing.T, s string) *apd.Decimal {
	d, _, err := apd.NewFromString(s)
	require.NoError(t, err, s)
	return d
}

func TestMoneyVectors(t *testing.T) { // P5.25-P5.28
	raw, err := protocol.Vectors.ReadFile("vectors/money.json")
	require.NoError(t, err)
	var f struct {
		Cases []struct {
			ID, Kind, Value, Mode, Unit, Result, Formatted, Total string
			Scale                                                 int32
			Valid                                                 bool
			Weights, Parts                                        []string
		}
	}
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.Cases)
	for _, c := range f.Cases {
		switch c.Kind {
		case "parse":
			d, err := ParseAmount(c.Value, c.Scale)
			if assert.Equal(t, c.Valid, err == nil, "%s: %v", c.ID, err) && err == nil {
				assert.Equal(t, c.Formatted, Format(d, c.Scale), c.ID)
			}
		case "round":
			assert.Equal(t, c.Result, Format(Round(dec(t, c.Value), c.Scale, Mode(c.Mode)), c.Scale), c.ID)
		case "cash":
			r, err := RoundCash(dec(t, c.Value), dec(t, c.Unit), c.Scale, Mode(c.Mode))
			require.NoError(t, err, c.ID)
			assert.Equal(t, c.Result, Format(r, c.Scale), c.ID)
		case "allocate":
			ws := make([]*apd.Decimal, len(c.Weights))
			for i, w := range c.Weights {
				ws[i] = dec(t, w)
			}
			parts, err := Allocate(dec(t, c.Total), ws, c.Scale)
			require.NoError(t, err, c.ID)
			got := make([]string, len(parts))
			for i, p := range parts {
				got[i] = Format(p, c.Scale)
			}
			assert.Equal(t, strings.Join(c.Parts, " "), strings.Join(got, " "), c.ID)
		default:
			t.Errorf("%s: unknown kind %q", c.ID, c.Kind)
		}
	}
}

func TestAllocateRefusesBadInput(t *testing.T) {
	_, err := Allocate(dec(t, "1.005"), []*apd.Decimal{dec(t, "1")}, 2)
	assert.Error(t, err, "a total finer than the scale")
	_, err = Allocate(dec(t, "1"), []*apd.Decimal{dec(t, "0"), dec(t, "0")}, 2)
	assert.Error(t, err, "weights summing to zero")
	_, err = Allocate(dec(t, "1"), []*apd.Decimal{dec(t, "-1"), dec(t, "2")}, 2)
	assert.Error(t, err, "a negative weight")
	_, err = RoundCash(dec(t, "1"), dec(t, "0"), 2, HalfEven)
	assert.Error(t, err)
}

func TestRoundingNeverOverflows(t *testing.T) {
	product := dec(t, strings.Repeat("9", 38)+".9999")
	var sq apd.Decimal
	_, err := apd.BaseContext.WithPrecision(200).Mul(&sq, product, product)
	require.NoError(t, err)
	r := Round(&sq, 4, HalfAwayFromZero)
	assert.Equal(t, int32(-4), r.Exponent)
	assert.Equal(t, 0, r.Cmp(Round(r, 4, HalfEven)), "rounding a 77-digit product keeps every digit")
	a, err := ParseAmount("12.340", 2)
	require.NoError(t, err)
	assert.Equal(t, "12.34", a.Text('f'), "an amount comes back at the currency's scale")
}
