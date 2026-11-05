package authz

import (
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestASegmentOnANonPushdownableDimensionMakesTheListNotCompletable: a list whose segment constrains a dimension the
// resource declares not pushdownable answers QUERY_NOT_COMPLETABLE, never compiled as a local column or dropped
// (P8.17, security 01's AE16); all on it constrains nothing and compiles.
func TestASegmentOnANonPushdownableDimensionMakesTheListNotCompletable(t *testing.T) {
	r := &Resource{Type: "demo.doc", ViewOperation: "demo.doc.read",
		Dimensions: map[string]Dimension{"team": {Kind: ValueSet, Column: "team", Pushdown: NotPushdownable},
			"region": {Kind: ValueSet, Column: "region"}}}
	seg := func(team Atom) Segment {
		return Segment{ResourceType: "demo.doc", Operation: "demo.doc.read", Valid: Window{From: "2026-01-01T00:00:00Z"},
			Atoms: map[string]Atom{"team": team, "region": {Kind: "in", Values: []string{"east"}}}}
	}
	_, err := Compile(r, []Segment{seg(Atom{Kind: "in", Values: []string{"t1"}})}, "demo.doc.read", "p", time.Now(), "", 0)
	require.Error(t, err)
	assert.True(t, errs.Is(err, "be", errs.ReasonQueryNotCompletable), "%v", err)
	p, err := Compile(r, []Segment{seg(Atom{Kind: "all"})}, "demo.doc.read", "p", time.Now(), "", 0)
	require.NoError(t, err, "all on it constrains nothing")
	assert.NotContains(t, p.Where, "team")
}
