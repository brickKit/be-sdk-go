package authz

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The table below runs each E2–E5 rule against testdata/bundle.example.json:
// dev_sales_rep holds erp.sales.view; dev_wh_south holds erp.inventory.* until 1760000000;
// delegation dg_91 lets u_B act on_behalf of u_A for two workflow keys in [1759276800, 1759881600);
// stale_since u_123 = 1759400000; revoked_grants dg_77; profile view_as_ro lets erp.sales.view and
// erp.inventory.view through; capabilities delegation=true, agents=false, impersonation=false.
func TestDecideRulesOnExampleBundle(t *testing.T) {
	now := time.Unix(1759500000, 0)
	iat := time.Unix(1759499900, 0)
	rep := func(mut func(*Token)) Token {
		tok := Token{Sub: "u_9", IssuedAt: iat, Roles: []string{"dev_sales_rep"}}
		if mut != nil {
			mut(&tok)
		}
		return tok
	}
	cases := []struct {
		name string
		tok  Token
		key  string
		now  time.Time
		want Decision
	}{
		{"E5 role holds the key", rep(nil), "erp.sales.view", now, allow},
		{"E5 no role holds the key", rep(nil), "erp.sales.ship", now, deny(ReasonMissingPermission)},
		{"E5 unknown role holds nothing", rep(func(t *Token) { t.Roles = []string{"ghost"} }), "erp.sales.view", now, deny(ReasonMissingPermission)},
		{"E5 no roles at all", rep(func(t *Token) { t.Roles = nil }), "erp.sales.view", now, deny(ReasonMissingPermission)},

		{"E2.1 stale: iat earlier than stale_since - 5", Token{Sub: "u_123", IssuedAt: time.Unix(1759399994, 0), Roles: []string{"dev_sales_rep"}}, "erp.sales.view", now, deny(ReasonTokenStale)},
		{"E2.1 not stale: iat exactly stale_since - 5", Token{Sub: "u_123", IssuedAt: time.Unix(1759399995, 0), Roles: []string{"dev_sales_rep"}}, "erp.sales.view", now, allow},
		{"E2.1 stale applies only to its sub", rep(func(t *Token) { t.IssuedAt = time.Unix(1, 0) }), "erp.sales.view", now, allow},
		{"E2.2 revoked grant is stale", rep(func(t *Token) { t.DG = "dg_77"; t.Ceil = []string{"view_as_ro"} }), "erp.sales.view", now, deny(ReasonTokenStale)},
		{"E2 order: stale wins over an unsupported act chain", Token{Sub: "u_123", IssuedAt: time.Unix(1, 0), Act: &Act{Sub: "a", Kind: "agent"}}, "erp.sales.view", now, deny(ReasonTokenStale)},
		{"E2.3 delegated token with delegation on", rep(func(t *Token) { t.DG = "dg_5"; t.Ceil = []string{"view_as_ro"} }), "erp.sales.view", now, allow},
		{"E2.4 act kind agent needs agents", rep(func(t *Token) { t.Act = &Act{Sub: "bot", Kind: "agent"} }), "erp.sales.view", now, deny(ReasonUnsupportedDelegation)},
		{"E2.4 act kind user needs impersonation", rep(func(t *Token) { t.Act = &Act{Sub: "u_admin", Kind: "user"} }), "erp.sales.view", now, deny(ReasonUnsupportedDelegation)},
		{"E2.4 act kind svc needs nothing more", rep(func(t *Token) { t.Act = &Act{Sub: "svc:edi", Kind: "svc"} }), "erp.sales.view", now, allow},
		{"E2.4 nested agent in the chain", rep(func(t *Token) {
			t.Act = &Act{Sub: "svc:edi", Kind: "svc", Act: &Act{Sub: "bot", Kind: "agent"}}
		}), "erp.sales.view", now, deny(ReasonUnsupportedDelegation)},
		{"E2.4 unknown kind", rep(func(t *Token) { t.Act = &Act{Sub: "x", Kind: "robot"} }), "erp.sales.view", now, deny(ReasonUnsupportedDelegation)},

		{"E3 role inside its window", Token{Sub: "u_w", IssuedAt: iat, Roles: []string{"dev_wh_south"}}, "erp.inventory.adjust", time.Unix(1759999999, 0), allow},
		{"E3 role at until contributes nothing", Token{Sub: "u_w", IssuedAt: iat, Roles: []string{"dev_wh_south"}}, "erp.inventory.adjust", time.Unix(1760000000, 0), deny(ReasonMissingPermission)},

		{"E4 ceiling lets the key through", rep(func(t *Token) { t.DG = "dg_5"; t.Ceil = []string{"view_as_ro"} }), "erp.sales.view", now, allow},
		{"E4 ceiling blocks a key the role holds", rep(func(t *Token) { t.DG = "dg_5"; t.Ceil = []string{"view_as_ro"} }), "erp.sales.cancel", now, deny(ReasonMissingPermission)},
		{"E4 unknown ceiling profile is empty", rep(func(t *Token) { t.DG = "dg_5"; t.Ceil = []string{"ghost"} }), "erp.sales.view", now, deny(ReasonMissingPermission)},
		{"E4 every ceiling must allow", rep(func(t *Token) { t.DG = "dg_5"; t.Ceil = []string{"view_as_ro", "ghost"} }), "erp.sales.view", now, deny(ReasonMissingPermission)},

		{"E5 on_behalf delegation covers the key", Token{Sub: "u_B", IssuedAt: iat}, "infra.workflow.task.act", now, allow},
		{"E5 on_behalf delegation is for another key", Token{Sub: "u_B", IssuedAt: iat}, "erp.sales.view", now, deny(ReasonMissingPermission)},
		{"E5 on_behalf delegation is for another sub", Token{Sub: "u_C", IssuedAt: iat}, "infra.workflow.task.act", now, deny(ReasonMissingPermission)},
		{"E3 on_behalf delegation before its window", Token{Sub: "u_B", IssuedAt: iat}, "infra.workflow.task.act", time.Unix(1759276799, 0), deny(ReasonMissingPermission)},
		{"E3 on_behalf delegation at until", Token{Sub: "u_B", IssuedAt: iat}, "infra.workflow.task.act", time.Unix(1759881600, 0), deny(ReasonMissingPermission)},
	}
	b, err := ParseBundle(loadExample(t))
	require.NoError(t, err)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, Decide(b, c.tok, c.key, c.now))
		})
	}
}

func TestDecideDelegationCapabilityOff(t *testing.T) {
	b, err := ParseBundle([]byte(`{"contract":"authz/2.0","revision":"1","capabilities":{"core":true},
		"roles":{"r":["a.b.c"]},"grants":{},"stale_since":{},
		"delegations":[{"id":"d1","mode":"on_behalf","from":"u_A","to":"u_B","keys":["a.b.c"]}]}`))
	require.NoError(t, err)
	now := time.Unix(100, 0)
	for name, c := range map[string]struct {
		tok  Token
		want Decision
	}{
		"act needs delegation":  {Token{Sub: "u", Roles: []string{"r"}, Act: &Act{Sub: "s", Kind: "svc"}}, deny(ReasonUnsupportedDelegation)},
		"ceil needs delegation": {Token{Sub: "u", Roles: []string{"r"}, Ceil: []string{"p"}}, deny(ReasonUnsupportedDelegation)},
		"dg needs delegation":   {Token{Sub: "u", Roles: []string{"r"}, DG: "d"}, deny(ReasonUnsupportedDelegation)},
		"on_behalf ignored":     {Token{Sub: "u_B"}, deny(ReasonMissingPermission)},
		"plain token allowed":   {Token{Sub: "u", Roles: []string{"r"}}, allow},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, c.want, Decide(b, c.tok, "a.b.c", now))
		})
	}
}

func TestDecideAgentsAndImpersonationOn(t *testing.T) {
	b, err := ParseBundle([]byte(`{"contract":"authz/2.0","revision":"1",
		"capabilities":{"core":true,"delegation":true,"agents":true,"impersonation":true},
		"roles":{"r":["a.b.c"]},"grants":{},"stale_since":{}}`))
	require.NoError(t, err)
	tok := Token{Sub: "u", Roles: []string{"r"}, Act: &Act{Sub: "u2", Kind: "user", Act: &Act{Sub: "bot", Kind: "agent"}}}
	require.Equal(t, allow, Decide(b, tok, "a.b.c", time.Unix(100, 0)))
}

func TestDecideWithoutBundle(t *testing.T) {
	require.Equal(t, deny(ReasonAuthzNotReady), Decide(nil, Token{Sub: "u"}, "a.b.c", time.Unix(1, 0)))
	require.Equal(t, ReasonAuthzNotReady, CheckToken(nil, Token{Sub: "u"}))
	require.False(t, HasKey(nil, Token{Sub: "u"}, "a.b.c", time.Unix(1, 0)))
}

var allow = Decision{Allow: true}

func deny(reason string) Decision { return Decision{Reason: reason} }
