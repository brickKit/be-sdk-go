package pg

import (
	"strings"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

func TestProbePassesOnFreshIdentity(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		t.Run("pg"+major, func(t *testing.T) {
			id := testpg.NewOn(t, major)
			asOwner(t, id, `CREATE TABLE widget (id uuid PRIMARY KEY)`,
				`CREATE TABLE ev (id uuid, created_at timestamptz, PRIMARY KEY (id, created_at)) PARTITION BY RANGE (created_at)`,
				`CREATE TABLE ev_2026 PARTITION OF ev FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')`)
			_, s := standalone(t, id, 1)
			r := Probe(within(t, 10e9), s, id.Owner, false)
			require.NoError(t, r.Err)
			require.NoError(t, r.Fatal)
			require.Empty(t, r.IdentityProblems)
			require.Zero(t, r.ComponentVersion)
			require.False(t, r.ComponentDirty)
			require.Zero(t, r.PlatformVersion)
		})
	}
}

func TestProbeReportsIdentityProblems(t *testing.T) {
	id := testpg.New(t)
	asOwner(t, id, `CREATE TABLE widget (id int)`, `CREATE TABLE gadget (id int)`)
	super := testpg.Open(t, id.SuperDSN)
	testpg.Exec(t, super,
		`CREATE TABLE `+id.Schema+`.stray (id int)`, // owned by the superuser
		`GRANT SELECT, INSERT, UPDATE, DELETE ON `+id.Schema+`.stray TO `+id.User,
		`REVOKE DELETE ON `+id.Schema+`.gadget FROM `+id.User,
		`GRANT CREATE ON SCHEMA `+id.Schema+` TO `+id.User)
	_, s := standalone(t, id, 1)
	r := Probe(within(t, 10e9), s, id.Owner, false)
	require.NoError(t, r.Err)
	require.NoError(t, r.Fatal)
	all := strings.Join(r.IdentityProblems, "\n")
	require.Len(t, r.IdentityProblems, 3, all)
	require.Contains(t, all, "stray")
	require.Contains(t, all, "lacks DELETE on table \"gadget\"")
	require.Contains(t, all, "has CREATE")
	require.NotContains(t, all, "widget")

	// a runtime role that is a member of the owner (it would inherit DDL)
	m := testpg.New(t)
	testpg.Exec(t, super, `GRANT `+m.Owner+` TO `+m.User)
	_, sm := standalone(t, m, 1)
	r = Probe(within(t, 10e9), sm, m.Owner, false)
	require.NoError(t, r.Err)
	require.Equal(t, []string{
		"PG_USER \"" + m.User + "\" has CREATE on schema \"" + m.Schema + "\"", // inherited from the owner
		"PG_USER \"" + m.User + "\" is a member of \"" + m.Owner + "\"",
	}, r.IdentityProblems)
}

func TestProbeReportsMissingSchemaAndRole(t *testing.T) {
	id := testpg.New(t)
	p := poolFor(t, id, id.User, id.Password, 1)
	r := Probe(within(t, 10e9), NewStore(p, StoreConfig{ComponentID: "c/x", Role: id.User, Schema: "no_such_schema"}), id.Owner, false)
	require.NoError(t, r.Err)
	require.Len(t, r.IdentityProblems, 1)
	require.Contains(t, r.IdentityProblems[0], "no_such_schema")

	r = Probe(within(t, 10e9), NewStore(p, StoreConfig{ComponentID: "c/x", Role: id.User, Schema: id.Schema}), "no_such_owner", false)
	require.NoError(t, r.Err)
	require.Len(t, r.IdentityProblems, 1)
	require.Contains(t, r.IdentityProblems[0], "no_such_owner")

	other := testpg.New(t)
	r = Probe(within(t, 10e9), NewStore(p, StoreConfig{ComponentID: "c/x", Role: other.User, Schema: id.Schema}), id.Owner, false)
	require.NoError(t, r.Err, "a role the login role cannot switch to is an identity problem, not a probe failure")
	require.Len(t, r.IdentityProblems, 1)
	require.Contains(t, r.IdentityProblems[0], other.User)
}

func TestProbeCapabilities(t *testing.T) {
	id14 := testpg.NewOn(t, "14")
	_, s14 := standalone(t, id14, 1)
	require.NoError(t, Probe(within(t, 10e9), s14, id14.Owner, false).Fatal)
	r := Probe(within(t, 10e9), s14, id14.Owner, true)
	require.True(t, problem.Is(r.Fatal, problem.DomainBe, "CAPABILITY_UNAVAILABLE"), "%v", r.Fatal)
	require.Contains(t, r.Fatal.Error(), "160000")

	id16 := testpg.New(t)
	_, s16 := standalone(t, id16, 1)
	require.NoError(t, Probe(within(t, 10e9), s16, id16.Owner, true).Fatal)
}

func TestProbeReadsMigrationVersions(t *testing.T) {
	id := testpg.New(t)
	asOwner(t, id,
		`CREATE TABLE schema_migrations_`+id.Schema+` (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL)`,
		`INSERT INTO schema_migrations_`+id.Schema+` VALUES (7, true)`,
		`CREATE TABLE besdk_platform_version (component TEXT PRIMARY KEY, version INT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`,
		`INSERT INTO besdk_platform_version (component, version) VALUES ('conformance/widget', 1), ('someone/else', 9)`)
	_, s := standalone(t, id, 1)
	r := Probe(within(t, 10e9), s, id.Owner, false)
	require.NoError(t, r.Err)
	require.Empty(t, r.IdentityProblems)
	require.Equal(t, uint(7), r.ComponentVersion)
	require.True(t, r.ComponentDirty)
	require.Equal(t, 1, r.PlatformVersion)
}

func TestProbeConnectionFailureIsErr(t *testing.T) {
	id := testpg.New(t)
	p := poolFor(t, id, id.User, "wrong-password", 1)
	r := Probe(within(t, 10e9), NewStore(p, StoreConfig{ComponentID: "c/x", Role: id.User, Schema: id.Schema}), id.Owner, false)
	require.Error(t, r.Err)
	require.NoError(t, r.Fatal)
	require.Empty(t, r.IdentityProblems)
}
