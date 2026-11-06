package tiering

import (
	"context"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func catalogWith(t *testing.T, cl Class, env map[string]string) (*Catalog, error) {
	t.Helper()
	values, problems := config.Parse(Keys([]Class{cl}), config.Map(env))
	require.Empty(t, problems)
	return NewCatalog(context.Background(), nil, []Class{cl}, values, "p")
}

// P15.4, TR24: a retention given at deployment replaces the declared one (a pending one included); without it a
// pending class stays pending; a malformed one, or one that makes a class destroyable without its approval operation,
// refuses the start.
func TestARetentionGivenAtDeployment(t *testing.T) {
	cl := orders()
	cl.Retention, cl.ApproveOperation, cl.Routes = Retention{Pending: true}, "", []string{RoutePostgres} // no store here
	c, err := catalogWith(t, cl, nil)
	require.NoError(t, err)
	assert.True(t, c.Classes[cl.Name].Retention.Pending, "nothing given: pending")
	assert.Equal(t, []string{cl.Name}, c.PendingRetentions(), "listed for the go-live (TR24)")

	_, err = catalogWith(t, cl, map[string]string{RetentionKey(cl.Name): "2555/3650/commercial law, region ES"})
	assert.ErrorContains(t, err, "approval operation", "destroyable now: it needs its approval operation")
	cl.ApproveOperation = "sales.order.approve_destruction"
	c, err = catalogWith(t, cl, map[string]string{RetentionKey(cl.Name): "2555/3650/commercial law, region ES"})
	require.NoError(t, err)
	assert.Equal(t, Retention{MinDays: 2555, MaxDays: 3650, Basis: "commercial law, region ES"}, c.Classes[cl.Name].Retention)
	assert.Empty(t, c.PendingRetentions())

	for _, bad := range []string{"2555", "x/3650/law", "3650/2555/law", "0/10/law", "10/20/ "} {
		_, err := catalogWith(t, cl, map[string]string{RetentionKey(cl.Name): bad})
		assert.ErrorContains(t, err, RetentionKey(cl.Name), bad)
	}
}

// P15.35: the deployment picks the target among the routes the class allows; another route refuses the start; the
// object store chosen without a store refuses it too.
func TestARouteChosenAtDeployment(t *testing.T) {
	cl := orders()
	cl.Routes = []string{RoutePostgres, RouteObject}
	c, err := catalogWith(t, cl, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{RoutePostgres, RouteObject}, c.Classes[cl.Name].Routes, "the declared first")
	_, err = catalogWith(t, cl, map[string]string{RouteKey(cl.Name): RouteObject})
	assert.ErrorContains(t, err, "TIERING_STORE_URL", "the object store chosen: it needs one")
	cl.Routes = []string{RouteObject, RoutePostgres}
	c, err = catalogWith(t, cl, map[string]string{RouteKey(cl.Name): RoutePostgres})
	require.NoError(t, err)
	assert.Equal(t, []string{RoutePostgres, RouteObject}, c.Classes[cl.Name].Routes, "the chosen first")
	_, err = catalogWith(t, cl, map[string]string{RouteKey(cl.Name): "iceberg"})
	assert.ErrorContains(t, err, RouteKey(cl.Name))
}
