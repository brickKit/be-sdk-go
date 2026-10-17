package runtime

import (
	"testing"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/stretchr/testify/assert"
)

func TestEnvPrefixMatchesBrickKit(t *testing.T) {
	assert.Equal(t, "INFRA_REDIS_EVENT_BUS", EnvPrefix("infra/redis-event-bus"))
	assert.Equal(t, "ERP_API_ADMIN_API", EnvPrefix("erp/api")+"_"+EnvPrefix("admin-api"))
}

func TestDependencyAddresses(t *testing.T) {
	rt := New(Parts{Addresses: config.Map{
		"ERP_INVENTORY_ENDPOINT":      "http://erp-inventory-3-0-0:8086",
		"ERP_INVENTORY_GRPC_ENDPOINT": "http://erp-inventory-3-0-0:9096",
		"MDM_PARTNER_ENDPOINT":        "",
	}})
	ep, ok := rt.Dependency("erp/inventory")
	assert.True(t, ok)
	assert.Equal(t, "erp-inventory-3-0-0:8086", ep.Host)
	g, ok := rt.DependencyPort("erp/inventory", "grpc")
	assert.True(t, ok)
	assert.Equal(t, "erp-inventory-3-0-0:9096", g.Host)
	_, ok = rt.Dependency("mdm/partner")
	assert.False(t, ok, "an empty value means absent, never an empty address")
	_, ok = rt.Dependency("erp/credit")
	assert.False(t, ok, "an optional dependency that is not installed is absent")
}
