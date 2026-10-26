package runtime

import (
	"io"
	"log/slog"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestConfiguredAddresses(t *testing.T) { // P4.26
	m, err := calls.NewMember(prometheus.NewRegistry())
	require.NoError(t, err)
	logs := slog.New(slog.NewTextHandler(io.Discard, nil))
	rt := New(Parts{Outbound: m, Logger: logs})
	h, ok := rt.HTTPAt("test/probe-go2", "http://test-probe-go2-3-1-0:8602")
	assert.True(t, ok)
	assert.NotNil(t, h)
	_, ok = rt.HTTPAt("test/probe-go2", "")
	assert.False(t, ok, "no address, no client")
	_, ok = rt.GRPCAt("test/probe-go2", "http://test-probe-go2-3-1-0:9602")
	assert.False(t, ok, "never plaintext: without a service identity the system surface is not dialled")
	m.SetTLS(sectest.New(t).TLS(t, "test/probe-go"))
	conn, ok := rt.GRPCAt("test/probe-go2", "http://test-probe-go2-3-1-0:9602")
	require.True(t, ok)
	assert.Equal(t, "passthrough:///test-probe-go2-3-1-0:9602", conn.Target(), "the $endpoint form is dialled by host and port, resolved per connection (P4.28)")
	same, _ := rt.GRPCAt("test/probe-go2", "test-probe-go2-3-1-0:9602")
	assert.Same(t, conn, same, "one connection per component and address, kept for the member's life")
}
