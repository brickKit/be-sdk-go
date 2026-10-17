package launcher

import (
	"context"
	"strconv"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/internal/sectest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/stretchr/testify/assert"
)

type stray struct{}

func (stray) Routes(r *httpx.Routes) error {
	r.GET("/api/x", "demo.alpha.export", func(c *httpx.Context) error { return c.NoContent(204) })
	return nil
}

func TestARouteWithAnUndeclaredOperationStopsTheStart(t *testing.T) { // P8.7, CK-AZ-01
	def := component.Definition{ID: "demo/alpha", Version: "3.0.0", Authz: sectest.Defs("demo/alpha", "demo.alpha", "demo.alpha.read"),
		New: func(*runtime.Runtime) (component.Module, error) { return stray{}, nil }}
	var out syncBuffer
	code := Run(context.Background(), def, Options{Env: env(secured(t, def, map[string]string{"COMPONENT_ID": "demo/alpha",
		"COMPONENT_VERSION": "3.0.0", "PORT": strconv.Itoa(freePort(t))})), Stdout: &out, Stderr: &out})
	assert.Equal(t, ExitFailure, code)
	assert.Contains(t, out.String(), "not declared: demo.alpha.export")
}

func TestProtectedRoutesNeedTheSecurityConfiguration(t *testing.T) {
	def := definition(&demo{})
	var out syncBuffer
	code := Run(context.Background(), def, Options{Env: env(map[string]string{"COMPONENT_ID": "demo/alpha",
		"COMPONENT_VERSION": "3.0.0", "PORT": strconv.Itoa(freePort(t))}), Stdout: &out, Stderr: &out})
	assert.Equal(t, ExitConfiguration, code)
	for _, key := range []string{"DEPLOYMENT_ID", "IAM_ISSUER", "IAM_JWKS_URL", "AUTHZ_GRPC_URL", "SERVICE_TLS_CERT_FILE"} {
		assert.Contains(t, out.String(), `"key":"`+key+`"`)
	}
}
