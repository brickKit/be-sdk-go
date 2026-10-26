package launcher

import (
	"context"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestReadyWithoutWaitingForTheDefinitions: a member with protected routes submits its definitions at start; while the
// authorization member refuses them (or cannot be reached) the member is ready all the same (P8.24: on Kubernetes the
// submission may need the member's own readiness), its protected routes answer AUTHORIZATION_VIEW_EXPIRED (P8.13),
// and it retries until they are accepted.
func TestReadyWithoutWaitingForTheDefinitions(t *testing.T) {
	port := freePort(t)
	base := "http://127.0.0.1:" + strconv.Itoa(port)
	d := &demo{release: make(chan struct{})}
	def := definition(d)
	vars := secured(t, def, map[string]string{"COMPONENT_ID": "demo/alpha", "COMPONENT_VERSION": "3.0.0",
		"PORT": strconv.Itoa(port)})
	w := worldOf(t)
	w.Refuse("DEFINITION_OWNER_MISMATCH")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	var stdout syncBuffer
	go func() { done <- Run(ctx, def, Options{Env: env(vars), Stdout: &stdout, Stderr: io.Discard}) }()
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 5*time.Second, 20*time.Millisecond,
		"ready although the definitions were refused")
	assert.Equal(t, 1, strings.Count(stdout.String(), `"event":"ready"`), "the ready line (P1.12)")
	code, body := get(t, base+"/api/hello")
	assert.Equal(t, 503, code, "no view before the definitions are accepted")
	assert.Contains(t, string(body), "AUTHORIZATION_VIEW_EXPIRED")
	_, ok := w.Submitted("demo/alpha", "3.0.0")
	assert.False(t, ok)
	assert.NotContains(t, stdout.String(), `"event":"definitions_accepted"`)

	w.Refuse("")
	require.Eventually(t, func() bool { c, _ := get(t, base+"/api/hello"); return c == 200 }, 5*time.Second, 50*time.Millisecond,
		"a retry is accepted and a view arrives")
	assert.Equal(t, 1, strings.Count(stdout.String(), `"event":"definitions_accepted"`), "the acceptance is logged once")
	assert.Equal(t, 1, strings.Count(stdout.String(), `"event":"ready"`), "and the ready line stays single")
	set, ok := w.Submitted("demo/alpha", "3.0.0")
	assert.True(t, ok)
	sealed, err := def.Authz.Records(w.Deployment, "3.0.0")
	require.NoError(t, err)
	want, _ := authz.SetDigest("demo/alpha", "3.0.0", sealed)
	assert.Equal(t, want, set, "the member accepted exactly the declared definitions")
	cancel()
	close(d.release)
	<-done
}
