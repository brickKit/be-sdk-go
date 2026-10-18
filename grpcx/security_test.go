package grpcx_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/internal/echotest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func serveOnlyFor(t *testing.T, id string, allowed ...string) string {
	var s grpcx.Services
	s.Register(&echoDesc, echotest.Say(func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("ok"), nil
	}), map[string]grpcx.Method{"Say": {Operation: id + ".say", Callers: allowed}})
	srv, err := grpcx.NewServer(deps(t, id), &s)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

func say(conn *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return conn.Invoke(ctx, echotest.Method, wrapperspb.String("x"), new(wrapperspb.StringValue), calls.Idempotent())
}

func TestOnlyPermittedComponentsMayCall(t *testing.T) { // P8.6, CK-ID-04
	addr := serveOnlyFor(t, "erp/inventory", "erp/sales")
	ok, err := member(t, "erp/sales").GRPC("erp/inventory", addr)
	require.NoError(t, err)
	assert.NoError(t, say(ok))
	other, err := member(t, "erp/billing").GRPC("erp/inventory", addr)
	require.NoError(t, err)
	assert.True(t, errs.Is(say(other), "be", errs.ReasonPermissionDenied))
}

func TestConnectionsWithoutAValidIdentityAreRefused(t *testing.T) { // P8.5, CK-ID-04
	addr := serveOnlyFor(t, "erp/inventory", "erp/sales")
	w := worldOf(t)

	plain, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer plain.Close()
	assert.Error(t, say(plain), "no TLS at all")

	cert, key, ca := w.CertsIn(t, "0190f3a1-0000-7000-8000-00000000dfff", "erp/sales")
	foreign, err := identity.LoadServiceTLS("0190f3a1-0000-7000-8000-00000000dfff", "erp/sales", cert, key, ca, os.ReadFile)
	require.NoError(t, err)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(foreign.ClientConfig("erp/inventory"))))
	require.NoError(t, err)
	defer conn.Close()
	assert.Error(t, say(conn), "a certificate of another deployment")

	m, err := calls.NewMember(prometheus.NewRegistry())
	require.NoError(t, err)
	m.SetTLS(w.TLS(t, "erp/sales"))
	impostor, err := m.GRPC("erp/finance", addr) // the client expects finance; the server is inventory
	require.NoError(t, err)
	assert.Error(t, say(impostor), "the client refuses a server that is not the target")

	_, err = grpcx.NewServer(grpcx.Deps{Member: deps(t, "x/y").Member, Logger: deps(t, "x/y").Logger,
		Registry: prometheus.NewRegistry(), Tracer: deps(t, "x/y").Tracer}, func() *grpcx.Services {
		var s grpcx.Services
		s.Register(&echoDesc, echotest.Say(nil), map[string]grpcx.Method{"Say": {Operation: "x", Callers: []string{"a/b"}}})
		return &s
	}())
	assert.ErrorContains(t, err, "no service identity", "never served without TLS")
}

func TestACertificateWithoutTheDerivedDNSNameIsRefused(t *testing.T) { // P8.5
	w := worldOf(t)
	cert, key, ca := w.CertsWithoutDNS(t, "erp/sales")
	_, err := identity.LoadServiceTLS(w.Deployment, "erp/sales", cert, key, ca, os.ReadFile)
	assert.ErrorContains(t, err, `no DNS name "erp.sales.component.`+w.Deployment+`"`)
}

func TestClientsSendTheTargetsDerivedNameAsSNI(t *testing.T) { // P8.5: what gRPC core's server matches on
	w := worldOf(t)
	cfg := w.TLS(t, "erp/sales").ClientConfig("erp/inventory")
	assert.Equal(t, identity.DNSName(w.Deployment, "erp/inventory"), cfg.ServerName)
}
