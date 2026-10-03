package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
	bev1 "github.com/brickKit/be-sdk-go/proto/be/v1"
)

func probeFiles(t *testing.T) *protoregistry.Files {
	t.Helper()
	files := new(protoregistry.Files)
	require.NoError(t, files.RegisterFile(bev1.File_be_v1_limits_proto))
	require.NoError(t, files.RegisterFile(probepb.File_betest_rpc_v1_probe_proto))
	return files
}

// The spec's JSON (P7 "Service config"), with every retryable method of every service in one entry,
// sorted by service and method.
func TestServiceConfigRetriesOnlyIdempotentMethods(t *testing.T) {
	got, err := DefaultServiceConfig(probeFiles(t))
	require.NoError(t, err)
	require.JSONEq(t, `{
	  "methodConfig": [{
	    "name": [
	      {"service": "betest.rpc.v1.Probe", "method": "BatchGet"},
	      {"service": "betest.rpc.v1.Probe", "method": "Read"},
	      {"service": "betest.rpc.v1.Probe", "method": "Touch"}
	    ],
	    "retryPolicy": {
	      "maxAttempts": 3,
	      "initialBackoff": "0.05s",
	      "maxBackoff": "0.5s",
	      "backoffMultiplier": 2,
	      "retryableStatusCodes": ["UNAVAILABLE"]
	    }
	  }],
	  "retryThrottling": {"maxTokens": 10, "tokenRatio": 0.1}
	}`, got)
}

func TestServiceConfigWithoutRetryableMethodsKeepsThrottling(t *testing.T) {
	files := new(protoregistry.Files)
	require.NoError(t, files.RegisterFile(bev1.File_be_v1_limits_proto))
	got, err := DefaultServiceConfig(files)
	require.NoError(t, err)
	require.JSONEq(t, `{"methodConfig": [], "retryThrottling": {"maxTokens": 10, "tokenRatio": 0.1}}`, got)
}

func TestServiceConfigFromGlobalFilesIsAcceptedByGRPC(t *testing.T) {
	sc, err := DefaultServiceConfig(protoregistry.GlobalFiles)
	require.NoError(t, err)
	require.Contains(t, sc, `"method":"Touch"`)
	require.NotContains(t, sc, `"method":"Create"`)
	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(sc))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}
