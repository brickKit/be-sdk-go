package rpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

func ids(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("id-%d", i)
	}
	return out
}

func TestFieldLimitReadsTheGoOption(t *testing.T) {
	md := (&probepb.BatchRequest{}).ProtoReflect().Descriptor()
	require.Equal(t, 3, fieldLimit(md.Fields().ByName("ids")))
	require.Equal(t, DefaultMaxItems, fieldLimit(md.Fields().ByName("tags")))
	filter := (&probepb.Filter{}).ProtoReflect().Descriptor()
	require.Equal(t, 2, fieldLimit(filter.Fields().ByName("sku_ids")))
}

func TestBatchWithinLimitsPasses(t *testing.T) {
	var b batchLimits
	req := &probepb.BatchRequest{Ids: ids(3), Tags: ids(500), Filter: &probepb.Filter{SkuIds: ids(2)}}
	require.Nil(t, b.check(req))
}

func requireBatchTooLarge(t *testing.T, e *problem.Error, field string, max, got int) {
	t.Helper()
	require.NotNil(t, e)
	require.Equal(t, codes.InvalidArgument, e.Code)
	require.Equal(t, "be", e.Domain)
	require.Equal(t, "BATCH_TOO_LARGE", e.Reason)
	require.Equal(t, map[string]string{"field": field, "max": fmt.Sprint(max), "got": fmt.Sprint(got)}, e.Metadata)
	require.Len(t, e.Violations, 1)
	require.Equal(t, field, e.Violations[0].Field)
	require.Equal(t, "BATCH_TOO_LARGE", e.Violations[0].Reason)
}

func TestBatchOverExplicitLimit(t *testing.T) {
	var b batchLimits
	requireBatchTooLarge(t, b.check(&probepb.BatchRequest{Ids: ids(4)}), "ids", 3, 4)
}

func TestBatchOverDefaultLimit(t *testing.T) {
	var b batchLimits
	requireBatchTooLarge(t, b.check(&probepb.BatchRequest{Tags: ids(501)}), "tags", 500, 501)
}

func TestBatchOverLimitInNestedMessage(t *testing.T) {
	var b batchLimits
	req := &probepb.BatchRequest{Filter: &probepb.Filter{SkuIds: ids(3)}}
	requireBatchTooLarge(t, b.check(req), "filter.sku_ids", 2, 3)
}

func TestBatchMapFieldsAreNotLimited(t *testing.T) {
	var b batchLimits
	labels := map[string]string{}
	for i := range 600 {
		labels[fmt.Sprint(i)] = "x"
	}
	require.Nil(t, b.check(&probepb.BatchRequest{Labels: labels}))
}

func TestBatchNonProtoMessageIsIgnored(t *testing.T) {
	var b batchLimits
	require.Nil(t, b.check("not a proto message"))
	require.Nil(t, b.check(nil))
}
