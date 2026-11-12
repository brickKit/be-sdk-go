package events

import (
	"testing"

	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/stretchr/testify/assert"
)

// TestAStreamMayBePulledByAnyComponent: a watermark stream's pullers and a record read's readers may be every
// component of the deployment (grpcx.AnyComponent), as a method's callers may; the service identity and the operation
// are still checked.
func TestAStreamMayBePulledByAnyComponent(t *testing.T) {
	assert.True(t, allowed([]string{grpcx.AnyComponent}, "erp/purchase"))
	assert.True(t, allowed([]string{"infra/authz"}, "infra/authz"))
	assert.False(t, allowed([]string{"infra/authz"}, "erp/purchase"))
	assert.False(t, allowed([]string{grpcx.AnyComponent}, ""), "no service identity")
}
