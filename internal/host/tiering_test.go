package host

import (
	"testing"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/stretchr/testify/assert"
)

// P15.1: a class's hold and approval operations are the component's declared operations.
func TestATieringClassNamesDeclaredOperations(t *testing.T) {
	defs := &authz.Definitions{Operations: []authz.Operation{{Key: "demo.hold"}, {Key: "demo.approve"}}}
	cl := tiering.Class{Name: "demo.notes", HoldOperation: "demo.hold", ApproveOperation: "demo.approve"}
	assert.NoError(t, tieringOperations(component.Definition{Authz: defs, Tiering: []tiering.Class{cl}}))
	cl.ApproveOperation = "demo.destroy"
	assert.ErrorContains(t, tieringOperations(component.Definition{Authz: defs, Tiering: []tiering.Class{cl}}), "demo.destroy")
	assert.Error(t, tieringOperations(component.Definition{Tiering: []tiering.Class{cl}}), "no definitions at all")
}
