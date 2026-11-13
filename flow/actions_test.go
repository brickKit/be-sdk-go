package flow

import (
	"testing"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func actionsRuntime(t *testing.T, sources string, subj func(string) *authz.Subject) *runtime.Runtime {
	t.Helper()
	v, probs := config.Parse(Keys(), config.Map{ActionSourcesKey: sources})
	require.Empty(t, probs)
	return runtime.New(runtime.Parts{Config: v, Subject: subj})
}

// P17.20: UseActions takes the sources from FLOW_ACTION_SOURCES and the principals' authorization from the runtime.
func TestUseActions(t *testing.T) {
	want := &authz.Subject{}
	k := &Kernel{}
	require.NoError(t, k.UseActions(actionsRuntime(t, "integration/im-dingtalk", func(string) *authz.Subject { return want })))
	assert.Equal(t, []string{"integration/im-dingtalk"}, k.ActionSources)
	assert.Same(t, want, k.Subject("u1"))

	k = &Kernel{}
	require.NoError(t, k.UseActions(actionsRuntime(t, "", nil)))
	assert.Empty(t, k.ActionSources, "unset: no channel's actions are taken")
	assert.Nil(t, k.Subject("u1"), "a member without an authorization view gives no subject (no grants)")

	assert.Error(t, (&Kernel{}).UseActions(actionsRuntime(t, "Not A Component", nil)), "a malformed source stops the member")
}
