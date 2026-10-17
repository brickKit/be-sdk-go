package launcher

import (
	"maps"
	"net/http"
	"sync"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/internal/sectest"
)

// worlds keeps one test deployment per test.
var worlds sync.Map

func worldOf(t *testing.T) *sectest.World {
	if w, ok := worlds.Load(t.Name()); ok {
		return w.(*sectest.World)
	}
	w := sectest.New(t)
	worlds.Store(t.Name(), w)
	t.Cleanup(func() { worlds.Delete(t.Name()) })
	return w
}

// secured adds the deployment's security configuration for the component and grants Alice every operation it
// declares; requests made with get and post then carry Alice's token.
func secured(t *testing.T, def component.Definition, vars map[string]string) map[string]string {
	w := worldOf(t)
	out := maps.Clone(vars)
	maps.Copy(out, w.Env(t, def.ID))
	if def.Authz != nil {
		w.GrantAll(sectest.Alice, def.Authz)
	}
	return out
}

func authorize(t *testing.T, req *http.Request) {
	if w, ok := worlds.Load(t.Name()); ok {
		req.Header.Set("Authorization", w.(*sectest.World).Bearer(sectest.Alice))
	}
}
