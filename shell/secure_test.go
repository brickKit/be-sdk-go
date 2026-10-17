package shell

import (
	"maps"
	"net/http"
	"sync"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/internal/sectest"
)

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

// secure gives every served member its security configuration and grants Alice what each definition declares.
func secure(t *testing.T, defs []component.Definition, members ...Served) []Served {
	w := worldOf(t)
	out := make([]Served, len(members))
	for i, m := range members {
		cfg := maps.Clone(m.Config)
		if cfg == nil {
			cfg = map[string]string{}
		}
		maps.Copy(cfg, w.Env(t, m.ComponentID))
		m.Config = cfg
		out[i] = m
	}
	for _, d := range defs {
		if d.Authz != nil {
			w.GrantAll(sectest.Alice, d.Authz)
		}
	}
	return out
}

func authorize(t *testing.T, req *http.Request) {
	if w, ok := worlds.Load(t.Name()); ok {
		req.Header.Set("Authorization", w.(*sectest.World).Bearer(sectest.Alice))
	}
}
