package besdk

import (
	"context"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authn"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/gin-gonic/gin"
)

// PermKey.On decides the record named by the path parameter before the handler (P6.6, A3): an
// invisible record answers 404, a visible one outside the key's scope 403, an allowed one reaches the
// handler; PermKey.List gives the handler the scope of its type.
func TestResourceGuardsDecideTheRecord(t *testing.T) {
	rt, _ := accessRuntime(t)
	things := map[string]thing{"t1": {ID: "t1", Owner: "alice"}, "t2": {ID: "t2", Owner: "bob"}}
	rt.deps.loaders = map[ResourceType]SharingLoader{"test.thing.thing": {Type: "test.thing.thing",
		Load: func(_ context.Context, _ *Tx, ids []string) (map[string]Resource, error) {
			out := map[string]Resource{}
			for _, id := range ids {
				if th, ok := things[id]; ok {
					out[id] = th
				}
			}
			return out, nil
		}}}
	claims := &authn.Claims{Sub: "alice", Roles: []string{"rep"}, IssuedAt: time.Now()}
	g := &authGuardian{verifier: fakeVerifier{claims: claims}, bundles: staticBundle{mustBundle(t, thingBundle)},
		now: time.Now, rt: rt}
	gin.SetMode(gin.ReleaseMode)
	eng := gin.New()
	r := newRouter(eng, routerConfig{componentID: "test/thing", catalogue: problem.NewCatalogue(), locale: "en",
		defaultDeadline: 5 * time.Second, guardian: g})
	const edit PermKey = "test.thing.edit"
	const view PermKey = "test.thing.view"
	PUT(r, "/things/:id", edit.On("test.thing.thing", "id"), func(c *gin.Context) { Respond(c, 200, "ok") })
	GET(r, "/things", view.List("test.thing.thing"), func(c *gin.Context) {
		a, _ := AccessFrom(c.Request.Context())
		Respond(c, 200, map[string]any{"owners": a.ListScope().Params().Owners})
	})
	for path, want := range map[string]int{"/test/thing/things/t1": 200, "/test/thing/things/t2": 404, "/test/thing/things/none": 404} {
		if s, b := call(eng, "PUT", path, "Bearer good"); s != want {
			t.Fatalf("%s: %d %v", path, s, b)
		}
	}
	if s, b := call(eng, "GET", "/test/thing/things", "Bearer good"); s != 200 || b["owners"].([]any)[0] != "alice" {
		t.Fatalf("list scope: %d %v", s, b)
	}
}
