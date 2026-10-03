package besdk

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
)

// P4.1 (rc.2, Spec.ErrorDomain): a slot-family member answers its own reasons with the family's ID;
// its routes stay under its own ID.
func TestErrorDomainOfASlotFamilyMember(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	eng := gin.New()
	r := newRouter(eng, routerConfig{componentID: "infra/authz-openfga", errorDomain: "infra/authz",
		catalogue: problem.NewCatalogue(), locale: "en", defaultDeadline: 10 * time.Second, guardian: &fakeGuardian{}})
	GET(r, "/x", Public, func(c *gin.Context) { Fail(c, Errorf(codes.NotFound, "ROLE_NOT_FOUND", nil, "no role")) })
	rec := httptest.NewRecorder()
	eng.ServeHTTP(rec, httptest.NewRequest("GET", "/infra/authz-openfga/x", nil))
	if rec.Code != 404 || !strings.Contains(rec.Body.String(), `"domain":"infra/authz"`) {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	if got := (Spec{ID: "erp/sales"}).errorDomain(); got != "erp/sales" {
		t.Fatalf("default error domain %q", got)
	}
}
