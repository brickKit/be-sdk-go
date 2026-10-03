package besdk

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authn"
	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/telemetry"
	"github.com/gin-gonic/gin"
)

// authSetup is the token verifier and the bundle poller of a component with protected routes
// (profile auth: AUTHZ_URL, IAM_URL, IAM_ISSUER, TENANT_ID). nil when the component declares none.
type authSetup struct {
	verifier *authn.Verifier
	poller   *authz.Poller
	metrics  *telemetry.AuthzMetrics
}

func newAuthSetup(b *boot, rt *Runtime) (*authSetup, error) {
	if !declared(b.vals, "AUTHZ_URL") && !declared(b.vals, "IAM_URL") {
		return nil, nil
	}
	iam, okIAM := b.vals.Family("IAM_URL")
	az, okAZ := b.vals.Family("AUTHZ_URL")
	if !okIAM || !okAZ {
		return nil, fmt.Errorf("AUTHZ_URL and IAM_URL are both required for protected routes")
	}
	m, err := telemetry.NewAuthzMetrics(b.member.Registerer())
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 3 * time.Second} // P5.4, P6.1: each fetch ≤ 3 s
	a := &authSetup{metrics: m}
	a.verifier = authn.NewVerifier(authn.Config{JWKSURL: iam + "/.well-known/jwks.json",
		Issuer: optString(b.vals, "IAM_ISSUER", ""), Audience: optString(b.vals, "TENANT_ID", ""),
		Client: client, Now: rt.clock, Logger: b.log})
	a.poller = authz.NewPoller(authz.PollerConfig{URL: az + "/authz/v2/bundle", Client: client, Logger: b.log})
	return a, nil
}

// guardian returns the route decision chain; without auth configuration every protected route is
// refused at registration time instead (a protected route needs the auth profile's keys).
func (a *authSetup) guardian(rt *Runtime) guardian {
	if a == nil {
		return noAuthGuardian{}
	}
	return &authGuardian{verifier: a.verifier, bundles: a.poller, now: rt.clock, rt: rt,
		denied: func(r string) { a.metrics.Denied.WithLabelValues(r).Inc() }}
}

// run starts the verifier's and the poller's loops under the supervisor and keeps the bundle age
// gauge and the readiness condition current.
func (a *authSetup) run(sup *supervisor, ready *readiness) {
	sup.Go("be.authn.jwks", a.verifier.Run)
	sup.Go("be.authz.bundle", a.poller.Run)
	sup.Go("be.authz.watch", func(ctx context.Context) error {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			if a.poller.Current() != nil {
				ready.Set("bundle", true)
				a.metrics.BundleAge.Set(time.Since(a.poller.LoadedAt()).Seconds())
			}
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
			}
		}
	})
}

// noAuthGuardian serves a component that declares no auth keys: only Public routes exist.
type noAuthGuardian struct{}

func (noAuthGuardian) check(c *gin.Context, g Guard) *problem.Error {
	if guardKey(g) == Public {
		return nil
	}
	return problem.Wrap(fmt.Errorf("protected route %s without AUTHZ_URL / IAM_URL in configSchema", c.FullPath()),
		"INTERNAL", nil)
}
