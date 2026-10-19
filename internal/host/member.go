// Package host builds and runs one member: the same code path for a component running alone (launcher) and for a
// member of a shell, so the two behave the same (protocol 17: "each member alone and in a shell behaves the same").
package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	authzv3 "github.com/brickKit/contract-infra-authz/v3/gen/go/be/authz/v3"

	protocol "github.com/brickKit/be-protocol/v3"
	sdk "github.com/brickKit/be-sdk-go/v3"
	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/bus"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
)

// States of protocol chapter 01.
const (
	Starting = "starting"
	Ready    = "ready"
	Stopping = "stopping"
)

// Defaults of the SDK's own keys.
const (
	DefaultDrainDelay  = 2 * time.Second
	DefaultStopTimeout = 30 * time.Second
	secretRereadEvery  = 30 * time.Second
)

// SDKKeys are read for every member on top of its own keys.
func SDKKeys() []config.Key {
	return []config.Key{
		{Name: "LOG_LEVEL", Type: config.Enum, Values: []string{"debug", "info", "warn", "error"}, Default: "info"},
		{Name: "OTEL_URL", Type: config.String, Process: true},
		{Name: "STOP_DRAIN_DELAY", Type: config.Duration, Default: DefaultDrainDelay},
		{Name: "STOP_TIMEOUT", Type: config.Duration, Default: DefaultStopTimeout},
	}
}

// SecurityKeys are the identity and authorization keys (protocol chapters 02 and 08); which are required depends on
// what the member serves and calls, checked when it is built.
func SecurityKeys() []config.Key {
	return []config.Key{
		{Name: "DEPLOYMENT_ID", Type: config.String, Process: true},
		{Name: "IAM_ISSUER", Type: config.String, Process: true},
		{Name: "IAM_JWKS_URL", Type: config.String, Process: true},
		{Name: "AUTHZ_GRPC_URL", Type: config.String, Process: true},
		{Name: "AUDIT_GRPC_URL", Type: config.String, Process: true},
		{Name: "SERVICE_TLS_CERT_FILE", Type: config.String},
		{Name: "SERVICE_TLS_KEY_FILE", Type: config.String},
		{Name: "SERVICE_TLS_CA_FILE", Type: config.String},
	}
}

// Keys returns a definition's keys plus the SDK's (and the database keys when the component owns a database).
func Keys(def component.Definition) []config.Key {
	keys := append(append(append([]config.Key(nil), def.Keys...), SDKKeys()...), SecurityKeys()...)
	if def.Migrations != nil {
		keys = append(keys, db.ServeKeys()...)
	}
	if len(def.Streams) > 0 || def.Subscribes {
		keys = append(append(keys, bus.Keys(len(def.Streams) > 0)...), events.StoreKeys()...)
	}
	if def.Subscribes {
		keys = append(keys, events.ConsumerKeys()...)
	}
	return keys
}

// Member is one built member.
type Member struct {
	Def      component.Definition
	Values   *config.Values
	Logger   *slog.Logger
	Registry *prometheus.Registry
	Tracers  *sdktrace.TracerProvider
	Module   component.Module
	Engine   *gin.Engine
	// System is the member's gRPC server; nil when the module registers no services.
	System   *grpc.Server
	Outbound *calls.Member
	// forwarder and consumers run between Start and Stop.
	forwarder *events.Forwarder
	consumers []*events.Consumer
	subs      []events.Subscription
	transport events.Transport
	runner    *work.Runner
	hasJobs   bool
	holder    *authz.Holder
	// definitions submits the declared definitions at start; defsAccepted gates readiness (protected routes only).
	definitions  authzv3.DefinitionServiceClient
	deployment   string
	defsAccepted atomic.Bool
	tls          *identity.ServiceTLS
	state        atomic.Value
	reread       func(string) ([]byte, error)
	// started and onReady are set by MarkReady; announced writes the ready line once (P1.12)
	started   time.Time
	onReady   func()
	announced sync.Once
}

// State returns the member's lifecycle state.
func (m *Member) State() string { return m.state.Load().(string) }

// IsReady reports the member ready: started, and with protected routes its definitions accepted.
func (m *Member) IsReady() bool {
	return m.State() == Ready && (m.holder == nil || m.defsAccepted.Load())
}

// SetState changes it (the launcher and the shell drive the transitions).
func (m *Member) SetState(s string) { m.state.Store(s) }

// MarkReady enters the ready state. The ready line (P1.12) is written, and then is called, when the member is ready:
// at once, or, while its definitions wait for the authorization member, when they are accepted. then may be nil.
func (m *Member) MarkReady(started time.Time, then func()) {
	m.started, m.onReady = started, then
	m.SetState(Ready)
	m.announce()
}

// announce writes the ready line once the member is ready; MarkReady and the accepted submission both call it, so
// whichever comes second writes it.
func (m *Member) announce() {
	if !m.IsReady() {
		return
	}
	m.announced.Do(func() {
		m.Logger.Info("ready", "event", "ready", "protocol", protocol.Version, "sdk", sdk.Version,
			"startup_ms", time.Since(m.started).Milliseconds())
		if m.onReady != nil {
			m.onReady()
		}
	})
}

// Describe is the member's self-description (P1.8).
func (m *Member) Describe() map[string]any {
	caps := m.Def.Capabilities
	if caps == nil {
		caps = []string{}
	}
	return map[string]any{"component_id": m.Def.ID, "version": m.Def.Version, "protocol": protocol.Version,
		"sdk": "be-sdk-go/" + sdk.Version, "capabilities": caps}
}

// Start runs the module's background work (if any), the event forwarder and consumers, and the secret re-reading,
// all bound to bg.
func (m *Member) Start(bg context.Context) error {
	if s, ok := m.Module.(component.Starter); ok {
		if err := s.Start(bg); err != nil {
			return err
		}
	}
	if m.forwarder != nil {
		if err := m.forwarder.Start(bg); err != nil {
			return err
		}
	}
	if m.hasJobs {
		if err := m.runner.Start(bg); err != nil {
			return err
		}
	}
	if m.tls != nil {
		go m.tls.Watch(bg, secretRereadEvery, func(err error) {
			m.Logger.Warn("the service certificate could not be re-read; keeping the previous one", "error", err.Error())
		})
	}
	if m.holder != nil {
		// the definitions first, then one verification before ready, so the first requests find a view; both best
		// effort: the authorization member may be down, and then the member stays not ready until it is back
		first, cancel := context.WithTimeout(bg, 2*time.Second)
		if m.submit(first) {
			if err := m.holder.Verify(first); err != nil {
				m.Logger.Warn("no authorization view yet; protected requests wait for one", "error", err.Error())
			}
			cancel()
			go m.holder.Run(bg)
		} else {
			cancel()
			go m.submitUntilAccepted(bg)
		}
	}
	for _, c := range m.consumers {
		sub, err := m.transport.Subscribe(bg, c.Spec(), c.Handle)
		if err != nil {
			return fmt.Errorf("subscription %s: %w", c.Sub.Name, err)
		}
		m.subs = append(m.subs, sub)
	}
	go m.rereadSecrets(bg)
	return nil
}

// submit sends the declared definitions once (DefinitionService.Submit) and reports whether they were accepted.
func (m *Member) submit(ctx context.Context) bool {
	err := m.Def.Authz.Submit(ctx, m.definitions, m.deployment, m.Def.Version)
	if err == nil {
		m.defsAccepted.Store(true)
		m.announce()
		return true
	}
	var rejected *authz.RejectedError
	if errors.As(err, &rejected) {
		m.Logger.Error("the authorization member refused this version's definitions; the member stays not ready", "error", err.Error())
	} else {
		m.Logger.Warn("the definitions could not be submitted yet; the member stays not ready", "error", err.Error())
	}
	return false
}

// submitUntilAccepted retries the submission with a growing pause, then starts the view holder.
func (m *Member) submitUntilAccepted(bg context.Context) {
	pause := time.Second
	for bg.Err() == nil {
		select {
		case <-bg.Done():
			return
		case <-time.After(pause):
		}
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		ok := m.submit(ctx)
		if ok {
			_ = m.holder.Verify(ctx)
		}
		cancel()
		if ok {
			m.holder.Run(bg)
			return
		}
		pause = min(pause*2, 30*time.Second)
	}
}

// Stop stops the module's work within ctx, then releases its outbound connections.
func (m *Member) Stop(ctx context.Context) error {
	defer m.Outbound.Close()
	for _, s := range m.subs {
		_ = s.Stop() // the events in hand finish; the rest stay with the transport
	}
	if m.hasJobs {
		m.runner.Stop() // handlers were cancelled with the background context; their leases are released
	}
	if s, ok := m.Module.(component.Stopper); ok {
		return s.Stop(ctx)
	}
	return nil
}

func (m *Member) rereadSecrets(ctx context.Context) {
	secrets := m.Values.Secrets()
	if len(secrets) == 0 || m.reread == nil {
		return
	}
	t := time.NewTicker(secretRereadEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, s := range secrets {
				if changed, err := s.Refresh(m.reread); err != nil {
					m.Logger.Warn("a secret file could not be re-read; keeping the previous value", "error", err.Error())
				} else if changed {
					m.Logger.Info("a secret file changed; new connections use the new value")
				}
			}
		}
	}
}

// Gin's mode is process-wide: the SDK sets it once here (the launcher and the shell both import host), never a module.
func init() { gin.SetMode(gin.ReleaseMode) }

// ReportConfig writes configuration problems one JSON line each (P2.6), naming the member.
func ReportConfig(w io.Writer, member obs.Member, problems []config.Problem) {
	log := obs.NewLogger(member, slog.LevelInfo, w)
	for _, p := range problems {
		log.Error("configuration", "key", p.Key, "kind", p.Kind, "expected", p.Expected, "got", p.Got)
	}
}
