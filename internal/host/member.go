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
	"github.com/brickKit/be-sdk-go/v3/extcmd"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/runprofile"
	"github.com/brickKit/be-sdk-go/v3/tiering"
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
	// RecoveryReady is ready in recovery mode (P1.22): serving what the phase allows.
	RecoveryReady = "recovery_ready"
)

// RecoveryTool is the full recovery tool's service identity (be-ops recover): the only caller of the replay
// (P12.19), and the evidence store's reader.
const RecoveryTool = "ops/recover"

// The recovery phases (P1.23): a deployment variable read at start; a phase change restarts.
const (
	PhaseFrozen       = "frozen"
	PhaseRedelivering = "redelivering"
	PhaseReplaying    = "replaying"
	PhaseVerifying    = "verifying" // steps 7-9: external commands query only; the checks over the system surface
	PhaseReadOnly     = "read_only" // the read-only opening: reads, and the routes that declare ReadOnlyOpen
)

// running are the phases events and projections run in (P1.23): all but frozen.
func running(phase string) bool { return phase != PhaseFrozen }

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
		{Name: "RECOVERY_PHASE", Type: config.Enum, Values: []string{PhaseFrozen, PhaseRedelivering, PhaseReplaying,
			PhaseVerifying, PhaseReadOnly}, Process: true},
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
	if def.Evidence {
		keys = append(keys, runprofile.Keys()...)
	}
	if def.ExternalCommands {
		keys = append(keys, extcmd.Keys()...)
	}
	if len(def.Tiering) > 0 {
		keys = append(keys, tiering.Keys(def.Tiering)...)
		if !def.Evidence { // the release reads the backup records (P15.21)
			keys = append(keys, runprofile.Keys()...)
		}
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
	watermark *events.WatermarkSource // answers pulls when the member has watermark streams
	records   *events.RecordSource    // answers batch reads and snapshots when the member's streams declare them
	resender  *events.Resender        // answers redeliveries when the member has streams (P6.38)
	consumers []*events.Consumer
	redeliver *events.Redelivery // asks the producers for a redelivery when the database went back (P6.39)
	subs      []events.Subscription
	transport events.Transport
	runner    *work.Runner
	hasJobs   bool
	// external are the member's external commands; in verifying and read_only they only query (P13.10, P1.23).
	external *extcmd.Commands
	// loops are the SDK's work that runs outside the job runner in some recovery phases (P1.23); looping ends with
	// the background context.
	loops   []loop
	looping sync.WaitGroup
	// db keeps the statistics of the SDK's tables fresh between Start and Stop (P5.33); nil without a database.
	db         *db.DB
	statistics chan struct{}
	holder     *authz.Holder
	// definitions submits the declared definitions at start; readiness does not wait for them (P8.24), and until
	// defsAccepted the view holder does not run, so protected requests answer AUTHORIZATION_VIEW_EXPIRED.
	definitions  authzv3.DefinitionServiceClient
	deployment   string
	defsAccepted atomic.Bool
	tls          *identity.ServiceTLS
	state        atomic.Value
	reread       func(string) ([]byte, error)
}

// State returns the member's lifecycle state.
func (m *Member) State() string { return m.state.Load().(string) }

// IsReady reports the member ready, or ready in recovery mode (P1.22).
func (m *Member) IsReady() bool { s := m.State(); return s == Ready || s == RecoveryReady }

// Phase is the recovery phase the member started in; empty: normal running (P1.22).
func (m *Member) Phase() string { return m.Values.String("RECOVERY_PHASE") }

// SetState changes it (the launcher and the shell drive the transitions).
func (m *Member) SetState(s string) { m.state.Store(s) }

// MarkReady enters the ready state, writes the ready line (P1.12) and then calls then (may be nil).
func (m *Member) MarkReady(started time.Time, then func()) {
	if phase := m.Phase(); phase != "" {
		m.SetState(RecoveryReady)
		m.Logger.Info("ready", "event", "ready", "protocol", protocol.Version, "sdk", sdk.Version,
			"startup_ms", time.Since(started).Milliseconds(), "phase", phase)
	} else {
		m.SetState(Ready)
		m.Logger.Info("ready", "event", "ready", "protocol", protocol.Version, "sdk", sdk.Version,
			"startup_ms", time.Since(started).Milliseconds())
	}
	if then != nil {
		then()
	}
}

// Describe is the member's self-description (P1.8).
func (m *Member) Describe() map[string]any {
	caps := m.Def.Capabilities
	if caps == nil {
		caps = []string{}
	}
	out := map[string]any{"component_id": m.Def.ID, "version": m.Def.Version, "protocol": protocol.Version,
		"sdk": "be-sdk-go/" + sdk.Version, "capabilities": caps}
	if phase := m.Phase(); phase != "" {
		out["recovery_phase"] = phase
	}
	return out
}

// Start runs the module's background work (if any), the event forwarder and consumers, and the secret re-reading,
// all bound to bg.
//
// In recovery mode only what the phase allows runs (P1.23): frozen runs no module background, forwarding,
// subscription or job; redelivering runs the module's background (its projections), forwarding and subscriptions,
// and no job; replaying adds the owners' replay; verifying and read_only run the external commands query only, outside
// the job runner (P13.10). The system surface and the authorization view run in every phase.
func (m *Member) Start(bg context.Context) error {
	phase := m.Phase()
	moving := running(phase) // events and projections run
	if s, ok := m.Module.(component.Starter); ok && moving {
		if err := s.Start(bg); err != nil {
			return err
		}
	}
	if m.forwarder != nil && moving {
		if err := m.forwarder.Start(bg); err != nil {
			return err
		}
	}
	if m.hasJobs && phase == "" {
		if err := m.runner.Start(bg); err != nil {
			return err
		}
	}
	if m.external != nil && (phase == PhaseVerifying || phase == PhaseReadOnly) {
		m.external.QueryOnly = true // what is unknown is asked, nothing is sent (P13.10)
		m.loops = append(m.loops, loop{name: "the external commands", every: extcmd.RunEvery,
			in:  func(string) bool { return true },
			run: func(ctx context.Context) error { _, err := m.external.Run(ctx); return err }})
	}
	for _, l := range m.loops {
		if l.in(phase) {
			m.looping.Add(1)
			go func() { defer m.looping.Done(); m.runLoop(bg, l) }()
		}
	}
	if m.db != nil {
		m.statistics = make(chan struct{})
		go func() { defer close(m.statistics); db.KeepStatistics(bg, m.db, m.Logger) }()
	}
	if m.tls != nil {
		go m.tls.Watch(bg, secretRereadEvery, func(err error) {
			m.Logger.Warn("the service certificate could not be re-read; keeping the previous one", "error", err.Error())
		})
	}
	if m.holder != nil {
		// the definitions first, then one verification before ready, so the first requests find a view; both best
		// effort: the authorization member may be down or, on Kubernetes, reachable only once this member (or its
		// shell) is ready, so readiness does not wait (P8.24); the submission goes on in the background
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
		if !moving {
			break
		}
		if err := events.StartRun(bg, c.DB, c.Sub.Name); err != nil { // a run of this subscription (P6.36)
			return fmt.Errorf("subscription %s: %w", c.Sub.Name, err)
		}
		sub, err := m.transport.Subscribe(bg, c.Spec(), c.Handle)
		if err != nil {
			return fmt.Errorf("subscription %s: %w", c.Sub.Name, err)
		}
		m.subs = append(m.subs, sub)
	}
	if m.redeliver != nil && moving {
		go m.redeliver.Run(bg)
	}
	go m.rereadSecrets(bg)
	return nil
}

// submit sends the declared definitions once (DefinitionService.Submit) and reports whether they were accepted.
func (m *Member) submit(ctx context.Context) bool {
	err := m.Def.Authz.Submit(ctx, m.definitions, m.deployment, m.Def.Version)
	if err == nil {
		if m.defsAccepted.CompareAndSwap(false, true) {
			m.Logger.Info("definitions accepted", "event", "definitions_accepted")
		}
		return true
	}
	var rejected *authz.RejectedError
	if errors.As(err, &rejected) {
		m.Logger.Error("the authorization member refused this version's definitions; protected requests wait for a view", "error", err.Error())
	} else {
		m.Logger.Warn("the definitions could not be submitted yet; protected requests wait for a view", "error", err.Error())
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
	if m.forwarder != nil {
		m.forwarder.Wait() // ended with the background context; its leases given up (P6.11)
	}
	if m.statistics != nil {
		<-m.statistics // ends with the background context
	}
	m.looping.Wait() // ends with the background context
	if s, ok := m.Module.(component.Stopper); ok {
		return s.Stop(ctx)
	}
	return nil
}

// loop is SDK work the host runs itself, outside the job runner, in the recovery phases in reports true for.
type loop struct {
	name  string
	every time.Duration
	in    func(phase string) bool
	run   func(ctx context.Context) error
}

// runLoop runs l every l.every until the background context ends.
func (m *Member) runLoop(bg context.Context, l loop) {
	for {
		if err := l.run(bg); err != nil && bg.Err() == nil {
			m.Logger.Warn(l.name+" failed in the recovery phase; trying again", "error", err.Error())
		}
		select {
		case <-bg.Done():
			return
		case <-time.After(l.every):
		}
	}
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
