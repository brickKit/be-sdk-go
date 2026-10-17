// Package host builds and runs one member: the same code path for a component running alone (launcher) and for a
// member of a shell, so the two behave the same (protocol 17: "each member alone and in a shell behaves the same").
package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	sdk "github.com/brickKit/be-sdk-go/v3"
	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/bus"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/runtime"
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

// Keys returns a definition's keys plus the SDK's (and the database keys when the component owns a database).
func Keys(def component.Definition) []config.Key {
	keys := append(append([]config.Key(nil), def.Keys...), SDKKeys()...)
	if def.Migrations != nil {
		keys = append(keys, db.ServeKeys()...)
	}
	if len(def.Streams) > 0 || def.Subscribes {
		keys = append(keys, bus.Keys(len(def.Streams) > 0)...)
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
	state     atomic.Value
	reread    func(string) ([]byte, error)
}

// State returns the member's lifecycle state.
func (m *Member) State() string { return m.state.Load().(string) }

// SetState changes it (the launcher and the shell drive the transitions).
func (m *Member) SetState(s string) { m.state.Store(s) }

// Describe is the member's self-description (P1.8).
func (m *Member) Describe() map[string]any {
	caps := m.Def.Capabilities
	if caps == nil {
		caps = []string{}
	}
	return map[string]any{"component_id": m.Def.ID, "version": m.Def.Version, "protocol": protocol.Version,
		"sdk": "be-sdk-go/" + sdk.Version, "capabilities": caps}
}

// Build constructs the member from parsed values. addresses is where *_ENDPOINT values are looked up; read is used to
// re-read secret files; database is the member's handle on its pool (nil without a database).
func Build(ctx context.Context, def component.Definition, values *config.Values, addresses config.Source,
	read func(string) ([]byte, error), out io.Writer, database *db.DB, transport events.Transport) (*Member, error) {
	if (len(def.Streams) > 0 || def.Subscribes) && (database == nil || transport == nil) {
		return nil, errors.New("events need the component's database (Migrations) and the bus (BUS_URL)")
	}
	member := obs.Member{ID: def.ID, Version: def.Version}
	m := &Member{Def: def, Values: values, Registry: obs.NewRegistry(), reread: read,
		Logger: obs.NewLogger(member, obs.ParseLevel(values.String("LOG_LEVEL")), out)}
	m.state.Store(Starting)
	tp, err := obs.NewTracerProvider(ctx, member, values.String("OTEL_URL"))
	if err != nil {
		return nil, fmt.Errorf("tracing exporter: %w", err)
	}
	m.Tracers = tp
	if m.Outbound, err = calls.NewMember(m.Registry); err != nil {
		return nil, fmt.Errorf("outbound metrics: %w", err)
	}
	var producer *events.Producer
	if len(def.Streams) > 0 {
		if producer, err = events.NewProducer(def.ID, def.Streams, nil); err != nil {
			return nil, err
		}
		m.forwarder = &events.Forwarder{DB: database, Producer: producer, Transport: transport, Source: def.ID,
			Impl: values.String("IMPLEMENTATION_INSTANCE"), Log: m.Logger}
	}
	m.transport = transport
	rt := runtime.New(member, values, m.Logger, m.Registry, tp.Tracer(def.ID), addresses, database, m.Outbound, producer)
	if m.Module, err = newModule(def, rt); err != nil {
		return nil, fmt.Errorf("the module could not be built: %w", err)
	}
	var routes httpx.Routes
	if err := m.Module.Routes(&routes); err != nil {
		return nil, fmt.Errorf("the module's routes could not be registered: %w", err)
	}
	m.Engine, err = httpx.NewEngine(httpx.Deps{Member: member, Logger: m.Logger, Registry: m.Registry, Tracer: rt.Tracer,
		Platform: httpx.Platform{
			Ready:    func() (bool, string, []string) { s := m.State(); return s == Ready, s, nil },
			Describe: func() any { return m.Describe() },
		}}, &routes)
	if err != nil {
		return nil, fmt.Errorf("the user surface could not be built: %w", err)
	}
	if sub, ok := m.Module.(events.Subscriber); ok {
		if !def.Subscribes {
			return nil, errors.New("the module subscribes to events but the definition does not say Subscribes")
		}
		var subs events.Subscriptions
		if err := sub.Subscriptions(&subs); err != nil {
			return nil, fmt.Errorf("the module's subscriptions could not be registered: %w", err)
		}
		for _, s := range subs.List() {
			m.consumers = append(m.consumers, &events.Consumer{Member: def.ID, Sub: s, DB: database, Log: m.Logger})
		}
	}
	if sm, ok := m.Module.(component.SystemModule); ok {
		var services grpcx.Services
		if err := sm.Services(&services); err != nil {
			return nil, fmt.Errorf("the module's services could not be registered: %w", err)
		}
		if !services.Empty() {
			if m.System, err = grpcx.NewServer(grpcx.Deps{Member: member, Logger: m.Logger, Registry: m.Registry,
				Tracer: rt.Tracer}, &services); err != nil {
				return nil, fmt.Errorf("the system surface could not be built: %w", err)
			}
		}
	}
	return m, nil
}

// newModule calls the module entry, turning a panic into an error: a module never takes the process down.
func newModule(def component.Definition, rt *runtime.Runtime) (mod component.Module, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic in New: %v", p)
		}
	}()
	if def.New == nil {
		return nil, errors.New("the definition has no New")
	}
	return def.New(rt)
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

// Stop stops the module's work within ctx, then releases its outbound connections.
func (m *Member) Stop(ctx context.Context) error {
	defer m.Outbound.Close()
	for _, s := range m.subs {
		_ = s.Stop() // the events in hand finish; the rest stay with the transport
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
func ReportConfig(w io.Writer, member string, problems []config.Problem) {
	for _, p := range problems {
		line, _ := json.Marshal(map[string]string{"level": "ERROR", "msg": "configuration", "component": member,
			"key": p.Key, "kind": p.Kind, "expected": p.Expected, "got": p.Got})
		fmt.Fprintln(w, string(line))
	}
}
