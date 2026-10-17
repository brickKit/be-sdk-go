package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"time"

	"github.com/brickKit/be-sdk-go/v3/audit"
	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/guard"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/idem"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/work"
)

// Shared is what the launcher or the shell hands every member: process-level resources and the member's own
// database handle.
type Shared struct {
	Addresses config.Source                // where *_ENDPOINT values are looked up
	Read      func(string) ([]byte, error) // re-reads secret and certificate files
	Out       io.Writer
	DB        *db.DB           // the member's database; nil without one
	Transport events.Transport // the process's bus; nil without one
	Keys      *identity.KeySet // the process's identity keys; nil when IAM_JWKS_URL is not set
}

// OpenKeys creates the process's identity key set when IAM_JWKS_URL is set; a failing first fetch is reported but
// the set keeps retrying (the identity member may start later).
func OpenKeys(ctx context.Context, values *config.Values) (*identity.KeySet, error) {
	url := values.String("IAM_JWKS_URL")
	if url == "" {
		return nil, nil
	}
	return identity.NewKeySet(ctx, url)
}

// ConfigError is a build failure caused by missing or inconsistent configuration (exit 78).
type ConfigError struct{ Problems []config.Problem }

func (e *ConfigError) Error() string { return fmt.Sprintf("configuration: %v", e.Problems) }

// Build constructs the member from parsed values.
func Build(ctx context.Context, def component.Definition, values *config.Values, sh Shared) (*Member, error) {
	if (len(def.Streams) > 0 || def.Subscribes) && (sh.DB == nil || sh.Transport == nil) {
		return nil, errors.New("events need the component's database (Migrations) and the bus (BUS_URL)")
	}
	member := obs.Member{ID: def.ID, Version: def.Version}
	m := &Member{Def: def, Values: values, Registry: obs.NewRegistry(), reread: sh.Read,
		Logger: obs.NewLogger(member, obs.ParseLevel(values.String("LOG_LEVEL")), sh.Out)}
	m.state.Store(Starting)
	tp, err := obs.NewTracerProvider(ctx, member, values.String("OTEL_URL"))
	if err != nil {
		return nil, fmt.Errorf("tracing exporter: %w", err)
	}
	m.Tracers = tp
	if m.Outbound, err = calls.NewMember(m.Registry); err != nil {
		return nil, fmt.Errorf("outbound metrics: %w", err)
	}
	if err := m.serviceIdentity(values, sh); err != nil {
		return nil, err
	}
	producer, err := m.producer(values, sh)
	if err != nil {
		return nil, err
	}
	var auditor *audit.Auditor
	if sh.DB != nil {
		m.runner = work.NewRunner(sh.DB, def.ID, m.Logger)
		auditor = audit.New(sh.DB)
		if err := m.auditTransfer(auditor, values); err != nil {
			return nil, err
		}
		if err := m.runner.Add(partitionJob(m.Logger)); err != nil {
			return nil, err
		}
		for _, j := range []work.Job{idem.Job(sh.DB), events.PurgeJob(sh.DB), work.PurgeJob(sh.DB)} {
			if err := m.runner.Add(j); err != nil {
				return nil, err
			}
		}
	}
	rt := runtime.New(runtime.Parts{Member: member, Config: values, Logger: m.Logger, Metrics: m.Registry,
		Tracer: tp.Tracer(def.ID), Addresses: sh.Addresses, DB: sh.DB, Outbound: m.Outbound, Events: producer,
		Work: m.runner, Audit: auditor})
	if m.Module, err = newModule(def, rt); err != nil {
		return nil, fmt.Errorf("the module could not be built: %w", err)
	}
	if err := m.userSurface(member, values, sh, rt); err != nil {
		return nil, err
	}
	if err := m.background(sh); err != nil {
		return nil, err
	}
	if err := m.systemSurface(member, rt); err != nil {
		return nil, err
	}
	return m, nil
}

// auditTransfer registers the audit transfer job; without AUDIT_GRPC_URL the records stay local (P8.21).
func (m *Member) auditTransfer(a *audit.Auditor, values *config.Values) error {
	var sender *audit.GRPCSender
	if addr := values.String("AUDIT_GRPC_URL"); addr != "" {
		conn, err := m.Outbound.GRPC("infra/audit", hostPort(addr))
		if err != nil {
			return err
		}
		sender = &audit.GRPCSender{Conn: conn}
	} else {
		m.Logger.Warn("AUDIT_GRPC_URL is not set: audit records stay in the local table until it is")
	}
	if err := m.runner.Add(a.Job(sender, m.Logger)); err != nil {
		return err
	}
	m.hasJobs = true
	return nil
}

// partitionJob keeps the declared tables' monthly partitions ahead (P5.24); a boundary anomaly fails the run.
func partitionJob(log *slog.Logger) work.Job {
	return work.Job{Name: "be.partitions.maintain", Kind: work.Singleton, Effect: work.Local, Interval: time.Hour,
		Handle: func(ctx context.Context, run *work.Run) error {
			return run.Complete(ctx, func(ctx context.Context, tx db.Tx) error {
				created, err := db.MaintainPartitions(ctx, tx)
				if err != nil {
					return fmt.Errorf("maintaining partitions: %w", err)
				}
				if len(created) > 0 {
					log.Info("partitions created", "event", "partitions_created", "partitions", created)
				}
				return nil
			})
		}}
}

// serviceIdentity loads the member's mTLS identity when its certificate is configured (P8.5).
func (m *Member) serviceIdentity(values *config.Values, sh Shared) error {
	cert, key, ca := values.String("SERVICE_TLS_CERT_FILE"), values.String("SERVICE_TLS_KEY_FILE"), values.String("SERVICE_TLS_CA_FILE")
	if cert == "" && key == "" && ca == "" {
		return nil
	}
	var problems []config.Problem
	for name, v := range map[string]string{"SERVICE_TLS_CERT_FILE": cert, "SERVICE_TLS_KEY_FILE": key,
		"SERVICE_TLS_CA_FILE": ca, "DEPLOYMENT_ID": values.String("DEPLOYMENT_ID")} {
		if v == "" {
			problems = append(problems, config.Problem{Key: name, Kind: "missing", Expected: "set together with the other SERVICE_TLS_* keys"})
		}
	}
	if len(problems) > 0 {
		return &ConfigError{problems}
	}
	t, err := identity.LoadServiceTLS(values.String("DEPLOYMENT_ID"), m.Def.ID, cert, key, ca, sh.Read)
	if err != nil {
		return err
	}
	m.tls = t
	m.Outbound.SetTLS(t)
	return nil
}

func (m *Member) producer(values *config.Values, sh Shared) (*events.Producer, error) {
	m.transport = sh.Transport
	if len(m.Def.Streams) == 0 {
		return nil, nil
	}
	p, err := events.NewProducer(m.Def.ID, m.Def.Streams, nil)
	if err != nil {
		return nil, err
	}
	m.forwarder = &events.Forwarder{DB: sh.DB, Producer: p, Transport: sh.Transport, Source: m.Def.ID,
		Impl: values.String("IMPLEMENTATION_INSTANCE"), Log: m.Logger}
	return p, nil
}

func (m *Member) userSurface(member obs.Member, values *config.Values, sh Shared, rt *runtime.Runtime) error {
	var routes httpx.Routes
	if err := m.Module.Routes(&routes); err != nil {
		return fmt.Errorf("the module's routes could not be registered: %w", err)
	}
	var ops []string
	for _, r := range routes.List() {
		if !r.Public {
			ops = append(ops, r.Operation)
		}
	}
	var g httpx.Guard
	if len(ops) > 0 {
		var err error
		if g, err = m.guard(values, sh, ops); err != nil {
			return err
		}
	}
	var err error
	m.Engine, err = httpx.NewEngine(httpx.Deps{Member: member, Logger: m.Logger, Registry: m.Registry, Tracer: rt.Tracer,
		Guard: g, Platform: httpx.Platform{
			Ready:    func() (bool, string, []string) { s := m.State(); return s == Ready, s, nil },
			Describe: func() any { return m.Describe() },
		}}, &routes)
	if err != nil {
		return fmt.Errorf("the user surface could not be built: %w", err)
	}
	return nil
}

// guard builds the admission of protected routes: declared operations, the identity keys, and a view holder that
// verifies with the authorization member over mTLS (P8.7, P8.9).
func (m *Member) guard(values *config.Values, sh Shared, ops []string) (httpx.Guard, error) {
	if m.Def.Authz == nil {
		return nil, errors.New("the module has protected routes but the definition declares no resources and operations (Authz)")
	}
	if err := m.Def.Authz.Validate(); err != nil {
		return nil, err
	}
	if err := guard.CheckOperations(m.Def.Authz, ops); err != nil {
		return nil, err
	}
	var problems []config.Problem
	need := func(key, why string) {
		if values.String(key) == "" {
			problems = append(problems, config.Problem{Key: key, Kind: "missing", Expected: why})
		}
	}
	need("DEPLOYMENT_ID", "the deployment identity: protected routes accept its audience")
	need("IAM_ISSUER", "the identity member's issuer: protected routes verify tokens")
	need("IAM_JWKS_URL", "the identity member's keys: protected routes verify tokens")
	need("AUTHZ_GRPC_URL", "the authorization member's system surface: protected routes need a view")
	if m.tls == nil {
		problems = append(problems, config.Problem{Key: "SERVICE_TLS_CERT_FILE", Kind: "missing",
			Expected: "the member's service identity: the view is verified over mTLS"})
	}
	if len(problems) > 0 || sh.Keys == nil {
		return nil, &ConfigError{problems}
	}
	conn, err := m.Outbound.GRPC("infra/authz", hostPort(values.String("AUTHZ_GRPC_URL")))
	if err != nil {
		return nil, err
	}
	m.holder = authz.NewHolder(m.Def.ID, authz.GRPCSource{Conn: conn}, m.Logger)
	return &guard.User{Defs: m.Def.Authz, Holder: m.holder, Log: m.Logger,
		Verifier: &identity.Verifier{Issuer: values.String("IAM_ISSUER"), Audience: "be:deployment:" + values.String("DEPLOYMENT_ID"),
			Keys: sh.Keys}}, nil
}

// hostPort accepts an address as BrickKit derives it (http://service:port) or a bare host:port.
func hostPort(addr string) string {
	if u, err := url.Parse(addr); err == nil && u.Host != "" {
		return u.Host
	}
	return addr
}

func (m *Member) background(sh Shared) error {
	if wk, ok := m.Module.(work.Worker); ok {
		if m.runner == nil {
			return errors.New("the module has background jobs but the component has no database (Migrations)")
		}
		var jobs work.Jobs
		if err := wk.Jobs(&jobs); err != nil {
			return fmt.Errorf("the module's jobs could not be registered: %w", err)
		}
		if err := m.runner.AddAll(&jobs); err != nil {
			return err
		}
		m.hasJobs = true
	}
	if sub, ok := m.Module.(events.Subscriber); ok {
		if !m.Def.Subscribes {
			return errors.New("the module subscribes to events but the definition does not say Subscribes")
		}
		var subs events.Subscriptions
		if err := sub.Subscriptions(&subs); err != nil {
			return fmt.Errorf("the module's subscriptions could not be registered: %w", err)
		}
		for _, s := range subs.List() {
			m.consumers = append(m.consumers, &events.Consumer{Member: m.Def.ID, Sub: s, DB: sh.DB, Log: m.Logger})
		}
	}
	return nil
}

func (m *Member) systemSurface(member obs.Member, rt *runtime.Runtime) error {
	sm, ok := m.Module.(component.SystemModule)
	if !ok {
		return nil
	}
	var services grpcx.Services
	if err := sm.Services(&services); err != nil {
		return fmt.Errorf("the module's services could not be registered: %w", err)
	}
	if services.Empty() {
		return nil
	}
	if m.Def.Authz == nil {
		return errors.New("the module has system methods but the definition declares no operations (Authz)")
	}
	if err := m.Def.Authz.Validate(); err != nil {
		return err
	}
	if err := guard.CheckOperations(m.Def.Authz, services.Operations()); err != nil {
		return err
	}
	var err error
	if m.System, err = grpcx.NewServer(grpcx.Deps{Member: member, Logger: m.Logger, Registry: m.Registry,
		Tracer: rt.Tracer, TLS: m.tls}, &services); err != nil {
		return fmt.Errorf("the system surface could not be built: %w", err)
	}
	return nil
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
