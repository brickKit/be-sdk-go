package host

import (
	"context"
	"errors"
	"fmt"
	"github.com/brickKit/be-sdk-go/v3/flow"
	"google.golang.org/grpc"
	"io"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/audit"
	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/extcmd"
	"github.com/brickKit/be-sdk-go/v3/externalpb"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/guard"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/idem"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/internal/evidencepb"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/recovery"
	"github.com/brickKit/be-sdk-go/v3/recoverypb"
	"github.com/brickKit/be-sdk-go/v3/runprofile"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
	authzv3 "github.com/brickKit/contract-infra-authz/v3/gen/go/be/authz/v3"
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
	var writer *evidence.Writer
	if sh.DB != nil {
		m.db = sh.DB
		m.runner = work.NewRunner(sh.DB, def.ID, m.Logger)
		auditor = audit.New(sh.DB)
		auditor.Restricted, auditor.Strict = auditPolicy(def.Authz)
		if err := m.auditTransfer(auditor, values); err != nil {
			return nil, err
		}
		if writer, err = m.evidenceWriter(values, sh); err != nil {
			return nil, err
		}
		if err := m.addSDKJob(partitionJob(m.Logger)); err != nil {
			return nil, err
		}
		for _, j := range []work.Job{idem.Job(sh.DB), events.PurgeJob(sh.DB), work.PurgeJob(sh.DB)} {
			if err := m.addSDKJob(j); err != nil {
				return nil, err
			}
		}
	}
	var external *extcmd.Commands
	if def.ExternalCommands {
		if sh.DB == nil {
			return nil, errors.New("the definition says ExternalCommands but the component has no database (Migrations)")
		}
		external = &extcmd.Commands{DB: sh.DB, Component: def.ID, Evidence: writer,
			QueryOnly: values.Bool(extcmd.QueryOnlyKey), Log: m.Logger}
		m.external = external
	}
	var catalog *tiering.Catalog
	if len(def.Tiering) > 0 { // the data classes (be-protocol chapter 15): checked at start (P15.1-P15.5)
		if sh.DB == nil {
			return nil, errors.New("the definition declares tiering classes but the component has no database (Migrations)")
		}
		if catalog, err = tiering.NewCatalog(ctx, sh.DB, def.Tiering, values, strings.ReplaceAll(def.ID, "/", "-")); err != nil {
			return nil, err
		}
		if err := tieringOperations(def); err != nil {
			return nil, err
		}
		for _, name := range catalog.PendingRetentions() { // never destroyed until the customer sets it (P15.4, TR24)
			m.Logger.Warn("a data class has no retention: never destroyed; set it at go-live", "event",
				"tiering_retention_pending", "class", name, "key", tiering.RetentionKey(name))
		}
		if values.String("RUN_PROFILE_URL") != "" && m.tls != nil { // when the latest backup finished (P15.21)
			rp, err := runprofile.New(runprofile.Options{Endpoint: values.String("RUN_PROFILE_URL"),
				Region: values.String("RUN_PROFILE_REGION"), Bucket: values.String("RUN_PROFILE_BUCKET"),
				AccessKey: values.String("RUN_PROFILE_ACCESS_KEY"), Secret: func() string { return values.String("RUN_PROFILE_SECRET_KEY") },
				Deployment: values.String("DEPLOYMENT_ID"), Writer: runprofile.Writer{Component: def.ID, Instance: def.ID},
				Identity: m.tls})
			if err != nil {
				return nil, err
			}
			catalog.Backups = tiering.BackupsFrom(rp)
		}
		catalog.Log = m.Logger
		catalog.Evidence = writer          // the destructions' evidence (P15.29); their replay is registered with the module's
		catalog.Enqueue = m.runner.Enqueue // the history exports (P15.38), under the requester's authorization then
		catalog.Subject = func(principal string) *authz.Subject {
			if m.holder == nil {
				return nil
			}
			return &authz.Subject{Principal: principal, Defs: m.Def.Authz, Holder: m.holder, At: time.Now()}
		}
		for _, j := range []work.Job{catalog.CandidatesJob(values.String(tiering.KeyWindow), m.Logger),
			catalog.TransferJob(m.Logger), catalog.ReleaseJob(m.Logger), catalog.ExportJobOf(m.Logger),
			catalog.ExportCleanupJob(m.Logger), catalog.DestroyJob(m.Logger), catalog.EraseJobOf(m.Logger),
			catalog.CleanupJob(m.Logger)} {
			if err := m.addSDKJob(j); err != nil {
				return nil, err
			}
		}
		m.hasJobs = true
	}
	rt := runtime.New(runtime.Parts{Member: member, Config: values, Logger: m.Logger, Metrics: m.Registry,
		Tracer: tp.Tracer(def.ID), Addresses: sh.Addresses, DB: sh.DB, Outbound: m.Outbound, Events: producer,
		Work: m.runner, Audit: auditor, Evidence: writer, External: external, Tiering: catalog,
		Subject: func(principal string) *authz.Subject {
			if m.holder == nil {
				return nil
			}
			return &authz.Subject{Principal: principal, Defs: m.Def.Authz, Holder: m.holder, At: time.Now()}
		}})
	if m.Module, err = newModule(def, rt); err != nil {
		return nil, fmt.Errorf("the module could not be built: %w", err)
	}
	if err := m.userSurface(member, values, sh, rt); err != nil {
		return nil, err
	}
	if err := m.background(sh, rt); err != nil {
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
		a.Sender = sender // a strict read's receipt (P8.19)
	} else {
		m.Logger.Warn("AUDIT_GRPC_URL is not set: audit records stay in the local table until it is")
	}
	if err := m.addSDKJob(a.Job(sender, m.Logger)); err != nil {
		return err
	}
	m.hasJobs = true
	// in the recovery phases but frozen no job runs, yet the audit store never goes back (P1.23)
	m.loops = append(m.loops, loop{name: "the audit transfer", every: audit.TransferEvery,
		in:  func(phase string) bool { return phase != "" && phase != PhaseFrozen },
		run: func(ctx context.Context) error { return a.Cycle(ctx, sender, m.Logger) }})
	return nil
}

// auditPolicy is what the auditor takes from the member's definitions: the restricted fields of each resource type
// (their values become digests in a record's changes) and the operations of the strict tier (P8.19).
func auditPolicy(defs *authz.Definitions) (map[string]map[string]bool, map[string]bool) {
	restricted, strict := map[string]map[string]bool{}, map[string]bool{}
	if defs == nil {
		return restricted, strict
	}
	for _, r := range defs.Resources {
		for name, f := range r.Fields {
			if f.Class == authz.Restricted {
				if restricted[r.Type] == nil {
					restricted[r.Type] = map[string]bool{}
				}
				restricted[r.Type][name] = true
			}
		}
	}
	for _, o := range defs.Operations {
		if o.StrictAudit {
			strict[o.Key] = true
		}
	}
	return restricted, strict
}

// tieringOperations checks that a class's hold and approval operations are the component's (P15.1, P15.28, P15.29).
func tieringOperations(def component.Definition) error {
	for _, cl := range def.Tiering {
		for _, op := range []string{cl.HoldOperation, cl.ApproveOperation} {
			if op == "" {
				continue
			}
			if def.Authz == nil {
				return fmt.Errorf("%s: the operation %s needs the component's authorization definitions (Definition.Authz)", cl.Name, op)
			}
			if !slices.ContainsFunc(def.Authz.Operations, func(o authz.Operation) bool { return o.Key == op }) {
				return fmt.Errorf("%s: the operation %s is not declared in the component's authorization definitions", cl.Name, op)
			}
		}
	}
	return nil
}

// evidenceWriter builds the member's evidence writer and registers be.evidence.transfer (P12.15): the evidence store
// is the audit component's EvidenceService at AUDIT_GRPC_URL; the alert channel is the run profile, for the
// components that declare Evidence and configure RUN_PROFILE_URL.
func (m *Member) evidenceWriter(values *config.Values, sh Shared) (*evidence.Writer, error) {
	instance := m.Def.ID
	if len(m.Def.Streams) > 0 {
		instance = values.String("IMPLEMENTATION_INSTANCE")
	}
	w := &evidence.Writer{DB: sh.DB, Component: m.Def.ID, Instance: instance, Log: m.Logger}
	if addr := values.String("AUDIT_GRPC_URL"); addr != "" {
		conn, err := m.Outbound.GRPC("infra/audit", hostPort(addr))
		if err != nil {
			return nil, err
		}
		w.Store = evidence.GRPCStore{Conn: conn}
	}
	if m.Def.Evidence && values.String("RUN_PROFILE_URL") != "" && m.tls != nil {
		rp, err := runprofile.New(runprofile.Options{Endpoint: values.String("RUN_PROFILE_URL"),
			Region: values.String("RUN_PROFILE_REGION"), Bucket: values.String("RUN_PROFILE_BUCKET"),
			AccessKey: values.String("RUN_PROFILE_ACCESS_KEY"), Secret: func() string { return values.String("RUN_PROFILE_SECRET_KEY") },
			Deployment: values.String("DEPLOYMENT_ID"), Writer: runprofile.Writer{Component: m.Def.ID, Instance: instance},
			Identity: m.tls, Webhook: values.String("ALERT_WEBHOOK_URL"),
			Log: func(msg string, args ...any) { m.Logger.Error(msg, args...) }})
		if err != nil {
			return nil, err
		}
		w.Alert = rp
	}
	if err := m.addSDKJob(evidence.Job(sh.DB, w.Store, m.Logger)); err != nil {
		return nil, err
	}
	m.hasJobs = true
	// in the recovery phases but frozen no job runs, yet the evidence store never goes back: a conclusion written
	// during the recovery reaches it before the replay (P1.23)
	if w.Store != nil {
		m.loops = append(m.loops, loop{name: "the evidence transfer", every: evidence.TransferEvery,
			in: func(phase string) bool { return phase != "" && phase != PhaseFrozen },
			run: func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, 30*time.Second) // calls need a deadline (P4.4)
				defer cancel()
				_, err := evidence.Transfer(ctx, sh.DB, w.Store, m.Logger)
				return err
			}})
	}
	return w, nil
}

// partitionJob keeps the declared tables' monthly partitions ahead (P5.24); a boundary anomaly fails the run.
func partitionJob(log *slog.Logger) work.Job {
	return work.Job{Name: "be.partitions.maintain", Kind: work.Singleton, Effect: work.Local, Interval: time.Hour,
		MaxAttempts: work.Unlimited,
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
	store, err := events.NewS3Store(values)
	if err != nil {
		return nil, err
	}
	var claims events.ClaimStore
	if store != nil {
		claims = store
	}
	p, err := events.NewProducer(m.Def.ID, m.Def.Streams, claims)
	if err != nil {
		return nil, err
	}
	m.forwarder = &events.Forwarder{DB: sh.DB, Producer: p, Transport: sh.Transport, Source: m.Def.ID,
		Impl: values.String("IMPLEMENTATION_INSTANCE"), Log: m.Logger}
	m.watermark = &events.WatermarkSource{DB: sh.DB, Producer: p, Source: m.Def.ID, Impl: m.forwarder.Impl,
		Deployment: values.String("DEPLOYMENT_ID"), Log: m.Logger}
	m.records = &events.RecordSource{DB: sh.DB, Producer: p, Impl: m.forwarder.Impl,
		Deployment: values.String("DEPLOYMENT_ID"), Log: m.Logger}
	m.resender = &events.Resender{DB: sh.DB, Producer: p, Transport: sh.Transport, Source: m.Def.ID,
		Impl: m.forwarder.Impl, Log: m.Logger}
	return p, nil
}

func (m *Member) userSurface(member obs.Member, values *config.Values, sh Shared, rt *runtime.Runtime) error {
	var routes httpx.Routes
	if err := m.Module.Routes(&routes); err != nil {
		return fmt.Errorf("the module's routes could not be registered: %w", err)
	}
	prefix := m.Def.Prefix()
	if rt.Tiering != nil { // holds, approvals and export status of the classes (P15.40)
		rt.Tiering.Routes(&routes, prefix)
	}
	if fo, ok := m.Module.(flow.Owner); ok && fo.FlowKernel() != nil { // the flow kernel's standard routes (P17.14)
		if prefix != component.DefaultPrefix(m.Def.ID) {
			return fmt.Errorf("a flow manager's public prefix is %s, not %s: the worklist derives the decision "+
				"entry from the component (be-protocol P11.11, P17.14)", component.DefaultPrefix(m.Def.ID), prefix)
		}
		fo.FlowKernel().Routes(&routes)
	}
	for _, r := range routes.List() { // every user-surface route under the public prefix (P11.11)
		if !component.UnderPrefix(prefix, r.Path) {
			return fmt.Errorf("the route %s %s is not under the component's public prefix %s (be-protocol P11.11)",
				r.Method, r.Path, prefix)
		}
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
		Guard: g, Phase: m.Phase(), Platform: httpx.Platform{
			Ready: func() (bool, string, []string) {
				return m.IsReady(), m.State(), nil
			},
			Describe: func() any { return m.Describe() },
			Phase:    m.Phase,
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
	m.holder.Defs = m.Def.Authz
	m.definitions = authzv3.NewDefinitionServiceClient(conn)
	m.deployment = values.String("DEPLOYMENT_ID")
	return &guard.User{Defs: m.Def.Authz, Holder: m.holder, Log: m.Logger,
		Verifier: &identity.Verifier{Issuer: values.String("IAM_ISSUER"), Audience: "be:deployment:" + values.String("DEPLOYMENT_ID"),
			Keys: sh.Keys}}, nil
}

// reconcileTimeout bounds the external reconciliation: a listing, or one query per open command.
const reconcileTimeout = 10 * time.Minute

// recoveryChecks registers a full recovery's checks (chapter 14) for a member with a database: the SDK's own and the
// module's; their operation, Definition.Recovery, also guards the external reconciliation.
func (m *Member) recoveryChecks(services *grpcx.Services, catalog *tiering.Catalog) error {
	if m.db == nil {
		if _, ok := m.Module.(recovery.Checker); ok {
			return errors.New("a module that registers recovery checks has a database (Migrations)")
		}
		return nil
	}
	if m.Def.Recovery == "" {
		return errors.New("a component with a database declares the operation of a full recovery's checks (Definition.Recovery, P14.1)")
	}
	reg := &recovery.Checks{}
	if catalog != nil { // the SDK's tiering check, before the component's own (P15.36)
		if err := reg.Add(catalog.RecoveryCheck()); err != nil {
			return err
		}
	}
	if c, ok := m.Module.(recovery.Checker); ok {
		if err := c.RecoveryChecks(reg); err != nil {
			return fmt.Errorf("the module's recovery checks could not be registered: %w", err)
		}
	}
	method := grpcx.Method{Operation: m.Def.Recovery, Callers: []string{RecoveryTool}, Timeout: reconcileTimeout}
	services.Register(&recoverypb.CheckService_ServiceDesc, recovery.NewServer(m.db, m.Def.Subscribes, reg),
		map[string]grpcx.Method{"Check": method, "Present": method, "Numbers": method, "AdvanceNumbers": method})
	return nil
}

// hostPort accepts an address as BrickKit derives it (http://service:port) or a bare host:port.
func hostPort(addr string) string {
	if u, err := url.Parse(addr); err == nil && u.Host != "" {
		return u.Host
	}
	return addr
}

func (m *Member) background(sh Shared, rt *runtime.Runtime) error {
	p, provides := m.Module.(extcmd.Provider)
	switch {
	case provides && rt.External == nil:
		return errors.New("the module sends external commands but the definition does not say ExternalCommands")
	case !provides && rt.External != nil:
		return errors.New("the definition says ExternalCommands but the module does not implement extcmd.Provider")
	case provides: // the adapter and its operations; the SDK's job sends and queries (be-protocol chapter 13)
		rt.External.Adapter, rt.External.Ops = p.ExternalCommands()
		if err := m.addSDKJob(rt.External.Job()); err != nil {
			return err
		}
		m.hasJobs = true
	}
	if p, ok := m.Module.(tiering.Participant); ok { // the module's blockers (P15.12)
		if rt.Tiering == nil {
			return errors.New("the module takes part in the tiering but the definition declares no classes (Definition.Tiering)")
		}
		if err := p.Tiering(rt.Tiering.Registry); err != nil {
			return fmt.Errorf("the module's tiering could not be registered: %w", err)
		}
	}
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
			m.consumers = append(m.consumers, &events.Consumer{Member: m.Def.ID, Sub: s, DB: sh.DB, Log: m.Logger,
				Claims: events.NewHTTPClaims(), Configured: int(m.Values.Int(events.ConcurrencyKey))})
		}
		m.redeliver = &events.Redelivery{DB: sh.DB, Subs: subs.List(), Log: m.Logger, Conn: func(c string) (grpc.ClientConnInterface, bool) {
			conn, ok := rt.GRPC(c)
			return conn, ok
		}}
	}
	return nil
}

func (m *Member) systemSurface(member obs.Member, rt *runtime.Runtime) error {
	var services grpcx.Services
	if sm, ok := m.Module.(component.SystemModule); ok {
		if err := sm.Services(&services); err != nil {
			return fmt.Errorf("the module's services could not be registered: %w", err)
		}
	}
	if m.watermark != nil { // the pull of the member's watermark streams (P9.13)
		method, ok, err := m.watermark.Method()
		if err != nil {
			return err
		}
		if ok {
			services.Register(&sourcepb.WatermarkService_ServiceDesc, m.watermark, map[string]grpcx.Method{"Pull": method})
		}
	}
	rp, replayer := m.Module.(evidence.Replayer)
	destroys := rt.Tiering != nil && rt.Tiering.Replaying()
	if replayer || destroys { // the owner's replay of its evidence (P12.19), the tiering's destructions with it
		var replays evidence.Replays
		if replayer {
			if err := rp.Replays(&replays); err != nil {
				return fmt.Errorf("the module's replays could not be registered: %w", err)
			}
		}
		if destroys {
			rt.Tiering.Replays(&replays)
		}
		if err := replays.Err(); err != nil {
			return err
		}
		if m.Def.Replay == "" {
			return errors.New("a module that replays its evidence declares the operation of the replay (Definition.Replay, P12.19)")
		}
		if m.db == nil {
			return errors.New("a module that replays its evidence has a database (Migrations)")
		}
		services.Register(&evidencepb.ReplayService_ServiceDesc, &evidence.ReplayServer{DB: m.db, Component: m.Def.ID,
			Replays: &replays, Log: m.Logger}, map[string]grpcx.Method{"Replay": {Operation: m.Def.Replay,
			Callers: []string{RecoveryTool}}})
	}
	if err := m.recoveryChecks(&services, rt.Tiering); err != nil {
		return err
	}
	if m.external != nil { // the external reconciliation of a full recovery's step 7 (P13.11)
		services.Register(&externalpb.ReconcileService_ServiceDesc, &extcmd.ReconcileServer{Commands: m.external},
			map[string]grpcx.Method{"Reconcile": {Operation: m.Def.Recovery, Callers: []string{RecoveryTool}, Timeout: reconcileTimeout}})
	}
	if m.resender != nil { // the redelivery of the member's streams (P6.38)
		if m.Def.Resend == "" {
			return errors.New("a component with streams declares the operation of its redelivery (Definition.Resend, P6.38)")
		}
		services.Register(&sourcepb.ResendService_ServiceDesc, m.resender, map[string]grpcx.Method{"Resend": {
			Operation: m.Def.Resend, Callers: []string{grpcx.AnyComponent}, Timeout: events.ResendCallTimeout}})
	}
	if m.records != nil { // the batch read and snapshot of the member's streams (P9.30)
		method, ok, err := m.records.Method()
		if err != nil {
			return err
		}
		if ok {
			services.Register(&sourcepb.RecordService_ServiceDesc, m.records,
				map[string]grpcx.Method{"Read": method, "Snapshot": method})
		}
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
