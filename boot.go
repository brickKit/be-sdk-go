package besdk

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/lifecycle"
	"github.com/brickKit/be-sdk-go/internal/logx"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/telemetry"
)

// processEnv is what the entry point takes from the process; tests pass their own.
type processEnv struct {
	lookup     config.Source // os.LookupEnv standalone
	stdout     io.Writer
	instanceID string // hostname: the container or pod
}

// boot is one component's configuration, logging and telemetry, built before anything listens.
type boot struct {
	spec         Spec
	id           string
	version      string
	man          manifest
	vals         *config.Values
	log          *slog.Logger
	catalogue    *problem.Catalogue
	locale       string
	platform     *telemetry.Platform
	member       *telemetry.Member
	secretM      *telemetry.SecretMetrics
	lifecycle    *lifecycle.Declaration // migrations/lifecycle.yaml (P16.1); nil without a database
	lifecycleCfg lifecycle.Config       // DATA_LIFECYCLE (P16.9)
	authzCatalog *authz.Catalog         // Spec.Catalog's resource types (P6.10)
	instance     string                 // service.instance.id: the container or pod; job and lease holders (P14)
}

// configFailure is a start failure that exits 78 (P1.2): every problem gets one log line naming its key.
type configFailure struct{ errs []*config.Error }

func (f *configFailure) Error() string { return fmt.Sprintf("%d configuration error(s)", len(f.errs)) }

// bootstrap reads and checks the configuration (P1.2 step 1, P2) and builds the logger and the
// telemetry of the component.
func bootstrap(ctx context.Context, s Spec, pe processEnv) (*boot, error) {
	man, err := parseManifest(s.Manifest)
	if err != nil {
		return nil, &configFailure{errs: []*config.Error{{Reason: config.ReasonInvalid, Key: "component.yaml", Detail: err.Error()}}}
	}
	cat, err := config.LoadCatalogue()
	if err != nil {
		return nil, fmt.Errorf("be-protocol catalogue: %w", err)
	}
	schema, err := config.ParseSchema(s.Manifest, cat)
	if err != nil {
		return nil, &configFailure{errs: asConfigErrors(err)}
	}
	vals, errs := config.Load(schema, pe.lookup)
	errs = append(errs, identityErrors(s, schema, pe.lookup)...)
	if len(errs) > 0 {
		return nil, &configFailure{errs: errs}
	}
	b := &boot{spec: s, id: s.ID, version: schema.Version, man: man, vals: vals, instance: pe.instanceID}
	if b.instance == "" {
		b.instance = "local"
	}
	if v, ok := pe.lookup("COMPONENT_VERSION"); ok && v != "" {
		b.version = v
	}
	level, _ := logx.ParseLevel(optString(vals, "LOG_LEVEL", "info"))
	b.log = logx.New(logx.Options{ComponentID: b.id, ComponentVersion: b.version, Level: level, Writer: pe.stdout})
	locale, known := catalogueLocale(optString(vals, "DEFAULT_LOCALE", "zh-CN"))
	if !known {
		b.log.Warn("DEFAULT_LOCALE has no catalogue language; falling back to en", slog.String("key", "DEFAULT_LOCALE"))
	}
	b.locale = locale
	if b.catalogue, err = loadCatalogue(s.Contracts); err != nil {
		return nil, &configFailure{errs: []*config.Error{{Reason: config.ReasonInvalid, Key: "contracts/errors.yaml", Detail: err.Error()}}}
	}
	if s.Catalog != "" {
		if b.authzCatalog, err = authz.ParseCatalog([]byte(s.Catalog)); err != nil {
			return nil, &configFailure{errs: []*config.Error{{Reason: config.ReasonInvalid, Key: "Spec.Catalog", Detail: err.Error()}}}
		}
	}
	if err := loadLifecycle(b); err != nil {
		return nil, &configFailure{errs: asConfigErrors(err)}
	}
	if err := b.initTelemetry(ctx, pe); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *boot) initTelemetry(ctx context.Context, pe processEnv) error {
	var err error
	b.platform, err = telemetry.NewPlatform(ctx, telemetry.PlatformOptions{
		OTELBaseURL: optString(b.vals, "OTEL_BASE_URL", ""), ServiceName: b.id, ServiceVersion: b.version})
	if err != nil {
		return &configFailure{errs: []*config.Error{{Reason: config.ReasonInvalid, Key: "OTEL_BASE_URL", Detail: err.Error()}}}
	}
	b.member, err = telemetry.NewMember(b.platform, memberResource(b.id, b.version, pe.instanceID,
		optString(b.vals, "DEPLOY_ENV", "")))
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	b.secretM, err = telemetry.NewSecretMetrics(b.member.Registerer())
	return err
}

// catalogueLocale keeps a locale whose primary subtag is a catalogue language (zh, en) and otherwise
// falls back to en (P4.1, DEFAULT_LOCALE); false reports the fallback.
func catalogueLocale(v string) (string, bool) {
	primary, _, _ := strings.Cut(strings.ToLower(strings.ReplaceAll(v, "_", "-")), "-")
	if primary == "zh" || primary == "en" {
		return v, true
	}
	return "en", false
}

// memberResource is the member's OpenTelemetry resource (P18.1): service.namespace is the domain (the
// ID's first segment), deployment.environment.name is DEPLOY_ENV, "dev" when unset.
func memberResource(id, version, instance, env string) telemetry.Resource {
	domain, _, _ := strings.Cut(id, "/")
	if env == "" {
		env = "dev"
	}
	return telemetry.Resource{ComponentID: id, ComponentVersion: version, Namespace: domain,
		InstanceID: instance, Environment: env}
}

// identityErrors checks the component ID three ways: Spec, manifest and the injected COMPONENT_ID.
func identityErrors(s Spec, schema config.Schema, lookup config.Source) []*config.Error {
	var errs []*config.Error
	if schema.ID != s.ID {
		errs = append(errs, &config.Error{Reason: config.ReasonInvalid, Key: "metadata.id",
			Detail: fmt.Sprintf("component.yaml says %q, Spec.ID says %q", schema.ID, s.ID)})
	}
	if v, ok := lookup("COMPONENT_ID"); ok && v != s.ID {
		errs = append(errs, &config.Error{Reason: config.ReasonInvalid, Key: "COMPONENT_ID",
			Detail: fmt.Sprintf("injected %q, the image is %q", v, s.ID)})
	}
	return errs
}

func asConfigErrors(err error) []*config.Error {
	if es, ok := err.(config.Errors); ok {
		return es
	}
	if e, ok := config.AsError(err); ok {
		return []*config.Error{e}
	}
	return []*config.Error{{Reason: config.ReasonInvalid, Key: "component.yaml", Detail: err.Error()}}
}

// optString reads a key the component may or may not declare.
func optString(vals *config.Values, key, def string) string {
	if _, declared := vals.Decl(key); !declared {
		return def
	}
	if s, ok := vals.String(key); ok && s != "" {
		return s
	}
	return def
}

func optDuration(vals *config.Values, key string, def time.Duration) time.Duration {
	if _, declared := vals.Decl(key); !declared {
		return def
	}
	if d, ok := vals.Duration(key); ok {
		return d
	}
	return def
}

func declared(vals *config.Values, key string) bool {
	_, ok := vals.Decl(key)
	return ok
}

func loadCatalogue(contracts fs.FS) (*problem.Catalogue, error) {
	c := problem.NewCatalogue()
	if contracts == nil {
		return c, nil
	}
	b, err := fs.ReadFile(contracts, "errors.yaml")
	if err != nil {
		return c, nil // a component without its own reasons
	}
	return c, c.Add(b)
}

// logConfigErrors writes one JSON line per configuration problem, naming the key (P1.2).
func logConfigErrors(log *slog.Logger, f *configFailure) {
	for _, e := range f.errs {
		log.Error("configuration error", slog.String("key", e.Key), slog.String("error.reason", e.Reason),
			slog.String("error", e.Detail))
	}
}
