[English](README.md) · [中文](README.zh.md)

# be-sdk-go

The official Go implementation of the BrickEnterprise component protocol **be-protocol 1.0**
(`github.com/brickKit/be-protocol`, pinned at `v1.0.0-rc.2`). A Go component declares a `Spec` and calls
`besdk.Main`; the SDK owns the process, the ports, the database identity, the bus, every timeout and every
protocol surface, and hands the component a `Runtime`. It is not a brickKit component and holds no
business logic.

**Version 0.6.0 (in progress).** Implements protocol 1.0 for the runtime, configuration, HTTP surface,
errors, token verification and the feature-key decision, observability, outbound calls, the database
and migrations, events (tasks G1–G3), command idempotency, background jobs, data scopes with the
resource contract and the ACL projection, and the lifecycle engine for hot and warm tiers (G4–G7).
Calendars and money, snapshots, the PostgreSQL bus adapter, the `besdktest` package and the shell
launcher follow in later waves; their protocol surfaces are not offered yet (`/_be/info` lists what is).

## A component

```go
var Spec = besdk.Spec{ID: "erp/sales", Manifest: manifest /* go:embed component.yaml */,
    Migrations: migrations.FS, Contracts: contracts.FS, Catalog: authzgen.CatalogJSON, New: New}

func main() { besdk.Main(Spec) }

func New(ctx context.Context, rt *besdk.Runtime) (*besdk.Module, error) {
    store, err := rt.Store()
    if err != nil { return nil, err }
    return &besdk.Module{
        HTTP: func(r *besdk.Router) {
            besdk.GET(r, "/orders/:id", authzgen.SalesView.On(authzgen.SalesOrder, "id"), getOrder(store))
        },
        GRPC: func(s *grpc.Server) { salesv1.RegisterSalesServiceServer(s, svc) },
        Events: besdk.Events{Publishes: []string{"sales.order.created.v1"}},
    }, nil
}
```

`component.yaml`: `migration.command: [./component, migrate, up]`, `healthCheck` `/healthz`,
`readinessCheck` `/readyz`, `deployment.stopGracePeriodSeconds: 30`, ports with `protocol: http` / `grpc`.

## What the SDK does (by protocol chapter)

| Chapter | Here |
|---|---|
| P1 process | one binary: serve, `migrate up / down <n> / status`, `job run <name>` (capability `job_run`; unknown job 64); exit codes 0 / 1 / 64 / 78; start order config → ports → background connections; supervised background work (1 s → 5 min); `/healthz`, latching `/readyz`; SIGTERM → `SHUTDOWN_GRACE` (25 s) |
| P2 configuration | only `configSchema` keys of the embedded `component.yaml`, strictly typed, every error at once (exit 78); secrets only from `…_FILE` files, re-read within 30 s, last good value kept; family addresses `AUTHZ_URL`, `IAM_URL`, `*_GRPC_URL` |
| P3 HTTP | Gin inside an outer layer: request ID, trace extraction, route deadline answered as 504 even when a handler ignores it, body limit (413), server timeouts 5 s / 30 s / deadline + 5 s / 120 s, dual-stack listen, access log |
| P4 errors | problem+json with AIP-193 members, the 36 `be` reasons of rc.2, gRPC `ErrorInfo` / `BadRequest` / `RetryInfo`, generic text for INTERNAL; `besdk.Errorf`. A body `Bind` cannot decode and a gRPC request that does not decode answer `REQUEST_INVALID` with violations; an unreachable database (refused, timed-out or lost connection, SQLSTATE class `08`, `57P01`–`57P03`), `rt.Conn` or `rt.UserHTTP` dependency answers `DEPENDENCY_UNAVAILABLE` with `metadata.dependency` = `db` or the component ID |
| P6.3–P6.15 | levels × dimensions, subject sets, relations, ceilings and fields per contract-infra-authz EVALUATION E1–E12 (all 62 decision vectors, every member); `Access.Scope` / `ListScope` with the canonical predicate (`Where`, three disjoint branches), `Access.Can` (an invisible record is 404), `RowActions`, `Mask`, `CheckWritable` / `CheckSortable`; `PermKey.On` decides the record before the handler through `Module.Sharing`; the resource contract `_authz/check`, `_authz/explain`, `_shares` (501 without the capability, writes through `WriteTuples` at `AUTHZ_GRPC_URL`); the ACL projection (`be.authz.changes`, 410 rebuild, poke on `infra.authz.changed.v1`). Go note: `Can` takes the command's `*Tx` (the ACL rows are in the database) |
| P13 | `besdk.Idempotent[T]`, `Tx.IdemLookup / IdemClaim / IdemComplete / IdemRelease`: RFC 8785 fingerprints, keys in the caller's namespace (`user:` / `svc:` / `system`), atomic claim, replay with the stored status, 30-day expiry |
| P14 | `Module.Jobs` (every, singleton with lease and epoch, cron with `@every` or five fields in `BUSINESS_TIMEZONE`), `Module.Workers` + `tx.Enqueue`, `besdk.NewReconciler`; `JOBS_OVERRIDES`; runtime jobs `be.cleanup` (done queue rows 7 days, slots 30 days, idempotency keys, cursors) and `be.lifecycle`; `job run <name>` through the same tables |
| P16 (P0) | `migrations/lifecycle.yaml` checked at start; the platform migration creates every declared partition window (`<table>_<YYYY>w<WW>` / `m<MM>` / `<YYYY>`); `be.lifecycle` keeps windows ahead, seals units with the canonical digest chain, drops expired outbox and queue partitions; `tx.Seal`, `tx.OpenListPartition`; `-- be:contract after=` stops the migration while an older version's session (`<id>@<version>`, kept open by every process) is connected. Cold tiers are not offered |
| P5, P6.1–P6.2 | JWT (RS256 / ES256 / EdDSA, `iss`, `aud`, `typ=access`), JWKS from `{IAM_URL}/.well-known/jwks.json`; bundle v2 polling, the route chain with feature keys (token first: 401 `TOKEN_INVALID`; then, until the first bundle, 503 `AUTHZ_NOT_READY` on every non-Public route, Authenticated included); `besdk.AccessFrom` |
| P7, P8 | gRPC server limits and interceptors, batch limits from `(be.v1.max_items)`; `rt.Conn` (one connection per dependency, retries from `idempotency_level`, maxAttempts 3, budget per ClientConn, bulkhead 64); `rt.UserHTTP`, `rt.ExternalHTTP`; no network inside a transaction |
| P10, P11 | `rt.Store()`: per-transaction `SET LOCAL ROLE / search_path / application_name / timeouts`, `/* be:<schema> */` statement prefix, retries of 40001 / 40P01, SQLSTATE mapping, pool budget; migrations as the owner role with golang-migrate (`schema_migrations_<schema>`), then the platform migration (`besdk_migrations_<schema>`); the authorization projection (`besdk_authz_acl`, `besdk_authz_cursor`) only when `Spec.Catalog` declares resource types |
| P12 | outbox in the business transaction (a payload over 64 KiB is refused at publish), pump with PubAck, JetStream durables created only when absent, runtime-side redelivery delays and dead letters, aggregate-stream cursor |
| P18 | JSON logs with redaction and lines of at most 2048 bytes including the newline; the access-log line at ERROR for 500, WARN for 503 / 504, INFO otherwise, ops endpoints at DEBUG; `be_` metrics with the `component` label (`be_tx_retries_total{sqlstate}`); per-member tracer and meter providers, explicit propagator; an unsampled inbound `traceparent` is propagated with its flag and not recorded |

Go-specific notes the protocol asks every runtime to state: the gRPC server enforces keepalive `MinTime`
20 s (P7.5); the retry budget is per ClientConn, one per member, dependency and port, refilled on resolver
updates (P7.8); `PG_POOL_MIN_IDLE` is not a protocol key: a Go component may declare it in its own
`configSchema` (default 2), but `database/sql` keeps no idle floor, so it is only validated.

## Packages

`besdk` (root: the whole public API) · `proto/be/v1` (generated code of `be/v1/limits.proto`; map
`Mbe/v1/limits.proto=github.com/brickKit/be-sdk-go/proto/be/v1`) · `internal/…`: `config`, `problem`,
`logx`, `telemetry`, `httpx`, `authn`, `authz`, `rpc`, `pg`, `envelope`, `bus/jetstream`, `events`, `idem`, `jobs`, `jobs/schedule`, `authz/acl`, `lifecycle`.

## Tests

| Command | What |
|---|---|
| `make test` | unit tests and every be-protocol vector the implemented chapters use (config, errors, envelope, redaction), plus the contract-infra-authz decision vectors; integration tests skip |
| `make test-integration` | the same plus integration tests on throwaway PostgreSQL 16, PostgreSQL 14 and NATS 2.12 containers (prefix `sdkb-go-ci`), removed afterwards |
| `make lint`, `make import-scan` | vet, gofmt; no dependency on any component repository |
