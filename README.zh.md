[English](README.md) · [中文](README.zh.md)

# be-sdk-go

BrickEnterprise 组件协议 **be-protocol 1.0**（`github.com/brickKit/be-protocol`，钉在 `v1.0.0-rc.2`）的
官方 Go 实现。Go 组件声明一个 `Spec` 并调用 `besdk.Main`；进程、端口、库身份、总线、所有超时和协议的
每个面都归 SDK，组件拿到的是一个 `Runtime`。它不是 brickKit 组件，不含业务逻辑。

**版本 0.6.0（进行中）。** 已实现协议 1.0 的运行时、配置、HTTP 面、错误、令牌验证与功能键判定、
可观测、出站调用、数据库与迁移、事件（任务 G1–G3）。命令幂等、后台工作、数据范围与资源契约、
生命周期引擎、日历与金额、`besdktest` 包和外壳启动器在后续波次；它们的协议面暂未提供（`/_be/info` 列出已提供的）。

## 一个组件

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

`component.yaml`：`migration.command: [./component, migrate, up]`、`healthCheck` `/healthz`、
`readinessCheck` `/readyz`、`deployment.stopGracePeriodSeconds: 30`、端口带 `protocol: http` / `grpc`。

## SDK 做了什么（按协议章节）

| 章节 | 这里 |
|---|---|
| P1 进程 | 一个二进制：服务、`migrate up / down <n> / status`、`job run <name>`（暂未提供：退出 64）；退出码 0 / 1 / 64 / 78；启动顺序 配置 → 端口 → 后台连接；后台工作受监督（1 s → 5 min）；`/healthz`、锁定式 `/readyz`；SIGTERM → `SHUTDOWN_GRACE`（25 s） |
| P2 配置 | 只读嵌入的 `component.yaml` 里 `configSchema` 声明的键，严格类型，错误一次报全（退出 78）；密钥只来自 `…_FILE` 文件，30 s 内重读，失败保留上一个有效值；族地址 `AUTHZ_URL`、`IAM_URL`、`*_GRPC_URL` |
| P3 HTTP | Gin 外面一层：请求 ID、trace 提取、路由截止时间（处理器不理会也答 504）、请求体上限（413）、服务端超时 5 s / 30 s / 截止 + 5 s / 120 s、双栈监听、访问日志 |
| P4 错误 | problem+json 带 AIP-193 成员、`be` reason（rc.1 的 33 个，加上 rc.1 之后裁决新增的 `REQUEST_INVALID` 和 `DEPENDENCY_UNAVAILABLE`）、gRPC `ErrorInfo` / `BadRequest` / `RetryInfo`、INTERNAL 只给通用文案；`besdk.Errorf`。`Bind` 解不开的请求体、解码失败的 gRPC 请求答 `REQUEST_INVALID` 并带 violations；连不上数据库（连接被拒、连接超时或断开、SQLSTATE `08` 类、`57P01`–`57P03`）或 `rt.Conn` / `rt.UserHTTP` 的依赖时答 `DEPENDENCY_UNAVAILABLE`，`metadata.dependency` 为 `db` 或依赖的组件 ID |
| P5、P6.1–P6.2 | JWT（RS256 / ES256 / EdDSA、`iss`、`aud`、`typ=access`），JWKS 取 `{IAM_URL}/.well-known/jwks.json`；bundle v2 轮询、按功能键的路由判定链（先验 token：401 `TOKEN_INVALID`；拿到第一份 bundle 之前，所有非 Public 路由——Authenticated 也算——答 503 `AUTHZ_NOT_READY`）；`besdk.AccessFrom` |
| P7、P8 | gRPC 服务端参数与拦截器、按 `(be.v1.max_items)` 的批量上限；`rt.Conn`（每个依赖一条连接、按 `idempotency_level` 重试、maxAttempts 3、预算按 ClientConn、舱壁 64）；`rt.UserHTTP`、`rt.ExternalHTTP`；事务里不许网络调用 |
| P10、P11 | `rt.Store()`：每个事务 `SET LOCAL ROLE / search_path / application_name / 超时`、`/* be:<schema> */` 语句前缀、40001 / 40P01 重试、SQLSTATE 映射、池预算；迁移以属主角色跑 golang-migrate（`schema_migrations_<schema>`），再跑平台迁移（`besdk_migrations_<schema>`）；授权投影表（`besdk_authz_acl`、`besdk_authz_cursor`）只在 `Spec.Catalog` 声明了资源类型时才建 |
| P12 | outbox 与业务同事务（payload 超过 64 KiB 在发布时拒绝）、拿到 PubAck 才算发布的泵、JetStream durable 只建不改、由运行时执行的重投延迟与死信、按聚合流的游标 |
| P18 | 带脱敏、每行含换行符最多 2048 字节的 JSON 日志；访问日志 500 记 ERROR、503 / 504 记 WARN、其余 INFO，运维端点记 DEBUG；带 `component` 标签的 `be_` 指标（`be_tx_retries_total{sqlstate}`）；按成员的 tracer / meter provider，显式传播器；入站 `traceparent` 未采样时照样带着标志往下传，但不记录 |

协议要求每个运行时写明的 Go 特有说明：gRPC 服务端强制 keepalive `MinTime` 20 s（P7.5）；重试预算按
ClientConn 计，每个（成员，依赖，端口）一个，resolver 更新时回满（P7.8）；`PG_POOL_MIN_IDLE` 不是协议键：Go 组件可以在自己的
`configSchema` 里声明它（默认 2），但 `database/sql` 没有空闲下限，所以只做校验。

## 包

`besdk`（根包：全部公开 API）· `proto/be/v1`（`be/v1/limits.proto` 的生成代码；映射
`Mbe/v1/limits.proto=github.com/brickKit/be-sdk-go/proto/be/v1`）· `internal/…`：`config`、`problem`、
`logx`、`telemetry`、`httpx`、`authn`、`authz`、`rpc`、`pg`、`envelope`、`bus/jetstream`、`events`。

## 测试

| 命令 | 内容 |
|---|---|
| `make test` | 单测，以及已实现章节用到的全部 be-protocol 向量（config、errors、envelope、redaction）和 contract-infra-authz 的判定向量；集成测试跳过 |
| `make test-integration` | 同上，再加在一次性 PostgreSQL 16、PostgreSQL 14、NATS 2.12 容器（前缀 `sdkb-go-ci`）上的集成测试，跑完删除 |
| `make lint`、`make import-scan` | vet、gofmt；不依赖任何组件仓库 |
