# be-sdk-go

Go 横切基础库（总纲 §4 SOP-L 十四项能力）。**不是 brickKit 组件**，也**不是公共 model 包**——零业务逻辑、零组件 model、零组件间引用。它是 `be-acceptance` 铁律六 import 扫描的唯一白名单。

## 它替所有 Go 组件挡住的坑

| 能力 | 文件 | 挡住的坑 |
|---|---|---|
| 组件地址剥 scheme | `endpoint.go` | `grpc.Dial("http://host:9094")` 连不上，报错指向名称解析（十八条第 1 条） |
| `SET LOCAL` 事务 | `tx.go` | 不带 `LOCAL` 的 `SET` 之后连接还回池，下一个借用者原样继承，悄悄读写别人的数据（十八条第 2 条） |
| 单跑/合并统一入口 | `standalone.go`、`module.go`、`runtime.go`、`gin.go` | 每个组件各发明一个 `main`，合并那天全部重写（十八条第 18 条） |

## 现状（阶段三 Task 5，权限判定真正上线）

`RequirePermission`/`ScopeOf` 从阶段二的 fail-closed stub 换成真实判定——这是三个 `be-sdk-*` 共用的机制，`infra-authz` 建成之后才有真实数据可以对着测。

- **JWT 本地验签**：`iamJwksUrl` 指向的 JWKS 端点，用 [`MicahParks/keyfunc`](https://github.com/MicahParks/keyfunc)（自带 JWK Set 后台刷新，不用自己写缓存）配 [`golang-jwt/jwt/v5`](https://github.com/golang-jwt/jwt)，只认 `RS256`。`infra-iam-casdoor` 要到阶段三 Task 7 才建仓库，暂时没有真实签发方——测试自己起一对 RSA 密钥 + 一个 `httptest.Server` 当 JWKS 端点，加密运算是真的，只是身份是测试夹具。
- **bundle 轮询**：15 秒条件 GET `authzBundleUrl`（`If-None-Match`，未变化 304 不重新解析），进程级单例，模块代码看不见（§14.1.4）。有一条测试真等 15 秒验证"改角色分配不重启组件也能生效"，不是 mock 时钟。
- **`Authenticated` 新哨兵值**：阶段三 Task 4 写 `infra-authz` 时发现的真实缺口——`Public`/具体权限键两档之间缺"已登录即可，不需要权限键"这一档（`GET /api/me/permissions` 这类端点）。仍然验签、仍然查 `stale_since`，只是跳过权限键查找。
- **降级语义按 §14.1.9 精确区分三种状态**：`iamJwksUrl` 没配 → 阶段二遗留行为，非 Public 一律 403；配了但 bundle 从没连上过 → 503（不是 403，语义更准）；连上过但角色没这条权限 → 403。
- **`ScopeOf` 是纯函数**（§14.2.4）：`Prefix`/`Exact`/`Owner` 三个字段永远从同一份 JWT 的 `dept_path`/`sub` 填，"这次查询该用哪一档"是调用方某条 `.sql` 的静态选择，不是 `ScopeOf` 自己判断。⚠️ **一处容易反方向的细节**：`ctx` 里取不到 Claims 时不能返回零值 `ScopeFilter{}`——§14.2.4 的 SQL 约定"空字符串表示不限"，零值会被下游解读成放行一切，是 fail-open 不是 fail-closed。这里改成 `panic`，让编程错误（在 `Start()`/事件 handler 里误用）在联调阶段就现形。
- 真机验证：起了本地 `infra-authz` 容器，`be-sdk-go` 的轮询客户端直接打它真实的 `GET /authz/bundle`，确认认得出自举种子数据 `authz_admin`/`infra.authz.admin`——两边是分开写的，这条测试专门抓"字段名各写各的"这类耦合裂缝。

## 从 `v0.1.1` 到 `v0.1.6` 修的六个 bug，五个是同一类问题

下面六条各自的根因都不一样，但读完会发现一条更值钱的共性：**六条里有五条（除了 `v0.1.3` 那条纯 SQL 语义问题）的根本原因都是"测试构造被测对象的路径，和生产环境构造它的路径不是同一条"**——`v0.1.4` 的注释里已经点破过一次，这里提到顶部是因为它不只是那一条 bug 的教训，是这整个基础库的测试**该怎么写**的判据：**能走真实的 `RunStandalone`/子进程/真实端口，就不要在测试里手工拼一个"看起来等价"的对象去代替它**。下一个语言的 `be-sdk-python`/`be-sdk-ts` 写测试时，先确认这一条。

## 现状（阶段二 Task 10，`v0.1.8`）

`erp-inventory` 是阶段二第一个真的调用 `Consume` 的组件（阶段一只有 SDK 自己的测试验过它）——一用就压出一个 bug：`handleOne` 只 `db.BeginTx` 过，没有像 `WithTx` 那样 `SET LOCAL ROLE` + `SET LOCAL search_path`。业务代码在 `fn` 里按 `WithTx` 的约定写"不带 schema 前缀"的 SQL（如 `INSERT INTO product_tracking_snapshots ...`），在 `Consume` 给的 `tx` 上会直接报 `relation does not exist`——两个入口给业务代码的假设不一致。`Consume` 签名加了 `role` 参数，`handleOne` 内部现在做和 `WithTx` 完全一样的两条 `SET LOCAL`。因为在这之前没有任何组件真的调用过 `Consume`（阶段一到阶段二 Task 10 之前，唯一的调用方是 SDK 自己的测试），改签名不影响任何真实调用方，走的是同一套"形状还没定型就趁早改对"的判据（同 Task 1 的三处签名）。新增回归测试 `TestConsume_fn拿到的tx已经切好schema`：断言 `fn` 里不带 schema 前缀也能查到表，手动回退过一次验证它在没有这个修复时真的会红（报 `42P01 relation does not exist`）。

## 现状（阶段一 Task 16，`v0.1.6`）

验证验收标准 5（"停 PostgreSQL，`/healthz` 仍应 200"）时，真的 `docker stop` 了 postgres——结果不是健康检查失败，而是**整个容器进入几百毫秒一次的重启死循环**。根因：`StartOutboxPump` 的 `pumpOnce` 查询失败被当成硬错误直接 `return`，这个 error 顺着 `Module.Start` 的 `errCh` 一路传到 `RunStandalone` 顶层，被当成"服务异常退出"，进程退出，Docker 重启策略又把它拉起来，立刻重连又立刻失败，如此循环——这段时间里 HTTP/gRPC 完全没人能连，是一条完全独立于"`/healthz` 不查依赖"设计之外的故障传播路径。同一个 `Module.Start` 里的 `partition.Start` 从一开始就没有这个问题（失败只记日志、留到下一轮重试），`StartOutboxPump` 没有对齐这套容错方式。已给 `StartOutboxPump` 加 `logger *slog.Logger` 参数，`pumpOnce` 失败（非 ctx 取消）只记日志、循环继续。**这条对任何用 Outbox 的组件都成立**，不是 mdm-customer 专属。

## 现状（阶段一 Task 16，`v0.1.5`）

修完 `v0.1.4` 的 panic 之后，容器还是 `unhealthy`——这次日志干净，只有一条条 404。平台生成的健康检查是 `wget -q --spider .../healthz`，`--spider` 发的是 **HEAD** 请求，不是 GET；`/healthz` 只注册了 `engine.GET`，Gin 的路由不会像标准库 `http.ServeMux` 那样让 GET 处理器顺带接住 HEAD。已同时注册 `engine.HEAD("/healthz", ...)`。

## 现状（阶段一 Task 16，`v0.1.4`）

`RunStandalone` 构造 `Runtime` 时一直没有把 `Tracer`/`Meter` 两个字段填进去（一直是 nil interface）——`mdm-customer` 第一次真的 `brickkit up`（不是 `--dry-run`）把容器跑起来后才暴露：`NewGinEngine` 的 `tracingMiddleware` 对每个请求都调 `rt.Tracer.Start(...)`，`/healthz` 也不例外，容器因此对任何请求都必然 panic，健康检查永远过不了。已改成 `Bootstrap` 返回后用 `otel.Tracer(componentID)`/`otel.Meter(componentID)` 取真实值赋给这两个字段。

这个 bug 存在的原因很有代表性：`gin_test.go` 的测试 helper 一直手工塞了一个真 tracer，从没测过"`RunStandalone` 自己组出来的 `Runtime`"这条生产路径——**测试构造对象的方式和生产构造对象的方式不是同一条路径时，测试再多也测不出这类问题**。已经补了一条走真实子进程 + 真实 HTTP 请求的回归测试。

## 现状（阶段一 Task 16，`v0.1.3`）

`BatchGetRouted` 原本假设每个组件都有 `{schema}_archive.{table}` 这张表——但真实情况是不少组件（比如 `mdm-customer`，主数据不分区不归档，设计计划 §7）压根不会有归档表，那个 schema 建了但里面永远没有表。

第一版修复（`v0.1.2`）思路是错的：先查、报 `relation does not exist` 就在 Go 这层当空结果处理。**PostgreSQL 里一条语句真的执行失败之后，整个事务会被标记成 aborted——即使调用方选择不把这个错误向上传播，事务在数据库那一侧已经回不去了**，随后的 `COMMIT` 会拿到 `pgx.ErrTxCommitRollback`（"commit unexpectedly resulted in rollback"）。`v0.1.3` 改成用 `to_regclass` 在真正查询之前先问一句"这张表存在吗"——查不到只返回 `NULL`，不报错、不污染事务，存在才真的去查。

## 现状（阶段一 Task 16，`v0.1.1`）

`v0.1.0` 的 `RunStandalone` 里有三处是"等第一个真实组件出现才能核对"的占位：`HTTP_PORT`/`PG_DSN`/`NATS_URL` 三个环境变量从来没有被平台真正注入过。`mdm-customer` 第一次真的 `brickkit up --dry-run` 之后核对出实际契约并修复：

| 占位时的假设 | 实际契约 | 改成什么 |
|---|---|---|
| 读整段 `HTTP_PORT` 环境变量 | 平台不注入"我该监听哪个端口"（§13.8.1），端口只在组件自己的 `component.yaml` 里 | 新增 `manifest.go` 的 `loadOwnPorts`，读 `component.yaml` 的 `deployment.port`/`extraPorts` |
| 读整段 `PG_DSN` 环境变量 | 平台注入的是分开的 `DATABASE_HOST/PORT/USER/PASSWORD/NAME` | `buildPGDSN()` 从五片拼 |
| 读整段 `NATS_URL` 环境变量 | 平台注入的是分开的 `MQ_HOST/PORT`（+ 可选 `MQ_USER/MQ_PASSWORD`） | `buildNATSURL()` 从这几片拼，兼容无认证的情形 |

顺带用 `-race -count=20` 复测抓到一个真实（非误报）的数据竞争：`serveExtraPort` 内部先 `net.Listen` 再 `register(srv)`，测试用裸 `bool` 记录 `register` 有没有跑过、靠 `waitForListen`（只探测 TCP 连通性）去读，两者之间没有同步——已改成 `chan struct{}` + `select` 等待。

## 现状（阶段一 Task 7 完成，`v0.1.0`）

SOP-L 十四项能力 + 结构三件套全部是真实实现，测试用真 PG（`besdk_*_probe` schema）+ 真 NATS 验证，不是 mock：

| 文件 | 干什么 | 覆盖的坑 |
|---|---|---|
| `endpoint.go` | 组件地址剥 scheme、`STORAGE_ENDPOINT` 反向加 scheme | 十八条第 1/12 条 |
| `tx.go` | `SET LOCAL ROLE/search_path`，COMMIT 后自动还原 | 十八条第 2 条 |
| `otel.go` | `otelBaseURL` 为空时 Blackhole（零 SpanProcessor，不联网不阻塞） | §7.5 优雅降级 |
| `logging.go` | 结构化 JSON + trace_id 自动注入 + `RedactPII`（脱敏+2KB截断） | §7.3 |
| `metrics.go` | 每模块独立 `Registry`，不用默认全局那个 | §12.5.2 |
| `query.go` | `ListWindow`：默认 90 天窗口 + `Limit` 上限（无 `Offset` 字段，决策 53） | §11.4、决策 53 |
| `archive.go` | `BatchGetRouted`：热表命中优先，缺失才查 `{schema}_archive` | §11.6.1 |
| `outbox.go` | `PublishOutbox`（同事务）+ `StartOutboxPump`（轮询发 NATS，失败重试不丢） | §3.10 |
| `events.go` | `Consume`：`event_inbox` 幂等 + `hop_count>5` 转发 `dlq.<subject>` + version 单调跳过乱序 | §3.10、§4.6、决策 42 |
| `standalone.go` / `gin.go` / `runtime.go` / `module.go` | `RunStandalone`/`Bootstrap`/`NewGinEngine` 编排；`gin.SetMode(Release)` 只设一次 | §12.5、十八条第 17/18 条 |

⚠️ **`Consume` 的范围声明**：走普通 NATS 核心订阅，没接 JetStream 手动 ack/重投——业务 `fn` 报错时只记日志，不会让消息重新投递。完整的 at-least-once 送达语义需要 JetStream durable consumer，是否升级留作待决问题。

## 用法

```go
package module

func New(ctx context.Context, rt *besdk.Runtime) (*besdk.Module, error) {
    engine := besdk.NewGinEngine(rt)
    engine.GET("/orders", handleList)
    return &besdk.Module{HTTPHandler: engine}, nil
}
```

```go
// main.go 只有一行
func main() { besdk.RunStandalone(module.New) }
```

## 依赖

`gin` / `pgx`（不用 `lib/pq`）/ `nats.go` / `go.opentelemetry.io/otel` / `google.golang.org/grpc` / `prometheus/client_golang` / `golang-jwt/jwt/v5` / `MicahParks/keyfunc`（+ 间接依赖 `MicahParks/jwkset`）。版本精确锁定（`go.mod` 里没有 `latest`），`go 1.25`——理由见 `docs/dev/实测踩坑记录.md` 类别 D0。
