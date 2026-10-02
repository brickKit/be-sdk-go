# be-sdk-go

Go 横切基础库（总纲 §4 SOP-L 十四项能力）。**不是 brickKit 组件**，也**不是公共 model 包**——零业务逻辑、零组件 model、零组件间引用。它是 `be-acceptance` 铁律六 import 扫描的唯一白名单。

## 它替所有 Go 组件挡住的坑

| 能力 | 文件 | 挡住的坑 |
|---|---|---|
| 依赖地址剥 scheme | `endpoint.go` 的 `cfg.Endpoint` | `grpc.Dial("http://host:9094")` 连不上，报错指向名称解析（十八条第 1 条） |
| `SET LOCAL` 事务 | `tx.go` | 不带 `LOCAL` 的 `SET` 之后连接还回池，下一个借用者原样继承，悄悄读写别人的数据（十八条第 2 条） |
| 单跑/合并统一入口 | `standalone.go`、`module.go`、`runtime.go`、`gin.go` | 每个组件各发明一个 `main`，合并那天全部重写（十八条第 18 条） |
| 外壳装配 | `shell/` | 每个外壳各写一遍 N 模块装配；一个模块的后台循环出错就把整个外壳拖下线；成员端口死了外壳 `/healthz` 却还是绿的；成员配置从外壳共享的进程环境里读、互相顶掉 |
| gRPC panic 恢复 | `standalone.go` 的 `grpcRecoveryInterceptor` | handler 里一个未处理的 panic（比如 ctx 没有 Claims 时误调 `ScopeOf`）不受任何东西保护，会一路冲出 grpc-go 崩掉整个进程——HTTP 侧一直有 `recoveryAndErrorMappingMiddleware`，gRPC 侧直到 `v0.2.4` 才补上对应物 |

## 配置（v0.3.0 起，brickKit v1 契约）

模块读配置只有一个入口 `rt.Config`；单跑时 `RunStandalone` 把进程环境整份灌进去，合并态由 `shell` 包给每个成员一份只属于它自己的 map。

- **键名精确匹配**：`configSchema` 的键就是环境变量名（UPPER_SNAKE），`cfg.String("PG_SCHEMA")` 原样查找，SDK 不再做 camelCase → SNAKE 转换。传 `pgSchema` 拿不到值。
- **数据库**：`besdk.PGDSN(cfg)` 从 `PG_HOST`/`PG_PORT`/`PG_DATABASE`/`PG_USER`/`PG_PASSWORD` 拼 DSN（口令经 `url.UserPassword` 转义，含 `@ : / %` 也不会截断）。`PG_PASSWORD` 可以是空串，但键必须存在。平台不注入 `DATABASE_*`。
- **TLS（`sslmode`）**：DSN 不带 `sslmode`，用 pgx 默认的 `prefer`——服务端支持 TLS 就用，不支持就退回明文，所以不开 TLS 的本地库直接可用。SDK 没有 `sslmode` 配置键；部署者要**强制** TLS，在 `PG_HOST` 指向的那一层解决：服务端 `pg_hba.conf` 只放行 `hostssl`（`prefer` 会先试 TLS，于是只剩 TLS 连接能进来），或者让 `PG_HOST` 指向一个只接受 TLS 的代理。注意 `prefer` 不校验服务端证书。迁移入口 `migrate.Main` 用的是同一套拼法，行为相同。
- **NATS**：`besdk.NATSURL(cfg)` 读 `NATS_URL`（完整 URL，可含凭据）。不再从 `MQ_*` 拼。
- **依赖地址**：`cfg.Endpoint(dep, extra)` / `cfg.MustEndpoint(dep, extra)` 读 `<ID>[_<PORT>]_ENDPOINT` 并剥掉 `http://`。读的是 `Config`，不是进程环境——外壳里成员的依赖地址只在它自己那一项成员配置里。
- **对象存储**：`cfg.S3URL()` 读 `S3_URL`（完整 URL，原样返回）。`STORAGE_ENDPOINT` 不再注入，`*_ENDPOINT` 后缀是保留名，不能当配置键。
- **gRPC 客户端**：`besdk.UserClient(ctx, cfg, dep, extra)` / `besdk.SystemClient(cfg, dep, extra)`，地址从 `cfg` 取。
- **权限判定**：读 `IAM_JWKS_URL` / `AUTHZ_BUNDLE_URL`。
- **额外端口**：`component.yaml` 声明了 `extraPorts` 而模块没有返回 `RegisterGRPC` 时，`RunStandalone` 以非零码退出并点名端口（与外壳同一条规矩）。
- **可观测**：`OTEL_BASE_URL`，为空时 Blackhole。

## Module

模块的构造函数 `func New(ctx context.Context, rt *besdk.Runtime) (*besdk.Module, error)` 交回一个 `*besdk.Module`，模块自己不 Listen、不注册全局、不装信号处理器：

| 字段 | 必填 | 含义 |
|---|---|---|
| `HTTPHandler` | 是 | 用 `besdk.NewGinEngine(rt)` 构造；单跑由 `RunStandalone`、合并态由 `shell` 包在 `rt.HTTPPort` 上 Serve |
| `RegisterGRPC` | 声明了 `extraPorts` 时必填 | 在 SDK 构造的 gRPC server 上注册服务（自带 panic 恢复拦截器）；声明了额外端口却为 nil，单跑与外壳都启动即失败 |
| `Start` | 否 | 后台循环，必须接 ctx、cancel 时返回 |
| `Stop` | 否 | 关停时调用，带 30 秒超时的 ctx |

⚠️ **v0.4.0 删除了 `Module.Migrations` 字段**（破坏性）：迁移从来不由 `RunStandalone` 或外壳执行，留着这个字段只会让人以为"填了就会跑"。组件改用下面的 `migrate.Main`。

## 迁移

迁移由 brickKit 负责：在组件（单跑）或外壳（合并部署）启动之前，用**组件自己的镜像和它自己的配置**跑一次性迁移。入口是组件 `backend/cmd/migrate/main.go`，只有一行：

```go
package main

import (
    "github.com/brickKit/be-sdk-go/migrate"
    "github.com/brickKit/<repo>/v2/migrations" // //go:embed *.sql 的 FS
)

func main() { migrate.Main(migrations.FS) }
```

- **参数**：恰好一个，`up` 或 `down`。参数不对时打印用法、以 **2** 退出，不读环境、不连库。
- **配置**：从进程环境读 `PG_HOST`/`PG_PORT`/`PG_DATABASE`/`PG_USER`/`PG_PASSWORD`/`PG_SCHEMA`，缺任一以 **1** 退出并点名缺的键（`PG_PASSWORD` 可以是空串，但键必须存在）。`PG_SCHEMA` 必须是小写标识符。
- **DSN**：复用 `besdk.PGDSN` 的拼法（口令转义），加 `search_path=<PG_SCHEMA>`（迁移里不带 schema 前缀的 SQL 和状态表都落在组件自己的 schema）和 `x-migrations-table=schema_migrations_<PG_SCHEMA>`（裸表名，不带 schema 前缀，不加 `x-migrations-table-quoted`）。schema 本身由装配项目的建库脚本预先建好，迁移不建 schema。
- **幂等**：`ErrNoChange`（已是最新 / 已全部回滚）不是错误，同一个迁移连跑两次都成功。
- **实现**：golang-migrate + `database/pgx/v5` + `source/iofs`。golang-migrate 只在 `migrate` 子包里 import，根包 `besdk` 不依赖它（`go list -deps github.com/brickKit/be-sdk-go | grep golang-migrate` 为空）。
- `migrate.Run(ctx, env, args, src)` 是可测形式：env 与 args 显式传入，返回错误不退出。

## Shell

外壳（把 N 个组件合进一个进程的部署形态）的全部装配在 `shell` 包里。外壳的 `main.go` 只有一行：

```go
package main

import (
    "github.com/brickKit/be-sdk-go/shell"
    mdmcustomer "github.com/brickKit/mdm-customer/backend/module"
    mdmproduct "github.com/brickKit/mdm-product/backend/module"
)

func main() {
    shell.Main("be-go-core", shell.Registry{
        "mdm/customer": mdmcustomer.New,
        "mdm/product":  mdmproduct.New,
    })
}
```

- **`Registry` 必须与外壳 `component.yaml` 的 `shell.members` 一一对应**。镜像里编进了谁由这里的静态 import 决定；平台下发的成员在 `Registry` 里找不到时，`Run` 启动即失败并点名该成员。
- **成员清单来自 `BRICKKIT_SERVED_MEMBERS_CONFIG`**（JSON 数组，每项 `componentId`/`version`/`httpPort`/`extraPorts`/`config`）。`config` 是这个成员独立部署时会拿到的全部变量（已求值，含 `*_ENDPOINT`），直接成为它的 `rt.Config`；外壳进程自己的环境只用来构造外壳自己的配置。三种状态含义不同：
  - 未设置：进程不是 brickkit 作为外壳启动的，报错退出；
  - `[]`：这次部署没有成员归这个外壳，只起外壳自己的 `/healthz`，不构造任何模块；
  - 空字符串：平台从不这样给，当作数据损坏，报错退出。
- **外壳自己的配置**：`PG_*`、`NATS_URL`、`OTEL_BASE_URL`、`AUTHZ_BUNDLE_URL`、`IAM_JWKS_URL` 写在外壳自己的 `configSchema` 里。`Run` 用它们开**一个**共享连接池和**一条** NATS 连接给全部成员，`InitShellAuthz` 只调一次。`AUTHZ_BUNDLE_URL` / `IAM_JWKS_URL` 缺任一（或为空白）时外壳启动即失败并点名缺的键（与 Python 外壳一致）——单跑组件缺它们只是 fail-closed，外壳里同样的缺失会让全部成员一起 403/503。外壳自己 `component.yaml` 的 `deployment.port` 只答 `/healthz`，不查任何成员或依赖。
- **迁移不在外壳里跑**：brickKit 在外壳启动前用每个成员自己的镜像和配置跑迁移（见下文「迁移」）。
- **失败处理**（三类，处理方式不同）：
  - **启动阶段失败**（任何一步，包括某个成员的构造函数返回错误、`Registry` 里找不到成员、构造函数返回 nil `Module`/`HTTPHandler`、外壳端口为 0、外壳缺 `AUTHZ_BUNDLE_URL`/`IAM_JWKS_URL`）：中止启动，先关掉共享池与 NATS 连接，再返回错误，外壳以非零码退出。
  - **端口失败**（外壳自己的 `/healthz`，或任一成员的 HTTP/额外端口；包括端口绑定失败，以及服务协程在没有收到关停信号时意外返回）：`Run` 返回点名该成员的错误，其余成员优雅退出（`Stop` 会被调用），外壳以非零码退出。这样做是为了把故障暴露出来：外壳 `/healthz` 只代表进程活着，成员端口死了它照样答 200，平台的 probe 看不到。
  - **成员 `Start()` 失败**（panic 或返回错误）：隔离。记一条 ERROR 日志（带 `module_component_id`；panic 时还带 `stack`，即 recover 处的完整堆栈），其余成员照常服务，外壳继续运行。这个成员的后台循环会一直停着，直到外壳下次重启；它自己的 HTTP/额外端口不受影响。
  - 收到 SIGTERM/SIGINT 时全部优雅退出，`Run` 返回 nil。关停开始之后，任何服务协程不论带着什么错误返回（比如 gRPC 的 `Serve` 晚于 `GracefulStop` 才开始时返回的 `grpc.ErrServerStopped`），都算正常关停。
  - 启动阶段还有一项校验：成员声明了额外端口，构造函数却没有返回 `RegisterGRPC`，同样中止启动，错误点名成员和端口——那个端口没人监听，就是一次静默故障。
- **panic 隔离的边界**（逐项核对过）：
  - **会被兜住的**：
    - 运行 `Start()` 的那个 goroutine 里的 panic：`shell` 包 recover，按上面「Start 失败」处理。只限这一个 goroutine，`Start()` 自己另起的 goroutine 不在其中。
    - HTTP handler 的 panic：用 `besdk.NewGinEngine` 构造的 engine 由 `recoveryAndErrorMappingMiddleware` 转成 500；即使不走 gin，`net/http` 自己也会 recover 这一个请求的 panic（记日志、断开这条连接），进程不会退出。
    - 一元 gRPC handler 的 panic：`ServeExtraPort` 装的 `grpcRecoveryInterceptor` 转成 `codes.Internal`。
  - **不会被兜住的**（一旦 panic，整个外壳进程退出，所有成员一起下线）：
    - 流式 gRPC handler（只装了 `UnaryInterceptor`，没有 stream 拦截器）；
    - `besdk.Consume` 的回调——`fn` 跑在 nats.go 的订阅回调 goroutine 里；
    - `Start()` 或构造函数自己另起的 goroutine；
    - 模块构造函数本身（启动阶段）；
    - `Stop()`（关停阶段）。

## 现状（阶段三 Task 8，`v0.2.4`）

`erp-inventory` 真机测试时崩了三次（`RestartCount` 0→3）：`Receive`/`Adjust`/`GetBalance`/`ListMovements` 四个方法自阶段三 Task 6 起会在 `service` 层调 `besdk.ScopeOf(ctx)`，这四个方法本来只该走 REST（有 `RequirePermission` 中间件保证 ctx 里有 Claims），但它们同时也在 gRPC 服务定义里——被直接用 gRPC 调用时（本仓库自己的测试代码图省事这么调过）ctx 里没有 Claims，`ScopeOf` 按设计 panic（fail-loud 是刻意的，见 `scope.go`），而 `serveExtraPort` 的 `grpc.NewServer()` 从 `v0.1.0` 起就是裸的、零拦截器——panic 没有任何防护，一路把整个容器进程带崩，不是"这一个 RPC 报错"。

这不是 `erp-inventory` 一个组件的问题：**任何**组件的**任何** gRPC handler 未来出现类似疏漏都会是同样的后果（`Bootstrap`/`gin.go` 早就有的教训——十八条第 18 条"一个模块不许把整组拖下水"，但那条规则此前只在 HTTP 侧被真正兜住）。修法：`serveExtraPort` 的 `grpc.NewServer()` 加一个 `grpc.UnaryInterceptor(grpcRecoveryInterceptor(logger))`，同 `gin.go` 的 `recoveryAndErrorMappingMiddleware` 是同一个判据——`recover()` 到内容只记日志（`rt.Logger.Error`），不回传给客户端（同 HTTP 侧不泄露内部细节的既有判据），返回一个干净的 `codes.Internal`。新增回归测试 `TestServeExtraPort_handler里panic不崩进程返回Internal错误`：手写一个最小 `grpc.ServiceDesc`（不需要专门写 `.proto`）注册一个必然 panic 的方法，真拨号真调用，断言客户端收到干净的 `codes.Internal`（不是连接被重置/EOF）、panic 原始内容没有回传、且同一条连接紧接着能再调一次证明 server 本身没有被拖垮。

详细的真机复现过程（`docker logs`/`RestartCount` 实证）记在 `be-assembly-standard` 仓库的 `docs/dev/实测踩坑记录.md` C11——那份文档追的是装配仓库真机部署时的坑，这里只记 SDK 自身改了什么。

## 现状（阶段三 Task 5，权限判定真正上线）

`RequirePermission`/`ScopeOf` 从阶段二的 fail-closed stub 换成真实判定——这是三个 `be-sdk-*` 共用的机制，`infra-authz` 建成之后才有真实数据可以对着测。⚠️ **这套机制本身的协议描述（JWT claims 约定、bundle 的 wire format、判定链、ScopeFilter 语义）见 [`docs/authz-protocol.md`](docs/authz-protocol.md)**——独立写的，不假设读者知道 brickKit 是什么，换一个签发方/策略服务实现也能对着它接。

- **JWT 本地验签**：`IAM_JWKS_URL` 指向的 JWKS 端点，用 [`MicahParks/keyfunc`](https://github.com/MicahParks/keyfunc)（自带 JWK Set 后台刷新，不用自己写缓存）配 [`golang-jwt/jwt/v5`](https://github.com/golang-jwt/jwt)，只认 `RS256`。`infra-iam-casdoor` 要到阶段三 Task 7 才建仓库，暂时没有真实签发方——测试自己起一对 RSA 密钥 + 一个 `httptest.Server` 当 JWKS 端点，加密运算是真的，只是身份是测试夹具。
- **bundle 轮询**：15 秒条件 GET `AUTHZ_BUNDLE_URL`（`If-None-Match`，未变化 304 不重新解析），进程级单例，模块代码看不见（§14.1.4）。有一条测试真等 15 秒验证"改角色分配不重启组件也能生效"，不是 mock 时钟。
- **`Authenticated` 新哨兵值**：阶段三 Task 4 写 `infra-authz` 时发现的真实缺口——`Public`/具体权限键两档之间缺"已登录即可，不需要权限键"这一档（`GET /api/me/permissions` 这类端点）。仍然验签、仍然查 `stale_since`，只是跳过权限键查找。
- **降级语义按 §14.1.9 精确区分三种状态**：`IAM_JWKS_URL` 没配 → 阶段二遗留行为，非 Public 一律 403；配了但 bundle 从没连上过 → 503（不是 403，语义更准）；连上过但角色没这条权限 → 403。
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

> 下表是 brickKit v0 的契约，v0.3.0 已整体换成上文「配置」一节的 `PG_*`/`NATS_URL`，`buildPGDSN`/`buildNATSURL` 已删除。端口仍从 `component.yaml` 读。

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
| `endpoint.go` | 依赖地址剥 scheme（`cfg.Endpoint`）、对象存储地址（v0.3.0 起 `cfg.S3URL` 读完整 URL，原 `STORAGE_ENDPOINT` 反向加 scheme 已删除） | 十八条第 1/12 条 |
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
// backend/cmd/server/main.go 只有一行
func main() { besdk.RunStandalone(module.New) }

// backend/cmd/migrate/main.go 也只有一行
func main() { migrate.Main(migrations.FS) }
```

```go
// 读配置、拨依赖：一律从 rt.Config
schema := rt.Config.MustString("PG_SCHEMA")
conn, err := besdk.SystemClient(rt.Config, "mdm/customer", "grpc") // 只许在 Start() / 事件 handler 里用
```

外壳的 `main.go` 见上文「Shell」一节。

## 依赖

`gin` / `pgx`（不用 `lib/pq`）/ `nats.go` / `go.opentelemetry.io/otel` / `google.golang.org/grpc` / `prometheus/client_golang` / `golang-jwt/jwt/v5` / `MicahParks/keyfunc`（+ 间接依赖 `MicahParks/jwkset`）；`golang-migrate/migrate/v4` 只被 `migrate` 子包使用。版本精确锁定（`go.mod` 里没有 `latest`），`go 1.25.11`（golang-migrate v4.20.1 的最低要求；`golang:1.25-alpine` 构建镜像满足）。
