# be-sdk-go

组件协议（[be-protocol](https://github.com/brickKit/be-protocol)）的官方 Go 实现：组件只写业务，生命周期、配置、错误、观测、用户面的中间件链、外壳里的合并安全由 SDK 负责。

旧版本（0.x）在分支 `legacy/v0`（最新的旧代码来自 `stage-b`），标签保留；本主分支按终局设计从零开始（D59），版本从 `3.0.0` 起（A110），模块路径 `github.com/brickKit/be-sdk-go/v3`。设计依据在装配项目 [be-assembly-standard](https://github.com/brickKit/be-assembly-standard) 的协议 17、18 与组件实现约定；规则正文在 `be-protocol`。

## 组件怎样用

```go
// 组件根包：一个模块入口，单独运行与进外壳调用的是同一个
var Definition = component.Definition{
	ID: "erp/sales", Version: "3.0.0",
	Keys: []config.Key{{Name: "SALES_QUOTE_DAYS", Type: config.Integer, Default: 30}},
	Migrations: migrations.Files, // embed.FS of <version>_<title>.up.sql; nil: no database
	New: func(rt *runtime.Runtime) (component.Module, error) { return newModule(rt) },
}

// cmd/server/main.go：单独运行
func main() { launcher.Main(sales.Definition) }
```

模块从 `rt` 取一切：配置（`rt.Config`）、依赖地址（`rt.Dependency`，可选依赖缺席时返回“不存在”）、数据库（`rt.DB`，只经 `rt.DB.Tx` 与 `BackgroundTx` 进入事务）、调用其他组件（`rt.HTTP` 走用户面、`rt.GRPC` 走系统面）、日志、指标注册器、追踪器。声明了 `Migrations` 的组件自动带上数据库的键；镜像的 `migrate` 入口先执行 SDK 自己的表（来自 `be-protocol` 的 `migrations/`），再执行组件的。模块在 `Routes` 里登记路由，每条路由声明它的操作（`httpx.Route.Operation`），公开接口显式标 `Public`；有系统面的模块实现 `Services`，用 `grpcx.Services.Register` 登记服务，每个方法同样声明操作，定义里写 `GRPCPort`（`component.yaml` 中 `grpc` 额外端口）。

调用其他组件只经运行时的客户端：每个（成员，依赖）一个并发上限、熔断器与重试预算；每次调用的时限逐跳缩短，只有声明为幂等的方法（gRPC 加 `calls.Idempotent()`，HTTP 的 `GET` 或 `Request.Idempotent`）在依赖自己不可用时重试，最多 3 次；依赖转述的更下游故障不重试，所以一条调用链只有一层重试。最终失败按协议向上答（`DEPENDENCY_UNAVAILABLE`、`DEADLINE_EXCEEDED`、非幂等时 `RESULT_UNKNOWN`），业务错误原样穿透。替用户读取用 `Request.ForwardUser` 转发原凭据。事务内的调用被拒绝。

产生事件的组件在定义里声明 `Streams`，在业务事务内调用 `rt.Events.Emit`；SDK 负责编号、转发与信封。消费事件的组件声明 `Subscribes` 并实现 `Subscriptions`，处理函数在 SDK 开的事务里执行，收件表与业务写入同时提交，重复投递只生效一次，最后一次仍失败的记为死信。两者都需要数据库与 `BUS_URL`。

## 包

| 包 | 内容 | 协议 |
| --- | --- | --- |
| `component` | 组件定义与模块接口（`Module`、可选的 `Starter`、`Stopper`） | 组件实现约定 |
| `launcher` | 单独运行：`serve`、`migrate` 入口、退出码、就绪、体面停止 | 第 1 章 |
| `config` | 声明的键、严格解析、一次报全、密钥与 `_FILE`、重读 | 第 2 章 |
| `errs` | 一个错误对象、平台原因目录、码与状态、problem+json | 第 3 章 |
| `httpx` | 每个成员一个 Gin 引擎与中间件链；路由必须声明操作；平台端点 | 第 1、3、4 章 |
| `runtime` | 模块唯一的入口 | 组件实现约定 |
| `obs` | 每个成员的日志、指标注册表、追踪提供者 | 协议 17 的观测 |
| `clock` | 计入挂起的时钟（`CLOCK_BOOTTIME`）与启动检查 | 第 1 章 P1.11 |
| `db` | 连接池（单独运行一个、外壳按目标共享）、事务入口（事务内设置角色、schema、超时；预算；重试与错误映射）、启动检查、迁移入口与迁移检查、单号序列 | 第 5 章 |
| `ids` | 记录身份 UUIDv7 | 第 5 章 P5.17 |
| `calls` | 出站调用：出站时限、重试决定与预算、熔断、并发上限、结果分类与向上报告；HTTP 与 gRPC 客户端 | 第 4 章 |
| `grpcx` | 系统面：每个成员一个 gRPC 服务端与拦截器链，方法必须声明操作 | 第 1、3、4 章 |
| `reqctx` | 出站调用转发的请求范围的值（请求 ID、用户凭据） | 第 4 章 P4.2 |
| `events` | 事件：流与分区、发件表（`rt.Events.Emit` 在业务事务内写）、锁内编号与纪元、转发、信封、运输端口与内存参考实现、消费者（收件表去重、死信、大消息凭证、跳数） | 第 6 章 |
| `events/jetstream`、`events/pgqueue`、`events/bus` | 两个运输适配器与按 `BUS_URL` 选择；`events/transporttest` 是两者共跑的运输检查 | 第 6 章 P6.16–P6.24 |
| `shell` | 外壳启动器：按构建信息核对成员、拒绝 `replace`、每个成员独立的端口与运行时、共享池 | 协议 17 的外壳义务 |
| `mergesafety` | 合并安全检查：读进程环境、初始化进程级的东西、退出进程、绕过 SDK 建引擎与连接 | 组件实现约定的合并安全 |

后台工作、身份与授权、列表与投影随实验计划第 2 步的后续任务加入（装配项目 `dev/rebuild/step2-plan.md`）。

## 测试

`make test`：L1 测试，含协议向量（按 `go.mod` 中 `be-protocol` 的精确版本）与 SDK 自身的合并安全检查。需要真实 PostgreSQL（16 及以上）与消息系统的测试在构建标签 `integration` 下，`make test-integration` 运行：环境变量 `BE_SDK_TEST_PG_ADMIN` 给出超级用户的连接串（装配项目的 `be-postgres`），`BE_SDK_TEST_NATS_URL` 给出开启 JetStream 的 NATS（`be-nats`），测试在库 `be_sdk_test` 中自建角色与 schema；各包共用这个库，所以按包串行（`-p 1`）。缺环境时失败，不跳过。
