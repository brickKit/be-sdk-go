# be-sdk-go

组件协议（`be-protocol`）的官方 Go 实现。组件怎样用 SDK 写在 `README.md`；本文件写给要修改 SDK、或要查某个机制在 SDK 里怎样实现的人：包地图、从哪开始、怎样测试、哪些不能弄坏。规则正文是 `be-protocol` 的 `spec/`（`P<章>.<序号>` 编号，`spec/00-reading.md` 有章节与装配项目协议的对照）；能力的实现状态在 `features.json`。

## 代码地图

| 路径 | 管什么 | 规则 |
| --- | --- | --- |
| `component/` | 组件交给启动器或外壳的定义：身份、版本、配置键、迁移、事件流、授权定义、模块的接口 | 第 01 章 |
| `launcher/` | 单独运行一个组件：`serve` 与 `migrate` 入口、配置、用户面与系统面的监听；`configSchema` 的生成与核对（`schema.go`） | 第 01、02 章 |
| `shell/` | 外壳启动器：一个进程承载编译进来的成员 | 协议 17 的外壳义务 |
| `runtime/` | 模块对外的唯一入口：配置、依赖地址、数据库、调用、事件、工作、审计、日志与指标 | 第 01 章 |
| `config/` | 读取声明的配置键：严格类型，空即未设 | 第 02 章 |
| `errs/` | 唯一的错误对象：原因、领域、gRPC 码与 HTTP 状态；原因目录在 `catalog.go` | 第 03 章 |
| `calls/` | 出站调用：并发上限、熔断、重试预算、逐跳缩短的时限、幂等键与转发用户凭据 | 第 04 章 |
| `reqctx/` | 出站调用要转发的请求级值 | 第 04 章 |
| `idem/` | 命令的幂等键：认领、写入与结果同一个事务，表 `be_idempotency`，过期清理作业 | P4.21–P4.25 |
| `db/` | 进入数据库的唯一方式：事务入口（`SET LOCAL` 角色、`search_path` 与超时）、迁移、单号、分区 | 第 05 章 |
| `ids/` | 记录身份（UUIDv7，组件在插入前生成） | 第 05 章 |
| `money/` | 金额与其他十进制：解析、格式化、舍入、分摊 | P5.25–P5.28 |
| `bizdate/` | 按法人时区的业务日期 | P5.29 |
| `events/` | 发件表、提交后编号、转发、订阅与收件表；`bus/` 按 `BUS_URL` 选运输，`jetstream/`、`pgqueue/` 是两个适配器，`transporttest/` 是它们共用的一致性套件 | 第 06 章 |
| `work/` | 后台工作项：原子认领、租约与纪元、副作用种类 | 第 07 章 |
| `identity/`、`guard/`、`authz/`、`audit/`、`records/`、`canon/` | 身份核验、用户面准入、授权（资源与操作定义、视图、`Check`、`List`、`Mask`）、审计、安全记录的封装与规范编码 | 第 08 章 |
| `list/`、`search/`、`projection/` | 列表的游标与筛选、搜索列、其他组件记录的投影 | 第 09 章 |
| `obs/` | 每个成员自己的日志、指标与追踪 | 第 10 章 |
| `httpx/`、`grpcx/` | 用户面（Gin）与系统面（gRPC）的服务器与中间件链 | 第 04、08 章 |
| `clock/` | 系统挂起时也计时的时钟 | P1.11 |
| `mergesafety/` | 找出合进外壳会坏的代码（进程级的全局状态） | 协议 17 |
| `sectest/` | 测试用的安全部署：令牌、信任根、授权与审计的替身（只给测试） | — |
| `seed/` | 组件种子命令共用的部分：授予、管理单元、按自然键找回 | 种子与测试数据 |
| `internal/` | 只给 SDK 自己：`host/`（启动器与外壳共用的组装）、`pgtest/`、`dbtest/`、`s3test/`、`echotest/` | — |
| `features.json` | 每项能力的实现状态与证据 | 协议 18 |

| 想查或想改 | 从这里开始 | 然后 |
| --- | --- | --- |
| 一个机制在 SDK 里怎样实现 | 上表找到包，读包的说明（`doc.go` 或包注释） | 包里的 `*_integration_test.go` 是可运行的用法 |
| 新加一条配置键或改启动行为 | `launcher/` | `component/`、`runtime/` |
| 新加一个错误原因 | `errs/catalog.go` | `be-protocol` 的错误目录与向量 |
| 事务与超时的行为 | `db/tx.go` | `db` 的集成测试 |
| 事件的发出与消费 | `events/producer.go`（发件表）、`events/forwarder.go`（转发）、`events/consumer.go`（订阅与收件表） | `events/transporttest/` |
| 一个能力的实现状态 | `features.json` | `features_test.go` |

## 构建与测试

- `make test`：`go vet`、单元测试、协议向量、SDK 自己的合并安全检查。
- `make test-integration`：要 `BE_SDK_TEST_PG_ADMIN`（PostgreSQL 16+ 的超级用户 DSN，装配项目的 `be-postgres`）与 `BE_SDK_TEST_NATS_URL`（开了 JetStream 的 NATS，`be-nats`）；包之间共用一个测试库，所以 `-p 1`。

## 设计取舍

- 规则在 `be-protocol`，SDK 只是一个实现：行为变了先改协议的条款与向量，再改 SDK；Python SDK 按同一份条款实现。
- 每个机制的入口只有一个（事务只经 `db`，出站只经 `calls`，事件只经 `events`），组件不能绕过：绕过就丢了身份、追踪、超时与合并安全。

## 易错点

| 不许 | 症状 | 原因 |
| --- | --- | --- |
| 在 SDK 里用进程级的全局状态（默认指标注册表、全局追踪提供者、`slog.SetDefault`） | 合进外壳后最后一个成员赢，或第二个成员注册时崩溃 | 一个进程承载多个成员；`mergesafety` 会找出来 |
| 改了行为不改 `features.json` 与协议向量 | 能力的状态与实际不符，Python SDK 照旧行为实现 | 状态与向量是两种 SDK 对齐的依据 |
| 在事务里发出站调用 | 运行时拒绝 | P4.20 |

## 改代码前自查

1. 这个改动对应 `be-protocol` 的哪条条款？条款或向量要不要先改？
2. `features.json` 的状态要不要变？
3. 同一个包的集成测试跑过了吗？
4. 升了版本吗（`version.go` 与标签）？
