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
| `db/` | 进入数据库的唯一方式：事务入口（`tx.go`：角色、`search_path`、超时都用 `set_config(…, true)` 设成事务级，与 `SET LOCAL` 等价）、拒绝把状态留在连接上的语句（`session.go`）、归还连接前比较服务端参数（`pool.go`）、迁移与迁移检查（`migrate.go`、`lint.go`）、单号、分区 | 第 05 章（P5.5、P5.30、P5.31） |
| `ids/` | 记录身份（UUIDv7，组件在插入前生成） | 第 05 章 |
| `money/` | 金额与其他十进制：解析、格式化、舍入、分摊 | P5.25–P5.28 |
| `bizdate/` | 按法人时区的业务日期 | P5.29 |
| `events/` | 发件表、提交后编号、转发、订阅与收件表、按位置读一条流（`readafter.go`，身份成员的已结束会话用它）、进度与补发（`progress.go` 求进度，`resend.go` 生产者的 `ResendService`，`redelivery.go` 消费者库回退后要补发，P6.36–P6.40）、记录的批量读取与快照（`records.go`，标准系统方法 `RecordService`，流在 `Stream.Records` 里声明读取函数）；`bus/` 按 `BUS_URL` 选运输，`jetstream/`、`pgqueue/` 是两个适配器，`transporttest/` 是它们共用的一致性套件 | 第 06 章 |
| `work/` | 后台工作项：原子认领、租约与纪元、副作用种类 | 第 07 章 |
| `identity/`、`guard/`、`authz/`、`audit/`、`records/`、`canon/` | 身份核验（含 `be_sv`）、用户面准入（含令牌陈旧）、授权（资源与操作定义、视图、`Check`、`List`、`Mask`）、审计、安全记录的封装与规范编码 | 第 08 章 |
| `list/`、`search/`、`projection/` | 列表的游标与筛选、搜索列、其他组件记录的投影（来源的客户端 `RecordClient`、`PullClient` 在 `projection/records.go`，逐页核对来源的声明） | 第 09 章 |
| `obs/` | 每个成员自己的日志、指标与追踪 | 第 10 章 |
| `httpx/`、`grpcx/` | 用户面（Gin）与系统面（gRPC）的服务器与中间件链 | 第 04、08 章 |
| `clock/` | 系统挂起时也计时的时钟 | P1.11 |
| `mergesafety/` | 找出合进外壳会坏的代码（进程级的全局状态） | 协议 17 |
| `evidence/` | 记录者一侧的证据：收紧两段写（意图、结论、取消）、一般记录、变更历史与提交序号、“已体现”、转运作业 `be.evidence.transfer`；属主的重放（`replay.go`，`ReplayService`，P12.19–P12.21）；运行时的 `rt.Evidence`，组件定义的 `Evidence` 标记给它运行档案的键 | 第 12 章 |
| `runprofile/` | 运行档案与告警渠道：对象锁的桶、签名的记录、恢复代次的读取与推进、告警的两份（`internal/s3sig` 是 SDK 自己的 SigV4，事件的大载荷也用）；信任材料历史（`trust.go`：受信的 CA 沿双签的轮换、按接收时刻判吊销，`ListAll` 按它核验，签发者用 `AppendWith` 以 CA 的钥匙写）；记录的写入者必须是签名证书的服务身份（`writerIsSigner`，P12.4） | 第 12 章 |
| `tiering/` | 冷热转层：组件声明的数据类别、封存与写屏障、转层与释放、读取与导出、保全销毁与擦除、路线更换、恢复核对；文件与职责、宿主与模块怎样接上，见包说明 `tiering/doc.go` | 第 15 章 |
| `sectest/` | 测试用的安全部署：令牌、信任根、授权与审计的替身、账号的安全变更（只给测试） | — |
| `sessionpb/` | `be.session.v1` 的生成代码（`make gen`），身份成员实现 `SessionService` 时用 | P8.28 |
| `reserve/` | 预占的参与方：分支的状态机（`lifecycle.go` 的 `Decide`，对协议向量）、命令与到期作业（`reserve.go`）；组件只写占用、归还、消耗与事件 | 第 16 章 |
| `flow/` | 流程内核（动作授权的签发与动作事件的收下在 `actions.go`，P17.19、P17.20）：流程定义、实例与任务、按岗位或个人分配（员工投影的目录 `employees.go`）、以人为单位的职责分离、唯一决定（纯函数 `decide.go` 对向量）、取代与撤回、放行核对、标准路由与重新解析、心跳（`routes.go`）、任务投影与通知意图的发布（`instance.go`、`intents.go`） | 第 17、18 章 |
| `storage/` | 存储端口：属主自己的桶与前缀上的条件放入（先探测）、条件复制（重写元数据）、读取与元数据、带长度与类型的预签名上传、强制保存的预签名下载（浏览器可达的地址 `OBJECT_STORE_PUBLIC_URL`）、删除与列举、错误分类（`internal/s3sig` 的 SigV4） | 第 19 章 |
| `channel/`、`channelpb/` | 渠道成员与所属组件共用的：动作事件与投递结果的载荷与发出、事件类型的推导、所属组件对一个动作的判断（`Take`，对向量 `channel-action`）；`channelpb` 是 `be.channel.v1.ChannelService` 的生成代码（`make gen`），通知组件调用、渠道成员实现；流程内核的签发与收下在 `flow/actions.go` | 第 20 章 |
| `notify/` | 通知意图的发送方一侧：共享意图契约、按协议的 schema 核对、经发件表发到发送方自己的流；流程内核的审批待办意图在 `flow/intents.go` | 第 18 章 |
| `extcmd/` | 对外命令：命令的写入与幂等、状态表（`state.go`，纯函数）、发送与查询的作业（`commands.go`）、整套恢复的外部对账（`reconcile.go` 的匹配是纯函数，`reconcileserver.go` 是 `ReconcileService`） | 第 13 章 |
| `recovery/` | 整套恢复的核对 `CheckService`：组件的登记（`checks.go`）、SDK 自带的检查（`builtin.go`）、服务（`server.go`）；单号的读取与提升在 `db/docno.go` | 第 14 章 |
| `externalpb/`、`recoverypb/` | `be.external.v1`、`be.recovery.v1` 的生成代码（`make gen`），恢复工具调用时用 | 第 13、14 章 |
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
- `go test -tags simulation -run TestTheLongSimulation -timeout 0 -v ./tiering/`：转层的长时间推演（`tiering/sim_test.go`：真实的 PostgreSQL 与对象存储、四个副本其中两个承载两个成员、每个状态迁移点注入崩溃、终态核对），要 `make test-integration` 的环境与 `BE_SDK_TEST_S3_*`；`SIM_MINUTES` 是注入崩溃的时长（默认 30），跑完还要等作业做完，按长任务放后台。

## 设计取舍

- 规则在 `be-protocol`，SDK 只是一个实现：行为变了先改协议的条款与向量，再改 SDK；Python SDK 按同一份条款实现。
- 每个机制的入口只有一个（事务只经 `db`，出站只经 `calls`，事件只经 `events`），组件不能绕过：绕过就丢了身份、追踪、超时与合并安全。

## 易错点

| 不许 | 症状 | 原因 |
| --- | --- | --- |
| 加一个 SDK 自己的后台作业而不写它在恢复阶段怎样（`internal/host/recoveryjobs.go` 的表） | 宿主拒绝启动；不拒绝时，它在整套恢复中不运行，往不回退的存储写的东西要到开放才送达（审计转运、证据转运各被演练撞出一次） | P1.23；第 4 项复盘 R1 |
| 在 SDK 里用进程级的全局状态（默认指标注册表、全局追踪提供者、`slog.SetDefault`） | 合进外壳后最后一个成员赢，或第二个成员注册时崩溃 | 一个进程承载多个成员；`mergesafety` 会找出来 |
| 改了行为不改 `features.json` 与协议向量 | 能力的状态与实际不符，Python SDK 照旧行为实现 | 状态与向量是两种 SDK 对齐的依据 |
| 在事务里发出站调用 | 运行时拒绝 | P4.20 |

## 改代码前自查

1. 这个改动对应 `be-protocol` 的哪条条款？条款或向量要不要先改？
2. `features.json` 的状态要不要变？完成或改了一项能力时，`rg` 这项能力的名字与条款号，`features.json` 里提到它的待验证说明（包括别的能力条目里的）一起改；待验证说明写还缺什么、在哪里验证过，不写“随第 N 步”“随某任务”这种会过时的去处（第 6 项复盘 P5：几条“随 T8”“需要后台工作项”的说明在做完之后留了三天）。
3. 同一个包的集成测试跑过了吗？
4. 升了版本吗（`version.go` 与标签）？
