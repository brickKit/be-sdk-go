# be-sdk-go

组件协议（[be-protocol](https://github.com/brickKit/be-protocol)）的官方 Go 实现：组件只写业务，生命周期、配置、错误、观测、用户面的中间件链、外壳里的合并安全由 SDK 负责。

旧版本（0.x）在分支 `legacy/v0`（最新的旧代码来自 `stage-b`），标签保留；本主分支按终局设计从零开始（D59），版本从 `3.0.0` 起（A110），模块路径 `github.com/brickKit/be-sdk-go/v3`。设计依据在装配项目 [be-assembly-standard](https://github.com/brickKit/be-assembly-standard) 的协议 17、18 与组件实现约定；规则正文在 `be-protocol`。

## 组件怎样用

### 定义与入口

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

模块从 `rt` 取一切：配置（`rt.Config`）、依赖地址（`rt.Dependency`，可选依赖缺席时返回“不存在”）、数据库（`rt.DB`）、调用其他组件（`rt.HTTP`、`rt.GRPC`）、事件（`rt.Events`）、后台工作（`rt.Work`）、审计（`rt.Audit`）、日志、指标注册器、追踪器。

### 用户面、系统面与授权

模块在 `Routes` 里登记路由，每条路由声明它的操作（`httpx.Route.Operation`），公开接口显式标 `Public`；有系统面的模块实现 `Services`，用 `grpcx.Services.Register` 登记服务，每个方法同样声明操作与允许调用它的组件，定义里写 `GRPCPort`（`component.yaml` 中 `grpc` 额外端口）。系统面只接受部署 mTLS，方法里用 `grpcx.Caller` 得到调用的组件。

受保护的路由与系统方法的操作都要在定义的 `Authz` 中声明（未声明则启动失败）。启动时 SDK 把这些声明作为定义记录提交给授权成员（`contract-infra-authz` 的 `DefinitionService.Submit`），被接受之前成员不就绪；同一版本只能有一套定义，改了声明就要升组件的版本。视图、定义与审计记录都以规范 JSON 字节传输，收到的按 be-protocol 的 `schemas/security/` 校验（D76）。用户面的请求由 SDK 核验凭据并取得授权视图，处理函数用 `authz.From(ctx)` 得到请求的 `Subject`：单条记录用 `Check`（看不见答 404、看得见无权答 403、无法证明答 503），列表用 `List` 得到要与自己的筛选相与的谓词与逐行命中位图，输出用 `authz.Mask(资源, 对象, 可读字段)` 成形：不可读的声明字段为 `null` 并列在 `_masked`，秘密与未声明的键不出现（P8.31）；`Check` 给的可读字段是所问操作自己的段的（导出、特殊读取不借读取页面的字段，P8.15）。视图里与组件自己的资源定义不合的段（少一维、多一维等）不参与求值（P8.14）。列表可以用 `ReadableOnly` 只看某些受限字段可读的行（P9.4）。写事务在提交前自动再查一次截止时间。

### 数据库与数据形态

只经 `rt.DB.Tx`（前台）与 `BackgroundTx`（后台）进入事务。声明了 `Migrations` 的组件自动带上数据库的键；镜像的 `migrate` 入口先执行 SDK 自己的表（来自 `be-protocol` 的 `migrations/`），再执行组件的。记录身份用 `ids.New`（UUIDv7），单号用 `db.NextNumber`（连续序列在签发的事务中取号）。按月分区的表在组件迁移中建好后用 `SELECT be_declare_monthly_partitions('<表>', <预建月数>)` 声明，之后由 SDK 自带的作业预建。金额与其他十进制用 `money`（解析、按币种格式化、舍入、现金舍入、分摊），业务日期与“今天”用 `bizdate` 按法人时区计算后作参数传给 SQL。

### 命令的幂等键

有副作用的命令：处理函数先 `idem.FromHTTP(c)`（系统面 `idem.FromGRPC`）得到请求，再把业务写入放进 `idem.Do`，返回命令的决定（例如新记录的身份）；同键同请求重放这个决定，处理函数按调用者当前的授权重新读取后回答。

### 调用其他组件

只经运行时的客户端（`rt.HTTP` 走用户面、`rt.GRPC` 走系统面）：每个（成员，依赖）一个并发上限、熔断器与重试预算；每次调用的时限逐跳缩短，只有声明为幂等的方法（gRPC 加 `calls.Idempotent()`，HTTP 的 `GET` 或 `Request.Idempotent`）在依赖自己不可用时重试，最多 3 次；依赖转述的更下游故障不重试，所以一条调用链只有一层重试。最终失败按协议向上答（`DEPENDENCY_UNAVAILABLE`、`DEADLINE_EXCEEDED`、非幂等时 `RESULT_UNKNOWN`），业务错误原样穿透。命令带幂等键（`Request.IdempotencyKey`、`calls.IdempotencyKey`）；替用户读取用 `Request.ForwardUser` 转发原凭据。事务内的调用被拒绝。

### 事件

产生事件的组件在定义里声明 `Streams`，在业务事务内调用 `rt.Events.Emit`；SDK 负责编号、转发与信封。超过 64 KiB 的载荷经对象存储：生产者配置自己的 `OBJECT_STORE_*`，转发时写入并发出预签名的只读地址，订阅者不需要存储凭据。消费事件的组件声明 `Subscribes` 并实现 `Subscriptions`，处理函数在 SDK 开的事务里执行，收件表与业务写入同时提交，重复投递只生效一次，最后一次仍失败的记为死信。两者都需要数据库与 `BUS_URL`。发布的事件类型同时列在 `component.yaml` 的 `events.publishes`：组件的测试用 `launcher.EventProblems` 核对它与 `contract/events/` 下的契约一一对应；订阅的列在 `events.subscribes`，由组件自己保持一致。

补发（P6.36–P6.40）：有流的组件在定义里写 `Resend`（补发方法 `be.source.v1.ResendService` 的操作，惯例是 `<前缀>.system.resend`，登记在本组件的 `system` 资源下），SDK 挂上这个系统方法，部署内任何组件可以调用；没写时启动失败。消费者的收件表记下每个事件在来源的位置，订阅每次启动记一行；库回退过（恢复代次推进或时间线变了）的消费者启动时按进度向它见过的生产者要补发，生产者的地址取依赖的 `grpc` 端口（`<依赖>_GRPC_ENDPOINT`），没有地址的记错误日志。补发覆盖不了的分区（之后的行已清掉）记进 `be_resend_gap`；读模型的订阅在 `Sub.Repair` 里给出修复（通常是投影的 `Align`）。

### 后台工作

有后台工作的模块实现 `Jobs`，每个作业声明种类（queue、cron、singleton、every）与副作用种类；queue 作业在业务事务内用 `rt.Work.Enqueue` 排入。处理函数拿到 `*work.Run`：`local` 作业用 `Complete` 把效果与完成标记一起提交；非幂等外部调用先 `BeforeCall` 再发出；singleton 用 `Checkpoint` 分段提交进度。返回 `work.Permanent`、`work.NotEffective`、`work.Wait` 区分失败，其余错误按退避重试；每次运行有截止时间（`Job.Timeout`，缺省 5 分钟），出站调用靠它。

### 对外命令

向外部系统（银行、税局、承运商）发命令的模块实现 `extcmd.Provider`：给出适配器（`Send` 发出并把结果分成未到达、可能到达、受理、成功、失败、拒绝；`Query` 按业务参考号查询）与每个操作的能力（去重保留期与计期起点、能否查询、过期、最长处理时间、终结时限、是否不可撤回），定义里写 `ExternalCommands`。业务事务里用 `rt.External.Request` 写命令（按请求方与业务参考号幂等），提交之后 SDK 的作业发出：结果不知道的先查询再决定是否重发，重发沿用同一个参考号并守重发截止时刻，不可撤回的命令发出前先把发出记录交给证据存储（定义还要写 `Evidence`），转人工的由人用 `Register` 登记结果。部署级 `EXTERNAL_COMMANDS_QUERY_ONLY` 只查询不发送（第 13 章）。适配器另外实现 `extcmd.Lister`（按时间窗列出外部记录）时，整套恢复的对账用一次列出代替逐条查询。

### 整套恢复

整套恢复（装配项目协议 15）由恢复工具 `ops/recover` 驱动，组件要准备的是：

- **阶段**（P1.22–P1.24）：部署级配置键 `RECOVERY_PHASE` 有值时成员进入“恢复就绪”：`/readyz` 答 200 并带阶段；系统面一直照常。阶段只在启动时读，换阶段就重启（恢复工具改共用变量再 `brickkit up`）。模块有自己的后台要在某个阶段收住时，读 `rt.RecoveryPhase()`。

  | 阶段 | 用户面 | 模块的 `Start`、转发、订阅 | 后台作业 |
  | --- | --- | --- | --- |
  | `frozen` | 全部 `MAINTENANCE` | 不跑 | 不跑 |
  | `redelivering`、`replaying` | 全部 `MAINTENANCE` | 跑（补发、投影） | 不跑 |
  | `verifying` | 全部 `MAINTENANCE` | 跑 | 只有对外命令，只查询不发送 |
  | `read_only` | 放行 `GET`、`HEAD` 与声明了 `httpx.Route.ReadOnlyOpen` 的写入（身份成员的一次性码、重设密码、登记第二因素），其余 `MAINTENANCE` | 跑 | 同 `verifying` |

  `frozen` 以外的阶段里，SDK 的审计转运与证据转运在后台作业之外照常运行（审计存储与证据存储都不随恢复回退：恢复中写下的审计要在开放之前到达，证据的结论要在重放之前到达，P1.23）。

- **恢复操作**：有数据库的组件在定义里写 `Recovery`（操作，惯例 `<前缀>.system.recover`，登记在本组件的 `system` 资源下），没写时启动失败。下面的系统方法都用它，只许恢复工具调用。
- **双通道失效**（P12.23）：一条收紧的意图证据存储与告警渠道都没收下时，SDK 除了记严重日志，还在本地单独提交一条类别为 `evidence_double_failure` 的一般记录（点名那条意图：记录身份、类别、主体、对象、时刻），随证据转运送到证据存储；恢复工具不重放它，第 9 步列为损失。
- **证据的重放**（P12.19–P12.22）：写证据的模块实现 `evidence.Replayer`，按类别注册重新执行的函数（`ReplayFunc`：在给定的事务里按记录的 `change` 重新施加，不重做业务前置校验，被引用的对象不在时返回 `evidence.ErrDangling`，依赖的东西还没到时返回 `evidence.ErrDeferred`：答 `deferred`、回滚，恢复工具在 6.4 之后再问），定义里写 `Replay`（操作，惯例 `<前缀>.system.replay`）。SDK 挂上 `be.evidence.v1.ReplayService`，按记录身份判“已体现”，重新执行与变更历史在一个事务里。效果在恢复组之外、不能重新执行的类别（协议 15 的 6.7：身份产品不在恢复组内时的凭据收紧）改用 `Replays.Check` 注册核对（`CheckFunc`，事务之外问那一方，答成立与否；问不出算不成立），SDK 答 `consistent` 或 `inconsistent`，不写变更历史；一个类别只能注册一种（宿主拒绝启动）。在阶段 `replaying` 下做。
- **外部对账**（P13.11–P13.13）：发对外命令的成员自动挂上 `be.external.v1.ReconcileService`：对上外部记录的按外部推进，没对上的写重发截止、转 `unknown` 或 `manual`，并记下最早一次可能的发送与故障时刻供开放之后判断；结论按恢复留存，重做时回答同样的结论。
- **核对**（第 14 章）：每个有数据库的成员自动挂上 `be.recovery.v1.CheckService`。SDK 自带三项检查（投影已对齐且没有被隔离的主体、消费者在当前纪元记下了进度且补发没有留下空缺、本地审计积压）；模块实现 `recovery.Checker`，用 `Checks.Add` 加自己负责的业务检查（读库只经传进来的 `Read`，每次一个总是回滚的事务；跨组件的一项在两次读取之间调用另一边；只读本库的用 `recovery.InTx` 包一下），用 `Checks.Presence` 为每个审计业务变更或敏感读取的对象类型登记“还在不在、版本多少”的查询（损失范围；有版本列的表用 `recovery.TablePresence`；审计的 `target` 要带变更之后的 `version`，业务变更用 `audit.TargetOf(类型, 身份, 变更后的对象)` 构造；签发连续单号的业务变更在 `changes` 里放 `document_number: audit.DocumentNumber(序列, 号)`，恢复工具从它求已用的最大值），有连续单号序列的用 `Checks.Void` 登记怎样作废提升跨过的空号。

### 列表与投影

列表接口用 `list.Build`：传入资源的列声明（可排序、可筛选、可搜索）与请求（排序、筛选、`q`、游标、页大小），得到与授权谓词拼好的条件、参数、排序与命中位图；取到一页后用 `Next` 得到下一页的游标。授权或资源定义变了，旧游标答 `QUERY_PLAN_CHANGED`。

投影其他组件的记录用 `projection.Projection`：消费者在处理事件的事务里 `Apply`，返回需要取回时排入取回工作项（`Repair`）；来源换了实现实例或纪元时 `Align` 全量重新取回。

### 审计

需要审计的操作经 `rt.Audit` 记录：变更用 `Write`（与业务同一事务），敏感读取与导出用 `Read`（先记，写不进去就拒绝读取），安全拒绝用 `Denial`。编号、汇总、转运与删除由 SDK 在后台完成（`AUDIT_GRPC_URL`）。

### SDK 自带的作业

每个有数据库的成员都运行这些 singleton 作业，组件不用登记：

| 作业 | 间隔 | 做什么 | 协议 |
| --- | --- | --- | --- |
| `be.audit.transfer` | 5 秒，积压时连续 | 审计行的编号、拒绝汇总、转运、按已备份水位删除 | P8.20–P8.23 |
| `be.partitions.maintain` | 1 小时 | 声明的按月分区表预建到当月加预建月数；边界异常时失败 | P5.24 |
| `be.idempotency.cleanup` | 1 小时 | 删除过期的幂等记录（保留 24 小时） | P4.25 |
| `be.outbox.purge` | 1 小时 | 删除超过补发保留（7 天）的已发布发件行 | P6.12 |
| `be.work.cleanup` | 1 小时 | 删除超过保留的已完成工作项 | P7.19 |

打开 PostgreSQL 队列运输的进程另外每小时清理超过运输保留（7 天）的消息（P6.27）。

## 包

| 包 | 内容 | 协议 |
| --- | --- | --- |
| `component` | 组件定义与模块接口（`Module`、可选的 `Starter`、`Stopper`） | 组件实现约定 |
| `runtime` | 模块唯一的入口 | 组件实现约定 |
| `launcher` | 单独运行：`serve`、`migrate` 入口、退出码、就绪、体面停止 | 第 1 章 |
| `shell` | 外壳启动器：按构建信息核对成员、拒绝 `replace`、每个成员独立的端口与运行时、共享池 | 协议 17 的外壳义务 |
| `config` | 声明的键、严格解析、一次报全、密钥与 `_FILE`、重读 | 第 2 章 |
| `errs` | 一个错误对象、平台原因目录、码与状态、problem+json | 第 3 章 |
| `obs` | 每个成员的日志、指标注册表、追踪提供者 | 协议 17 的观测 |
| `clock` | 计入挂起的时钟（`CLOCK_BOOTTIME`）与启动检查 | 第 1 章 P1.11 |
| `mergesafety` | 合并安全检查：读进程环境、初始化进程级的东西、退出进程、绕过 SDK 建引擎与连接 | 组件实现约定的合并安全 |
| `httpx` | 用户面：每个成员一个 Gin 引擎与中间件链；路由必须声明操作；平台端点 | 第 1、3、4 章 |
| `grpcx` | 系统面：每个成员一个 gRPC 服务端与拦截器链，方法必须声明操作与允许的调用组件 | 第 1、3、4、8 章 |
| `calls` | 出站调用：出站时限、重试决定与预算、熔断、并发上限、结果分类与向上报告；HTTP 与 gRPC 客户端 | 第 4 章 |
| `reqctx` | 出站调用转发的请求范围的值（请求 ID、用户凭据） | 第 4 章 P4.2 |
| `idem` | 命令的幂等键：键的校验与作用域、请求指纹、认领与业务写入与结果同一事务、重放与冲突、过期清理 | 第 4 章 P4.21–P4.25 |
| `db` | 连接池（单独运行一个、外壳按目标共享）、事务入口（事务内设置角色、schema、超时；预算；重试与错误映射）、启动检查、迁移入口与迁移检查、单号序列、按月分区的维护 | 第 5 章 |
| `ids` | 记录身份 UUIDv7 | 第 5 章 P5.17 |
| `money` | 金额与十进制：契约字符串的严格解析与按币种小数位格式化、两种舍入方式、现金舍入、最大余数法分摊；不经过浮点 | 第 5 章 P5.25–P5.28 |
| `bizdate` | 业务日期：时刻在法人时区中的本地日期与“今天”；SDK 自带时区数据库 | 第 5 章 P5.29 |
| `events` | 事件：流与分区、发件表（锁内编号、纪元、转发、保留）、信封、运输端口与内存参考实现、消费者（收件表去重、死信、大消息凭证、跳数） | 第 6 章 |
| `events/jetstream`、`events/pgqueue`、`events/bus` | 两个运输适配器与按 `BUS_URL` 选择；`events/transporttest` 是两者共跑的运输检查 | 第 6 章 P6.16–P6.24、P6.27 |
| `work` | 后台工作项：原子认领、租约与代次、停止时释放租约、副作用种类、重试与死信、cron 时间槽与物化水位、singleton、每副本计时器、运维动作与清理 | 第 7 章 |
| `identity` | 用户访问凭据的核验（`at+jwt`，三种算法，公钥集合每进程一个、刷新与限速）；服务身份（部署 mTLS，URI SAN）与调用方 | 第 8 章 P8.1–P8.6 |
| `authz` | 资源、操作与字段的声明；授权视图的持有与核验（有效期从发出请求起算、恢复代次、停顿检测）；单对象三值求值与字段；列表编译成 SQL 与命中位图；请求级的 `Subject` | 第 8 章 P8.7–P8.18 |
| `guard` | 用户面的准入：核验凭据、操作必须已声明、视图有效，请求带上主体与截止时间 | 第 8 章 |
| `canon` | 安全记录的规范编码与摘要（RFC 8785、十进制、时刻、NFC） | 第 8 章 P8.8 |
| `audit` | 本地审计表：业务与安全变更同事务写入、敏感读取先记后放、拒绝的明细预算与汇总、锁内编号、经系统面转运与回执、按已备份水位删除、积压容量保护 | 第 8 章 P8.19–P8.23 |
| `list` | 列表：游标分页（排序键加身份、不透明游标、筛选摘要与计划摘要）、页大小、声明的筛选与排序字段、受限字段的可查询性、关键字搜索；与授权谓词拼成一条 SQL | 第 9 章 P9.1–P9.8 |
| `search` | 规范化搜索列（版本 1）：折叠、全拼与首字母（`be-protocol` 的拼音表），查询折叠与子串匹配 | 第 9 章 P9.6–P9.8 |
| `projection` | 投影：按协议 02 的状态表接收完整、部分与删除；全量重新取回（对齐代次、快照直接覆盖、对齐期间写入的行按版本比较、快照之外的行按身份读取）；补全不完整的行 | 第 9 章 P9.10–P9.12 |
| `evidence` | 证据的记录者一侧（收紧两段写、一般记录、变更历史）与属主的重放 | 第 12 章 |
| `runprofile` | 运行档案与告警渠道、恢复代次的读取与推进 | 第 12 章 |
| `extcmd` | 对外命令：命令与尝试、状态表、发送与查询的作业、只查询模式、整套恢复的外部对账 | 第 13 章 |
| `recovery` | 整套恢复的核对：SDK 自带的检查与组件的检查、对象是否还在、连续单号的已用最大值与提升 | 第 14 章 |
| `seed` | 组件种子命令用的工具（只在开发与测试环境）：调用组件用户面的客户端、经身份成员的开发换取登录测试账号、等授予到达组件；在授权成员的生效点建角色、授予、撤销与管理单元，变更键带组件与被链接的主体，重跑不重复 | 本项目的种子与测试数据约定 |

## 功能矩阵

`features.json` 按 `be-protocol` 的能力清单（`capabilities.json`）标出每项能力在本 SDK 中的状态、证明它的测试与尚待验证的规则；状态的含义见 `be-protocol` 阅读说明的“能力清单与功能矩阵”。`TestFeatureMatrix` 核对它与所引用协议版本的能力清单一一对应、证据都是本仓库真实存在的测试。只留形状的能力（`service_proxy`、严格路径、委托与扮演、水位、影子投影、成员切换、高可用、事件签名）标为 `not_implemented`，组件声明它们时装配准入拒绝。

## 测试

`make test`：L1 测试，含协议向量（按 `go.mod` 中 `be-protocol` 的精确版本）、功能矩阵与 SDK 自身的合并安全检查。需要真实 PostgreSQL（16 及以上）与消息系统的测试在构建标签 `integration` 下，`make test-integration` 运行：环境变量 `BE_SDK_TEST_PG_ADMIN` 给出超级用户的连接串（装配项目的 `be-postgres`），`BE_SDK_TEST_NATS_URL` 给出开启 JetStream 的 NATS（`be-nats`），`BE_SDK_TEST_S3_URL`、`BE_SDK_TEST_S3_ACCESS_KEY`、`BE_SDK_TEST_S3_SECRET_KEY` 给出对象存储（`be-rustfs`，测试建桶 `be-sdk-test`），测试在库 `be_sdk_test` 中自建角色与 schema；各包共用这个库，所以按包串行（`-p 1`）。缺环境时失败，不跳过。
