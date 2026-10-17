# be-sdk-go

组件协议（[be-protocol](https://github.com/brickKit/be-protocol)）的官方 Go 实现：组件只写业务，生命周期、配置、错误、观测、用户面的中间件链、外壳里的合并安全由 SDK 负责。

旧版本（0.x）在分支 `legacy/v0`（最新的旧代码来自 `stage-b`），标签保留；本主分支按终局设计从零开始（D59），版本从 `3.0.0` 起（A110），模块路径 `github.com/brickKit/be-sdk-go/v3`。设计依据在装配项目 [be-assembly-standard](https://github.com/brickKit/be-assembly-standard) 的协议 17、18 与组件实现约定；规则正文在 `be-protocol`。

## 组件怎样用

```go
// 组件根包：一个模块入口，单独运行与进外壳调用的是同一个
var Definition = component.Definition{
	ID: "erp/sales", Version: "3.0.0",
	Keys: []config.Key{{Name: "SALES_QUOTE_DAYS", Type: config.Integer, Default: 30}},
	New: func(rt *runtime.Runtime) (component.Module, error) { return newModule(rt) },
}

// cmd/server/main.go：单独运行
func main() { launcher.Main(sales.Definition) }
```

模块从 `rt` 取一切：配置（`rt.Config`）、依赖地址（`rt.Dependency`，可选依赖缺席时返回“不存在”）、日志、指标注册器、追踪器。模块在 `Routes` 里登记路由，每条路由声明它的操作（`httpx.Route.Operation`），公开接口显式标 `Public`。

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
| `mergesafety` | 合并安全检查：读进程环境、初始化进程级的东西、退出进程、绕过 SDK 建引擎与连接 | 组件实现约定的合并安全 |

数据库、组件间调用、事件、后台工作、身份与授权、外壳启动器随实验计划第 2 步的后续任务加入（装配项目 `dev/rebuild/step2-plan.md`）。

## 测试

`make test`：L1 测试，含协议向量（按 `go.mod` 中 `be-protocol` 的精确版本）与 SDK 自身的合并安全检查。需要真实 PostgreSQL 与消息系统的测试在构建标签 `integration` 下，`make test-integration` 运行；缺环境时失败，不跳过。
