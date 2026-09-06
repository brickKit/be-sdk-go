# be-sdk-go

Go 横切基础库（总纲 §4 SOP-L 十四项能力）。**不是 brickKit 组件**，也**不是公共 model 包**——零业务逻辑、零组件 model、零组件间引用。它是 `be-acceptance` 铁律六 import 扫描的唯一白名单。

## 它替所有 Go 组件挡住的坑

| 能力 | 文件 | 挡住的坑 |
|---|---|---|
| 组件地址剥 scheme | `endpoint.go` | `grpc.Dial("http://host:9094")` 连不上，报错指向名称解析（十八条第 1 条） |
| `SET LOCAL` 事务 | `tx.go` | 不带 `LOCAL` 的 `SET` 之后连接还回池，下一个借用者原样继承，悄悄读写别人的数据（十八条第 2 条） |
| 单跑/合并统一入口 | `standalone.go`、`module.go`、`runtime.go`、`gin.go` | 每个组件各发明一个 `main`，合并那天全部重写（十八条第 18 条） |

## 现状（阶段一 Task 6）

`Runtime` / `Module` / `RunStandalone` / `NewGinEngine` / `Bootstrap` / `Endpoint` / `WithTx` 已经是**真实实现**——这是"结构三件套"，决定外壳能不能把模块挂进来，必须先钉死。

`otel.go` / `logging.go` / `metrics.go` / `query.go` / `archive.go` / `outbox.go` / `events.go` 现在只有签名 + 文档注释 + `panic("未实现：Task 7 补")`。**调用它们会 panic，这是预期行为**，不是 bug——Task 7 会用 TDD 逐个补上。

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

`gin` / `pgx`（不用 `lib/pq`）/ `nats.go` / `go.opentelemetry.io/otel` / `google.golang.org/grpc` / `prometheus/client_golang`。版本精确锁定（`go.mod` 里没有 `latest`），`go 1.25`——理由见 `docs/dev/实测踩坑记录.md` 类别 D0。
