# be-sdk-go

Go 横切基础库（总纲 §4 SOP-L 十四项能力）。**不是 brickKit 组件**，也**不是公共 model 包**——零业务逻辑、零组件 model、零组件间引用。它是 `be-acceptance` 铁律六 import 扫描的唯一白名单。

## 它替所有 Go 组件挡住的坑

| 能力 | 文件 | 挡住的坑 |
|---|---|---|
| 组件地址剥 scheme | `endpoint.go` | `grpc.Dial("http://host:9094")` 连不上，报错指向名称解析（十八条第 1 条） |
| `SET LOCAL` 事务 | `tx.go` | 不带 `LOCAL` 的 `SET` 之后连接还回池，下一个借用者原样继承，悄悄读写别人的数据（十八条第 2 条） |
| 单跑/合并统一入口 | `standalone.go`、`module.go`、`runtime.go`、`gin.go` | 每个组件各发明一个 `main`，合并那天全部重写（十八条第 18 条） |

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

`gin` / `pgx`（不用 `lib/pq`）/ `nats.go` / `go.opentelemetry.io/otel` / `google.golang.org/grpc` / `prometheus/client_golang`。版本精确锁定（`go.mod` 里没有 `latest`），`go 1.25`——理由见 `docs/dev/实测踩坑记录.md` 类别 D0。
