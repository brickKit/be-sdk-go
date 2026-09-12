package besdk

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

// ShellModuleConfig 是外壳装配单个模块 *Runtime 所需的全部输入——
// RunStandalone 从进程环境变量 + 自己的 component.yaml 推导出这些值，
// 外壳不能这样做：一个进程只有一份 environ，11 个模块各自的
// DATABASE_*/COMPONENT_ID/config 项互相顶掉，不报错，模块按别人的
// schema 建表写数据（§12.5.3、决策 110）。每个字段都必须来自 `be-ops`
// 产出 4/7（合并清单 + 每外壳环境变量表，设计书 §13.8.2），不能从
// 外壳进程自己的 os.Environ() 读。
type ShellModuleConfig struct {
	ComponentID      string
	ComponentVersion string
	Env              map[string]string
	HTTPPort         int
	ExtraPorts       map[string]int
}

// NewShellRuntime 为外壳里的一个模块构造 *Runtime，字段逐一对应
// RunStandalone 自己组装 Runtime 的那一段（standalone.go），区别只在
// DB/NATS 由外壳传入共享实例，不是各自新开一个连接——§13.3 铁律二：
// 一个外壳一个连接池，模块通过 besdk.WithTx 的 SET LOCAL ROLE 切身份，
// 从不持有自己的池。
//
// Tracer/Meter 仍然按模块各自的 componentID 取（otel.Tracer(id)/
// otel.Meter(id)）——这两个调用只是从共享的 TracerProvider/MeterProvider
// 上取一个按名字区分的 instrumentation scope，不是重新初始化一份，所以
// 多个模块共享同一条 OTel 导出链路的同时，每个模块的 span/metric 依然
// 能按 componentID 区分（不会退化成"22 个模块的 trace 全挤在一个
// service.name 下分不清谁是谁"那类坏结果）。真正"只能有一份"的
// TracerProvider 由 Bootstrap 在外壳启动最开始装配一次，这里不重复调。
func NewShellRuntime(cfg ShellModuleConfig, db *sql.DB, nc *nats.Conn) *Runtime {
	return &Runtime{
		ComponentID:      cfg.ComponentID,
		ComponentVersion: cfg.ComponentVersion,
		Config:           NewConfig(cfg.Env),
		DB:               db,
		NATS:             nc,
		Logger:           NewLogger(cfg.ComponentID),
		Tracer:           otel.Tracer(cfg.ComponentID),
		Meter:            otel.Meter(cfg.ComponentID),
		Registry:         NewRegistry(),
		HTTPPort:         cfg.HTTPPort,
		ExtraPorts:       cfg.ExtraPorts,
	}
}

// InitShellAuthz 给整个外壳进程装配**恰好一份** JWT 验签器 + bundle
// 轮询——不是每个模块各调一次。
//
// ⚠️ authzRuntime（authz.go）是包级全局状态，RunStandalone 假设"一个
// 进程一个模块"所以调一次没有问题；外壳如果照搬"每个模块各自调
// setupAuthzRuntime"，11 次调用里只有最后一次真正生效（后面 10 个模块
// 的 RequirePermission 判定全部读的是最后一个模块的配置），且前 10 次
// 启动的 bundle 轮询 goroutine 从此没人再持有引用、没人能停下来
// ——这正是导读第 17 条"最后一个 init 的赢"那类问题在权限判定状态上的
// 翻版。本项目目前全部业务组件的 iamJwksUrl/authzBundleUrl 都指向同一个
// infra-iam-casdoor/infra-authz 实例（见 docs/design/_调研记录/
// 04-阶段四.md），所以外壳只需要用任意一个模块的 Config 调一次本函数
// 即可对齐全部模块的判定行为——调用时机：Bootstrap 之后、任何模块的
// HTTP/gRPC server 开始接请求之前。
func InitShellAuthz(ctx context.Context, cfg Config, logger *slog.Logger) {
	verifier, bundle := setupAuthzRuntime(ctx, cfg, logger)
	setAuthzRuntime(verifier, bundle)
}

// ServeHTTP 是 serveHTTP 的导出别名，供外壳按模块循环调用——不是重新
// 实现一遍。A4g 的教训是"测试用的构造路径和生产用的构造路径不是同一条
// 路径"，外壳如果自己重新写一遍 Listen/优雅关闭逻辑，等于在 11 倍的
// 爆炸半径上重演同一类风险；这里保证外壳、RunStandalone 走的是完全
// 同一份实现。
func ServeHTTP(ctx context.Context, port int, handler http.Handler) error {
	return serveHTTP(ctx, port, handler)
}

// ServeExtraPort 是 serveExtraPort 的导出别名，理由同 ServeHTTP——
// 包括它内部已经挂好的 grpcRecoveryInterceptor（C11 的教训：裸
// grpc.NewServer() 对 panic 没有任何防护），外壳复用这个导出别名就自动
// 带上这层防护，不需要也不应该自己重新装一遍拦截器。
func ServeExtraPort(ctx context.Context, name string, port int, register func(*grpc.Server), logger *slog.Logger) error {
	return serveExtraPort(ctx, name, port, register, logger)
}

// BuildPGDSN 与 BuildNATSURL 是 buildPGDSN/buildNATSURL 的导出别名。
//
// 与 ShellModuleConfig 的字段不同，这两个不是"每模块各一份"——一个外壳
// 只有一个共享登录角色、一条共享连接串（设计书 §13.3："brickkit.yaml
// 里只有 5 个外壳登录角色"），这份 DATABASE_*/MQ_* 是外壳进程级的，
// 从外壳自己的 os.Environ() 读一次就够，不属于"一个进程一份 environ
// 会互相顶掉"那类风险（那条风险专指每模块各自的 config，不是整个外壳
// 共享的连接信息）。导出这两个纯粹是为了不让外壳重新拼一遍同样的
// DSN 格式化逻辑。
func BuildPGDSN() (string, error) {
	return buildPGDSN()
}

func BuildNATSURL() string {
	return buildNATSURL()
}
