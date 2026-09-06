package besdk

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v5/stdlib" // 注册 "pgx" 驱动，§12.4：不用 lib/pq
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

// Bootstrap 做进程级、只能有一份的那些初始化（OTel provider、日志根、
// Gin 的包级全局模式）。调用方（RunStandalone 或外壳）调它恰好一次；
// 模块一律不许碰（§12.5.2）。
func Bootstrap(ctx context.Context, serviceName, otelBaseURL string) (shutdown func(context.Context) error, err error) {
	// ⚠️ gin.SetMode 是包级全局变量，不是某个 *gin.Engine 的字段——
	// 一个模块调 gin.SetMode(gin.DebugMode)，另外 21 个模块的 engine 会
	// 一起进 debug 模式，panic 堆栈直接吐给客户端（十八条第 17 条）。
	// 只在这里设一次，NewGinEngine 不碰它。
	gin.SetMode(gin.ReleaseMode)

	otelShutdown, err := InitOTel(ctx, serviceName, otelBaseURL)
	if err != nil {
		return nil, fmt.Errorf("InitOTel: %w", err)
	}
	return otelShutdown, nil
}

// RunStandalone 是单跑形态的全部装配，也是全项目唯一允许读进程环境变量的
// 地方（§12.5.3）。它做：Bootstrap → 填 Runtime → 调 newModule → 跑迁移
// → Listen HTTP 与全部 extraPorts → 装信号处理器 → 优雅关停。
//
// 于是每个组件的 main.go 只有一行：
//
//	func main() { besdk.RunStandalone(module.New) }
func RunStandalone(newModule func(context.Context, *Runtime) (*Module, error)) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	componentID := mustGetenv("COMPONENT_ID")
	componentVersion := mustGetenv("COMPONENT_VERSION")

	// ⚠️ 端口不是平台注入的（§13.8.1）：环境变量表里只有"别人在哪"
	// （*_ENDPOINT），没有"我该监听哪"。全拆态下唯一权威来源是组件自己的
	// component.yaml——镜像里必须把它跟二进制放在一起（Dockerfile 的
	// WORKDIR，与 migrations/ 同级）。v0.1.0 曾经等一个从来不存在的
	// HTTP_PORT 环境变量，mdm-customer 第一次真的 up 起来才核对出这个坑。
	ports, err := loadOwnPorts("component.yaml")
	if err != nil {
		exitf(componentID, "读自己的 component.yaml 失败：%v", err)
	}

	shutdownOTel, err := Bootstrap(ctx, componentID, os.Getenv("OTEL_BASE_URL"))
	if err != nil {
		exitf(componentID, "Bootstrap 失败：%v", err)
	}
	defer func() { _ = shutdownOTel(context.Background()) }()

	// ⚠️ 平台注入的是分开的 DATABASE_HOST/PORT/USER/PASSWORD/NAME
	// （006 §4.4 的 5 层表），没有单个 PG_DSN——同样是 v0.1.0 等一个从来
	// 不存在的变量。DSN 由 buildPGDSN 从这几片拼。
	pgDSN, err := buildPGDSN()
	if err != nil {
		exitf(componentID, "拼数据库连接串失败：%v", err)
	}
	db, err := sql.Open("pgx", pgDSN)
	if err != nil {
		exitf(componentID, "打开数据库连接池失败：%v", err)
	}
	defer func() { _ = db.Close() }()

	// 同理，NATS 的连接信息是 MQ_HOST/MQ_PORT（+ 可选 MQ_USER/MQ_PASSWORD），
	// 不是单个 NATS_URL。
	nc, err := nats.Connect(buildNATSURL())
	if err != nil {
		exitf(componentID, "连接 NATS 失败：%v", err)
	}
	defer nc.Close()

	rt := &Runtime{
		ComponentID:      componentID,
		ComponentVersion: componentVersion,
		Config:           NewConfig(envSnapshot()),
		DB:               db,
		NATS:             nc,
		Logger:           NewLogger(componentID),
		// ⚠️ 实测踩坑：这两行漏了的话，NewGinEngine 的 tracingMiddleware
		// 一收到请求就 panic（rt.Tracer 是 nil interface）——包括 /healthz
		// 本身，容器因此永远不健康。Bootstrap 已经在 InitOTel 里调用过
		// otel.SetTracerProvider(tp)，这里用 otel.Tracer(componentID) 从
		// 刚设好的 provider 上取一个真 tracer，不能让 Runtime 带着零值
		// 传下去。be-sdk-go 自己的 gin_test.go 从没抓到这个问题，因为它的
		// newTestRuntime 测试 helper 手工塞了一个真 tracer，从没测过
		// "RunStandalone 自己组出来的 Runtime" 这条路径。
		Tracer:     otel.Tracer(componentID),
		Meter:      otel.Meter(componentID),
		Registry:   NewRegistry(),
		HTTPPort:   ports.HTTPPort,
		ExtraPorts: ports.ExtraPorts,
	}

	mod, err := newModule(ctx, rt)
	if err != nil {
		exitf(componentID, "组件初始化失败：%v", err)
	}

	// ⚠️ 全拆态迁移不在这里跑：平台为每个组件单独生成一次性迁移容器
	// （入口是各组件自己的 backend/cmd/migrate，见 mdm-customer Task 14），
	// RunStandalone 服务的是应用进程本身，不重复跑一遍迁移。mod.Migrations
	// 这个 fs.FS 只被合并态的外壳启动器消费（§13.3 铁律五，阶段四）。

	errCh := make(chan error, 1+len(rt.ExtraPorts))
	go func() { errCh <- serveHTTP(ctx, rt.HTTPPort, mod.HTTPHandler) }()
	for name, port := range rt.ExtraPorts {
		name, port := name, port
		go func() { errCh <- serveExtraPort(ctx, name, port, mod.RegisterGRPC) }()
	}
	if mod.Start != nil {
		go func() {
			if err := mod.Start(ctx); err != nil {
				errCh <- fmt.Errorf("Start: %w", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			rt.Logger.Error("服务异常退出", "error", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if mod.Stop != nil {
		_ = mod.Stop(shutdownCtx)
	}
}

func serveHTTP(ctx context.Context, port int, handler http.Handler) error {
	srv := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: handler}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP 服务退出：%w", err)
	}
	return nil
}

func serveExtraPort(ctx context.Context, name string, port int, register func(*grpc.Server)) error {
	if register == nil {
		return nil
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("额外端口 %s（:%d）监听失败：%w", name, port, err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("额外端口 %s 服务退出：%w", name, err)
	}
	return nil
}

// mustGetenv 是 RunStandalone 内部用的——它是全项目唯一允许读进程环境变量
// 的地方（§12.5.3），模块代码里出现 os.Getenv 就是违规。
//
// ⚠️ 读 COMPONENT_ID 本身失败时还不知道是哪个组件——这是启动阶段唯一
// 一处 exitf 的 componentID 参数必然是空的情况，属于物理限制，不是漏传。
func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		exitf(os.Getenv("COMPONENT_ID"), "必需的环境变量 %s 未设置", key)
	}
	return v
}

// envSnapshot 把当前进程环境变量拍成一份快照灌进 Config。
//
// ⚠️ 合并态下这个函数不会被调用——外壳启动器会给每个模块构造自己那一份
// env map（§13.8.2），不是从共享的 os.Environ() 里读，否则就是「22 个模块
// 的 PG_SCHEMA 互相顶掉」那条雷（§12.5.3）。
func envSnapshot() map[string]string {
	out := make(map[string]string, len(os.Environ()))
	for _, kv := range os.Environ() {
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				out[kv[:i]] = kv[i+1:]
				break
			}
		}
	}
	return out
}

// buildPGDSN 从平台注入的 DATABASE_* 前缀变量拼出一个 pgx 认得的 DSN。
//
// ⚠️ 没有单个 PG_DSN 这种东西——`006` §4.4 的资源注入是分开的五个变量
// （HOST/PORT/USER/PASSWORD/NAME），brickkit up --dry-run 生成的 compose
// 环境变量表可以直接核对。sslmode=disable 是本地/内网部署的默认值，
// TLS 需求留给未来客户按需求提，不在这一批范围内。
func buildPGDSN() (string, error) {
	host, ok := os.LookupEnv("DATABASE_HOST")
	if !ok {
		return "", fmt.Errorf("DATABASE_HOST 未设置")
	}
	port, ok := os.LookupEnv("DATABASE_PORT")
	if !ok {
		return "", fmt.Errorf("DATABASE_PORT 未设置")
	}
	user, ok := os.LookupEnv("DATABASE_USER")
	if !ok {
		return "", fmt.Errorf("DATABASE_USER 未设置")
	}
	password, ok := os.LookupEnv("DATABASE_PASSWORD")
	if !ok {
		return "", fmt.Errorf("DATABASE_PASSWORD 未设置")
	}
	name, ok := os.LookupEnv("DATABASE_NAME")
	if !ok {
		return "", fmt.Errorf("DATABASE_NAME 未设置")
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, password, host, port, name), nil
}

// buildNATSURL 从 MQ_* 前缀变量拼出 nats.Connect 认得的 URL。
//
// ⚠️ 同样没有单个 NATS_URL。MQ_USER/MQ_PASSWORD 是否存在取决于这个部署的
// nats 资源有没有配认证——本项目的 nats-shared 没配，所以要支持两种形态，
// 不能假设一定有认证信息。
func buildNATSURL() string {
	host := os.Getenv("MQ_HOST")
	port := os.Getenv("MQ_PORT")
	user, hasUser := os.LookupEnv("MQ_USER")
	password := os.Getenv("MQ_PASSWORD")
	if hasUser && user != "" {
		return fmt.Sprintf("nats://%s:%s@%s:%s", user, password, host, port)
	}
	return fmt.Sprintf("nats://%s:%s", host, port)
}

// exitf 是 RunStandalone 内部专用的错误退出路径——它本身不算「模块
// log.Fatal」（十八条第 18 条禁的是模块代码，不是启动器自己）：启动阶段
// 踩到不可恢复的配置错误，本来就应该让这一个组件的进程退出，不作为
// error 向上层传播，因为这里已经是调用链的最外层。
//
// ⚠️ componentID 打进日志——"进程以非零码退出且日志说清是哪个模块"
// 是明确的断言（合并态排障时，一堆组件的日志混在一起，不带 componentID
// 根本不知道是谁挂了）。componentID 为空时退化成不带前缀，仅见于读
// COMPONENT_ID 本身失败那一种情况。
func exitf(componentID, format string, args ...any) {
	prefix := ""
	if componentID != "" {
		prefix = "[" + componentID + "] "
	}
	fmt.Fprintf(os.Stderr, prefix+format+"\n", args...)
	os.Exit(1)
}
