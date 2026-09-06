package besdk

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v5/stdlib" // 注册 "pgx" 驱动，§12.4：不用 lib/pq
	"github.com/nats-io/nats.go"
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
//
func RunStandalone(newModule func(context.Context, *Runtime) (*Module, error)) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	componentID := mustGetenv("COMPONENT_ID")
	componentVersion := mustGetenv("COMPONENT_VERSION")
	httpPort, err := strconv.Atoi(mustGetenv("HTTP_PORT"))
	if err != nil {
		exitf(componentID, "HTTP_PORT 不是合法端口号：%v", err)
	}

	shutdownOTel, err := Bootstrap(ctx, componentID, os.Getenv("OTEL_BASE_URL"))
	if err != nil {
		exitf(componentID, "Bootstrap 失败：%v", err)
	}
	defer func() { _ = shutdownOTel(context.Background()) }()

	db, err := sql.Open("pgx", mustGetenv("PG_DSN"))
	if err != nil {
		exitf(componentID, "打开数据库连接池失败：%v", err)
	}
	defer func() { _ = db.Close() }()

	nc, err := nats.Connect(mustGetenv("NATS_URL"))
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
		Registry:         NewRegistry(),
		HTTPPort:         httpPort,
		ExtraPorts:       extraPortsFromEnv(),
	}

	mod, err := newModule(ctx, rt)
	if err != nil {
		exitf(componentID, "组件初始化失败：%v", err)
	}

	// 迁移由外壳/RunStandalone 按拓扑顺序跑，组件自己不碰（§13.3 铁律五）。
	// ⚠️ 还没实现——不在本任务的断言范围内（Task 7 只锁 Listen/优雅关停/
	// newModule 失败退出这三条）。真正实现时用 golang-migrate 读
	// mod.Migrations，目标 schema 与迁移状态表都要显式指定（§11.2.3：
	// 默认 public 会和其它组件的迁移表打架）。

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

// extraPortsFromEnv 留空实现——额外端口的注入格式（EXTRA_PORTS_GRPC 一类
// 变量名，还是单个 JSON）现在还没定，不在本任务的断言范围内（Task 7 只
// 锁 Listen/优雅关停/newModule 失败退出这三条）。等第一个真实组件
// （mdm-customer）声明 extraPorts 之后才能核对格式对不对。
func extraPortsFromEnv() map[string]int {
	return map[string]int{}
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
