package shell

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	besdk "github.com/brickKit/be-sdk-go"
	_ "github.com/jackc/pgx/v5/stdlib" // 注册 "pgx" 驱动，§12.4：不用 lib/pq
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

// Constructor 是组件 backend/module.New 的签名。
type Constructor = func(context.Context, *besdk.Runtime) (*besdk.Module, error)

// Registry 把成员 ID 映射到编译进本外壳的构造函数。它必须与外壳 component.yaml 的
// shell.members 一一对应：镜像里编进了谁，由 main.go 的静态 import 决定。
// 构造函数只能是 Go 源码里的静态 import，不能从数据动态加载——成员清单 JSON
// 只负责"这次部署装谁、端口和配置是什么"，"谁的代码在这个二进制里"是编译期的事。
type Registry map[string]Constructor

// Config 是外壳进程级的输入。ShellConfig 是外壳**自己的**配置（它自己 configSchema 的
// PG_*/NATS_URL/OTEL_BASE_URL/AUTHZ_BUNDLE_URL/IAM_JWKS_URL），不是任何成员的。
type Config struct {
	ShellName   string
	ShellConfig besdk.Config
	HTTPPort    int // 外壳自己 component.yaml 的 deployment.port，只答 /healthz
}

// Main 是外壳 main.go 的唯一一行。它读外壳自己的环境（外壳进程级，允许），
// 读自己的 component.yaml 拿端口，解析成员清单，然后调 Run。任何错误都打印后以 1 退出——
// 这是外壳进程本身的入口，不是模块，所以这里可以退出进程。
func Main(shellName string, registry Registry) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	logger := besdk.NewLogger(shellName)
	raw, present := os.LookupEnv("BRICKKIT_SERVED_MEMBERS_CONFIG")
	members, err := ParseServedMembers(raw, present)
	if err != nil {
		logger.Error("外壳启动失败", "err", err)
		os.Exit(1)
	}
	port, err := besdk.LoadOwnHTTPPort("component.yaml")
	if err != nil {
		logger.Error("读外壳自己的 component.yaml 失败", "err", err)
		os.Exit(1)
	}
	cfg := Config{ShellName: shellName, ShellConfig: besdk.NewConfig(besdk.EnvSnapshot()), HTTPPort: port}
	if err := Run(ctx, cfg, members, registry, logger); err != nil {
		logger.Error("外壳异常退出", "err", err)
		os.Exit(1)
	}
}

type builtModule struct {
	id  string
	rt  *besdk.Runtime
	mod *besdk.Module
}

// Run：Bootstrap 一次 → 用外壳自己的 PG_*/NATS_URL 开一个共享池和一条 NATS 连接 →
// InitShellAuthz 一次 → 逐个成员用它自己的 Config 构造 Runtime 并调用构造函数 →
// errgroup 一起 Listen（外壳 /healthz + 每个成员的端口）→ ctx 取消时全部优雅退出。
// 成员迁移不在这里跑：v1 由 brickKit 在外壳启动前用每个成员自己的镜像和配置跑完。
//
// 启动阶段（任何一步、包括任一成员的构造函数）失败即中止并返回错误，返回前关掉共享池与
// NATS 连接。启动之后，单个成员的失败（Start panic、Start 返回错误、它自己的端口退出）
// 只记日志、不牵连其余成员：Run 只在 ctx 取消（返回 nil）或外壳自己的 /healthz 服务
// 失败（返回该错误）时结束。
func Run(ctx context.Context, cfg Config, members []ServedMember, registry Registry, logger *slog.Logger) error {
	if cfg.HTTPPort <= 0 {
		return fmt.Errorf("外壳 %s 的 HTTP 端口未设置（component.yaml 的 deployment.port）", cfg.ShellName)
	}

	// runCtx 在 Run 返回时一定被取消：InitShellAuthz 起的 bundle 轮询等后台协程挂在它上面，
	// 不能比 Run 活得更久。
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	shutdownOTel, err := besdk.Bootstrap(runCtx, cfg.ShellName, cfg.ShellConfig.StringOr("OTEL_BASE_URL", ""))
	if err != nil {
		return fmt.Errorf("Bootstrap: %w", err)
	}
	defer func() { _ = shutdownOTel(context.Background()) }()

	// 一个外壳一个连接池（§13.3 铁律二）：用外壳自己的登录角色，模块经 besdk.WithTx 的
	// SET LOCAL ROLE 切身份，从不持有自己的池。
	dsn, err := besdk.PGDSN(cfg.ShellConfig)
	if err != nil {
		return fmt.Errorf("外壳共享连接池：%w", err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("打开外壳共享连接池失败：%w", err)
	}
	defer func() { _ = db.Close() }()

	nurl, err := besdk.NATSURL(cfg.ShellConfig)
	if err != nil {
		return fmt.Errorf("外壳共享 NATS 连接：%w", err)
	}
	nc, err := nats.Connect(nurl)
	if err != nil {
		return fmt.Errorf("连接 NATS 失败：%w", err)
	}
	defer nc.Close()

	// 权限判定是包级全局状态，整个外壳恰好装配一次，且在任何成员开始接请求之前。
	besdk.InitShellAuthz(runCtx, cfg.ShellConfig, logger)

	built, err := buildModules(runCtx, members, registry, db, nc)
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(runCtx)

	// 外壳自己的 /healthz 是整个容器唯一的 probe（§13.6）。判据同每个模块自己的
	// /healthz：只答"外壳进程活着"，不查任何成员、不查任何依赖（导读第 7 条）。
	// 它是 errgroup 里唯一会把错误交回去的服务：它挂了，平台就再也探不到这个容器，
	// 整个外壳退出交给重启策略。
	g.Go(func() error {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
		err := besdk.ServeHTTP(gctx, cfg.HTTPPort, mux)
		if err != nil {
			logger.Error("外壳自己的健康检查端口退出", "error", err)
		}
		return err
	})

	// ⚠️ 成员的 goroutine 一律只记日志、返回 nil，绝不把错误流回 errgroup：
	// errgroup.WithContext 收到第一个错误就取消 gctx，而 gctx 是全部成员的 HTTP/额外端口/
	// Start 共用的同一个 ctx——一个成员出事等于把其余 N-1 个健康成员一起带下线，合并部署
	// 就白白放弃了独立部署本来就有的故障隔离（阶段四 Task 11 真机复现过的缺口）。
	// 代价：出事的那个成员从此停在故障状态，直到整个外壳下一次重启——这是有意接受的降级，
	// 日志带 module_component_id 指出是谁。
	for _, b := range built {
		b := b
		g.Go(func() error {
			if err := besdk.ServeHTTP(gctx, b.rt.HTTPPort, b.mod.HTTPHandler); err != nil {
				logger.Error("模块 HTTP 服务退出（已隔离，不影响外壳内其余模块）", "module_component_id", b.id, "error", err)
			}
			return nil
		})
		for name, port := range b.rt.ExtraPorts {
			name, port := name, port
			g.Go(func() error {
				if err := besdk.ServeExtraPort(gctx, name, port, b.mod.RegisterGRPC, b.rt.Logger); err != nil {
					logger.Error("模块额外端口服务退出（已隔离，不影响外壳内其余模块）", "module_component_id", b.id, "port_name", name, "error", err)
				}
				return nil
			})
		}
		if b.mod.Start != nil {
			g.Go(func() error {
				// Go 的 panic 不会被 errgroup 或别的 goroutine 拦住，不在这里 recover
				// 就会直接终止整个外壳进程。RunStandalone 不需要这层：单模块进程里
				// panic 让进程退出就是正确行为（爆炸半径 = 1 个组件）。
				defer func() {
					if r := recover(); r != nil {
						logger.Error("模块后台循环 panic（已隔离，不影响外壳内其余模块）", "module_component_id", b.id, "recovered", r)
					}
				}()
				if err := b.mod.Start(gctx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("模块后台循环退出（已隔离，不影响外壳内其余模块）", "module_component_id", b.id, "error", err)
				}
				return nil
			})
		}
	}

	runErr := g.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, b := range built {
		if b.mod.Stop != nil {
			if err := b.mod.Stop(shutdownCtx); err != nil {
				logger.Error("模块 Stop 失败", "module_component_id", b.id, "error", err)
			}
		}
	}

	return runErr
}

// buildModules 为每个成员构造 Runtime（Env 用成员 JSON 里的 Config，绝不用外壳进程环境）并调用构造函数。
// 一个外壳进程只有一份 environ，成员的 PG_SCHEMA/*_ENDPOINT/业务配置要是从那里读，
// N 个成员会互相顶掉、不报错（导读第 16 条）。
func buildModules(ctx context.Context, members []ServedMember, registry Registry, db *sql.DB, nc *nats.Conn) ([]builtModule, error) {
	out := make([]builtModule, 0, len(members))
	for _, m := range members {
		ctor, ok := registry[m.ComponentID]
		if !ok {
			return nil, fmt.Errorf("成员 %s 在 BRICKKIT_SERVED_MEMBERS_CONFIG 里，但本外壳没有编译它（Registry 未登记）——检查外壳 component.yaml 的 shell.members 与 main.go 的 import", m.ComponentID)
		}
		extra := make(map[string]int, len(m.ExtraPorts))
		for _, p := range m.ExtraPorts {
			extra[p.Name] = p.Port
		}
		rt := besdk.NewShellRuntime(besdk.ShellModuleConfig{
			ComponentID: m.ComponentID, ComponentVersion: m.Version,
			Env: m.Config, HTTPPort: m.HTTPPort, ExtraPorts: extra,
		}, db, nc)
		mod, err := ctor(ctx, rt)
		if err != nil {
			return nil, fmt.Errorf("成员 %s 初始化失败：%w", m.ComponentID, err)
		}
		// nil Handler 交给 http.Server 会落到 DefaultServeMux，等于把进程全局的 mux 暴露出去。
		if mod == nil || mod.HTTPHandler == nil {
			return nil, fmt.Errorf("成员 %s 的构造函数没有返回 HTTPHandler", m.ComponentID)
		}
		out = append(out, builtModule{id: m.ComponentID, rt: rt, mod: mod})
	}
	return out, nil
}
