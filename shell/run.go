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
	"runtime/debug"
	"strings"
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
// NATS 连接。启动之后分两类：
//   - 任何端口的监听/服务失败（外壳 /healthz，或任一成员的 HTTP/额外端口，包括绑定失败和
//     服务协程意外返回）→ Run 返回错误，外壳以非零码退出。成员端口死了而外壳 /healthz
//     还是绿的，就是一次平台探测不到的静默故障，必须闹大；
//   - 成员 Start() 的失败（panic 或返回错误）→ 隔离：记一条带 module_component_id 的
//     ERROR 日志，其余成员照常服务，外壳继续运行。
//
// ctx 取消时全部优雅退出，返回 nil。
func Run(ctx context.Context, cfg Config, members []ServedMember, registry Registry, logger *slog.Logger) error {
	if cfg.HTTPPort <= 0 {
		return fmt.Errorf("外壳 %s 的 HTTP 端口未设置（component.yaml 的 deployment.port）", cfg.ShellName)
	}
	if err := validateMemberPorts(members); err != nil {
		return err
	}
	if err := requireShellAuthzURLs(cfg); err != nil {
		return err
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
	// 它挂了，平台就再也探不到这个容器，整个外壳退出交给重启策略。
	g.Go(func() error {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
		return serveResult(gctx, logger, "", "/healthz", besdk.ServeHTTP(gctx, cfg.HTTPPort, mux))
	})

	// 成员的端口与外壳 /healthz 同一判据：任一失败都把错误交回 errgroup，取消 gctx，
	// 全部成员优雅退出，Run 返回错误——外壳 /healthz 只代表"进程活着"，成员端口死了它
	// 照样答 200，不闹大就没有任何东西会发现。
	for _, b := range built {
		b := b
		g.Go(func() error {
			return serveResult(gctx, logger, b.id, "HTTP", besdk.ServeHTTP(gctx, b.rt.HTTPPort, b.mod.HTTPHandler))
		})
		// buildModules 已保证：声明了额外端口的成员一定有 RegisterGRPC。
		for name, port := range b.rt.ExtraPorts {
			name, port := name, port
			g.Go(func() error {
				return serveResult(gctx, logger, b.id, "额外端口 "+name,
					besdk.ServeExtraPort(gctx, name, port, b.mod.RegisterGRPC, b.rt.Logger))
			})
		}
		if b.mod.Start != nil {
			// ⚠️ Start 的失败（panic 或返回错误）一律只记日志、返回 nil，绝不流回 errgroup：
			// gctx 是全部成员共用的，一个成员的后台循环出事就取消它，等于把其余 N-1 个健康
			// 成员一起带下线，合并部署就白白放弃了独立部署本来就有的故障隔离（阶段四 Task 11
			// 真机复现过的缺口）。代价：这个成员的后台循环从此停着，直到外壳下次重启——
			// 有意接受的降级，ERROR 日志带 module_component_id 指出是谁。
			g.Go(func() error {
				// Go 的 panic 不会被 errgroup 或别的 goroutine 拦住，不在这里 recover
				// 就会直接终止整个外壳进程。RunStandalone 不需要这层：单模块进程里
				// panic 让进程退出就是正确行为（爆炸半径 = 1 个组件）。
				defer func() {
					if r := recover(); r != nil {
						logger.Error("模块后台循环 panic（已隔离，不影响外壳内其余模块）", "module_component_id", b.id, "recovered", r, "stack", string(debug.Stack()))
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

// requireShellAuthzURLs：外壳自己的 AUTHZ_BUNDLE_URL / IAM_JWKS_URL 缺任一（或为空白）就
// 启动即失败，点名缺的键。单跑组件缺这两个键是 fail-closed（每条受保护路由 403/503），
// 外壳里同样的缺失会让全部成员一起 fail-closed，而日志里只有一条 Info——与 Python 外壳的
// must_string 同一判据，在连库之前查。
func requireShellAuthzURLs(cfg Config) error {
	var missing []string
	for _, k := range []string{"AUTHZ_BUNDLE_URL", "IAM_JWKS_URL"} {
		if v, ok := cfg.ShellConfig.String(k); !ok || strings.TrimSpace(v) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("外壳 %s 缺少权限地址配置：%s（外壳自己的 configSchema，见 config/vars.yaml）", cfg.ShellName, strings.Join(missing, ", "))
	}
	return nil
}

// validateMemberPorts：成员的 httpPort 或任一额外端口 ≤ 0 就启动即失败，点名成员 ID。
// 端口 0 交给 Listen 会绑一个随机端口，平台按 component.yaml 的端口去连永远连不上，
// 外壳 /healthz 却是绿的——同 R15 的判据，在连库之前查。
func validateMemberPorts(members []ServedMember) error {
	for _, m := range members {
		if m.HTTPPort <= 0 {
			return fmt.Errorf("成员 %s 的 httpPort 无效（%d）：BRICKKIT_SERVED_MEMBERS_CONFIG 里每个成员都必须带它 component.yaml 的 deployment.port", m.ComponentID, m.HTTPPort)
		}
		for _, p := range m.ExtraPorts {
			if p.Port <= 0 {
				return fmt.Errorf("成员 %s 的额外端口 %s 无效（%d）", m.ComponentID, p.Name, p.Port)
			}
		}
	}
	return nil
}

// serveResult 把一个服务协程的结束归一成 errgroup 的返回值：ctx 已取消之后，不论带着什么
// 错误返回都算正常关停，返回 nil——gRPC 的 Serve 如果在 GracefulStop 之后才开始，会返回
// grpc.ErrServerStopped，启动窗口内收到 SIGTERM 时这是常态，不是成员故障。ctx 未取消时，
// 出错或意外返回 nil 都记 ERROR 并返回点名了归属的错误（R15：端口死了必须闹大）。
// memberID 为空表示外壳自己的端口。日志键用 module_component_id：外壳 logger 的
// component_id 已经是外壳自己的名字。
func serveResult(ctx context.Context, logger *slog.Logger, memberID, what string, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	if err == nil {
		err = errors.New("服务协程意外返回")
	}
	if memberID == "" {
		logger.Error("外壳自己的端口服务退出，外壳整体退出", "port", what, "error", err)
		return fmt.Errorf("外壳 %s 服务失败：%w", what, err)
	}
	logger.Error("成员端口服务退出，外壳整体退出", "module_component_id", memberID, "port", what, "error", err)
	return fmt.Errorf("成员 %s 的 %s 服务失败：%w", memberID, what, err)
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
		// 声明了额外端口却没有 RegisterGRPC：那个端口没人监听，调用方连不上，外壳 /healthz
		// 却是绿的——同 R15 的判据，启动即失败。
		if mod.RegisterGRPC == nil && len(m.ExtraPorts) > 0 {
			ports := make([]string, 0, len(m.ExtraPorts))
			for _, p := range m.ExtraPorts {
				ports = append(ports, fmt.Sprintf("%s(:%d)", p.Name, p.Port))
			}
			return nil, fmt.Errorf("成员 %s 声明了额外端口 %s，但构造函数没有返回 RegisterGRPC", m.ComponentID, strings.Join(ports, ", "))
		}
		out = append(out, builtModule{id: m.ComponentID, rt: rt, mod: mod})
	}
	return out, nil
}
