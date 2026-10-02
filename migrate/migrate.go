// Package migrate 是组件迁移镜像的入口。组件的 backend/cmd/migrate/main.go 只有一行：
//
//	func main() { migrate.Main(migrations.FS) }
//
// brickKit v1 在组件（单跑）或外壳（合并部署）启动之前，用组件自己的镜像和它自己的配置
// 跑这个入口——外壳和 RunStandalone 都不跑迁移。
//
// golang-migrate 只在本子包里 import：根包 besdk 不依赖它，模块代码不会被它拖进来。
package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	besdk "github.com/brickKit/be-sdk-go"
	gomigrate "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // 注册 pgx5:// 驱动；与 SDK 同用 pgx，不引 lib/pq
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// ErrUsage 表示命令行参数不对：恰好一个参数，up 或 down。
var ErrUsage = errors.New("用法：migrate up|down")

// pgKeys 是迁移需要的全部连接键，顺序即报错时列出的顺序。
var pgKeys = []string{"PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER", "PG_PASSWORD", "PG_SCHEMA"}

// schemaRe 与 besdk.WithTx 对 schema 名的要求一致：它会原样拼进 search_path 和状态表名。
var schemaRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Main 是迁移进程的入口：先校验参数（不对就打印用法、以 2 退出，不读环境、不连库），
// 再从进程环境读 PG_*，跑 Run。其它任何错误都以 1 退出。这是进程入口不是模块，可以读
// 环境、可以退出进程。
func Main(src fs.FS) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger) // 迁移进程自己的入口，Run 经 slog.Default 记的日志也走 JSON
	if _, err := parseArgs(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	env := make(map[string]string, len(pgKeys))
	for _, k := range pgKeys {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
	}
	// SIGTERM/SIGINT 时跑完当前这条迁移再停（两条之间查 ctx），宽限期内不会在一条迁移中途被杀、留下 dirty 状态。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := Run(ctx, env, os.Args[1:], src); err != nil {
		logger.Error("迁移失败", "error", err)
		os.Exit(1)
	}
}

// Run 是 Main 的可测形式：env 与 args 显式传入，返回错误不退出。
// 已是最新 / 已全部回滚不是错误——迁移必须能连跑两次都成功。
// ctx 取消时在当前这条迁移跑完后停下，并返回错误（没跑完不报成功）。
func Run(ctx context.Context, env map[string]string, args []string, src fs.FS) error {
	direction, err := parseArgs(args)
	if err != nil {
		return err
	}
	dsn, err := dsn(env)
	if err != nil {
		return err
	}
	srcDriver, err := iofs.New(src, ".")
	if err != nil {
		return fmt.Errorf("读取迁移文件失败：%w", err)
	}
	m, err := gomigrate.NewWithSourceInstance("iofs", srcDriver, dsn)
	if err != nil {
		return fmt.Errorf("迁移初始化失败（schema %s）：%w", env["PG_SCHEMA"], err)
	}
	defer func() { _, _ = m.Close() }()

	var aborted bool
	if direction == "up" {
		aborted, err = stepUp(ctx, m, srcDriver, env["PG_SCHEMA"])
	} else {
		aborted, err = stepDown(ctx, m)
	}
	if err != nil {
		logFinalVersion(m, direction, env["PG_SCHEMA"], "failed")
		return fmt.Errorf("迁移 %s 失败（schema %s）：%w", direction, env["PG_SCHEMA"], err)
	}
	// 在两条迁移之间看到 ctx 已取消就停下：没跑完不能报成功。
	if aborted {
		logFinalVersion(m, direction, env["PG_SCHEMA"], "aborted")
		return fmt.Errorf("迁移 %s 被中止（schema %s），可能只执行了一部分：%w", direction, env["PG_SCHEMA"], ctx.Err())
	}
	logFinalVersion(m, direction, env["PG_SCHEMA"], "ok")
	return nil
}

// logFinalVersion 在结束时记下库里的最终迁移版本——迁移容器的日志是排障的主要入口。
// outcome：ok / aborted / failed。全部回滚后没有版本时 version 记为 none。
func logFinalVersion(m *gomigrate.Migrate, direction, schema, outcome string) {
	level := slog.LevelInfo
	if outcome != "ok" {
		level = slog.LevelWarn
	}
	attrs := []any{"direction", direction, "schema", schema, "outcome", outcome}
	v, dirty, err := m.Version()
	switch {
	case errors.Is(err, gomigrate.ErrNilVersion):
		attrs = append(attrs, "version", "none")
	case err != nil:
		attrs = append(attrs, "version_error", err.Error())
	default:
		attrs = append(attrs, "version", v, "dirty", dirty)
	}
	slog.Default().Log(context.Background(), level, "迁移结束", attrs...)
}

// stepUp 一条一条地 up（m.Steps(1)），每两条之间看一眼 ctx：取消了就在当前这条跑完之后停下，
// 返回 aborted=true。不用 golang-migrate 的 GracefulStop：v4.20.1 里它的 isGracefulStop 被两个
// goroutine 无同步地读写，-race 会报数据竞争。
//
// Steps(1) 返回 os.ErrNotExist 有三种含义，按库的当前版本区分：
//   - 版本 = 本迁移集的最后一个版本：已是最新，结束；
//   - 版本 > 最后一个版本：库比本镜像新。brickKit 多版本并存时按版本号串联迁移，低版本先跑、
//     高版本后跑，而且每次 up 都重跑，低版本镜像的迁移集里自然没有高版本迁到的版本号。本镜像无事
//     可做：记一条带两个版本号的 WARN，结束。版本间的数据兼容由组件作者负责（迁移只做加法）；
//   - 其它（版本落在迁移集范围内却找不到，即迁移文件缺号；或 dirty）：原样返回错误。
func stepUp(ctx context.Context, m *gomigrate.Migrate, src source.Driver, schema string) (bool, error) {
	latest, hasAny := lastVersion(src)
	for {
		if ctx.Err() != nil {
			return true, nil
		}
		err := m.Steps(1)
		if err == nil {
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		dbVersion, dirty, verr := m.Version()
		switch {
		case errors.Is(verr, gomigrate.ErrNilVersion) && !hasAny:
			return false, nil // 空迁移集、空库：无事可做
		case verr != nil || dirty || !hasAny:
			return false, err
		case dbVersion == latest:
			return false, nil
		case dbVersion > latest:
			slog.Default().Warn("库的迁移版本比本镜像的迁移集新，跳过 up（多版本并存时低版本在高版本之后重跑属正常）",
				"schema", schema, "db_version", dbVersion, "image_latest_version", latest)
			return false, nil
		default:
			return false, err
		}
	}
}

// stepDown 一条一条地回滚到底（m.Steps(-1)），同样每两条之间看 ctx。回滚到没有版本即结束；
// 其它 os.ErrNotExist（库的版本不在本迁移集里，比如库比本镜像新）一律报错——down 不放宽。
func stepDown(ctx context.Context, m *gomigrate.Migrate) (bool, error) {
	for {
		if ctx.Err() != nil {
			return true, nil
		}
		err := m.Steps(-1)
		if err == nil {
			continue
		}
		if errors.Is(err, os.ErrNotExist) {
			if _, _, verr := m.Version(); errors.Is(verr, gomigrate.ErrNilVersion) {
				return false, nil
			}
		}
		return false, err
	}
}

// lastVersion 沿 First/Next 走完迁移集，返回最后一个版本；迁移集为空时 ok=false。
func lastVersion(src source.Driver) (uint, bool) {
	v, err := src.First()
	if err != nil {
		return 0, false
	}
	for {
		next, err := src.Next(v)
		if err != nil {
			return v, true
		}
		v = next
	}
}

func parseArgs(args []string) (string, error) {
	if len(args) != 1 || (args[0] != "up" && args[0] != "down") {
		return "", fmt.Errorf("%w（收到 %q）", ErrUsage, args)
	}
	return args[0], nil
}

// dsn 复用 besdk.PGDSN 的拼法（口令经 url.UserPassword 转义，含 @ : / % 也不会截断），
// 换成 golang-migrate 的 pgx5 scheme，再加两个参数：
//   - search_path=<PG_SCHEMA>：迁移里的 SQL 和状态表都落在组件自己的 schema；
//   - x-migrations-table=schema_migrations_<PG_SCHEMA>：裸表名，不带 schema 前缀。
//     不加 x-migrations-table-quoted——不加时 "schema.table" 会被当成一个带字面点号的表名。
//
// 不追加 sslmode：pgx 默认 prefer，对不开 TLS 的本地库可用（与 besdk.PGDSN 一致）。
func dsn(env map[string]string) (string, error) {
	var missing []string
	for _, k := range pgKeys {
		v, ok := env[k]
		// PG_PASSWORD 允许为空串（trust 认证），但键必须存在——同 besdk.PGDSN。
		if !ok || (v == "" && k != "PG_PASSWORD") {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("缺少数据库连接配置：%s", strings.Join(missing, ", "))
	}
	schema := env["PG_SCHEMA"]
	if !schemaRe.MatchString(schema) {
		return "", fmt.Errorf("PG_SCHEMA %q 不是合法的 schema 名（小写字母开头，只含小写字母、数字、下划线）", schema)
	}
	raw, err := besdk.PGDSN(besdk.NewConfig(env))
	if err != nil {
		return "", err
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("拼接迁移 DSN 失败：%w", err)
	}
	u.Scheme = "pgx5"
	q := u.Query()
	q.Set("search_path", schema)
	q.Set("x-migrations-table", "schema_migrations_"+schema)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
