package besdk

import (
	"context"
	"net/http"

	"google.golang.org/grpc"
)

// Module 是模块交回去的一切。模块自己不 Listen、不注册全局、不装信号处理器
// （设计书 §12.5、§13.3 铁律七）。
//
// 迁移不在这里：brickKit 在组件（或外壳）启动前用组件自己的镜像跑迁移，入口是组件
// backend/cmd/migrate/main.go 里的一行 migrate.Main(migrations.FS)（本 SDK 的 migrate 子包）。
type Module struct {
	HTTPHandler  http.Handler // ⭐ 外壳对 Gin 完全无感，它只 Serve 一个 handler
	RegisterGRPC func(*grpc.Server)
	Start        func(context.Context) error // 后台循环。必须接 ctx，cancel 时返回
	Stop         func(context.Context) error
}
