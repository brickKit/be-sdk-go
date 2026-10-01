package besdk

import (
	"context"
	"io/fs"
	"net/http"

	"google.golang.org/grpc"
)

// Module 是模块交回去的一切。模块自己不 Listen、不注册全局、不装信号处理器
// （设计书 §12.5、§13.3 铁律七）。
type Module struct {
	HTTPHandler  http.Handler // ⭐ 外壳对 Gin 完全无感，它只 Serve 一个 handler
	RegisterGRPC func(*grpc.Server)
	Migrations   fs.FS                       // v0.3.0 起 SDK 不再消费：单跑与合并态的迁移都由 brickKit 用组件自己的镜像跑；字段暂留，免得现有组件编译失败
	Start        func(context.Context) error // 后台循环。必须接 ctx，cancel 时返回
	Stop         func(context.Context) error
}
