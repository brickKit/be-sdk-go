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
	HTTPHandler  http.Handler                 // ⭐ 外壳对 Gin 完全无感，它只 Serve 一个 handler
	RegisterGRPC func(*grpc.Server)
	Migrations   fs.FS                        // 外壳按拓扑顺序跑（§13.3 铁律五）
	Start        func(context.Context) error  // 后台循环。必须接 ctx，cancel 时返回
	Stop         func(context.Context) error
}
