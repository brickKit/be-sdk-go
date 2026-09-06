package besdk

import (
	"context"
	"testing"

	"github.com/gin-gonic/gin"
)

// 十八条第 17 条：gin.SetMode 是包级全局，必须在 Bootstrap 里设一次，
// 不能让它停在默认的 DebugMode（panic 堆栈会直接吐给客户端）。
func TestBootstrap_把Gin设成ReleaseMode(t *testing.T) {
	gin.SetMode(gin.DebugMode) // 模拟"还没调用过 Bootstrap"的初始状态
	shutdown, err := Bootstrap(context.Background(), "test-service", "")
	if err != nil {
		t.Fatalf("Bootstrap 不该报错：%v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	if gin.Mode() != gin.ReleaseMode {
		t.Fatalf("期望 gin.Mode()=release，实际 %q", gin.Mode())
	}
}
