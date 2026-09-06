package besdk

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Config 是模块读配置的唯一入口（§12.5.3）。模块代码里零 os.Getenv。
//
// 数据来源不由 Config 自己决定：
//   单跑：RunStandalone 从进程环境变量读一份快照灌进来
//   合并：外壳启动器给每个模块一份只属于它自己的 env map（§13.8.2）
//
// ⚠️ 值全部是字符串（平台把 configSchema 的每一项都渲染成环境变量），
// 类型转换在这里做一次，业务代码不重复解析。
type Config struct {
	values map[string]string
}

// NewConfig 供 RunStandalone / 外壳启动器构造，模块自己不调用。
func NewConfig(values map[string]string) Config {
	return Config{values: values}
}

func (c Config) String(key string) (string, bool) {
	v, ok := c.values[key]
	return v, ok
}

func (c Config) StringOr(key, def string) string {
	if v, ok := c.values[key]; ok {
		return v
	}
	return def
}

// MustString 用于 configSchema 里没写 default 的必填项：拿不到直接 panic，
// 因为这类配置缺失属于部署错误，不该让模块带着一个空字符串跑起来。
func (c Config) MustString(key string) string {
	v, ok := c.values[key]
	if !ok {
		panic(fmt.Sprintf("必填配置项 %q 未注入", key))
	}
	return v
}

func (c Config) Int(key string) (int, bool) {
	v, ok := c.values[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (c Config) IntOr(key string, def int) int {
	if n, ok := c.Int(key); ok {
		return n
	}
	return def
}

func (c Config) Bool(key string) (bool, bool) {
	v, ok := c.values[key]
	if !ok {
		return false, false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, false
	}
	return b, true
}

func (c Config) BoolOr(key string, def bool) bool {
	if b, ok := c.Bool(key); ok {
		return b
	}
	return def
}

// Runtime 是调用方交给模块的一切。模块自己不去取任何一样（设计书 §12.5、
// §13.3 铁律七，总纲 SOP-L 的 L-1）。
//
//	单跑：由 RunStandalone 填（读进程环境变量、自己开池、自己连 NATS）
//	合并：由外壳启动器填（本模块那一份 env map、外壳的唯一全局池、共用的 NATS 连接）
type Runtime struct {
	ComponentID      string
	ComponentVersion string
	Config           Config               // ⭐ 模块读配置的唯一入口
	DB               *sql.DB              // ⭐ 合并态下是外壳的唯一池（§13.3 铁律二）
	NATS             *nats.Conn
	Logger           *slog.Logger         // 已注入 component_id 与 trace 上下文
	Tracer           trace.Tracer
	Meter            metric.Meter
	Registry         *prometheus.Registry // ⭐ 每模块一个，不是默认全局那个
	HTTPPort         int                  // 从 component.yaml 来，平台不注入（§13.8.1）
	ExtraPorts       map[string]int       // {"grpc": 9090}
}
