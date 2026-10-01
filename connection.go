package besdk

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// PGDSN 从统一连接键拼出 pgx 认得的 DSN。v1 起平台不再注入 DATABASE_* 资源变量，
// 连接信息是组件自己 configSchema 里的普通配置项（docs/conventions/configuration.md）。
//
// 口令用 url.UserPassword 编码——含 @ : / % 的口令原样拼进去会把 DSN 截断。
// PG_PASSWORD 允许为空字符串（trust 认证），但键必须存在：缺键说明 config 漏写，不是"没有密码"。
func PGDSN(cfg Config) (string, error) {
	var missing []string
	need := func(k string) string {
		v, ok := cfg.String(k)
		if !ok || v == "" {
			missing = append(missing, k)
		}
		return v
	}
	host, port, db, user := need("PG_HOST"), need("PG_PORT"), need("PG_DATABASE"), need("PG_USER")
	pw, ok := cfg.String("PG_PASSWORD")
	if !ok {
		missing = append(missing, "PG_PASSWORD")
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("缺少数据库连接配置：%s", strings.Join(missing, ", "))
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword(user, pw), Host: net.JoinHostPort(host, port), Path: "/" + db}
	return u.String(), nil
}

// NATSURL 读 NATS_URL（完整 URL，可含凭据）。
func NATSURL(cfg Config) (string, error) {
	v, ok := cfg.String("NATS_URL")
	if !ok || v == "" {
		return "", fmt.Errorf("缺少 NATS_URL")
	}
	return v, nil
}
