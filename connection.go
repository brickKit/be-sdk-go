package besdk

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// PGDSN 从统一连接键拼出 pgx 认得的 DSN。连接信息是组件自己 configSchema 里的普通配置项
// PG_HOST/PG_PORT/PG_DATABASE/PG_USER/PG_PASSWORD（装配项目的 docs/en/01-conventions/04-configuration.md）。
//
// 不追加 sslmode：用 pgx 默认的 prefer——服务端支持 TLS 就用，不支持就退回明文，所以对不开
// TLS 的本地库也能直接连。SDK 不提供 sslmode 配置键；部署者要强制 TLS，在 PG_HOST 指向的
// 那一层解决（服务端 pg_hba 只放行 hostssl，或指向一个只接受 TLS 的代理）。
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
