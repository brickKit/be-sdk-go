package besdk

import (
	"context"
	"fmt"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// Claims 是本地验签后从 JWT 里取出的身份信息（设计书 §14.1.5：JWT 只带
// 身份，权限键一个都不进）。
type Claims struct {
	Sub      string
	Roles    []string
	DeptPath string
	OrgID    string
	IssuedAt time.Time
}

// jwtClaims 是 golang-jwt 解析用的具体结构——业务字段之外内嵌
// RegisteredClaims 拿 sub/iat 这两个标准字段。
type jwtClaims struct {
	jwt.RegisteredClaims
	Roles    []string `json:"roles"`
	DeptPath string   `json:"dept_path"`
	OrgID    string   `json:"org_id"`
}

// jwtVerifier 包一层 keyfunc.Keyfunc——它自带 JWK Set 的后台刷新，
// be-sdk 不需要自己再写一份 JWKS 缓存（决策 32：有现成的就用现成的）。
type jwtVerifier struct {
	kf keyfunc.Keyfunc
}

// newJWTVerifier 对接 iamJwksUrl。ctx 用于结束 keyfunc 内部的刷新协程
// （传 RunStandalone 收到 SIGTERM/SIGINT 时会取消的那个 ctx）。
func newJWTVerifier(ctx context.Context, jwksURL string) (*jwtVerifier, error) {
	kf, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
	if err != nil {
		return nil, fmt.Errorf("初始化 JWKS 客户端: %w", err)
	}
	return &jwtVerifier{kf: kf}, nil
}

// Verify 验签 + 解析。⚠️ 只认 RS256——Casdoor（`infra-iam-casdoor` 的
// slot:iam 默认实现）与绝大多数 JWKS 发布方的默认算法，显式白名单防
// "alg: none" 之类的算法混淆攻击。
func (v *jwtVerifier) Verify(tokenString string) (*Claims, error) {
	var claims jwtClaims
	token, err := jwt.ParseWithClaims(tokenString, &claims, v.kf.Keyfunc,
		jwt.WithValidMethods([]string{"RS256"}))
	if err != nil {
		return nil, fmt.Errorf("验签失败: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("token 无效")
	}
	sub, err := claims.GetSubject()
	if err != nil || sub == "" {
		return nil, fmt.Errorf("token 缺少 sub")
	}
	iat, err := claims.GetIssuedAt()
	if err != nil || iat == nil {
		return nil, fmt.Errorf("token 缺少 iat")
	}
	return &Claims{
		Sub: sub, Roles: claims.Roles, DeptPath: claims.DeptPath, OrgID: claims.OrgID,
		IssuedAt: iat.Time,
	}, nil
}
