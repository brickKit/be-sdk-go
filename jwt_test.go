package besdk

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/golang-jwt/jwt/v5"
)

// testJWKS 是一整套"自己签发、自己验签"的测试夹具——infra-iam-casdoor
// 要到阶段三 Task 7 才建仓库，现在没有真实签发方可用。这里的加密运算
// 是真实的（真 RSA 密钥对、真 JWKS 端点、真 RS256 签名与验签），只是
// 身份是测试用的，不是"假装验证成功"的 mock。
type testJWKS struct {
	server  *httptest.Server
	priv    *rsa.PrivateKey
	kid     string
	handler func(w http.ResponseWriter, r *http.Request) // 可替换，测试 keyfunc 的刷新/出错路径用
}

func newTestJWKS(t *testing.T) *testJWKS {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tj := &testJWKS{priv: priv, kid: "test-key-1"}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		if tj.handler != nil {
			tj.handler(w, r)
			return
		}
		tj.writeJWKS(w)
	})
	tj.server = httptest.NewServer(mux)
	t.Cleanup(tj.server.Close)
	return tj
}

func (tj *testJWKS) writeJWKS(w http.ResponseWriter) {
	jwk, err := jwkset.NewJWKFromKey(tj.priv.Public(), jwkset.JWKOptions{
		Metadata: jwkset.JWKMetadataOptions{KID: tj.kid, ALG: jwkset.AlgRS256, USE: jwkset.UseSig},
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body := map[string]any{"keys": []jwkset.JWKMarshal{jwk.Marshal()}}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func (tj *testJWKS) url() string { return tj.server.URL + "/.well-known/jwks.json" }

// sign 签一个测试用的 JWT——sub/roles/dept_path/org_id 与真实
// infra-iam-casdoor 将来签发的应用 token 字段名完全一致（设计计划 §3）。
func (tj *testJWKS) sign(t *testing.T, sub string, roles []string, deptPath, orgID string, iat time.Time) string {
	t.Helper()
	claims := jwtClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   sub,
			IssuedAt:  jwt.NewNumericDate(iat),
			ExpiresAt: jwt.NewNumericDate(iat.Add(10 * time.Minute)),
		},
		Roles: roles, DeptPath: deptPath, OrgID: orgID,
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = tj.kid
	signed, err := tok.SignedString(tj.priv)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func TestJWTVerifier_验签合法token拿到正确claims(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	token := tj.sign(t, "u_zhangsan", []string{"sales_manager"}, "/root/china/east", "org1", time.Now())

	claims, err := v.Verify(token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Sub != "u_zhangsan" || claims.DeptPath != "/root/china/east" || claims.OrgID != "org1" {
		t.Fatalf("claims 解析不对：%+v", claims)
	}
	if len(claims.Roles) != 1 || claims.Roles[0] != "sales_manager" {
		t.Fatalf("roles 解析不对：%+v", claims.Roles)
	}
}

// TestJWTVerifier_签名被篡改会验签失败 是最基本的安全断言：换一把不
// 相关的私钥签同样的 payload，验签必须失败。
func TestJWTVerifier_签名被篡改会验签失败(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	claims := jwtClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject: "u_attacker", IssuedAt: jwt.NewNumericDate(time.Now()),
	}}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = tj.kid // 冒充已知的 kid，但用的是另一把私钥
	forged, err := tok.SignedString(otherKey)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := v.Verify(forged); err == nil {
		t.Fatal("用不匹配的私钥签的 token 应该验签失败")
	}
}

func TestJWTVerifier_algNone攻击被拒绝(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	claims := jwtClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject: "u_attacker", IssuedAt: jwt.NewNumericDate(time.Now()),
	}}
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	unsigned, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := v.Verify(unsigned); err == nil {
		t.Fatal("alg:none 的未签名 token 应该被拒绝——WithValidMethods 白名单只认 RS256")
	}
}
