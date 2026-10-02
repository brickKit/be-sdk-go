package shell

import (
	"strings"
	"testing"
)

func TestParseServedMembersUnsetIsError(t *testing.T) {
	_, err := ParseServedMembers("", false)
	if err == nil || !strings.Contains(err.Error(), "未设置") {
		t.Fatalf("未设置应报错：%v", err)
	}
}

func TestParseServedMembersEmptyStringIsError(t *testing.T) {
	if _, err := ParseServedMembers("  ", true); err == nil {
		t.Fatal("空字符串应报错：零成员时平台给的是 []")
	}
}

func TestParseServedMembersZeroMembers(t *testing.T) {
	ms, err := ParseServedMembers("[]", true)
	if err != nil {
		t.Fatal(err)
	}
	if ms == nil || len(ms) != 0 {
		t.Fatalf("零成员应得到非 nil 的空切片，got %#v", ms)
	}
}

func TestParseServedMembersFields(t *testing.T) {
	raw := `[{"componentId":"mdm/customer","version":"2.0.0","httpPort":8101,
	  "extraPorts":[{"name":"grpc","port":9101}],
	  "config":{"PG_SCHEMA":"mdm_customer","INFRA_AUTHZ_ENDPOINT":"http://be-go-infra-1-0-0:8223"}}]`
	ms, err := ParseServedMembers(raw, true)
	if err != nil {
		t.Fatal(err)
	}
	m := ms[0]
	if m.ComponentID != "mdm/customer" || m.Version != "2.0.0" || m.HTTPPort != 8101 {
		t.Fatalf("%#v", m)
	}
	if len(m.ExtraPorts) != 1 || m.ExtraPorts[0].Name != "grpc" || m.ExtraPorts[0].Port != 9101 {
		t.Fatalf("extraPorts %#v", m.ExtraPorts)
	}
	if m.Config["PG_SCHEMA"] != "mdm_customer" {
		t.Fatalf("config %#v", m.Config)
	}
}

func TestParseServedMembersSecretsSurviveVerbatim(t *testing.T) {
	pem := "-----BEGIN PRIVATE KEY-----\nMIIB$abc\"q\\x\n-----END PRIVATE KEY-----\n"
	raw := `[{"componentId":"infra/iam-casdoor","version":"2.0.0","httpPort":8200,"extraPorts":[],
	  "config":{"APP_TOKEN_SIGNING_KEY_PEM":"-----BEGIN PRIVATE KEY-----\nMIIB$abc\"q\\x\n-----END PRIVATE KEY-----\n"}}]`
	ms, err := ParseServedMembers(raw, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := ms[0].Config["APP_TOKEN_SIGNING_KEY_PEM"]; got != pem {
		t.Fatalf("PEM 未逐字节保留：%q", got)
	}
}

func TestParseServedMembersMalformedJSON(t *testing.T) {
	if _, err := ParseServedMembers(`[{"componentId":`, true); err == nil {
		t.Fatal("坏 JSON 应报错")
	}
}

// "null" 不是零成员：brickKit 零成员时给的是 []，null 说明数据在传递中损坏，必须报错，
// 不能悄悄起一个什么都不服务的外壳。
func TestParseServedMembersNullIsError(t *testing.T) {
	for _, raw := range []string{"null", " null\n", "[null]", `[{"componentId":"","httpPort":8101}]`} {
		ms, err := ParseServedMembers(raw, true)
		if err == nil || !(strings.Contains(err.Error(), "null") || strings.Contains(err.Error(), "componentId")) {
			t.Fatalf("%q 应报错并说明是 null 或缺 componentId，got ms=%#v err=%v", raw, ms, err)
		}
	}
}
