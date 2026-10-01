package besdk

import (
	"strings"
	"testing"
)

func TestEndpointFromConfigStripsScheme(t *testing.T) {
	c := NewConfig(map[string]string{
		"MDM_CUSTOMER_ENDPOINT":      "http://be-go-core-1-0-0:8101/",
		"MDM_CUSTOMER_GRPC_ENDPOINT": "http://be-go-core-1-0-0:9101",
	})
	if v, ok := c.Endpoint("mdm/customer", ""); !ok || v != "be-go-core-1-0-0:8101" {
		t.Fatalf("http = %q,%v", v, ok)
	}
	if v, ok := c.Endpoint("mdm/customer", "grpc"); !ok || v != "be-go-core-1-0-0:9101" {
		t.Fatalf("grpc = %q,%v", v, ok)
	}
}

func TestEndpointIgnoresProcessEnv(t *testing.T) {
	// 外壳成员的依赖地址只在成员 JSON 的 config 里；进程环境里的同名变量属于外壳自己，不能串进来
	t.Setenv("MDM_CUSTOMER_ENDPOINT", "http://wrong:1")
	if _, ok := NewConfig(nil).Endpoint("mdm/customer", ""); ok {
		t.Fatal("Config 里没有就必须是 ok=false，不能回落到进程环境")
	}
}

func TestEndpointAbsentAndEmptyAreBothMissing(t *testing.T) {
	if _, ok := NewConfig(nil).Endpoint("infra/workflow", ""); ok {
		t.Fatal("可选依赖缺失：键不存在 → ok=false")
	}
	if _, ok := NewConfig(map[string]string{"INFRA_WORKFLOW_ENDPOINT": ""}).Endpoint("infra/workflow", ""); ok {
		t.Fatal("键存在但为空 → ok=false")
	}
}

func TestEndpointNameDerivation(t *testing.T) {
	c := NewConfig(map[string]string{"INTEGRATION_IM_DINGTALK_GRPC_ENDPOINT": "http://h:1"})
	if _, ok := c.Endpoint("integration/im-dingtalk", "grpc"); !ok {
		t.Fatal("ID 中的 / 与 - 都换成 _")
	}
}

func TestMustEndpointPanicsWithVarName(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("应 panic")
		}
		if s, _ := r.(string); s == "" || !strings.Contains(s, "ERP_INVENTORY_GRPC_ENDPOINT") {
			t.Fatalf("panic 信息应点名变量：%v", r)
		}
	}()
	NewConfig(nil).MustEndpoint("erp/inventory", "grpc")
}

func TestS3URL(t *testing.T) {
	if v, ok := NewConfig(map[string]string{"S3_URL": "http://rustfs:9000"}).S3URL(); !ok || v != "http://rustfs:9000" {
		t.Fatalf("S3URL = %q,%v", v, ok)
	}
	if _, ok := NewConfig(nil).S3URL(); ok {
		t.Fatal("缺失 → ok=false")
	}
}
