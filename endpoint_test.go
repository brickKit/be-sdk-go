package besdk

import "testing"

func TestEndpoint_剥掉scheme(t *testing.T) {
	t.Setenv("MDM_CUSTOMER_GRPC_ENDPOINT", "http://mdm-customer-1-0-0:9090")
	got, ok := Endpoint("mdm/customer", "grpc")
	if !ok || got != "mdm-customer-1-0-0:9090" {
		t.Fatalf("期望 mdm-customer-1-0-0:9090/true，得到 %q/%v", got, ok)
	}
}

func TestEndpoint_主端口变量名不带额外端口段(t *testing.T) {
	t.Setenv("MDM_CUSTOMER_ENDPOINT", "http://mdm-customer-1-0-0:8080")
	got, ok := Endpoint("mdm/customer", "")
	if !ok || got != "mdm-customer-1-0-0:8080" {
		t.Fatalf("期望 mdm-customer-1-0-0:8080/true，得到 %q/%v", got, ok)
	}
}

// 弱依赖缺失时变量根本不存在，不是空串（§3.6）。
// 这一条守的是「组件代码不能用 os.Environ["X"] 崩掉」。
func TestEndpoint_弱依赖缺失返回false而不panic(t *testing.T) {
	if got, ok := Endpoint("infra/workflow", ""); ok || got != "" {
		t.Fatalf("期望 \"\"/false，得到 %q/%v", got, ok)
	}
}
