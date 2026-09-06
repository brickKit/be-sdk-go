package besdk

import (
	"os"
	"path/filepath"
	"testing"
)

// §13.8.1：平台明确不注入"我该监听哪个端口"——环境变量表里只有"别人在哪"
// （*_ENDPOINT），没有"我该监听哪"。RunStandalone 必须自己读 component.yaml
// 的 deployment.port / deployment.extraPorts，而不是等一个不存在的
// HTTP_PORT 环境变量（v0.1.0 的实现读的是 HTTP_PORT，从来没被平台真正
// 注入过——mdm-customer 是第一个真实组件，这里才核对出来）。
func TestLoadOwnPorts_解析port与extraPorts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "component.yaml")
	yaml := `
apiVersion: brickkit/v1
kind: Component
metadata:
  id: mdm/customer
  version: 1.0.0
deployment:
  type: container
  image: brickenterprise/mdm-customer:1.0.0
  port: 8080
  extraPorts:
    - { name: grpc, port: 9090 }
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := loadOwnPorts(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.HTTPPort != 8080 {
		t.Fatalf("期望 HTTPPort=8080，得到 %d", got.HTTPPort)
	}
	if got.ExtraPorts["grpc"] != 9090 {
		t.Fatalf("期望 ExtraPorts[grpc]=9090，得到 %+v", got.ExtraPorts)
	}
}

func TestLoadOwnPorts_没有extraPorts时返回空map不报错(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "component.yaml")
	yaml := "deployment:\n  port: 8080\n"
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := loadOwnPorts(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.ExtraPorts) != 0 {
		t.Fatalf("没有 extraPorts 时应该是空 map，得到 %+v", got.ExtraPorts)
	}
}

func TestLoadOwnPorts_文件不存在报清楚的错(t *testing.T) {
	_, err := loadOwnPorts(filepath.Join(t.TempDir(), "不存在.yaml"))
	if err == nil {
		t.Fatal("文件不存在应该报错")
	}
}
