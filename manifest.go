package besdk

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ownPorts 是 RunStandalone 从组件自己的 component.yaml 里读出来的那两样
// ——§13.8.1：平台明确不注入"我该监听哪个端口"，环境变量表里只有"别人在
// 哪"（*_ENDPOINT），全拆态下这是唯一的权威来源，不许另写一份端口表。
type ownPorts struct {
	HTTPPort   int
	ExtraPorts map[string]int
}

// componentManifest 只解析 RunStandalone 关心的那几个字段，不是
// component.yaml 的完整镜像——完整校验是平台自己的事。
type componentManifest struct {
	Deployment struct {
		Port       int `yaml:"port"`
		ExtraPorts []struct {
			Name string `yaml:"name"`
			Port int    `yaml:"port"`
		} `yaml:"extraPorts"`
	} `yaml:"deployment"`
}

// loadOwnPorts 读 path 指向的 component.yaml，取 deployment.port 与
// deployment.extraPorts。
func loadOwnPorts(path string) (ownPorts, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ownPorts{}, fmt.Errorf("读 %s: %w", path, err)
	}
	var m componentManifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return ownPorts{}, fmt.Errorf("解析 %s: %w", path, err)
	}
	out := ownPorts{HTTPPort: m.Deployment.Port, ExtraPorts: map[string]int{}}
	for _, p := range m.Deployment.ExtraPorts {
		out.ExtraPorts[p.Name] = p.Port
	}
	return out, nil
}
