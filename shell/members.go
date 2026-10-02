// Package shell 是外壳进程的全部装配：把 RunStandalone 的"1 模块"形状推广到"N 模块"。
// 外壳的 main.go 只有一行：shell.Main("<外壳名>", shell.Registry{...})。
package shell

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ExtraPort 是成员 component.yaml 里的一条 extraPorts。
type ExtraPort struct {
	Name string `json:"name"`
	Port int    `json:"port"`
}

// ServedMember 是 BRICKKIT_SERVED_MEMBERS_CONFIG 数组的一项。Config 是这个成员独立部署时
// 会拿到的全部环境变量（已求值：$var:、${}、file:// 都已替换，*_ENDPOINT 也在里面），
// 不含 COMPONENT_ID / COMPONENT_VERSION（那两个就是 ComponentID / Version）。
type ServedMember struct {
	ComponentID string            `json:"componentId"`
	Version     string            `json:"version"`
	HTTPPort    int               `json:"httpPort"`
	ExtraPorts  []ExtraPort       `json:"extraPorts"`
	Config      map[string]string `json:"config"`
}

// ParseServedMembers 解析平台注入的成员清单。present 是 os.LookupEnv 的第二个返回值。
//
// ⚠️ 三种状态含义相反，不能混为一谈：
//   - 未设置：这个进程不是 brickkit 作为外壳启动的（比如手工 docker run），报错；
//   - "[]"：这次部署所有成员都被移出外壳，零成员正常启动；
//   - 空字符串或 null（或某一项没有 componentId）：平台从不这样给，视为数据损坏，报错。
//
// 绝不能把"没有成员"退化成"启动全部编译进来的模块"。
func ParseServedMembers(raw string, present bool) ([]ServedMember, error) {
	if !present {
		return nil, errors.New("BRICKKIT_SERVED_MEMBERS_CONFIG 未设置：这个进程看起来不是由 brickkit 作为外壳启动的")
	}
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("BRICKKIT_SERVED_MEMBERS_CONFIG 为空字符串：零成员时平台给的是 []，空串说明数据在传递中损坏")
	}
	var ms []ServedMember
	if err := json.Unmarshal([]byte(raw), &ms); err != nil {
		return nil, fmt.Errorf("解析 BRICKKIT_SERVED_MEMBERS_CONFIG 失败：%w", err)
	}
	// "[]" 解出来是非 nil 的空切片；只有 null 会留下 nil。
	if ms == nil {
		return nil, errors.New("BRICKKIT_SERVED_MEMBERS_CONFIG 为 null：零成员时平台给的是 []，null 说明数据在传递中损坏")
	}
	for i, m := range ms {
		if strings.TrimSpace(m.ComponentID) == "" {
			return nil, fmt.Errorf("BRICKKIT_SERVED_MEMBERS_CONFIG 第 %d 项没有 componentId（null 项或数据损坏）", i)
		}
	}
	return ms, nil
}
