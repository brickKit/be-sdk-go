package besdk

// LoadOwnHTTPPort 供 shell 包读外壳自己的 component.yaml（deployment.port）。
// 外壳和单跑组件一样，平台不注入"我该监听哪个端口"，唯一权威来源是镜像里那份 component.yaml。
func LoadOwnHTTPPort(path string) (int, error) {
	p, err := loadOwnPorts(path)
	return p.HTTPPort, err
}

// EnvSnapshot 供 shell 包读外壳进程自己的环境（外壳进程级，不属于任何模块）。
// ⚠️ 只能用来构造外壳自己的 Config；成员的 Config 一律来自 BRICKKIT_SERVED_MEMBERS_CONFIG。
func EnvSnapshot() map[string]string { return envSnapshot() }
