package besdk

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// manifest is the part of the image's component.yaml the runtime reads besides configSchema (which
// internal/config reads): ports (P1.13, P7.14) and the declared events (P12.16).
type manifest struct {
	Metadata struct {
		ID      string `yaml:"id"`
		Version string `yaml:"version"`
	} `yaml:"metadata"`
	Deployment struct {
		Port       int `yaml:"port"`
		ExtraPorts []struct {
			Name string `yaml:"name"`
			Port int    `yaml:"port"`
		} `yaml:"extraPorts"`
	} `yaml:"deployment"`
	Events struct {
		Publishes  []string `yaml:"publishes"`
		Subscribes []string `yaml:"subscribes"`
	} `yaml:"events"`
}

func parseManifest(b []byte) (manifest, error) {
	var m manifest
	if len(b) == 0 {
		return m, fmt.Errorf("Spec.Manifest is empty: embed the component's component.yaml")
	}
	if err := yaml.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("component.yaml: %w", err)
	}
	if m.Deployment.Port <= 0 {
		return m, fmt.Errorf("component.yaml: deployment.port missing")
	}
	return m, nil
}

// extraPort returns the port of the named extra port, 0 when not declared.
func (m manifest) extraPort(name string) int {
	for _, p := range m.Deployment.ExtraPorts {
		if p.Name == name {
			return p.Port
		}
	}
	return 0
}
