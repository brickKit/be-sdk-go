package authz

import (
	"encoding/json"
	"fmt"
)

// Capabilities are the provider's capability flags (contract-infra-authz capabilities.yaml). The
// fields E2–E5 need are typed; every other name, known or not, is reachable through Has. Unknown
// names are ignored (E1); a known flag of the wrong JSON type refuses the bundle.
type Capabilities struct {
	Core          bool
	AdminWrite    bool
	Sharing       bool
	RelationSync  bool
	Check         bool
	Graph         bool
	Delegation    bool
	Agents        bool
	Impersonation bool
	AccessReview  bool
	ExplainPaths  bool
	Conditions    bool
	// ListObjectsMax is list_objects.max_results, 0 when list_objects is absent or false.
	ListObjectsMax int

	raw map[string]json.RawMessage
}

// Has reports whether the capability called name is on: its value is true or an object
// (list_objects). An absent or unknown name is off.
func (c Capabilities) Has(name string) bool {
	v, ok := c.raw[name]
	if !ok {
		return false
	}
	var on bool
	if json.Unmarshal(v, &on) == nil {
		return on
	}
	var obj map[string]json.RawMessage
	return json.Unmarshal(v, &obj) == nil && obj != nil
}

// UnmarshalJSON reads the typed flags strictly and keeps every member for Has.
func (c *Capabilities) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("capabilities: %w", err)
	}
	flags := map[string]*bool{
		"core": &c.Core, "admin_write": &c.AdminWrite, "sharing": &c.Sharing,
		"relation_sync": &c.RelationSync, "check": &c.Check, "graph": &c.Graph,
		"delegation": &c.Delegation, "agents": &c.Agents, "impersonation": &c.Impersonation,
		"access_review": &c.AccessReview, "explain_paths": &c.ExplainPaths, "conditions": &c.Conditions,
	}
	for name, dst := range flags {
		v, ok := raw[name]
		if !ok {
			continue
		}
		if err := json.Unmarshal(v, dst); err != nil {
			return fmt.Errorf("capability %s: %w", name, err)
		}
	}
	if v, ok := raw["list_objects"]; ok {
		max, err := parseListObjects(v)
		if err != nil {
			return err
		}
		c.ListObjectsMax = max
	}
	c.raw = raw
	return nil
}

// parseListObjects reads list_objects: false, or {"max_results": n}.
func parseListObjects(v json.RawMessage) (int, error) {
	var off bool
	if json.Unmarshal(v, &off) == nil {
		if off {
			return 0, fmt.Errorf("capability list_objects: true is not a valid value")
		}
		return 0, nil
	}
	var obj struct {
		MaxResults int `json:"max_results"`
	}
	if err := json.Unmarshal(v, &obj); err != nil {
		return 0, fmt.Errorf("capability list_objects: %w", err)
	}
	return obj.MaxResults, nil
}
