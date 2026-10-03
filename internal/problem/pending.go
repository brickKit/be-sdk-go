package problem

import (
	_ "embed"
	"fmt"
)

// pendingBe holds the reasons ruled into domain be after rc.1: REQUEST_INVALID (malformed request)
// and DEPENDENCY_UNAVAILABLE (metadata.dependency = "db" | a component id | "bus").
// Delete when be-protocol rc.2 is pinned (its errors-be.yaml carries both).
//
//go:embed errors-be-pending.yaml
var pendingBe []byte

// mergePending adds each pending be reason the pinned catalogue lacks; a reason the pinned file
// already carries wins.
func (c *Catalogue) mergePending() error {
	p := &Catalogue{domains: map[string]map[string]reasonEntry{}}
	if err := p.add(pendingBe, nil); err != nil {
		return fmt.Errorf("pending be reasons: %w", err)
	}
	be := c.domains[DomainBe]
	for reason, r := range p.domains[DomainBe] {
		if _, ok := be[reason]; !ok {
			be[reason] = r
		}
	}
	return nil
}
