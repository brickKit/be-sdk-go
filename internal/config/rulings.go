package config

import "slices"

// applyRulings brings the pinned rc.1 catalogue in line with the stage-B rulings that be-protocol
// rc.2 carries (schemas/config-keys.yaml): EVENTS_MAX_DELIVER and EVENTS_BACKOFF lose their
// catalogue defaults (P12.5: a platform-injected default would always be "set" and override every
// subscription's own value), and PG_POOL_MIN_IDLE is no protocol key (P10.5: idle connections are each
// SDK's own behaviour). Each step is a no-op once the pinned catalogue already says so.
// Delete when be-protocol rc.2 is pinned.
func applyRulings(c *Catalogue) {
	for _, name := range []string{"EVENTS_MAX_DELIVER", "EVENTS_BACKOFF"} {
		if i, ok := c.byName[name]; ok {
			c.Keys[i].Default = nil
		}
	}
	if i, ok := c.byName["PG_POOL_MIN_IDLE"]; ok {
		c.Keys = slices.Delete(c.Keys, i, i+1)
		c.byName = make(map[string]int, len(c.Keys))
		for j, k := range c.Keys {
			c.byName[k.Name] = j
		}
	}
}
