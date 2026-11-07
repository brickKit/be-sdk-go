package tiering

import "context"

// SetCrash sets the crash points' hook (crash.go): the long simulation's replicas end themselves at a chosen point.
func SetCrash(f func(point string)) { crash = f }

// ClaimAgain claims a published or released unit as a rewrite does (a test makes another writer win the unit).
func (c *Catalog) ClaimAgain(ctx context.Context, unit string) (Claim, error) {
	return c.claim(ctx, unit, StatePublished, StateReleased)
}
