package shell

import (
	"context"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
)

func needsDB(plans []planned) bool {
	for _, p := range plans {
		if p.def.Migrations != nil {
			return true
		}
	}
	return false
}

// openPools opens one pool per database target with the shell's login role, sized to the sum of the members' budgets
// on that target; each member's budget still bounds what it may hold (protocol 17, "budgets").
func openPools(ctx context.Context, self Identity, sv *config.Values, plans []planned) (map[db.Target]*db.Pool, error) {
	sizes := map[db.Target]int32{}
	prepared := map[db.Target]bool{}
	for _, p := range plans {
		if p.def.Migrations == nil {
			continue
		}
		s := db.SettingsFrom(p.values)
		sizes[s.Target] += int32(s.Foreground + s.Background)
		prepared[s.Target] = s.Prepared // targetsAgree has refused disagreement
	}
	pools := map[db.Target]*db.Pool{}
	for t, n := range sizes {
		p, err := db.Open(ctx, db.PoolConfig{Target: t, User: sv.String("PG_SHELL_USER"), Password: sv.Secret("PG_SHELL_PASSWORD"),
			AppName: self.ID, MaxConns: n, Prepared: prepared[t]})
		if err != nil {
			return pools, err
		}
		pools[t] = p
	}
	return pools, nil
}
