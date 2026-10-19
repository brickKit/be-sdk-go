package shell

import (
	"context"
	"log/slog"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/bus"
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
func openPools(ctx context.Context, self Identity, sv *config.Values, plans []planned,
	log *slog.Logger) (map[db.Target]*db.Pool, error) {
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
			AppName: self.ID, MaxConns: n, Prepared: prepared[t], Log: log})
		if err != nil {
			return pools, err
		}
		pools[t] = p
	}
	return pools, nil
}

// openBus opens the one bus connection the members share (protocol 17); BUS_URL agrees across them (P2.11).
func openBus(ctx context.Context, self Identity, _ *config.Values, plans []planned, log *slog.Logger) (events.Transport, error) {
	for _, p := range plans {
		if len(p.def.Streams) > 0 || p.def.Subscribes {
			return bus.Open(ctx, p.values.Secret("BUS_URL").Value(), self.ID, log)
		}
	}
	return nil, nil
}
