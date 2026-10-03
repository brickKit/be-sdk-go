package besdk

import (
	"context"
	"log/slog"

	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/pg"
)

// runMigrate is the migrate entry point (P1.1, P11.1, P11.3): the owner's dedicated connection runs
// the component's migrations, then the platform migration, then (with the events wave) the event
// streams and durables. It is the only place the owner's password file is read (P10.12).
func runMigrate(ctx context.Context, b *boot, cmd command) int {
	if !declared(b.vals, "PG_SCHEMA") {
		b.log.Info("migrate: the component declares no database; nothing to do")
		return exitOK
	}
	mc, err := migrateConfig(b)
	if err != nil {
		if ce, ok := config.AsError(err); ok {
			logConfigErrors(b.log, &configFailure{errs: []*config.Error{ce}})
			return exitConfig
		}
		b.log.Error("migrate", slog.String("error", err.Error()))
		return exitFailure
	}
	var res pg.MigrateResult
	switch cmd.kind {
	case cmdMigrateUp:
		res, err = pg.MigrateUp(ctx, mc)
		if err == nil && !res.Newer {
			err = ensureEventTopology(ctx, b)
		}
	case cmdMigrateDown:
		res, err = pg.MigrateDown(ctx, mc, cmd.n)
	case cmdMigrateStatus:
		res, err = pg.MigrateStatus(ctx, mc)
	}
	if err != nil {
		b.log.Error("migration failed", slog.String("error", err.Error()))
		return exitFailure
	}
	b.log.Info("migration done", slog.Uint64("from", uint64(res.From)), slog.Uint64("to", uint64(res.To)),
		slog.Uint64("image", uint64(res.Latest)), slog.Bool("dirty", res.Dirty), slog.Bool("schema_newer", res.Newer),
		slog.Int("platform", res.PlatformVersion), slog.Any("partitions_created", res.PartitionsCreated))
	return exitOK
}

func migrateConfig(b *boot) (pg.MigrateConfig, error) {
	path, ok := b.vals.String("PG_OWNER_PASSWORD_FILE")
	if !ok {
		return pg.MigrateConfig{}, &config.Error{Reason: config.ReasonMissing, Key: "PG_OWNER_PASSWORD_FILE", Detail: "the migrate step needs it"}
	}
	sec, err := config.OpenSecret("PG_OWNER_PASSWORD_FILE", path, config.SecretOptions{Required: true})
	if err != nil {
		return pg.MigrateConfig{}, err
	}
	v := b.vals
	return pg.MigrateConfig{Host: optString(v, "PG_MIGRATION_HOST", optString(v, "PG_HOST", "")),
		Port: int(intOr(v, "PG_MIGRATION_PORT", intOr(v, "PG_PORT", 5432))), Database: optString(v, "PG_DATABASE", ""),
		Owner: optString(v, "PG_OWNER_USER", ""), OwnerPassword: sec.Current(), Schema: optString(v, "PG_SCHEMA", ""),
		ComponentID: b.id, Component: b.spec.Migrations, Logger: b.log}, nil
}
