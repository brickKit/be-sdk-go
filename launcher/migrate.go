package launcher

import (
	"context"
	"fmt"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/internal/host"
	"github.com/brickKit/be-sdk-go/v3/obs"
)

// migrate is the migrate entry (P1.1, P1.2): the SDK's tables, then the component's, as the owner role directly on the
// database. Only the database keys are read, so the entry does not depend on the rest of the configuration.
func migrate(ctx context.Context, def component.Definition, o Options) int {
	if def.Migrations == nil {
		fmt.Fprintf(o.Stderr, "{\"level\":\"ERROR\",\"component\":%q,\"msg\":\"migrate: this component has no database\"}\n", def.ID)
		return ExitUsage
	}
	keys := append(db.MigrateKeys(), host.SDKKeys()[0]) // LOG_LEVEL
	values, problems := config.Parse(keys, o.Env)
	if len(problems) > 0 {
		host.ReportConfig(o.Stderr, def.ID, problems)
		return ExitConfiguration
	}
	log := obs.NewLogger(obs.Member{ID: def.ID, Version: def.Version}, obs.ParseLevel(values.String("LOG_LEVEL")), o.Stdout)
	cfg, problems := db.MigrateConfigFrom(def.ID, values, log)
	if len(problems) > 0 {
		host.ReportConfig(o.Stderr, def.ID, problems)
		return ExitConfiguration
	}
	if err := db.Migrate(ctx, cfg, db.SDKMigrations(),
		db.MigrationSet{Name: def.ID, Table: "schema_migrations", Files: def.Migrations}); err != nil {
		log.Error("migrate failed", "error", err.Error()) // the last line names the failed migration (P1.2)
		return ExitFailure
	}
	log.Info("migrate done")
	return ExitOK
}
