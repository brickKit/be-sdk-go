package besdk

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/telemetry"
)

// wireStore opens the component's pool and store (P10.1, P10.5); nothing connects yet.
func (p *process) wireStore() error {
	if !p.hasDatabase() {
		return nil
	}
	v := p.b.vals
	pw, ok := p.rt.cfg.secrets["PG_PASSWORD_FILE"]
	if !ok {
		return &config.Error{Reason: config.ReasonMissing, Key: "PG_PASSWORD_FILE", Detail: "the db profile needs it"}
	}
	poolMax := int(intOr(v, "PG_POOL_MAX", pg.DefaultPoolMax))
	pool, err := pg.OpenPool(pg.PoolConfig{Host: optString(v, "PG_HOST", ""), Port: int(intOr(v, "PG_PORT", 5432)),
		Database: optString(v, "PG_DATABASE", ""), User: optString(v, "PG_USER", ""), Password: pw.Current,
		MaxConns: poolMax, MinIdle: int(intOr(v, "PG_POOL_MIN_IDLE", 2)),
		MaxLifetime: optDuration(v, "PG_CONN_MAX_LIFETIME", 30*time.Minute),
		MaxIdleTime: optDuration(v, "PG_CONN_MAX_IDLE_TIME", 5*time.Minute), ApplicationName: p.b.id})
	if err != nil {
		return err
	}
	p.closers = append(p.closers, func(context.Context) { _ = pool.Close() })
	m, err := telemetry.NewDBMetrics(p.b.member.Registerer())
	if err != nil {
		return err
	}
	p.dbm = m
	p.rt.deps.store = pg.NewStore(pool, pg.StoreConfig{ComponentID: p.b.id, Role: optString(v, "PG_USER", ""),
		Schema: optString(v, "PG_SCHEMA", ""), Budget: poolMax,
		AcquireTimeout: optDuration(v, "PG_POOL_ACQUIRE_TIMEOUT", pg.DefaultAcquireTimeout),
		Hooks: pg.Hooks{
			OnTxRetry:  func(r string) { m.TxRetries.WithLabelValues(r).Inc() },
			OnPoolWait: func(d time.Duration) { m.PoolWait.Observe(d.Seconds()) },
			OnInUse:    func(delta int) { m.PoolInUse.Add(float64(delta)) },
		}})
	return nil
}

// superviseStore runs the start-up probe until the identity and the migrations check out (P10.7,
// P1.4), and keeps the outbox's partition window ahead (P16.6) until the lifecycle engine takes over.
func (p *process) superviseStore() {
	if p.rt.deps.store == nil {
		return
	}
	p.sup.Go("be.db.probe", p.probeLoop)
	p.sup.Go("be.outbox.window", p.outboxWindowLoop)
}

func (p *process) probeLoop(ctx context.Context) error {
	latest, err := pg.LatestVersion(p.b.spec.Migrations)
	if err != nil {
		p.fail(fmt.Errorf("component migrations: %w", err))
		return nil
	}
	owner := optString(p.b.vals, "PG_OWNER_USER", "")
	wait, reported := 500*time.Millisecond, ""
	for {
		r := pg.Probe(ctx, p.rt.deps.store, owner, false)
		switch {
		case r.Fatal != nil:
			p.fail(r.Fatal)
			return nil
		case r.Err != nil:
			reported = p.reportOnce(reported, "database not reachable yet", r.Err.Error())
		case len(r.IdentityProblems) > 0:
			p.dbm.IdentityOK.Set(0)
			reported = p.reportOnce(reported, "database identity check failed", strings.Join(r.IdentityProblems, "; "))
		case r.ComponentVersion > latest:
			p.fail(fmt.Errorf("the schema is at migration %d, newer than this image's %d (P1.8)", r.ComponentVersion, latest))
			return nil
		default:
			p.dbm.IdentityOK.Set(1)
			p.ready.Set("db_identity", true)
			if r.ComponentVersion == latest && !r.ComponentDirty && r.PlatformVersion == pg.PlatformVersion {
				p.ready.Set("migrations", true)
				return nil
			}
			reported = p.reportOnce(reported, "waiting for migrations",
				fmt.Sprintf("schema %d (platform %d), image %d (platform %d)", r.ComponentVersion, r.PlatformVersion, latest, pg.PlatformVersion))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		wait = min(2*wait, 15*time.Second)
	}
}

// reportOnce logs a problem when it differs from the last one reported.
func (p *process) reportOnce(last, msg, detail string) string {
	if key := msg + detail; key != last {
		p.b.log.Error(msg, slog.String("error", detail))
		return key
	}
	return last
}

func (p *process) outboxWindowLoop(ctx context.Context) error {
	for {
		err := p.rt.deps.store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
			_, err := pg.EnsureOutboxWindow(ctx, tx, time.Now(), pg.DefaultOutboxAhead)
			return err
		})
		wait := 6 * time.Hour
		if err != nil && ctx.Err() == nil {
			wait = time.Minute
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

func (p *process) migrationsInfo() MigrationsInfo {
	if !p.hasDatabase() {
		return MigrationsInfo{}
	}
	latest, _ := pg.LatestVersion(p.b.spec.Migrations)
	comp, plat := fmt.Sprintf("%04d", latest), pg.PlatformVersion
	return MigrationsInfo{Component: &comp, Platform: &plat}
}

func intOr(v *config.Values, key string, def int64) int64 {
	if !declared(v, key) {
		return def
	}
	if n, ok := v.Int(key); ok {
		return n
	}
	return def
}
