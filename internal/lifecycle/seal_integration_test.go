package lifecycle_test

import (
	"context"
	"encoding/hex"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/lifecycle"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// runner adapts a store to the engine's RunFunc, as the root does.
func runner(s *pg.Store) lifecycle.RunFunc {
	return func(ctx context.Context, lockTimeout time.Duration, fn func(context.Context, lifecycle.Tx) error) error {
		return s.Run(ctx, pg.TxOptions{LockTimeout: lockTimeout}, func(ctx context.Context, tx *pg.Tx) error { return fn(ctx, tx) })
	}
}

// published collects the events the engine hands to the root's publisher.
type published struct {
	mu     sync.Mutex
	events []lifecycle.Event
}

func (p *published) publish(_ context.Context, _ lifecycle.Tx, ev lifecycle.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	return nil
}

func (p *published) subjects() []string { return p.of("") }

// of lists "<subject> <table>/<unit>" of the events with the given action ("" = all).
func (p *published) of(action string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, e := range p.events {
		if action == "" || e.Action == action {
			out = append(out, e.Subject+" "+e.Table+"/"+e.Unit)
		}
	}
	return out
}

func engine(t *testing.T, e *env, mode string, pub *published) *lifecycle.Engine {
	t.Helper()
	cfg, err := lifecycle.ParseConfig("mode: " + mode)
	require.NoError(t, err)
	en, err := lifecycle.New(e.decl, lifecycle.Options{ComponentID: componentID, Config: cfg, Publish: pub.publish})
	require.NoError(t, err)
	return en
}

func step(t *testing.T, e *env, en *lifecycle.Engine, now time.Time) lifecycle.Report {
	t.Helper()
	r, err := en.Step(within(t, time.Minute), runner(e.store), now)
	require.NoError(t, err)
	return r
}

func insertLedger(t *testing.T, e *env, at time.Time, amount string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO widget_ledger (id, created_at, legal_entity_id, widget_id, entry_no,
		  posting_date, fiscal_period, currency, amount) VALUES ($1, $2, 'LE01', $1, 'E-1', '2026-10-03', '2026-P10', 'CNY', $3)`, id, at, amount)
		return err
	}))
	return id
}

func requireSealed(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.True(t, problem.Is(err, "be", "UNIT_SEALED"), "want UNIT_SEALED, got %v", err)
}

// A ledger sealed `immediate` refuses UPDATE / DELETE / TRUNCATE from the partition's creation
// (P16.5, G5); once the range has ended the engine records it SEALED with its digest and chain, logs
// it and announces it (P16.7). besdk_lifecycle_log refuses changes the same way.
func TestSealImmediate(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		t.Run("pg"+major, func(t *testing.T) {
			e := newEnv(t, major, widgetFS(t), true)
			id := insertLedger(t, e, day0, "12.5000")
			insertLedger(t, e, day0.Add(time.Hour), "-3.2500")

			requireSealed(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE widget_ledger SET amount = 0 WHERE id = $1`, id)
				return err
			}))
			requireSealed(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
				_, err := tx.ExecContext(ctx, `DELETE FROM widget_ledger WHERE id = $1`, id)
				return err
			}))
			// the runtime role has no TRUNCATE privilege; the owner has, and the guard still refuses it
			requireSealed(t, ownerStore(t, e).Run(within(t, 10*time.Second), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
				_, err := tx.ExecContext(ctx, `TRUNCATE widget_ledger_2026m10`)
				return err
			}))

			var pub published
			en := engine(t, e, "on", &pub)
			r := step(t, e, en, day0)
			require.Empty(t, r.Sealed, "October has not ended")

			r = step(t, e, en, time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC))
			require.Contains(t, r.Sealed, "widget_ledger_2026m10")
			require.Contains(t, r.Sealed, "widget_audit_2026m10")
			require.NotContains(t, r.Sealed, "widget_ledger_2026m11")
			require.Contains(t, pub.subjects(), "conformance.widget.lifecycle.sealed.v1 widget_ledger/widget_ledger_2026m10")

			var state string
			var rows int64
			var unitDigest, chain []byte
			require.NoError(t, e.super.QueryRow(`SELECT state, rows, unit_digest, chain_digest FROM `+e.id.Schema+`.besdk_lifecycle_units
			  WHERE table_name = 'widget_ledger' AND unit_key = 'widget_ledger_2026m10'`).Scan(&state, &rows, &unitDigest, &chain))
			require.Equal(t, "SEALED", state)
			require.Equal(t, int64(2), rows)
			var want []byte
			e.scalar(t, &want, `SELECT sha256(convert_to(string_agg(concat_ws(chr(31), id, created_at, legal_entity_id, widget_id,
			  entry_no, posting_date, fiscal_period, currency, amount), chr(30) ORDER BY id, created_at), 'UTF8')) FROM widget_ledger_2026m10`)
			require.Equal(t, hex.EncodeToString(want), hex.EncodeToString(unitDigest), "canonical digest, computed independently in SQL")
			require.Equal(t, lifecycle.Chain(nil, unitDigest), chain, "the table's first link")

			var logged int
			e.scalar(t, &logged, `SELECT count(*) FROM besdk_lifecycle_log WHERE action = 'sealed' AND unit_key = 'widget_ledger_2026m10'`)
			require.Equal(t, 1, logged)
			require.Empty(t, step(t, e, en, time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)).Sealed, "sealing is idempotent")

			requireSealed(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE besdk_lifecycle_log SET actor = 'x'`)
				return err
			}))
			requireSealed(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
				_, err := tx.ExecContext(ctx, `DELETE FROM besdk_lifecycle_log`)
				return err
			}))
		})
	}
}

// ownerStore is a store logged in as the owner, for statements the runtime role may not run.
func ownerStore(t *testing.T, e *env) *pg.Store {
	t.Helper()
	p, err := pg.OpenPool(pg.PoolConfig{Host: e.id.Host, Port: e.id.Port, Database: e.id.Database, User: e.id.Owner,
		Password: func() string { return e.id.OwnerPassword }, SSLMode: "disable", MaxConns: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return pg.NewStore(p, pg.StoreConfig{ComponentID: componentID, Role: e.id.Owner, Schema: e.id.Schema})
}

func insertWidget(t *testing.T, e *env, at time.Time, status string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO widgets (id, created_at, legal_entity_id, number, document_date, kind_code, name,
		  region, owner_id, status, currency, price, quantity, amount, version, updated_at)
		  VALUES ($1, $2, 'LE01', 'W-1', '2026-10-03', 'STD', 'w', 'east', 'u1', $3, 'CNY', 1, 1, 1, 1, $2)`, id, at, status)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO widget_lines (id, created_at, widget_id, line_no, description, quantity)
		  VALUES ($1, $2, $3, 1, 'line', 1)`, uuid.Must(uuid.NewV7()), at, id)
		return err
	}))
	return id
}

// `18mo after closed`: a unit with an open row is BLOCKED naming it; once every row is closed and the
// last close is 18 months past, the unit seals together with its follower (P16 "Seal").
func TestSealAfterClosed(t *testing.T) {
	e := newEnv(t, "16", widgetFS(t), true)
	open := insertWidget(t, e, day0, "DRAFT")
	var pub published
	en := engine(t, e, "on", &pub)
	june2028 := time.Date(2028, 6, 1, 0, 0, 0, 0, time.UTC)

	r := step(t, e, en, june2028)
	require.Contains(t, r.Blocked, "widgets_2026m10")
	require.NotContains(t, r.Sealed, "widgets_2026m10")
	var reason string
	e.scalar(t, &reason, `SELECT blocked_reason FROM besdk_lifecycle_units WHERE unit_key = 'widgets_2026m10'`)
	require.Contains(t, reason, open.String())

	require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE widgets SET status = 'APPROVED', updated_at = '2026-12-01Z' WHERE id = $1`, open)
		return err
	}))
	r = step(t, e, en, june2028.Add(-time.Hour))
	require.NotContains(t, r.Sealed, "widgets_2026m10", "closed 2026-12-01: sealable from 2028-06-01")
	r = step(t, e, en, june2028)
	require.Contains(t, r.Sealed, "widgets_2026m10")
	require.Contains(t, r.Sealed, "widget_lines_2026m10", "a follower seals with its parent")
	requireSealed(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM widget_lines WHERE widget_id = $1`, open)
		return err
	}))
}

var periodFS = fstest.MapFS{
	"1_gl.up.sql": {Data: []byte(`CREATE TABLE gl_entries (id uuid NOT NULL, created_at timestamptz NOT NULL, period text NOT NULL,
	  amount numeric(19,4) NOT NULL, flag boolean, PRIMARY KEY (id, period)) PARTITION BY LIST (period);`)},
	"1_gl.down.sql":  {Data: []byte(`DROP TABLE gl_entries;`)},
	"lifecycle.yaml": {Data: []byte("lifecycle: v1\ntables:\n  gl_entries:\n    class: ledger\n    partition: {by: period, kind: list, opened_by: command}\n    tiers: {seal: on_signal}\n")},
}

// on_signal: a list partition opened by a command and sealed by the business transaction (tx.Seal);
// the engine's step never seals it by itself. Booleans are digested in their text output form (t).
func TestSealOnSignal(t *testing.T) {
	e := newEnv(t, "16", periodFS, true)
	var pub published
	en := engine(t, e, "on", &pub)
	require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		name, err := en.EnsureListPartition(ctx, tx, "gl_entries", "2026-P07", day0)
		require.Equal(t, "gl_entries_2026_p07", name)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO gl_entries VALUES ($1, $2, '2026-P07', 10, true), ($3, $2, '2026-P07', 5, NULL)`,
			uuid.Must(uuid.NewV7()), day0, uuid.Must(uuid.NewV7()))
		return err
	}))
	require.Empty(t, step(t, e, en, day0.AddDate(1, 0, 0)).Sealed)

	require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		return en.Seal(ctx, tx, "gl_entries", "2026-P07", day0)
	}))
	requireSealed(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE gl_entries SET amount = 0`)
		return err
	}))
	var digest []byte
	e.scalar(t, &digest, `SELECT unit_digest FROM besdk_lifecycle_units WHERE unit_key = 'gl_entries_2026_p07' AND state = 'SEALED'`)
	var want []byte
	e.scalar(t, &want, `SELECT sha256(convert_to(string_agg(concat_ws(chr(31), id, created_at, period, amount,
	  CASE WHEN flag IS NULL THEN '\N' WHEN flag THEN 't' ELSE 'f' END), chr(30) ORDER BY id, period), 'UTF8')) FROM gl_entries`)
	require.Equal(t, want, digest)
	// (the step a year later also expired the empty outbox partitions; only the seal matters here)
	require.Equal(t, []string{"conformance.widget.lifecycle.sealed.v1 gl_entries/gl_entries_2026_p07"}, pub.of("sealed"))
	require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		return en.Seal(ctx, tx, "gl_entries", "gl_entries_2026_p07", day0)
	}), "sealing again is a no-op, by value or by unit key")
	require.Len(t, pub.of("sealed"), 1)
}

// dry-run plans without acting; off seals and drops nothing; both still keep the window (G1).
func TestModesDryRunAndOff(t *testing.T) {
	e := newEnv(t, "16", widgetFS(t), true)
	insertLedger(t, e, day0, "1")
	nov := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	var pub published
	r := step(t, e, engine(t, e, "dry-run", &pub), nov)
	require.Empty(t, r.Sealed)
	require.Contains(t, r.Planned, "seal widget_ledger_2026m10")
	require.Contains(t, r.Created, "widgets_2027m02", "the window is kept in every mode")
	r = step(t, e, engine(t, e, "off", &pub), nov.AddDate(0, 1, 0))
	require.Empty(t, r.Sealed)
	require.Empty(t, r.Planned)
	require.Contains(t, r.Created, "widgets_2027m03")
	require.Empty(t, pub.subjects())
	var state string
	e.scalar(t, &state, `SELECT state FROM besdk_lifecycle_units WHERE unit_key = 'widget_ledger_2026m10'`)
	require.Equal(t, "ACTIVE", state)
}

// One executor per step and schema (G2): while another transaction holds the step lock, the step is
// skipped and reported busy, never waited for.
func TestStepLockedElsewhereIsBusy(t *testing.T) {
	e := newEnv(t, "16", widgetFS(t), true)
	held, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- e.store.Run(within(t, 30*time.Second), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
			if ok, err := tx.TryLock(ctx, lifecycle.Actor, "ensure"); err != nil || !ok {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	var pub published
	r := step(t, e, engine(t, e, "on", &pub), day0.AddDate(0, 2, 0))
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, []string{"ensure"}, r.Busy)
	require.Empty(t, r.Created)
}
