package lifecycle

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// unitRow is a besdk_lifecycle_units row as the seal step reads it.
type unitRow struct {
	Key      string
	From, To sql.NullTime
	Value    sql.NullString
	State    string
}

// sealable is the set of states a unit may be sealed from.
func sealable(state string) bool { return state == "ACTIVE" || state == "BLOCKED" }

// sealStep evaluates every range-partitioned table sealed `immediate` or `<n> after created|closed`
// (followers seal with their parent; on_signal only through Seal; an anchor fiscal_year_end needs the
// calendar hook and is not evaluated by this SDK version). One transaction per unit.
func (e *Engine) sealStep(ctx context.Context, run RunFunc, now time.Time, r *Report) error {
	var errs []error
	for _, name := range e.decl.Names() {
		t := e.decl.Tables[name]
		if t.Follows != "" || !t.Partition.IsRange() || !stepSeals(t.Seal) {
			continue
		}
		var units []unitRow
		err := run(ctx, e.o.StepLockTimeout, func(ctx context.Context, tx Tx) error {
			var err error
			units, err = dueUnits(ctx, tx, name, now)
			return err
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, u := range units {
			errs = append(errs, e.locked(ctx, run, r, "seal", func(ctx context.Context, tx Tx) error {
				return e.sealCandidate(ctx, tx, t, u, now, r)
			}))
		}
	}
	return errors.Join(errs...)
}

func stepSeals(s Seal) bool {
	return s.Kind == SealImmediate || (s.Kind == SealAfter && (s.After.Anchor == "created" || s.After.Anchor == "closed"))
}

func dueUnits(ctx context.Context, tx Tx, table string, now time.Time) ([]unitRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT unit_key, range_from, range_to, list_value, state FROM besdk_lifecycle_units
	 WHERE table_name = $1 AND state IN ('ACTIVE', 'BLOCKED') AND range_to IS NOT NULL AND range_to <= $2
	 ORDER BY range_from, unit_key`, table, now)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []unitRow
	for rows.Next() {
		var u unitRow
		if err := rows.Scan(&u.Key, &u.From, &u.To, &u.Value, &u.State); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// sealCandidate decides one due unit: wait, block (an open row, or a guard refusing) or seal.
//
// Decision tree: immediate → seal; `n after created` → seal once range end + n has passed;
// `n after closed` → an open row blocks (reason names up to 100 ids), otherwise seal once the last
// close (range end for an empty unit) + n has passed. A unit that waits is not blocked any more.
// dry-run reports instead of writing.
func (e *Engine) sealCandidate(ctx context.Context, tx Tx, t *Table, u unitRow, now time.Time, r *Report) error {
	due, reason, err := e.decide(ctx, tx, t, u, now)
	if err == nil && due && t.Guard != "" {
		if gerr := e.o.Guards[t.Guard](ctx, tx, t.Name, u.Key); gerr != nil {
			due, reason = false, "GUARD "+t.Guard+": "+gerr.Error()
		}
	}
	switch {
	case err != nil:
		return err
	case e.o.Config.Mode == ModeDryRun:
		if due {
			r.Planned = append(r.Planned, "seal "+u.Key)
		} else if reason != "" {
			r.Planned = append(r.Planned, "block "+u.Key)
		}
		return nil
	case due:
		sealed, err := e.sealUnit(ctx, tx, t, u.Key, now)
		r.Sealed = append(r.Sealed, sealed...)
		return err
	case reason != "":
		r.Blocked = append(r.Blocked, u.Key)
		return e.block(ctx, tx, t.Name, u, reason, now)
	case u.State == "BLOCKED":
		_, err := tx.ExecContext(ctx, `UPDATE besdk_lifecycle_units SET state = 'ACTIVE', blocked_reason = NULL,
		  updated_at = now() WHERE table_name = $1 AND unit_key = $2`, t.Name, u.Key)
		return err
	}
	return nil
}

func (e *Engine) decide(ctx context.Context, tx Tx, t *Table, u unitRow, now time.Time) (bool, string, error) {
	if t.Seal.Kind == SealImmediate {
		return true, "", nil
	}
	anchor := u.To.Time
	if t.Seal.After.Anchor == "closed" {
		open, ids, err := openRows(ctx, tx, t, u.Key)
		if err != nil || open > 0 {
			return false, fmt.Sprintf("OPEN_ROWS: %d rows not closed; first ids: %s", open, strings.Join(ids, ",")), err
		}
		last, err := lastClosed(ctx, tx, t, u.Key)
		if err != nil {
			return false, "", err
		}
		if last.Valid {
			anchor = last.Time
		}
	}
	return !t.Seal.After.AddTo(anchor).After(now), "", nil
}

// openRows counts a partition's rows that are not closed and returns up to 100 of their ids.
func openRows(ctx context.Context, tx Tx, t *Table, part string) (int64, []string, error) {
	cond := fmt.Sprintf("%[1]s IS NULL OR %[1]s::text <> ALL($1::text[])", quoteIdent(t.Closed.Column))
	var n int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM `+quoteIdent(part)+` WHERE `+cond, t.Closed.In).Scan(&n); err != nil || n == 0 {
		return n, nil, err
	}
	pk, err := primaryKeyOf(ctx, tx, t.Name)
	if err != nil || len(pk) == 0 {
		return n, nil, err
	}
	ids, err := strings1(ctx, tx, `SELECT `+quoteIdent(pk[0])+`::text FROM `+quoteIdent(part)+` WHERE `+cond+` ORDER BY 1 LIMIT 100`, t.Closed.In)
	return n, ids, err
}

func lastClosed(ctx context.Context, tx Tx, t *Table, part string) (sql.NullTime, error) {
	var last sql.NullTime
	err := tx.QueryRowContext(ctx, `SELECT max(`+quoteIdent(t.Closed.At)+`) FROM `+quoteIdent(part)).Scan(&last)
	return last, err
}

func (e *Engine) block(ctx context.Context, tx Tx, table string, u unitRow, reason string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `UPDATE besdk_lifecycle_units SET state = 'BLOCKED', blocked_reason = $3,
	  updated_at = now() WHERE table_name = $1 AND unit_key = $2`, table, u.Key, reason); err != nil {
		return err
	}
	e.log.Warn("lifecycle unit blocked", "table", table, "unit", u.Key, "reason", reason)
	if u.State == "BLOCKED" {
		return nil
	}
	return logAction(ctx, tx, now, table, u.Key, "blocked", Actor, map[string]any{"reason": reason})
}

// Seal seals one unit of an on_signal table inside the caller's business transaction (tx.Seal, e.g.
// erp/finance locking a period). unit is the unit key (partition name) or a list partition's value.
// Sealing a sealed unit is a no-op. The declared guard runs first. It works in every DATA_LIFECYCLE
// mode: it is the component's own integrity rule, not an engine decision.
func (e *Engine) Seal(ctx context.Context, tx Tx, table, unit string, now time.Time) error {
	t, ok := e.decl.Tables[table]
	if !ok || t.Follows != "" || t.Seal.Kind != SealOnSignal {
		return fmt.Errorf("lifecycle: %s is not a declared table sealed on_signal", table)
	}
	var key string
	err := tx.QueryRowContext(ctx, `SELECT unit_key FROM besdk_lifecycle_units WHERE table_name = $1
	  AND (unit_key = $2 OR list_value = $2) ORDER BY unit_key = $2 DESC LIMIT 1`, table, unit).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("lifecycle: %s has no unit %s", table, unit)
	}
	if err != nil {
		return err
	}
	if t.Guard != "" {
		if err := e.o.Guards[t.Guard](ctx, tx, table, key); err != nil {
			return err
		}
	}
	_, err = e.sealUnit(ctx, tx, t, key, now)
	return err
}

// sealUnit seals a unit of t and the same unit of each follower, and returns the units it sealed.
func (e *Engine) sealUnit(ctx context.Context, tx Tx, t *Table, key string, now time.Time) ([]string, error) {
	u, ok, err := e.sealOne(ctx, tx, t.Name, key, now)
	if err != nil || !ok {
		return nil, err
	}
	sealed := []string{key}
	for _, f := range e.decl.Followers(t.Name) {
		var fkey string
		err := tx.QueryRowContext(ctx, `SELECT unit_key FROM besdk_lifecycle_units WHERE table_name = $1
		  AND ((range_from = $2 AND range_to = $3) OR list_value = $4)`, f, u.From, u.To, u.Value).Scan(&fkey)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return sealed, err
		}
		if _, ok, err := e.sealOne(ctx, tx, f, fkey, now); err != nil {
			return sealed, err
		} else if ok {
			sealed = append(sealed, fkey)
		}
	}
	return sealed, nil
}

// sealOne: lock the unit's chain and its partition against writes, digest it, link it, install the
// guard, record SEALED, log and announce it (P16 "Seal", G5, G6, G11). ok is false when the unit was
// sealed already.
func (e *Engine) sealOne(ctx context.Context, tx Tx, table, key string, now time.Time) (unitRow, bool, error) {
	u := unitRow{Key: key}
	if err := tx.Lock(ctx, Actor+".seal", table); err != nil {
		return u, false, err
	}
	err := tx.QueryRowContext(ctx, `SELECT range_from, range_to, list_value, state FROM besdk_lifecycle_units
	  WHERE table_name = $1 AND unit_key = $2 FOR UPDATE`, table, key).Scan(&u.From, &u.To, &u.Value, &u.State)
	if err != nil || !sealable(u.State) {
		return u, false, err
	}
	if _, err := tx.ExecContext(ctx, `LOCK TABLE `+quoteIdent(key)+` IN SHARE MODE`); err != nil {
		return u, false, err
	}
	d, err := digestPartition(ctx, tx, table, key)
	if err != nil {
		return u, false, err
	}
	var prev []byte
	err = tx.QueryRowContext(ctx, `SELECT chain_digest FROM besdk_lifecycle_units WHERE table_name = $1 AND sealed_at IS NOT NULL
	  ORDER BY sealed_at DESC, range_from DESC NULLS LAST, unit_key DESC LIMIT 1`, table).Scan(&prev)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return u, false, err
	}
	chain := Chain(prev, d.Digest)
	if _, err := tx.ExecContext(ctx, `SELECT besdk_seal_table($1)`, key); err != nil {
		return u, false, err
	}
	var version int64
	if err := tx.QueryRowContext(ctx, `UPDATE besdk_lifecycle_units SET state = 'SEALED', rows = $3, min_id = $4, max_id = $5,
	  unit_digest = $6, chain_digest = $7, sealed_at = $8, blocked_reason = NULL, version = version + 1, updated_at = now()
	  WHERE table_name = $1 AND unit_key = $2 RETURNING version`,
		table, key, d.Rows, d.MinID, d.MaxID, d.Digest, chain, now).Scan(&version); err != nil {
		return u, false, err
	}
	payload := map[string]any{"table": table, "unit": key, "rows": d.Rows, "unit_digest": hex.EncodeToString(d.Digest),
		"chain_digest": hex.EncodeToString(chain), "sealed_at": now.UTC().Format(time.RFC3339Nano)}
	addBounds(payload, u)
	if err := logAction(ctx, tx, now, table, key, "sealed", Actor, payload); err != nil {
		return u, false, err
	}
	return u, true, e.o.Publish(ctx, tx, e.event("sealed", table, key, version, payload))
}

func addBounds(payload map[string]any, u unitRow) {
	if u.From.Valid {
		payload["range_from"] = u.From.Time.UTC().Format(time.RFC3339)
		payload["range_to"] = u.To.Time.UTC().Format(time.RFC3339)
	}
	if u.Value.Valid {
		payload["list_value"] = u.Value.String
	}
}
