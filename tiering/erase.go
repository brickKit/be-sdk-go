package tiering

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/jackc/pgx/v5"
)

// Erasing one aggregate's personal data (be-protocol P15.31; the assembly protocol 07's erasure): the class's
// erasable columns of its root and children become null. Where it is depends on its unit: still online (no unit, or
// an open one) the caller erases its own rows; a sealed unit's online rows are erased and its digest computed again
// in the same transaction (a transfer under way then fails to publish and starts again, TR27); a published or
// released unit is written again as a new manifest version on its route, its state unchanged, the thawed copy
// dropped. A live hold over the unit defers it with an alert (TR15). The evidence (category erasure) is written in
// two phases, as a destruction's.

// Erasure outcomes and the SDK's job that redoes an erasure a full recovery replayed.
const (
	CategoryErasure = "erasure"
	ErasedOnline    = "online"   // not in a unit past open: the caller erases its rows
	ErasureDeferred = "deferred" // a live hold covers it: alerted, done once lifted
	Erased          = "erased"
	EraseJob        = "be.tiering.erase"
)

var errHeld = errors.New("tiering: a live hold covers the unit")

// eraseAttempts bounds how many times an erasure starts again when the unit moved on under it (ErrStaleClaim).
const eraseAttempts = 5

// Erase erases one aggregate's erasable columns for a basis (the request's reference); it answers the outcome.
func (c *Catalog) Erase(ctx context.Context, class, root, basis string) (string, error) {
	return c.erase(ctx, class, root, basis, false)
}

// erase is Erase; redo is a replayed erasure's re-execution: no hold check, no new evidence (P12.21).
func (c *Catalog) erase(ctx context.Context, class, root, basis string, redo bool) (string, error) {
	cl, err := c.declared(class)
	if err != nil {
		return "", err
	}
	if len(cl.Erasable) == 0 {
		return "", errs.New(errs.ReasonRequestInvalid, class+" declares no erasable columns")
	}
	var unit string
	var u unitFacts
	var holds int
	err = c.tx(ctx, "erase.find", func(ctx context.Context, tx db.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT unit::text FROM be_tier_member WHERE class = $1 AND root_id = $2", class, root).
			Scan(&unit); err != nil {
			return err
		}
		if u, err = readUnit(ctx, tx, unit, false); err != nil {
			return err
		}
		holds, err = holdsOn(ctx, tx, unit, class)
		return err
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows), err == nil && u.state == StateOpen:
		return ErasedOnline, nil
	case err != nil:
		return "", err
	case u.state == StateDestroyed:
		return Erased, nil // nothing of it is left
	case holds > 0 && !redo:
		c.deferred(unit, class, root)
		return ErasureDeferred, nil
	}
	var p *evidence.Pending
	if !redo {
		if c.Evidence == nil {
			return "", errors.New("tiering: no evidence writer: an erasure is never done without its evidence (P15.31)")
		}
		if p, err = c.Evidence.Intent(ctx, CategoryErasure, "", evidence.Object{Type: "be.tier.record", ID: class + "/" + root,
			Version: strconv.Itoa(u.manifest)}, map[string]any{"class": class, "root": root, "unit": unit,
			"digest": u.digest, "basis": basis, "columns": append([]any(nil), anyOf(cl.Erasable)...)}); err != nil {
			return "", err
		}
	}
	conclude := func(ctx context.Context, tx db.Tx) error {
		if redo {
			return nil
		}
		if _, err := tx.Exec(ctx, "LOCK TABLE be_tier_hold IN SHARE MODE"); err != nil {
			return err
		}
		if n, err := holdsOn(ctx, tx, unit, class); err != nil || n > 0 {
			return errors.Join(err, errHeld)
		}
		return p.Effective(ctx, tx)
	}
	gone := false
	for attempt := 1; ; attempt++ {
		if u.state == StateSealed {
			err = c.tx(ctx, "erase.sealed", func(ctx context.Context, tx db.Tx) error { return c.eraseSealed(ctx, tx, cl, unit, root, conclude) })
		} else {
			err = c.eraseAgain(ctx, cl, unit, root, conclude)
		}
		if !errors.Is(err, ErrStaleClaim) || attempt == eraseAttempts {
			break
		}
		// the unit moved on meanwhile (published, or written again by a transfer, a route change or another erasure):
		// the erasure starts again from its state now; the intent stays the one written
		if err = c.tx(ctx, "erase.find", func(ctx context.Context, tx db.Tx) (err error) {
			u, err = readUnit(ctx, tx, unit, false)
			return err
		}); err != nil {
			break
		}
		if u.state == StateDestroyed { // nothing of it is left to erase
			gone = true
			break
		}
	}
	if gone && p != nil {
		err = p.Cancelled(context.WithoutCancel(ctx))
		if err == nil {
			return Erased, nil
		}
	}
	if err != nil && p != nil {
		err = errors.Join(err, p.Cancelled(context.WithoutCancel(ctx)))
	}
	if errors.Is(err, errHeld) {
		c.deferred(unit, class, root)
		return ErasureDeferred, nil
	}
	if err != nil {
		return "", err
	}
	return Erased, nil
}

func anyOf(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

func (c *Catalog) deferred(unit, class, root string) {
	if c.Log != nil {
		c.Log.Error("an erasure is deferred: a live hold covers its unit", "event", "tiering_erasure_deferred",
			"unit", unit, "class", class, "root", root)
	}
}

// VoidExports makes the class's exports that may hold an erased aggregate unreadable (P15.31, assembly project A159):
// the finished ones expire now (their status answers NOT_FOUND, the cleanup deletes their objects); a pending one is
// marked, and its job reads again rather than finishing. It runs in the transaction that erases: the SDK's erasures
// call it; a component erasing its online rows (ErasedOnline) calls it in its own transaction.
func VoidExports(ctx context.Context, tx db.Tx, class string) error {
	if _, err := tx.Exec(ctx, "UPDATE be_tier_export SET expires_at = now() WHERE class = $1 AND state = 'done' "+
		"AND (expires_at IS NULL OR expires_at > now())", class); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "UPDATE be_tier_export SET erased_at = now() WHERE class = $1 AND state = 'pending'", class)
	return err
}

// nullOnline sets an aggregate's erasable columns null in the online tables, naming its unit for the write barrier,
// and voids the class's exports.
func nullOnline(ctx context.Context, tx db.Tx, cl Class, unit, root string) error {
	if _, err := tx.Exec(ctx, "SELECT set_config('be.tier_unit', $1, true)", unit); err != nil {
		return err
	}
	if err := VoidExports(ctx, tx, cl.Name); err != nil {
		return err
	}
	for _, t := range cl.Tables() {
		var set []string
		for _, col := range cl.Erasable {
			if _, ok := t.Columns[col]; ok {
				set = append(set, ident(col)+" = NULL")
			}
		}
		if len(set) == 0 {
			continue
		}
		key := "t." + ident(cl.Root.PK) + "::text"
		if ch, ok := cl.child(t.Name); ok {
			key = cl.rootExpr(ch, "t")
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s AS t SET %s WHERE %s = $1", ident(t.Name), strings.Join(set, ", "),
			key), root); err != nil {
			return err
		}
	}
	return nil
}

// eraseSealed erases a sealed unit's aggregate online and computes the unit's digest again, in one transaction.
func (c *Catalog) eraseSealed(ctx context.Context, tx db.Tx, cl Class, unit, root string, conclude func(context.Context, db.Tx) error) error {
	if _, err := tx.Exec(ctx, "SELECT 1 FROM be_tier_unit WHERE id::text = $1 AND state = 'sealed' FOR UPDATE", unit); err != nil {
		return err
	}
	if err := nullOnline(ctx, tx, cl, unit, root); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, "SELECT root_id FROM be_tier_member WHERE unit::text = $1", unit)
	if err != nil {
		return err
	}
	roots, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	digest, err := c.digest(ctx, tx, cl, roots)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, "UPDATE be_tier_unit SET digest = $2 WHERE id::text = $1 AND state = 'sealed'", unit, digest)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 { // published meanwhile: the caller starts again
		return fmt.Errorf("%w: unit %s left sealed during the erasure", ErrStaleClaim, unit)
	}
	return conclude(ctx, tx)
}

// eraseAgain writes a published or released unit again with the aggregate erased, as a new manifest version on its
// route (the publish rule, P15.15-P15.18); the online rows a published unit still has are erased in the publish.
func (c *Catalog) eraseAgain(ctx context.Context, cl Class, unit, root string, conclude func(context.Context, db.Tx) error) error {
	erasable := map[string]bool{}
	for _, col := range cl.Erasable {
		erasable[col] = true
	}
	null := func(row map[string]any) {
		for col := range row {
			if erasable[col] {
				delete(row, col) // null: absent from the canonical row
			}
		}
	}
	_, err := c.rewrite(ctx, unit, "", func(a *Aggregate) {
		if fmt.Sprint(a.Root[cl.Root.PK]) != root {
			return
		}
		null(a.Root)
		for _, rows := range a.Children {
			for _, r := range rows {
				null(r)
			}
		}
	}, func(ctx context.Context, tx db.Tx, claim Claim) error {
		if _, err := tx.Exec(ctx, "DELETE FROM be_tier_row WHERE unit::text = $1 AND version = -1", unit); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM be_tier_thaw WHERE unit::text = $1", unit); err != nil {
			return err
		}
		if claim.State == StatePublished { // its online rows are still there until released
			if err := nullOnline(ctx, tx, cl, unit, root); err != nil {
				return err
			}
		} else if err := VoidExports(ctx, tx, cl.Name); err != nil {
			return err
		}
		return conclude(ctx, tx)
	})
	return err
}

// EraseJobOf is the queue job that redoes an erasure replayed in a full recovery (P15.30) once the member is open.
func (c *Catalog) EraseJobOf(log *slog.Logger) work.Job {
	return work.Job{Name: EraseJob, Kind: work.Queue, Effect: work.Local, MaxAttempts: work.Unlimited, Timeout: time.Hour,
		Handle: func(ctx context.Context, run *work.Run) error {
			var a struct{ Class, Root, Basis string }
			if err := json.Unmarshal(run.Args, &a); err != nil {
				return work.Permanent(err)
			}
			if _, err := c.erase(ctx, a.Class, a.Root, a.Basis, true); err != nil {
				log.Warn("a replayed erasure is not done yet", "event", "tiering_erasure_redo_failed", "class", a.Class,
					"root", a.Root, "error", err.Error())
				return err
			}
			return run.Complete(ctx, func(context.Context, db.Tx) error { return nil })
		}}
}

// replayErasure re-executes a replayed erasure (P15.30): a sealed unit's rows at once, in the replay's transaction;
// a published or released unit's new version by the erase job once open; online, the online rows.
func (c *Catalog) replayErasure(ctx context.Context, tx db.Tx, rec evidence.Replayed) error {
	class, _ := rec.Change["class"].(string)
	root, _ := rec.Change["root"].(string)
	basis, _ := rec.Change["basis"].(string)
	cl, ok := c.Classes[class]
	if !ok || root == "" {
		return evidence.ErrDangling
	}
	var unit, state string
	err := tx.QueryRow(ctx, "SELECT u.id::text, u.state FROM be_tier_member m JOIN be_tier_unit u ON u.id = m.unit "+
		"WHERE m.class = $1 AND m.root_id = $2 FOR UPDATE OF u", class, root).Scan(&unit, &state)
	switch {
	case errors.Is(err, pgx.ErrNoRows) || (err == nil && state == StateOpen):
		return nullOnlineNoUnit(ctx, tx, cl, root)
	case err != nil:
		return err
	case state == StateDestroyed:
		return nil
	case state == StateSealed:
		return c.eraseSealed(ctx, tx, cl, unit, root, func(context.Context, db.Tx) error { return nil })
	}
	if c.Enqueue == nil {
		return fmt.Errorf("tiering: the erasure of %s/%s needs the erase job", class, root)
	}
	_, err = c.Enqueue(ctx, tx, EraseJob, map[string]string{"class": class, "root": root, "basis": basis})
	return err
}

// nullOnlineNoUnit erases an aggregate that is in no unit past open (the online tables, no barrier).
func nullOnlineNoUnit(ctx context.Context, tx db.Tx, cl Class, root string) error {
	return nullOnline(ctx, tx, cl, "", root)
}
