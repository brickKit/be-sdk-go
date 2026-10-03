package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Ensured says what one window pass did: partitions it created, existing partitions with other names
// it adopted by their bounds, and window units it skipped because an existing partition overlaps
// them partially (the caller logs those at WARN).
type Ensured struct {
	Created, Adopted, Skipped []string
}

// EnsureWindows creates the current window of the outbox and of every range-partitioned table of d
// (nil d = the outbox only), followers in the same pass with their parent's bounds (P11.3, P16.6, G1).
// It works for the owner in the migration step and for the runtime role, through the platform's
// SECURITY DEFINER functions (P10.12). Partitions are matched by bounds, never by name. A partition of
// a table sealed `immediate` gets the seal guard at once; every business partition has an ACTIVE row
// in besdk_lifecycle_units; every creation is logged in besdk_lifecycle_log with actor (P16.7).
// It is idempotent.
func EnsureWindows(ctx context.Context, q Querier, d *Declaration, now time.Time, actor string) (Ensured, error) {
	var out Ensured
	w := windower{q: q, now: now, actor: actor, out: &out}
	if err := w.table(ctx, OutboxWindow(now, OutboxAhead), false, false); err != nil {
		return out, err
	}
	if d == nil {
		return out, nil
	}
	for _, name := range d.Names() {
		t := d.Tables[name]
		if !t.Partition.IsRange() || t.Follows != "" {
			continue
		}
		units := RangeWindow(name, t.Partition.Grain, now, t.Partition.Ahead)
		if err := w.table(ctx, units, t.Seal.Kind == SealImmediate, true); err != nil {
			return out, err
		}
		for _, f := range d.Followers(name) {
			if err := w.table(ctx, followerUnits(f, units), d.Tables[f].Seal.Kind == SealImmediate, true); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

func followerUnits(follower string, parent []Unit) []Unit {
	out := make([]Unit, len(parent))
	for i, u := range parent {
		// the parent's grain: the follower's unit key carries the same suffix (P16.10)
		out[i] = Unit{Table: follower, Name: follower + strings.TrimPrefix(u.Name, u.Table), From: u.From, To: u.To}
	}
	return out
}

type windower struct {
	q     Querier
	now   time.Time
	actor string
	out   *Ensured
}

// table ensures one table's window units, all of the same parent.
func (w windower) table(ctx context.Context, units []Unit, guard, track bool) error {
	if len(units) == 0 {
		return nil
	}
	parts, err := listPartitions(ctx, w.q, units[0].Table)
	if err != nil {
		return err
	}
	for _, u := range units {
		exact, overlap := match(parts, u.From, u.To)
		name := u.Name
		switch {
		case exact != nil:
			name = exact.Name
			if name != u.Name {
				w.out.Adopted = append(w.out.Adopted, name)
			}
		case overlap:
			w.out.Skipped = append(w.out.Skipped, u.Name)
			continue
		default:
			var created bool
			if err := w.q.QueryRowContext(ctx, `SELECT besdk_ensure_range_partition($1, $2, $3, $4)`,
				u.Table, u.Name, u.From, u.To).Scan(&created); err != nil {
				return fmt.Errorf("lifecycle: create partition %s: %w", u.Name, err)
			}
			if created {
				w.out.Created = append(w.out.Created, u.Name)
				detail := map[string]any{"partition": u.Name, "from": u.From, "to": u.To}
				if err := logAction(ctx, w.q, w.now, u.Table, u.Name, "created", w.actor, detail); err != nil {
					return err
				}
			}
		}
		if err := w.track(ctx, u.Table, name, guard, track, u.From, u.To); err != nil {
			return err
		}
	}
	return nil
}

// track installs the immediate seal guard and records the unit as ACTIVE when it is new.
func (w windower) track(ctx context.Context, table, name string, guard, track bool, from, to time.Time) error {
	if guard {
		if _, err := w.q.ExecContext(ctx, `SELECT besdk_seal_table($1)`, name); err != nil {
			return fmt.Errorf("lifecycle: guard %s: %w", name, err)
		}
	}
	if !track {
		return nil
	}
	_, err := w.q.ExecContext(ctx, `INSERT INTO besdk_lifecycle_units (table_name, unit_key, range_from, range_to, state)
	  VALUES ($1, $2, $3, $4, 'ACTIVE') ON CONFLICT (table_name, unit_key) DO NOTHING`, table, name, from, to)
	return err
}

// logAction appends one row to the append-only besdk_lifecycle_log (P16.7, G11).
func logAction(ctx context.Context, q Querier, at time.Time, table, unit, action, actor string, detail map[string]any) error {
	b, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO besdk_lifecycle_log (at, table_name, unit_key, action, actor, detail)
	  VALUES ($1, $2, $3, $4, $5, $6::jsonb)`, at, table, unit, action, actor, string(b))
	if err != nil {
		return fmt.Errorf("lifecycle: log %s %s: %w", action, unit, err)
	}
	return nil
}

var listSuffix = regexp.MustCompile(`[^a-z0-9_]+`)

// listName is a LIST partition's name: <table>_<value lowered, every other character run as _>.
func listName(table, value string) string {
	return table + "_" + strings.Trim(listSuffix.ReplaceAllString(strings.ToLower(value), "_"), "_")
}

// EnsureListPartition opens the LIST partition of table for value, and its followers', through
// besdk_ensure_list_partition (a command such as erp/finance's open-fiscal-year calls it in its own
// transaction; P16.6). An existing partition for the value is adopted whatever its name. It returns
// the table's partition name; it is idempotent.
func EnsureListPartition(ctx context.Context, q Querier, d *Declaration, table, value string, now time.Time, actor string) (string, error) {
	t, ok := d.Tables[table]
	if !ok || !t.Partition.IsList() || t.Follows != "" {
		return "", fmt.Errorf("lifecycle: %s is not a declared list-partitioned table", table)
	}
	if strings.Trim(listSuffix.ReplaceAllString(strings.ToLower(value), "_"), "_") == "" {
		return "", fmt.Errorf("lifecycle: list value %q gives no partition name", value)
	}
	name, err := ensureList(ctx, q, table, value, t.Seal.Kind == SealImmediate, now, actor)
	if err != nil {
		return "", err
	}
	for _, f := range d.Followers(table) {
		if _, err := ensureList(ctx, q, f, value, d.Tables[f].Seal.Kind == SealImmediate, now, actor); err != nil {
			return "", err
		}
	}
	return name, nil
}

func ensureList(ctx context.Context, q Querier, table, value string, guard bool, now time.Time, actor string) (string, error) {
	parts, err := listPartitions(ctx, q, table)
	if err != nil {
		return "", err
	}
	name := ""
	for _, p := range parts {
		if p.Value.Valid && strings.ReplaceAll(p.Value.String, "''", "'") == value {
			name = p.Name
		}
	}
	if name == "" {
		name = listName(table, value)
		var created bool
		if err := q.QueryRowContext(ctx, `SELECT besdk_ensure_list_partition($1, $2, $3)`, table, name, value).Scan(&created); err != nil {
			return "", fmt.Errorf("lifecycle: create partition %s: %w", name, err)
		}
		if err := logAction(ctx, q, now, table, name, "created", actor, map[string]any{"partition": name, "value": value}); err != nil {
			return "", err
		}
	}
	if guard {
		if _, err := q.ExecContext(ctx, `SELECT besdk_seal_table($1)`, name); err != nil {
			return "", fmt.Errorf("lifecycle: guard %s: %w", name, err)
		}
	}
	_, err = q.ExecContext(ctx, `INSERT INTO besdk_lifecycle_units (table_name, unit_key, list_value, state)
	  VALUES ($1, $2, $3, 'ACTIVE') ON CONFLICT (table_name, unit_key) DO NOTHING`, table, name, value)
	return name, err
}
