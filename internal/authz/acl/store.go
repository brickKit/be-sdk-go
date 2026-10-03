package acl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/pg"
)

// Querier is what Load needs: *pg.Tx (sqlc's DBTX) satisfies it.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Load reads the projection rows of one record (P6.12), for the single-record decision (E10): every
// relation and subject, expired rows included; the evaluator compares expiry with its own clock (E3).
// Rows come sorted by relation and subject.
func Load(ctx context.Context, q Querier, rtype, rid string) ([]authz.ACLRow, error) {
	m, err := LoadMany(ctx, q, rtype, []string{rid})
	return m[rid], err
}

// LoadMany is Load for several records of one type in one query (row actions, _authz/check batches):
// the rows of each record by id; a record without rows has no entry.
func LoadMany(ctx context.Context, q Querier, rtype string, rids []string) (map[string][]authz.ACLRow, error) {
	rows, err := q.QueryContext(ctx, `SELECT rid, relation, subject, expires_at FROM besdk_authz_acl
		WHERE rtype = $1::text AND rid = ANY($2::text[]) ORDER BY rid, relation, subject`, rtype, rids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]authz.ACLRow{}
	for rows.Next() {
		r := authz.ACLRow{RType: rtype}
		var exp sql.NullTime
		if err := rows.Scan(&r.RID, &r.Relation, &r.Subject, &exp); err != nil {
			return nil, err
		}
		if exp.Valid {
			at := exp.Time.UTC()
			r.ExpiresAt = &at
		}
		out[r.RID] = append(out[r.RID], r)
	}
	return out, rows.Err()
}

// readCursor is the scope's revision, 0 when there is no cursor row yet.
func (p *Projection) readCursor(ctx context.Context) (int64, error) {
	var rev int64
	err := p.cfg.Store.Run(ctx, pg.TxOptions{ReadOnly: true}, func(ctx context.Context, tx *pg.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT revision FROM besdk_authz_cursor WHERE scope = $1::text`, p.scope).Scan(&rev)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	})
	return rev, err
}

// lockCursor creates the scope's cursor row when missing and locks it for the transaction, so replicas
// that share the schema apply pages one after another.
func (p *Projection) lockCursor(ctx context.Context, tx *pg.Tx) (int64, error) {
	if _, err := tx.ExecContext(ctx, `INSERT INTO besdk_authz_cursor (scope, revision) VALUES ($1::text, 0)
		ON CONFLICT (scope) DO NOTHING`, p.scope); err != nil {
		return 0, err
	}
	var rev int64
	err := tx.QueryRowContext(ctx, `SELECT revision FROM besdk_authz_cursor WHERE scope = $1::text FOR UPDATE`, p.scope).Scan(&rev)
	return rev, err
}

// apply writes one page and moves the cursor to max(cursor, to) in one transaction. Changes at or
// below the locked cursor were applied already (by another replica) and are skipped, so an older page
// never undoes a newer change. It returns the cursor after the transaction.
func (p *Projection) apply(ctx context.Context, changes []change, to int64) (int64, error) {
	var result int64
	err := p.cfg.Store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		cur, err := p.lockCursor(ctx, tx)
		if err != nil {
			return err
		}
		for _, c := range changes {
			if c.rev <= cur || !slices.Contains(p.types, c.row.RType) {
				continue
			}
			if err := applyChange(ctx, tx, c); err != nil {
				return err
			}
		}
		result = max(cur, to)
		_, err = tx.ExecContext(ctx, `UPDATE besdk_authz_cursor SET revision = $2::bigint WHERE scope = $1::text`, p.scope, result)
		return err
	})
	return result, err
}

func applyChange(ctx context.Context, tx *pg.Tx, c change) error {
	r := c.row
	if c.del {
		_, err := tx.ExecContext(ctx, `DELETE FROM besdk_authz_acl WHERE rtype = $1::text AND rid = $2::text
			AND relation = $3::text AND subject = $4::text`, r.RType, r.RID, r.Relation, r.Subject)
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO besdk_authz_acl (rtype, rid, relation, subject, expires_at, revision)
		VALUES ($1::text, $2::text, $3::text, $4::text, $5::timestamptz, $6::bigint)
		ON CONFLICT (rtype, rid, relation, subject) DO UPDATE SET expires_at = EXCLUDED.expires_at, revision = EXCLUDED.revision`,
		r.RType, r.RID, r.Relation, r.Subject, r.ExpiresAt, c.rev)
	return err
}

// snapshot is one type's tuples at one revision.
type snapshot struct {
	rows     []authz.ACLRow
	revision int64
}

// rebuild replaces the projection of every pulled type with the provider's snapshot (P6.12, after a
// 410) and sets the cursor to the lowest snapshot revision, so the changes pulled next bring every type
// to the same point. It changes nothing when another replica moved the cursor away from after.
func (p *Projection) rebuild(ctx context.Context, after int64) error {
	snaps := make([]snapshot, 0, len(p.types))
	for _, t := range p.types {
		s, err := p.walk(ctx, t)
		if err != nil {
			return err
		}
		snaps = append(snaps, s)
	}
	low := snaps[0].revision
	for _, s := range snaps[1:] {
		low = min(low, s.revision)
	}
	err := p.cfg.Store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		cur, err := p.lockCursor(ctx, tx)
		if err != nil || cur != after {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM besdk_authz_acl WHERE rtype = ANY($1::text[])`, p.types); err != nil {
			return err
		}
		for _, s := range snaps {
			if err := insertRows(ctx, tx, s); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `UPDATE besdk_authz_cursor SET revision = $2::bigint, rebuilt_at = now()
			WHERE scope = $1::text`, p.scope, low)
		return err
	})
	if err == nil {
		p.log.InfoContext(ctx, "authz projection rebuilt from the snapshot", "types", p.scope, "revision", low)
	}
	return err
}

// walk reads every page of one type's snapshot; all pages must carry the same revision.
func (p *Projection) walk(ctx context.Context, typ string) (snapshot, error) {
	s := snapshot{revision: -1}
	cursor := ""
	for {
		body, err := p.get(ctx, p.tuplesURL(typ, cursor))
		if err != nil {
			return s, err
		}
		page, err := decodeTuples(body)
		if err != nil {
			return s, err
		}
		if s.revision >= 0 && page.revision != s.revision {
			return s, fmt.Errorf("acl: snapshot of %s moved from revision %d to %d mid-walk", typ, s.revision, page.revision)
		}
		s.revision = page.revision
		for _, r := range page.rows {
			if r.RType == typ {
				s.rows = append(s.rows, r)
			}
		}
		if page.cursor == "" {
			return s, nil
		}
		cursor = page.cursor
	}
}

// insertRows writes a snapshot in batches of 1000 rows.
func insertRows(ctx context.Context, tx *pg.Tx, s snapshot) error {
	const batch = 1000
	for start := 0; start < len(s.rows); start += batch {
		part := s.rows[start:min(start+batch, len(s.rows))]
		cols := [4][]string{}
		exp := make([]*time.Time, len(part))
		revs := make([]int64, len(part))
		for i, r := range part {
			cols[0], cols[1] = append(cols[0], r.RType), append(cols[1], r.RID)
			cols[2], cols[3] = append(cols[2], r.Relation), append(cols[3], r.Subject)
			exp[i], revs[i] = r.ExpiresAt, s.revision
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO besdk_authz_acl (rtype, rid, relation, subject, expires_at, revision)
			SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[], $6::bigint[])
			ON CONFLICT (rtype, rid, relation, subject) DO UPDATE SET expires_at = EXCLUDED.expires_at, revision = EXCLUDED.revision`,
			cols[0], cols[1], cols[2], cols[3], exp, revs); err != nil {
			return err
		}
	}
	return nil
}
