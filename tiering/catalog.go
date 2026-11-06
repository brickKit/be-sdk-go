package tiering

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/jackc/pgx/v5"
)

// The owner's catalog: candidates and sealing (be-protocol P15.6, P15.11, P15.12). A unit groups a class's aggregates
// by its group columns and the anchor's UTC month; it is created open at its first try, and sealed in one local
// transaction holding FOR UPDATE on the roots it seals, so the write barrier (P15.13) serializes with it.

// WhyEventsPending is the SDK's own blocker: an aggregate whose events are not published yet.
const WhyEventsPending = "events_pending"

// DefaultMaxMembers bounds one unit (its digest is sorted in memory); the rest of a grouping go to the next unit.
const DefaultMaxMembers = 100_000

// Blocker answers, for roots of a class about to be sealed, why each cannot be sealed yet (an absent root has no
// reason): an open reservation, an open period, an unreconciled reference. It runs in the seal's transaction.
type Blocker func(ctx context.Context, tx db.Tx, class string, roots []string) (map[string][]string, error)

// Registry is what a module registers with the tiering (P15.12's blockers).
type Registry struct {
	blockers map[string][]Blocker
}

// NewRegistry is an empty registry.
func NewRegistry() *Registry { return &Registry{blockers: map[string][]Blocker{}} }

// Block registers a blocker of a class.
func (r *Registry) Block(class string, b Blocker) { r.blockers[class] = append(r.blockers[class], b) }

// Participant is implemented by modules that declare classes and have blockers (or later converters and approvals).
type Participant interface {
	Tiering(r *Registry) error
}

// Catalog is one member's catalog over its database.
type Catalog struct {
	DB         *db.DB
	Classes    map[string]Class
	Registry   *Registry
	Residence  map[string]int // the configured residence per class (TIERING_RESIDENCE_<class>), Never for never
	MaxMembers int
	Store      *Store // the cold route's bucket; nil without TIERING_STORE_URL
	// N is the lists' default range and Grace the wait after a publish (TIERING_LIST_DAYS, TIERING_GRACE).
	N, Grace time.Duration
	// Backups tells when the deployment's latest backup finished; nil: never released (no run profile).
	Backups BackupSource
	// Batch bounds the rows a release deletes per table and transaction (default ReleaseBatch).
	Batch int
	// StoreLocked: the store has object lock (no erasable class, no exports).
	StoreLocked bool
	// ColdSyncLimit bounds a synchronous history read and ColdConcurrency the member's cold reads at once
	// (TIERING_COLD_SYNC_LIMIT, TIERING_COLD_CONCURRENCY; 10 s and 4 when zero).
	ColdSyncLimit   time.Duration
	ColdConcurrency int
	// Enqueue queues a job's work item in the transaction (the member's runner); nil: no exports.
	Enqueue func(ctx context.Context, tx db.Tx, job string, args any) (string, error)
	// Evidence writes the destructions' and erasures' evidence (P15.29-P15.31); nil: neither is done.
	Evidence *evidence.Writer
	// Subject is a principal's authorization now (the member's view), for the exports; nil or a nil answer: none.
	Subject func(principal string) *authz.Subject

	mu        sync.Mutex
	readers   map[string]Reader
	slotsOnce sync.Once
	slots     chan struct{}
}

// Grouping is one grouping of a class: its canonical key, the group columns' values (text) and the period.
type Grouping struct {
	Key    string
	Values map[string]string
	Period string // YYYY-MM of the anchor in UTC
}

func (c *Catalog) class(name string) (Class, int, error) {
	cl, ok := c.Classes[name]
	if !ok || !cl.Tierable() {
		return Class{}, 0, fmt.Errorf("tiering: no tierable class %q", name)
	}
	days := cl.ResidenceDays
	if v, ok := c.Residence[name]; ok {
		days = v
	}
	return cl, days, nil
}

func ident(s string) string { return pgx.Identifier{s}.Sanitize() }

// candidates is the WHERE over the root (alias r) that selects a class's sealable candidates: terminal, past the
// residence ($1 days), not a member yet ($2 the class).
func candidates(cl Class) string {
	return fmt.Sprintf("(%s) AND r.%s <= now() - make_interval(days => $1) AND NOT EXISTS (SELECT 1 FROM be_tier_member m "+
		"WHERE m.class = $2 AND m.root_id = r.%s::text)", cl.Terminal, ident(cl.Anchor), ident(cl.Root.PK))
}

func period(cl Class) string {
	return fmt.Sprintf("to_char(r.%s AT TIME ZONE 'UTC', 'YYYY-MM')", ident(cl.Anchor))
}

// GroupKey is a grouping's canonical key (P15.6).
func GroupKey(values map[string]string, period string) (string, error) {
	obj := map[string]any{"period": period}
	for k, v := range values {
		obj["g:"+k] = v
	}
	b, err := canon.Encode(obj)
	return string(b), err
}

// Groupings are the groupings of a class with candidates now (none when its residence is never).
func (c *Catalog) Groupings(ctx context.Context, class string) ([]Grouping, error) {
	cl, days, err := c.class(class)
	if err != nil || days == Never {
		return nil, err
	}
	sel := make([]string, len(cl.Group))
	for i, g := range cl.Group {
		sel[i] = "r." + ident(g) + "::text"
	}
	sel = append(sel, period(cl))
	q := fmt.Sprintf("SELECT DISTINCT %s FROM %s r WHERE %s ORDER BY %d", strings.Join(sel, ", "), ident(cl.Root.Name),
		candidates(cl), len(sel))
	var out []Grouping
	err = c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, q, days, class)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			vals := make([]*string, len(sel))
			ptrs := make([]any, len(sel))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			g := Grouping{Values: map[string]string{}, Period: *vals[len(vals)-1]}
			for i, name := range cl.Group {
				if vals[i] != nil {
					g.Values[name] = *vals[i]
				}
			}
			if g.Key, err = GroupKey(g.Values, g.Period); err != nil {
				return err
			}
			out = append(out, g)
		}
		return rows.Err()
	})
	return out, err
}

// SealResult is one seal's outcome: the unit, and either its members and digest or why it was not sealed.
type SealResult struct {
	Unit    string
	Members int
	Digest  string
	Blocked map[string][]string // root -> reasons; empty when sealed
}

// Seal tries to seal one grouping's candidates (P15.12) in one transaction. Blocked: nothing is sealed, the reasons
// are written on the open unit.
func (c *Catalog) Seal(ctx context.Context, class string, g Grouping) (SealResult, error) {
	cl, days, err := c.class(class)
	if err != nil {
		return SealResult{}, err
	}
	if days == Never {
		return SealResult{}, errors.New("tiering: the class stays online")
	}
	limit := c.MaxMembers
	if limit <= 0 {
		limit = DefaultMaxMembers
	}
	var res SealResult
	err = c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		res = SealResult{}
		// one sealer per grouping at a time
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", class+"/"+g.Key); err != nil {
			return err
		}
		unit, err := openUnit(ctx, tx, class, g.Key)
		if err != nil {
			return err
		}
		res.Unit = unit
		args := []any{days, class, g.Period}
		where := []string{candidates(cl), period(cl) + " = $3"}
		for _, name := range cl.Group {
			if v, ok := g.Values[name]; ok {
				args = append(args, v)
				where = append(where, fmt.Sprintf("r.%s::text = $%d", ident(name), len(args)))
			} else {
				where = append(where, fmt.Sprintf("r.%s IS NULL", ident(name)))
			}
		}
		args = append(args, limit)
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT r.%s::text FROM %s r WHERE %s ORDER BY r.%s LIMIT $%d FOR UPDATE OF r",
			ident(cl.Root.PK), ident(cl.Root.Name), strings.Join(where, " AND "), ident(cl.Root.PK), len(args)), args...)
		if err != nil {
			return err
		}
		roots, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil || len(roots) == 0 {
			return err
		}
		blocked, err := c.blockers(ctx, tx, class, roots)
		if err != nil {
			return err
		}
		if len(blocked) > 0 {
			res.Blocked = blocked
			raw, _ := json.Marshal(blocked)
			// blocked since it came due: the latest anchor among the candidates plus the residence (the alert counts
			// 30 days from there, protocol 11)
			_, err := tx.Exec(ctx, fmt.Sprintf("UPDATE be_tier_unit SET blocked = $2, blocked_since = coalesce(blocked_since, "+
				"(SELECT max(r.%s) FROM %s r WHERE r.%s::text = ANY($3)) + make_interval(days => $4)) WHERE id = $1",
				ident(cl.Anchor), ident(cl.Root.Name), ident(cl.Root.PK)), unit, raw, roots, days)
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO be_tier_member (class, root_id, unit) SELECT $1, unnest($2::text[]), $3",
			class, roots, unit); err != nil {
			return err
		}
		digest, err := c.digest(ctx, tx, cl, roots)
		if err != nil {
			return err
		}
		res.Members, res.Digest = len(roots), digest
		_, err = tx.Exec(ctx, fmt.Sprintf("UPDATE be_tier_unit SET state = 'sealed', members = $2, digest = $3, sealed_at = now(), "+
			"blocked = NULL, blocked_since = NULL, anchor_max = (SELECT max(r.%[1]s) FROM %[2]s r WHERE r.%[3]s::text = ANY($4)), "+
			"newest_created = (SELECT max(r.%[4]s) FROM %[2]s r WHERE r.%[3]s::text = ANY($4)) WHERE id = $1",
			ident(cl.Anchor), ident(cl.Root.Name), ident(cl.Root.PK), ident(cl.Created)), unit, len(roots), digest, roots)
		return err
	})
	return res, err
}

// openUnit is the grouping's open unit, created (with the next sequence number) when there is none.
func openUnit(ctx context.Context, tx db.Tx, class, grouping string) (string, error) {
	var id string
	err := tx.QueryRow(ctx, "SELECT id::text FROM be_tier_unit WHERE class = $1 AND grouping = $2 AND state = 'open'",
		class, grouping).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	id = ids.New().String()
	_, err = tx.Exec(ctx, "INSERT INTO be_tier_unit (id, class, grouping, seq, state) VALUES ($1, $2, $3, "+
		"(SELECT coalesce(max(seq), 0) + 1 FROM be_tier_unit WHERE class = $2 AND grouping = $3), 'open')", id, class, grouping)
	return id, err
}

// blockers are the SDK's own (events not published yet) and the module's, merged per root.
func (c *Catalog) blockers(ctx context.Context, tx db.Tx, class string, roots []string) (map[string][]string, error) {
	out := map[string][]string{}
	rows, err := tx.Query(ctx, "SELECT DISTINCT record_id FROM be_outbox WHERE published_at IS NULL AND record_id = ANY($1)", roots)
	if err != nil {
		return nil, err
	}
	pending, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	for _, r := range pending {
		out[r] = append(out[r], WhyEventsPending)
	}
	if c.Registry != nil {
		for _, b := range c.Registry.blockers[class] {
			why, err := b(ctx, tx, class, roots)
			if err != nil {
				return nil, err
			}
			for r, reasons := range why {
				out[r] = append(out[r], reasons...)
			}
		}
	}
	for r := range out {
		sort.Strings(out[r])
		out[r] = slices.Compact(out[r])
	}
	return out, nil
}

// selectList is a table's columns in name order, each as the driver should give it for its type (numeric and jsonb
// as their text).
func selectList(t Table, alias string) ([]string, string) {
	names := make([]string, 0, len(t.Columns))
	for n := range t.Columns {
		names = append(names, n)
	}
	sort.Strings(names)
	exprs := make([]string, len(names))
	for i, n := range names {
		e := alias + "." + ident(n)
		if typ := t.Columns[n]; typ == TypeNumeric || typ == TypeJSONB {
			e += "::text"
		}
		exprs[i] = e
	}
	return names, strings.Join(exprs, ", ")
}

// digest computes the sealed content's digest (P15.9) from the roots and their children, in the seal's transaction.
func (c *Catalog) digest(ctx context.Context, tx db.Tx, cl Class, roots []string) (string, error) {
	var sums []TableSum
	add := func(t Table, where string) error {
		names, list := selectList(t, "t")
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT %s FROM %s t WHERE %s", list, ident(t.Name), where), roots)
		if err != nil {
			return err
		}
		defer rows.Close()
		var h TableHash
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			m := make(map[string]any, len(names))
			for i, n := range names {
				m[n] = vals[i]
			}
			k, err := PK(t.Columns, []string{t.PK}, m)
			if err != nil {
				return err
			}
			b, err := Row(t.Columns, m)
			if err != nil {
				return err
			}
			h.Add(k, b)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		sums = append(sums, TableSum{Name: t.Name, Rows: h.Len(), SHA256: h.Sum()})
		return nil
	}
	if err := add(cl.Root, "t."+ident(cl.Root.PK)+"::text = ANY($1)"); err != nil {
		return "", err
	}
	for _, ch := range cl.Children {
		if err := add(ch.Table, "t."+ident(ch.FK)+"::text = ANY($1)"); err != nil {
			return "", err
		}
	}
	return UnitDigest(cl.Name, cl.SchemaVersion, sums)
}
