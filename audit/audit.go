// Package audit is the component side of reliable audit (protocol P8.19-P8.23; assembly project protocol 03): the
// local append-only audit table written with the business change, sensitive reads recorded before the data is
// released, denials within a per-minute budget, numbering after commit, transfer to the audit component with
// receipts, and deletion only once a receipt is covered by the audit component's backup.
package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/records"
)

// Categories (protocol 03, "what needs auditing").
const (
	BusinessChange  = "business_change"
	SecurityChange  = "security_change"
	SensitiveRead   = "sensitive_read"
	Delegated       = "delegated"
	SecurityDenial  = "security_denial"
	WorkDisposition = "work_disposition"
)

// Outcomes.
const (
	Succeeded = "succeeded"
	Denied    = "denied"
	Failed    = "failed"
)

// Parameters of protocol 03.
const (
	DenialBudget    = 20 // details per principal, reason and minute
	DefaultCapacity = 100_000
	MinRetention    = 7 * 24 * time.Hour
)

// Record is one audit record. Restricted values go in as digests or masks, never in clear.
type Record struct {
	Category    string
	Operation   string
	Outcome     string
	Reason      string
	Actor       string // the acting principal
	Represented string // the principal acted for, when delegated
	Target      map[string]any
	Changes     map[string]any
	Request     map[string]any // request and trace IDs, call mode
}

// Auditor writes a member's audit records.
type Auditor struct {
	DB       *db.DB
	Capacity int64 // the backlog above which required audited operations are refused
	backlog  atomic.Int64
}

// New returns an auditor for a member's database.
func New(d *db.DB) *Auditor { return &Auditor{DB: d, Capacity: DefaultCapacity} }

// ErrCapacity is the refusal when the backlog is over capacity.
var ErrCapacity = errs.New(errs.ReasonAuditCapacityExhausted, "audited operations are paused until the audit backlog drains")

func body(r Record, id string, at time.Time) (map[string]any, error) {
	b := map[string]any{"id": id, "occurred_at": canon.Time(at), "category": r.Category, "operation": r.Operation,
		"outcome": r.Outcome}
	for k, v := range map[string]string{"reason": r.Reason, "actor": r.Actor, "represented": r.Represented} {
		if v != "" {
			b[k] = v
		}
	}
	for k, v := range map[string]map[string]any{"target": r.Target, "changes": r.Changes, "request": r.Request} {
		if v != nil {
			c, err := canonMap(v)
			if err != nil {
				return nil, err
			}
			b[k] = c
		}
	}
	return b, nil
}

// canonMap round-trips a map through JSON so it contains only the encoder's types: numbers stay json.Number, so an
// integer (a version, a count) is encoded and a fraction is refused (decimals travel as strings, protocol 14).
func canonMap(m map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	dropNulls(out)
	return out, nil
}

// dropNulls removes the null members of every object: the canonical encoding has no null (security 09a), and an
// absent member says the same as a null one (a record's state with no parent, no cash unit).
func dropNulls(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			if e == nil {
				delete(x, k)
				continue
			}
			dropNulls(e)
		}
	case []any:
		for _, e := range x {
			dropNulls(e)
		}
	}
}

func jsonOrNil(v any) any {
	if v == nil {
		return nil
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func (a *Auditor) insert(ctx context.Context, tx db.Tx, r Record) (string, error) {
	id, at := ids.New().String(), time.Now().UTC()
	b, err := body(r, id, at)
	if err != nil {
		return "", err
	}
	raw, digest, err := records.Seal("be.audit.record", b)
	if err != nil {
		return "", err
	}
	if err := records.Validate("be.audit.record", b); err != nil { // a bad record is refused here, not at transfer
		return "", err
	}
	actor, represented := any(nil), any(nil)
	if r.Actor != "" {
		actor = jsonOrNil(map[string]string{"id": r.Actor})
	}
	if r.Represented != "" {
		represented = jsonOrNil(map[string]string{"id": r.Represented})
	}
	var reason any
	if r.Reason != "" {
		reason = r.Reason
	}
	_, err = tx.Exec(ctx, `INSERT INTO be_audit_log (id, occurred_at, category, operation, outcome, reason, actor, represented,
		target, changes, request, digest, record) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		id, at, r.Category, r.Operation, r.Outcome, reason, actor, represented, jsonOrNil(nilMap(r.Target)),
		jsonOrNil(nilMap(r.Changes)), jsonOrNil(nilMap(r.Request)), digest, raw)
	return id, err
}

func nilMap(m map[string]any) any {
	if m == nil {
		return nil
	}
	return m
}

// Write records a business or security change inside its transaction (P8.19): if the transaction fails, neither
// happened. Over capacity it refuses, so the change does not happen either.
func (a *Auditor) Write(ctx context.Context, tx db.Tx, r Record) (string, error) {
	if a.Capacity > 0 && a.backlog.Load() >= a.Capacity {
		return "", ErrCapacity
	}
	return a.insert(ctx, tx, r)
}

// Read records a sensitive read or export in its own short transaction; call it before releasing the data and
// refuse the read when it fails (P8.19).
func (a *Auditor) Read(ctx context.Context, r Record) error {
	if a.Capacity > 0 && a.backlog.Load() >= a.Capacity {
		return ErrCapacity
	}
	r.Category = SensitiveRead
	return a.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := a.insert(ctx, tx, r)
		return err
	})
}

// Denial records a security denial within the per-minute budget; past it the window only counts, and Flush later
// appends one summary row with the count (P8.23). Its failure never changes the denial; it is returned for logging.
func (a *Auditor) Denial(ctx context.Context, r Record) error {
	r.Category, r.Outcome = SecurityDenial, Denied
	now := time.Now().UTC()
	return a.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var detailed int
		if err := tx.QueryRow(ctx, `INSERT INTO be_audit_denial_budget (principal, reason, minute, detailed) VALUES ($1, $2, $3, 1)
			ON CONFLICT (principal, reason, minute) DO UPDATE SET detailed = be_audit_denial_budget.detailed + 1
			RETURNING detailed`, r.Actor, r.Reason, now.Truncate(time.Minute)).Scan(&detailed); err != nil {
			return err
		}
		if detailed <= DenialBudget {
			_, err := a.insert(ctx, tx, r)
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE be_audit_denial_budget SET overflow = overflow + 1, first_at = coalesce(first_at, $4),
			last_at = $4 WHERE principal = $1 AND reason = $2 AND minute = $3`, r.Actor, r.Reason, now.Truncate(time.Minute), now)
		return err
	})
}

// Flush appends one summary row for every finished window whose denials went past the budget.
func (a *Auditor) Flush(ctx context.Context) (int, error) {
	n := 0
	err := a.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		n = 0
		rows, err := tx.Query(ctx, `SELECT principal, reason, minute, overflow, first_at, last_at FROM be_audit_denial_budget
			WHERE overflow > 0 AND NOT summarized AND minute < date_trunc('minute', now()) FOR UPDATE SKIP LOCKED`)
		if err != nil {
			return err
		}
		type window struct {
			principal, reason   string
			minute, first, last time.Time
			count               int
		}
		var ws []window
		for rows.Next() {
			var w window
			if err := rows.Scan(&w.principal, &w.reason, &w.minute, &w.count, &w.first, &w.last); err != nil {
				rows.Close()
				return err
			}
			ws = append(ws, w)
		}
		rows.Close()
		for _, w := range ws {
			r := Record{Category: SecurityDenial, Outcome: Denied, Reason: w.reason, Actor: w.principal,
				Operation: "be.audit.denials_over_budget", Changes: map[string]any{"count": strconv.Itoa(w.count),
					"first": canon.Time(w.first), "last": canon.Time(w.last), "window": canon.Time(w.minute)}}
			if _, err := a.insert(ctx, tx, r); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE be_audit_denial_budget SET summarized = true WHERE principal = $1 AND reason = $2
				AND minute = $3`, w.principal, w.reason, w.minute); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}
