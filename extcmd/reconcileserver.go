package extcmd

import (
	"context"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/externalpb"
	"github.com/jackc/pgx/v5"
)

// ReconcileServer answers be.external.v1.ReconcileService for one member (P13.11-P13.13): only the recovery tool
// calls it, in the verifying phase of a full recovery.
type ReconcileServer struct {
	externalpb.UnimplementedReconcileServiceServer
	Commands *Commands
}

// reconcileRequest is a parsed ReconcileRequest.
type reconcileRequest struct {
	recovery string
	t        time.Time
	before   time.Duration
	failedAt *time.Time
	issued   []string
}

func parseReconcile(req *externalpb.ReconcileRequest) (reconcileRequest, error) {
	r := reconcileRequest{recovery: req.GetRecovery(), before: time.Duration(req.GetWindowBeforeSeconds()) * time.Second,
		issued: req.GetIssued()}
	if r.recovery == "" {
		return r, errs.New(errs.ReasonRequestInvalid, "recovery is required")
	}
	if req.GetWindowBeforeSeconds() < 0 {
		return r, errs.New(errs.ReasonRequestInvalid, "window_before_seconds is negative")
	}
	var err error
	if r.t, err = time.Parse(time.RFC3339Nano, req.GetT()); err != nil {
		return r, errs.New(errs.ReasonRequestInvalid, "t is not an RFC 3339 time")
	}
	if req.GetFailedAt() != "" {
		f, err := time.Parse(time.RFC3339Nano, req.GetFailedAt())
		if err != nil {
			return r, errs.New(errs.ReasonRequestInvalid, "failed_at is not an RFC 3339 time")
		}
		r.failedAt = &f
	}
	return r, nil
}

// Reconcile reads the open commands, asks the external system (a listing, or one query per reference) outside any
// transaction, then matches, applies and keeps the conclusions in one transaction, and answers every conclusion of
// this recovery. The external system not answering fails the whole call and writes nothing (P13.11).
func (s *ReconcileServer) Reconcile(ctx context.Context, req *externalpb.ReconcileRequest) (*externalpb.ReconcileResponse, error) {
	r, err := parseReconcile(req)
	if err != nil {
		return nil, err
	}
	c := s.Commands
	open, local, err := c.readOpen(ctx, r.issued)
	if err != nil {
		return nil, err
	}
	records, err := c.ask(ctx, open, local, r)
	if err != nil {
		return nil, errs.New(errs.ReasonDependencyUnavailable, "the external system's records: "+err.Error())
	}
	var out []*externalpb.Conclusion
	err = c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := c.applyReconcile(ctx, tx, r, open, records, local); err != nil {
			return err
		}
		out, err = reconciled(ctx, tx, r.recovery)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &externalpb.ReconcileResponse{Conclusions: out}, nil
}

// readOpen reads the commands not final (manual included) with the start of a send attempt not ended, which of the
// issued references have a command.
func (c *Commands) readOpen(ctx context.Context, issued []string) (open []Open, local map[string]bool, err error) {
	local = map[string]bool{}
	err = c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if open, err = queryOpen(ctx, tx, false); err != nil {
			return err
		}
		return markLocal(ctx, tx, local, issued)
	})
	return open, local, err
}

func queryOpen(ctx context.Context, tx db.Tx, lock bool) ([]Open, error) {
	q := `SELECT reference, id::text, operation, state, resend_deadline, expires_at, first_reached_at, last_reached_at,
			unknown_since, accepted_since, unrecorded_until,
			(SELECT min(started_at) FROM be_external_attempt a WHERE a.command = c.id AND a.kind = 'send' AND a.ended_at IS NULL)
		FROM be_external_command c WHERE state NOT IN ('succeeded', 'failed', 'rejected') ORDER BY reference`
	if lock {
		q += " FOR UPDATE"
	}
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Open, error) {
		var o Open
		err := row.Scan(&o.Reference, &o.Command, &o.Operation, &o.State, &o.ResendDeadline, &o.ExpiresAt, &o.FirstReached,
			&o.LastReached, &o.UnknownSince, &o.AcceptedSince, &o.UnrecordedUntil, &o.SendingAt)
		return o, err
	})
}

// markLocal sets local[ref] for each of refs this member has a command for, in any state.
func markLocal(ctx context.Context, tx db.Tx, local map[string]bool, refs []string) error {
	if len(refs) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, "SELECT DISTINCT reference FROM be_external_command WHERE reference = ANY($1)", refs)
	if err != nil {
		return err
	}
	found, err := pgx.CollectRows(rows, pgx.RowTo[string])
	for _, ref := range found {
		local[ref] = true
	}
	return err
}

// ask lists the external records from T minus the window to now when the adapter can (P13.11), and queries by
// reference what the listing does not cover: an open command that may have been sent before the window, every
// open command and issued reference when the adapter cannot list. Commands of an operation without the query
// capability are not asked. An answer it cannot get is an error.
func (c *Commands) ask(ctx context.Context, open []Open, local map[string]bool, r reconcileRequest) (map[string]Record, error) {
	records := map[string]Record{}
	from := r.t.Add(-r.before)
	lister, lists := c.Adapter.(Lister)
	if lists {
		lctx, cancel := context.WithTimeout(ctx, ListTimeout)
		recs, err := lister.List(lctx, from, time.Now())
		cancel()
		if err != nil {
			return nil, fmt.Errorf("the listing: %w", err)
		}
		for _, rec := range recs {
			records[rec.Reference] = rec
		}
	}
	query := func(cmd Command) error {
		qctx, cancel := context.WithTimeout(ctx, SendTimeout)
		ans, err := c.Adapter.Query(qctx, cmd)
		cancel()
		if err != nil {
			return fmt.Errorf("the query of %s: %w", cmd.Reference, err)
		}
		if ans.Found {
			records[cmd.Reference] = Record{Reference: cmd.Reference, State: ans.State, ExternalID: ans.ExternalID,
				Outcome: ans.Outcome, Response: ans.Response}
		}
		return nil
	}
	for _, o := range open {
		if (lists && !earliest(o, r.t).Before(from)) || !c.Ops[o.Operation].Query {
			continue
		}
		if err := query(Command{ID: o.Command, Requester: c.Component, Reference: o.Reference, Operation: o.Operation}); err != nil {
			return nil, err
		}
	}
	if lists || !c.anyQuery() {
		return records, nil
	}
	for _, ref := range r.issued {
		if !local[ref] {
			if err := query(Command{Requester: c.Component, Reference: ref}); err != nil {
				return nil, err
			}
		}
	}
	return records, nil
}

func (c *Commands) anyQuery() bool {
	for _, caps := range c.Ops {
		if caps.Query {
			return true
		}
	}
	return false
}

// applyReconcile matches under the lock of the open commands (only those read before asking: one created since was
// not asked about), applies each conclusion and keeps it for this recovery.
func (c *Commands) applyReconcile(ctx context.Context, tx db.Tx, r reconcileRequest, before []Open,
	records map[string]Record, local map[string]bool) error {
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return err
	}
	locked, err := queryOpen(ctx, tx, true)
	if err != nil {
		return err
	}
	asked := map[string]bool{}
	for _, o := range before {
		asked[o.Reference], local[o.Reference] = true, true // one final since is still local
	}
	var open []Open
	byRef := map[string]Open{}
	for _, o := range locked {
		if asked[o.Reference] {
			open, byRef[o.Reference] = append(open, o), o
		}
	}
	states := make(map[string]string, len(records))
	refs := make([]string, 0, len(records))
	for ref, rec := range records {
		states[ref], refs = rec.State, append(refs, ref)
	}
	if err := markLocal(ctx, tx, local, refs); err != nil {
		return err
	}
	until := now
	if r.failedAt != nil {
		until = *r.failedAt
	}
	for _, cl := range Match(open, states, local, r.issued, func(op string) Capabilities { return c.Ops[op] }, r.t, until, now) {
		o, has := byRef[cl.Reference]
		state := ""
		if has {
			if state, err = c.conclude(ctx, tx, o, cl, records[cl.Reference], until, now); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO be_external_reconciled (recovery, reference, command, conclusion, state,
				resend_deadline, detail) VALUES ($1, $2, nullif($3, '')::uuid, $4, nullif($5, ''), $6, nullif($7, ''))
			ON CONFLICT (recovery, reference) DO UPDATE SET command = excluded.command, conclusion = excluded.conclusion,
				state = excluded.state, resend_deadline = excluded.resend_deadline, detail = excluded.detail, updated_at = now()`,
			r.recovery, cl.Reference, cl.Command, cl.Conclusion, state, cl.ResendDeadline, cl.Detail); err != nil {
			return err
		}
	}
	return nil
}

// conclude applies one conclusion to its command and answers the command's state after it.
func (c *Commands) conclude(ctx context.Context, tx db.Tx, o Open, cl Conclusion, rec Record, until, now time.Time) (string, error) {
	r := &row{Command: Command{ID: o.Command, Reference: o.Reference, Operation: o.Operation}, Facts: o.Facts,
		caps: c.Ops[o.Operation], now: now}
	switch cl.Conclusion {
	case ConclusionMatched: // the external record, not sent again
		if _, err := tx.Exec(ctx, `UPDATE be_external_command SET external_id = coalesce(nullif($2, ''), external_id),
			outcome = coalesce($3, outcome) WHERE id = $1`, o.Command, rec.ExternalID, outcomeOf(rec.Outcome)); err != nil {
			return "", err
		}
		return cl.State, c.set(ctx, tx, r, cl.State, "reconciled: the external record", c.nextAt(r, cl.State, 0))
	case ConclusionManual:
		if o.State == Manual {
			return Manual, nil
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE be_external_command SET first_reached_at = $2, unrecorded_until = $3,
		resend_deadline = coalesce($4, resend_deadline) WHERE id = $1`, o.Command, cl.FirstReached, until, cl.ResendDeadline); err != nil {
		return "", err
	}
	switch cl.Conclusion {
	case ConclusionUnknown, ConclusionManual:
		return cl.Conclusion, c.set(ctx, tx, r, cl.Conclusion, "reconciled: "+cl.Detail, now)
	}
	return o.State, nil // deadline: the state unchanged
}

// reconciled are the conclusions kept for a recovery, with each command's state now (P13.13).
func reconciled(ctx context.Context, tx db.Tx, recovery string) ([]*externalpb.Conclusion, error) {
	rows, err := tx.Query(ctx, `SELECT r.reference, coalesce(r.command::text, ''), r.conclusion, coalesce(c.state, r.state, ''),
			r.resend_deadline, coalesce(r.detail, '')
		FROM be_external_reconciled r LEFT JOIN be_external_command c ON c.id = r.command
		WHERE r.recovery = $1 ORDER BY r.reference`, recovery)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*externalpb.Conclusion, error) {
		var c externalpb.Conclusion
		var deadline *time.Time
		err := row.Scan(&c.Reference, &c.Command, &c.Conclusion, &c.State, &deadline, &c.Detail)
		if deadline != nil {
			c.ResendDeadline = deadline.UTC().Format(time.RFC3339Nano)
		}
		return &c, err
	})
}
