package tiering

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/jackc/pgx/v5"
)

// Reading a class's history (be-protocol P15.26, P15.38): the online part and the units that left the online tables,
// under one catalog snapshot and the caller's current authorization, complete within the sync limit or not at all;
// past it the read becomes an export the SDK's queue job writes to the cold store.

// History limits' configuration keys and the exports' lifetimes.
const (
	KeyColdSyncLimit   = "TIERING_COLD_SYNC_LIMIT"
	KeyColdConcurrency = "TIERING_COLD_CONCURRENCY"
	// ExportTTL is how long a finished export is kept; ExportURLTTL how long its presigned address is valid.
	ExportTTL    = 7 * 24 * time.Hour
	ExportURLTTL = 15 * time.Minute
	// ExportJob is the queue job that writes the exports.
	ExportJob = "be.tiering.export"
)

// ColdKeys are the cold reads' limits.
func ColdKeys() []config.Key {
	one := int64(1)
	return []config.Key{{Name: KeyColdSyncLimit, Type: config.Duration, Default: 10 * time.Second},
		{Name: KeyColdConcurrency, Type: config.Integer, Default: int64(4), Min: &one}}
}

// Reader is the owner's code for reading one class's history: never the caller's SQL or object addresses (P15.26).
type Reader struct {
	// Op is the authorization operation the read is for: the resource's view operation, or an export operation.
	Op string
	// Facts are a cold aggregate's facts for the authorization, from its root's canonical row.
	Facts func(root map[string]any) authz.Facts
	// Output shapes a visible cold aggregate for the caller under its decision (authz.Mask on d.Fields).
	Output func(a Aggregate, d authz.Decision) (map[string]any, error)
	// Online reads the online part under the snapshot: the class's rows the subject may see (its compiled list),
	// leaving out the members already off the online tables (Snapshot.OnlineExclusion), each emitted shaped.
	Online func(ctx context.Context, tx db.Tx, s Snapshot, sub *authz.Subject, params json.RawMessage,
		emit func(map[string]any) error) error
}

// Register registers the reader of a class's history.
func (c *Catalog) Register(class string, r Reader) error {
	if _, ok := c.Classes[class]; !ok {
		return fmt.Errorf("tiering: no class %q to read", class)
	}
	if r.Op == "" || r.Facts == nil || r.Output == nil || r.Online == nil {
		return fmt.Errorf("tiering: the reader of %s needs Op, Facts, Output and Online", class)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readers == nil {
		c.readers = map[string]Reader{}
	}
	c.readers[class] = r
	return nil
}

func (c *Catalog) reader(class string) (Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.readers[class]
	if !ok {
		return Reader{}, errs.New(errs.ReasonQueryNotCompletable, "the history of "+class+" has no reader")
	}
	return r, nil
}

// slot takes one of the member's cold-read slots (TIERING_COLD_CONCURRENCY); waiting counts toward the caller's limit.
func (c *Catalog) slot(ctx context.Context) (func(), error) {
	c.slotsOnce.Do(func() {
		n := c.ColdConcurrency
		if n <= 0 {
			n = 4
		}
		c.slots = make(chan struct{}, n)
	})
	select {
	case c.slots <- struct{}{}:
		return func() { <-c.slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// read reads a class's history under a new snapshot: the online part, then every cold unit by its route, each
// aggregate decided by the subject's current authorization; every visible one is emitted.
func (c *Catalog) read(ctx context.Context, sub *authz.Subject, class string, r Reader, params json.RawMessage,
	purpose string, emit func(map[string]any) error) error {
	s, err := c.Snapshot(ctx, purpose)
	if err != nil {
		return err
	}
	defer func() { _ = c.End(context.WithoutCancel(ctx), s) }()
	if err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error { return r.Online(ctx, tx, s, sub, params, emit) }); err != nil {
		return err
	}
	ctx, decide, err := sub.Decider(ctx, r.Op)
	if err != nil {
		return err
	}
	units, err := c.ColdUnits(ctx, s, class)
	if err != nil {
		return err
	}
	for _, u := range units {
		release, err := c.slot(ctx)
		if err != nil {
			return err
		}
		aggs, err := c.ReadUnit(ctx, s, u, nil)
		release()
		if err != nil {
			return err
		}
		for _, a := range aggs {
			d := decide(r.Facts(a.Root))
			if d.Visible != authz.True || d.Action != authz.True {
				continue
			}
			out, err := r.Output(a, d)
			if err != nil {
				return err
			}
			if err := emit(out); err != nil {
				return err
			}
		}
	}
	return nil
}

// Pending is a history read that became an export (the component answers 202 with its id).
type Pending struct{ Export string }

// History reads a class's history for the subject: complete within the sync limit (TIERING_COLD_SYNC_LIMIT); past
// it, the read is started as an export and Pending names it; never a part (P15.26).
func (c *Catalog) History(ctx context.Context, sub *authz.Subject, class string, params json.RawMessage) ([]map[string]any, *Pending, error) {
	r, err := c.reader(class)
	if err != nil {
		return nil, nil, err
	}
	limit := c.ColdSyncLimit
	if limit <= 0 {
		limit = 10 * time.Second
	}
	syncCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	var out []map[string]any
	err = c.read(syncCtx, sub, class, r, params, "history", func(o map[string]any) error { out = append(out, o); return nil })
	switch {
	case err == nil:
		return out, nil, nil
	case ctx.Err() == nil && errors.Is(syncCtx.Err(), context.DeadlineExceeded):
		id, err := c.StartExport(ctx, sub.Principal, class, params)
		if err != nil {
			return nil, nil, err
		}
		return nil, &Pending{Export: id}, nil
	}
	return nil, nil, err
}

// StartExport records an export of a class's history for a principal and queues it (P15.38). It cannot be done
// without a cold store, on an object-locked one (an export holds personal data and must go when it expires), or
// without a reader: QUERY_NOT_COMPLETABLE.
func (c *Catalog) StartExport(ctx context.Context, principal, class string, params json.RawMessage) (string, error) {
	if _, err := c.reader(class); err != nil {
		return "", err
	}
	switch {
	case c.Store == nil:
		return "", errs.New(errs.ReasonQueryNotCompletable, "the history cannot be read within the limit and there is no store to export it to")
	case c.StoreLocked:
		return "", errs.New(errs.ReasonQueryNotCompletable, "the history cannot be read within the limit and the object-locked store takes no exports")
	case c.Enqueue == nil:
		return "", errs.New(errs.ReasonQueryNotCompletable, "the history cannot be read within the limit and exports are not running")
	}
	if len(params) == 0 {
		params = json.RawMessage("{}")
	}
	id := ids.New().String()
	err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO be_tier_export (id, class, principal, params) VALUES ($1, $2, $3, $4)",
			id, class, principal, string(params)); err != nil {
			return err
		}
		_, err := c.Enqueue(ctx, tx, ExportJob, map[string]string{"export": id})
		return err
	})
	return id, err
}

// ExportStatus is an export as its requester sees it.
type ExportStatus struct {
	State  string // pending, done, failed
	Rows   int64
	Reason string    // why it failed
	URL    string    // done: a presigned address, valid ExportURLTTL
	Expiry time.Time // done: when it goes
}

// Export answers an export's status to its requester; anyone else gets NOT_FOUND (P15.38).
func (c *Catalog) Export(ctx context.Context, id, principal string) (ExportStatus, error) {
	var st ExportStatus
	var key, reason *string
	var rows *int64
	var expiry *time.Time
	err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT state, object_key, rows, reason, expires_at FROM be_tier_export WHERE id::text = $1 "+
			"AND principal = $2 AND (expires_at IS NULL OR expires_at > now())", id, principal).Scan(&st.State, &key, &rows, &reason, &expiry)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return st, errs.New(errs.ReasonNotFound, "not found")
	}
	if err != nil {
		return st, err
	}
	if rows != nil {
		st.Rows = *rows
	}
	if reason != nil {
		st.Reason = *reason
	}
	if st.State == "done" && key != nil && c.Store != nil {
		st.URL, st.Expiry = c.Store.PresignGet(*key, ExportURLTTL), *expiry
	}
	return st, nil
}

// ExportJobOf is the queue job writing the exports (P15.38): a new snapshot, the requester's authorization when it
// runs (Subject), the history as NDJSON (an output object a line, the JSON a synchronous read answers: masked fields null,
// P8.31) created under <prefix>/exports/. A
// QUERY_* or authorization answer fails the export with that reason; anything else is retried.
func (c *Catalog) ExportJobOf(log *slog.Logger) work.Job {
	return work.Job{Name: ExportJob, Kind: work.Queue, Effect: work.Local, MaxAttempts: 5, Timeout: time.Hour,
		Handle: func(ctx context.Context, run *work.Run) error {
			var args struct{ Export string }
			if err := json.Unmarshal(run.Args, &args); err != nil {
				return work.Permanent(err)
			}
			body, rows, reason, err := c.writeExport(ctx, args.Export)
			if err != nil && run.Attempts < run.MaxAttempts {
				return err
			}
			if err != nil { // the last attempt: the requester sees it failed rather than pending for ever
				log.Error("an archive export failed on its last attempt", "event", "tiering_export_failed",
					"export", args.Export, "error", err.Error())
				reason = errs.ReasonInternal
			}
			return run.Complete(ctx, func(ctx context.Context, tx db.Tx) error {
				if reason != "" {
					log.Warn("an archive export failed", "event", "tiering_export_failed", "export", args.Export, "reason", reason)
					_, err := tx.Exec(ctx, "UPDATE be_tier_export SET state = 'failed', reason = $2, finished_at = now(), "+
						"expires_at = now() + make_interval(secs => $3) WHERE id = $1 AND state = 'pending'", args.Export, reason,
						ExportTTL.Seconds())
					return err
				}
				_, err := tx.Exec(ctx, "UPDATE be_tier_export SET state = 'done', object_key = $2, rows = $3, finished_at = now(), "+
					"expires_at = now() + make_interval(secs => $4) WHERE id = $1 AND state = 'pending'", args.Export, body, rows,
					ExportTTL.Seconds())
				return err
			})
		}}
}

// ExportKey is where an export's object goes.
func (c *Catalog) ExportKey(id string) string { return c.Store.Prefix + "/exports/" + id + ".ndjson" }

// writeExport writes one pending export's object; it answers the key and the rows, or the reason it cannot be done.
func (c *Catalog) writeExport(ctx context.Context, id string) (key string, rows int64, reason string, err error) {
	var class, principal, state string
	var params []byte
	if err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT class, principal, params, state FROM be_tier_export WHERE id = $1", id).
			Scan(&class, &principal, &params, &state)
	}); err != nil {
		return "", 0, "", err
	}
	if state != "pending" {
		return "", 0, "", nil
	}
	r, err := c.reader(class)
	if err != nil {
		return "", 0, errs.ReasonQueryNotCompletable, nil
	}
	if c.Store == nil || c.Subject == nil {
		return "", 0, errs.ReasonQueryNotCompletable, nil
	}
	sub := c.Subject(principal)
	if sub == nil {
		return "", 0, errs.ReasonQueryNotCompletable, nil
	}
	key = c.ExportKey(id)
	var buf bytes.Buffer
	err = c.read(ctx, sub, class, r, params, "export", func(o map[string]any) error {
		line, err := json.Marshal(o) // map keys sorted
		if err != nil {
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
		rows++
		return nil
	})
	var e *errs.Error
	if errors.As(err, &e) && e.Domain == "be" && answered[e.Reason] {
		return "", 0, e.Reason, nil
	}
	if err != nil {
		return "", 0, "", err
	}
	switch err := c.Store.Put(ctx, key, buf.Bytes()); {
	case errors.Is(err, ErrExists): // an earlier attempt wrote it whole (a put is all or nothing) and did not record it
		body, err := c.Store.Get(ctx, key)
		if err != nil {
			return "", 0, "", err
		}
		return key, int64(bytes.Count(body, []byte("\n"))), "", nil
	case err != nil:
		return "", 0, "", err
	}
	return key, rows, "", nil
}

// answered are the reasons an export fails with, rather than retried: the read's own answers.
var answered = map[string]bool{errs.ReasonQueryNotCompletable: true, errs.ReasonQueryPlanChanged: true,
	errs.ReasonNotFound: true, errs.ReasonPermissionDenied: true, errs.ReasonAuthorizationUnprovable: true,
	errs.ReasonAuthorizationViewExpired: true}

// ExportCleanupJob deletes the expired exports: their objects, then their rows (P15.38).
func (c *Catalog) ExportCleanupJob(log *slog.Logger) work.Job {
	return work.Job{Name: "be.tiering.export.cleanup", Kind: work.Singleton, Effect: work.Local, Interval: time.Hour,
		MaxAttempts: work.Unlimited, Timeout: time.Hour,
		Handle: func(ctx context.Context, run *work.Run) error {
			n, err := c.CleanExports(ctx)
			if err != nil {
				return err
			}
			if n > 0 {
				log.Info("expired archive exports removed", "event", "tiering_exports_removed", "count", n)
			}
			return run.Complete(ctx, func(context.Context, db.Tx) error { return nil })
		}}
}

// CleanExports removes the expired exports and answers how many.
func (c *Catalog) CleanExports(ctx context.Context) (int, error) {
	type gone struct{ id, key string }
	var list []gone
	if err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT id::text, coalesce(object_key, '') FROM be_tier_export WHERE expires_at <= now()")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var g gone
			if err := rows.Scan(&g.id, &g.key); err != nil {
				return err
			}
			list = append(list, g)
		}
		return rows.Err()
	}); err != nil {
		return 0, err
	}
	for _, g := range list {
		if g.key != "" {
			if c.Store == nil {
				return 0, fmt.Errorf("tiering: export %s has an object and there is no store to delete it from", g.id)
			}
			if err := c.Store.Delete(ctx, g.key); err != nil {
				return 0, err
			}
		}
		if err := c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := tx.Exec(ctx, "DELETE FROM be_tier_export WHERE id = $1", g.id)
			return err
		}); err != nil {
			return 0, err
		}
	}
	return len(list), nil
}
