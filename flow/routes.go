package flow

import (
	"context"
	"errors"
	comp "github.com/brickKit/be-sdk-go/v3/component"
	"time"

	sdkauthz "github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/idem"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/jackc/pgx/v5"
)

// RoutePrefix is where the standard routes are (P17.14) under the component's public prefix, which for a flow
// manager is the default /api/<name> (P11.11).
func RoutePrefix(component string) string { return comp.DefaultPrefix(component) + "/flow/" }

// DecisionPath is a task's decision entry on its component (P17.14): the worklist derives it from the publisher,
// never from the projection.
func DecisionPath(component, task string) string {
	return RoutePrefix(component) + "tasks/" + task + "/decisions"
}

// Routes registers the standard routes (P17.14): the decision, reading a task, the paused tasks (the exception entry).
// The host adds them after the module's own routes, as it does the tiering's.
func (k *Kernel) Routes(r *httpx.Routes) {
	p := RoutePrefix(k.Component)
	r.Add(httpx.Route{Method: "POST", Path: p + "tasks/:id/decisions", Operation: k.Operation, Handle: k.decideRoute})
	r.Add(httpx.Route{Method: "GET", Path: p + "tasks/:id", Operation: k.Operation, Handle: k.getRoute})
	r.Add(httpx.Route{Method: "GET", Path: p + "tasks", Operation: k.ManageOperation, Handle: k.pausedRoute})
}

func (k *Kernel) decideRoute(c *httpx.Context) error {
	req, err := idem.FromHTTP(c)
	if err != nil {
		return err
	}
	var in DecisionIn
	if err := c.BindJSON(&in); err != nil {
		return err
	}
	subj, _ := sdkauthz.From(c.Ctx())
	t, err := k.Decide(c.Ctx(), subj, req, c.Param("id"), in)
	if err != nil {
		return err
	}
	return c.JSON(200, t)
}

func (k *Kernel) getRoute(c *httpx.Context) error {
	subj, _ := sdkauthz.From(c.Ctx())
	id := c.Param("id")
	if !uuidForm(id) {
		return errNotFound()
	}
	var out Task
	err := k.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
		t, err := k.readTask(ctx, tx, id, false)
		if err != nil {
			return err
		}
		d, err := k.definition(t.instance.FlowKey, t.instance.FlowVersion)
		if err != nil {
			return err
		}
		_, _, facts, err := k.Document(ctx, tx, t.instance.Document)
		if err != nil {
			return err
		}
		if !k.visible(ctx, subj, t, d, facts) {
			return errNotFound()
		}
		out = t
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return errNotFound()
	}
	if err != nil {
		return err
	}
	return c.JSON(200, out)
}

// pausedRoute lists the paused tasks with their reasons (the exception entry, P17.14), the oldest first.
func (k *Kernel) pausedRoute(c *httpx.Context) error {
	if c.Query("state") != string(Paused) {
		return errs.New(errs.ReasonRequestInvalid, "only state=paused is listed").
			WithViolations(errs.Violation{Field: "state", Reason: "INVALID", Description: "paused"})
	}
	subj, _ := sdkauthz.From(c.Ctx())
	if _, _, err := subj.Check(c.Ctx(), k.ManageOperation, sdkauthz.Facts{}); err != nil {
		return err
	}
	out := []Task{}
	err := k.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
		ids, err := k.taskIDs(ctx, tx, `SELECT id::text FROM be_flow_task WHERE state = 'paused' ORDER BY created_at LIMIT 200`)
		if err != nil {
			return err
		}
		for _, id := range ids {
			t, err := k.readTask(ctx, tx, id, false)
			if err != nil {
				return err
			}
			out = append(out, t)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return c.JSON(200, map[string]any{"tasks": out})
}

func (k *Kernel) taskIDs(ctx context.Context, tx db.Tx, q string, args ...any) ([]string, error) {
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// The runtime's jobs (protocol 01).
const (
	JobResolve   = "be-flow-resolve"
	JobHeartbeat = "be-flow-heartbeat"
	// JobResolveNow is the re-resolution queued when the employee projection changes while tasks wait (P17.8).
	JobResolveNow = "be-flow-resolve-now"
	// HeartbeatEvery is P17.18's interval.
	HeartbeatEvery = 5 * time.Minute
)

// Jobs registers the re-resolution of the paused tasks (P17.8; queued when the employee projection changes, and every
// minute as the floor) and the heartbeat (P17.18).
func (k *Kernel) Jobs(j *work.Jobs) {
	j.Add(work.Job{Name: JobResolveNow, Kind: work.Queue, Effect: work.Local, MaxAttempts: work.Unlimited,
		Handle: func(ctx context.Context, _ *work.Run) error { return k.ResolvePaused(ctx) }})
	j.Add(work.Job{Name: JobResolve, Kind: work.Singleton, Effect: work.Local, Interval: time.Minute,
		MaxAttempts: work.Unlimited, Handle: func(ctx context.Context, _ *work.Run) error { return k.ResolvePaused(ctx) }})
	j.Add(work.Job{Name: JobHeartbeat, Kind: work.Singleton, Effect: work.EmitOnly, Interval: HeartbeatEvery,
		MaxAttempts: work.Unlimited, Handle: func(ctx context.Context, _ *work.Run) error { return k.Heartbeat(ctx) }})
}

// ResolvePaused resolves every paused task again, each in its own transaction under its instance's lock; the ones that
// now have candidates become pending, the others keep their reason (or change it).
func (k *Kernel) ResolvePaused(ctx context.Context) error {
	var paused []string
	err := k.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var err error
		paused, err = k.taskIDs(ctx, tx, `SELECT id::text FROM be_flow_task WHERE state = 'paused' ORDER BY created_at LIMIT 500`)
		return err
	})
	if err != nil {
		return err
	}
	for _, id := range paused {
		if err := k.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			t, err := k.readTask(ctx, tx, id, true)
			if err != nil || t.State != Paused || t.instance.State != Running {
				return err
			}
			cands, reason, err := k.resolve(ctx, tx, t.instance)
			if err != nil {
				return err
			}
			switch {
			case reason == "":
				return k.bump(ctx, tx, id, Pending, "", cands)
			case t.PausedReason == nil || *t.PausedReason != reason:
				return k.bump(ctx, tx, id, Paused, reason, nil)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Heartbeat publishes the heartbeat while the component has pending or paused tasks (P17.18).
func (k *Kernel) Heartbeat(ctx context.Context) error {
	return k.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var pending, paused int
		if err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE state = 'pending'), count(*) FILTER (WHERE state = 'paused')
			FROM be_flow_task WHERE state IN ('pending', 'paused')`).Scan(&pending, &paused); err != nil {
			return err
		}
		if pending+paused == 0 {
			return nil
		}
		id := ids.New().String()
		_, err := k.Events.Emit(ctx, tx, k.Stream, events.Event{Type: k.HeartbeatEvent, RecordID: id, Version: 1,
			Projection: events.Full, Data: map[string]any{"fact_id": id, "component": k.Component,
				"at": canon.Time(time.Now()), "pending": pending, "paused": paused}})
		return err
	})
}

// Enqueuer queues a work item in the caller's transaction (the runtime's work runner).
type Enqueuer interface {
	Enqueue(ctx context.Context, tx db.Tx, job string, args any) (string, error)
}

// EmployeesChanged is called in the transaction that changed the employee projection (P17.8): while tasks wait paused
// and no re-resolution is queued yet, it queues one, so a task waits for the projection, not for the minute.
func (k *Kernel) EmployeesChanged(ctx context.Context, tx db.Tx, w Enqueuer) error {
	var due bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM be_flow_task WHERE state = 'paused')
		AND NOT EXISTS (SELECT 1 FROM be_work_item WHERE job = $1 AND state = 'pending')`, JobResolveNow).Scan(&due); err != nil {
		return err
	}
	if !due {
		return nil
	}
	_, err := w.Enqueue(ctx, tx, JobResolveNow, struct{}{})
	return err
}
