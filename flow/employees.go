package flow

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/projection"
	"github.com/brickKit/be-sdk-go/v3/runtime"
)

// EmployeeProjection is the runtime's projection of mdm.employee the assignment resolves against (P17.8): the
// appointments (an employee on a position in a legal entity for a period) and the account links (an account to an
// employee) of the watermark source mdm/employee, in be_projection under this name.
const EmployeeProjection = "be.flow.employee"

// EmployeeSource is the watermark source.
const EmployeeSource = "mdm/employee"

// Employees is the Directory over the watermark projection of mdm.employee: an answer is complete only while the source
// is usable at the freshness (P9.24) and the projection has no incomplete row.
type Employees struct {
	W         *projection.Watermarked
	Freshness time.Duration
	// work and changed queue a re-resolution when an event changes the projection (P17.8): the kernel that uses this
	// directory sets changed (Kernel.Check), NewEmployees the runtime's work runner.
	work    Enqueuer
	changed func(ctx context.Context, tx db.Tx, w Enqueuer) error
}

// complete says whether the projection may be relied on now.
func (e Employees) complete(ctx context.Context, tx db.Tx) (bool, error) {
	j, err := e.W.JudgeMany(ctx, tx, nil, e.Freshness)
	if err != nil {
		return false, err
	}
	if j.Source != projection.Usable {
		return false, nil
	}
	var incomplete bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM be_projection WHERE projection = $1 AND state = 'incomplete')`,
		EmployeeProjection).Scan(&incomplete)
	return !incomplete, err
}

// holding is the appointments in effect on a date for a position of a legal entity, and the active links of their
// employees: the accounts that hold it.
const holding = `SELECT DISTINCT l.data->>'account'
	FROM be_projection a JOIN be_projection l ON l.projection = a.projection
		AND l.data->>'employee_id' = a.data->>'employee_id' AND l.data ? 'account' AND l.data->>'state' = 'active'
		AND l.state = 'complete'
	WHERE a.projection = $1 AND a.state = 'complete' AND a.data ? 'position_id'
		AND a.data->>'legal_entity_id' = $2 AND a.data->>'position_id' = $3
		AND a.data->>'state' IN ('planned', 'active') AND (a.data->>'start_date')::date <= $4::date
		AND (a.data->>'end_date' IS NULL OR (a.data->>'end_date')::date >= $4::date)`

// Holders answers the accounts holding the position on the date.
func (e Employees) Holders(ctx context.Context, tx db.Tx, legalEntity, positionID, date string) ([]string, bool, error) {
	ok, err := e.complete(ctx, tx)
	if err != nil || !ok {
		return nil, false, err
	}
	rows, err := tx.Query(ctx, holding+` ORDER BY 1`, EmployeeProjection, legalEntity, positionID, date)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, false, err
		}
		out = append(out, a)
	}
	return out, true, rows.Err()
}

// Holds answers whether the account holds the position on the date.
func (e Employees) Holds(ctx context.Context, tx db.Tx, account, legalEntity, positionID, date string) (bool, bool, error) {
	ok, err := e.complete(ctx, tx)
	if err != nil || !ok {
		return false, false, err
	}
	var holds bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (`+holding+` AND l.data->>'account' = $5)`, EmployeeProjection, legalEntity,
		positionID, date, account).Scan(&holds)
	return holds, true, err
}

// PersonOf answers the employee an account is linked to now, or the account itself when it has no link (P17.12).
func (e Employees) PersonOf(ctx context.Context, tx db.Tx, account string) (string, error) {
	var person *string
	err := tx.QueryRow(ctx, `SELECT max(data->>'employee_id') FROM be_projection WHERE projection = $1 AND state = 'complete'
		AND data->>'account' = $2 AND data->>'state' = 'active'`, EmployeeProjection, account).Scan(&person)
	if err != nil || person == nil {
		return account, err
	}
	return "employee:" + *person, nil
}

// SourceHigh is protocol 02's D_src_high for a position's holders (protocol 09: the qualification by position uses
// it), the starting value the authorization member uses too.
const SourceHigh = time.Minute

// EmployeeStream is the watermark source's stream.
var EmployeeStream = events.Stream{Name: "mdm.employee", Partitions: 4}

// NewEmployees builds the directory when the assembly has mdm/employee (an optional dependency of the component): the
// watermark projection of its stream, pulled and read over its system surface. ok is false without it: a position rule
// then answers no_assignee (P17.3).
func NewEmployees(rt *runtime.Runtime) (*Employees, bool) {
	conn, ok := rt.GRPC(EmployeeSource)
	if !ok {
		return nil, false
	}
	w := &projection.Watermarked{Projection: projection.Projection{Name: EmployeeProjection}, DB: rt.DB,
		Source: EmployeeSource, Stream: EmployeeStream, Deployment: rt.Config.String("DEPLOYMENT_ID"), Freshness: SourceHigh,
		Pull: projection.PullClient{Conn: conn}, Snapshot: projection.RecordClient{Conn: conn, Stream: EmployeeStream.Name},
		Log: rt.Logger}
	out := &Employees{W: w, Freshness: SourceHigh}
	if rt.Work != nil {
		out.work = rt.Work
	}
	return out, true
}

// Sub is the projection's subscription (the component adds it to its subscriptions): each event applied, the waiting
// tasks are resolved again soon (P17.8).
func (e *Employees) Sub() events.Sub {
	s := e.W.Sub("be-flow-employee")
	handle := s.Handle
	s.Handle = func(ctx context.Context, tx db.Tx, ev events.Received) error {
		if err := handle(ctx, tx, ev); err != nil || e.changed == nil || e.work == nil {
			return err
		}
		return e.changed(ctx, tx, e.work)
	}
	return s
}

// OnChange sets what runs in the transaction of each event that changed the projection, with the runtime's work
// runner: the kernel queues its re-resolution (Kernel.Check sets it); another user of the directory, such as the
// notification component expanding positions, queues its own work.
func (e *Employees) OnChange(f func(ctx context.Context, tx db.Tx, w Enqueuer) error) { e.changed = f }

// Start installs the source once and runs its pulls and alignment (the component's Start).
func (e *Employees) Start(ctx context.Context) error {
	if err := e.W.Install(ctx); err != nil {
		return err
	}
	go e.W.Run(ctx)
	return nil
}
