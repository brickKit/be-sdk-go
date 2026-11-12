package flow

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/projection"
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
