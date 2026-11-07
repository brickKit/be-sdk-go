package reserve

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/jackc/pgx/v5"
)

// Kind is one participation of a component: the branches of one resource it owns (a component may hold several).
type Kind struct {
	// Name tells this participation's branches apart in the runtime's table.
	Name string
	// Period is the hold period (P16.10; the protocol's start value is 5 minutes).
	Period time.Duration
	// Lock locks the resources of branch id in the participant's fixed order, before the runtime locks the branch
	// (P16.11). A branch with nothing of the component's (a tombstone) locks nothing.
	Lock func(ctx context.Context, tx db.Tx, id string) error
	// GiveBack gives back what branch b has not consumed, its resources locked.
	GiveBack func(ctx context.Context, tx db.Tx, b Branch) error
	// Changed publishes branch b after a change, in the same transaction (P16.2): the component's event, its payload
	// the component's own record of the branch.
	Changed func(ctx context.Context, tx db.Tx, b Branch) error
}

// Branch is what the runtime keeps of a branch (be_reserve_branch).
type Branch struct {
	ID            string
	Kind          string
	State         State
	Reason        string // rejected: why, the component's words
	Digest        string // the hold's content digest; "" on a tombstone
	HoldExpiresAt *time.Time
	ConfirmedAt   *time.Time
	Version       int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	past          bool // held and past its hold period, by the database's clock when read
}

// Answer is what a command did: the branch after it (State Absent when none is stored), a content conflict, a
// consumption's refusal, or a repeat (a hold of a branch seen before with the same content, a consumption ID used
// before with the same content: nothing was done again, the component answers what it answered the first time). The
// component answers it on its own contract, in its own words.
type Answer struct {
	Branch   Branch
	Conflict bool
	Refused  Refusal
	Repeated bool
}

// Take answers whether a new branch's hold can take all its content (P16.3): it locks the component's resources in its
// order, takes them when enough, and writes the component's own rows of the branch either way; reason says why not.
type Take func(ctx context.Context, tx db.Tx) (enough bool, reason string, err error)

// Use takes a consumption off branch b (committed; its resources and the branch locked) and answers whether something
// is left; beyond refuses it, nothing taken. Any other refusal of the component's own is an error the component
// recognizes after the transaction is rolled back.
type Use func(ctx context.Context, tx db.Tx, b Branch) (left, beyond bool, err error)

// ErrKind is a branch ID stored under another kind.
var ErrKind = errors.New("reserve: the branch ID is another participation's")

const cols = `id::text, kind, state, coalesce(reason, ''), coalesce(content_digest, ''), hold_expires_at, confirmed_at,
	version, created_at, updated_at, state = 'held' AND hold_expires_at <= now()`

func read(ctx context.Context, tx db.Tx, k Kind, id string, lock bool) (Branch, error) {
	q := `SELECT ` + cols + ` FROM be_reserve_branch WHERE id = $1::uuid`
	if lock {
		q += ` FOR UPDATE`
	}
	var b Branch
	var st string
	err := tx.QueryRow(ctx, q, id).Scan(&b.ID, &b.Kind, &st, &b.Reason, &b.Digest, &b.HoldExpiresAt, &b.ConfirmedAt,
		&b.Version, &b.CreatedAt, &b.UpdatedAt, &b.past)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Branch{ID: id, Kind: k.Name, State: Absent}, nil
	case err != nil:
		return b, err
	case b.Kind != k.Name:
		return b, ErrKind
	}
	b.State = State(st)
	return b, nil
}

// move writes the decided state, raises the version and publishes the branch (P16.2).
func move(ctx context.Context, tx db.Tx, k Kind, id string, to State) (Branch, error) {
	if _, err := tx.Exec(ctx, `UPDATE be_reserve_branch SET state = $2,
		hold_expires_at = CASE WHEN $2 = 'held' THEN hold_expires_at END,
		confirmed_at = CASE WHEN $2 = 'committed' AND confirmed_at IS NULL THEN now() ELSE confirmed_at END,
		version = version + 1, updated_at = now() WHERE id = $1::uuid`, id, string(to)); err != nil {
		return Branch{}, err
	}
	b, err := read(ctx, tx, k, id, false)
	if err != nil {
		return b, err
	}
	return b, k.Changed(ctx, tx, b)
}

// apply carries out decision d on b, whose resources and row are locked: gives back, moves, publishes.
func apply(ctx context.Context, tx db.Tx, k Kind, b Branch, d Decision) (Answer, error) {
	if d.Resource == GivesBack {
		if err := k.GiveBack(ctx, tx, b); err != nil {
			return Answer{}, err
		}
	}
	if !d.Changed {
		return Answer{Branch: b, Conflict: d.Conflict, Refused: d.Refused}, nil
	}
	after, err := move(ctx, tx, k, b.ID, d.To)
	return Answer{Branch: after}, err
}

// locked reads branch id after locking its resources (P16.11): Absent when none is stored.
func locked(ctx context.Context, tx db.Tx, k Kind, id string) (Branch, error) {
	if b, err := read(ctx, tx, k, id, false); err != nil || b.State == Absent {
		return b, err
	}
	if err := k.Lock(ctx, tx, id); err != nil {
		return Branch{}, err
	}
	return read(ctx, tx, k, id, true)
}

// Hold is the hold of branch id with content digest (P16.3), in tx: a new branch is written first, then take decides
// held or rejected; a branch seen before answers as it is (a tombstone cancelled whatever the content), or a conflict.
func Hold(ctx context.Context, tx db.Tx, k Kind, id, digest string, take Take) (Answer, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO be_reserve_branch (id, kind, state, content_digest) VALUES ($1, $2, 'held', $3)
		ON CONFLICT (id) DO NOTHING`, id, k.Name, digest)
	if err != nil {
		return Answer{}, err
	}
	if tag.RowsAffected() == 0 {
		b, err := read(ctx, tx, k, id, true)
		if err != nil {
			return Answer{}, err
		}
		c := OnHoldOther
		if b.Digest == digest {
			c = OnHoldSame
		}
		d := Decide(b.State, b.past, c)
		return Answer{Branch: b, Conflict: d.Conflict, Repeated: !d.Conflict}, nil
	}
	enough, reason, err := take(ctx, tx)
	if err != nil {
		return Answer{}, err
	}
	c := OnHoldShort
	if enough {
		c = OnHoldEnough
	}
	d := Decide(Absent, false, c)
	var why *string
	if !enough && reason != "" {
		why = &reason
	}
	if _, err := tx.Exec(ctx, `UPDATE be_reserve_branch SET state = $2, reason = $3,
		hold_expires_at = CASE WHEN $2 = 'held' THEN now() + make_interval(secs => $4) END WHERE id = $1::uuid`,
		id, string(d.To), why, k.Period.Seconds()); err != nil {
		return Answer{}, err
	}
	b, err := read(ctx, tx, k, id, false)
	if err != nil {
		return Answer{}, err
	}
	return Answer{Branch: b}, k.Changed(ctx, tx, b)
}

// Confirm is the confirm of branch id (P16.4), in tx: held within its period becomes committed; past it, expired
// and given back; anything else answers as it is; none stored answers Absent and leaves nothing.
func Confirm(ctx context.Context, tx db.Tx, k Kind, id string) (Answer, error) {
	b, err := locked(ctx, tx, k, id)
	if err != nil || b.State == Absent {
		return Answer{Branch: b}, err
	}
	return apply(ctx, tx, k, b, Decide(b.State, b.past, OnConfirm))
}

// Cancel is the cancel of branch id (P16.5), in tx: none stored leaves a tombstone; held or committed is released and
// what it has not consumed given back; anything else answers as it is.
func Cancel(ctx context.Context, tx db.Tx, k Kind, id string) (Answer, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO be_reserve_branch (id, kind, state) VALUES ($1, $2, 'cancelled')
		ON CONFLICT (id) DO NOTHING`, id, k.Name)
	if err != nil {
		return Answer{}, err
	}
	if tag.RowsAffected() == 1 {
		b, err := read(ctx, tx, k, id, false)
		if err != nil {
			return Answer{}, err
		}
		return Answer{Branch: b}, k.Changed(ctx, tx, b)
	}
	b, err := locked(ctx, tx, k, id)
	if err != nil {
		return Answer{}, err
	}
	return apply(ctx, tx, k, b, Decide(b.State, b.past, OnCancel))
}

// Query answers branch id as it is (P16.6), in tx; none stored answers Absent and leaves nothing.
func Query(ctx context.Context, tx db.Tx, k Kind, id string) (Answer, error) {
	b, err := read(ctx, tx, k, id, false)
	return Answer{Branch: b}, err
}

// Consume is a consumption of branch id (P16.7), in tx: a consumption ID used before answers the branch as it is
// with the same content, a conflict with other content; a new one is refused on a branch not stored or not committed;
// use takes it off, refusing it beyond what is left; the branch stays committed while something is left, else it is
// consumed. Nothing is recorded on a refusal.
func Consume(ctx context.Context, tx db.Tx, k Kind, id, consumption, digest string, use Use) (Answer, error) {
	var usedBranch, usedDigest string
	err := tx.QueryRow(ctx, `SELECT branch_id::text, content_digest FROM be_reserve_consumption WHERE id = $1`,
		consumption).Scan(&usedBranch, &usedDigest)
	switch {
	case err == nil:
		b, err := read(ctx, tx, k, id, false)
		if err != nil {
			return Answer{}, err
		}
		c := OnConsumeUsedOther
		if usedBranch == id && usedDigest == digest {
			c = OnConsumeUsedSame
		}
		d := Decide(b.State, b.past, c)
		return Answer{Branch: b, Conflict: d.Conflict, Repeated: !d.Conflict}, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return Answer{}, err
	}
	b, err := locked(ctx, tx, k, id)
	if err != nil {
		return Answer{}, err
	}
	if d := Decide(b.State, b.past, OnConsumeWithin); d.Refused != "" {
		return Answer{Branch: b, Refused: d.Refused}, nil
	}
	left, beyond, err := use(ctx, tx, b)
	if err != nil {
		return Answer{}, err
	}
	c := OnConsumeExact
	switch {
	case beyond:
		c = OnConsumeBeyond
	case left:
		c = OnConsumeWithin
	}
	d := Decide(b.State, b.past, c)
	if d.Refused != "" {
		return Answer{Branch: b, Refused: d.Refused}, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO be_reserve_consumption (id, branch_id, content_digest) VALUES ($1, $2, $3)`,
		consumption, id, digest); err != nil {
		return Answer{}, err
	}
	return apply(ctx, tx, k, b, d)
}

// ExpireDue is the participant's expiry job (P16.8): the held branches past their period, a batch at a time, each in
// its own background transaction in the lock order, judged again under the lock.
func ExpireDue(ctx context.Context, d *db.DB, k Kind) error {
	const batch = 100
	for {
		var due []string
		if err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			rows, err := tx.Query(ctx, `SELECT id::text FROM be_reserve_branch WHERE kind = $1 AND state = 'held'
				AND hold_expires_at <= now() ORDER BY hold_expires_at LIMIT $2`, k.Name, batch)
			if err != nil {
				return err
			}
			due, err = pgx.CollectRows(rows, pgx.RowTo[string])
			return err
		}); err != nil {
			return err
		}
		for _, id := range due {
			if err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
				b, err := locked(ctx, tx, k, id)
				if err != nil || b.State == Absent {
					return err
				}
				_, err = apply(ctx, tx, k, b, Decide(b.State, b.past, OnExpire))
				return err
			}); err != nil {
				return fmt.Errorf("reserve: expiring %s: %w", id, err)
			}
		}
		if len(due) < batch {
			return nil
		}
	}
}
