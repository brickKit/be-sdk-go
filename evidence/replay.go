package evidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/internal/evidencepb"
	"google.golang.org/grpc"
)

// Replayed is an evidence record its owner re-executes during a full recovery (be-protocol P12.19-P12.21).
type Replayed struct {
	ID, Category, Phase, Subject string
	Object                       Object
	Change                       map[string]any
	At                           time.Time // the record's at: when it was recorded
}

// ReplayFunc re-applies a record's change in the replay's transaction, without the business preconditions (P12.21);
// it returns ErrDangling when an object the change refers to no longer exists.
type ReplayFunc func(ctx context.Context, tx db.Tx, r Replayed) error

// ErrDangling: an object the change refers to no longer exists; nothing is done and the record goes to review.
var ErrDangling = errors.New("evidence: an object the change refers to no longer exists")

// ErrDeferred: what the re-execution depends on has not arrived yet (for example a source's replayed state not yet
// applied); nothing is done, and the recovery tool asks again once the authorization member applied its sources
// (P12.20, P12.22).
var ErrDeferred = errors.New("evidence: the replay depends on what has not arrived yet")

// CheckFunc checks a record whose effect lies outside the recovery group (P12.20: the assembly protocol 15's 6.7, a
// credential tightening in an identity product outside the group): outside any transaction it asks the other side
// whether the effect still holds. It answers false with the reason when it does not; an error (the other side could
// not be asked) is an inconsistent answer too.
type CheckFunc func(ctx context.Context, r Replayed) (consistent bool, detail string, err error)

// Replays collects a module's replay functions and checks by evidence category; a category is one or the other.
type Replays struct {
	byCategory map[string]ReplayFunc
	checks     map[string]CheckFunc
	err        error
}

// Add registers the replay of one category.
func (r *Replays) Add(category string, f ReplayFunc) {
	if r.byCategory == nil {
		r.byCategory = map[string]ReplayFunc{}
	}
	r.twice(category)
	r.byCategory[category] = f
}

// Check registers the check of one category whose effect lies outside the recovery group (P12.20).
func (r *Replays) Check(category string, f CheckFunc) {
	if r.checks == nil {
		r.checks = map[string]CheckFunc{}
	}
	r.twice(category)
	r.checks[category] = f
}

func (r *Replays) twice(category string) {
	_, replayed := r.byCategory[category]
	_, checked := r.checks[category]
	if (replayed || checked) && r.err == nil {
		r.err = fmt.Errorf("the category %s is registered twice: one replay or one check (P12.20)", category)
	}
}

// Err is the registration's mistake, if any: the host refuses to start with it.
func (r *Replays) Err() error { return r.err }

// Replayer is implemented by modules that re-execute their own evidence (P12.19); the definition names the
// operation of the replay (component.Definition.Replay).
type Replayer interface {
	Replays(r *Replays) error
}

// Outcomes of a replay (P12.20, P12.21).
const (
	AlreadyReflected = "reflected"
	Applied          = "applied"
	Dangling         = "dangling"
	Unsupported      = "unsupported"
	Deferred         = "deferred"     // not yet: asked again after 6.4 (ErrDeferred)
	Consistent       = "consistent"   // checked: the effect still holds outside the recovery group
	Inconsistent     = "inconsistent" // checked: it does not, or could not be asked (for review)
)

// ReplayServer answers be.evidence.v1.ReplayService for one member (P12.19-P12.21).
type ReplayServer struct {
	evidencepb.UnimplementedReplayServiceServer
	DB        *db.DB
	Component string
	Replays   *Replays
	Log       *slog.Logger
}

var errRollback = errors.New("evidence: replay rolled back")

// Replay implements be.evidence.v1.ReplayService: one record, reflected by its identity or re-executed by its
// category's function with the change history in one transaction.
func (s *ReplayServer) Replay(ctx context.Context, req *evidencepb.ReplayRequest) (*evidencepb.ReplayResponse, error) {
	r, err := ParseReplayed(req.GetRecord(), s.Component)
	if err != nil {
		return nil, errs.New(errs.ReasonRequestInvalid, err.Error())
	}
	if check := s.Replays.checks[r.Category]; check != nil {
		return s.check(ctx, req.GetRecovery(), r, check)
	}
	f := s.Replays.byCategory[r.Category]
	if f == nil {
		return &evidencepb.ReplayResponse{Outcome: Unsupported, Detail: "no replay for the category " + r.Category}, nil
	}
	outcome, detail := "", ""
	err = s.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		outcome, detail = "", ""
		done, err := ReplayTx(ctx, tx, r, f)
		switch {
		case errors.Is(err, ErrDangling):
			outcome, detail = Dangling, err.Error()
			return errRollback
		case errors.Is(err, ErrDeferred):
			outcome, detail = Deferred, err.Error()
			return errRollback
		case err != nil:
			return err
		case done:
			outcome = AlreadyReflected
		default:
			outcome = Applied
		}
		return nil
	})
	if err != nil && !errors.Is(err, errRollback) {
		return nil, err
	}
	s.log().Info("an evidence record replayed", "event", "evidence_replayed", "id", r.ID, "category", r.Category,
		"phase", r.Phase, "outcome", outcome, "recovery", req.GetRecovery())
	return &evidencepb.ReplayResponse{Outcome: outcome, Detail: detail}, nil
}

// check answers a record of a checked category (P12.20): reflected by its identity in the change history, else the
// component's check outside any transaction; nothing written either way.
func (s *ReplayServer) check(ctx context.Context, recovery string, r Replayed, f CheckFunc) (*evidencepb.ReplayResponse, error) {
	var done bool
	if err := s.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		var err error
		done, err = Reflected(ctx, tx, r.ID)
		return err
	}); err != nil {
		return nil, err
	}
	outcome, detail := AlreadyReflected, ""
	if !done {
		ok, why, err := f(ctx, r)
		switch {
		case err != nil:
			outcome, detail = Inconsistent, "could not be checked: "+err.Error()
		case ok:
			outcome = Consistent
		default:
			outcome, detail = Inconsistent, why
		}
	}
	s.log().Info("an evidence record checked", "event", "evidence_checked", "id", r.ID, "category", r.Category,
		"phase", r.Phase, "outcome", outcome, "detail", detail, "recovery", recovery)
	return &evidencepb.ReplayResponse{Outcome: outcome, Detail: detail}, nil
}

func (s *ReplayServer) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.New(slog.DiscardHandler)
}

// ReplayTx replays one record in the caller's transaction (P12.20, P12.21): reflected by its identity already, it does
// nothing and answers true; else f re-executes it and the change history is written under the record's identity. An
// error (ErrDangling among them) leaves the caller to roll the transaction back. ReplayServer uses it; an owner uses
// it for a replay of its own besides the standard method (the authorization member's deferred revocations, 6.5).
func ReplayTx(ctx context.Context, tx db.Tx, r Replayed, f ReplayFunc) (reflected bool, err error) {
	done, err := Reflected(ctx, tx, r.ID)
	if err != nil || done {
		return done, err
	}
	if err := f(ctx, tx, r); err != nil {
		return false, err
	}
	return false, reflect(ctx, tx, r.ID, r.Object)
}

// ParseReplayed reads a be.evidence.record that this component recorded, in a phase that is replayed (P12.20).
func ParseReplayed(raw []byte, component string) (Replayed, error) {
	var rec struct {
		RecordType string `json:"record_type"`
		ID         string `json:"id"`
		Recorder   struct {
			Component string `json:"component"`
		} `json:"recorder"`
		Category string         `json:"category"`
		Phase    string         `json:"phase"`
		Subject  string         `json:"subject"`
		Object   map[string]any `json:"object"`
		Change   map[string]any `json:"change"`
		At       string         `json:"at"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		return Replayed{}, fmt.Errorf("the record is not JSON: %w", err)
	}
	switch {
	case rec.RecordType != RecordType || rec.ID == "":
		return Replayed{}, errors.New("not a be.evidence.record")
	case rec.Recorder.Component != component:
		return Replayed{}, fmt.Errorf("recorded by %s, not this component", rec.Recorder.Component)
	case rec.Phase != "effective" && rec.Phase != "general" && rec.Phase != "intent":
		return Replayed{}, fmt.Errorf("a record in phase %s is not replayed", rec.Phase)
	}
	obj := Object{}
	obj.Type, _ = rec.Object["type"].(string)
	obj.ID, _ = rec.Object["id"].(string)
	obj.Version, _ = rec.Object["version"].(string)
	at, err := time.Parse(time.RFC3339Nano, rec.At)
	if err != nil {
		return Replayed{}, fmt.Errorf("the record's at %q is not a time: %w", rec.At, err)
	}
	return Replayed{ID: rec.ID, Category: rec.Category, Phase: rec.Phase, Subject: rec.Subject, Object: obj,
		Change: rec.Change, At: at}, nil
}

// reflect writes the change history row of a replayed record (P12.21): its own identity, the current epoch and a new
// commit sequence; no record goes to the evidence outbox (the store has it).
func reflect(ctx context.Context, tx db.Tx, id string, obj Object) error {
	pos, err := commit(ctx, tx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO be_change_history (evidence_id, object_type, object_id, version_before, epoch,
		commit_seq) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6)`, id, obj.Type, obj.ID, obj.Version, pos.epoch, pos.seq)
	return err
}

// ReplayClient calls an owner's ReplayService: the recovery tool's side (P12.22), and the owners' tests. The
// connection presents the recovery tool's service identity (ops/recover).
type ReplayClient struct {
	Conn grpc.ClientConnInterface
}

// Replay asks the owner to replay one record; it returns the outcome (Applied, AlreadyReflected, Dangling,
// Unsupported, Deferred; Consistent, Inconsistent for a checked category) and its detail.
func (c ReplayClient) Replay(ctx context.Context, record []byte, recovery string) (outcome, detail string, err error) {
	out, err := evidencepb.NewReplayServiceClient(c.Conn).Replay(ctx, &evidencepb.ReplayRequest{Record: record,
		Recovery: recovery})
	if err != nil {
		return "", "", err
	}
	return out.GetOutcome(), out.GetDetail(), nil
}
