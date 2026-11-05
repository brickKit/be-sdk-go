package evidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/internal/evidencepb"
)

// Replayed is an evidence record its owner re-executes during a full recovery (be-protocol P12.19-P12.21).
type Replayed struct {
	ID, Category, Phase, Subject string
	Object                       Object
	Change                       map[string]any
}

// ReplayFunc re-applies a record's change in the replay's transaction, without the business preconditions (P12.21);
// it returns ErrDangling when an object the change refers to no longer exists.
type ReplayFunc func(ctx context.Context, tx db.Tx, r Replayed) error

// ErrDangling: an object the change refers to no longer exists; nothing is done and the record goes to review.
var ErrDangling = errors.New("evidence: an object the change refers to no longer exists")

// Replays collects a module's replay functions by evidence category.
type Replays struct{ byCategory map[string]ReplayFunc }

// Add registers the replay of one category.
func (r *Replays) Add(category string, f ReplayFunc) {
	if r.byCategory == nil {
		r.byCategory = map[string]ReplayFunc{}
	}
	r.byCategory[category] = f
}

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
	r, err := parseReplayed(req.GetRecord(), s.Component)
	if err != nil {
		return nil, errs.New(errs.ReasonRequestInvalid, err.Error())
	}
	f := s.Replays.byCategory[r.Category]
	if f == nil {
		return &evidencepb.ReplayResponse{Outcome: Unsupported, Detail: "no replay for the category " + r.Category}, nil
	}
	outcome, detail := "", ""
	err = s.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		outcome, detail = "", ""
		done, err := Reflected(ctx, tx, r.ID)
		if err != nil {
			return err
		}
		if done {
			outcome = AlreadyReflected
			return nil
		}
		if err := f(ctx, tx, r); errors.Is(err, ErrDangling) {
			outcome, detail = Dangling, err.Error()
			return errRollback
		} else if err != nil {
			return err
		}
		if err := reflect(ctx, tx, r.ID, r.Object); err != nil {
			return err
		}
		outcome = Applied
		return nil
	})
	if err != nil && !errors.Is(err, errRollback) {
		return nil, err
	}
	s.log().Info("an evidence record replayed", "event", "evidence_replayed", "id", r.ID, "category", r.Category,
		"phase", r.Phase, "outcome", outcome, "recovery", req.GetRecovery())
	return &evidencepb.ReplayResponse{Outcome: outcome, Detail: detail}, nil
}

func (s *ReplayServer) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.New(slog.DiscardHandler)
}

// parseReplayed reads a be.evidence.record that this component recorded, in a phase that is replayed.
func parseReplayed(raw []byte, component string) (Replayed, error) {
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
	return Replayed{ID: rec.ID, Category: rec.Category, Phase: rec.Phase, Subject: rec.Subject, Object: obj,
		Change: rec.Change}, nil
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
