package recovery

import (
	"context"
	"errors"
	"sort"
	"strconv"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/recoverypb"
)

// errReadOnly rolls back a check's transaction: checks never write.
var errReadOnly = errors.New("read-only")

// Server answers be.recovery.v1.CheckService for one member.
type Server struct {
	recoverypb.UnimplementedCheckServiceServer
	db     *db.DB
	checks []Check
	reg    *Checks
}

// NewServer is the member's CheckService: the SDK's own checks, then the component's (reg may be nil).
func NewServer(d *db.DB, subscribes bool, reg *Checks) *Server {
	if reg == nil {
		reg = &Checks{}
	}
	return &Server{db: d, checks: append(builtins(subscribes), reg.checks...), reg: reg}
}

// readOnly runs fn in a transaction that is always rolled back.
func (s *Server) readOnly(ctx context.Context, fn func(context.Context, db.Tx) error) error {
	err := s.db.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := fn(ctx, tx); err != nil {
			return err
		}
		return errReadOnly
	})
	if errors.Is(err, errReadOnly) {
		return nil
	}
	return err
}

// Check runs every check (P14.1, P14.2), reading only through rolled-back transactions. A check that fails to run answers
// unprovable with the error, so one broken check does not hide the others.
func (s *Server) Check(ctx context.Context, req *recoverypb.CheckRequest) (*recoverypb.CheckResponse, error) {
	if req.GetRecovery() == "" {
		return nil, errs.New(errs.ReasonRequestInvalid, "recovery is required")
	}
	out := &recoverypb.CheckResponse{}
	for _, ch := range s.checks {
		res, err := ch.Run(ctx, s.readOnly)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			res = Result{Outcome: Unprovable, Disposal: "the check could not run: " + err.Error()}
		}
		r := &recoverypb.CheckResult{Check: ch.Name, Basis: ch.Basis, Outcome: res.Outcome, Disposal: res.Disposal}
		for _, d := range res.Differences {
			r.Differences = append(r.Differences, &recoverypb.Difference{ObjectType: d.ObjectType, ObjectId: d.ObjectID, Detail: d.Detail})
		}
		out.Results = append(out.Results, r)
	}
	return out, nil
}

// Present answers, for each object, whether it is still in the database and at which revision (P14.3). An object
// type without a registered presence question answers CAPABILITY_UNSUPPORTED; a version that is not a revision
// number, REQUEST_INVALID.
func (s *Server) Present(ctx context.Context, req *recoverypb.PresentRequest) (*recoverypb.PresentResponse, error) {
	if req.GetRecovery() == "" {
		return nil, errs.New(errs.ReasonRequestInvalid, "recovery is required")
	}
	byType := map[string][]string{}
	for _, o := range req.GetObjects() {
		if _, ok := s.reg.presence[o.GetType()]; !ok {
			return nil, errs.New(errs.ReasonCapabilityUnsupported, "no presence question for the object type "+o.GetType())
		}
		if v := o.GetVersion(); v != "" {
			if _, err := strconv.ParseInt(v, 10, 64); err != nil {
				return nil, errs.New(errs.ReasonRequestInvalid, "version "+v+" is not a revision number")
			}
		}
		byType[o.GetType()] = append(byType[o.GetType()], o.GetId())
	}
	found := map[string]map[string]int64{}
	types := make([]string, 0, len(byType))
	for t := range byType {
		types = append(types, t)
	}
	sort.Strings(types)
	err := s.readOnly(ctx, func(ctx context.Context, tx db.Tx) error {
		for _, t := range types {
			got, err := s.reg.presence[t](ctx, tx, byType[t])
			if err != nil {
				return err
			}
			found[t] = got
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := &recoverypb.PresentResponse{}
	for _, o := range req.GetObjects() {
		a := &recoverypb.PresentAnswer{Object: o, Answer: Absent}
		if rev, ok := found[o.GetType()][o.GetId()]; ok {
			a.Answer, a.Version = Present, strconv.FormatInt(rev, 10)
			if want, _ := strconv.ParseInt(o.GetVersion(), 10, 64); o.GetVersion() != "" && rev < want {
				a.Answer = Older
			}
		}
		out.Answers = append(out.Answers, a)
	}
	return out, nil
}

func seriesOf(u db.UsedMax) *recoverypb.Series {
	return &recoverypb.Series{LegalEntity: u.LegalEntity, Code: u.Code, Period: u.Period, MaxUsed: u.Max}
}

// Numbers lists the continuous series and their used maximum (P14.5).
func (s *Server) Numbers(ctx context.Context, req *recoverypb.NumbersRequest) (*recoverypb.NumbersResponse, error) {
	if req.GetRecovery() == "" {
		return nil, errs.New(errs.ReasonRequestInvalid, "recovery is required")
	}
	var maxima []db.UsedMax
	if err := s.readOnly(ctx, func(ctx context.Context, tx db.Tx) (err error) {
		maxima, err = db.ContinuousMaxima(ctx, tx)
		return err
	}); err != nil {
		return nil, err
	}
	out := &recoverypb.NumbersResponse{}
	for _, u := range maxima {
		out.Series = append(out.Series, seriesOf(u))
	}
	return out, nil
}

// AdvanceNumbers raises each series' used maximum (never lowers it) and has the component void the numbers it skips
// over, all in one transaction (P14.5); it answers each series' maximum after.
func (s *Server) AdvanceNumbers(ctx context.Context, req *recoverypb.AdvanceNumbersRequest) (*recoverypb.AdvanceNumbersResponse, error) {
	if req.GetRecovery() == "" {
		return nil, errs.New(errs.ReasonRequestInvalid, "recovery is required")
	}
	out := &recoverypb.AdvanceNumbersResponse{}
	err := s.db.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		out.Series = nil
		for _, in := range req.GetSeries() {
			sr := db.Series{LegalEntity: in.GetLegalEntity(), Code: in.GetCode(), Period: in.GetPeriod()}
			if in.GetMaxUsed() < 0 {
				return errs.New(errs.ReasonRequestInvalid, "max_used is negative")
			}
			before, err := db.RaiseContinuous(ctx, tx, sr, in.GetMaxUsed())
			if err != nil {
				return err
			}
			after := max(before, in.GetMaxUsed())
			if after > before {
				if s.reg.void == nil {
					return errs.New(errs.ReasonCapabilityUnsupported, "this component registers no voiding of skipped numbers (recovery.Checks.Void)")
				}
				if err := s.reg.void(ctx, tx, sr, before+1, after); err != nil {
					return err
				}
			}
			out.Series = append(out.Series, seriesOf(db.UsedMax{Series: sr, Max: after}))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
