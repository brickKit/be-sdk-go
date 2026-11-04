package projection

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"github.com/jackc/pgx/v5"
)

// Parameters of the watermark consumer (protocol 02's starting values).
const (
	QuarantineAttempts = 3                // a failing event is quarantined after this many attempts ...
	QuarantineAfter    = 30 * time.Second // ... or this long after it first failed (P9.25)
	StepEvery          = time.Second      // how often the consumer looks whether to pull, align or re-fetch
)

// Puller is a watermark source's pull (be.source.v1.WatermarkService/Pull) as the consumer calls it.
type Puller interface {
	Pull(ctx context.Context, req *sourcepb.PullRequest) (*sourcepb.PullResponse, error)
}

// Watermarked keeps one projection of a watermark source and proves it fresh (chapter 09, P9.19-P9.29): it applies
// the stream in position order, pulls when its watermark is half its freshness old, adopts the source's identity
// through the alignment state table, aligns from P0, quarantines what keeps failing, and judges reads of it.
type Watermarked struct {
	Projection
	DB         *db.DB
	Source     string        // the source component
	Stream     events.Stream // its watermark stream: name and partitions
	Deployment string        // this deployment's identity
	Freshness  time.Duration // what the judgements accept
	Pull       Puller
	Snapshot   Source // the source's snapshot and batch read (S3), for the re-fetch
	Claims     events.ClaimReader
	// Input turns an event into the projection's input; nil: a full, partial or delete input with the payload as data.
	Input func(e events.Received) (Input, error)
	Log   *slog.Logger

	mu   sync.Mutex // one pull, alignment or re-fetch at a time
	poke chan struct{}
}

// Install records the source once (P9.26's first state: aligning, nothing adopted).
func (w *Watermarked) Install(ctx context.Context) error {
	return w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO be_projection_source (projection, status) VALUES ($1, 'aligning')
			ON CONFLICT DO NOTHING`, w.Name); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO be_watermark_source (projection, source, stream, partitions, deployment)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, w.Name, w.Source, w.Stream.Name, w.Stream.Partitions,
			w.Deployment); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO be_watermark_partition (projection, partition)
			SELECT $1, g FROM generate_series(0, $2 - 1) g ON CONFLICT DO NOTHING`, w.Name, w.Stream.Partitions)
		return err
	})
}

// Sub is the subscription: every event of the stream (no type filter, P9.19), one at a time.
func (w *Watermarked) Sub(name string) events.Sub {
	return events.Sub{Name: name, Streams: []string{w.Stream.Name}, Handle: w.Handle, Ordered: true}
}

// ---------------------------------------------------------------------------------------------- state

type partState struct {
	seq      *int64
	identity json.RawMessage
	p0       *int64
	echoMP   *time.Time
}

type wmState struct {
	SourceState
	adopted  Identity
	alignGen int64
	refetch  bool
	parts    []partState
}

func (w *Watermarked) load(ctx context.Context, tx db.Tx, lock bool) (*wmState, error) {
	q := `SELECT ps.status, coalesce(ps.impl, ''), coalesce(ps.epoch, ''), ps.align_gen, ws.deployment,
			coalesce(ws.system_id, ''), ws.confirmed, ws.rollback, ws.refetch, ws.stalled_gen, ws.blocked_gen,
			coalesce(ws.prior_state, '')
		FROM be_projection_source ps JOIN be_watermark_source ws USING (projection) WHERE projection = $1`
	if lock {
		q += " FOR UPDATE OF ps, ws"
	}
	st := &wmState{}
	if err := tx.QueryRow(ctx, q, w.Name).Scan(&st.State, &st.adopted.Impl, &st.adopted.Epoch, &st.alignGen,
		&st.adopted.Deployment, &st.adopted.SystemID, &st.Confirmed, &st.Rollback, &st.refetch, &st.StalledGen,
		&st.BlockedGen, &st.Prior); err != nil {
		return nil, fmt.Errorf("projection %s: the watermark source is not installed: %w", w.Name, err)
	}
	rows, err := tx.Query(ctx, `SELECT seq, row_identity, p0, echo_mp FROM be_watermark_partition WHERE projection = $1
		ORDER BY partition`, w.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var p partState
		if err := rows.Scan(&p.seq, &p.identity, &p.p0, &p.echoMP); err != nil {
			return nil, err
		}
		st.parts = append(st.parts, p)
	}
	return st, rows.Err()
}

// saveState writes the source's state and adopted identity.
func (w *Watermarked) saveState(ctx context.Context, tx db.Tx, st *wmState) error {
	var impl, epoch, system, prior any
	if st.adopted.Impl != "" {
		impl, epoch = st.adopted.Impl, st.adopted.Epoch
	}
	if st.adopted.SystemID != "" {
		system = st.adopted.SystemID
	}
	if st.Prior != "" {
		prior = st.Prior
	}
	if _, err := tx.Exec(ctx, `UPDATE be_projection_source SET status = $2, impl = $3, epoch = $4 WHERE projection = $1`,
		w.Name, st.State, impl, epoch); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE be_watermark_source SET system_id = $2, confirmed = $3, rollback = $4, refetch = $5,
		stalled_gen = $6, blocked_gen = $7, prior_state = $8 WHERE projection = $1`, w.Name, system, st.Confirmed,
		st.Rollback, st.refetch, st.StalledGen, st.BlockedGen, prior)
	return err
}

func (w *Watermarked) savePart(ctx context.Context, tx db.Tx, n int, p partState) error {
	var identity any
	if p.identity != nil {
		identity = string(p.identity)
	}
	_, err := tx.Exec(ctx, `UPDATE be_watermark_partition SET seq = $3, row_identity = $4::jsonb, p0 = $5, echo_mp = $6
		WHERE projection = $1 AND partition = $2`, w.Name, n, p.seq, identity, p.p0, p.echoMP)
	return err
}

// transition applies the alignment state table and notes a due re-fetch; it returns the action.
func (w *Watermarked) transition(st *wmState, ev SourceEvent) string {
	next, action := NextState(st.SourceState, ev)
	if next.State != st.State {
		w.Log.Warn("a watermark source changed state", "event", "watermark_source_state", "projection", w.Name,
			"from", st.State, "to", next.State, "input", ev.Kind)
	}
	st.SourceState = next
	switch action {
	case ActRefetch, ActRowCheckRefetch, ActClearAndRefetch:
		st.refetch = true
	case ActAlert, ActAlertAndHold:
		w.Log.Error("a watermark source needs attention", "event", "watermark_source_alert", "projection", w.Name,
			"state", next.State, "input", ev.Kind,
			"exit", "confirm the source's data, then advance the deployment's recovery generation")
	}
	return action
}

// invalidate forgets positions and watermark after a new epoch or instance was adopted (P9.27).
func (st *wmState) invalidate() {
	for i := range st.parts {
		st.parts[i] = partState{}
	}
}

func (w *Watermarked) wake() {
	if w.poke == nil {
		return
	}
	select {
	case w.poke <- struct{}{}:
	default:
	}
}

// ---------------------------------------------------------------------------------------------- applying

var errGap = errors.New("projection: a position is missing before this one; the pull fills it")

// Handle is the subscription's handler (P9.19, P9.27): inputs of another instance block the source, of a newer
// epoch only trigger a pull, of an older one are ignored; while aligning, nothing at or below P0; else positions are
// applied contiguously, and a hole waits for the pull.
func (w *Watermarked) Handle(ctx context.Context, tx db.Tx, e events.Received) error {
	st, err := w.load(ctx, tx, true)
	if err != nil {
		return err
	}
	switch {
	case st.adopted.Impl == "":
		w.wake() // only a primary's answer establishes the source (P9.26)
		return nil
	case e.Impl != st.adopted.Impl:
		w.transition(st, SourceEvent{Kind: "foreign", Gen: Generation(e.Epoch)})
		return w.saveState(ctx, tx, st)
	}
	switch cmpEpoch(epochKey(e.Epoch), epochKey(st.adopted.Epoch)) {
	case 1:
		w.wake() // only a pull's answer adopts it
		return nil
	case -1:
		return nil
	}
	if st.State == Blocked || e.Partition >= len(st.parts) {
		return nil
	}
	p := st.parts[e.Partition]
	if st.State == Aligning && (p.p0 == nil || e.Seq <= *p.p0) {
		return nil // the snapshot holds it (P9.28)
	}
	contiguous := st.State != Aligning
	err = w.applyAt(ctx, tx, st, e, contiguous)
	if errors.Is(err, errGap) {
		w.wake()
		return nil
	}
	return err
}

// applyAt applies one row of the adopted instance and epoch and, when contiguous, advances the partition's
// contiguous position; a row that keeps failing is quarantined instead (P9.25).
func (w *Watermarked) applyAt(ctx context.Context, tx db.Tx, st *wmState, e events.Received, contiguous bool) error {
	p := &st.parts[e.Partition]
	if contiguous {
		switch {
		case p.seq == nil:
			return errGap
		case e.Seq <= *p.seq:
			return nil // applied already
		case e.Seq != *p.seq+1:
			return errGap
		}
	}
	if done, err := w.quarantineIfDue(ctx, tx, st, e); done || err != nil {
		if done && contiguous {
			seq := e.Seq
			p.seq, p.identity = &seq, identityOf(e)
			return w.savePart(ctx, tx, e.Partition, *p)
		}
		return err
	}
	if e.Type == events.EchoType {
		var mp time.Time
		err := tx.QueryRow(ctx, "SELECT m_p FROM be_watermark_pull WHERE projection = $1 AND nonce = $2", w.Name, e.RecordID).Scan(&mp)
		switch {
		case err == nil:
			p.echoMP = &mp // this consumer's own echo, its pull still recorded (P9.23)
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
	} else if err := w.applyInput(ctx, tx, st, e); err != nil {
		tx.AfterRollback(func(ctx context.Context) { w.noteFailure(ctx, e) }) // counted outside the failing transaction
		return err
	}
	if !contiguous {
		return nil
	}
	seq := e.Seq
	p.seq, p.identity = &seq, identityOf(e)
	return w.savePart(ctx, tx, e.Partition, *p)
}

func (w *Watermarked) applyInput(ctx context.Context, tx db.Tx, st *wmState, e events.Received) error {
	in, err := w.input(e)
	if err != nil {
		return err
	}
	in.Impl, in.Epoch, in.Seq = st.adopted.Impl, st.adopted.Epoch, e.Seq
	if _, err := w.Projection.Apply(ctx, tx, in); err != nil {
		return err
	}
	if e.Projection == events.Full {
		return w.digestTripwire(ctx, tx, st, e)
	}
	return nil
}

// digestTripwire is P9.29 (c): a complete row of the same instance and epoch whose content differs at the same version.
func (w *Watermarked) digestTripwire(ctx context.Context, tx db.Tx, st *wmState, e events.Received) error {
	var differs bool
	err := tx.QueryRow(ctx, `SELECT data IS DISTINCT FROM $3::jsonb FROM be_projection WHERE projection = $1
		AND record_id = $2 AND state = 'complete' AND version = $4 AND impl = $5 AND epoch = $6`,
		w.Name, e.RecordID, string(e.Data), e.Version, st.adopted.Impl, st.adopted.Epoch).Scan(&differs)
	if errors.Is(err, pgx.ErrNoRows) || err != nil || !differs {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	w.transition(st, SourceEvent{Kind: "rollback", Gen: Generation(st.adopted.Epoch)})
	return w.saveState(ctx, tx, st)
}

func (w *Watermarked) input(e events.Received) (Input, error) {
	if w.Input != nil {
		return w.Input(e)
	}
	kind := map[events.Projection]string{events.Full: Full, events.Partial: Partial, events.Delete: Delete}[e.Projection]
	if kind == "" {
		kind = Partial // an event type this consumer does not know: incomplete and fetched (P9.19)
	}
	return Input{RecordID: e.RecordID, Kind: kind, Version: e.Version, Data: e.Data}, nil
}

func identityOf(e events.Received) json.RawMessage {
	if e.Type == events.EchoType {
		b, _ := json.Marshal(map[string]string{"echo_nonce": e.RecordID})
		return b
	}
	b, _ := json.Marshal(map[string]any{"record": map[string]any{"record_id": e.RecordID, "version": e.Version,
		"digest": e.Headers[events.HeaderDigest]}})
	return b
}

// noteFailure counts a failed attempt at a position; it runs after the failing transaction rolled back.
func (w *Watermarked) noteFailure(ctx context.Context, e events.Received) {
	err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO be_watermark_failure (projection, partition, seq, attempts) VALUES ($1, $2, $3, 1)
			ON CONFLICT (projection, partition, seq) DO UPDATE SET attempts = be_watermark_failure.attempts + 1`,
			w.Name, e.Partition, e.Seq)
		return err
	})
	if err != nil {
		w.Log.Error("a failed attempt could not be counted", "projection", w.Name, "partition", e.Partition, "seq", e.Seq,
			"error", err.Error())
	}
}

// quarantineIfDue quarantines the event's subjects when it failed often or long enough (P9.25); it reports whether it
// did. Without subjects it does not, and the error keeps the watermark where it is.
func (w *Watermarked) quarantineIfDue(ctx context.Context, tx db.Tx, st *wmState, e events.Received) (bool, error) {
	var due bool
	err := tx.QueryRow(ctx, `SELECT attempts >= $4 OR first_at <= now() - make_interval(secs => $5) FROM be_watermark_failure
		WHERE projection = $1 AND partition = $2 AND seq = $3`, w.Name, e.Partition, e.Seq, QuarantineAttempts,
		QuarantineAfter.Seconds()).Scan(&due)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !due) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var subjects []string
	if raw := e.Headers[events.HeaderSubjects]; raw != "" {
		if json.Unmarshal([]byte(raw), &subjects) != nil {
			subjects = nil
		}
	}
	if len(subjects) == 0 || len(subjects) > events.MaxSubjects {
		return false, fmt.Errorf("projection %s: the event at %d/%d keeps failing and lists no subjects to quarantine; "+
			"the watermark stops", w.Name, e.Partition, e.Seq)
	}
	content, _ := json.Marshal(map[string]any{"headers": e.Headers, "payload": string(e.Data)})
	for _, s := range subjects {
		if _, err := tx.Exec(ctx, `INSERT INTO be_watermark_quarantine (projection, subject, impl, epoch, align_gen, partition,
			seq, event) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING`, w.Name, s, st.adopted.Impl,
			st.adopted.Epoch, st.alignGen, e.Partition, e.Seq, string(content)); err != nil {
			return false, err
		}
	}
	if _, err := tx.Exec(ctx, "DELETE FROM be_watermark_failure WHERE projection = $1 AND partition = $2 AND seq = $3",
		w.Name, e.Partition, e.Seq); err != nil {
		return false, err
	}
	w.Log.Error("an event keeps failing; its subjects are quarantined", "event", "watermark_quarantine",
		"projection", w.Name, "partition", e.Partition, "seq", e.Seq, "subjects", len(subjects))
	return true, nil
}

// ---------------------------------------------------------------------------------------------- judging

// Judge answers whether a decision may rely on the projected record (P9.24): usable, provably_absent or unprovable.
// subjects are the subjects asked about and behind the row. One statement: the watermark and the row are read in
// one snapshot.
func (w *Watermarked) Judge(ctx context.Context, tx db.Tx, recordID string, subjects []string) (string, error) {
	if subjects == nil {
		subjects = []string{}
	}
	var in JudgeInput
	var age *float64
	var rowState, rowImpl, rowEpoch, impl, epoch *string
	err := tx.QueryRow(ctx, `SELECT ps.status, coalesce(ps.impl, ''), coalesce(ps.epoch, ''),
			(SELECT CASE WHEN count(*) FILTER (WHERE echo_mp IS NULL) > 0 THEN NULL
				ELSE extract(epoch FROM now() - min(echo_mp))::float8 END
				FROM be_watermark_partition WHERE projection = $1),
			EXISTS (SELECT 1 FROM be_watermark_quarantine WHERE projection = $1 AND subject = ANY($3)),
			r.state, r.impl, r.epoch
		FROM be_projection_source ps LEFT JOIN be_projection r ON r.projection = ps.projection AND r.record_id = $2
		WHERE ps.projection = $1`, w.Name, recordID, subjects).Scan(&in.SourceState, &impl, &epoch, &age, &in.Quarantined,
		&rowState, &rowImpl, &rowEpoch)
	if err != nil {
		return "", err
	}
	in.WatermarkSource, in.ProjectionState, in.Freshness = true, ServingState, w.Freshness.Seconds()
	if age != nil {
		in.HasWatermark, in.WatermarkAge = true, *age
	}
	if rowState != nil {
		in.Row = &JudgedRow{State: *rowState, Adopted: deref(rowImpl) == deref(impl) && deref(rowEpoch) == deref(epoch)}
	}
	return Judge(in), nil
}

// Judged is one record's judgement and, when it is usable, what the projection holds of it.
type Judged struct {
	Verdict string // Usable, ProvablyAbsent or Unprovable
	Version int64
	Data    json.RawMessage
}

// Judgement is JudgeMany's answer: every asked record's judgement, and the source's own state, so a caller can hold
// it to another freshness too (security 03: high-risk and ordinary reads close at different watermark ages).
type Judgement struct {
	Source       string // Usable when aligned, serving and the watermark no older than asked; Unprovable otherwise
	SourceState  string // the source's alignment state (P9.26): JudgeApplied's caller decides on it (P9.34)
	HasWatermark bool
	Age          time.Duration // the watermark's age on the consumer primary's clock
	Records      map[string]Judged
}

// JudgeMany judges many records at once by the same table as Judge (P9.24), at the freshness within: asked maps each
// record ID to the subjects behind it. One statement reads the watermark, the quarantine of the subjects and the rows,
// so they come from one snapshot; a decision over a whole view does not cost a statement per record.
func (w *Watermarked) JudgeMany(ctx context.Context, tx db.Tx, asked map[string][]string, within time.Duration) (Judgement, error) {
	return w.judgeMany(ctx, tx, asked, within, false)
}

// JudgeApplied judges many records by the last applied facts (P9.34), for security 03's tightening-only operations
// when the source is past its freshness: a stalled source is judged by its rows too, the watermark's age does not
// count. The caller decides on SourceState and Age whether a second person is needed (beyond T_tight, no
// watermark, or stalled) and marks the result "basis not proven latest".
func (w *Watermarked) JudgeApplied(ctx context.Context, tx db.Tx, asked map[string][]string) (Judgement, error) {
	return w.judgeMany(ctx, tx, asked, 0, true)
}

func (w *Watermarked) judgeMany(ctx context.Context, tx db.Tx, asked map[string][]string, within time.Duration,
	lastApplied bool) (Judgement, error) {
	ids := make([]string, 0, len(asked))
	subjects := []string{}
	for id, subs := range asked {
		ids = append(ids, id)
		subjects = append(subjects, subs...)
	}
	rows, err := tx.Query(ctx, `WITH src AS (SELECT status, coalesce(impl, '') AS impl, coalesce(epoch, '') AS epoch
				FROM be_projection_source WHERE projection = $1),
			wm AS (SELECT CASE WHEN count(*) FILTER (WHERE echo_mp IS NULL) > 0 OR count(*) = 0 THEN NULL
				ELSE extract(epoch FROM now() - min(echo_mp))::float8 END AS age
				FROM be_watermark_partition WHERE projection = $1),
			q AS (SELECT coalesce(array_agg(DISTINCT subject), '{}') AS subjects FROM be_watermark_quarantine
				WHERE projection = $1 AND subject = ANY($3))
		SELECT src.status, src.impl, src.epoch, wm.age, q.subjects, r.record_id, r.state, r.version, r.data, r.impl, r.epoch
		FROM src CROSS JOIN wm CROSS JOIN q
			LEFT JOIN be_projection r ON r.projection = $1 AND r.record_id = ANY($2)`, w.Name, ids, subjects)
	if err != nil {
		return Judgement{}, err
	}
	defer rows.Close()
	var status, impl, epoch string
	var age *float64
	var quarantined []string
	found := map[string]struct {
		state, impl, epoch string
		version            int64
		data               json.RawMessage
	}{}
	seen := false
	for rows.Next() {
		var id, state, rImpl, rEpoch *string
		var version *int64
		var data []byte
		if err := rows.Scan(&status, &impl, &epoch, &age, &quarantined, &id, &state, &version, &data, &rImpl, &rEpoch); err != nil {
			return Judgement{}, err
		}
		seen = true
		if id != nil {
			found[*id] = struct {
				state, impl, epoch string
				version            int64
				data               json.RawMessage
			}{deref(state), deref(rImpl), deref(rEpoch), *version, data}
		}
	}
	if err := rows.Err(); err != nil {
		return Judgement{}, err
	}
	if !seen {
		return Judgement{}, fmt.Errorf("projection %s: the watermark source is not installed", w.Name)
	}
	q := map[string]bool{}
	for _, s := range quarantined {
		q[s] = true
	}
	base := JudgeInput{LastApplied: lastApplied, WatermarkSource: true, SourceState: status, ProjectionState: ServingState,
		Freshness: within.Seconds()}
	out := Judgement{Source: Unprovable, SourceState: status, Records: make(map[string]Judged, len(asked))}
	if age != nil {
		base.HasWatermark, base.WatermarkAge = true, *age
		out.HasWatermark, out.Age = true, time.Duration(*age*float64(time.Second))
	}
	if Judge(base) != Unprovable { // the source rows of the table, before any record is read
		out.Source = Usable
	}
	for id, subs := range asked {
		in := base
		for _, s := range subs {
			in.Quarantined = in.Quarantined || q[s]
		}
		j := Judged{}
		if r, ok := found[id]; ok {
			in.Row = &JudgedRow{State: r.state, Adopted: r.impl == impl && r.epoch == epoch}
			j.Version = r.version
			if r.state == Complete {
				j.Data = r.data
			}
		}
		j.Verdict = Judge(in)
		if j.Verdict != Usable {
			j.Data = nil
		}
		out.Records[id] = j
	}
	return out, nil
}

// Confirm records an operator's confirmation that a blocked source's configuration is corrected (P9.26); the next
// primary answer decides.
func (w *Watermarked) Confirm(ctx context.Context) error {
	return w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		st, err := w.load(ctx, tx, true)
		if err != nil {
			return err
		}
		w.transition(st, SourceEvent{Kind: "operator_confirms"})
		w.Log.Warn("an operator confirmed a blocked watermark source corrected", "event", "watermark_confirmed",
			"projection", w.Name)
		return w.saveState(ctx, tx, st)
	})
}

// ---------------------------------------------------------------------------------------------- the loop

// Run keeps the projection fresh until ctx ends: every StepEvery (or when an input asks) it re-fetches, aligns or
// pulls, whichever is due.
func (w *Watermarked) Run(ctx context.Context) {
	if w.poke == nil {
		w.poke = make(chan struct{}, 1)
	}
	t := time.NewTicker(StepEvery)
	defer t.Stop()
	for {
		if err := w.Step(ctx); err != nil && ctx.Err() == nil {
			w.Log.Warn("watermark upkeep failed; it tries again", "projection", w.Name, "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-w.poke:
		}
	}
}

// Step does what is due now: a re-fetch (alignment), or a pull when the watermark is half its freshness old.
func (w *Watermarked) Step(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var st *wmState
	var age, last *float64 // the watermark's age; the time since the last pull
	if err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var err error
		if st, err = w.load(ctx, tx, false); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT (SELECT CASE WHEN count(*) FILTER (WHERE echo_mp IS NULL) > 0 THEN NULL
				ELSE extract(epoch FROM now() - min(echo_mp))::float8 END FROM be_watermark_partition WHERE projection = $1),
			(SELECT extract(epoch FROM now() - max(m_p))::float8 FROM be_watermark_pull WHERE projection = $1)`,
			w.Name).Scan(&age, &last)
	}); err != nil {
		return err
	}
	half := w.Freshness.Seconds() / 2
	switch {
	case st.State == Blocked && !st.Confirmed:
		return nil // a misconfigured address: no pull writes echoes into another deployment
	case st.adopted.Impl != "" && (st.refetch || st.State == Aligning):
		return w.align(ctx, st)
	case st.adopted.Impl == "", st.State == Aligned && (age == nil || *age >= half):
		return w.pull(ctx)
	case st.State != Aligned && (last == nil || *last >= half):
		return w.pull(ctx) // stalled or a confirmed correction: look for a larger epoch at the watermark's pace
	}
	return nil
}

func nonce() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// record records a pull's nonce and m_p on the consumer primary's clock, after the clock check (P9.20, P9.21).
func (w *Watermarked) record(ctx context.Context, n string) error {
	return w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var back, epochChanged bool
		var dbEpoch string
		if err := tx.QueryRow(ctx, `SELECT coalesce(now() < max_seen, false), e.generation || '.' || e.timeline,
				coalesce(db_epoch IS DISTINCT FROM e.generation || '.' || e.timeline AND db_epoch IS NOT NULL, false)
			FROM be_watermark_source, be_current_db_epoch() e WHERE projection = $1`, w.Name).
			Scan(&back, &dbEpoch, &epochChanged); err != nil {
			return err
		}
		if back || epochChanged {
			w.Log.Warn("the consumer's clock went back or its epoch changed: every watermark is dropped", "projection", w.Name)
			if _, err := tx.Exec(ctx, "DELETE FROM be_watermark_pull WHERE projection = $1", w.Name); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "UPDATE be_watermark_partition SET echo_mp = NULL WHERE projection = $1", w.Name); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE be_watermark_source SET max_seen = greatest(coalesce(max_seen, now()), now()),
			db_epoch = $2 WHERE projection = $1`, w.Name, dbEpoch); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO be_watermark_pull (projection, nonce, m_p) VALUES ($1, $2, now())", w.Name, n)
		return err
	})
}

func declared(d *sourcepb.Declaration) Identity {
	return Identity{Deployment: d.GetDeployment(), Impl: d.GetImpl(), SystemID: d.GetSystemId(), Epoch: d.GetEpoch(),
		Primary: d.GetPrimary()}
}

// pull is one pull of every partition from its contiguous position (P9.21, P9.22).
func (w *Watermarked) pull(ctx context.Context) error {
	n := nonce()
	if err := w.record(ctx, n); err != nil {
		return err
	}
	var st *wmState
	if err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var err error
		st, err = w.load(ctx, tx, false)
		return err
	}); err != nil {
		return err
	}
	req := &sourcepb.PullRequest{Stream: w.Stream.Name, Nonce: n}
	requested := make([]int64, len(st.parts))
	if st.adopted.Impl != "" {
		for i, p := range st.parts {
			if p.seq == nil {
				continue
			}
			var row sourcepb.RowIdentity
			var id struct {
				EchoNonce string `json:"echo_nonce"`
				Record    *struct {
					RecordID string `json:"record_id"`
					Version  int64  `json:"version"`
					Digest   string `json:"digest"`
				} `json:"record"`
			}
			_ = json.Unmarshal(p.identity, &id)
			if id.Record != nil {
				row.Row = &sourcepb.RowIdentity_Record_{Record: &sourcepb.RowIdentity_Record{RecordId: id.Record.RecordID,
					Version: id.Record.Version, Digest: id.Record.Digest}}
			} else {
				row.Row = &sourcepb.RowIdentity_EchoNonce{EchoNonce: id.EchoNonce}
			}
			requested[i] = *p.seq
			req.Partitions = append(req.Partitions, &sourcepb.PartitionPosition{Partition: uint32(i), Row: &row,
				Position: &sourcepb.Position{Impl: st.adopted.Impl, Epoch: st.adopted.Epoch, Seq: uint64(*p.seq)}})
		}
	}
	var first *sourcepb.Declaration
	for {
		resp, err := w.Pull.Pull(ctx, req)
		if err != nil {
			return err // the watermark stays where it is (hold)
		}
		if first == nil {
			first = resp.GetSource()
			if go1, err := w.identify(ctx, first, requested, resp); err != nil || !go1 {
				return err
			}
		} else if declared(resp.GetSource()) != declared(first) {
			return errors.New("projection: the pages of one pull declare different sources; the pull is abandoned")
		}
		if err := w.applyPage(ctx, resp); err != nil {
			return err
		}
		if resp.GetNextPageToken() == "" {
			return nil
		}
		req = &sourcepb.PullRequest{PageToken: resp.GetNextPageToken()}
	}
}

// identify handles a pull's declaration and the partitions that need a snapshot; it reports whether the content may
// be applied.
func (w *Watermarked) identify(ctx context.Context, d *sourcepb.Declaration, requested []int64, resp *sourcepb.PullResponse) (bool, error) {
	use := false
	err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		st, err := w.load(ctx, tx, true)
		if err != nil {
			return err
		}
		r := declared(d)
		decision := DecideEpoch(st.adopted, r)
		if st.State == Blocked && decision != Block {
			// a confirmed correction: the adopted instance and deployment answering again (P9.26)
			matches := decision == Use || decision == Adopt
			check := "passed"
			for _, pr := range resp.GetPartitions() {
				if pr.GetNeedSnapshot() == sourcepb.NeedSnapshot_NEED_SNAPSHOT_ROLLBACK_DETECTED {
					check = "failed"
				}
			}
			w.transition(st, SourceEvent{Kind: "primary_response", Gen: Generation(r.Epoch), Matches: matches, RowCheck: check})
			return w.saveState(ctx, tx, st)
		}
		switch decision {
		case Establish:
			st.adopted.Impl, st.adopted.SystemID, st.adopted.Epoch = r.Impl, r.SystemID, r.Epoch
			st.invalidate()
			w.transition(st, SourceEvent{Kind: "first_contact", Gen: Generation(r.Epoch)})
		case Adopt:
			st.adopted.SystemID, st.adopted.Epoch = r.SystemID, r.Epoch
			st.invalidate()
			w.transition(st, SourceEvent{Kind: "adopt_epoch", Gen: Generation(r.Epoch)})
		case Stall:
			w.transition(st, SourceEvent{Kind: "rollback", Gen: Generation(r.Epoch)})
		case Block:
			w.transition(st, SourceEvent{Kind: "foreign", Gen: Generation(r.Epoch)})
		case IgnoreIt, Discard:
			return nil
		case Use:
			for _, pr := range resp.GetPartitions() {
				n := int(pr.GetPartition())
				var echo int64
				if pr.GetEcho() != nil {
					echo = int64(pr.GetEcho().GetSeq())
				} else if pr.GetNeedSnapshot() == sourcepb.NeedSnapshot_NEED_SNAPSHOT_UNSPECIFIED {
					continue // a later page carries the echo
				}
				need := map[sourcepb.NeedSnapshot]string{sourcepb.NeedSnapshot_NEED_SNAPSHOT_BEYOND_RETENTION: "beyond_retention",
					sourcepb.NeedSnapshot_NEED_SNAPSHOT_ROLLBACK_DETECTED: "rollback_detected"}[pr.GetNeedSnapshot()]
				switch PullOutcome(requested[n], false, need, echo) {
				case Realign:
					w.transition(st, SourceEvent{Kind: "beyond_retention", Gen: Generation(r.Epoch)})
					return w.saveState(ctx, tx, st)
				case StallIt:
					w.transition(st, SourceEvent{Kind: "rollback", Gen: Generation(r.Epoch)})
					return w.saveState(ctx, tx, st)
				}
			}
			use = true
			return nil
		}
		for i := range st.parts {
			if err := w.savePart(ctx, tx, i, st.parts[i]); err != nil {
				return err
			}
		}
		return w.saveState(ctx, tx, st)
	})
	return use, err
}

// applyPage applies a page's rows partition by partition, contiguously; the echo on the last page advances the
// watermark.
func (w *Watermarked) applyPage(ctx context.Context, resp *sourcepb.PullResponse) error {
	for _, pr := range resp.GetPartitions() {
		if len(pr.GetEvents()) == 0 {
			continue
		}
		var rows []events.Received
		for _, ev := range pr.GetEvents() {
			r, err := events.Receive(ctx, events.Message{Subject: "", Headers: ev.GetHeaders(), Data: ev.GetPayload()}, w.Claims)
			if err != nil {
				return err
			}
			rows = append(rows, r)
		}
		if err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			st, err := w.load(ctx, tx, true)
			if err != nil {
				return err
			}
			if st.State == Blocked {
				return nil
			}
			for _, r := range rows {
				if r.Impl != st.adopted.Impl || r.Epoch != st.adopted.Epoch {
					return nil // the source moved on meanwhile: the next pull sorts it out
				}
				if err := w.applyAt(ctx, tx, st, r, st.State != Aligning); err != nil && !errors.Is(err, errGap) {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// align re-fetches the source (P9.28): an echo-only pull gives P0, the snapshot is read after it, and the completion
// sets every partition's contiguous position to P0 in one transaction.
func (w *Watermarked) align(ctx context.Context, st *wmState) error {
	p0 := make([]int64, w.Stream.Partitions)
	var p0Nonce string
	h := alignHooks{status: st.State,
		started: func(ctx context.Context, gen int64, write func(func(context.Context, db.Tx) error) error) error {
			p0Nonce = nonce()
			if err := w.record(ctx, p0Nonce); err != nil {
				return err
			}
			resp, err := w.Pull.Pull(ctx, &sourcepb.PullRequest{Stream: w.Stream.Name, Nonce: p0Nonce})
			if err != nil {
				return err
			}
			if DecideEpoch(st.adopted, declared(resp.GetSource())) != Use {
				return errors.New("projection: the source's identity changed before the re-fetch began; it starts again")
			}
			for _, pr := range resp.GetPartitions() {
				if pr.GetEcho() == nil {
					return errors.New("projection: the echo-only pull gave no P0")
				}
				p0[pr.GetPartition()] = int64(pr.GetEcho().GetSeq())
			}
			return write(func(ctx context.Context, tx db.Tx) error {
				for i := range p0 {
					v := p0[i]
					if err := w.savePart(ctx, tx, i, partState{p0: &v}); err != nil {
						return err
					}
				}
				_, err := tx.Exec(ctx, `UPDATE be_watermark_source SET align_db_epoch = (SELECT generation || '.' || timeline
					FROM be_current_db_epoch()) WHERE projection = $1`, w.Name)
				return err
			})
		},
		purgedAt: func(id string) *int64 {
			v := p0[w.Stream.PartitionOf(id)]
			return &v
		},
		done: func(ctx context.Context, tx db.Tx, gen int64) error {
			st, err := w.load(ctx, tx, true)
			if err != nil {
				return err
			}
			identity, _ := json.Marshal(map[string]string{"echo_nonce": p0Nonce})
			for i := range st.parts {
				v := p0[i]
				st.parts[i] = partState{seq: &v, identity: identity}
				if err := w.savePart(ctx, tx, i, st.parts[i]); err != nil {
					return err
				}
			}
			st.refetch = false
			if st.State == Aligning {
				w.transition(st, SourceEvent{Kind: "alignment_done", Gen: Generation(st.adopted.Epoch)})
			}
			// subjects whose content the snapshot brought: causes of another epoch or instance, or at or below P0
			if _, err := tx.Exec(ctx, `DELETE FROM be_watermark_quarantine q WHERE projection = $1 AND (impl <> $2 OR epoch <> $3
				OR seq <= ($4::bigint[])[partition + 1])`, w.Name, st.adopted.Impl, st.adopted.Epoch, p0); err != nil {
				return err
			}
			return w.saveState(ctx, tx, st)
		}}
	err := w.Projection.alignTo(ctx, w.DB, w.Snapshot,
		Identity{Deployment: w.Deployment, Impl: st.adopted.Impl, Epoch: st.adopted.Epoch}, h)
	if err == nil {
		w.Log.Info("a watermark source was re-fetched", "event", "watermark_aligned", "projection", w.Name)
	}
	return err
}
