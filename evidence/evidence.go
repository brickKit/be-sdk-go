// Package evidence is the recorder's side of the evidence (be-protocol chapter 12, P12.15-P12.18): what a full
// restore must not lose is written to the evidence store (the audit component). A tightening is written in two
// phases (the intent before the business transaction, the conclusion in it); general records and conclusions go
// through the local evidence outbox, which the be.evidence.transfer job empties; every evidence-bearing change writes
// its change history row, by which a replay knows it is already reflected.
package evidence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/jackc/pgx/v5"
)

// RecordType is every evidence record's type (P12.9).
const RecordType = "be.evidence.record"

// IntentTimeout is how long the intent waits for the evidence store, and then the alert channel (P12.16).
const IntentTimeout = 2 * time.Second

// Object is what a record changes, with its version before the change.
type Object struct {
	Type, ID, Version string
}

// Store is the evidence store's append (the audit component's EvidenceService); Statuses answers each sealed record
// in order: "accepted", "duplicate", "conflict" or "invalid".
type Store interface {
	Append(ctx context.Context, recs []Sealed) ([]string, error)
}

// Alerter is the alert channel (runprofile.Store.Alert): whether its machine-readable copy was written.
type Alerter interface {
	Alert(ctx context.Context, body any) bool
}

// Sealed is a record's canonical bytes and digest.
type Sealed struct {
	ID, Phase, Digest string
	Record            []byte
}

// Writer writes one member's evidence.
type Writer struct {
	DB        *db.DB
	Store     Store   // nil: the evidence store is not configured; every intent goes to the alert channel
	Alert     Alerter // nil: no alert channel; a failed intent is a double failure
	Component string
	Instance  string
	Log       *slog.Logger
	Now       func() time.Time
}

func (w *Writer) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

func (w *Writer) log() *slog.Logger {
	if w.Log != nil {
		return w.Log
	}
	return slog.New(slog.DiscardHandler)
}

// record builds one be.evidence.record's fields (P12.9).
func (w *Writer) record(id, category, phase, subject string, obj Object, change map[string]any, pos *position) map[string]any {
	o := map[string]any{"type": obj.Type, "id": obj.ID}
	if obj.Version != "" {
		o["version"] = obj.Version
	}
	r := map[string]any{"record_type": RecordType, "schema": "1.0", "id": id,
		"recorder": map[string]any{"component": w.Component, "instance": w.Instance}, "category": category,
		"phase": phase, "object": o, "change": change, "at": canon.Time(w.now())}
	if subject != "" {
		r["subject"] = subject
	}
	if pos != nil {
		r["position"] = map[string]any{"epoch": pos.epoch, "commit_seq": pos.seq}
	}
	return r
}

// Seal is a record's canonical bytes and digest (the security records' canonical form, prefix be.evidence.record.v1).
func Seal(r map[string]any) (Sealed, error) {
	raw, err := canon.Encode(r)
	if err != nil {
		return Sealed{}, err
	}
	return Sealed{ID: r["id"].(string), Phase: r["phase"].(string), Record: raw,
		Digest: canon.DigestBytes(RecordType, 1, raw)}, nil
}

// Pending is a tightening whose intent is written: its conclusion follows in the business transaction.
type Pending struct {
	w        *Writer
	ID       string
	category string
	subject  string
	obj      Object
	change   map[string]any
	// Intent is where the intent went: "store", "alert" or "lost" (both failed: the double failure).
	Intent string
}

// Intent writes a tightening's intent before the business transaction (P12.16): to the evidence store within
// IntentTimeout, else to the alert channel within the same limit, else the double failure is logged; the caller
// goes on in every case. The change is the change itself (what is removed), never the resulting state.
func (w *Writer) Intent(ctx context.Context, category, subject string, obj Object, change map[string]any) (*Pending, error) {
	p := &Pending{w: w, ID: ids.New().String(), category: category, subject: subject, obj: obj, change: change}
	rec := w.record(p.ID, category, "intent", subject, obj, change, nil)
	s, err := Seal(rec)
	if err != nil {
		return nil, err
	}
	if w.Store != nil {
		c, cancel := context.WithTimeout(context.WithoutCancel(ctx), IntentTimeout)
		st, err := w.Store.Append(c, []Sealed{s})
		cancel()
		if err == nil && len(st) == 1 && (st[0] == "accepted" || st[0] == "duplicate") {
			p.Intent = "store"
			return p, nil
		}
		w.log().Warn("the evidence store did not take an intent: it goes to the alert channel", "id", p.ID,
			"category", category, "error", fmt.Sprint(err, st))
	}
	if w.Alert != nil && w.Alert.Alert(ctx, rec) {
		p.Intent = "alert"
		return p, nil
	}
	w.log().Error("an intent reached neither the evidence store nor the alert channel", "event", "evidence_double_failure",
		"id", p.ID, "category", category)
	p.Intent = "lost"
	return p, nil
}

type position struct {
	epoch string
	seq   int64
}

// commit assigns the next commit sequence under the counter's row lock (P12.17): the evidence transactions of one
// database are serialized from here to their commit, so the sequence's order is the commit order.
func commit(ctx context.Context, tx db.Tx) (position, error) {
	var g, tl int64
	if err := tx.QueryRow(ctx, "SELECT generation, timeline FROM be_current_db_epoch()").Scan(&g, &tl); err != nil {
		return position{}, err
	}
	epoch := strconv.FormatInt(g, 10) + "." + strconv.FormatInt(tl, 10)
	var last int64
	if err := tx.QueryRow(ctx, "SELECT last_seq FROM be_evidence_counter FOR UPDATE").Scan(&last); err != nil {
		return position{}, err
	}
	last++
	if _, err := tx.Exec(ctx, "UPDATE be_evidence_counter SET epoch = $1, last_seq = $2", epoch, last); err != nil {
		return position{}, err
	}
	return position{epoch: epoch, seq: last}, nil
}

// history writes the change history row and queues the record in the evidence outbox, in the business transaction.
func (w *Writer) history(ctx context.Context, tx db.Tx, id, category, phase, subject string, obj Object,
	change map[string]any) error {
	pos, err := commit(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO be_change_history (evidence_id, object_type, object_id, version_before, epoch,
		commit_seq) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6)`, id, obj.Type, obj.ID, obj.Version, pos.epoch, pos.seq); err != nil {
		return err
	}
	return w.queue(ctx, tx, w.record(id, category, phase, subject, obj, change, &pos))
}

func (w *Writer) queue(ctx context.Context, tx db.Tx, rec map[string]any) error {
	s, err := Seal(rec)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO be_evidence_outbox (id, phase, record, digest) VALUES ($1, $2, $3, $4)
		ON CONFLICT (id, phase) DO NOTHING`, s.ID, s.Phase, s.Record, s.Digest)
	return err
}

// Effective is the tightening's conclusion in the business transaction (P12.16, P12.17): the change history row and
// the effective record, at the commit sequence.
func (p *Pending) Effective(ctx context.Context, tx db.Tx) error {
	return p.w.history(ctx, tx, p.ID, p.category, "effective", p.subject, p.obj, p.change)
}

// Cancelled records that the business transaction did not commit (P12.16), in a transaction of its own.
func (p *Pending) Cancelled(ctx context.Context) error {
	return p.w.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		return p.w.queue(ctx, tx, p.w.record(p.ID, p.category, "cancelled", p.subject, p.obj, p.change, nil))
	})
}

// General records a change that needs no intent (a widening, an external command's outcome) in the business
// transaction (P12.15): its change history row and the general record. It returns the record's id.
func (w *Writer) General(ctx context.Context, tx db.Tx, category, subject string, obj Object, change map[string]any) (string, error) {
	id := ids.New().String()
	return id, w.history(ctx, tx, id, category, "general", subject, obj, change)
}

// Reflected reports whether a record's change is in this database (P12.18): its id is in the change history.
func Reflected(ctx context.Context, tx db.Tx, id string) (bool, error) {
	var one int
	err := tx.QueryRow(ctx, "SELECT 1 FROM be_change_history WHERE evidence_id = $1", id).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
