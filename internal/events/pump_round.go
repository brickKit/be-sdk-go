package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/envelope"
)

// round claims due rows, publishes them outside any transaction and marks each one (P12.1).
// It returns how many rows it claimed. Once rows are claimed, publishing and marking run on a
// context detached from ctx, so a shutdown finishes the batch in hand.
func (r *pumpRunner) round(ctx context.Context) (int, error) {
	if ctx.Err() != nil {
		return 0, nil
	}
	rows, err := r.ob.claim(ctx)
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), BatchTimeout)
	defer cancel()
	var sendable []claimedRow
	var msgs []jetstream.Message
	var failed []failedRow
	for _, row := range rows {
		m, err := r.message(row)
		if err != nil {
			r.log.Error("outbox row cannot be published; kept and retried", "id", row.id, "subject", row.subject, "err", err)
			failed = append(failed, failure(row, err))
			continue
		}
		sendable = append(sendable, row)
		msgs = append(msgs, m)
	}
	var published []claimedRow
	if len(msgs) > 0 {
		var more []failedRow
		published, more = r.settle(sendable, r.Bus.PublishBatch(bctx, msgs))
		failed = append(failed, more...)
	}
	if err := r.ob.markPublished(bctx, published); len(published) > 0 && err != nil {
		r.log.Warn("outbox rows published but not marked; the claim expires and the duplicate window drops the copy", "err", err)
	}
	if err := r.ob.markFailed(bctx, failed); len(failed) > 0 && err != nil {
		r.log.Warn("failed outbox rows not marked; the claim expires and they are retried", "err", err)
	}
	return len(rows), nil
}

// settle splits a published batch by its errors and keeps the outage log to one line per outage.
func (r *pumpRunner) settle(rows []claimedRow, errs []error) (published []claimedRow, failed []failedRow) {
	unavailable := false
	for i, row := range rows {
		err := errs[i]
		switch {
		case err == nil:
			published = append(published, row)
			if r.Metrics.Published != nil {
				r.Metrics.Published(row.subject)
			}
			continue
		case errors.Is(err, jetstream.ErrUnavailable):
			unavailable = true
		default:
			r.log.Warn("outbox row not published; retried", "id", row.id, "subject", row.subject,
				"attempts", row.attempts, "err", err)
		}
		failed = append(failed, failure(row, err))
	}
	switch {
	case unavailable && !r.outage:
		r.outage = true
		r.log.Warn("event bus unavailable; outbox rows stay PENDING and are retried", "rows", len(failed))
	case !unavailable && len(published) > 0 && r.outage:
		r.outage = false
		r.log.Info("event bus available again; outbox publishing resumed")
	}
	return published, failed
}

func failure(row claimedRow, err error) failedRow {
	msg := err.Error()
	if len(msg) > maxLastError {
		msg = strings.ToValidUTF8(msg[:maxLastError], "")
	}
	return failedRow{id: row.id, createdAt: row.createdAt, delay: pumpBackoff(row.attempts), err: msg}
}

// message builds the bus message of a row: the envelope headers (envelope.Headers) with the
// producer's version and contract file taken from the row's stored ce-dataschema, so a row is
// published as the version that wrote it.
func (r *pumpRunner) message(row claimedRow) (jetstream.Message, error) {
	var stored map[string]string
	if err := json.Unmarshal(row.headers, &stored); err != nil {
		return jetstream.Message{}, fmt.Errorf("stored headers: %w", err)
	}
	version, file, ok := parseDataSchema(stored[envelope.HeaderDataSchema], r.ComponentID, row.subject)
	if !ok {
		return jetstream.Message{}, fmt.Errorf("row has no valid ce-dataschema for %s", r.ComponentID)
	}
	h, err := envelope.Headers(envelope.Producer{ComponentID: r.ComponentID, Version: version, EventsFile: file},
		row.envelopeRow(), false)
	if err != nil {
		return jetstream.Message{}, err
	}
	delete(h, envelope.HeaderNatsMsgID) // set from ID by the bus
	return jetstream.Message{Subject: row.subject, ID: row.id, Header: h, Data: row.payload}, nil
}

// parseDataSchema splits <component>@<version>/contracts/events/<file>#<subject>.
func parseDataSchema(ds, componentID, subject string) (version, file string, ok bool) {
	rest, ok := strings.CutPrefix(ds, componentID+"@")
	if !ok {
		return "", "", false
	}
	rest, ok = strings.CutSuffix(rest, "#"+subject)
	if !ok {
		return "", "", false
	}
	version, file, ok = strings.Cut(rest, "/contracts/events/")
	return version, file, ok && version != "" && file != ""
}
