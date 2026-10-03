package acl

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authz"
)

// revisionPattern is changefeed.schema.json's revision: a decimal int64 without a leading zero.
var revisionPattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,18})$`)

// parseRevision reads a provider revision (changefeed.schema.json $defs.revision).
func parseRevision(s string) (int64, error) {
	if !revisionPattern.MatchString(s) {
		return 0, fmt.Errorf("revision %q is not a decimal int64", s)
	}
	return strconv.ParseInt(s, 10, 64)
}

// wireTuple is changefeed.schema.json $defs.tuple; unknown members are ignored.
type wireTuple struct {
	Object struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	} `json:"object"`
	Relation  string  `json:"relation"`
	Subject   string  `json:"subject"`
	ExpiresAt *string `json:"expires_at"`
}

// row checks a tuple and turns it into a projection row.
func (w wireTuple) row() (authz.ACLRow, error) {
	r := authz.ACLRow{RType: w.Object.Type, RID: w.Object.ID, Relation: w.Relation, Subject: w.Subject}
	if r.RType == "" || r.RID == "" || r.Relation == "" || r.Subject == "" {
		return r, errors.New("tuple without type, id, relation or subject")
	}
	if w.ExpiresAt != nil {
		at, err := time.Parse(time.RFC3339, *w.ExpiresAt)
		if err != nil {
			return r, fmt.Errorf("tuple expires_at: %w", err)
		}
		at = at.UTC()
		r.ExpiresAt = &at
	}
	return r, nil
}

// change is one decoded changefeed entry.
type change struct {
	rev int64
	del bool
	row authz.ACLRow
}

// changesPage is a decoded changes_page.
type changesPage struct {
	changes         []change
	next, watermark int64
}

// decodeChanges reads GET /authz/v2/changes (changefeed.schema.json $defs.changes_page) and checks that
// revisions strictly increase.
func decodeChanges(body []byte) (changesPage, error) {
	var w struct {
		Changes []struct {
			Revision string    `json:"revision"`
			Op       string    `json:"op"`
			Tuple    wireTuple `json:"tuple"`
		} `json:"changes"`
		Next      *string `json:"next"`
		Watermark *string `json:"watermark"`
	}
	var p changesPage
	if err := json.Unmarshal(body, &w); err != nil {
		return p, fmt.Errorf("changes page: %w", err)
	}
	if w.Next == nil || w.Watermark == nil {
		return p, errors.New("changes page: next and watermark are required")
	}
	var err error
	if p.next, err = parseRevision(*w.Next); err != nil {
		return p, fmt.Errorf("changes page next: %w", err)
	}
	if p.watermark, err = parseRevision(*w.Watermark); err != nil {
		return p, fmt.Errorf("changes page watermark: %w", err)
	}
	last := int64(-1)
	for i, c := range w.Changes {
		var ch change
		if ch.rev, err = parseRevision(c.Revision); err != nil {
			return p, fmt.Errorf("change %d: %w", i, err)
		}
		if ch.rev <= last {
			return p, fmt.Errorf("change %d: revision %d does not increase", i, ch.rev)
		}
		last = ch.rev
		switch c.Op {
		case "upsert":
		case "delete":
			ch.del = true
		default:
			return p, fmt.Errorf("change %d: unknown op %q", i, c.Op)
		}
		if ch.row, err = c.Tuple.row(); err != nil {
			return p, fmt.Errorf("change %d: %w", i, err)
		}
		p.changes = append(p.changes, ch)
	}
	return p, nil
}

// tuplesPage is a decoded tuples_page.
type tuplesPage struct {
	rows     []authz.ACLRow
	cursor   string
	revision int64
}

// decodeTuples reads GET /authz/v2/tuples (changefeed.schema.json $defs.tuples_page).
func decodeTuples(body []byte) (tuplesPage, error) {
	var w struct {
		Tuples     []wireTuple `json:"tuples"`
		NextCursor *string     `json:"next_cursor"`
		Revision   string      `json:"revision"`
	}
	var p tuplesPage
	if err := json.Unmarshal(body, &w); err != nil {
		return p, fmt.Errorf("tuples page: %w", err)
	}
	if w.NextCursor == nil {
		return p, errors.New("tuples page: next_cursor is required")
	}
	p.cursor = *w.NextCursor
	var err error
	if p.revision, err = parseRevision(w.Revision); err != nil {
		return p, fmt.Errorf("tuples page revision: %w", err)
	}
	for i, t := range w.Tuples {
		r, err := t.row()
		if err != nil {
			return p, fmt.Errorf("tuple %d: %w", i, err)
		}
		p.rows = append(p.rows, r)
	}
	return p, nil
}
