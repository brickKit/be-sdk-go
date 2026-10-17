// Package idem gives commands idempotency keys (chapter 04, P4.21-P4.25): the claim, the command's writes and its
// outcome commit in one transaction; the same key and request again returns the stored outcome without running the
// command, the same key with another request is IDEMPOTENCY_KEY_REUSED.
package idem

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// Retention is how long a key's record is kept (P4.25; assembly project protocol 10).
const Retention = 24 * time.Hour

// Request is one command's idempotency identity: who sent it, the key, and what the request was.
type Request struct {
	Principal string // the user's subject, or the calling component on the system surface
	Key       string
	Operation string
	Target    string // "<METHOD> <path>[?<query>]" or the full gRPC method
	Body      []byte // the body's raw bytes; a gRPC message deterministically marshaled
}

// ValidKey reports whether k is 1-255 visible ASCII characters (P4.21).
func ValidKey(k string) bool {
	if len(k) < 1 || len(k) > 255 {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return false
		}
	}
	return true
}

// Fingerprint is the request's canonical digest (P4.22).
func (r Request) Fingerprint() (string, error) {
	sum := sha256.Sum256(r.Body)
	return canon.Digest("be.idempotency.request", 1, map[string]any{"principal": canon.Text(r.Principal),
		"operation": canon.Text(r.Operation), "target": canon.Text(r.Target), "body_sha256": hex.EncodeToString(sum[:])})
}

func keyError(key string) error {
	reason := "is missing"
	if key != "" {
		reason = "must be 1-255 visible ASCII characters"
	}
	return errs.New(errs.ReasonRequestInvalid, "the Idempotency-Key header "+reason).
		WithViolations(errs.Violation{Field: "Idempotency-Key", Reason: "INVALID", Description: reason})
}

// FromHTTP is a user-surface command's request: the authenticated subject, the Idempotency-Key header, the route's
// operation, the method and request URI, and the body (read once; BindJSON still works afterwards).
func FromHTTP(c *httpx.Context) (Request, error) {
	key := c.Header(calls.HeaderIdempotencyKey)
	if !ValidKey(key) {
		return Request{}, keyError(key)
	}
	s, ok := authz.From(c.Ctx())
	if !ok {
		return Request{}, errs.New(errs.ReasonInternal, "").Wrap(errors.New("idempotency keys need an authenticated route"))
	}
	body, err := c.Body()
	if err != nil {
		return Request{}, err
	}
	return Request{Principal: s.Principal, Key: key, Operation: c.Operation(), Body: body,
		Target: c.Request().Method + " " + c.Request().URL.RequestURI()}, nil
}

// FromGRPC is a system-surface command's request: the calling component, the idempotency-key metadata, the
// operation, the full method and the message.
func FromGRPC(ctx context.Context, operation string, msg proto.Message) (Request, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	var key string
	if v := md.Get(calls.MetadataIdempotencyKey); len(v) > 0 {
		key = v[0]
	}
	if !ValidKey(key) {
		return Request{}, keyError(key)
	}
	caller, ok := grpcx.Caller(ctx)
	method, _ := grpc.Method(ctx)
	if !ok || method == "" {
		return Request{}, errs.New(errs.ReasonInternal, "").Wrap(errors.New("idempotency keys need a system-surface call"))
	}
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return Request{}, err
	}
	return Request{Principal: caller, Key: key, Operation: operation, Target: method, Body: body}, nil
}

// Outcome is what the command decided (P4.24): a status and a result such as the record's or process's ID. A
// replay returns it, and the handler answers by reading under the caller's current authorization.
type Outcome struct {
	Status int
	Result json.RawMessage
}

// Do runs fn once per key (P4.23): in one transaction it claims the key, runs fn and stores its outcome; an expired
// record not yet cleaned up counts as gone, so the key is claimed afresh (P4.25). A stored
// outcome for the same request is returned with replayed=true and fn does not run; a different request under the
// key is IDEMPOTENCY_KEY_REUSED; a first use not yet committed is IDEMPOTENCY_IN_PROGRESS. When fn fails, nothing
// is kept and the key can be used again.
func Do(ctx context.Context, d *db.DB, r Request, fn func(ctx context.Context, tx db.Tx) (Outcome, error)) (out Outcome, replayed bool, err error) {
	fp, err := r.Fingerprint()
	if err != nil {
		return Outcome{}, false, err
	}
	err = d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		out, replayed = Outcome{}, false
		tag, err := tx.Exec(ctx, `INSERT INTO be_idempotency (principal, key, fingerprint, operation, status, expires_at)
			VALUES ($1, $2, $3, $4, 0, now() + make_interval(secs => $5))
			ON CONFLICT (principal, key) DO UPDATE SET fingerprint = excluded.fingerprint, operation = excluded.operation,
				status = 0, result = NULL, created_at = now(), expires_at = excluded.expires_at
			WHERE be_idempotency.expires_at <= now()`,
			r.Principal, r.Key, fp, r.Operation, Retention.Seconds())
		var pg *pgconn.PgError
		if errors.As(err, &pg) && pg.Code == "55P03" {
			return errs.New(errs.ReasonIdempotencyInProgress, "the first request with this key has not finished")
		}
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			replayed = true
			return stored(ctx, tx, r, fp, &out)
		}
		if out, err = fn(ctx, tx); err != nil {
			return err
		}
		if out.Status == 0 {
			out.Status = 200
		}
		var result any
		if out.Result != nil {
			result = string(out.Result)
		}
		_, err = tx.Exec(ctx, "UPDATE be_idempotency SET status = $3, result = $4::jsonb WHERE principal = $1 AND key = $2",
			r.Principal, r.Key, out.Status, result)
		return err
	})
	return out, replayed, err
}

func stored(ctx context.Context, tx db.Tx, r Request, fp string, out *Outcome) error {
	var have string
	var result []byte
	err := tx.QueryRow(ctx, "SELECT fingerprint, status, result FROM be_idempotency WHERE principal = $1 AND key = $2",
		r.Principal, r.Key).Scan(&have, &out.Status, &result)
	if errors.Is(err, pgx.ErrNoRows) {
		// the earlier claim was removed between the insert and this read (cleanup); the caller may retry
		return errs.New(errs.ReasonIdempotencyInProgress, "the key's earlier use is being removed; retry")
	}
	if err != nil {
		return err
	}
	if have != fp {
		return errs.New(errs.ReasonIdempotencyKeyReused, "this idempotency key was used for a different request")
	}
	out.Result = result
	return nil
}
