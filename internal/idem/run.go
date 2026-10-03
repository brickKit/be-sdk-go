package idem

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Idempotent is the one-step command (P13.3, P13.6): claim the key, run do, store its result, all in
// the caller's transaction q. It returns do's result (or the replayed one), the HTTP status to answer
// with (c.Status, or the stored status on replay) and whether it was a replay.
//
//	c.Key == ""        → no idempotency: run do, store nothing
//	Claim errors       → ErrMismatch / ErrInProgress / a database error; do is not run
//	Claim finds DONE   → decode the stored body into T, replayed = true; do is not run
//	otherwise          → run do; on error release the claim (best effort) and return the error,
//	                     so the claim never outlives a failed attempt even if the caller commits;
//	                     on success Complete with {http: c.Status, body: result}
//
// T may be a proto message (a gRPC response): it is stored as protojson and decoded into a fresh
// message; anything else goes through encoding/json.
func Idempotent[T any](ctx context.Context, q Querier, c Command, now time.Time, do func() (T, error)) (res T, status int, replayed bool, err error) {
	status = c.Status
	if status == 0 {
		status = 200
	}
	if c.Key == "" {
		res, err = do()
		return res, status, false, err
	}
	p, err := Claim(ctx, q, c, now)
	if err != nil {
		return res, 0, false, err
	}
	if p.Found {
		res, err = DecodeBody[T](p.Result.Body)
		return res, p.Result.HTTP, err == nil, err
	}
	res, err = do()
	if err != nil {
		_ = Release(ctx, q, c)
		return res, 0, false, err
	}
	body, err := EncodeBody(res)
	if err != nil {
		return res, 0, false, err
	}
	if err := Complete(ctx, q, c, Result{HTTP: status, Body: body}, now); err != nil {
		return res, 0, false, err
	}
	return res, status, false, nil
}

// EncodeBody is the stored form of a result (Result.Body): protojson for a proto message,
// encoding/json otherwise. The root uses it for Tx.IdemComplete.
func EncodeBody(v any) (json.RawMessage, error) {
	if m, ok := v.(proto.Message); ok {
		return protojson.Marshal(m)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("idem: encode result: %w", err)
	}
	return b, nil
}

// DecodeBody reads a stored body into a T; for a proto message pointer type it allocates a fresh
// message. The root uses it to decode a Prior.Result of Tx.IdemClaim / IdemLookup.
func DecodeBody[T any](b json.RawMessage) (T, error) {
	var res T
	if _, ok := any(res).(proto.Message); ok {
		rt := reflect.TypeOf(res)
		if rt.Kind() == reflect.Pointer {
			res = reflect.New(rt.Elem()).Interface().(T)
			if err := protojson.Unmarshal(b, any(res).(proto.Message)); err != nil {
				return res, fmt.Errorf("idem: decode stored result: %w", err)
			}
			return res, nil
		}
	}
	if err := json.Unmarshal(b, &res); err != nil {
		return res, fmt.Errorf("idem: decode stored result: %w", err)
	}
	return res, nil
}
