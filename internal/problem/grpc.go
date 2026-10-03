package problem

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ToStatus renders the public form of e as a gRPC status (P4.2): the detail text as the message,
// ErrorInfo always, BadRequest with the violations and RetryInfo with the delay when present.
func (c *Catalogue) ToStatus(e *Error, locale string) *status.Status {
	pub := Public(e)
	_, detail := c.Text(pub, locale)
	st := status.New(pub.Code, detail)
	details := []any{&errdetails.ErrorInfo{Reason: pub.Reason, Domain: pub.Domain, Metadata: pub.Metadata}}
	if len(pub.Violations) > 0 {
		br := &errdetails.BadRequest{}
		for _, v := range pub.Violations {
			br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{
				Field: v.Field, Reason: v.Reason, Description: v.Description})
		}
		details = append(details, br)
	}
	if e.RetryAfter > 0 && (pub.Code == codes.ResourceExhausted || pub.Code == codes.Unavailable) {
		details = append(details, &errdetails.RetryInfo{RetryDelay: durationpb.New(e.RetryAfter)})
	}
	return withDetails(st, details)
}

func withDetails(st *status.Status, details []any) *status.Status {
	for _, d := range details {
		var next *status.Status
		var err error
		switch v := d.(type) {
		case *errdetails.ErrorInfo:
			next, err = st.WithDetails(v)
		case *errdetails.BadRequest:
			next, err = st.WithDetails(v)
		case *errdetails.RetryInfo:
			next, err = st.WithDetails(v)
		}
		if err == nil && next != nil {
			st = next
		}
	}
	return st
}

// FromStatus restores a gRPC status as an error, keeping a dependency's domain and reason (P4.2, P4.9).
func FromStatus(st *status.Status) *Error {
	e := &Error{Code: st.Code(), Detail: st.Message()}
	for _, d := range st.Details() {
		switch v := d.(type) {
		case *errdetails.ErrorInfo:
			e.Domain, e.Reason, e.Metadata = v.GetDomain(), v.GetReason(), v.GetMetadata()
		case *errdetails.BadRequest:
			for _, f := range v.GetFieldViolations() {
				e.Violations = append(e.Violations, Violation{Field: f.GetField(), Reason: f.GetReason(),
					Description: f.GetDescription()})
			}
		case *errdetails.RetryInfo:
			e.RetryAfter = v.GetRetryDelay().AsDuration()
		}
	}
	return e
}

// grpcStatusOf finds a gRPC status in an error chain.
func grpcStatusOf(err error) (*status.Status, bool) {
	var se interface{ GRPCStatus() *status.Status }
	if errors.As(err, &se) {
		st := se.GRPCStatus()
		return st, st != nil && st.Code() != codes.OK
	}
	return nil, false
}
