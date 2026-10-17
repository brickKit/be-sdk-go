package errs

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/durationpb"
)

var grpcCodes = map[Code]codes.Code{
	InvalidArgument: codes.InvalidArgument, FailedPrecondition: codes.FailedPrecondition, OutOfRange: codes.OutOfRange,
	Unauthenticated: codes.Unauthenticated, PermissionDenied: codes.PermissionDenied, NotFound: codes.NotFound,
	AlreadyExists: codes.AlreadyExists, Aborted: codes.Aborted, ResourceExhausted: codes.ResourceExhausted,
	Cancelled: codes.Canceled, Internal: codes.Internal, Unknown: codes.Unknown, DataLoss: codes.DataLoss,
	Unimplemented: codes.Unimplemented, Unavailable: codes.Unavailable, DeadlineExceeded: codes.DeadlineExceeded,
}

// GRPCCode is the grpc-go code of a protocol code.
func GRPCCode(c Code) codes.Code {
	if g, ok := grpcCodes[c]; ok {
		return g
	}
	return codes.Unknown
}

// CodeOf is the protocol code of a grpc-go code ("OK" for codes.OK).
func CodeOf(g codes.Code) Code {
	if g == codes.OK {
		return "OK"
	}
	for c, x := range grpcCodes {
		if x == g {
			return c
		}
	}
	return Unknown
}

// GRPCStatus renders the error for the system surface (P3.2): exactly one ErrorInfo, a BadRequest for field errors,
// a RetryInfo when the caller should retry later. INTERNAL carries nothing but the generic detail (P3.4). grpc-go
// calls it for any returned error that has the method.
func (e *Error) GRPCStatus() *status.Status {
	p := e.Problem("")
	st := status.New(GRPCCode(e.Code), p.Detail)
	if p.Reason == ReasonInternal {
		st = status.New(codes.Internal, p.Detail)
	}
	details := []protoadapt.MessageV1{protoadapt.MessageV1Of(&errdetails.ErrorInfo{Reason: p.Reason, Domain: p.Domain,
		Metadata: p.Metadata})}
	if len(p.Violations) > 0 {
		br := &errdetails.BadRequest{}
		for _, v := range p.Violations {
			br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{Field: v.Field,
				Reason: v.Reason, Description: v.Description})
		}
		details = append(details, protoadapt.MessageV1Of(br))
	}
	if e.RetryAfter > 0 && p.Reason != ReasonInternal {
		details = append(details, protoadapt.MessageV1Of(&errdetails.RetryInfo{RetryDelay: durationpb.New(e.RetryAfter)}))
	}
	if withDetails, err := st.WithDetails(details...); err == nil {
		return withDetails
	}
	return st
}

// FromGRPC reads a system-surface error. ok is false when the status carries no ErrorInfo (a transport failure or a
// server outside the protocol); the code is still returned.
func FromGRPC(err error) (e *Error, code Code, ok bool) {
	st, isStatus := status.FromError(err)
	if !isStatus {
		return nil, Unknown, false
	}
	code = CodeOf(st.Code())
	for _, d := range st.Details() {
		info, isInfo := d.(*errdetails.ErrorInfo)
		if !isInfo {
			continue
		}
		e = &Error{Reason: info.Reason, Domain: info.Domain, Code: code, HTTP: statusOf(info.Domain, info.Reason, code),
			Title: titleOf(info.Domain, info.Reason), Detail: st.Message(), Metadata: info.Metadata, cause: err}
		ok = true
	}
	if !ok {
		return nil, code, false
	}
	for _, d := range st.Details() {
		switch x := d.(type) {
		case *errdetails.BadRequest:
			for _, v := range x.FieldViolations {
				e.Violations = append(e.Violations, Violation{Field: v.Field, Reason: v.Reason, Description: v.Description})
			}
		case *errdetails.RetryInfo:
			e.RetryAfter = x.RetryDelay.AsDuration()
		}
	}
	return e, code, true
}

func statusOf(domain, reason string, code Code) int {
	if domain == PlatformDomain {
		if entry, ok := platform[reason]; ok {
			return entry.status()
		}
	}
	return HTTPStatus(code)
}

func titleOf(domain, reason string) string {
	if domain == PlatformDomain {
		if entry, ok := platform[reason]; ok {
			return entry.Title
		}
	}
	return ""
}
