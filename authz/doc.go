// Package authz is the component side of authorization (protocol P8.7-P8.18): the resource, operation and field
// declarations, the bounded local view held and verified against the authorization member, three-valued
// evaluation of complete segments on one record, and the compilation of a subject's segments into a SQL predicate
// with a per-row hit bitmap for field masks.
//
// The files:
//
//   - defs.go: resource, operation and field declarations and their check; submit.go: the definitions sealed and
//     submitted to the authorization member (D76).
//   - view.go: the bounded local view, held and verified (Holder; security 03); source.go: the authorization member's
//     verification over the system surface (GRPCSource, OpenView).
//   - segment.go: a grant's segment, its canonical form and digest; eval.go: three-valued evaluation of segments on
//     one record (security 07).
//   - subject.go: one verified request's authorization (Subject: Check, Decider, List, Mask).
//   - sql.go: a subject's segments compiled into a SQL predicate with the per-row hit bitmap for field masks (P8.17).
//   - change.go: the candidate digest of a change request; owner.go: the deployment owner's confirmation of a
//     protected change (security 08).
//   - refuse.go: segments that do not fit the resource definitions are refused from the view (P8.14).
package authz
