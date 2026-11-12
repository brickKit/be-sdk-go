package flow

// The decision's rules as a pure function (be-protocol chapter 17: P17.10's checks in their order, P17.11's replay,
// P17.2's forms, P17.13's outcomes); the runtime gathers the situation in its transaction and carries the answer out.

// TaskState is a task's state (P17.9).
type TaskState string

const (
	Pending    TaskState = "pending"
	Decided    TaskState = "decided"
	Superseded TaskState = "superseded"
	Paused     TaskState = "paused"
)

// InstanceState is an instance's state (P17.4).
type InstanceState string

const (
	Running            InstanceState = "running"
	Approved           InstanceState = "approved"
	Rejected           InstanceState = "rejected"
	Returned           InstanceState = "returned"
	Withdrawn          InstanceState = "withdrawn"
	SupersededInstance InstanceState = "superseded"
)

// Qualification is whether the decider still qualifies under the step's assignment now (P17.10).
type Qualification string

const (
	Qualified    Qualification = "qualified"
	NotQualified Qualification = "not_qualified"
	Unknown      Qualification = "unknown" // the employee projection is not complete
)

// Choice is a decision.
type Choice string

const (
	Approve Choice = "approve"
	Reject  Choice = "reject"
)

// The reasons a decision is refused (be-protocol chapter 03's catalog).
const (
	ReasonTaskNotPending       = "TASK_NOT_PENDING"
	ReasonVersionConflict      = "VERSION_CONFLICT"
	ReasonSelfApproval         = "SELF_APPROVAL"
	ReasonNotAssignee          = "NOT_ASSIGNEE"
	ReasonPermissionDenied     = "PERMISSION_DENIED"
	ReasonNotQualified         = "NOT_QUALIFIED"
	ReasonQualificationUnknown = "QUALIFICATION_UNKNOWN"
	ReasonRevisionChanged      = "REVISION_CHANGED"
)

// Situation is what a decision meets, gathered under the locks of P17.17.
type Situation struct {
	Task            TaskState
	Instance        InstanceState
	VersionMatches  bool
	Self            bool // the decider's person is the initiator's (D10)
	Candidate       bool
	Operation       bool // the decider holds the definition's operation
	Qualification   Qualification
	RevisionMatches bool // the instance's key revision is the document's now
	Replay          bool // the idempotency key has a result already
}

// Outcome is what the decision does.
type Outcome struct {
	Replay          bool   // answer the first result; nothing changes
	Refused         string // the first check that failed; nothing changes
	Task            TaskState
	Instance        InstanceState
	SupersedeOthers bool // the step's other pending tasks become superseded
	NextStep        bool // make the next step's tasks
	Changed         bool
}

// Decide answers a decision: the replay first, then P17.10's checks in order, then the outcome of P17.13 (others is
// whether the step has other pending tasks, last whether this is the last step, onReject the definition's outcome of a
// rejection).
func Decide(s Situation, c Choice, others, last bool, onReject InstanceState) Outcome {
	keep := Outcome{Task: s.Task, Instance: s.Instance}
	if s.Replay {
		keep.Replay = true
		return keep
	}
	qual := map[Qualification]string{NotQualified: ReasonNotQualified, Unknown: ReasonQualificationUnknown}[s.Qualification]
	for _, r := range []struct {
		fails  bool
		reason string
	}{
		{s.Task != Pending || s.Instance != Running, ReasonTaskNotPending},
		{!s.VersionMatches, ReasonVersionConflict},
		{s.Self, ReasonSelfApproval},
		{!s.Candidate, ReasonNotAssignee},
		{!s.Operation, ReasonPermissionDenied},
		{qual != "", qual},
		{!s.RevisionMatches, ReasonRevisionChanged},
	} {
		if r.fails {
			keep.Refused = r.reason
			return keep
		}
	}
	out := Outcome{Task: Decided, Instance: Running, SupersedeOthers: others, Changed: true}
	switch {
	case c == Reject:
		out.Instance = onReject
	case last:
		out.Instance = Approved
	default:
		out.NextStep = true
	}
	return out
}
