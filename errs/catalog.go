package errs

// PlatformDomain is the domain of the reasons the protocol itself defines (P3.7).
const PlatformDomain = "be"

// Platform reasons (protocol chapter 03).
const (
	ReasonRequestInvalid           = "REQUEST_INVALID"
	ReasonBodyTooLarge             = "BODY_TOO_LARGE"
	ReasonCursorInvalid            = "CURSOR_INVALID"
	ReasonBatchTooLarge            = "BATCH_TOO_LARGE"
	ReasonTokenInvalid             = "TOKEN_INVALID"
	ReasonTokenStale               = "TOKEN_STALE"
	ReasonServiceCredentialInvalid = "SERVICE_CREDENTIAL_INVALID"
	ReasonPermissionDenied         = "PERMISSION_DENIED"
	ReasonContainmentNotProven     = "CONTAINMENT_NOT_PROVEN"
	ReasonNotFound                 = "NOT_FOUND"
	ReasonQueryNotCompletable      = "QUERY_NOT_COMPLETABLE"
	ReasonFieldNotQueryable        = "FIELD_NOT_QUERYABLE"
	ReasonQueryPlanChanged         = "QUERY_PLAN_CHANGED"
	ReasonChangeKeyConflict        = "CHANGE_KEY_CONFLICT"
	ReasonChangeRevisionStale      = "CHANGE_REVISION_STALE"
	ReasonLastAdminProtected       = "LAST_ADMIN_PROTECTED"
	ReasonIdempotencyKeyReused     = "IDEMPOTENCY_KEY_REUSED"
	ReasonIdempotencyInProgress    = "IDEMPOTENCY_IN_PROGRESS"
	ReasonLockTimeout              = "LOCK_TIMEOUT"
	ReasonTxConflict               = "TX_CONFLICT"
	ReasonStatementTimeout         = "STATEMENT_TIMEOUT"
	ReasonServiceBusy              = "SERVICE_BUSY"
	ReasonOutboundLimit            = "OUTBOUND_LIMIT"
	ReasonCircuitOpen              = "CIRCUIT_OPEN"
	ReasonDependencyUnavailable    = "DEPENDENCY_UNAVAILABLE"
	ReasonDeadlineBudgetExhausted  = "DEADLINE_BUDGET_EXHAUSTED"
	ReasonDeadlineExceeded         = "DEADLINE_EXCEEDED"
	ReasonRequestCancelled         = "REQUEST_CANCELLED"
	ReasonNotReady                 = "NOT_READY"
	ReasonAuthorizationUnprovable  = "AUTHORIZATION_UNPROVABLE"
	ReasonAuthorizationViewExpired = "AUTHORIZATION_VIEW_EXPIRED"
	ReasonRecoveryInProgress       = "RECOVERY_IN_PROGRESS"
	ReasonMemberSwitchInProgress   = "MEMBER_SWITCH_IN_PROGRESS"
	ReasonDefinitionPending        = "DEFINITION_PENDING"
	ReasonCapabilityUnsupported    = "CAPABILITY_UNSUPPORTED"
	ReasonResultUnknown            = "RESULT_UNKNOWN"
	ReasonAuditCapacityExhausted   = "AUDIT_CAPACITY_EXHAUSTED"
	ReasonInternal                 = "INTERNAL"
)

// Retryability of a reason, as the catalog states it.
const (
	RetryNo           = "no"
	RetryYes          = "yes"
	RetryAfterRefresh = "after_refresh"
	RetryQueryFirst   = "query_first"
	RetryNone         = "none"
)

// Entry is one reason of the catalog.
type Entry struct {
	Reason string
	Code   Code
	HTTP   int // 0: by the code
	Retry  string
	Title  string
}

var platform = map[string]Entry{}

func init() {
	for _, e := range []Entry{
		{ReasonRequestInvalid, InvalidArgument, 0, RetryNo, "The request is not valid"},
		{ReasonBodyTooLarge, InvalidArgument, 413, RetryNo, "The request body is too large"},
		{ReasonCursorInvalid, InvalidArgument, 0, RetryNo, "The cursor does not match the request"},
		{ReasonBatchTooLarge, InvalidArgument, 0, RetryNo, "Too many identities in one batch"},
		{ReasonTokenInvalid, Unauthenticated, 0, RetryNo, "The credentials are not valid"},
		{ReasonTokenStale, Unauthenticated, 0, RetryAfterRefresh, "The credentials are out of date"},
		{ReasonServiceCredentialInvalid, Unauthenticated, 0, RetryNo, "The calling service is not authenticated"},
		{ReasonPermissionDenied, PermissionDenied, 0, RetryNo, "Not allowed"},
		{ReasonContainmentNotProven, PermissionDenied, 0, RetryNo, "The change exceeds what can be proven"},
		{ReasonNotFound, NotFound, 0, RetryNo, "Not found"},
		{ReasonQueryNotCompletable, FailedPrecondition, 422, RetryNo, "The query cannot be completed"},
		{ReasonFieldNotQueryable, FailedPrecondition, 422, RetryNo, "A field cannot be used in this query"},
		{ReasonQueryPlanChanged, Aborted, 0, RetryNo, "The query changed; run it again"},
		{ReasonChangeKeyConflict, AlreadyExists, 0, RetryNo, "The change key was used for different content"},
		{ReasonChangeRevisionStale, Aborted, 0, RetryNo, "The target changed; preview again"},
		{ReasonLastAdminProtected, FailedPrecondition, 409, RetryNo, "The last administrator is protected"},
		{ReasonIdempotencyKeyReused, InvalidArgument, 422, RetryNo, "The idempotency key was used for another request"},
		{ReasonIdempotencyInProgress, Aborted, 0, RetryYes, "The first use of this idempotency key has not finished"},
		{ReasonLockTimeout, Aborted, 0, RetryYes, "Waiting for a lock took too long"},
		{ReasonTxConflict, Aborted, 0, RetryYes, "A concurrent change conflicted"},
		{ReasonStatementTimeout, DeadlineExceeded, 0, RetryYes, "The database took too long"},
		{ReasonServiceBusy, Unavailable, 0, RetryYes, "The service is busy"},
		{ReasonOutboundLimit, ResourceExhausted, 503, RetryYes, "Too many calls to a dependency are in progress"},
		{ReasonCircuitOpen, Unavailable, 0, RetryYes, "A dependency is failing; calls to it are paused"},
		{ReasonDependencyUnavailable, Unavailable, 0, RetryYes, "A dependency is unavailable"},
		{ReasonDeadlineBudgetExhausted, DeadlineExceeded, 0, RetryNo, "Not enough time was left to make a call"},
		{ReasonDeadlineExceeded, DeadlineExceeded, 0, RetryQueryFirst, "The request ran out of time"},
		{ReasonRequestCancelled, Cancelled, 0, RetryNone, "The request was cancelled"},
		{ReasonNotReady, Unavailable, 0, RetryYes, "The service is not ready"},
		{ReasonAuthorizationUnprovable, Unavailable, 0, RetryYes, "Authorization cannot be proven right now"},
		{ReasonAuthorizationViewExpired, Unavailable, 0, RetryYes, "The authorization view has expired"},
		{ReasonRecoveryInProgress, Unavailable, 0, RetryYes, "The deployment is being recovered"},
		{ReasonMemberSwitchInProgress, Unavailable, 0, RetryYes, "A provider switch is in progress"},
		{ReasonDefinitionPending, Unavailable, 0, RetryYes, "A definition the change needs has not been accepted yet"},
		{ReasonCapabilityUnsupported, Unimplemented, 0, RetryNo, "This capability is not supported"},
		{ReasonResultUnknown, DeadlineExceeded, 0, RetryQueryFirst, "The result is not known yet; query it"},
		{ReasonAuditCapacityExhausted, Unavailable, 0, RetryYes, "Audited operations are paused until the audit backlog drains"},
		{ReasonInternal, Internal, 0, RetryNo, "Internal error"},
	} {
		platform[e.Reason] = e
	}
}

// Platform returns the catalog entry of a platform reason.
func Platform(reason string) (Entry, bool) {
	e, ok := platform[reason]
	return e, ok
}

func (e Entry) status() int {
	if e.HTTP != 0 {
		return e.HTTP
	}
	return HTTPStatus(e.Code)
}

// New builds a platform error (domain "be"). An unknown reason is a programming error and yields INTERNAL
// keeping the bad reason in the cause, so it shows in the logs and tests rather than on the wire.
func New(reason, detail string) *Error {
	e, ok := platform[reason]
	if !ok {
		in := platform[ReasonInternal]
		return &Error{Reason: in.Reason, Domain: PlatformDomain, Code: in.Code, HTTP: in.status(), Title: in.Title,
			Detail: internalDetail, cause: errUnknownReason(reason)}
	}
	return &Error{Reason: e.Reason, Domain: PlatformDomain, Code: e.Code, HTTP: e.status(), Title: e.Title, Detail: detail}
}

type errUnknownReason string

func (r errUnknownReason) Error() string {
	return "errs.New called with a reason outside the catalog: " + string(r)
}

// Declared is a component's own reason, declared in its contract (P3.7).
type Declared struct {
	Domain string
	Entry
}

// Define declares a component reason; domain is the component (or family) ID.
func Define(domain, reason string, code Code, http int, retry, title string) Declared {
	return Declared{Domain: domain, Entry: Entry{Reason: reason, Code: code, HTTP: http, Retry: retry, Title: title}}
}

// New builds an error of a declared component reason.
func (d Declared) New(detail string) *Error {
	return &Error{Reason: d.Reason, Domain: d.Domain, Code: d.Code, HTTP: d.status(), Title: d.Title, Detail: detail}
}
