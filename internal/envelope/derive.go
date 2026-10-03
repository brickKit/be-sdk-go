package envelope

// OriginKind is the kind of work an event is published (or a job enqueued) from (P12.8, vectors
// envelope/derive). The zero value is OriginRequest.
type OriginKind int

// Origin kinds (P12.8). Every kind except OriginEvent and OriginQueuedJob starts a new chain.
const (
	OriginRequest    OriginKind = iota // a user request or a system rpc
	OriginEvent                        // an event handler
	OriginQueuedJob                    // a queued job, carrying what it captured when enqueued
	OriginCron                         // a cron job run
	OriginSingleton                    // a singleton job run
	OriginEvery                        // an every-interval job run
	OriginReconciler                   // a reconciler step
)

// Origin is the context an event is published in (P12.8).
type Origin struct {
	Kind OriginKind
	// OriginEvent: the handled event's ce-id and ce-hopcount.
	// OriginQueuedJob: the job row's stored causation id and hop count (see EnqueueContext).
	// Ignored for the other kinds.
	CausationID string
	HopCount    int
}

// Derive returns the ce-causationid ("" = header absent) and ce-hopcount of an event published in
// o (P12.8): in an event handler the handled ce-id and its hop count + 1; in a queued job exactly the
// values stored when it was enqueued; from a request or any scheduled job "" and 0. Publishing is
// never refused for its hop count: the consumer dead-letters above HopLimit.
func Derive(o Origin) (causationID string, hopCount int) {
	switch o.Kind {
	case OriginEvent:
		return o.CausationID, o.HopCount + 1
	case OriginQueuedJob:
		return o.CausationID, o.HopCount
	default:
		return "", 0
	}
}

// EnqueueContext returns the causation id and hop count a queue row stores when it is enqueued in
// o: what an event published at that moment would carry (P12.8, vectors envelope/derive
// enqueue_context). The job later publishes with exactly these values (Origin{Kind: OriginQueuedJob}).
func EnqueueContext(o Origin) (causationID string, hopCount int) {
	return Derive(o)
}
