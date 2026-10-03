package envelope

import "regexp"

// Header patterns of envelope.schema.json and events-contract.schema.json (P12 envelope). Compiled
// regexps are immutable and safe to share between members of one shell.
var (
	aggregateTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
	traceParentPattern   = regexp.MustCompile(`^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)
	versionPattern       = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+[^/]*$`)
	eventsFilePattern    = regexp.MustCompile(`^[^#/]+$`)
	dataSchemaPattern    = regexp.MustCompile(`^[a-z][a-z0-9-]*/[a-z][a-z0-9-]*@[0-9]+\.[0-9]+\.[0-9]+[^/]*/contracts/events/[^#]+#[a-z0-9_.]+$`)
	decimalPattern       = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
	idPattern            = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)
