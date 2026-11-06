package host

import (
	"fmt"

	"github.com/brickKit/be-sdk-go/v3/work"
)

// recoveryRule is what one of the SDK's own jobs does in the recovery phases (P1.23): in none of them a job runs as
// a job; a few run as the host's recovery-phase loops instead, because what they write to never goes back.
type recoveryRule struct {
	loop bool   // runs as a recovery-phase loop (registered next to the job)
	why  string // why it runs there, or why not
}

// inRecovery names every job the SDK registers, and how it behaves in a full recovery. A job added to the SDK must
// be added here (checkRecoveryRule refuses it otherwise): the audit and the evidence transfers were each found not
// running in the recovery phases only by a drill (foundations item 4, T10e and T11).
var inRecovery = map[string]recoveryRule{
	"be.audit.transfer": {loop: true, why: "the audit store never goes back: the records written during the recovery " +
		"reach it before the opening (the check's audit backlog, the loss range)"},
	"be.evidence.transfer": {loop: true, why: "the evidence store never goes back: a conclusion written during the " +
		"recovery reaches it before the replay"},
	"be.extcmd.process": {loop: true, why: "query only, in verifying and read_only (P13.10): nothing sent while the " +
		"external reconciliation decides"},
	"be.outbox.purge": {why: "not run: a purge during the recovery could drop published rows a redelivery still " +
		"needs; it runs once open"},
	"be.idempotency.cleanup": {why: "not run: housekeeping of the member's own database, which goes back; once open"},
	"be.work.cleanup":        {why: "not run: housekeeping of the member's own database, which goes back; once open"},
	"be.partitions.maintain": {why: "not run: the partitions ahead were made before T; hourly once open"},
}

// checkRecoveryRule refuses one of the SDK's jobs that does not say how it behaves in a recovery.
func checkRecoveryRule(j work.Job) error {
	if _, ok := inRecovery[j.Name]; !ok {
		return fmt.Errorf("the SDK's job %s does not say how it behaves in the recovery phases (internal/host/"+
			"recoveryjobs.go, P1.23)", j.Name)
	}
	return nil
}

// addSDKJob registers one of the SDK's own jobs, with its recovery rule.
func (m *Member) addSDKJob(j work.Job) error {
	if err := checkRecoveryRule(j); err != nil {
		return err
	}
	return m.runner.Add(j)
}
