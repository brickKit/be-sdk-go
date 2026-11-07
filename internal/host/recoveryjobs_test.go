package host

import (
	"slices"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/audit"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/extcmd"
	"github.com/brickKit/be-sdk-go/v3/idem"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every job the SDK registers says how it behaves in the recovery phases (P1.23; foundations item 4's retro R1: the
// audit and the evidence transfers were found not running in them one drill at a time). The table names exactly the
// SDK's jobs, and a job outside it is refused when it is registered.
func TestEverySDKJobSaysWhatItDoesInARecovery(t *testing.T) {
	jobs := []work.Job{partitionJob(nil), idem.Job(nil), events.PurgeJob(nil), work.PurgeJob(nil),
		(&audit.Auditor{}).Job(nil, nil), evidence.Job(nil, nil, nil), (&extcmd.Commands{}).Job(),
		(&tiering.Catalog{}).CandidatesJob("0 2 * * *", nil), (&tiering.Catalog{}).TransferJob(nil),
		(&tiering.Catalog{}).ReleaseJob(nil), (&tiering.Catalog{}).ExportJobOf(nil), (&tiering.Catalog{}).ExportCleanupJob(nil),
		(&tiering.Catalog{}).DestroyJob(nil), (&tiering.Catalog{}).EraseJobOf(nil), (&tiering.Catalog{}).CleanupJob(nil)}
	var names []string
	for _, j := range jobs {
		names = append(names, j.Name)
	}
	var keys []string
	for k := range inRecovery {
		keys = append(keys, k)
	}
	slices.Sort(names)
	slices.Sort(keys)
	assert.Equal(t, names, keys, "the table names exactly the SDK's jobs")
	for k, rule := range inRecovery {
		assert.NotEmpty(t, rule.why, k)
	}
	err := checkRecoveryRule(work.Job{Name: "be.something.new"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "be.something.new")
	assert.NoError(t, checkRecoveryRule(evidence.Job(nil, nil, nil)))
}
