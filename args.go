package besdk

import (
	"fmt"
	"strconv"
	"strings"
)

// Exit codes of the entry point (be-protocol P1 "Exit codes").
const (
	exitOK       = 0
	exitFailure  = 1
	exitShell    = 2
	exitUsage    = 64
	exitConfig   = 78
	maxDownSteps = 1000
)

type cmdKind int

const (
	cmdServe cmdKind = iota
	cmdMigrateUp
	cmdMigrateDown
	cmdMigrateStatus
	cmdJobRun
)

// command is the entry point's sub-command (P1.1): none = serve; migrate up|down N|status;
// job run <name> (P14.8).
type command struct {
	kind cmdKind
	n    int    // migrate down N
	job  string // job run <name>
}

// parseArgs parses os.Args[1:] before anything else is read; an unrecognised argument is a usage
// error (exit 64, P1.1), so a misspelt migration command never becomes a second server.
func parseArgs(args []string) (command, error) {
	usage := func() (command, error) {
		return command{}, fmt.Errorf("usage: <binary> | migrate up | migrate down <n> | migrate status | job run <name>; got %q",
			strings.Join(args, " "))
	}
	switch {
	case len(args) == 0:
		return command{kind: cmdServe}, nil
	case args[0] == "migrate" && len(args) == 2 && args[1] == "up":
		return command{kind: cmdMigrateUp}, nil
	case args[0] == "migrate" && len(args) == 2 && args[1] == "status":
		return command{kind: cmdMigrateStatus}, nil
	case args[0] == "migrate" && len(args) == 3 && args[1] == "down":
		n, err := strconv.Atoi(args[2])
		if err != nil || n < 1 || n > maxDownSteps || strconv.Itoa(n) != args[2] {
			return usage()
		}
		return command{kind: cmdMigrateDown, n: n}, nil
	case args[0] == "job" && len(args) == 3 && args[1] == "run" && args[2] != "":
		return command{kind: cmdJobRun, job: args[2]}, nil
	}
	return usage()
}
