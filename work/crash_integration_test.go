//go:build integration

package work

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The crash tests run the first replica as a child process (this test binary again) and kill it with SIGKILL while it
// holds the item: no release, no last renewal, its connections simply drop.
func TestMain(m *testing.M) {
	if point := os.Getenv("BE_WORK_CRASH_AT"); point != "" {
		crashChild(point)
		return
	}
	os.Exit(m.Run())
}

const crashLease = 2 * time.Second

// crashJob is the job both replicas declare; the child's handler announces where it is and then hangs there until it
// is killed, the parent's runs to the end.
func crashJob(point string, child bool, calls *atomic.Int32) Job {
	effect := func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1)", ids.New())
		return err
	}
	hang := func(marker string, r *Run) error {
		fmt.Println(marker, r.ID)
		select {}
	}
	j := Job{Name: "q", Kind: Queue, Effect: Local}
	switch point {
	case "handler": // killed while the handler runs, before any effect
		j.Handle = func(ctx context.Context, r *Run) error {
			if child {
				return hang("RUNNING", r)
			}
			calls.Add(1)
			return r.Complete(ctx, effect)
		}
	case "effect": // killed inside the transaction that commits the effect and the completion (P7.10)
		j.Handle = func(ctx context.Context, r *Run) error {
			if !child {
				calls.Add(1)
			}
			return r.Complete(ctx, func(ctx context.Context, tx db.Tx) error {
				if err := effect(ctx, tx); err != nil || !child {
					return err
				}
				return hang("RUNNING", r)
			})
		}
	case "call": // killed after marking a non-idempotent call as possibly sent (P7.12)
		j.Name, j.Effect = "pay", NonIdempotentExternal
		j.Handle = func(ctx context.Context, r *Run) error {
			if !child {
				calls.Add(1)
			}
			if err := r.BeforeCall(ctx); err != nil || !child {
				return err
			}
			return hang("RUNNING", r)
		}
	}
	return j
}

func crashChild(point string) {
	var env map[string]string
	if err := json.Unmarshal([]byte(os.Getenv("BE_WORK_CRASH_ENV")), &env); err != nil {
		panic(err)
	}
	values, problems := config.Parse(db.MigrateKeys(), config.Map(env))
	if len(problems) > 0 {
		panic(fmt.Sprint(problems))
	}
	d, _, err := dbtest.Open(values, env["PG_SCHEMA"])
	if err != nil {
		panic(err)
	}
	r := replica(d, crashLease, crashJob(point, true, nil))
	r.Concurrency = 1
	fmt.Println("HOLDER", r.Holder)
	if err := r.Start(context.Background()); err != nil {
		panic(err)
	}
	select {}
}

// A replica killed while it runs an item: once its lease runs out another replica takes the item over under the same
// operation ID, and the effect happens once. The test first checks the kill hit what it means to: the child held the
// item at that point (and, inside the effect's transaction, had written the effect without committing it).
func TestACrashedReplicasItemIsTakenOverAfterItsLease(t *testing.T) { // CK-WK-02, CK-WK-06, CK-WK-07, P7.5, P7.10, P7.12
	for _, point := range []string{"handler", "effect", "call"} {
		t.Run(point, func(t *testing.T) {
			name := "work_crash_" + point
			d, admin := dbtest.Schema(t, name, nil)
			host, port := pgtest.EnsureDatabase(t, pgtest.Database)
			var calls atomic.Int32
			job := crashJob(point, false, &calls)
			b := replica(d, 30*time.Second, job)
			id := enqueue(t, d, b, job.Name)
			var operation string
			require.NoError(t, admin.QueryRow(context.Background(), "SELECT operation_id FROM be_work_item WHERE id = $1", id).Scan(&operation))

			env, err := json.Marshal(pgtest.MemberEnv(host, port, name))
			require.NoError(t, err)
			child := exec.Command(os.Args[0], "-test.run=^$")
			child.Env = append(os.Environ(), "BE_WORK_CRASH_AT="+point, "BE_WORK_CRASH_ENV="+string(env))
			child.Stderr = os.Stderr
			out, err := child.StdoutPipe()
			require.NoError(t, err)
			require.NoError(t, child.Start())
			t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
			lines := make(chan string)
			go func() {
				s := bufio.NewScanner(out)
				for s.Scan() {
					lines <- s.Text()
				}
				close(lines)
			}()
			var holder string
			for running := false; !running; {
				select {
				case l, ok := <-lines:
					require.True(t, ok, "the child exited before it ran the item")
					if h, found := strings.CutPrefix(l, "HOLDER "); found {
						holder = h
					}
					running = l == "RUNNING "+id
				case <-time.After(10 * time.Second):
					t.Fatal("the child never ran the item")
				}
			}

			// the precondition: the child holds the item, its lease live, its attempt counted
			var st, held string
			var epoch int64
			var attempts int
			var live bool
			var leaseUntil time.Time
			require.NoError(t, admin.QueryRow(context.Background(), `SELECT state, holder, epoch, attempts,
				lease_until > now(), lease_until FROM be_work_item WHERE id = $1`, id).Scan(&st, &held, &epoch, &attempts, &live, &leaseUntil))
			require.Equal(t, "running", st)
			require.Equal(t, holder, held, "the child is the holder")
			require.True(t, live, "the child's lease has not run out yet")
			require.Equal(t, 1, attempts)
			if point == "effect" {
				var inTx int
				require.NoError(t, admin.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
					WHERE usename = $1 AND state = 'idle in transaction'`, "sdk_"+name+"_rw").Scan(&inTx))
				require.Equal(t, 1, inTx, "the child's effect transaction is open")
			}
			if point == "call" {
				var unknown bool
				require.NoError(t, admin.QueryRow(context.Background(), "SELECT outcome_unknown FROM be_work_item WHERE id = $1", id).Scan(&unknown))
				require.True(t, unknown, "the child marked the call as possibly sent")
			}

			require.NoError(t, child.Process.Signal(syscall.SIGKILL))
			_ = child.Wait()
			ws := child.ProcessState.Sys().(syscall.WaitStatus)
			require.True(t, ws.Signaled() && ws.Signal() == syscall.SIGKILL, "the child was killed: %v", child.ProcessState)

			ctx, cancel := context.WithCancel(context.Background())
			require.NoError(t, b.Start(ctx))
			want := "done"
			if point == "call" {
				want = "paused"
			}
			var seenAt time.Time // the database's clock when the taker's result was first seen
			require.Eventually(t, func() bool {
				var s string
				require.NoError(t, admin.QueryRow(context.Background(), "SELECT state, now() FROM be_work_item WHERE id = $1", id).
					Scan(&s, &seenAt))
				return s == want
			}, crashLease+10*time.Second, 50*time.Millisecond)
			cancel()
			b.Stop()

			var after struct {
				epoch     int64
				attempts  int
				operation string
				reason    *string
			}
			require.NoError(t, admin.QueryRow(context.Background(), `SELECT epoch, attempts, operation_id, pause_reason
				FROM be_work_item WHERE id = $1`, id).Scan(&after.epoch, &after.attempts, &after.operation, &after.reason))
			assert.Equal(t, epoch+1, after.epoch, "taken over: the epoch went up by one")
			assert.Equal(t, 2, after.attempts, "the crashed attempt stays counted")
			assert.Equal(t, operation, after.operation, "the operation ID is kept")
			assert.False(t, seenAt.Before(leaseUntil), "not taken over before the child's lease ran out")
			var effects int
			require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM items").Scan(&effects))
			if point == "call" {
				require.NotNil(t, after.reason)
				assert.Equal(t, "outcome_unknown", *after.reason)
				assert.Zero(t, calls.Load(), "the taker never calls again")
				assert.Zero(t, effects)
				return
			}
			assert.Equal(t, int32(1), calls.Load())
			assert.Equal(t, 1, effects, "the effect happened once")
		})
	}
}
