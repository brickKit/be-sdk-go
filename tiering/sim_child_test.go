//go:build simulation

package tiering_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
)

// A replica of the long simulation is this test binary again (TestMain): it hosts one member, or two as a shell does,
// each with the catalog the host builds from the deployment's configuration and every tiering job on a runner of its
// own, their intervals in seconds. With a crash point it ends itself (exit 3) the After-th time it gets there.

type simChildSpec struct {
	ID      string           `json:"id"`
	Members []simChildMember `json:"members"`
	Crash   string           `json:"crash"`
	After   int              `json:"after"`
}

type simChildMember struct {
	Name   string            `json:"name"`
	Prefix string            `json:"prefix"`
	Env    map[string]string `json:"env"`
}

// simKeeper is the evidence store of a replica: it takes every record (the simulation checks the data, not the store).
type simKeeper struct{ mu sync.Mutex }

func (k *simKeeper) Append(_ context.Context, recs []evidence.Sealed) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]string, len(recs))
	for i := range recs {
		out[i] = "accepted"
	}
	return out, nil
}

// simMemberCatalog is a member's catalog as the host builds it, with the simulation's backups and evidence store.
func simMemberCatalog(ctx context.Context, m simChildMember, instance string, log *slog.Logger) (*db.DB, *tiering.Catalog, error) {
	classes := []tiering.Class{simClassDef()}
	values, problems := config.Parse(append(db.MigrateKeys(), tiering.Keys(classes)...), config.Map(m.Env))
	if len(problems) > 0 {
		return nil, nil, fmt.Errorf("%s: %v", m.Name, problems)
	}
	d, _, err := dbtest.Open(values, m.Name)
	if err != nil {
		return nil, nil, err
	}
	cat, err := tiering.NewCatalog(ctx, d, classes, values, m.Prefix)
	if err != nil {
		return nil, nil, err
	}
	cat.Backups = simBackups(d)
	cat.Evidence = &evidence.Writer{DB: d, Store: &simKeeper{}, Component: "test/" + m.Name, Instance: instance, Log: log}
	cat.Log, cat.Batch, cat.OrphanAge, cat.Parts = log, 7, simOrphanAge, simParts
	return d, cat, nil
}

// fast is a job of the SDK with the simulation's interval: a cron job becomes a singleton.
func fast(j work.Job, every time.Duration) work.Job {
	j.Kind, j.Cron, j.TimeZone, j.Interval = work.Singleton, "", "", every
	return j
}

func simChild(raw string) {
	var s simChildSpec
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		panic(err)
	}
	var hits atomic.Int64
	if s.Crash != "" {
		tiering.SetCrash(func(p string) {
			if p == s.Crash && hits.Add(1) >= int64(s.After) {
				fmt.Printf("CRASH %s\n", p)
				os.Exit(3)
			}
		})
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx := context.Background()
	for _, m := range s.Members {
		d, cat, err := simMemberCatalog(ctx, m, s.ID, log)
		if err != nil {
			panic(err)
		}
		r := work.NewRunner(d, "test/"+m.Name, log)
		r.Lease, r.Poll, r.Concurrency = 3*time.Second, 100*time.Millisecond, 4
		cat.Enqueue = r.Enqueue
		if err := r.Add(fast(cat.CandidatesJob("* * * * *", log), time.Second), fast(cat.TransferJob(log), time.Second),
			fast(cat.ReleaseJob(log), 2*time.Second), fast(cat.DestroyJob(log), 3*time.Second), cat.EraseJobOf(log),
			fast(cat.CleanupJob(log), simCleanupEvery)); err != nil {
			panic(err)
		}
		if err := r.Start(ctx); err != nil {
			panic(err)
		}
	}
	fmt.Println("READY")
	select {}
}
