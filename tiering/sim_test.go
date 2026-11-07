//go:build simulation

package tiering_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/internal/s3test"
	"github.com/brickKit/be-sdk-go/v3/recovery"
	"github.com/brickKit/be-sdk-go/v3/recoverypb"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/jackc/pgx/v5"
)

// The long simulation (assembly protocol 11's replacement checks; foundations item 5, T11a), run with
// `go test -tags simulation -run TestTheLongSimulation -timeout 0 -v ./tiering/` and BE_SDK_TEST_PG_ADMIN and
// BE_SDK_TEST_S3_* set; SIM_MINUTES (default 30) is how long the crashes go on.
//
// Two members on a real PostgreSQL and object store, member a on the object route and b on the warm one, their routes
// swapped halfway (the route change and the old versions' release). Four replicas: two host both members (a shell),
// one each alone. Each replica runs with a crash point taken in turn from every state transition of the SDK and ends
// itself there; the parent starts it again, and kills one at random now and then. Meanwhile the parent writes new
// documents, changes open ones, closes some into the past, writes to closed ones (refused once sealed), erases (as a
// request, and as the queued redo a recovery's replay leaves), holds old ones, records backups and reads documents by
// their ID, each against what it last wrote. Then the crashes stop, the jobs run until nothing is left to do, and the
// final state is checked: every document reads as written (or is destroyed, past its retention and not held); every
// published copy verifies; the recovery check passes (no copy missing, no orphan, no released unit with online rows);
// no work item is stuck; every crash point was reached.

func TestMain(m *testing.M) {
	if spec := os.Getenv("SIM_CHILD"); spec != "" {
		simChild(spec)
		return
	}
	os.Exit(m.Run())
}

// simPoints are the crash points (crash.go) the simulation must reach: every background transaction that changes
// state, before and after its commit, and every object written or deleted.
var simPoints = []string{
	"seal/commit", "seal/done", "claim/commit", "claim/done", "copy.rows/commit", "copy.rows/done", "copy.object/done",
	"copy.manifest/done", "publish/commit", "publish/done", "release.decide/commit", "release.decide/done",
	"release.delete/commit", "release.delete/done", "destroy.decide/commit", "destroy.decide/done",
	"destroy.purge/commit", "destroy.purge/done", "destroy.object/done", "erase.find/done", "erase.sealed/commit",
	"erase.sealed/done", "rewrite.route/done", "releaseold.decide/done", "releaseold.delete/commit",
	"releaseold.delete/done", "releaseold.object/done", "orphans.object/done", "orphans.delete/commit",
	"orphans.delete/done",
}

const simBucket = "be-tiering-sim"

type simDoc struct {
	mu        sync.Mutex
	agg       string // as last written (simAggregate)
	open      bool
	old       bool // closed past the retention: may be destroyed
	held      bool
	busy      bool // a queued erasure not done yet: not compared
	destroyed bool
}

type simMember struct {
	name, prefix string
	env          map[string]string // the replicas' configuration (the route changes halfway)
	d            *db.DB
	cat          atomic.Pointer[tiering.Catalog] // the parent's own: reads and requested erasures
	queue        *work.Runner                    // never started: it only queues the erasures' redo
	mu           sync.Mutex
	docs         map[string]*simDoc
	ids          []string
}

type simRun struct {
	t        *testing.T
	ctx      context.Context
	members  []*simMember
	mu       sync.Mutex
	failures []string
	reached  map[string]int
	stats    struct{ writes, refused, reads, erasures, queued, holds, crashes, kills, transient atomic.Int64 }
}

func (r *simRun) fail(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
	if len(r.failures) <= 20 {
		fmt.Printf("SIM FAILURE %s\n", r.failures[len(r.failures)-1])
	}
}

func (r *simRun) member() *simMember { return r.members[rand.IntN(len(r.members))] }

func (m *simMember) pick(want func(*simDoc) bool) (string, *simDoc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < 20 && len(m.ids) > 0; i++ {
		id := m.ids[rand.IntN(len(m.ids))]
		if doc := m.docs[id]; want == nil || want(doc) {
			return id, doc
		}
	}
	return "", nil
}

// read is a document as a component reads it: from its archive when it has left the online tables, else online. The
// two reads are not one snapshot, so a document published and released in between is read again.
func (r *simRun) read(m *simMember, id string) (agg, where string, err error) {
	for attempt := 0; attempt < 5; attempt++ {
		a, found, err := m.cat.Load().Get(r.ctx, simClass, id)
		if errs.Is(err, "be", errs.ReasonNotFound) { // destroyed: gone from the decision on (P15.22, P15.29)
			return "", "", nil
		}
		if errs.Is(err, "be", errs.ReasonDependencyUnavailable) {
			r.stats.transient.Add(1)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != nil {
			return "", "", err
		}
		if found {
			return simAggregate(a.Root, a.Children), a.Route, nil
		}
		online, found, err := readOnline(r.ctx, m.d, id)
		if err != nil || found {
			return online, "online", err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", "", nil
}

// check reads a document and compares it with what was last written; doc is locked by the caller.
func (r *simRun) check(m *simMember, id string, doc *simDoc) {
	if doc.busy || doc.destroyed {
		return
	}
	r.stats.reads.Add(1)
	got, where, err := r.read(m, id)
	switch {
	case err != nil:
		r.fail("%s %s: read: %v", m.name, id, err)
	case where == "":
		r.gone(m, id, doc)
	case got != doc.agg:
		r.fail("%s %s: read from %s differs:\n got  %s\n want %s", m.name, id, where, got, doc.agg)
	}
}

// gone accounts for a document read nowhere: destroyed, which only one past its retention and not held may be.
func (r *simRun) gone(m *simMember, id string, doc *simDoc) {
	var state string
	_ = m.d.BackgroundTx(r.ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT u.state FROM be_tier_member b JOIN be_tier_unit u ON u.id = b.unit "+
			"WHERE b.class = $1 AND b.root_id = $2", simClass, id).Scan(&state)
	})
	switch {
	case state != tiering.StateDestroyed:
		r.fail("%s %s: not found (unit %q)", m.name, id, state)
	case !doc.old || doc.held:
		r.fail("%s %s: destroyed though old=%v held=%v", m.name, id, doc.old, doc.held)
	default:
		doc.destroyed = true
	}
}

func (r *simRun) tx(m *simMember, fn func(ctx context.Context, tx db.Tx) error) error {
	return m.d.BackgroundTx(r.ctx, fn)
}

// write makes a document: open, closed into the past (due for the archive), or closed past the retention.
func (r *simRun) write(m *simMember) {
	id := ids.New().String()
	kind := rand.IntN(10)
	closed := "NULL"
	switch {
	case kind < 5:
		closed = fmt.Sprintf("now() - interval '%d days'", 400+rand.IntN(240))
	case kind < 6:
		closed = fmt.Sprintf("now() - interval '%d days'", 3700+rand.IntN(40))
	}
	blob := make([]byte, rand.IntN(2048))
	for i := range blob {
		blob[i] = byte(rand.IntN(256))
	}
	err := r.tx(m, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO docs (id, grp, rev, code, note, amount, qty, small, flag, day, attrs, blob, "+
			"tags, closed_at, created_at) VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, "+closed+
			", now() - interval '4000 days')", id, fmt.Sprintf("g%d", rand.IntN(4)), "D-"+id[:8],
			"call +34 600 "+strconv.Itoa(rand.IntN(1e6)), fmt.Sprintf("%d.%04d", rand.IntN(1e6), rand.IntN(1e4)),
			rand.Int64N(1<<53), int16(rand.IntN(65536)-32768), rand.IntN(2) == 0, nullDay(), `{"n": [1, "é"]}`, blob,
			[]string{"t" + strconv.Itoa(rand.IntN(9)), "Ä"}); err != nil {
			return err
		}
		for l := 0; l < 1+rand.IntN(5); l++ {
			line := ids.New().String()
			if _, err := tx.Exec(ctx, "INSERT INTO doc_lines VALUES ($1, $2, $3, $4, $5)", line, id, l, "line "+strconv.Itoa(l),
				"person "+strconv.Itoa(rand.IntN(1000))); err != nil {
				return err
			}
			for s := 0; s < rand.IntN(4); s++ {
				if _, err := tx.Exec(ctx, "INSERT INTO line_marks VALUES ($1, $2, $3)", line, s, "mark"); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		r.fail("%s: write: %v", m.name, err)
		return
	}
	r.stats.writes.Add(1)
	doc := &simDoc{open: kind >= 6, old: kind == 5}
	doc.mu.Lock()
	defer doc.mu.Unlock()
	m.mu.Lock()
	m.docs[id] = doc
	m.ids = append(m.ids, id)
	m.mu.Unlock()
	r.reread(m, id, doc)
}

func nullDay() any {
	if rand.IntN(3) == 0 {
		return nil
	}
	return time.Date(2020+rand.IntN(6), time.Month(1+rand.IntN(12)), 1+rand.IntN(28), 0, 0, 0, 0, time.UTC)
}

// reread takes a document's form after a committed write; doc is locked by the caller.
func (r *simRun) reread(m *simMember, id string, doc *simDoc) {
	got, where, err := r.read(m, id)
	switch {
	case err != nil:
		r.fail("%s %s: reading back a write: %v", m.name, id, err)
	case where == "": // destroyed meanwhile (an old document)
		r.gone(m, id, doc)
	default:
		doc.agg = got
	}
}

// change edits an open document, closes one into the past, or writes to a closed one (refused once it is sealed).
func (r *simRun) change(m *simMember) {
	id, doc := m.pick(func(d *simDoc) bool { return !d.destroyed && !d.busy })
	if doc == nil {
		return
	}
	doc.mu.Lock()
	defer doc.mu.Unlock()
	var stmt string
	var args []any
	closing := false
	switch {
	case doc.open && rand.IntN(3) == 0:
		closing = true
		days := 400 + rand.IntN(240)
		if rand.IntN(6) == 0 {
			days = 3700 + rand.IntN(40)
		}
		stmt, args = "UPDATE docs SET rev = rev + 1, closed_at = now() - make_interval(days => $2) WHERE id = $1", []any{id, days}
	default:
		stmt, args = "UPDATE docs SET rev = rev + 1, amount = amount + 1, note = $2 WHERE id = $1", []any{id, "changed " + ids.New().String()[:8]}
	}
	err := r.tx(m, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, stmt, args...); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "UPDATE doc_lines SET label = label || '+' WHERE doc_id = $1 AND line = 0", id)
		return err
	})
	switch {
	case errs.Is(err, "be", errs.ReasonRecordSealed):
		r.stats.refused.Add(1)
		if doc.open {
			r.fail("%s %s: an open document refused a write", m.name, id)
		}
		return
	case err != nil:
		r.fail("%s %s: change: %v", m.name, id, err)
		return
	}
	r.stats.writes.Add(1)
	if closing {
		doc.open = false
		doc.old = args[1].(int) >= 3650
	}
	r.reread(m, id, doc)
}

// erase erases a closed document: as a request (the parent's catalog), or queued as a recovery's redo leaves it (a
// replica runs it, with the crash points). Half of the queued ones take a document of a unit sealed now: a unit is
// sealed for about a second, and the replicas would hardly ever erase one otherwise.
func (r *simRun) erase(m *simMember) {
	id, doc := m.pick(func(d *simDoc) bool { return !d.open && !d.destroyed && !d.busy })
	fromSealed := false
	if rand.IntN(4) == 0 {
		var sealed string
		_ = r.tx(m, func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT b.root_id FROM be_tier_member b JOIN be_tier_unit u ON u.id = b.unit "+
				"WHERE u.state = 'sealed' ORDER BY random() LIMIT 1").Scan(&sealed)
		})
		m.mu.Lock()
		if d := m.docs[sealed]; d != nil && !d.busy && !d.destroyed {
			id, doc, fromSealed = sealed, d, true
		}
		m.mu.Unlock()
	}
	if doc == nil {
		return
	}
	doc.mu.Lock()
	defer doc.mu.Unlock()
	if (fromSealed || rand.IntN(2) == 0) && !doc.held {
		// a recovery's replay queues the redo for a unit published or released (an online or sealed one it erases in the
		// replay's own transaction); queued for a sealed one too here, so that the replicas reach the sealed erasure's
		// crash points (the job erases whatever state the unit is in when it runs)
		var job string
		err := r.tx(m, func(ctx context.Context, tx db.Tx) (err error) {
			var state string
			err = tx.QueryRow(ctx, "SELECT coalesce((SELECT u.state FROM be_tier_member b JOIN be_tier_unit u ON u.id = b.unit "+
				"WHERE b.class = $1 AND b.root_id = $2 FOR SHARE OF u), 'none')", simClass, id).Scan(&state)
			if err != nil || (state != tiering.StateSealed && state != tiering.StatePublished && state != tiering.StateReleased) {
				return err
			}
			job, err = m.queue.Enqueue(ctx, tx, tiering.EraseJob, map[string]string{"class": simClass, "root": id, "basis": "sim"})
			return err
		})
		if err != nil {
			r.fail("%s %s: queueing an erasure: %v", m.name, id, err)
			return
		}
		if job == "" {
			return
		}
		r.stats.queued.Add(1)
		doc.busy = true
		want := erased(doc.agg)
		go r.awaitErasure(m, id, doc, job, want)
		return
	}
	for attempt := 0; attempt < 5; attempt++ {
		out, err := m.cat.Load().Erase(r.ctx, simClass, id, "sim")
		switch {
		case err != nil:
			r.fail("%s %s: erase: %v", m.name, id, err)
			return
		case out == tiering.ErasureDeferred:
			return
		case out == tiering.ErasedOnline: // not in a unit past open: the component erases its online rows
			err := r.tx(m, func(ctx context.Context, tx db.Tx) error {
				if _, err := tx.Exec(ctx, "UPDATE docs SET note = NULL WHERE id = $1", id); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, "UPDATE doc_lines SET person = NULL WHERE doc_id = $1", id)
				return err
			})
			if errs.Is(err, "be", errs.ReasonRecordSealed) {
				continue // sealed meanwhile: the catalog erases it now
			}
			if err != nil {
				r.fail("%s %s: erasing online: %v", m.name, id, err)
				return
			}
		}
		r.stats.erasures.Add(1)
		want := erased(doc.agg)
		r.reread(m, id, doc)
		if doc.agg != want && !doc.destroyed {
			r.fail("%s %s: after the erasure (%s):\n got  %s\n want %s", m.name, id, out, doc.agg, want)
		}
		return
	}
}

// awaitErasure waits for a queued erasure and checks its outcome.
func (r *simRun) awaitErasure(m *simMember, id string, doc *simDoc, job, want string) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) && r.ctx.Err() == nil {
		var state string
		_ = r.tx(m, func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT state FROM be_work_item WHERE id = $1", job).Scan(&state)
		})
		if state == "done" {
			doc.mu.Lock()
			defer doc.mu.Unlock()
			doc.busy = false
			r.stats.erasures.Add(1)
			r.reread(m, id, doc)
			if doc.agg != want && !doc.destroyed {
				r.fail("%s %s: after the queued erasure:\n got  %s\n want %s", m.name, id, doc.agg, want)
				doc.agg = want
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	if r.ctx.Err() == nil {
		r.fail("%s %s: the queued erasure %s did not finish in 5 minutes", m.name, id, job)
	}
}

// hold puts a legal hold on an old document (never lifted: it must outlive its retention).
func (r *simRun) hold(m *simMember) {
	id, doc := m.pick(func(d *simDoc) bool { return d.old && !d.held && !d.destroyed && !d.busy })
	if doc == nil {
		return
	}
	doc.mu.Lock()
	defer doc.mu.Unlock()
	var state string
	err := r.tx(m, func(ctx context.Context, tx db.Tx) error {
		// a hold on a document already destroyed protects nothing: the test holds only what is still there
		err := tx.QueryRow(ctx, "SELECT coalesce((SELECT u.state FROM be_tier_member b JOIN be_tier_unit u ON u.id = b.unit "+
			"WHERE b.class = $1 AND b.root_id = $2 FOR SHARE OF u), 'none')", simClass, id).Scan(&state)
		if err != nil || state == tiering.StateDestroyed {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO be_tier_hold (id, scope_kind, scope, source, authorized_by, reason) VALUES "+
			"($1, 'record', $2, 'legal', 'sim', 'the simulation')", ids.New().String(), simClass+"/"+id)
		return err
	})
	if err != nil && strings.Contains(err.Error(), "40P01") { // a destruction deciding at the same moment: try another
		return
	}
	if err != nil {
		r.fail("%s %s: hold: %v", m.name, id, err)
		return
	}
	if state != tiering.StateDestroyed {
		doc.held = true
		r.stats.holds.Add(1)
	}
}

func (r *simRun) every(wg *sync.WaitGroup, stop <-chan struct{}, period time.Duration, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(period):
				fn()
			}
		}
	}()
}

// replica is one slot of the four: its members, and the child process running now.
type replica struct {
	members []*simMember
	mu      sync.Mutex
	cmd     *exec.Cmd
	logs    io.Writer
}

func (r *simRun) spawn(rp *replica, crash string, after int) (*exec.Cmd, <-chan string, error) {
	spec := simChildSpec{ID: ids.New().String(), Crash: crash, After: after}
	for _, m := range rp.members {
		m.mu.Lock()
		env := map[string]string{}
		for k, v := range m.env {
			env[k] = v
		}
		m.mu.Unlock()
		spec.Members = append(spec.Members, simChildMember{Name: m.name, Prefix: m.prefix, Env: env})
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "SIM_CHILD="+string(raw))
	cmd.Stderr = rp.logs
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	lines := make(chan string, 4)
	go func() {
		s := bufio.NewScanner(out)
		for s.Scan() {
			lines <- s.Text()
		}
		close(lines)
	}()
	return cmd, lines, nil
}

// supervise runs a replica with crash points in turn until stop, then without until done.
func (r *simRun) supervise(wg *sync.WaitGroup, rp *replica, stop, done <-chan struct{}, next *atomic.Int64) {
	defer wg.Done()
	for {
		crash, after := "", 0
		select {
		case <-done:
			return
		case <-stop:
		default:
			crash, after = simPoints[int(next.Add(1))%len(simPoints)], 1+rand.IntN(3)
		}
		cmd, lines, err := r.spawn(rp, crash, after)
		if err != nil {
			r.fail("starting a replica: %v", err)
			return
		}
		rp.mu.Lock()
		rp.cmd = cmd
		rp.mu.Unlock()
		limit := time.After(90 * time.Second) // a point this replica does not reach soon: another one
		if crash == "" {
			limit = nil
		}
		ended := false
		for !ended {
			select {
			case l, ok := <-lines:
				if !ok {
					ended = true
					break
				}
				if p, found := strings.CutPrefix(l, "CRASH "); found {
					r.mu.Lock()
					r.reached[p]++
					r.mu.Unlock()
					r.stats.crashes.Add(1)
				}
			case <-limit:
				_ = cmd.Process.Kill()
				limit = nil
			case <-stop:
				if crash != "" {
					_ = cmd.Process.Kill()
				}
				stop = nil
			case <-done:
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				return
			}
		}
		_ = cmd.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}

func (r *simRun) progress(start time.Time) {
	var parts []string
	for _, m := range r.members {
		var states []string
		_ = r.tx(m, func(ctx context.Context, tx db.Tx) error {
			rows, err := tx.Query(ctx, "SELECT state || ':' || count(*) FROM be_tier_unit GROUP BY state ORDER BY state")
			if err != nil {
				return err
			}
			states, err = pgx.CollectRows(rows, pgx.RowTo[string])
			return err
		})
		m.mu.Lock()
		parts = append(parts, fmt.Sprintf("%s docs=%d units=%s", m.name, len(m.ids), strings.Join(states, ",")))
		m.mu.Unlock()
	}
	r.mu.Lock()
	failures, reached := len(r.failures), len(r.reached)
	r.mu.Unlock()
	fmt.Printf("SIM %dm %s writes=%d refused=%d reads=%d erasures=%d queued=%d holds=%d crashes=%d points=%d/%d kills=%d "+
		"transient=%d failures=%d\n", int(time.Since(start).Minutes()), strings.Join(parts, " "), r.stats.writes.Load(),
		r.stats.refused.Load(), r.stats.reads.Load(), r.stats.erasures.Load(), r.stats.queued.Load(), r.stats.holds.Load(),
		r.stats.crashes.Load(), reached, len(simPoints), r.stats.kills.Load(), r.stats.transient.Load(), failures)
}

func TestTheLongSimulation(t *testing.T) {
	minutes := 30
	if v, err := strconv.Atoi(os.Getenv("SIM_MINUTES")); err == nil {
		minutes = v
	}
	ctx := context.Background()
	url, access, secret := os.Getenv("BE_SDK_TEST_S3_URL"), os.Getenv("BE_SDK_TEST_S3_ACCESS_KEY"), os.Getenv("BE_SDK_TEST_S3_SECRET_KEY")
	if url == "" || access == "" || secret == "" {
		t.Fatal("BE_SDK_TEST_S3_URL, BE_SDK_TEST_S3_ACCESS_KEY and BE_SDK_TEST_S3_SECRET_KEY must name object storage")
	}
	_, err := s3test.Client(url, access, secret).CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(simBucket)})
	var owned *types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &owned) {
		t.Fatal(err)
	}
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	run := &simRun{t: t, ctx: ctx, reached: map[string]int{}}
	logs, err := os.Create(os.Getenv("SIM_REPLICA_LOG"))
	if err != nil {
		logs, _ = os.Create(os.DevNull)
	}
	defer logs.Close()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	tag := ids.New().String()[:8]
	for _, spec := range []struct{ name, route string }{{"tier_sim_a", tiering.RouteObject}, {"tier_sim_b", tiering.RoutePostgres}} {
		d, admin := dbtest.Schema(t, spec.name, fstest.MapFS{"0001_sim.up.sql": {Data: []byte(simSchemaSQL)}})
		stmts, err := tiering.BarrierSQL([]tiering.Class{simClassDef()})
		if err != nil {
			t.Fatal(err)
		}
		pgtest.Exec(t, admin, stmts...)
		pgtest.Exec(t, admin, "INSERT INTO be_tier_approval (id, scope_kind, scope, approved_by, reason) VALUES ('"+
			ids.New().String()+"', 'class', '"+simClass+"', 'sim', 'the retention has run')")
		env := pgtest.MemberEnv(host, port, spec.name)
		env[tiering.KeyStoreURL], env[tiering.KeyBucket] = url, simBucket
		for _, role := range []string{"WRITE", "READ", "DELETE"} {
			env["TIERING_"+role+"_ACCESS_KEY"], env["TIERING_"+role+"_SECRET_KEY"] = access, secret
		}
		env[tiering.RouteKey(simClass)], env[tiering.KeyGrace] = spec.route, "0s"
		m := &simMember{name: spec.name, prefix: "sim-" + tag + "-" + spec.name, env: env, docs: map[string]*simDoc{}}
		_, cat, err := simMemberCatalog(ctx, simChildMember{Name: m.name, Prefix: m.prefix, Env: env}, "parent", quiet)
		if err != nil {
			t.Fatal(err)
		}
		m.cat.Store(cat)
		m.d = d
		m.queue = work.NewRunner(d, "test/"+m.name, quiet)
		if err := m.queue.Add(m.cat.Load().EraseJobOf(quiet)); err != nil {
			t.Fatal(err)
		}
		run.members = append(run.members, m)
	}
	a, b := run.members[0], run.members[1]
	replicas := []*replica{{members: []*simMember{a, b}, logs: logs}, {members: []*simMember{a, b}, logs: logs},
		{members: []*simMember{a}, logs: logs}, {members: []*simMember{b}, logs: logs}}

	start := time.Now()
	stopCrashes, done, stopWork := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var supervisors, workers sync.WaitGroup
	var next atomic.Int64
	for _, rp := range replicas {
		supervisors.Add(1)
		go run.supervise(&supervisors, rp, stopCrashes, done, &next)
	}
	run.every(&workers, stopWork, 60*time.Millisecond, func() { run.write(run.member()) })
	run.every(&workers, stopWork, 40*time.Millisecond, func() { run.change(run.member()) })
	run.every(&workers, stopWork, 700*time.Millisecond, func() { run.erase(run.member()) })
	run.every(&workers, stopWork, 3*time.Second, func() { run.hold(run.member()) })
	for i := 0; i < 2; i++ {
		run.every(&workers, stopWork, 5*time.Millisecond, func() {
			m := run.member()
			if id, doc := m.pick(nil); doc != nil {
				doc.mu.Lock()
				run.check(m, id, doc)
				doc.mu.Unlock()
			}
		})
	}
	backups := make(chan struct{})
	var backupWG sync.WaitGroup
	run.every(&backupWG, backups, 5*time.Second, func() {
		for _, m := range run.members {
			_ = run.tx(m, func(ctx context.Context, tx db.Tx) error {
				_, err := tx.Exec(ctx, "INSERT INTO sim_backup VALUES (now())")
				return err
			})
		}
	})
	run.every(&backupWG, backups, 60*time.Second, func() { run.progress(start) })
	run.every(&workers, stopWork, 30*time.Second, func() { // a kill at no particular point
		rp := replicas[rand.IntN(len(replicas))]
		rp.mu.Lock()
		if rp.cmd != nil && rp.cmd.Process != nil && rand.IntN(2) == 0 {
			_ = rp.cmd.Process.Kill()
			run.stats.kills.Add(1)
		}
		rp.mu.Unlock()
	})

	half := time.Duration(minutes) * time.Minute / 2
	time.Sleep(half)
	fmt.Println("SIM the routes swap: a to the warm route, b to the object store")
	for _, m := range run.members {
		route := tiering.RoutePostgres
		if m == b {
			route = tiering.RouteObject
		}
		m.mu.Lock()
		m.env[tiering.RouteKey(simClass)] = route
		env := map[string]string{}
		for k, v := range m.env {
			env[k] = v
		}
		m.mu.Unlock()
		_, cat, err := simMemberCatalog(ctx, simChildMember{Name: m.name, Prefix: m.prefix, Env: env}, "parent", quiet)
		if err != nil {
			t.Fatal(err)
		}
		m.cat.Store(cat) // a request in flight keeps the old one
	}
	for _, rp := range replicas { // every replica starts again with the new configuration
		rp.mu.Lock()
		if rp.cmd != nil && rp.cmd.Process != nil {
			_ = rp.cmd.Process.Kill()
		}
		rp.mu.Unlock()
	}
	time.Sleep(half)

	fmt.Println("SIM the crashes and the workload stop; the jobs finish")
	close(stopWork)
	workers.Wait()
	close(stopCrashes)
	if !run.settle(30 * time.Minute) {
		run.fail("the jobs did not finish within 30 minutes of the end")
	}
	run.finalChecks() // the replicas still run: nothing is left for them to do
	run.progress(start)
	close(done)
	supervisors.Wait()
	close(backups)
	backupWG.Wait()

	var missing []string
	for _, p := range simPoints {
		if run.reached[p] == 0 {
			missing = append(missing, p)
		}
	}
	keys := make([]string, 0, len(run.reached))
	for p, n := range run.reached {
		keys = append(keys, fmt.Sprintf("%s=%d", p, n))
	}
	sort.Strings(keys)
	fmt.Printf("SIM crash points reached: %s\n", strings.Join(keys, " "))
	if len(missing) > 0 {
		run.fail("crash points never reached: %v", missing)
	}
	for _, f := range run.failures {
		t.Error(f)
	}
	fmt.Printf("SIM RESULT failures=%d\n", len(run.failures))
}

// settle waits until nothing is left for the jobs: nothing due unsealed, no unit sealed or published, every unit on its
// class's route with no older version kept, no queued erasure, nothing destroyable left; then past the orphans' window.
func (r *simRun) settle(limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		idle := true
		for _, m := range r.members {
			groups, err := m.cat.Load().Groupings(r.ctx, simClass)
			if err != nil || len(groups) > 0 {
				idle = false
				continue
			}
			route := m.cat.Load().Classes[simClass].Routes[0]
			var pending int
			_ = r.tx(m, func(ctx context.Context, tx db.Tx) error {
				return tx.QueryRow(ctx, "SELECT "+
					"(SELECT count(*) FROM be_tier_unit WHERE state IN ('sealed', 'published')) + "+
					"(SELECT count(*) FROM be_tier_unit WHERE state = 'released' AND route <> $1) + "+
					"(SELECT count(*) FROM be_tier_manifest f JOIN be_tier_unit u ON u.id = f.unit WHERE f.version < u.manifest_version "+
					"AND u.state <> 'destroyed' AND f.superseded_at IS NULL) + "+
					"(SELECT count(*) FROM be_work_item WHERE job = $2 AND state <> 'done') + "+
					"(SELECT count(*) FROM be_tier_unit u WHERE u.state = 'released' AND u.anchor_max < now() - interval '3650 days' "+
					"AND NOT EXISTS (SELECT 1 FROM be_tier_hold h JOIN be_tier_member b ON h.scope = $3 || '/' || b.root_id "+
					"WHERE b.unit = u.id AND h.released_at IS NULL))", route, tiering.EraseJob, simClass).Scan(&pending)
			})
			if pending > 0 {
				idle = false
			}
		}
		if idle {
			time.Sleep(simOrphanAge + simCleanupEvery + 15*time.Second) // past the window, and a cleanup since
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// finalChecks: every document, every published copy, the recovery check, the work items.
func (r *simRun) finalChecks() {
	for _, m := range r.members {
		m.mu.Lock()
		docIDs := append([]string(nil), m.ids...)
		m.mu.Unlock()
		for _, id := range docIDs {
			doc := m.docs[id]
			doc.mu.Lock()
			if doc.busy {
				r.fail("%s %s: a queued erasure never finished", m.name, id)
			}
			r.check(m, id, doc)
			doc.mu.Unlock()
		}
		type copyOf struct {
			unit string
			raw  []byte
		}
		var copies []copyOf
		_ = r.tx(m, func(ctx context.Context, tx db.Tx) error {
			rows, err := tx.Query(ctx, "SELECT u.id::text, f.manifest FROM be_tier_unit u JOIN be_tier_manifest f ON f.unit = u.id "+
				"AND f.version = u.manifest_version WHERE u.state IN ('published', 'released')")
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var c copyOf
				if err := rows.Scan(&c.unit, &c.raw); err != nil {
					return err
				}
				copies = append(copies, c)
			}
			return rows.Err()
		})
		for _, c := range copies {
			var man tiering.Manifest
			if err := json.Unmarshal(c.raw, &man); err != nil {
				r.fail("%s unit %s: manifest: %v", m.name, c.unit, err)
				continue
			}
			if err := m.cat.Load().Verify(r.ctx, man); err != nil {
				r.fail("%s unit %s: the published copy does not verify: %v", m.name, c.unit, err)
			}
		}
		reg := &recovery.Checks{}
		if err := reg.Add(m.cat.Load().RecoveryCheck()); err != nil {
			r.fail("%v", err)
			continue
		}
		resp, err := recovery.NewServer(m.d, false, reg).Check(r.ctx, &recoverypb.CheckRequest{Recovery: "sim"})
		if err != nil {
			r.fail("%s: recovery check: %v", m.name, err)
			continue
		}
		for _, res := range resp.GetResults() {
			if res.GetCheck() == tiering.CheckName && res.GetOutcome() != recovery.Pass {
				r.fail("%s: be.tiering %s: %v", m.name, res.GetOutcome(), res.GetDifferences())
			}
		}
		var stuck []string
		_ = r.tx(m, func(ctx context.Context, tx db.Tx) error {
			rows, err := tx.Query(ctx, "SELECT job || ' ' || state FROM be_work_item WHERE state NOT IN ('pending', 'running', 'done')")
			if err != nil {
				return err
			}
			stuck, err = pgx.CollectRows(rows, pgx.RowTo[string])
			return err
		})
		if len(stuck) > 0 {
			r.fail("%s: work items left: %v", m.name, stuck)
		}
		fmt.Printf("SIM %s: %d documents checked, %d copies verified\n", m.name, len(docIDs), len(copies))
	}
}
