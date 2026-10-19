//go:build integration

package pgqueue

// Concurrent publishing (P6.30): 8 publishers each publishing new messages of 100 bytes, messages a second. A NOTIFY in
// the inserting transaction made these commits take an instance-wide lock one at a time; the NOTIFY in a statement of
// its own lets the inserts commit together.
// Run: go test -tags integration -run '^$' -bench ConcurrentPublish -benchtime 8000x -count 3 ./events/pgqueue/

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
)

func BenchmarkConcurrentPublish(b *testing.B) {
	tr, stream := factory(b)
	ctx := context.Background()
	// the test bus is shared: every later subscription would read past what is left (see forget)
	b.Cleanup(func() {
		pgtest.Exec(b, pgtest.Admin(b, pgtest.Database), "DELETE FROM be_bus_test.be_bus_message WHERE subject = '"+stream+".bench'")
	})
	data := make([]byte, 100)
	const publishers = 8
	var next atomic.Int64
	var wg sync.WaitGroup
	b.ResetTimer()
	start := time.Now()
	for range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= int64(b.N) {
				m := events.Message{Subject: stream + ".bench", Data: data,
					Headers: map[string]string{events.HeaderID: ids.New().String()}}
				if err := tr.Publish(ctx, m); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "msgs/s")
}
