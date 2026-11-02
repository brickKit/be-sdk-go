package bus

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// P6.34: nothing listens yet, so the component keeps starting and retries every second until it is stopped.
func TestReadyWaitsWhileNothingListens(t *testing.T) {
	for _, url := range []string{
		"nats://127.0.0.1:" + strconv.Itoa(freePort(t)),
		"pgqueue://u:p@127.0.0.1:" + strconv.Itoa(freePort(t)) + "/db?schema=be_bus",
	} {
		var logs bytes.Buffer
		ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
		start := time.Now()
		tr, err := Ready(ctx, url, "demo/x", slog.New(slog.NewTextHandler(&logs, nil)))
		cancel()
		assert.Nil(t, tr, url)
		assert.ErrorIs(t, err, context.DeadlineExceeded, url)
		assert.GreaterOrEqual(t, time.Since(start), 2*time.Second, "waited until stopped: %s", url)
		assert.Contains(t, logs.String(), "waiting for the bus", url)
		assert.NotContains(t, logs.String(), "u:p@", "the credentials never reach the logs")
	}
}

// P6.34: retrying can't fix a BUS_URL that names no transport.
func TestReadyRefusesAWrongSchemeAtOnce(t *testing.T) {
	start := time.Now()
	_, err := Ready(context.Background(), "amqp://127.0.0.1:5672", "demo/x", slog.New(slog.DiscardHandler))
	assert.True(t, errors.Is(err, ErrRefused), "%v", err)
	assert.Less(t, time.Since(start), time.Second)
}
