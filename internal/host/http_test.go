package host

import (
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/stretchr/testify/assert"
)

func TestServersKeepIdleConnectionsLongerThanClients(t *testing.T) { // P1.20
	// a server that closes an idle keep-alive connection first races the client reusing it: the request fails with
	// EOF, and a POST is not retried (measured: 2 in 2000 at 200/s on a saturated Python server that closed at 5 s)
	s := NewHTTPServer(nil)
	assert.GreaterOrEqual(t, s.IdleTimeout, 120*time.Second)
	assert.Greater(t, s.IdleTimeout, calls.ClientIdleTimeout, "the client lets an idle connection go before the server")
	assert.Equal(t, 10*time.Second, s.ReadHeaderTimeout)
}
