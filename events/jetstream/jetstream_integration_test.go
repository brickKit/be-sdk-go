//go:build integration

package jetstream

import (
	"context"
	"os"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/transporttest"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests need a JetStream-enabled NATS server in BE_SDK_TEST_NATS_URL (the assembly project's be-nats).
func factory(t transporttest.T) (events.Transport, string) {
	url := os.Getenv("BE_SDK_TEST_NATS_URL")
	if url == "" {
		t.Errorf("BE_SDK_TEST_NATS_URL is not set: the JetStream tests need a NATS server")
		t.FailNow()
	}
	tr, err := Connect(url, "be-sdk-go-test")
	if err != nil {
		t.Errorf("%v", err)
		t.FailNow()
	}
	stream := "sdktest.s" + ids.New().String()[24:]
	t.Cleanup(func() {
		_ = tr.DeleteStream(context.Background(), stream)
		_ = tr.Close()
	})
	return tr, stream
}

func TestJetStreamPassesTheSuite(t *testing.T) { transporttest.Run(t, factory) }

func TestStreamsKeepMessagesForTheTransportRetention(t *testing.T) { // P6.27
	tr, stream := factory(t)
	require.NoError(t, tr.EnsureStream(context.Background(), events.Stream{Name: stream, Partitions: 1}))
	info, err := tr.(*Transport).js.Stream(context.Background(), events.JetStreamName(stream))
	require.NoError(t, err)
	assert.Equal(t, events.TransportRetention, info.CachedInfo().Config.MaxAge)
}
