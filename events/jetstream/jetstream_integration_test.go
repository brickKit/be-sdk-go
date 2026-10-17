//go:build integration

package jetstream

import (
	"context"
	"os"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/transporttest"
	"github.com/brickKit/be-sdk-go/v3/ids"
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
