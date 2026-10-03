package events

import (
	"context"
	"errors"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/stretchr/testify/require"
)

type fakeTopology struct {
	streams  map[string][]string
	dlq      int
	durables map[string]string // durable -> stream|filter
	failOn   string
}

func newFakeTopology() *fakeTopology {
	return &fakeTopology{streams: map[string][]string{}, durables: map[string]string{}}
}

func (f *fakeTopology) EnsureStream(ctx context.Context, name string, subjects []string, o jetstream.StreamOptions) error {
	if name == f.failOn {
		return errors.New("stream " + name + ": permission denied")
	}
	f.streams[name] = subjects
	return nil
}

func (f *fakeTopology) EnsureDLQStream(ctx context.Context) error { f.dlq++; return nil }

func (f *fakeTopology) EnsureDurable(ctx context.Context, stream, durable, filter string) ([]string, error) {
	f.durables[durable] = stream + "|" + filter
	if durable == "erp_finance__sales__order__created__v1" {
		return []string{"ack_wait is 1m0s, not 30s"}, nil
	}
	return nil, nil
}

func TestEnsureTopologyCreatesStreamsDLQAndDurables(t *testing.T) {
	f := newFakeTopology()
	warnings, err := EnsureTopology(context.Background(), f, "erp/finance",
		[]string{"erp.finance.entry.posted.v1", "erp.finance.period.locked.v1"},
		[]string{"sales.order.created.v1", "erp.sales.order.confirmed.v1"})
	require.NoError(t, err)
	require.Equal(t, map[string][]string{"BE_ERP": {"erp.>"}, "BE_SALES": {"sales.>"}}, f.streams)
	require.Equal(t, 1, f.dlq)
	require.Equal(t, map[string]string{
		"erp_finance__sales__order__created__v1":        "BE_SALES|sales.order.created.v1",
		"erp_finance__erp__sales__order__confirmed__v1": "BE_ERP|erp.sales.order.confirmed.v1",
	}, f.durables)
	require.Equal(t, []string{"erp_finance__sales__order__created__v1: ack_wait is 1m0s, not 30s"}, warnings)
}

func TestEnsureTopologyErrors(t *testing.T) {
	f := newFakeTopology()
	f.failOn = "BE_ERP"
	_, err := EnsureTopology(context.Background(), f, "erp/finance", []string{"erp.finance.entry.posted.v1"}, nil)
	require.ErrorContains(t, err, "BE_ERP")

	_, err = EnsureTopology(context.Background(), newFakeTopology(), "erp/finance", nil, []string{"sales.order.*"})
	require.Error(t, err)
	_, err = EnsureTopology(context.Background(), newFakeTopology(), "erp/finance", []string{"Bad"}, nil)
	require.Error(t, err)
	_, err = EnsureTopology(context.Background(), newFakeTopology(), "erp_finance", nil, []string{"sales.order.created.v1"})
	require.Error(t, err)
}

func TestJetStreamBusIsATopology(t *testing.T) {
	var _ Topology = (*jetstream.Bus)(nil)
	var _ Publisher = (*jetstream.Bus)(nil)
	var _ Settler = (*jetstream.Delivery)(nil)
	var _ Source = JetStream{}
}
