package events

import (
	"context"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/stretchr/testify/assert"
)

func TestAWatermarkStreamDeclaresItsRecordReads(t *testing.T) { // P9.30
	reads := &Records{Operation: "demo/x.read", Readers: []string{"demo/a"}, Read: nil, Page: nil}
	full := *reads
	full.Read = func(context.Context, db.Tx, []string) ([]ReadResult, error) { return nil, nil }
	full.Page = func(context.Context, db.Tx, string, int) ([]Record, error) { return nil, nil }
	wm := &Watermark{Operation: "demo/x.pull", Pullers: []string{"demo/a"}}
	assert.ErrorContains(t, Stream{Name: "demo.x", Partitions: 1, Watermark: wm}.Validate(), "declares its record reads")
	assert.ErrorContains(t, Stream{Name: "demo.x", Partitions: 1, Watermark: wm, Records: reads}.Validate(),
		"both read functions")
	other := full
	other.Readers = []string{"demo/b"}
	assert.ErrorContains(t, Stream{Name: "demo.x", Partitions: 1, Watermark: wm, Records: &other}.Validate(),
		"puller demo/a is not among the record readers")
	assert.NoError(t, Stream{Name: "demo.x", Partitions: 1, Watermark: wm, Records: &full}.Validate())
	assert.NoError(t, Stream{Name: "demo.x", Partitions: 1, Records: &full}.Validate(), "reads without a watermark")
}
