package rpc

import (
	"fmt"
	"strconv"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/brickKit/be-sdk-go/internal/problem"
	bev1 "github.com/brickKit/be-sdk-go/proto/be/v1"
)

// batchLimits enforces P7.10 on inbound requests. It keeps, per request message type, the table of
// its repeated fields with their limits, built the first time a message of that type arrives; the
// table covers every service on the server however it was registered, because it is keyed by the
// request's own descriptor. The zero value is ready to use; one value belongs to one server.
type batchLimits struct {
	tables sync.Map // protoreflect.FullName → *limitTable
}

// limitTable is what one message type contributes: its list fields with their limits, and its
// singular message fields to descend into.
type limitTable struct {
	lists  []limitedField
	nested []protoreflect.FieldDescriptor
}

type limitedField struct {
	fd    protoreflect.FieldDescriptor
	limit int
}

// fieldLimit is a repeated field's batch limit: (be.v1.max_items) when set and positive, else
// DefaultMaxItems (P7.10).
func fieldLimit(fd protoreflect.FieldDescriptor) int {
	opts := fd.Options()
	if opts == nil || !proto.HasExtension(opts, bev1.E_MaxItems) {
		return DefaultMaxItems
	}
	if n, ok := proto.GetExtension(opts, bev1.E_MaxItems).(int32); ok && n > 0 {
		return int(n)
	}
	return DefaultMaxItems
}

func (b *batchLimits) table(md protoreflect.MessageDescriptor) *limitTable {
	if t, ok := b.tables.Load(md.FullName()); ok {
		return t.(*limitTable)
	}
	t := &limitTable{}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		switch {
		case fd.IsList():
			t.lists = append(t.lists, limitedField{fd: fd, limit: fieldLimit(fd)})
		case fd.IsMap():
			// a map is not a batch of IDs: no limit
		case fd.Message() != nil:
			t.nested = append(t.nested, fd)
		}
	}
	actual, _ := b.tables.LoadOrStore(md.FullName(), t)
	return actual.(*limitTable)
}

// check returns BATCH_TOO_LARGE for the first repeated field of req (or of a set nested message)
// above its limit, nil otherwise. A value that is not a proto message is not checked.
func (b *batchLimits) check(req any) *problem.Error {
	m, ok := req.(proto.Message)
	if !ok || m == nil {
		return nil
	}
	return b.walk(m.ProtoReflect(), "")
}

func (b *batchLimits) walk(m protoreflect.Message, prefix string) *problem.Error {
	if !m.IsValid() {
		return nil
	}
	t := b.table(m.Descriptor())
	for _, lf := range t.lists {
		if got := m.Get(lf.fd).List().Len(); got > lf.limit {
			return batchTooLarge(prefix+string(lf.fd.Name()), lf.limit, got)
		}
	}
	for _, fd := range t.nested {
		if !m.Has(fd) {
			continue
		}
		if e := b.walk(m.Get(fd).Message(), prefix+string(fd.Name())+"."); e != nil {
			return e
		}
	}
	return nil
}

// batchTooLarge is the P7.10 error: ErrorInfo{BATCH_TOO_LARGE, be, {field, max, got}} plus one
// BadRequest field violation.
func batchTooLarge(field string, max, got int) *problem.Error {
	e := problem.Be("BATCH_TOO_LARGE", map[string]string{
		"field": field, "max": strconv.Itoa(max), "got": strconv.Itoa(got)})
	e.Violations = []problem.Violation{{Field: field, Reason: "BATCH_TOO_LARGE",
		Description: fmt.Sprintf("%d items, at most %d allowed", got, max)}}
	return e
}
