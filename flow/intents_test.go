package flow

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// P18.5, P18.10: the intent's title is the summary's title cut to 200 characters (not bytes); without one, the task
// type and the number.
func TestTheIntentsTitle(t *testing.T) {
	n := "PO-1"
	long := strings.Repeat("采", 250)
	assert.Equal(t, strings.Repeat("采", 200), title(Task{Summary: Summary{Title: long}}))
	assert.Equal(t, "采购订单 PO-1", title(Task{Summary: Summary{Title: "采购订单 PO-1"}}))
	assert.Equal(t, "demo.order.approval PO-1", title(Task{TaskType: "demo.order.approval", Document: TaskDocument{Number: &n}}))
	assert.Equal(t, "demo.order.approval", title(Task{TaskType: "demo.order.approval"}))
}
