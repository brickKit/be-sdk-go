package flow

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/notify"
)

// TaskTemplate is the template of the approval intents (P18.10): its parameters are the task type and the document
// number.
const TaskTemplate = "be.flow.task.pending"

// MaxNotified is how many candidates one approval intent names (P18.4's limit); the rest see the task in the
// worklist.
const MaxNotified = 1000

// TaskLink is the front end's path of a task in the unified worklist (P18.10).
func TaskLink(taskID string) string { return "/workflow/tasks/" + taskID }

// TaskCollapseKey is the merge key of a task's intents (P18.10): its version is the sequence.
func TaskCollapseKey(taskID string) string { return "task:" + taskID }

// intend emits the task's notification intent with its projection (P18.10): a pending task with candidates is
// delivered to them; any other state withdraws what its earlier versions delivered. The sequence is the task's
// version, so the notification component keeps the latest whatever order they arrive in (P18.7).
func (k *Kernel) intend(ctx context.Context, tx db.Tx, t Task) error {
	in := notify.Intent{Sender: k.Component, Category: notify.ApprovalTask, CollapseKey: TaskCollapseKey(t.ID),
		CollapseSeq: t.Version}
	if t.State != Pending || len(t.Candidates) == 0 {
		in.Kind = notify.Withdraw
		_, err := notify.Emit(ctx, tx, k.Events, in)
		return err
	}
	in.Kind = notify.Deliver
	cands := t.Candidates // sorted by the kernel (setCandidates)
	if len(cands) > MaxNotified {
		cands = cands[:MaxNotified]
	}
	for _, c := range cands {
		in.Recipients = append(in.Recipients, notify.Recipient{Principal: c})
	}
	in.Title = title(t)
	in.Link = TaskLink(t.ID)
	in.Template, in.Params = TaskTemplate, map[string]string{"task_type": t.TaskType}
	if t.Document.Number != nil {
		in.Params["number"] = *t.Document.Number
	}
	now := time.Now()
	in.OccurredAt = canon.Time(now)
	in.ExpiresAt = canon.Time(now.Add(notify.DefaultExpiry))
	if t.DueAt != nil { // an approval's intent lasts until its task is due (P18.6)
		if due, err := time.Parse(time.RFC3339Nano, *t.DueAt); err == nil && due.After(now) {
			in.ExpiresAt = *t.DueAt
		}
	}
	_, err := notify.Emit(ctx, tx, k.Events, in)
	return err
}

// title is the task summary's title cut to 200 characters (P18.5, P18.10); a summary without one names the task
// type and the number.
func title(t Task) string {
	s := t.Summary.Title
	if s == "" {
		s = t.TaskType
		if t.Document.Number != nil {
			s += " " + *t.Document.Number
		}
	}
	if r := []rune(s); len(r) > 200 {
		s = string(r[:200])
	}
	return s
}
