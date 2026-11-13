package flow

// The interactive actions (be-protocol P17.19, P17.20; chapter 20): an action grant for each principal candidate of a
// pending task's intent, not a secret; the channel members' action events taken in the consumer's transaction, the
// checks of channel.Take, and the decision of P17.10 for the grant's principal.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/audit"
	sdkauthz "github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/channel"
	"github.com/brickKit/be-sdk-go/v3/clock"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/notify"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/jackc/pgx/v5"
)

// ActionSourcesKey is the deployment's channel members whose actions a flow manager takes (P17.20).
const ActionSourcesKey = "FLOW_ACTION_SOURCES"

// Keys are the configuration keys of a component that runs the kernel.
func Keys() []config.Key {
	return []config.Key{{Name: ActionSourcesKey, Type: config.String}}
}

// UseActions configures the kernel's action sources from FLOW_ACTION_SOURCES and its principals' authorization from
// the runtime (P17.19, P17.20): a component that runs the kernel calls it in New, adds Keys() to its keys and
// ActionSubs() to its subscriptions.
func (k *Kernel) UseActions(rt *runtime.Runtime) error {
	src, err := ParseActionSources(rt.Config.String(ActionSourcesKey))
	if err != nil {
		return err
	}
	k.ActionSources = src
	k.Subject = func(p string) *sdkauthz.Subject {
		s, _ := rt.Subject(p)
		return s
	}
	return nil
}

// ParseActionSources reads FLOW_ACTION_SOURCES: comma-separated component IDs; empty: none. A malformed or repeated
// one stops the member (P17.20).
func ParseActionSources(v string) ([]string, error) {
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}
	var out []string
	for _, c := range strings.Split(v, ",") {
		c = strings.TrimSpace(c)
		if !componentForm(c) || contains(out, c) {
			return nil, errors.New("flow: " + ActionSourcesKey + " is comma-separated component IDs, each once: " + v)
		}
		out = append(out, c)
	}
	return out, nil
}

func componentForm(c string) bool {
	scope, name, ok := strings.Cut(c, "/")
	ok = ok && scope != "" && name != ""
	for _, r := range c {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '/') {
			return false
		}
	}
	return ok
}

// GrantLife is the longest an action grant lasts (protocol 10's parameter, P17.19); Grace is how long after its
// expiry an action clicked in time is still processed (P20.8).
const (
	GrantLife = 7 * 24 * time.Hour
	Grace     = time.Hour
)

func (k *Kernel) label(c Choice) string {
	if l, ok := k.ActionLabels[c]; ok {
		return l
	}
	return map[Choice]string{Approve: "同意", Reject: "驳回"}[c]
}

// grants issues one action grant for each principal candidate of a pending task's intent (P17.19), in tx; a
// candidate whose security version the view cannot tell right now gets none (the link still works). Nothing without
// action sources.
func (k *Kernel) grants(ctx context.Context, tx db.Tx, t Task, cands []string, expires time.Time) (map[string]notify.Grant, error) {
	if len(k.ActionSources) == 0 || k.Subject == nil {
		return nil, nil
	}
	d, err := k.definition(t.instance.FlowKey, t.instance.FlowVersion)
	if err != nil {
		return nil, err
	}
	allowed := []Choice{Approve}
	if !d.RejectComment { // a rejection that must say why is not a button (P17.19)
		allowed = append(allowed, Reject)
	}
	var acts []string
	var choices []notify.Choice
	for _, c := range allowed {
		acts = append(acts, string(c))
		choices = append(choices, notify.Choice{Action: string(c), Label: k.label(c)})
	}
	if max := time.Now().Add(GrantLife); expires.After(max) {
		expires = max
	}
	out := map[string]notify.Grant{}
	for _, p := range cands {
		subj := k.Subject(p)
		var sv int64
		if subj != nil {
			sv, err = subj.SecurityVersion(d.Operation)
		}
		if subj == nil || err != nil { // not judged now: no grant, the link still works (P17.19)
			why := "no authorization view"
			if err != nil {
				why = err.Error()
			}
			k.logger().Warn("an approval candidate gets no button", "event", "flow_grant_skipped", "task", t.ID,
				"principal", p, "why", why)
			continue
		}
		id := ids.New().String()
		if _, err := tx.Exec(ctx, `INSERT INTO be_flow_action_grant (id, task_id, principal, task_version, security_version,
			actions, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`, id, t.ID, p, t.Version, sv, acts, expires); err != nil {
			return nil, err
		}
		out[p] = notify.Grant{ID: id, Actions: choices}
	}
	return out, nil
}

// ActionSubs are the subscriptions to the configured channel members' action events (P17.20); none without sources.
func (k *Kernel) ActionSubs() []events.Sub {
	var out []events.Sub
	for _, src := range k.ActionSources {
		src := src
		st := notify.Stream(src)
		out = append(out, events.Sub{Name: "flow-actions-" + strings.ReplaceAll(st, ".", "-"), Streams: []string{st},
			Types:  []string{channel.ActionType(src)},
			Handle: func(ctx context.Context, tx db.Tx, e events.Received) error { return k.onAction(ctx, tx, src, e) }})
	}
	return out
}

type grantRow struct {
	id, task, principal string
	version, security   int64
	actions             []string
	expires             time.Time
	resultAction        *string
	result              *string
}

// errRetry is an outcome that records nothing: the event is nacked and delivered again (P20.8).
type errRetry struct{ reason string }

func (e errRetry) Error() string { return "flow: the action cannot be decided right now: " + e.reason }

// onAction takes one action event in, in the consumer's transaction (P20.7, P20.8): the checks of channel.Take in
// order, then the kernel's decision for the grant's principal; the final result recorded on the grant, a refusal
// audited and told to its holder, a retryable one nacked.
func (k *Kernel) onAction(ctx context.Context, tx db.Tx, src string, e events.Received) error {
	var a channel.Action
	shape := json.Unmarshal(e.Data, &a) == nil && channel.Validate("action", e.Data) == nil
	arr := channel.Arrival{ShapeOK: shape, AudienceIsSelf: a.Audience == k.Component,
		StreamIsSource: e.Stream == notify.Stream(src) && e.Source == "urn:be:"+src && a.Channel == src,
		Prior:          channel.PriorNone, ClickedInTime: true, ProcessedInTime: true}
	var g grantRow
	if shape && arr.AudienceIsSelf && arr.StreamIsSource && uuidForm(a.GrantID) {
		// the grant first, locked: only the action path locks it, so two actions on one grant take turns (P20.8)
		err := tx.QueryRow(ctx, `SELECT id::text, task_id::text, principal, task_version, security_version, actions,
			expires_at, result_action, result FROM be_flow_action_grant WHERE id = $1 FOR UPDATE`, a.GrantID).Scan(
			&g.id, &g.task, &g.principal, &g.version, &g.security, &g.actions, &g.expires, &g.resultAction, &g.result)
		switch {
		case err == nil:
			arr.GrantExists = true
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
	}
	if arr.GrantExists {
		arr.PrincipalMatches = a.Principal == g.principal
		arr.ActionAllowed = contains(g.actions, a.Action)
		if g.resultAction != nil {
			arr.Prior = channel.PriorOtherAction
			if *g.resultAction == a.Action {
				arr.Prior = channel.PriorSameAction
			}
		}
		clicked, err := time.Parse(time.RFC3339Nano, a.ClickedAt)
		arr.ClickedInTime = err == nil && !clicked.After(g.expires)
		arr.ProcessedInTime = !time.Now().After(g.expires.Add(Grace))
	}
	probe := arr // the action's own checks, as if the decision approved: executed means they all passed
	probe.Decision = channel.Approved
	o := channel.Take(probe)
	if o.Answer == channel.Executed { // the grant's principal: its security version, then the kernel's decision
		subj := k.Subject(g.principal)
		if subj == nil {
			return errRetry{reason: channel.ViewUnprovable}
		}
		d, err := k.taskDefinition(ctx, tx, g.task)
		if err != nil {
			return err
		}
		sv, err := subj.SecurityVersion(d.Operation)
		if err != nil {
			if Retryable(err) {
				return errRetry{reason: channel.ViewUnprovable}
			}
			return err
		}
		arr.SecurityVersionNewer = sv > g.security
		if arr.SecurityVersionNewer {
			o = channel.Take(arr)
		} else {
			arr.Decision, err = k.actionDecision(ctx, tx, subj, d, g, Choice(a.Action))
			if err != nil {
				return err
			}
			o = channel.Take(arr)
		}
	}
	switch o.Answer {
	case channel.Ignored, channel.Replayed:
		return nil
	case channel.Retry:
		return errRetry{reason: *o.Reason}
	}
	reason := ""
	if o.Reason != nil {
		reason = *o.Reason
	}
	if o.Record {
		result := channel.Approved
		if o.Answer == channel.Refused {
			result = reason
		}
		if _, err := tx.Exec(ctx, `UPDATE be_flow_action_grant SET result_action = $2, result = $3, result_at = now()
			WHERE id = $1`, g.id, a.Action, result); err != nil {
			return err
		}
	}
	if o.NotifyHolder {
		if err := k.tellHolder(ctx, tx, g, reason); err != nil {
			return err
		}
	}
	if o.Audit && k.Audit != nil {
		target := audit.TargetOf("flow.action_grant", a.GrantID, map[string]any{"channel": src, "action": a.Action})
		if derr := k.Audit.Denial(ctx, audit.Record{Operation: "be.flow.action", Actor: a.Principal, Target: target,
			Reason: reason}); derr != nil {
			k.logger().Warn("a refused action's audit record failed", "error", derr.Error())
		}
	}
	return nil
}

// taskDefinition is the definition of a grant's task.
func (k *Kernel) taskDefinition(ctx context.Context, tx db.Tx, task string) (Definition, error) {
	t, err := k.readTask(ctx, tx, task, false)
	if err != nil {
		return Definition{}, err
	}
	return k.definition(t.instance.FlowKey, t.instance.FlowVersion)
}

// actionDecision is P17.10 for the grant's principal at the grant's task version: approved, a final refusal's reason,
// or a retryable one's (channel.ViewUnprovable, channel.QualificationUnknown). The view's cutoff is checked before the
// transaction commits, as a request's is (P8.12).
func (k *Kernel) actionDecision(ctx context.Context, tx db.Tx, subj *sdkauthz.Subject, d Definition, g grantRow,
	c Choice) (string, error) {
	cctx, _, err := subj.Decider(ctx, d.Operation)
	if err != nil {
		if Retryable(err) {
			return channel.ViewUnprovable, nil
		}
		return "", err
	}
	_, err = k.decideIn(cctx, tx, subj, g.task, DecisionIn{Version: g.version, Decision: c})
	var r refused
	switch {
	case err == nil:
	case errors.As(err, &r):
		if r.reason == errs.ReasonQualificationUnknown {
			return channel.QualificationUnknown, nil
		}
		return r.reason, nil
	case Retryable(err):
		return channel.ViewUnprovable, nil
	default:
		var e *errs.Error
		if errors.As(err, &e) { // a refusal of the route's own checks (a rejection without a comment)
			return e.Reason, nil
		}
		return "", err
	}
	if cutoff, ok := reqctx.Cutoff(cctx); ok && clock.Boot() >= cutoff {
		return "", errRetry{reason: channel.ViewUnprovable} // the evidence ended before the commit: roll it back
	}
	return channel.Approved, nil
}

// tellHolder writes an in-app intent to the grant's holder: the action was not carried out, and why (P20.8).
func (k *Kernel) tellHolder(ctx context.Context, tx db.Tx, g grantRow, reason string) error {
	now := time.Now()
	in := notify.Intent{Sender: k.Component, Category: notify.ApprovalTask, Kind: notify.Deliver,
		Recipients: []notify.Recipient{{Principal: g.principal}}, Title: "按钮的操作没有执行：" + readable(reason),
		Template: ActionRefusedTemplate, Params: map[string]string{"reason": reason},
		Link: TaskLink(g.task), OccurredAt: canon.Time(now), ExpiresAt: canon.Time(now.Add(notify.DefaultExpiry)),
		InAppOnly: true} // the channel the button was in is no place to say it did not work (P20.8)
	_, err := notify.Emit(ctx, tx, k.Events, in)
	return err
}

// ActionRefusedTemplate is the note's template; its param "reason" is the refusal's code (P20.8).
const ActionRefusedTemplate = "be.flow.action.refused"

// readable is a refusal's reason as the holder reads it (P20.8); the code goes in the params.
func readable(reason string) string {
	switch reason {
	case errs.ReasonGrantUsed:
		return "这个按钮已经用来做过另一个决定"
	case errs.ReasonGrantStale:
		return "你的权限变了，请到系统里处理"
	case "expired": // the action's own outcome reason (channel.Take), not an error code
		return "按钮已经过期，请到系统里处理"
	case errs.ReasonTaskNotPending:
		return "任务已经处理过了"
	case errs.ReasonVersionConflict:
		return "任务在你点按钮之后变了，请到系统里重新处理"
	case errs.ReasonPermissionDenied:
		return "你现在不能处理这个任务"
	}
	return "请到系统里处理（" + reason + "）"
}

// Retryable says whether an error means "cannot decide right now": the view expired or cannot prove the
// authorization (P8.12, P8.24), a definition not accepted yet.
func Retryable(err error) bool {
	return errs.Is(err, "be", errs.ReasonAuthorizationViewExpired) || errs.Is(err, "be", errs.ReasonAuthorizationUnprovable) ||
		errs.Is(err, "be", errs.ReasonDefinitionPending)
}
