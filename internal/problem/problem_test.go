package problem

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

func TestStatusRoundTripKeepsIdentity(t *testing.T) {
	c := NewCatalogue()
	e := &Error{Code: codes.ResourceExhausted, Domain: DomainBe, Reason: "OUTBOUND_LIMIT",
		Metadata: map[string]string{"target": "erp/inventory"}, RetryAfter: 1500 * time.Millisecond}
	back := FromStatus(c.ToStatus(e, "en"))
	if back.Code != e.Code || back.Domain != e.Domain || back.Reason != e.Reason ||
		back.Metadata["target"] != "erp/inventory" || back.RetryAfter != e.RetryAfter {
		t.Fatalf("round trip lost identity: %+v", back)
	}
}

func TestStatusHidesInternal(t *testing.T) {
	c := NewCatalogue()
	e := &Error{Code: codes.Internal, Domain: "erp/x", Reason: "DB_BROKE", Detail: "SELECT secret", Cause: errors.New("pq: boom")}
	st := c.ToStatus(e, "en")
	back := FromStatus(st)
	if back.Reason != "INTERNAL" || back.Domain != DomainBe || st.Message() == "SELECT secret" {
		t.Fatalf("internal leaked: %v %+v", st.Message(), back)
	}
}

func TestFromClassifiesContextErrors(t *testing.T) {
	if e := From(fmt.Errorf("wrap: %w", context.Canceled)); e.Code != codes.Canceled {
		t.Fatalf("cancel: %+v", e)
	}
	if e := From(context.DeadlineExceeded); e.Reason != "DEADLINE_BUDGET_EXHAUSTED" || e.Code != codes.DeadlineExceeded {
		t.Fatalf("deadline: %+v", e)
	}
	if e := From(errors.New("x")); e.Reason != "INTERNAL" || e.Cause == nil {
		t.Fatalf("plain: %+v", e)
	}
	inner := Be("LOCK_TIMEOUT", nil)
	if e := From(fmt.Errorf("ctx: %w", inner)); e != inner {
		t.Fatalf("wrapped protocol error not found")
	}
}

func TestTextRendersCatalogueInLocale(t *testing.T) {
	c := NewCatalogue()
	e := Be("MISSING_PERMISSION", map[string]string{"permission": "erp.sales.view"})
	if _, d := c.Text(e, "zh-CN"); d != "你没有权限 erp.sales.view。" {
		t.Fatalf("zh: %q", d)
	}
	if title, d := c.Text(e, "en"); title != "Not permitted" || d != "You do not have the permission erp.sales.view." {
		t.Fatalf("en: %q %q", title, d)
	}
}

func TestAddComponentCatalogue(t *testing.T) {
	c := NewCatalogue()
	ok := []byte("domain: erp/x\nreasons:\n  - {reason: NOT_DRAFT, code: FAILED_PRECONDITION, http: 400, params: [s], title: {en: T, zh: 标}, message: {en: \"state {s}\", zh: \"状态 {s}\"}, since: \"1.0\", deprecated: false}\n")
	if err := c.Add(ok); err != nil {
		t.Fatal(err)
	}
	if _, d := c.Text(New(codes.FailedPrecondition, "erp/x", "NOT_DRAFT", map[string]string{"s": "DONE"}, ""), "en"); d != "state DONE" {
		t.Fatalf("render: %q", d)
	}
	reserved := []byte("domain: erp/y\nreasons:\n  - {reason: NOT_FOUND, code: NOT_FOUND, http: 404, params: [], title: {en: T}, message: {en: M}, since: \"1.0\", deprecated: false}\n")
	if err := c.Add(reserved); err == nil {
		t.Fatal("a reserved reason in a component domain must be refused")
	}
	if err := c.Add([]byte("domain: be\nreasons: []\n")); err == nil {
		t.Fatal("the be domain cannot be redefined")
	}
}

func TestBeUnknownReasonIsInternal(t *testing.T) {
	if e := Be("NO_SUCH_REASON", nil); e.Reason != "INTERNAL" || e.Code != codes.Internal {
		t.Fatalf("%+v", e)
	}
}
