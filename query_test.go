package besdk

import (
	"testing"
	"time"
)

func TestListWindow_未传时间范围时自动注入最近90天(t *testing.T) {
	before := time.Now()
	q := ListWindow(Query{})
	after := time.Now()

	if q.From.IsZero() {
		t.Fatal("From 不该是零值")
	}
	wantEarliest := before.Add(-90 * 24 * time.Hour)
	wantLatest := after.Add(-90 * 24 * time.Hour)
	if q.From.Before(wantEarliest) || q.From.After(wantLatest) {
		t.Fatalf("From 应该是「现在 - 90 天」附近，得到 %v（期望落在 %v ~ %v 之间）",
			q.From, wantEarliest, wantLatest)
	}
	if q.To.IsZero() {
		t.Fatal("To 不该是零值——「到现在」也要显式给一个值，业务代码不该自己判断零值含义")
	}
}

func TestListWindow_已传时间范围不覆盖(t *testing.T) {
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC)
	q := ListWindow(Query{From: from, To: to})
	if !q.From.Equal(from) || !q.To.Equal(to) {
		t.Fatalf("已显式指定的时间范围不该被覆盖，got From=%v To=%v", q.From, q.To)
	}
}

func TestListWindow_未传Limit时给默认值且有上限(t *testing.T) {
	q := ListWindow(Query{})
	if q.Limit <= 0 {
		t.Fatalf("Limit 应该有默认值，得到 %d", q.Limit)
	}

	// ⚠️ 决策 53：List 契约本来就没有 offset 字段，深分页在契约层面就不可
	// 表达——所以这里不测「offset 超阈值报错」（Query 里根本没有这个字段）。
	// 换成测它的等价物：Limit 本身有上限，不许业务代码传一个天文数字把
	// 整张表读出来，逼着走 Cursor 分页。
	big := ListWindow(Query{Limit: 1_000_000})
	if big.Limit >= 1_000_000 {
		t.Fatalf("Limit 应该被夹到某个上限以内，实际原样透传了 %d", big.Limit)
	}
}

func TestListWindow_Cursor原样透传(t *testing.T) {
	q := ListWindow(Query{Cursor: "abc123"})
	if q.Cursor != "abc123" {
		t.Fatalf("Cursor 不该被 ListWindow 改动，得到 %q", q.Cursor)
	}
}
