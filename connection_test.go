package besdk

import (
	"net/url"
	"strings"
	"testing"
)

func pgCfg(over map[string]string) Config {
	m := map[string]string{
		"PG_HOST": "db", "PG_PORT": "5432", "PG_DATABASE": "brickkit_db",
		"PG_USER": "erp_sales", "PG_PASSWORD": "pw",
	}
	for k, v := range over {
		m[k] = v
	}
	return NewConfig(m)
}

func TestPGDSN(t *testing.T) {
	dsn, err := PGDSN(pgCfg(nil))
	if err != nil {
		t.Fatal(err)
	}
	if dsn != "postgres://erp_sales:pw@db:5432/brickkit_db" {
		t.Fatalf("dsn = %s", dsn)
	}
}

func TestPGDSNEscapesSpecialPassword(t *testing.T) {
	dsn, err := PGDSN(pgCfg(map[string]string{"PG_PASSWORD": "p@ss:w/rd%1"}))
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if pw, _ := u.User.Password(); pw != "p@ss:w/rd%1" {
		t.Fatalf("password round-trip = %q", pw)
	}
}

func TestPGDSNEmptyPasswordAllowedButKeyRequired(t *testing.T) {
	if _, err := PGDSN(pgCfg(map[string]string{"PG_PASSWORD": ""})); err != nil {
		t.Fatalf("空口令应允许（trust 认证）：%v", err)
	}
	c := NewConfig(map[string]string{"PG_HOST": "db", "PG_PORT": "5432", "PG_DATABASE": "d", "PG_USER": "u"})
	_, err := PGDSN(c)
	if err == nil || !strings.Contains(err.Error(), "PG_PASSWORD") {
		t.Fatalf("缺 PG_PASSWORD 应报错并点名，got %v", err)
	}
}

func TestPGDSNListsAllMissing(t *testing.T) {
	_, err := PGDSN(NewConfig(map[string]string{"PG_PASSWORD": "x"}))
	if err == nil {
		t.Fatal("应报错")
	}
	for _, k := range []string{"PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER"} {
		if !strings.Contains(err.Error(), k) {
			t.Fatalf("错误信息应点名 %s：%v", k, err)
		}
	}
}

func TestNATSURL(t *testing.T) {
	if v, err := NATSURL(NewConfig(map[string]string{"NATS_URL": "nats://n:4222"})); err != nil || v != "nats://n:4222" {
		t.Fatalf("NATSURL = %q,%v", v, err)
	}
	if _, err := NATSURL(NewConfig(nil)); err == nil {
		t.Fatal("缺 NATS_URL 应报错")
	}
}
